/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import java.nio.ByteBuffer

import kafka.api.{ApiVersion, KAFKA_2_1_IV0}
import kafka.common.LongRef
import kafka.message.{CompressionCodec, NoCompressionCodec, ZStdCompressionCodec}
import kafka.utils.Logging
import org.apache.kafka.common.errors.{InvalidTimestampException, UnsupportedCompressionTypeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}

private[kafka] object LogValidator extends Logging {

  /**
   * 更新给定消息集的offset，并对消息进行进一步的验证
   * 1. 压缩的主题消息必须有key
   * 2. 当消息版本 ≥ 1时，压缩消息集的内部消息必须有从0开始单调递增的偏移量
   * 3. 当消息版本 ≥ 1时，验证并覆盖消息的时间戳
   * 4. DefaultRecordBatch中声明的消息数必须和其中包含的有效记录数匹配
   *
   * 此方法会必要的情况将消息转换成topic配置的消息格式版本，如果消息没有格式转换或者值覆盖，此方法不会执行任何操作，避免代价高昂的重压缩
   *
   * 返回的ValidationAndOffsetAssignResult对象包含了已校验的消息集，最大时间戳，拥有最大时间戳消息的偏移量
   * 以及表明消息大小是否发生变化的标识位
   */
  private[kafka] def validateMessagesAndAssignOffsets(records: MemoryRecords,
                                                      offsetCounter: LongRef,
                                                      time: Time,
                                                      now: Long,
                                                      sourceCodec: CompressionCodec,
                                                      targetCodec: CompressionCodec,
                                                      compactedTopic: Boolean,
                                                      magic: Byte,
                                                      timestampType: TimestampType,
                                                      timestampDiffMaxMs: Long,
                                                      partitionLeaderEpoch: Int,
                                                      isFromClient: Boolean,
                                                      interBrokerProtocolVersion: ApiVersion): ValidationAndOffsetAssignResult = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // 如果消息没有进行压缩
      // 校验消息的版本信息
      if (!records.hasMatchingMagic(magic))
        convertAndAssignOffsetsNonCompressed(records, offsetCounter, compactedTopic, time, now, timestampType,
          timestampDiffMaxMs, magic, partitionLeaderEpoch, isFromClient)
      else
      // 就地进行消息检验，分配offset以及设置时间戳
        assignOffsetsNonCompressed(records, offsetCounter, now, compactedTopic, timestampType, timestampDiffMaxMs,
          partitionLeaderEpoch, isFromClient, magic)
    } else {
      validateMessagesAndAssignOffsetsCompressed(records, offsetCounter, time, now, sourceCodec, targetCodec, compactedTopic,
        magic, timestampType, timestampDiffMaxMs, partitionLeaderEpoch, isFromClient, interBrokerProtocolVersion)
    }
  }

  /**
   * 消息转换，偏移量分配，消息没有进行压缩
   * @param records
   * @param offsetCounter
   * @param compactedTopic
   * @param time
   * @param now
   * @param timestampType
   * @param timestampDiffMaxMs
   * @param toMagicValue
   * @param partitionLeaderEpoch
   * @param isFromClient
   * @return
   */
  private def convertAndAssignOffsetsNonCompressed(records: MemoryRecords,
                                                   offsetCounter: LongRef,
                                                   compactedTopic: Boolean,
                                                   time: Time,
                                                   now: Long,
                                                   timestampType: TimestampType,
                                                   timestampDiffMaxMs: Long,
                                                   toMagicValue: Byte,
                                                   partitionLeaderEpoch: Int,
                                                   isFromClient: Boolean): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    // 计算消息压缩后需要占用的字节数
    val sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.value,
      CompressionType.NONE, records.records)

    val (producerId, producerEpoch, sequence, isTransactional) = {
      // 获取record头部信息，构建producerId，producerEpoch，sequence，isTransactional
      val first = records.batches.asScala.head
      (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
    }
    // 为压缩后的空间申请对外内存
    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    // 构建日志数据的写入通道
    val builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, timestampType,
      offsetCounter.value, now, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch)
    // 获取record中的第一个batch，压缩的情况下不允许有多个batch
    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, NoCompressionCodec)
    // 遍历records中所有的batch
    for (batch <- records.batches.asScala) {
      // 检验batch信息，尤其是版本号
      validateBatch(firstBatch, batch, isFromClient, toMagicValue)
      // 遍历batch的record
      for (record <- batch.asScala) {
        // 校验record
        validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
        // 追加到内存中
        // 每个record都有一个单独的offset
        builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
      }
    }

    val convertedRecords = builder.build()

    val info = builder.info
    val recordConversionStats = new RecordConversionStats(builder.uncompressedBytesWritten,
      builder.numRecords, time.nanoseconds - startNanos)
    ValidationAndOffsetAssignResult(
      validatedRecords = convertedRecords,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordConversionStats = recordConversionStats)
  }

  /**
   * 获取records中的第一个batch
   * @param records     需要落地的records
   * @param sourceCodec 压缩算法
   * @return
   */
  private[kafka] def getFirstBatchAndMaybeValidateNoMoreBatches(records: MemoryRecords, sourceCodec: CompressionCodec): RecordBatch = {
    // 获取records迭代器
    val batchIterator = records.batches.iterator
    // records中没有batch，抛出异常
    if (!batchIterator.hasNext) {
      throw new InvalidRecordException("Record batch has no batches at all")
    }
    // 获取第一个batch
    val batch = batchIterator.next()

    // 如果消息格式是V2或者之前，或者消息已经被压缩，需要交叉是否仅有一个batch
    if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || sourceCodec != NoCompressionCodec) {
      if (batchIterator.hasNext) {
        // 压缩的外部batch有多个batch
        throw new InvalidRecordException("Compressed outer record has more than one batch")
      }
    }
    // 返回第一个batch
    batch
  }

  /**
   * 对即将写入的batch进行校验
   * @param firstBatch   第一个batch
   * @param batch        当前batch
   * @param isFromClient 是否来自客户端
   * @param toMagic      需要进行转换的消息版本
   */
  private def validateBatch(firstBatch: RecordBatch, batch: RecordBatch, isFromClient: Boolean, toMagic: Byte): Unit = {
    // batch需要在相同的消息版本
    if (firstBatch.magic() != batch.magic())
      throw new InvalidRecordException(s"Batch magic ${batch.magic()} is not the same as the first batch'es magic byte ${firstBatch.magic()}")
    // 如果是来自客户端
    if (isFromClient) {
      // 如果消息版本是V2以上的版本
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
        // 校验即将写入的增量偏移量是否处于正常范围内
        val countFromOffsets = batch.lastOffset - batch.baseOffset + 1
        if (countFromOffsets <= 0)
          throw new InvalidRecordException(s"Batch has an invalid offset range: [${batch.baseOffset}, ${batch.lastOffset}]")

        // V2以上的消息版本必须有一个非空的数量
        val count = batch.countOrNull
        if (count <= 0)
          throw new InvalidRecordException(s"Invalid reported count for record batch: $count")

        // 如果两个数量不相等，也会抛出非一致性异常
        if (countFromOffsets != batch.countOrNull)
          throw new InvalidRecordException(s"Inconsistent batch offset range [${batch.baseOffset}, ${batch.lastOffset}] " +
            s"and count of records $count")
      }
      // 以上是V2版本消息的专门校验
      // 存在producerId的情况下，必须有正整数的baseSequence
      if (batch.hasProducerId && batch.baseSequence < 0)
        throw new InvalidRecordException(s"Invalid sequence number ${batch.baseSequence} in record batch " +
          s"with producerId ${batch.producerId}")

      // 如果是控制record，则客户端不允许写入control batch
      if (batch.isControlBatch)
        throw new InvalidRecordException("Clients are not allowed to write control records")
    }
    // 全局校验
    // 非客户端请求仅会进行如下校验
    // 事务batch仅支持V2以上的版本
    if (batch.isTransactional && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Transactional records cannot be used with magic version $toMagic")
    // 独立的batch仅支持V2以上的版本
    if (batch.hasProducerId && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Idempotent records cannot be used with magic version $toMagic")
  }

  /**
   * 检验record
   * @param batch
   * @param record
   * @param now
   * @param timestampType
   * @param timestampDiffMaxMs
   * @param compactedTopic
   */
  private def validateRecord(batch: RecordBatch, record: Record, now: Long, timestampType: TimestampType,
                             timestampDiffMaxMs: Long, compactedTopic: Boolean): Unit = {
    // record的版本号和batch的版本号不匹配
    if (!record.hasMagic(batch.magic))
      throw new InvalidRecordException(s"Log record $record's magic does not match outer magic ${batch.magic}")

    // 仅在VO、V1版本的压缩消息集的深层条目的情况下，才会验证record级别的CRC
    // 对于没有压缩的消息，没有内部的V0、V1版本的record，所以需要依赖batch级别的在Log.analyzeAndValidateRecords()中的CRC检查
    // 对于V2及以上的版本，不需要检查record级别的CRC
    if (batch.magic <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed)
      record.ensureValid()

    validateKey(record, compactedTopic)
    // 校验时间戳
    validateTimestamp(batch, record, now, timestampType, timestampDiffMaxMs)
  }

  private def assignOffsetsNonCompressed(records: MemoryRecords,
                                         offsetCounter: LongRef,
                                         now: Long,
                                         compactedTopic: Boolean,
                                         timestampType: TimestampType,
                                         timestampDiffMaxMs: Long,
                                         partitionLeaderEpoch: Int,
                                         isFromClient: Boolean,
                                         magic: Byte): ValidationAndOffsetAssignResult = {
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    val initialOffset = offsetCounter.value

    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, NoCompressionCodec)

    for (batch <- records.batches.asScala) {
      validateBatch(firstBatch, batch, isFromClient, magic)

      var maxBatchTimestamp = RecordBatch.NO_TIMESTAMP
      var offsetOfMaxBatchTimestamp = -1L

      for (record <- batch.asScala) {
        validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
        val offset = offsetCounter.getAndIncrement()
        if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && record.timestamp > maxBatchTimestamp) {
          maxBatchTimestamp = record.timestamp
          offsetOfMaxBatchTimestamp = offset
        }
      }

      if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
        maxTimestamp = maxBatchTimestamp
        offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp
      }

      batch.setLastOffset(offsetCounter.value - 1)

      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

      if (batch.magic > RecordBatch.MAGIC_VALUE_V0) {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now)
        else
          batch.setMaxTimestamp(timestampType, maxBatchTimestamp)
      }
    }

    if (timestampType == TimestampType.LOG_APPEND_TIME) {
      maxTimestamp = now
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        offsetOfMaxTimestamp = offsetCounter.value - 1
      else
        offsetOfMaxTimestamp = initialOffset
    }

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = maxTimestamp,
      shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp,
      messageSizeMaybeChanged = false,
      recordConversionStats = RecordConversionStats.EMPTY)
  }

  /**
   * We cannot do in place assignment in one of the following situations:
   * 1. Source and target compression codec are different
   * 2. When the target magic is not equal to batches' magic, meaning format conversion is needed.
   * 3. When the target magic is equal to V0, meaning absolute offsets need to be re-assigned.
   */
  def validateMessagesAndAssignOffsetsCompressed(records: MemoryRecords,
                                                 offsetCounter: LongRef,
                                                 time: Time,
                                                 now: Long,
                                                 sourceCodec: CompressionCodec,
                                                 targetCodec: CompressionCodec,
                                                 compactedTopic: Boolean,
                                                 toMagic: Byte,
                                                 timestampType: TimestampType,
                                                 timestampDiffMaxMs: Long,
                                                 partitionLeaderEpoch: Int,
                                                 isFromClient: Boolean,
                                                 interBrokerProtocolVersion: ApiVersion): ValidationAndOffsetAssignResult = {

    if (targetCodec == ZStdCompressionCodec && interBrokerProtocolVersion < KAFKA_2_1_IV0)
      throw new UnsupportedCompressionTypeException("Produce requests to inter.broker.protocol.version < 2.1 broker " +
        "are not allowed to use ZStandard compression")

    // No in place assignment situation 1
    var inPlaceAssignment = sourceCodec == targetCodec

    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    val expectedInnerOffset = new LongRef(0)
    val validatedRecords = new mutable.ArrayBuffer[Record]

    var uncompressedSizeInBytes = 0

    // Assume there's only one batch with compressed memory records; otherwise, return InvalidRecordException
    // One exception though is that with format smaller than v2, if sourceCodec is noCompression, then each batch is actually
    // a single record so we'd need to special handle it by creating a single wrapper batch that includes all the records
    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, sourceCodec)

    // No in place assignment situation 2 and 3: we only need to check for the first batch because:
    //  1. For most cases (compressed records, v2, for example), there's only one batch anyways.
    //  2. For cases that there may be multiple batches, all batches' magic should be the same.
    if (firstBatch.magic != toMagic || toMagic == RecordBatch.MAGIC_VALUE_V0)
      inPlaceAssignment = false

    // Do not compress control records unless they are written compressed
    if (sourceCodec == NoCompressionCodec && firstBatch.isControlBatch)
      inPlaceAssignment = true

    val batches = records.batches.asScala
    for (batch <- batches) {
      validateBatch(firstBatch, batch, isFromClient, toMagic)
      uncompressedSizeInBytes += AbstractRecords.recordBatchHeaderSizeInBytes(toMagic, batch.compressionType())

      // if we are on version 2 and beyond, and we know we are going for in place assignment,
      // then we can optimize the iterator to skip key / value / headers since they would not be used at all
      val recordsIterator = if (inPlaceAssignment && firstBatch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.skipKeyValueIterator(BufferSupplier.NO_CACHING)
      else
        batch.streamingIterator(BufferSupplier.NO_CACHING)

      try {
        for (record <- batch.asScala) {
          if (sourceCodec != NoCompressionCodec && record.isCompressed)
            throw new InvalidRecordException("Compressed outer record should not have an inner record with a " +
              s"compression attribute set: $record")
          validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)

          uncompressedSizeInBytes += record.sizeInBytes()
          if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && toMagic > RecordBatch.MAGIC_VALUE_V0) {
            // inner records offset should always be continuous
            val expectedOffset = expectedInnerOffset.getAndIncrement()
            if (record.offset != expectedOffset)
              throw new InvalidRecordException(s"Inner record $record inside the compressed record batch does not have incremental offsets, expected offset is $expectedOffset")
            if (record.timestamp > maxTimestamp)
              maxTimestamp = record.timestamp
          }

          validatedRecords += record
        }
      } finally {
        recordsIterator.close()
      }
    }

    if (!inPlaceAssignment) {
      val (producerId, producerEpoch, sequence, isTransactional) = {
        // note that we only reassign offsets for requests coming straight from a producer. For records with magic V2,
        // there should be exactly one RecordBatch per request, so the following is all we need to do. For Records
        // with older magic versions, there will never be a producer id, etc.
        val first = records.batches.asScala.head
        (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
      }
      buildRecordsAndAssignOffsets(toMagic, offsetCounter, time, timestampType, CompressionType.forId(targetCodec.codec), now,
        validatedRecords, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch, isFromClient,
        uncompressedSizeInBytes)
    } else {
      // we can update the batch only and write the compressed payload as is;
      // again we assume only one record batch within the compressed set
      val batch = records.batches.iterator.next()
      val lastOffset = offsetCounter.addAndGet(validatedRecords.size) - 1

      batch.setLastOffset(lastOffset)

      if (timestampType == TimestampType.LOG_APPEND_TIME)
        maxTimestamp = now

      if (toMagic >= RecordBatch.MAGIC_VALUE_V1)
        batch.setMaxTimestamp(timestampType, maxTimestamp)

      if (toMagic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

      val recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes, 0, 0)
      ValidationAndOffsetAssignResult(validatedRecords = records,
        maxTimestamp = maxTimestamp,
        shallowOffsetOfMaxTimestamp = lastOffset,
        messageSizeMaybeChanged = false,
        recordConversionStats = recordConversionStats)
    }
  }

  private def buildRecordsAndAssignOffsets(magic: Byte,
                                           offsetCounter: LongRef,
                                           time: Time,
                                           timestampType: TimestampType,
                                           compressionType: CompressionType,
                                           logAppendTime: Long,
                                           validatedRecords: Seq[Record],
                                           producerId: Long,
                                           producerEpoch: Short,
                                           baseSequence: Int,
                                           isTransactional: Boolean,
                                           partitionLeaderEpoch: Int,
                                           isFromClient: Boolean,
                                           uncompressedSizeInBytes: Int): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    val estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.value, compressionType,
      validatedRecords.asJava)
    val buffer = ByteBuffer.allocate(estimatedSize)
    val builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType, offsetCounter.value,
      logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch)

    validatedRecords.foreach { record =>
      builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
    }

    val records = builder.build()

    val info = builder.info

    // This is not strictly correct, it represents the number of records where in-place assignment is not possible
    // instead of the number of records that were converted. It will over-count cases where the source and target are
    // message format V0 or if the inner offsets are not consecutive. This is OK since the impact is the same: we have
    // to rebuild the records (including recompression if enabled).
    val conversionCount = builder.numRecords
    val recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes + builder.uncompressedBytesWritten,
      conversionCount, time.nanoseconds - startNanos)

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordConversionStats = recordConversionStats)
  }

  /**
   * 校验压缩的主题是否包含key
   * @param record         给定的record
   * @param compactedTopic 是否是压缩的主题
   */
  private def validateKey(record: Record, compactedTopic: Boolean): Unit = {
    // 压缩的主题是否包含key
    if (compactedTopic && !record.hasKey)
      throw new InvalidRecordException("Compacted topic cannot accept message without key.")
  }

  /**
   * 校验消息的时间戳
   * 如果消息使用的是创建时间，此方法用于校验是否在一个可用的范围内
   */
  private def validateTimestamp(batch: RecordBatch,
                                record: Record,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long): Unit = {
    if (timestampType == TimestampType.CREATE_TIME
      && record.timestamp != RecordBatch.NO_TIMESTAMP
      && math.abs(record.timestamp - now) > timestampDiffMaxMs)
    // 时间戳类型是创建时间类型，并且存在record时间说的情况下，record的记录已经超过了设定的时间，即抛出异常
      throw new InvalidTimestampException(s"Timestamp ${record.timestamp} of message with offset ${record.offset} is " +
        s"out of range. The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}]")
    if (batch.timestampType == TimestampType.LOG_APPEND_TIME)
    // 如果batch的时间类型是日志追加的时间，即抛出异常，因为Producer不应该设置LOG_APPEND_TIME时间戳类型
      throw new InvalidTimestampException(s"Invalid timestamp type in message $record. Producer should not set " +
        s"timestamp type to LogAppendTime.")
  }

  case class ValidationAndOffsetAssignResult(validatedRecords: MemoryRecords,
                                             maxTimestamp: Long,
                                             shallowOffsetOfMaxTimestamp: Long,
                                             messageSizeMaybeChanged: Boolean,
                                             recordConversionStats: RecordConversionStats)

}
