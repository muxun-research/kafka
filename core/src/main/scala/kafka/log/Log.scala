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

import java.io.{File, IOException}
import java.lang.{Long => JLong}
import java.nio.file.{Files, NoSuchFileException}
import java.text.NumberFormat
import java.util.Map.{Entry => JEntry}
import java.util.Optional
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}
import java.util.regex.Pattern

import com.yammer.metrics.core.Gauge
import kafka.api.{ApiVersion, KAFKA_0_10_0_IV0}
import kafka.common.{LogSegmentOffsetOverflowException, LongRef, OffsetsOutOfOrderException, UnexpectedAppendOffsetException}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.LeaderEpochFileCache
import kafka.utils._
import org.apache.kafka.common.errors._
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.{EpochEndOffset, ListOffsetRequest}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, Set, mutable}

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
    RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false, -1L)

  def unknownLogAppendInfoWithLogStartOffset(logStartOffset: Long): LogAppendInfo =
    LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false, -1L)
}

/**
 * 在追加到log之前，计算每个消息集的存储结构
 * @param firstOffset            消息集中第一条消息的offset
 * @param lastOffset             消息集最后一条消息的offset
 * @param maxTimestamp           消息集的最大时间戳
 * @param offsetOfMaxTimestamp   拥有最大时间戳的消息的offset
 * @param logAppendTime          消息集的log追加时间
 * @param logStartOffset         追加时的起始offset
 * @param recordConversionStats  Statistics collected during record processing, `null` if `assignOffsets` is `false`
 * @param sourceCodec            The source codec used in the message set (send by the producer)
 * @param targetCodec            The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount           The number of shallow messages
 * @param validBytes             合法的字节数量The number of valid bytes
 * @param offsetsMonotonic       Are the offsets in this message set monotonically increasing
 * @param lastOffsetOfFirstBatch The last offset of the first batch
 */
case class LogAppendInfo(var firstOffset: Option[Long],
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         var logStartOffset: Long,
                         var recordConversionStats: RecordConversionStats,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean,
                         lastOffsetOfFirstBatch: Long) {
  /**
   * Get the first offset if it exists, else get the last offset of the first batch
   * For magic versions 2 and newer, this method will return first offset. For magic versions
   * older than 2, we use the last offset of the first batch as an approximation of the first
   * offset to avoid decompressing the data.
   */
  def firstOrLastOffsetOfFirstBatch: Long = firstOffset.getOrElse(lastOffsetOfFirstBatch)

  /**
   * Get the (maximum) number of messages described by LogAppendInfo
   * @return Maximum possible number of messages described by LogAppendInfo
   */
  def numMessages: Long = {
    firstOffset match {
      case Some(firstOffsetVal) if (firstOffsetVal >= 0 && lastOffset >= 0) => (lastOffset - firstOffsetVal + 1)
      case _ => 0
    }
  }
}

/**
 * Container class which represents a snapshot of the significant offsets for a partition. This allows fetching
 * of these offsets atomically without the possibility of a leader change affecting their consistency relative
 * to each other. See [[kafka.cluster.Partition.fetchOffsetSnapshot()]].
 */
case class LogOffsetSnapshot(logStartOffset: Long,
                             logEndOffset: LogOffsetMetadata,
                             highWatermark: LogOffsetMetadata,
                             lastStableOffset: LogOffsetMetadata)

/**
 * Another container which is used for lower level reads using  [[kafka.cluster.Partition.readRecords()]].
 */
case class LogReadInfo(fetchedData: FetchDataInfo,
                       highWatermark: Long,
                       logStartOffset: Long,
                       logEndOffset: Long,
                       lastStableOffset: Long)

/**
 * A class used to hold useful metadata about a completed transaction. This is used to build
 * the transaction index after appending to the log.
 * @param producerId  The ID of the producer
 * @param firstOffset The first offset (inclusive) of the transaction
 * @param lastOffset  The last offset (inclusive) of the transaction. This is always the offset of the
 *                    COMMIT/ABORT control record which indicates the transaction's completion.
 * @param isAborted   Whether or not the transaction was aborted
 */
case class CompletedTxn(producerId: Long, firstOffset: Long, lastOffset: Long, isAborted: Boolean) {
  override def toString: String = {
    "CompletedTxn(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"isAborted=$isAborted)"
  }
}

/**
 * A class used to hold params required to decide to rotate a log segment or not.
 */
case class RollParams(maxSegmentMs: Long,
                      maxSegmentBytes: Int,
                      maxTimestampInMessages: Long,
                      maxOffsetInMessages: Long,
                      messagesSize: Int,
                      now: Long)

object RollParams {
  def apply(config: LogConfig, appendInfo: LogAppendInfo, messagesSize: Int, now: Long): RollParams = {
    new RollParams(config.maxSegmentMs,
      config.segmentSize,
      appendInfo.maxTimestamp,
      appendInfo.lastOffset,
      messagesSize, now)
  }
}

/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 * @param dir                                 The directory in which log segments are created.
 * @param config                              The log configuration settings
 * @param logStartOffset                      The earliest offset allowed to be exposed to kafka client.
 *                                            The logStartOffset can be updated by :
 *                       - user's DeleteRecordsRequest
 *                       - broker's log retention
 *                       - broker's log truncation
 *                                            The logStartOffset is used to decide the following:
 *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
 *                                            It may trigger log rolling if the active segment is deleted.
 *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
 *                                            we make sure that logStartOffset <= log's highWatermark
 *                                            Other activities such as log cleaning are not affected by logStartOffset.
 * @param recoveryPoint                       The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler                           The thread pool scheduler used for background actions
 * @param brokerTopicStats                    Container for Broker Topic Yammer Metrics
 * @param time                                The time instance used for checking the clock
 * @param maxProducerIdExpirationMs           The maximum amount of time to wait before a producer id is considered expired
 * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
 */
@threadsafe
class Log(@volatile var dir: File,
          @volatile var config: LogConfig,
          @volatile var logStartOffset: Long,
          @volatile var recoveryPoint: Long,
          scheduler: Scheduler,
          brokerTopicStats: BrokerTopicStats,
          val time: Time,
          val maxProducerIdExpirationMs: Int,
          val producerIdExpirationCheckIntervalMs: Int,
          val topicPartition: TopicPartition,
          val producerStateManager: ProducerStateManager,
          logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  this.logIdent = s"[Log partition=$topicPartition, dir=${dir.getParent}] "

  /* A lock that guards all modifications to the log */
  private val lock = new Object
  /**
   * 索引文件的内存映射buffer，可以通过delete()或者closeHandlers()
   * 在内存映射buffer关闭之后，此Log无法进行任何磁盘IO操作
   */
  @volatile private var isMemoryMappedBufferClosed = false

  /**
   * 上一次进行磁盘刷新的时间
   */
  private val lastFlushedTime = new AtomicLong(time.milliseconds)

  def initFileSize: Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  def updateConfig(updatedKeys: Set[String], newConfig: LogConfig): Unit = {
    val oldConfig = this.config
    this.config = newConfig
    if (updatedKeys.contains(LogConfig.MessageFormatVersionProp)) {
      val oldRecordVersion = oldConfig.messageFormatVersion.recordVersion
      val newRecordVersion = newConfig.messageFormatVersion.recordVersion
      if (newRecordVersion.precedes(oldRecordVersion))
        warn(s"Record format version has been downgraded from $oldRecordVersion to $newRecordVersion.")
      initializeLeaderEpochCache()
    }
  }

  private def checkIfMemoryMappedBufferClosed(): Unit = {
    if (isMemoryMappedBufferClosed)
      throw new KafkaStorageException(s"The memory mapped buffer for log of $topicPartition is already closed")
  }

  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* The earliest offset which is part of an incomplete transaction. This is used to compute the
   * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
   * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
   * will point to the log start offset, which may actually be either part of a completed transaction or not
   * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
   * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
   * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
   * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
   * that this could result in disagreement between replicas depending on when they began replicating the log.
   * In the worst case, the LSO could be seen by a consumer to go backwards.
   *
   */
  @volatile private var firstUnstableOffsetMetadata: Option[LogOffsetMetadata] = None

  /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
   * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
   * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
   * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
   */
  @volatile private var highWatermarkMetadata: LogOffsetMetadata = LogOffsetMetadata(logStartOffset)

  /* the actual segments of the log */
  /**
   * 日志管理了分区的所有日志分段
   * key：日志分段的基准偏移量
   */
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  // Visible for testing
  @volatile var leaderEpochCache: Option[LeaderEpochFileCache] = None

  locally {
    val startMs = time.milliseconds

    // create the log directory if it doesn't exist
    Files.createDirectories(dir.toPath)

    initializeLeaderEpochCache()

    val nextOffset = loadSegments()

    /* Calculate the offset of the next message */
    nextOffsetMetadata = LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size)

    leaderEpochCache.foreach(_.truncateFromEnd(nextOffsetMetadata.messageOffset))

    logStartOffset = math.max(logStartOffset, segments.firstEntry.getValue.baseOffset)

    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    leaderEpochCache.foreach(_.truncateFromStart(logStartOffset))

    // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
    // from scratch.
    if (!producerStateManager.isEmpty)
      throw new IllegalStateException("Producer state must be empty during log initialization")
    loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile)

    info(s"Completed load of log with ${segments.size} segments, log start offset $logStartOffset and " +
      s"log end offset $logEndOffset in ${time.milliseconds() - startMs} ms")
  }

  def highWatermark: Long = highWatermarkMetadata.messageOffset

  /**
   * Update the high watermark to a new offset. The new high watermark will be lower
   * bounded by the log start offset and upper bounded by the log end offset.
   *
   * This is intended to be called when initializing the high watermark or when updating
   * it on a follower after receiving a Fetch response from the leader.
   * @param hw the suggested new value for the high watermark
   * @return the updated high watermark offset
   */
  def updateHighWatermark(hw: Long): Long = {
    val newHighWatermark = if (hw < logStartOffset)
      logStartOffset
    else if (hw > logEndOffset)
      logEndOffset
    else
      hw
    updateHighWatermarkMetadata(LogOffsetMetadata(newHighWatermark))
    newHighWatermark
  }

  /**
   * Update the high watermark to a new value if and only if it is larger than the old value. It is
   * an error to update to a value which is larger than the log end offset.
   *
   * This method is intended to be used by the leader to update the high watermark after follower
   * fetch offsets have been updated.
   * @return the old high watermark, if updated by the new value
   */
  def maybeIncrementHighWatermark(newHighWatermark: LogOffsetMetadata): Option[LogOffsetMetadata] = {
    if (newHighWatermark.messageOffset > logEndOffset)
      throw new IllegalArgumentException(s"High watermark $newHighWatermark update exceeds current " +
        s"log end offset $logEndOffsetMetadata")

    val oldHighWatermark = fetchHighWatermarkMetadata

    // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
    // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||
      (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
      updateHighWatermarkMetadata(newHighWatermark)
      Some(oldHighWatermark)
    } else {
      None
    }
  }

  /**
   * Get the offset and metadata for the current high watermark. If offset metadata is not
   * known, this will do a lookup in the index and cache the result.
   */
  private def fetchHighWatermarkMetadata: LogOffsetMetadata = {
    checkIfMemoryMappedBufferClosed()

    val offsetMetadata = highWatermarkMetadata
    if (offsetMetadata.messageOffsetOnly) {
      lock.synchronized {
        val fullOffset = convertToOffsetMetadataOrThrow(highWatermark)
        updateHighWatermarkMetadata(fullOffset)
        fullOffset
      }
    } else {
      offsetMetadata
    }
  }

  /**
   * 更新highWatermark metadata
   * @param newHighWatermark 新的highWatermark
   */
  private def updateHighWatermarkMetadata(newHighWatermark: LogOffsetMetadata): Unit = {
    // 检查非法的message offset
    if (newHighWatermark.messageOffset < 0)
      throw new IllegalArgumentException("High watermark offset should be non-negative")

    lock synchronized {
      // 更新highWatermark
      highWatermarkMetadata = newHighWatermark
      // 更新producer state的highWatermark
      producerStateManager.onHighWatermarkUpdated(newHighWatermark.messageOffset)
      // 是否需要增量第一个不稳定的offset
      maybeIncrementFirstUnstableOffset()
    }
    trace(s"Setting high watermark $newHighWatermark")
  }

  /**
   * Get the first unstable offset. Unlike the last stable offset, which is always defined,
   * the first unstable offset only exists if there are transactions in progress.
   * @return the first unstable offset, if it exists
   */
  private[log] def firstUnstableOffset: Option[Long] = firstUnstableOffsetMetadata.map(_.messageOffset)

  private def fetchLastStableOffsetMetadata: LogOffsetMetadata = {
    checkIfMemoryMappedBufferClosed()

    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermark =>
        if (offsetMetadata.messageOffsetOnly) {
          lock synchronized {
            val fullOffset = convertToOffsetMetadataOrThrow(offsetMetadata.messageOffset)
            if (firstUnstableOffsetMetadata.contains(offsetMetadata))
              firstUnstableOffsetMetadata = Some(fullOffset)
            fullOffset
          }
        } else {
          offsetMetadata
        }
      case _ => fetchHighWatermarkMetadata
    }
  }

  /**
   * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
   * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
   * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
   * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
   * beyond the high watermark.
   */
  def lastStableOffset: Long = {
    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermark => offsetMetadata.messageOffset
      case _ => highWatermark
    }
  }

  def lastStableOffsetLag: Long = highWatermark - lastStableOffset

  /**
   * Fully materialize and return an offset snapshot including segment position info. This method will update
   * the LogOffsetMetadata for the high watermark and last stable offset if they are message-only. Throws an
   * offset out of range error if the segment info cannot be loaded.
   */
  def fetchOffsetSnapshot: LogOffsetSnapshot = {
    val lastStable = fetchLastStableOffsetMetadata
    val highWatermark = fetchHighWatermarkMetadata

    LogOffsetSnapshot(
      logStartOffset,
      logEndOffsetMetadata,
      highWatermark,
      lastStable
    )
  }

  private val tags = {
    val maybeFutureTag = if (isFuture) Map("is-future" -> "true") else Map.empty[String, String]
    Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++ maybeFutureTag
  }

  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  val producerExpireCheck = scheduler.schedule(name = "PeriodicProducerExpirationCheck", fun = () => {
    lock synchronized {
      producerStateManager.removeExpiredProducers(time.milliseconds)
    }
  }, period = producerIdExpirationCheckIntervalMs, delay = producerIdExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)

  /** The name of this log */
  def name = dir.getName()

  def recordVersion: RecordVersion = config.messageFormatVersion.recordVersion

  private def initializeLeaderEpochCache(): Unit = lock synchronized {
    val leaderEpochFile = LeaderEpochCheckpointFile.newFile(dir)

    def newLeaderEpochFileCache(): LeaderEpochFileCache = {
      val checkpointFile = new LeaderEpochCheckpointFile(leaderEpochFile, logDirFailureChannel)
      new LeaderEpochFileCache(topicPartition, logEndOffset _, checkpointFile)
    }

    if (recordVersion.precedes(RecordVersion.V2)) {
      val currentCache = if (leaderEpochFile.exists())
        Some(newLeaderEpochFileCache())
      else
        None

      if (currentCache.exists(_.nonEmpty))
        warn(s"Deleting non-empty leader epoch cache due to incompatible message format $recordVersion")

      Files.deleteIfExists(leaderEpochFile.toPath)
      leaderEpochCache = None
    } else {
      leaderEpochCache = Some(newLeaderEpochFileCache())
    }
  }

  /**
   * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
   * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
   * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
   * by this method.
   * @return Set of .swap files that are valid to be swapped in as segment files
   */
  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {

    def deleteIndicesIfExist(baseFile: File, suffix: String = ""): Unit = {
      info(s"Deleting index files with suffix $suffix for baseFile $baseFile")
      val offset = offsetFromFile(baseFile)
      Files.deleteIfExists(Log.offsetIndexFile(dir, offset, suffix).toPath)
      Files.deleteIfExists(Log.timeIndexFile(dir, offset, suffix).toPath)
      Files.deleteIfExists(Log.transactionIndexFile(dir, offset, suffix).toPath)
    }

    var swapFiles = Set[File]()
    var cleanFiles = Set[File]()
    var minCleanedFileOffset = Long.MaxValue

    for (file <- dir.listFiles if file.isFile) {
      if (!file.canRead)
        throw new IOException(s"Could not read file $file")
      val filename = file.getName
      if (filename.endsWith(DeletedFileSuffix)) {
        debug(s"Deleting stray temporary file ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath)
      } else if (filename.endsWith(CleanedFileSuffix)) {
        minCleanedFileOffset = Math.min(offsetFromFileName(filename), minCleanedFileOffset)
        cleanFiles += file
      } else if (filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the index files, complete the swap operation later
        // if an index just delete the index files, they will be rebuilt
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        info(s"Found file ${file.getAbsolutePath} from interrupted swap operation.")
        if (isIndexFile(baseFile)) {
          deleteIndicesIfExist(baseFile)
        } else if (isLogFile(baseFile)) {
          deleteIndicesIfExist(baseFile)
          swapFiles += file
        }
      }
    }

    // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
    // files could be part of an incomplete split operation that could not complete. See Log#splitOverflowedSegment
    // for more details about the split operation.
    val (invalidSwapFiles, validSwapFiles) = swapFiles.partition(file => offsetFromFile(file) >= minCleanedFileOffset)
    invalidSwapFiles.foreach { file =>
      debug(s"Deleting invalid swap file ${file.getAbsoluteFile} minCleanedFileOffset: $minCleanedFileOffset")
      val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
      deleteIndicesIfExist(baseFile, SwapFileSuffix)
      Files.deleteIfExists(file.toPath)
    }

    // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
    cleanFiles.foreach { file =>
      debug(s"Deleting stray .clean file ${file.getAbsolutePath}")
      Files.deleteIfExists(file.toPath)
    }

    validSwapFiles
  }

  /**
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded
   * It is possible that we encounter a segment with index offset overflow in which case the LogSegmentOffsetOverflowException
   * will be thrown. Note that any segments that were opened before we encountered the exception will remain open and the
   * caller is responsible for closing them appropriately, if needed.
   * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
   */
  private def loadSegmentFiles(): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      if (isIndexFile(file)) {
        // if it is an index file, make sure it has a corresponding .log file
        val offset = offsetFromFile(file)
        val logFile = Log.logFile(dir, offset)
        if (!logFile.exists) {
          warn(s"Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // if it's a log file, load the corresponding log segment
        val baseOffset = offsetFromFile(file)
        val timeIndexFileNewlyCreated = !Log.timeIndexFile(dir, baseOffset).exists()
        val segment = LogSegment.open(dir = dir,
          baseOffset = baseOffset,
          config,
          time = time,
          fileAlreadyExists = true)

        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            error(s"Could not find offset index file corresponding to log file ${segment.log.file.getAbsolutePath}, " +
              "recovering segment and rebuilding index files...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"Found a corrupted index file corresponding to log file ${segment.log.file.getAbsolutePath} due " +
              s"to ${e.getMessage}}, recovering segment and rebuilding index files...")
            recoverSegment(segment)
        }
        addSegment(segment)
      }
    }
  }

  /**
   * Recover the given segment.
   * @param segment          Segment to recover
   * @param leaderEpochCache Optional cache for updating the leader epoch during recovery
   * @return The number of bytes truncated from the segment
   * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
   */
  private def recoverSegment(segment: LogSegment,
                             leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = lock synchronized {
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    rebuildProducerState(segment.baseOffset, reloadFromCleanShutdown = false, producerStateManager)
    val bytesTruncated = segment.recover(producerStateManager, leaderEpochCache)
    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    producerStateManager.takeSnapshot()
    bytesTruncated
  }

  /**
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs
   * are loaded.
   * @throws LogSegmentOffsetOverflowException if the swap file contains messages that cause the log segment offset to
   *                                           overflow. Note that this is currently a fatal exception as we do not have
   *                                           a way to deal with it. The exception is propagated all the way up to
   *                                           KafkaServer#startup which will cause the broker to shut down if we are in
   *                                           this situation. This is expected to be an extremely rare scenario in practice,
   *                                           and manual intervention might be required to get out of it.
   */
  private def completeSwapOperations(swapFiles: Set[File]): Unit = {
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val baseOffset = offsetFromFile(logFile)
      val swapSegment = LogSegment.open(swapFile.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = SwapFileSuffix)
      info(s"Found log file ${swapFile.getPath} from interrupted swap operation, repairing.")
      recoverSegment(swapSegment)

      // We create swap files for two cases:
      // (1) Log cleaning where multiple segments are merged into one, and
      // (2) Log splitting where one segment is split into multiple.
      //
      // Both of these mean that the resultant swap segments be composed of the original set, i.e. the swap segment
      // must fall within the range of existing segment(s). If we cannot find such a segment, it means the deletion
      // of that segment was successful. In such an event, we should simply rename the .swap to .log without having to
      // do a replace with an existing segment.
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.readNextOffset).filter { segment =>
        segment.readNextOffset > swapSegment.baseOffset
      }
      replaceSegments(Seq(swapSegment), oldSegments.toSeq, isRecoveredSwapFile = true)
    }
  }

  /**
   * Load the log segments from the log files on disk and return the next offset.
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs
   * are loaded.
   * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that overflow index offset; or when
   *                                           we find an unexpected number of .log files with overflow
   */
  private def loadSegments(): Long = {
    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // Now do a second pass and load all the log and index files.
    // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
    // this happens, restart loading segment files from scratch.
    retryOnOffsetOverflow {
      // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
      // loading of segments. In that case, we also need to close all segments that could have been left open in previous
      // call to loadSegmentFiles().
      logSegments.foreach(_.close())
      segments.clear()
      loadSegmentFiles()
    }

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    completeSwapOperations(swapFiles)

    if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
      val nextOffset = retryOnOffsetOverflow {
        recoverLog()
      }

      // reset the index size of the currently active log segment to allow more entries
      activeSegment.resizeIndexes(config.maxIndexSize)
      nextOffset
    } else {
      if (logSegments.isEmpty) {
        addSegment(LogSegment.open(dir = dir,
          baseOffset = 0,
          config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = this.initFileSize,
          preallocate = false))
      }
      0
    }
  }

  /**
   * 更新logEndOffset
   * @param messageOffset 下一条消息的offset
   */
  private def updateLogEndOffset(messageOffset: Long): Unit = {
    // 构建下一条消息的新的offset metadata
    nextOffsetMetadata = LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size)

    // 阶段后更新highWatermark，以防它超出logEndOffset
    if (highWatermark > messageOffset) {
      updateHighWatermarkMetadata(nextOffsetMetadata)
    }
  }

  /**
   * Recover the log segments and return the next offset after recovery.
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all
   * logs are loaded.
   * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
   */
  private def recoverLog(): Long = {
    // if we have the clean shutdown marker, skip recovery
    if (!hasCleanShutdownFile) {
      // okay we need to actually recover this log
      val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).toIterator
      var truncated = false

      while (unflushed.hasNext && !truncated) {
        val segment = unflushed.next
        info(s"Recovering unflushed segment ${segment.baseOffset}")
        val truncatedBytes =
          try {
            recoverSegment(segment, leaderEpochCache)
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn("Found invalid offset during recovery. Deleting the corrupt segment and " +
                s"creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          warn(s"Corruption found in segment ${segment.baseOffset}, truncating to offset ${segment.readNextOffset}")
          removeAndDeleteSegments(unflushed.toList, asyncDelete = true)
          truncated = true
        }
      }
    }

    if (logSegments.nonEmpty) {
      val logEndOffset = activeSegment.readNextOffset
      if (logEndOffset < logStartOffset) {
        warn(s"Deleting all segments because logEndOffset ($logEndOffset) is smaller than logStartOffset ($logStartOffset). " +
          "This could happen if segment files were deleted from the file system.")
        removeAndDeleteSegments(logSegments, asyncDelete = true)
      }
    }

    if (logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at logStartOffset
      addSegment(LogSegment.open(dir = dir,
        baseOffset = logStartOffset,
        config,
        time = time,
        fileAlreadyExists = false,
        initFileSize = this.initFileSize,
        preallocate = config.preallocate))
    }

    recoveryPoint = activeSegment.readNextOffset
    recoveryPoint
  }

  // Rebuild producer state until lastOffset. This method may be called from the recovery code path, and thus must be
  // free of all side-effects, i.e. it must not update any log-specific state.
  private def rebuildProducerState(lastOffset: Long,
                                   reloadFromCleanShutdown: Boolean,
                                   producerStateManager: ProducerStateManager): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    val messageFormatVersion = config.messageFormatVersion.recordVersion.value
    val segments = logSegments
    val offsetsToSnapshot =
      if (segments.nonEmpty) {
        val nextLatestSegmentBaseOffset = lowerSegment(segments.last.baseOffset).map(_.baseOffset)
        Seq(nextLatestSegmentBaseOffset, Some(segments.last.baseOffset), Some(lastOffset))
      } else {
        Seq(Some(lastOffset))
      }
    info(s"Loading producer state till offset $lastOffset with message format version $messageFormatVersion")

    // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
    // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
    // but we have to be careful not to assume too much in the presence of broker failures. The two most common
    // upgrade cases in which we expect to find no snapshots are the following:
    //
    // 1. The broker has been upgraded, but the topic is still on the old message format.
    // 2. The broker has been upgraded, the topic is on the new message format, and we had a clean shutdown.
    //
    // If we hit either of these cases, we skip producer state loading and write a new snapshot at the log end
    // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
    // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
    // from the first segment.
    if (messageFormatVersion < RecordBatch.MAGIC_VALUE_V2 ||
      (producerStateManager.latestSnapshotOffset.isEmpty && reloadFromCleanShutdown)) {
      // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
      // last two segments and the last offset. This should avoid the full scan in the case that the log needs
      // truncation.
      offsetsToSnapshot.flatten.foreach { offset =>
        producerStateManager.updateMapEndOffset(offset)
        producerStateManager.takeSnapshot()
      }
    } else {
      val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
      producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())

      // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
      // offset (which would be the case on first startup) and there were active producers prior to truncation
      // (which could be the case if truncating after initial loading). If there weren't, then truncating
      // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
      // and we can skip the loading. This is an optimization for users which are not yet using
      // idempotent/transactional features yet.
      if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
        val segmentOfLastOffset = floorLogSegment(lastOffset)

        logSegments(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
          val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
          producerStateManager.updateMapEndOffset(startOffset)

          if (offsetsToSnapshot.contains(Some(segment.baseOffset)))
            producerStateManager.takeSnapshot()

          val maxPosition = if (segmentOfLastOffset.contains(segment)) {
            Option(segment.translateOffset(lastOffset))
              .map(_.position)
              .getOrElse(segment.size)
          } else {
            segment.size
          }

          val fetchDataInfo = segment.read(startOffset,
            maxSize = Int.MaxValue,
            maxPosition = maxPosition,
            minOneMessage = false)
          if (fetchDataInfo != null)
            loadProducersFromLog(producerStateManager, fetchDataInfo.records)
        }
      }
      producerStateManager.updateMapEndOffset(lastOffset)
      producerStateManager.takeSnapshot()
    }
  }

  private def loadProducerState(lastOffset: Long, reloadFromCleanShutdown: Boolean): Unit = lock synchronized {
    rebuildProducerState(lastOffset, reloadFromCleanShutdown, producerStateManager)
    maybeIncrementFirstUnstableOffset()
  }

  private def loadProducersFromLog(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.asScala.foreach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(batch,
          loadedProducers,
          firstOffsetMetadata = None,
          isFromClient = false)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private[log] def activeProducersWithLastSequence: Map[Long, Int] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      (producerId, producerIdEntry.lastSeq)
    }
  }

  private[log] def lastRecordsOfActiveProducers: Map[Long, LastRecord] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      val lastDataOffset = if (producerIdEntry.lastDataOffset >= 0) Some(producerIdEntry.lastDataOffset) else None
      val lastRecord = LastRecord(lastDataOffset, producerIdEntry.producerEpoch)
      producerId -> lastRecord
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile: Boolean = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log.
   * The memory mapped buffer for index files of this log will be left open until the log is deleted.
   */
  def close(): Unit = {
    debug("Closing log")
    lock synchronized {
      checkIfMemoryMappedBufferClosed()
      producerExpireCheck.cancel(true)
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
        // (the clean shutdown file is written after the logs are all closed).
        producerStateManager.takeSnapshot()
        logSegments.foreach(_.close())
      }
    }
  }

  /**
   * Rename the directory of the log
   * @throws KafkaStorageException if rename fails
   */
  def renameDir(name: String): Unit = {
    lock synchronized {
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
        val renamedDir = new File(dir.getParent, name)
        Utils.atomicMoveWithFallback(dir.toPath, renamedDir.toPath)
        if (renamedDir != dir) {
          dir = renamedDir
          logSegments.foreach(_.updateDir(renamedDir))
          producerStateManager.logDir = dir
          // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
          // the checkpoint file in renamed log directory
          initializeLeaderEpochCache()
        }
      }
    }
  }

  /**
   * Close file handlers used by log but don't write to disk. This is called if the log directory is offline
   */
  def closeHandlers(): Unit = {
    debug("Closing handlers")
    lock synchronized {
      logSegments.foreach(_.closeHandlers())
      isMemoryMappedBufferClosed = true
    }
  }

  /**
   * 追加消息集到活跃的log段中，分配offset和partition leader epoch
   * @param records                    需要追加的records
   * @param isFromClient               追加是否来自producer
   * @param interBrokerProtocolVersion 内部broker消息协议版本
   * @throws KafkaStorageException IO异常
   * @return 追加的信息，包括第一个和最后一个的offset
   */
  def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, isFromClient: Boolean = true,
                     interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion): LogAppendInfo = {
    append(records, isFromClient, interBrokerProtocolVersion, assignOffsets = true, leaderEpoch)
  }

  /**
   * Append this message set to the active segment of the log without assigning offsets or Partition Leader Epochs
   * @param records The records to append
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records, isFromClient = false, interBrokerProtocolVersion = ApiVersion.latestVersion, assignOffsets = false, leaderEpoch = -1)
  }

  /**
   * 追加消息集到活跃的log段中，必要时创建新的log段
   *
   * 此方法会负责分配消息的offset
   * 然而，如果设置了assignOffsets=false，只会检查存在的offset是否合法
   * @param records                    需要追加的records
   * @param isFromClient               追加动作是否来自于producer
   * @param interBrokerProtocolVersion 内部broker消息协议版本
   * @param assignOffsets              是否使用Log进行offset的分配，还是使用应用程序给出的offset
   * @param leaderEpoch                leader epoch
   * @throws KafkaStorageException           IO异常
   * @throws OffsetsOutOfOrderException      records中offset顺序错乱
   * @throws UnexpectedAppendOffsetException offset范围出现异常
   * @return 追加的信息，包括第一个和最后一个的offset
   */
  private def append(records: MemoryRecords, isFromClient: Boolean, interBrokerProtocolVersion: ApiVersion, assignOffsets: Boolean, leaderEpoch: Int): LogAppendInfo = {
    maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
      // 校验batch信息
      val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)

      // 如果没有合法的消息，或者是上一次追加的重复元素，直接返回校验结果
      if (appendInfo.shallowCount == 0)
        return appendInfo

      // trim any invalid bytes or partial messages before appending it to the on-disk log
      var validRecords = trimInvalidBytes(records, appendInfo)

      // 同步操作，进行追加
      lock synchronized {
        // 校验NIO索引写入文件通道是否关闭
        checkIfMemoryMappedBufferClosed()
        // 需要Kafka的offset分配模式
        if (assignOffsets) {
          // 分配offset给消息集
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          // 追加信息的第一个offset
          appendInfo.firstOffset = Some(offset.value)
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              offset,
              time,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.recordVersion.value,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              isFromClient,
              interBrokerProtocolVersion)
          } catch {
            case e: IOException =>
              throw new KafkaException(s"Error validating messages while appending to log $name", e)
          }
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          appendInfo.recordConversionStats = validateAndOffsetAssignResult.recordConversionStats
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException(s"Message batch size is ${batch.sizeInBytes} bytes in append to" +
                  s"partition $topicPartition which exceeds the maximum configured size of ${config.maxMessageSize}.")
              }
            }
          }
        } else {
          // we are taking the offsets we are given
          if (!appendInfo.offsetsMonotonic)
            throw new OffsetsOutOfOrderException(s"Out of order offsets found in append to $topicPartition: " +
              records.records.asScala.map(_.offset))

          if (appendInfo.firstOrLastOffsetOfFirstBatch < nextOffsetMetadata.messageOffset) {
            // we may still be able to recover if the log is empty
            // one example: fetching from log start offset on the leader which is not batch aligned,
            // which may happen as a result of AdminClient#deleteRecords()
            val firstOffset = appendInfo.firstOffset match {
              case Some(offset) => offset
              case None => records.batches.asScala.head.baseOffset()
            }

            val firstOrLast = if (appendInfo.firstOffset.isDefined) "First offset" else "Last offset of the first batch"
            throw new UnexpectedAppendOffsetException(
              s"Unexpected offset in append to $topicPartition. $firstOrLast " +
                s"${appendInfo.firstOrLastOffsetOfFirstBatch} is less than the next offset ${nextOffsetMetadata.messageOffset}. " +
                s"First 10 offsets in append: ${records.records.asScala.take(10).map(_.offset)}, last offset in" +
                s" append: ${appendInfo.lastOffset}. Log start offset = $logStartOffset",
              firstOffset, appendInfo.lastOffset)
          }
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        validRecords.batches.asScala.foreach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
            maybeAssignEpochStartOffset(batch.partitionLeaderEpoch, batch.baseOffset)
          } else {
            // In partial upgrade scenarios, we may get a temporary regression to the message format. In
            // order to ensure the safety of leader election, we clear the epoch cache so that we revert
            // to truncation by high watermark after the next leader election.
            leaderEpochCache.filter(_.nonEmpty).foreach { cache =>
              warn(s"Clearing leader epoch cache after unexpected append with message format v${batch.magic}")
              cache.clearAndFlush()
            }
          }
        }

        // check messages set size may be exceed config.segmentSize
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +
            s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
        }

        // maybe roll the log if this segment is full
        val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)

        val logOffsetMetadata = LogOffsetMetadata(
          messageOffset = appendInfo.firstOrLastOffsetOfFirstBatch,
          segmentBaseOffset = segment.baseOffset,
          relativePositionInSegment = segment.size)

        // now that we have valid records, offsets assigned, and timestamps updated, we need to
        // validate the idempotent/transactional state of the producers and collect some metadata
        val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(
          logOffsetMetadata, validRecords, isFromClient)

        maybeDuplicate.foreach { duplicate =>
          appendInfo.firstOffset = Some(duplicate.firstOffset)
          appendInfo.lastOffset = duplicate.lastOffset
          appendInfo.logAppendTime = duplicate.timestamp
          appendInfo.logStartOffset = logStartOffset
          return appendInfo
        }

        segment.append(largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // Increment the log end offset. We do this immediately after the append because a
        // write to the transaction index below may fail and we want to ensure that the offsets
        // of future appends still grow monotonically. The resulting transaction index inconsistency
        // will be cleaned up after the log directory is recovered. Note that the end offset of the
        // ProducerStateManager will not be updated and the last stable offset will not advance
        // if the append to the transaction index fails.
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // update the producer state
        for (producerAppendInfo <- updatedProducers.values) {
          producerStateManager.update(producerAppendInfo)
        }

        // update the transaction index with the true last stable offset. The last offset visible
        // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
        for (completedTxn <- completedTxns) {
          val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
          segment.updateTxnIndex(completedTxn, lastStableOffset)
          producerStateManager.completeTxn(completedTxn)
        }

        // always update the last producer id map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // update the first unstable offset (which is used to compute LSO)
        maybeIncrementFirstUnstableOffset()

        trace(s"Appended message set with last offset: ${appendInfo.lastOffset}, " +
          s"first offset: ${appendInfo.firstOffset}, " +
          s"next offset: ${nextOffsetMetadata.messageOffset}, " +
          s"and messages: $validRecords")

        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    }
  }

  def maybeAssignEpochStartOffset(leaderEpoch: Int, startOffset: Long): Unit = {
    leaderEpochCache.foreach { cache =>
      cache.assign(leaderEpoch, startOffset)
    }
  }

  def latestEpoch: Option[Int] = leaderEpochCache.flatMap(_.latestEpoch)

  def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
    leaderEpochCache.flatMap { cache =>
      val (foundEpoch, foundOffset) = cache.endOffsetFor(leaderEpoch)
      if (foundOffset == EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
        None
      else
        Some(OffsetAndEpoch(foundOffset, foundEpoch))
    }
  }

  /**
   * 增量首个不稳定的offset
   */
  private def maybeIncrementFirstUnstableOffset(): Unit = lock synchronized {
    // 检查写入通道是否已关闭
    checkIfMemoryMappedBufferClosed()
    // 获取producerStateManager中的firstUnstableOffset，可能是unreplicatedFirstOffset，也可能是undecidedFirstOffset
    val updatedFirstStableOffset = producerStateManager.firstUnstableOffset match {
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        // 如果offset只是message的offset信息或者messageOffset＜logStartOffset
        // 命中条件，offset将更新为logOffset.messageOffset和logStartOffset二者之间的最大值
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)
        Some(convertToOffsetMetadataOrThrow(offset))
      case other => other
    }

    if (updatedFirstStableOffset != this.firstUnstableOffsetMetadata) {
      debug(s"First unstable offset updated to $updatedFirstStableOffset")
      this.firstUnstableOffsetMetadata = updatedFirstStableOffset
    }
  }

  /**
   * 如果提供的offset比较大，增量移动logStartOffset
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long): Unit = {
    // 如果新的logStartOffset比当前的highWatermark大，抛出超出范围异常
    if (newLogStartOffset > highWatermark)
      throw new OffsetOutOfRangeException(s"Cannot increment the log start offset to $newLogStartOffset of partition $topicPartition " +
        s"since it is larger than the high watermark $highWatermark")

    // 不必立即将logStartOffset写入到log-start-offset-checkpoint中
    // 删除的offset可能在所有ISR都关闭之后丢失
    // 在log.flush.start.offset.checkpoint.interval.ms以不整洁的方式，但是发生这样的几率很小
    maybeHandleIOException(s"Exception while increasing log start offset for $topicPartition to $newLogStartOffset in dir ${dir.getParent}") {
      lock synchronized {
        // 检查文件写入是否已经关闭了
        checkIfMemoryMappedBufferClosed()
        // 如果新的offset比原有的logStartOffset大
        if (newLogStartOffset > logStartOffset) {
          info(s"Incrementing log start offset to $newLogStartOffset")
          // 更新logStartOffset
          logStartOffset = newLogStartOffset
          // 同时也更新leader epoch
          leaderEpochCache.foreach(_.truncateFromStart(logStartOffset))
          // 根据logStartOffset去除日志头
          producerStateManager.truncateHead(logStartOffset)
          // 增量首个不稳定的offset
          maybeIncrementFirstUnstableOffset()
        }
      }
    }
  }

  private def analyzeAndValidateProducerState(appendOffsetMetadata: LogOffsetMetadata,
                                              records: MemoryRecords,
                                              isFromClient: Boolean):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    var relativePositionInSegment = appendOffsetMetadata.relativePositionInSegment

    for (batch <- records.batches.asScala) {
      if (batch.hasProducerId) {
        val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

        // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
        // If we find a duplicate, we return the metadata of the appended batch to the client.
        if (isFromClient) {
          maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
            return (updatedProducers, completedTxns.toList, Some(duplicate))
          }
        }

        // We cache offset metadata for the start of each transaction. This allows us to
        // compute the last stable offset without relying on additional index lookups.
        val firstOffsetMetadata = if (batch.isTransactional)
          Some(LogOffsetMetadata(batch.baseOffset, appendOffsetMetadata.segmentBaseOffset, relativePositionInSegment))
        else
          None

        val maybeCompletedTxn = updateProducers(batch,
          updatedProducers,
          firstOffsetMetadata = firstOffsetMetadata,
          isFromClient = isFromClient)

        maybeCompletedTxn.foreach(completedTxns += _)
      }

      relativePositionInSegment += batch.sizeInBytes
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * 校验一下事情：
   * 每条消息是否匹配CRC
   * 每条消息的大小是否合法
   * 传入的batch序列号和现有状态是否彼此一致
   *
   * 也计算下面的数值：
   * 消息集中的第一个offset
   * 消息集中的最后一个offset
   * 消息的总个数
   * 合法的字节数
   * offset是否是单调递增的
   * 是否使用了压缩策略（如果使用多了个，则以最后一个为准）
   */
  private def analyzeAndValidateRecords(records: MemoryRecords, isFromClient: Boolean): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset: Option[Long] = None
    var lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    var readFirstMessage = false
    var lastOffsetOfFirstBatch = -1L

    for (batch <- records.batches.asScala) {
      // // 我们只会校验V2及更高的版本避免老客户端的兼容性
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && isFromClient && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch in the append to $topicPartition should " +
          s"be 0, but it is ${batch.baseOffset}")

      // 如果在第一条消息索引位置，更新第一个offset，对于那些版本号小于V2的，使用最后一个offset来避免需要解压数据（最后的offset可以从包装的消息中获取）
      // 对于V2版本，可以直接batch的头部直接获取offset
      // 当追加到leader节点时，会使用正确的值来更新LogAppendInfo.baseOffset
      // 对于追加到follower节点时，验证则会更宽松
      // 同时指出我们是否有准确的第一个offset
      if (!readFirstMessage) {
        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
          firstOffset = Some(batch.baseOffset)
        lastOffsetOfFirstBatch = batch.lastOffset
        readFirstMessage = true
      }

      // 判断是否是单调递增的
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // 更新最后的offset
      lastOffset = batch.lastOffset

      // 校验batch大小是否超过设定的每个batch的最大值
      val batchSize = batch.sizeInBytes
      if (batchSize > config.maxMessageSize) {
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size in the append to $topicPartition is $batchSize bytes " +
          s"which exceeds the maximum configured value of ${config.maxMessageSize}.")
      }

      // 通过验证CRC，来验证消息的合法性
      batch.ensureValid()

      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = lastOffset
      }

      shallowMessageCount += 1
      validBytesCount += batchSize
      // 压缩协议
      val messageCodec = CompressionCodec.getCompressionCodec(batch.compressionType.id)
      if (messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic, lastOffsetOfFirstBatch)
  }

  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              firstOffsetMetadata: Option[LogOffsetMetadata],
                              isFromClient: Boolean): Option[CompletedTxn] = {
    val producerId = batch.producerId
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, isFromClient))
    appendInfo.append(batch, firstOffsetMetadata)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   * @param records The records to trim
   * @param info    The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    val validBytes = info.validBytes
    if (validBytes < 0)
      throw new CorruptRecordException(s"Cannot append record batch with illegal length $validBytes to " +
        s"log for $topicPartition. A possible cause is a corrupted produce request.")
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // trim invalid bytes
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  private def emptyFetchDataInfo(fetchOffsetMetadata: LogOffsetMetadata,
                                 includeAbortedTxns: Boolean): FetchDataInfo = {
    val abortedTransactions =
      if (includeAbortedTxns) Some(List.empty[AbortedTransaction])
      else None
    FetchDataInfo(fetchOffsetMetadata,
      MemoryRecords.EMPTY,
      firstEntryIncomplete = false,
      abortedTransactions = abortedTransactions)
  }

  /**
   * Read messages from the log.
   * @param startOffset   The offset to begin reading at
   * @param maxLength     The maximum number of bytes to read
   * @param isolation     The fetch isolation, which controls the maximum offset we are allowed to read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def read(startOffset: Long,
           maxLength: Int,
           isolation: FetchIsolation,
           minOneMessage: Boolean): FetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace(s"Reading $maxLength bytes from offset $startOffset of length $size bytes")

      val includeAbortedTxns = isolation == FetchTxnCommitted

      // Because we don't use the lock for reading, the synchronization is a little bit tricky.
      // We create the local variables to avoid race conditions with updates to the log.
      val endOffsetMetadata = nextOffsetMetadata
      val endOffset = nextOffsetMetadata.messageOffset
      if (startOffset == endOffset)
        return emptyFetchDataInfo(endOffsetMetadata, includeAbortedTxns)

      var segmentEntry = segments.floorEntry(startOffset)

      // return error on attempt to read beyond the log end offset or read below log start offset
      if (startOffset > endOffset || segmentEntry == null || startOffset < logStartOffset)
        throw new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $logStartOffset to $endOffset.")

      val maxOffsetMetadata = isolation match {
        case FetchLogEnd => nextOffsetMetadata
        case FetchHighWatermark => fetchHighWatermarkMetadata
        case FetchTxnCommitted => fetchLastStableOffsetMetadata
      }

      if (startOffset > maxOffsetMetadata.messageOffset) {
        val startOffsetMetadata = convertToOffsetMetadataOrThrow(startOffset)
        return emptyFetchDataInfo(startOffsetMetadata, includeAbortedTxns)
      }

      // Do the read on the segment with a base offset less than the target offset
      // but if that segment doesn't contain any messages with an offset greater than that
      // continue to read from successive segments until we get some messages or we reach the end of the log
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue

        val maxPosition = {
          // Use the max offset position if it is on this segment; otherwise, the segment size is the limit.
          if (maxOffsetMetadata.segmentBaseOffset == segment.baseOffset) {
            maxOffsetMetadata.relativePositionInSegment
          } else {
            segment.size
          }
        }

        val fetchInfo = segment.read(startOffset, maxLength, maxPosition, minOneMessage)
        if (fetchInfo == null) {
          segmentEntry = segments.higherEntry(segmentEntry.getKey)
        } else {
          return if (includeAbortedTxns)
            addAbortedTransactions(startOffset, segmentEntry, fetchInfo)
          else
            fetchInfo
        }
      }

      // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
      // this can happen when all messages with offset larger than start offsets have been deleted.
      // In this case, we will return the empty set with log end offset metadata
      FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
    }
  }

  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val segmentEntry = segments.floorEntry(startOffset)
    val allAbortedTxns = ListBuffer.empty[AbortedTxn]

    def accumulator(abortedTxns: List[AbortedTxn]): Unit = allAbortedTxns ++= abortedTxns

    collectAbortedTransactions(logStartOffset, upperBoundOffset, segmentEntry, accumulator)
    allAbortedTxns.toList
  }

  private def addAbortedTransactions(startOffset: Long, segmentEntry: JEntry[JLong, LogSegment],
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    val upperBoundOffset = segmentEntry.getValue.fetchUpperBoundOffset(startOffsetPosition, fetchSize).getOrElse {
      val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
      if (nextSegmentEntry != null)
        nextSegmentEntry.getValue.baseOffset
      else
        logEndOffset
    }

    val abortedTransactions = ListBuffer.empty[AbortedTransaction]

    def accumulator(abortedTxns: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)

    collectAbortedTransactions(startOffset, upperBoundOffset, segmentEntry, accumulator)

    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }

  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegmentEntry: JEntry[JLong, LogSegment],
                                         accumulator: List[AbortedTxn] => Unit): Unit = {
    var segmentEntry = startingSegmentEntry
    while (segmentEntry != null) {
      val searchResult = segmentEntry.getValue.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)
      if (searchResult.isComplete)
        return
      segmentEntry = segments.higherEntry(segmentEntry.getKey)
    }
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   */
  def fetchOffsetByTimestamp(targetTimestamp: Long): Option[TimestampAndOffset] = {
    maybeHandleIOException(s"Error while fetching offset by timestamp for $topicPartition in dir ${dir.getParent}") {
      debug(s"Searching offset for timestamp $targetTimestamp")

      if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
        throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

      // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
      // constant time access while being safe to use with concurrent collections unlike `toArray`.
      val segmentsCopy = logSegments.toBuffer
      // For the earliest and latest, we do not need to return the timestamp.
      if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP) {
        // The first cached epoch usually corresponds to the log start offset, but we have to verify this since
        // it may not be true following a message format version bump as the epoch will not be available for
        // log entries written in the older format.
        val earliestEpochEntry = leaderEpochCache.flatMap(_.earliestEntry)
        val epochOpt = earliestEpochEntry match {
          case Some(entry) if entry.startOffset <= logStartOffset => Optional.of[Integer](entry.epoch)
          case _ => Optional.empty[Integer]()
        }
        return Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logStartOffset, epochOpt))
      } else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP) {
        val latestEpochOpt = leaderEpochCache.flatMap(_.latestEpoch).map(_.asInstanceOf[Integer])
        val epochOptional = Optional.ofNullable(latestEpochOpt.orNull)
        return Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logEndOffset, epochOptional))
      }

      val targetSeg = {
        // Get all the segments whose largest timestamp is smaller than target timestamp
        val earlierSegs = segmentsCopy.takeWhile(_.largestTimestamp < targetTimestamp)
        // We need to search the first segment whose largest timestamp is greater than the target timestamp if there is one.
        if (earlierSegs.length < segmentsCopy.length)
          Some(segmentsCopy(earlierSegs.length))
        else
          None
      }

      targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, logStartOffset))
    }
  }

  def legacyFetchOffsetsBefore(timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val segments = logSegments.toBuffer
    val lastSegmentHasSize = segments.last.size > 0

    val offsetTimeArray =
      if (lastSegmentHasSize)
        new Array[(Long, Long)](segments.length + 1)
      else
        new Array[(Long, Long)](segments.length)

    for (i <- segments.indices)
      offsetTimeArray(i) = (math.max(segments(i).baseOffset, logStartOffset), segments(i).lastModified)
    if (lastSegmentHasSize)
      offsetTimeArray(segments.length) = (logEndOffset, time.milliseconds)

    var startIndex = -1
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        startIndex = offsetTimeArray.length - 1
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -= 1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for (j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(-_)
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, throw an OffsetOutOfRangeException
   */
  private def convertToOffsetMetadataOrThrow(offset: Long): LogOffsetMetadata = {
    val fetchDataInfo = read(offset,
      maxLength = 1,
      isolation = FetchLogEnd,
      minOneMessage = false)
    fetchDataInfo.fetchOffsetMetadata
  }

  /**
   * 删除任何符合给定条件函数的log段
   * 从最老的段开始，一直到段不满足条件为止
   * @param predicate 该函数接受候选log段和下一个log段，如果可以删除候选log段，返回true
   * @return 删除的log段数量
   */
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean, reason: String): Int = {
    lock synchronized {
      // 获取需要删除的log段
      val deletable = deletableSegments(predicate)
      if (deletable.nonEmpty)
        info(s"Found deletable segments with base offsets [${deletable.map(_.baseOffset).mkString(",")}] due to $reason")
      // 删除这些log段
      deleteSegments(deletable)
    }
  }

  /**
   * 删除log段
   * @param deletable 可删除的log段集合
   * @return 删除的log段数量
   */
  private def deleteSegments(deletable: Iterable[LogSegment]): Int = {
    // 处理IO异常
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      // 首先，删除的log段数量就是要删除的log段集合的大小
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // 我们必须要有至少一个log段，所以如果需要删除所有的log段，就必须先创建一个
        if (segments.size == numToDelete)
          roll()
        lock synchronized {
          // 检验索引写入Channel是否关闭
          checkIfMemoryMappedBufferClosed()
          // 删除给定的log段
          removeAndDeleteSegments(deletable, asyncDelete = true)
          // 判断是否需要移动logStartOffset
          maybeIncrementLogStartOffset(segments.firstEntry.getValue.baseOffset)
        }
      }
      numToDelete
    }
  }

  /**
   * 从最老的log段开始查找，直到开发者提供的predicate为false，或者log段包含高水位
   * 我们不会删除在highWaterMark或之后的log段，也会确保logStartOffset也不会超过highWaterMark
   * 如果highWaterMark还没有初始化，也就不会删除任何log段
   * 最终的段如果是空的，不会返回
   * @param predicate 该函数接受候选log段和下一个log段，如果可以删除候选log段，返回true
   * @return 准备删除的log段
   */
  private def deletableSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean): Iterable[LogSegment] = {
    if (segments.isEmpty) {
      Seq.empty
    } else {
      val deletable = ArrayBuffer.empty[LogSegment]
      // 获取当前log段集合中，最老的log段
      var segmentEntry = segments.firstEntry
      // 拥有最老的log段的情况下
      while (segmentEntry != null) {
        // 获取log段
        val segment = segmentEntry.getValue
        // 并获取log段的下一个log段
        val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
        // 获取下一个段，下一个段的基准offset，是否是最后一个log段或者没有下一个段了（默认为false）
        val (nextSegment, upperBoundOffset, isLastSegmentAndEmpty) = if (nextSegmentEntry != null)
          (nextSegmentEntry.getValue, nextSegmentEntry.getValue.baseOffset, false)
        else
        // 如果没有下一个段，返回null，log的logEndOffset
          (null, logEndOffset, segment.size == 0)
        // 如果基准offset还没有超过highWaterMark，并且开发者提供的校验函数通过，并且当前不是最后一个log段
        if (highWatermark >= upperBoundOffset && predicate(segment, Option(nextSegment)) && !isLastSegmentAndEmpty) {
          // 添加到可删除段中，并移动游标到下一个段，继续进行判断
          deletable += segment
          segmentEntry = nextSegmentEntry
        } else {
          // 否则不再进行查找
          segmentEntry = null
        }
      }
      // 返回删除的log段
      deletable
    }
  }

  /**
   * 如果允许删除topic，删除任何log段可以根据基于保留的过期时间，或者log的大小超过了保留大小
   * 无论是否开启删除，都可以删除任何在logStartOffset之前的log段
   */
  def deleteOldSegments(): Int = {
    // 开启了删除策略
    if (config.delete) {
      // 删除超出保留时间的segments
      // 删除超过保留大小的segments
      // 删除没有超过logStartOffset的segments
      deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
    } else {
      // 未开启删除策略，则直接根据logStartOffset进行删除
      deleteLogStartOffsetBreachedSegments()
    }
  }

  /**
   * 删除需要保护时间外的日志
   * @return 删除的日志数量
   */
  private def deleteRetentionMsBreachedSegments(): Int = {
    if (config.retentionMs < 0) return 0
    // 获取当前的时间戳
    val startMs = time.milliseconds

    deleteOldSegments((segment, _) => startMs - segment.largestTimestamp > config.retentionMs,
      reason = s"retention time ${config.retentionMs}ms breach")
  }

  /**
   * 首先计算需要删除的log段的大小
   * 计算的规则是要么删除某个段
   * @return 删除的log段的数量
   */
  private def deleteRetentionSizeBreachedSegments(): Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize

    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }

    deleteOldSegments(shouldDelete, reason = s"retention size in bytes ${config.retentionSize} breach")
  }

  /**
   * 根据logStartOffset进行比较的，使用log段的基准值和logStartOffset进行比较
   * @return 删除的log段的数量
   */
  private def deleteLogStartOffsetBreachedSegments(): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) =
      nextSegmentOpt.exists(_.baseOffset <= logStartOffset)

    deleteOldSegments(shouldDelete, reason = s"log start offset $logStartOffset breach")
  }

  def isFuture: Boolean = dir.getName.endsWith(Log.FutureDirSuffix)

  /**
   * The size of the log in bytes
   */
  def size: Long = Log.sizeInBytes(logSegments)

  /**
   * 结束offset的metadata，一般用于client端读取操作
   * 值与下一个offset相同
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   * 下一条消息的offset
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * 滚动log到一个新的空的log段
   * @param messagesSize 消息集大小
   * @param appendInfo   log追加的信息
   *                     下列条件满足的情况下，log段才会发生滚动
   *                     log段已满
   *                     此log段中第一条消息插入的时间已经超过最大时间限制
   *                     索引已满
   * @return 当前活跃的log段
   */
  private def maybeRoll(messagesSize: Int, appendInfo: LogAppendInfo): LogSegment = {
    // 获取当前活跃的log段
    val segment = activeSegment
    // 获取当前的时间戳
    val now = time.milliseconds
    // 获取追加消息中最大消息的时间戳
    val maxTimestampInMessages = appendInfo.maxTimestamp
    // 获取追加消息中最大的offset
    val maxOffsetInMessages = appendInfo.lastOffset
    // 判断此log段是否需要滚动
    if (segment.shouldRoll(RollParams(config, appendInfo, messagesSize, now))) {
      debug(s"Rolling new log segment (log_size = ${segment.size}/${config.segmentSize}}, " +
        s"offset_index_size = ${segment.offsetIndex.entries}/${segment.offsetIndex.maxEntries}, " +
        s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
        s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")

      /*
        maxOffsetInMessages，当前log段中最大的消息offset，对于消息集中的第一个offset，是一个启发式的值
        因为消息中的offset之差不会超过Integer.MAX_VALUE，它保证≤小于消息集中第一个offset
        确定消息中真正的第一个offset需要解压缩，接下来的offset会尝试避免在log追加过程中避免解压缩
        前面的行为分配了 新的基准offset = 旧的log段的logEndOffset
        但是在这种情况下是有问题的，因为两个连续消息的offset相差Integer.MAX_VALUE.toLong + 2或更大
        在这种情况下，前面的行为将会滚动到一个新的log端中，这个log段的基准offset将会足够想来容纳接下来的消息集
        这种便捷场景发生于一个副本节点在进行从头到尾的恢复压缩topic
        需要注意的是，仅适用于V2之前的消息格式，因为这些格式不会在标头中存储第一个消息的offset
      */
      // 如果需要进行日志滚动
      // 获取追加信息的第一个offset
      appendInfo.firstOffset match {
        // 如果存在，则进行滚动
        case Some(firstOffset) => roll(Some(firstOffset))
        // 否则将使用Integer.MAX_VALUE作为滚动的起始位置计算差值
        case None => roll(Some(maxOffsetInMessages - Integer.MAX_VALUE))
      }
    } else {
      // 不需要追加，返回当前的log端
      segment
    }
  }

  /**
   * 滚动log到一个新的活跃的log段，起始的基准offset是当前的logEndOffset
   * 这会将索引修正到其当前包含条目数的确切大小
   * @return 新创建的log段
   */
  def roll(expectedNextOffset: Option[Long] = None): LogSegment = {
    maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
      val start = time.hiResClockMs()
      lock synchronized {
        // 校验写入通道是否关闭
        checkIfMemoryMappedBufferClosed()
        // 计算新的offset，可能是producer生产时指定的
        val newOffset = math.max(expectedNextOffset.getOrElse(0L), logEndOffset)
        // 创建新的日志文件
        val logFile = Log.logFile(dir, newOffset)
        // 如果当前已存在的log端已经包含了新的基准offset
        if (segments.containsKey(newOffset)) {
          // 如果相同的基准offset已经存在，并且已经加载
          if (activeSegment.baseOffset == newOffset && activeSegment.size == 0) {
            // 如果这的段就是当前活跃的log段，并且这个log段的大小为0
            // issue: KAFKA-6388
            // 在调用shouldRoll()对大小为0的log段返回true，由于其中一个索引是是满的（由于_maxEntries=0）
            warn(s"Trying to roll a new log segment with start offset $newOffset " +
              s"=max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already " +
              s"exists and is active with size 0. Size of time index: ${activeSegment.timeIndex.entries}," +
              s" size of offset index: ${activeSegment.offsetIndex.entries}.")
            // 删除当前的log段
            removeAndDeleteSegments(Seq(activeSegment), asyncDelete = true)
          } else {
            // 否则无法添加一个已创建的log段
            throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with start offset $newOffset" +
              s" =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already exists. Existing " +
              s"segment is ${segments.get(newOffset)}.")
          }
        } else if (!segments.isEmpty && newOffset < activeSegment.baseOffset) {
          // 当前情况不包含已存在的offset log段
          // 如果存在已有的log段，并且新的log段的offset小于当前活跃log段的基准offset，也抛出异常
          throw new KafkaException(
            s"Trying to roll a new log segment for topic partition $topicPartition with " +
              s"start offset $newOffset =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) lower than start offset of the active segment $activeSegment")
        } else {
          // 此时，新的基准offset≥当前活跃的基准offset
          // 创建offset的索引文件
          val offsetIdxFile = offsetIndexFile(dir, newOffset)
          // 创建时间索引文件
          val timeIdxFile = timeIndexFile(dir, newOffset)
          // 创建事务索引文件
          val txnIdxFile = transactionIndexFile(dir, newOffset)
          // 如果已存在文件，将会覆盖
          for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
            warn(s"Newly rolled segment file ${file.getAbsolutePath} already exists; deleting it first")
            Files.delete(file.toPath)
          }
          // 将当前活跃的log段置为非活跃
          Option(segments.lastEntry).foreach(_.getValue.onBecomeInactiveSegment())
        }

        // 对producer的状态进行一个快照，用于快速恢复
        // 拥有一个快照offset与新的log段offset对齐是很有用的，因为这将确保我们可以通过从相应的快照文件开始并扫描端数据数量来恢复log段
        // 因为log段的基准offset可能实际上领先于当前的producer state的offset（对应logEndOffset）
        // 在创建快照之前，我们在此手动覆盖producer state的offset
        producerStateManager.updateMapEndOffset(newOffset)
        producerStateManager.takeSnapshot()

        // 创建新的log段
        val segment = LogSegment.open(
          // 目录
          dir,
          // 基准offset
          baseOffset = newOffset,
          // 日志配置
          config,
          // 创建时间戳
          time = time,
          fileAlreadyExists = false,
          // 日志文件大小
          initFileSize = initFileSize,
          // 是否开启预分配
          preallocate = config.preallocate)
        // 添加创建好的log段
        addSegment(segment)
        // 我们需要更新段的基准offset，并将位置信息数据追加到metadata中
        // 下一个message的offset不会发生变化
        updateLogEndOffset(nextOffsetMetadata.messageOffset)
        // 调度一次针对旧log段的异步刷新任务
        // 用于将页缓存的数据写入磁盘中
        scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

        info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")
        // 返回当前的log段
        segment
      }
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages: Long = this.logEndOffset - this.recoveryPoint

  /**
   * 将所有log段刷入到磁盘上
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * 将所有的offset的log段刷入，直到offset达到offset-1
   * @param offset offset刷新到的地方，也可以说是新的检查点的位置
   */
  def flush(offset: Long): Unit = {
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset") {
      // 如果当前offset还没有超过当前的检查点，不进行刷入
      if (offset <= this.recoveryPoint)
        return
      debug(s"Flushing log up to offset $offset, last flushed: $lastFlushTime,  current time: ${time.milliseconds()}, " +
        s"unflushed: $unflushedMessages")
      // 过滤所有的段，并对段进行刷新
      for (segment <- logSegments(this.recoveryPoint, offset))
        segment.flush()
      // 刷新后
      lock synchronized {
        // 首先校验内存Channel连接是否关闭，Channel关于索引的文件写入
        checkIfMemoryMappedBufferClosed()
        // 设置新的检查点，及刷新的时间戳
        if (offset > this.recoveryPoint) {
          this.recoveryPoint = offset
          lastFlushedTime.set(time.milliseconds)
        }
      }
    }
  }

  /**
   * Cleanup old producer snapshots after the recovery point is checkpointed. It is useful to retain
   * the snapshots from the recent segments in case we need to truncate and rebuild the producer state.
   * Otherwise, we would always need to rebuild from the earliest segment.
   *
   * More specifically:
   *
   * 1. We always retain the producer snapshot from the last two segments. This solves the common case
   * of truncating to an offset within the active segment, and the rarer case of truncating to the previous segment.
   *
   * 2. We only delete snapshots for offsets less than the recovery point. The recovery point is checkpointed
   * periodically and it can be behind after a hard shutdown. Since recovery starts from the recovery point, the logic
   * of rebuilding the producer snapshots in one pass and without loading older segments is simpler if we always
   * have a producer snapshot for all segments being recovered.
   *
   * Return the minimum snapshots offset that was retained.
   */
  def deleteSnapshotsAfterRecoveryPointCheckpoint(): Long = {
    val minOffsetToRetain = minSnapshotsOffsetToRetain
    producerStateManager.deleteSnapshotsBefore(minOffsetToRetain)
    minOffsetToRetain
  }

  // Visible for testing, see `deleteSnapshotsAfterRecoveryPointCheckpoint()` for details
  private[log] def minSnapshotsOffsetToRetain: Long = {
    lock synchronized {
      val twoSegmentsMinOffset = lowerSegment(activeSegment.baseOffset).getOrElse(activeSegment).baseOffset
      // Prefer segment base offset
      val recoveryPointOffset = lowerSegment(recoveryPoint).map(_.baseOffset).getOrElse(recoveryPoint)
      math.min(recoveryPointOffset, twoSegmentsMinOffset)
    }
  }

  private def lowerSegment(offset: Long): Option[LogSegment] =
    Option(segments.lowerEntry(offset)).map(_.getValue)

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete(): Unit = {
    maybeHandleIOException(s"Error while deleting log for $topicPartition in dir ${dir.getParent}") {
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        removeLogMetrics()
        removeAndDeleteSegments(logSegments, asyncDelete = false)
        leaderEpochCache.foreach(_.clear())
        Utils.delete(dir)
        // File handlers will be closed if this log is deleted
        isMemoryMappedBufferClosed = true
      }
    }
  }

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    producerStateManager.takeSnapshot()
  }

  // visible for testing
  private[log] def latestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.latestSnapshotOffset
  }

  // visible for testing
  private[log] def oldestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.oldestSnapshotOffset
  }

  // visible for testing
  private[log] def latestProducerStateEndOffset: Long = lock synchronized {
    producerStateManager.mapEndOffset
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   * @return True iff targetOffset < logEndOffset
   */
  private[log] def truncateTo(targetOffset: Long): Boolean = {
    maybeHandleIOException(s"Error while truncating log to offset $targetOffset for $topicPartition in dir ${dir.getParent}") {
      if (targetOffset < 0)
        throw new IllegalArgumentException(s"Cannot truncate partition $topicPartition to a negative offset (%d).".format(targetOffset))
      if (targetOffset >= logEndOffset) {
        info(s"Truncating to $targetOffset has no effect as the largest offset in the log is ${logEndOffset - 1}")
        false
      } else {
        info(s"Truncating to offset $targetOffset")
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          if (segments.firstEntry.getValue.baseOffset > targetOffset) {
            truncateFullyAndStartAt(targetOffset)
          } else {
            val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
            removeAndDeleteSegments(deletable, asyncDelete = true)
            activeSegment.truncateTo(targetOffset)
            updateLogEndOffset(targetOffset)
            this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
            this.logStartOffset = math.min(targetOffset, this.logStartOffset)
            leaderEpochCache.foreach(_.truncateFromEnd(targetOffset))
            loadProducerState(targetOffset, reloadFromCleanShutdown = false)
          }
          true
        }
      }
    }
  }

  /**
   * Delete all data in the log and start at the new offset
   * @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long): Unit = {
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start at offset $newOffset")
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        removeAndDeleteSegments(logSegments, asyncDelete = true)
        addSegment(LogSegment.open(dir,
          baseOffset = newOffset,
          config = config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate))
        updateLogEndOffset(newOffset)
        leaderEpochCache.foreach(_.clearAndFlush())

        producerStateManager.truncate()
        producerStateManager.updateMapEndOffset(newOffset)
        maybeIncrementFirstUnstableOffset()

        this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
        this.logStartOffset = newOffset
      }
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime: Long = lastFlushedTime.get

  /**
   * 活跃的的log段
   * 任何时刻仅有一个活跃的log段
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * 从最老到最新排列的log段
   */
  def logSegments: Iterable[LogSegment] = segments.values.asScala

  /**
   * 从当前检查点，到给定的下一个检查点之间的所有log段
   * @param from 当前检查点
   * @param to   下一个检查点
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    lock synchronized {
      val view = Option(segments.floorKey(from)).map { floor =>
        segments.subMap(floor, to)
      }.getOrElse(segments.headMap(to))
      view.values.asScala
    }
  }

  /**
   * Get the largest log segment with a base offset less than or equal to the given offset, if one exists.
   * @return the optional log segment
   */
  private def floorLogSegment(offset: Long): Option[LogSegment] = {
    Option(segments.floorEntry(offset)).map(_.getValue)
  }

  override def toString: String = {
    val logString = new StringBuilder
    logString.append(s"Log(dir=$dir")
    logString.append(s", topic=${topicPartition.topic}")
    logString.append(s", partition=${topicPartition.partition}")
    logString.append(s", highWatermark=$highWatermark")
    logString.append(s", lastStableOffset=$lastStableOffset")
    logString.append(s", logStartOffset=$logStartOffset")
    logString.append(s", logEndOffset=$logEndOffset")
    logString.append(")")
    logString.toString
  }

  /**
   * 此方法删除给定的log段：
   * * 从segment集合中以删除，此segment不会再可读
   * * 它通过追加.deleted的形式重命名了索引文件
   * * 可以通过异步和同步的两种方式
   *
   * 异步删除允许在不同步的情况下进行并发读，并且在读取文件时无法物理删除文件
   * 此方法不需要将IOException转换为KafkaStorageException，因为它可以在加载所有日志之前被调用，或者直接调用者将捕获并处理IOException
   * @param segments    需要删除的log段
   * @param asyncDelete 删除是异步的还是同步的
   */
  private def removeAndDeleteSegments(segments: Iterable[LogSegment], asyncDelete: Boolean): Unit = {
    lock synchronized {
      // 由于大多数调用者将迭代器保存在segments中，并且removeAndDeleteSegment通过已删除的段对齐进行转变，我们应该在此处强制实现迭代器，以使迭代结果保持有效和确定性
      val toDelete = segments.toList
      toDelete.foreach { segment =>
        // 从segments集合中删除指定的segment
        this.segments.remove(segment.baseOffset)
      }
      // 不可读后，删除对应的segment文件
      deleteSegmentFiles(toDelete, asyncDelete)
    }
  }

  /**
   * 对给定的文件进行物理删除，允许异步或同步的方式
   * 此方法需要假定文件时存在的，并且不是线程安全的
   * 此方法不需要将IOException转换为KafkaStorageException，因为它可以在加载所有日志之前被调用，或者直接调用者将捕获并处理IOException
   * @throws IOException 文件不可以重命名，并且仍然存在的情况下，抛出IOException异常
   */
  private def deleteSegmentFiles(segments: Iterable[LogSegment], asyncDelete: Boolean): Unit = {
    segments.foreach(_.changeFileSuffixes("", Log.DeletedFileSuffix))

    def deleteSegments(): Unit = {
      info(s"Deleting segments $segments")
      maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
        segments.foreach(_.deleteIfExists())
      }
    }

    if (asyncDelete) {
      info(s"Scheduling segments for deletion $segments")
      scheduler.schedule("delete-file", () => deleteSegments, delay = config.fileDeleteDelayMs)
    } else {
      deleteSegments()
    }
  }

  /**
   * Swap one or more new segment in place and delete one or more existing segments in a crash-safe manner. The old
   * segments will be asynchronously deleted.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the caller will catch and handle IOException
   *
   * The sequence of operations is:
   * <ol>
   * <li> Cleaner creates one or more new segments with suffix .cleaned and invokes replaceSegments().
   * If broker crashes at this point, the clean-and-swap operation is aborted and
   * the .cleaned files are deleted on recovery in loadSegments().
   * <li> New segments are renamed .swap. If the broker crashes before all segments were renamed to .swap, the
   * clean-and-swap operation is aborted - .cleaned as well as .swap files are deleted on recovery in
   * loadSegments(). We detect this situation by maintaining a specific order in which files are renamed from
   * .cleaned to .swap. Basically, files are renamed in descending order of offsets. On recovery, all .swap files
   * whose offset is greater than the minimum-offset .clean file are deleted.
   * <li> If the broker crashes after all new segments were renamed to .swap, the operation is completed, the swap
   * operation is resumed on recovery as described in the next step.
   * <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   * If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   * replaceSegments() is then invoked to complete the swap with newSegment recreated from
   * the .swap file and oldSegments containing segments which were not renamed before the crash.
   * <li> Swap segment(s) are renamed to replace the existing segments, completing this operation.
   * If the broker crashes, any .deleted files which may be left behind are deleted
   * on recovery in loadSegments().
   * </ol>
   * @param newSegments         The new log segment to add to the log
   * @param oldSegments         The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */
  private[log] def replaceSegments(newSegments: Seq[LogSegment], oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false): Unit = {
    lock synchronized {
      val sortedNewSegments = newSegments.sortBy(_.baseOffset)
      // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
      // but before this method is executed. We want to filter out those segments to avoid calling asyncDeleteSegment()
      // multiple times for the same segment.
      val sortedOldSegments = oldSegments.filter(seg => segments.containsKey(seg.baseOffset)).sortBy(_.baseOffset)

      checkIfMemoryMappedBufferClosed()
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        sortedNewSegments.reverse.foreach(_.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix))
      sortedNewSegments.reverse.foreach(addSegment(_))

      // delete the old files
      for (seg <- sortedOldSegments) {
        // remove the index entry
        if (seg.baseOffset != sortedNewSegments.head.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment files
        deleteSegmentFiles(List(seg), asyncDelete = true)
      }
      // okay we are safe now, remove the swap suffix
      sortedNewSegments.foreach(_.changeFileSuffixes(Log.SwapFileSuffix, ""))
    }
  }

  /**
   * This function does not acquire Log.lock. The caller has to make sure log segments don't get deleted during
   * this call, and also protects against calling this function on the same segment in parallel.
   *
   * Currently, it is used by LogCleaner threads on log compact non-active segments only with LogCleanerManager's lock
   * to ensure no other logcleaner threads and retention thread can work on the same segment.
   */
  private[log] def getFirstBatchTimestampForSegments(segments: Iterable[LogSegment]): Iterable[Long] = {
    segments.map {
      segment =>
        segment.getFirstBatchTimestamp()
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }

  /**
   * 在当前日志文件内部，添加给定的log段到log段集合中
   * @param segment 需要添加的log段
   */
  @threadsafe
  def addSegment(segment: LogSegment): LogSegment = this.segments.put(segment.baseOffset, segment)

  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    try {
      fun
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir.getParent, msg, e)
        throw new KafkaStorageException(msg, e)
    }
  }

  private[log] def retryOnOffsetOverflow[T](fn: => T): T = {
    while (true) {
      try {
        return fn
      } catch {
        case e: LogSegmentOffsetOverflowException =>
          info(s"Caught segment overflow error: ${e.getMessage}. Split segment and retry.")
          splitOverflowedSegment(e.segment)
      }
    }
    throw new IllegalStateException()
  }

  /**
   * Split a segment into one or more segments such that there is no offset overflow in any of them. The
   * resulting segments will contain the exact same messages that are present in the input segment. On successful
   * completion of this method, the input segment will be deleted and will be replaced by the resulting new segments.
   * See replaceSegments for recovery logic, in case the broker dies in the middle of this operation.
   * <p>Note that this method assumes we have already determined that the segment passed in contains records that cause
   * offset overflow.</p>
   * <p>The split logic overloads the use of .clean files that LogCleaner typically uses to make the process of replacing
   * the input segment with multiple new segments atomic and recoverable in the event of a crash. See replaceSegments
   * and completeSwapOperations for the implementation to make this operation recoverable on crashes.</p>
   * @param segment Segment to split
   * @return List of new segments that replace the input segment
   */
  private[log] def splitOverflowedSegment(segment: LogSegment): List[LogSegment] = {
    require(isLogFile(segment.log.file), s"Cannot split file ${segment.log.file.getAbsoluteFile}")
    require(segment.hasOverflow, "Split operation is only permitted for segments with overflow")

    info(s"Splitting overflowed segment $segment")

    val newSegments = ListBuffer[LogSegment]()
    try {
      var position = 0
      val sourceRecords = segment.log

      while (position < sourceRecords.sizeInBytes) {
        val firstBatch = sourceRecords.batchesFrom(position).asScala.head
        val newSegment = LogCleaner.createNewCleanedSegment(this, firstBatch.baseOffset)
        newSegments += newSegment

        val bytesAppended = newSegment.appendFromFile(sourceRecords, position)
        if (bytesAppended == 0)
          throw new IllegalStateException(s"Failed to append records from position $position in $segment")

        position += bytesAppended
      }

      // prepare new segments
      var totalSizeOfNewSegments = 0
      newSegments.foreach { splitSegment =>
        splitSegment.onBecomeInactiveSegment()
        splitSegment.flush()
        splitSegment.lastModified = segment.lastModified
        totalSizeOfNewSegments += splitSegment.log.sizeInBytes
      }
      // size of all the new segments combined must equal size of the original segment
      if (totalSizeOfNewSegments != segment.log.sizeInBytes)
        throw new IllegalStateException("Inconsistent segment sizes after split" +
          s" before: ${segment.log.sizeInBytes} after: $totalSizeOfNewSegments")

      // replace old segment with new ones
      info(s"Replacing overflowed segment $segment with split segments $newSegments")
      replaceSegments(newSegments.toList, List(segment), isRecoveredSwapFile = false)
      newSegments.toList
    } catch {
      case e: Exception =>
        newSegments.foreach { splitSegment =>
          splitSegment.close()
          splitSegment.deleteIfExists()
        }
        throw e
    }
  }
}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /** a time index file */
  val TimeIndexFileSuffix = ".timeindex"

  val ProducerSnapshotFileSuffix = ".snapshot"

  /** an (aborted) txn index */
  val TxnIndexFileSuffix = ".txnindex"

  /** a file that is scheduled to be deleted */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /**
   * cleanshutdown文件证明broker在0.8或者更高的版本非常干净的关闭
   * 这个用于避免在一次干净的关闭后进行一次不必要的恢复，理论上，这可能会通过传递恢复节点来避免，然而查找正确的位置需要获取访问offset索引文件
   * 但是在非干净关闭过程中，这又是线程不安全的
   * 请看PR#2104
   */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** a directory that is scheduled to be deleted */
  val DeleteDirSuffix = "-delete"

  /**
   * 用于存储future partition的目录
   */
  val FutureDirSuffix = "-future"

  private[log] val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")
  private[log] val FutureDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$FutureDirSuffix")

  val UnknownOffset = -1L

  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long,
            recoveryPoint: Long,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time = Time.SYSTEM,
            maxProducerIdExpirationMs: Int,
            producerIdExpirationCheckIntervalMs: Int,
            logDirFailureChannel: LogDirFailureChannel): Log = {
    val topicPartition = Log.parseTopicPartitionName(dir)
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    new Log(dir, config, logStartOffset, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager, logDirFailureChannel)
  }

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset and the given suffix
   * @param dir    The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
   */
  def logFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix + suffix)

  /**
   * Return a directory name to rename the log directory to for async deletion.
   * The name will be in the following format: "topic-partitionId.uniqueId-delete".
   * If the topic name is too long, it will be truncated to prevent the total name
   * from exceeding 255 characters.
   */
  def logDeleteDirName(topicPartition: TopicPartition): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    val suffix = s"-${topicPartition.partition()}.${uniqueId}${DeleteDirSuffix}"
    val prefixLength = Math.min(topicPartition.topic().size, 255 - suffix.size)
    s"${topicPartition.topic().substring(0, prefixLength)}${suffix}"
  }

  /**
   * Return a future directory name for the given topic partition. The name will be in the following
   * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
   */
  def logFutureDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, FutureDirSuffix)
  }

  private def logDirNameWithSuffix(topicPartition: TopicPartition, suffix: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    s"${logDirName(topicPartition)}.$uniqueId$suffix"
  }

  /**
   * Return a directory name for the given topic partition. The name will be in the following
   * format: topic-partition where topic, partition are variables.
   */
  def logDirName(topicPartition: TopicPartition): String = {
    s"${topicPartition.topic}-${topicPartition.partition}"
  }

  /**
   * 使用给定的dir、基准offset、前缀来构建一个索引文件名称
   * @param dir    log将会存储的文件目录
   * @param offset log的基准offset
   * @param suffix log的文件名称的前缀
   */
  def offsetIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix + suffix)

  /**
   * Construct a time index file name in the given dir using the given base offset and the given suffix
   * @param dir    The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def timeIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix + suffix)

  def deleteFileIfExists(file: File, suffix: String = ""): Unit =
    Files.deleteIfExists(new File(file.getPath + suffix).toPath)

  /**
   * Construct a producer id snapshot file using the given offset.
   * @param dir    The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */
  def producerSnapshotFile(dir: File, offset: Long): File =
    new File(dir, filenamePrefixFromOffset(offset) + ProducerSnapshotFileSuffix)

  /**
   * Construct a transaction index file name in the given dir using the given base offset and the given suffix
   * @param dir    The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def transactionIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix + suffix)

  def offsetFromFileName(filename: String): Long = {
    filename.substring(0, filename.indexOf('.')).toLong
  }

  def offsetFromFile(file: File): Long = {
    offsetFromFileName(file.getName)
  }

  /**
   * Calculate a log's size (in bytes) based on its log segments
   * @param segments The log segments to calculate the size of
   * @return Sum of the log segments' sizes (in bytes)
   */
  def sizeInBytes(segments: Iterable[LogSegment]): Long =
    segments.map(_.size.toLong).sum

  /**
   * 从log的路径解析出topic partition的名称
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {
    if (dir == null)
      throw new KafkaException("dir should not be null")

    /**
     * 异常方法
     * @param dir 异常日志目录
     * @return Kafka异常
     */
    def exception(dir: File): KafkaException = {
      new KafkaException(s"Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
        "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
        "Kafka's log directories (and children) should only contain Kafka topic data.")
    }

    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)
    if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches ||
      dirName.endsWith(FutureDirSuffix) && !FutureDirPattern.matcher(dirName).matches)
      throw exception(dir)

    val name: String =
    // 带有后缀的路径名称，去掉后缀
      if (dirName.endsWith(DeleteDirSuffix) || dirName.endsWith(FutureDirSuffix)) dirName.substring(0, dirName.lastIndexOf('.'))
      else dirName
    // 获取索引名称
    val index = name.lastIndexOf('-')
    // 获取topic名称
    val topic = name.substring(0, index)
    // 获取partition名称
    val partitionString = name.substring(index + 1)
    // 如果topic或者partition名称为空，抛出异常
    if (topic.isEmpty || partitionString.isEmpty)
      throw exception(dir)
    // partition是数字标号，转换成数字
    val partition =
      try partitionString.toInt
      catch {
        case _: NumberFormatException => throw exception(dir)
      }
    // topic-partition信息
    new TopicPartition(topic, partition)
  }

  private def isIndexFile(file: File): Boolean = {
    val filename = file.getName
    filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix)
  }

  private def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileSuffix)

}
