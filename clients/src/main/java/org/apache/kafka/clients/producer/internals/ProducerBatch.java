/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * 一批batch，等待发送
 * 此类并不是线程安全的，如果需要进行修改，则需要进行外部同步操作
 */
public final class ProducerBatch {

	private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

	private enum FinalState {ABORTED, FAILED, SUCCEEDED}

	final long createdMs;
	/**
	 * batch关联的topic-partition信息
	 */
	final TopicPartition topicPartition;
	/**
	 * 消息发送请求结果
	 */
	final ProduceRequestResult produceFuture;

	private final List<Thunk> thunks = new ArrayList<>();
	/**
	 * 内存写入建造器
	 */
	private final MemoryRecordsBuilder recordsBuilder;
	private final AtomicInteger attempts = new AtomicInteger(0);
	private final boolean isSplitBatch;
	private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);
	/**
	 * 当前batch存储的record数量
	 */
	int recordCount;
	/**
	 * record最大数量限制
	 */
	int maxRecordSize;
	private long lastAttemptMs;
	/**
	 * 上一次追加record的时间
	 */
	private long lastAppendTime;
	private long drainedMs;
	private boolean retry;
	private boolean reopened;

	public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
		this(tp, recordsBuilder, createdMs, false);
	}

	public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs, boolean isSplitBatch) {
		this.createdMs = createdMs;
		this.lastAttemptMs = createdMs;
		this.recordsBuilder = recordsBuilder;
		this.topicPartition = tp;
		this.lastAppendTime = createdMs;
		this.produceFuture = new ProduceRequestResult(topicPartition);
		this.retry = false;
		this.isSplitBatch = isSplitBatch;
		float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
				recordsBuilder.compressionType());
		recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
	}

	/**
	 * 向record集合中添加record，返回record集合最近一次的offset
	 * @return 添加到record set的描述，或者是null
	 */
	public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
		// 校验当前batch还能继续写入
		if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
			// 不能继续写入，返回null
			return null;
		} else {
			// 向MemoryRecords中追加内容
			Long checksum = this.recordsBuilder.append(timestamp, key, value, headers);
			// 计算当前batch最大的record的大小
			this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
					recordsBuilder.compressionType(), key, value, headers));
			// 更新追加时间
			this.lastAppendTime = now;

			FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
					timestamp, checksum,
					key == null ? -1 : key.length,
					value == null ? -1 : value.length,
					Time.SYSTEM);
			// 我们必须确保每个返回给开发者的future，以防batch出现需要分隔成为几个新的batch，然后再重新发送
			thunks.add(new Thunk(callback, future));
			// 操作record计数器
			this.recordCount++;
			return future;
		}
	}

	/**
	 * This method is only used by {@link #split(int)} when splitting a large batch to smaller ones.
	 * @return true if the record has been successfully appended, false otherwise.
	 */
	private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
		if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
			return false;
		} else {
			// No need to get the CRC.
			this.recordsBuilder.append(timestamp, key, value, headers);
			this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
					recordsBuilder.compressionType(), key, value, headers));
			FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
					timestamp, thunk.future.checksumOrNull(),
					key == null ? -1 : key.remaining(),
					value == null ? -1 : value.remaining(),
					Time.SYSTEM);
			// Chain the future to the original thunk.
			thunk.future.chain(future);
			this.thunks.add(thunk);
			this.recordCount++;
			return true;
		}
	}

	/**
	 * Abort the batch and complete the future and callbacks.
	 * @param exception The exception to use to complete the future and awaiting callbacks.
	 */
	public void abort(RuntimeException exception) {
		if (!finalState.compareAndSet(null, FinalState.ABORTED))
			throw new IllegalStateException("Batch has already been completed in final state " + finalState.get());

		log.trace("Aborting batch for partition {}", topicPartition, exception);
		completeFutureAndFireCallbacks(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, exception);
	}

	/**
	 * Return `true` if {@link #done(long, long, RuntimeException)} has been invoked at least once, `false` otherwise.
	 */
	public boolean isDone() {
		return finalState() != null;
	}

	/**
	 * batch的结束状态，一旦状态置为结束，是不可变的
	 * 此方法可能会在一个batch上调用一次或两次
	 * 调用两次的原因：
	 * 1. 一个处于发送状态的batch在收到Kafka集群响应之前失效了，batch的最终状态设置为FAILED
	 * 但是batch却在broker上成功了，第二次调用batch.done()时，将batch的最终状态设置为SUCCEEDED
	 * 2. 在KafkaProducer强制关闭时，发生了事务中断，batch的最终状态置为ABORTED，但是broker返回了成功的生产请求响应，batch的最终状态置为SUCCEEDED
	 * 尝试从[FAILED | ABORTED]变更为SUCCEEDED，日志记录
	 * 尝试从一种失败状态，转移到相同，或者另一种失败状态，将会被忽略
	 * 尝试从SUCCEEDED状态，转移到相同，或者另外一种失败状态，抛出异常
	 * @param baseOffset    The base offset of the messages assigned by the server
	 * @param logAppendTime The log append time or -1 if CreateTime is being used
	 * @param exception     The exception that occurred (or null if the request was successful)
	 * @return true if the batch was completed successfully and false if the batch was previously aborted
	 */
	public boolean done(long baseOffset, long logAppendTime, RuntimeException exception) {
		// 最终状态确认，没有异常即为SUCCEEDED，存在异常即为FAILED
		final FinalState tryFinalState = (exception == null) ? FinalState.SUCCEEDED : FinalState.FAILED;

		// 日志记录
		if (tryFinalState == FinalState.SUCCEEDED) {
			log.trace("Successfully produced messages to {} with base offset {}.", topicPartition, baseOffset);
		} else {
			log.trace("Failed to produce messages to {} with base offset {}.", topicPartition, baseOffset, exception);
		}

		// 设置最终状态，从null转换为最终状态
		if (this.finalState.compareAndSet(null, tryFinalState)) {
			completeFutureAndFireCallbacks(baseOffset, logAppendTime, exception);
			return true;
		}

		// 状态发生了第二次变更，需要对不同的状态进行不同的操作
		if (this.finalState.get() != FinalState.SUCCEEDED) {
			if (tryFinalState == FinalState.SUCCEEDED) {
				// 流转前状态是不成功的状态，第二次最终状态变为成功
				log.debug("ProduceResponse returned {} for {} after batch with base offset {} had already been {}.",
						tryFinalState, topicPartition, baseOffset, this.finalState.get());
			} else {
				// FAILED --> FAILED and ABORTED --> FAILED transitions are ignored.
				// FAILED --> FAILED and ABORTED --> FAILED的转换
				log.debug("Ignored state transition {} -> {} for {} batch with base offset {}",
						this.finalState.get(), tryFinalState, topicPartition, baseOffset);
			}
		} else {
			// 一个最终状态已经为SUCCEEDED的batch，尝试修改最终状态的行为是异常的，需要抛出异常
			throw new IllegalStateException("A " + this.finalState.get() + " batch must not attempt another state change to " + tryFinalState);
		}
		return false;
	}

	/**
	 * 完成生产请求异步任务，并调用回调方法
	 * @param baseOffset    batch的基准offset
	 * @param logAppendTime 响应的中的log追加时间
	 * @param exception     响应中的异常
	 */
	private void completeFutureAndFireCallbacks(long baseOffset, long logAppendTime, RuntimeException exception) {
		// 设置从响应获取的future信息，为之后的回调方法调用，奠定基础
		// 我们依赖于它的状态进行onCompletion调用
		produceFuture.set(baseOffset, logAppendTime, exception);

		// 执行所有的回调任务
		for (Thunk thunk : thunks) {
			try {
				if (exception == null) {
					// 不存在异常的情况下，执行完成回调
					RecordMetadata metadata = thunk.future.value();
					if (thunk.callback != null)
						thunk.callback.onCompletion(metadata, null);
				} else {
					if (thunk.callback != null)
						// 异常状态下，执行异常回调
						thunk.callback.onCompletion(null, exception);
				}
				// 两种回调方式，入参不同而已
			} catch (Exception e) {
				log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
			}
		}

		produceFuture.done();
	}

	/**
	 * 将batch以指定的大小分割为小batch
	 * @param splitBatchSize 指定每个batch的大小
	 * @return 分割后batch的队列
	 */
	public Deque<ProducerBatch> split(int splitBatchSize) {
		Deque<ProducerBatch> batches = new ArrayDeque<>();
		MemoryRecords memoryRecords = recordsBuilder.build();

		Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();
		if (!recordBatchIter.hasNext())
			throw new IllegalStateException("Cannot split an empty producer batch.");

		RecordBatch recordBatch = recordBatchIter.next();
		if (recordBatch.magic() < MAGIC_VALUE_V2 && !recordBatch.isCompressed())
			throw new IllegalArgumentException("Batch splitting cannot be used with non-compressed messages " +
					"with version v0 and v1");

		if (recordBatchIter.hasNext())
			throw new IllegalArgumentException("A producer batch should only have one record batch.");

		Iterator<Thunk> thunkIter = thunks.iterator();
		ProducerBatch batch = null;
		// 遍历当前batch中的每条record
		for (Record record : recordBatch) {
			assert thunkIter.hasNext();
			Thunk thunk = thunkIter.next();
			if (batch == null)
				// 申请一块新内存创建batch
				batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);

			// 如果子batch已满，尝试创建新的batch，并追加record
			if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
				batches.add(batch);
				batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
				batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
			}
		}

		// 关闭最后一个batch
		if (batch != null)
			batches.add(batch);

		produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, new RecordBatchTooLargeException());
		produceFuture.done();

		// 重新设置batch的sequence序号
		if (hasSequence()) {
			int sequence = baseSequence();
			ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId(), producerEpoch());
			for (ProducerBatch newBatch : batches) {
				newBatch.setProducerState(producerIdAndEpoch, sequence, isTransactional());
				sequence += newBatch.recordCount;
			}
		}
		return batches;
	}

	private ProducerBatch createBatchOffAccumulatorForRecord(Record record, int batchSize) {
		int initialSize = Math.max(AbstractRecords.estimateSizeInBytesUpperBound(magic(),
				recordsBuilder.compressionType(), record.key(), record.value(), record.headers()), batchSize);
		ByteBuffer buffer = ByteBuffer.allocate(initialSize);

		// Note that we intentionally do not set producer state (producerId, epoch, sequence, and isTransactional)
		// for the newly created batch. This will be set when the batch is dequeued for sending (which is consistent
		// with how normal batches are handled).
		MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic(), recordsBuilder.compressionType(),
				TimestampType.CREATE_TIME, 0L);
		return new ProducerBatch(topicPartition, builder, this.createdMs, true);
	}

	public boolean isCompressed() {
		return recordsBuilder.compressionType() != CompressionType.NONE;
	}

	/**
	 * A callback and the associated FutureRecordMetadata argument to pass to it.
	 */
	final private static class Thunk {
		final Callback callback;
		final FutureRecordMetadata future;

		Thunk(Callback callback, FutureRecordMetadata future) {
			this.callback = callback;
			this.future = future;
		}
	}

	@Override
	public String toString() {
		return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
	}

	boolean hasReachedDeliveryTimeout(long deliveryTimeoutMs, long now) {
		return deliveryTimeoutMs <= now - this.createdMs;
	}

	public FinalState finalState() {
		return this.finalState.get();
	}

	int attempts() {
		return attempts.get();
	}

	void reenqueued(long now) {
		attempts.getAndIncrement();
		lastAttemptMs = Math.max(lastAppendTime, now);
		lastAppendTime = Math.max(lastAppendTime, now);
		retry = true;
	}

	long queueTimeMs() {
		return drainedMs - createdMs;
	}

	long waitedTimeMs(long nowMs) {
		return Math.max(0, nowMs - lastAttemptMs);
	}

	void drained(long nowMs) {
		this.drainedMs = Math.max(drainedMs, nowMs);
	}

	boolean isSplitBatch() {
		return isSplitBatch;
	}

	/**
	 * 判断batch是否在发送数据过程中进行了重试
	 */
	public boolean inRetry() {
		return this.retry;
	}

	public MemoryRecords records() {
		return recordsBuilder.build();
	}

	public int estimatedSizeInBytes() {
		return recordsBuilder.estimatedSizeInBytes();
	}

	public double compressionRatio() {
		return recordsBuilder.compressionRatio();
	}

	public boolean isFull() {
		return recordsBuilder.isFull();
	}

	public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
		recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
	}

	public void resetProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
		reopened = true;
		recordsBuilder.reopenAndRewriteProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
	}

	/**
	 * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
	 * possible to update the RecordBatch header.
	 */
	public void closeForRecordAppends() {
		recordsBuilder.closeForRecordAppends();
	}

	public void close() {
		recordsBuilder.close();
		if (!recordsBuilder.isControlBatch()) {
			CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
					recordsBuilder.compressionType(),
					(float) recordsBuilder.compressionRatio());
		}
		reopened = false;
	}

	/**
	 * 中断record builder，并且重置潜在buffer的状态
	 * 这个用于终于batch之前使用，确保之前追加的record不会被读取
	 * 用于在我们希望确保批处理最终被中止的情况下使用，但是在这种情况，调用完成回调任务是不安全的
	 */
	public void abortRecordAppends() {
		// 中断当前batch的追加
		recordsBuilder.abort();
	}

	public boolean isClosed() {
		return recordsBuilder.isClosed();
	}

	public ByteBuffer buffer() {
		return recordsBuilder.buffer();
	}

	public int initialCapacity() {
		return recordsBuilder.initialCapacity();
	}

	public boolean isWritable() {
		return !recordsBuilder.isClosed();
	}

	public byte magic() {
		return recordsBuilder.magic();
	}

	public long producerId() {
		return recordsBuilder.producerId();
	}

	public short producerEpoch() {
		return recordsBuilder.producerEpoch();
	}

	public int baseSequence() {
		return recordsBuilder.baseSequence();
	}

	public boolean hasSequence() {
		return baseSequence() != RecordBatch.NO_SEQUENCE;
	}

	public boolean isTransactional() {
		return recordsBuilder.isTransactional();
	}

	public boolean sequenceHasBeenReset() {
		return reopened;
	}
}
