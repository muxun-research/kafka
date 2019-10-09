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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private final Logger log;
    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final CompressionType compression;
    private final int lingerMs;
    private final long retryBackoffMs;
    private final int deliveryTimeoutMs;
    private final BufferPool free;
    private final Time time;
    private final ApiVersions apiVersions;
    private final ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches;
	/**
	 * 已经发送、还未发送，未收到ack的batch
	 */
	private final IncompleteBatches incomplete;
	// 下面的变量将仅会通过sender线程访问，所以我们无需关系并发的情况
    private final Map<TopicPartition, Long> muted;
	/**
	 * 进行排空的索引
	 */
	private int drainIndex;
    private final TransactionManager transactionManager;
    private long nextBatchExpiryTimeMs = Long.MAX_VALUE; // the earliest time (absolute) a batch will expire.

    /**
     * Create a new record accumulator
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     * @param apiVersions Request API versions for current connected brokers
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             CompressionType compression,
                             int lingerMs,
                             long retryBackoffMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this.log = logContext.logger(RecordAccumulator.class);
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.batches = new CopyOnWriteMap<>();
        this.free = bufferPool;
        this.incomplete = new IncompleteBatches();
        this.muted = new HashMap<>();
        this.time = time;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        MetricName rateMetricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName totalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(rateMetricName, totalMetricName));
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param headers the Headers for the record
     * @param callback The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     * @param abortOnNewBatch A boolean that indicates returning before a new batch is created and 
     *                        running the the partitioner's onNewBatch method before trying to append again
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     Callback callback,
                                     long maxTimeToBlock,
                                     boolean abortOnNewBatch) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // check if we have an in-progress batch
            Deque<ProducerBatch> dq = getOrCreateDeque(tp);
            synchronized (dq) {
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record batch try to allocate a new batch
            if (abortOnNewBatch) {
                // Return a result that will cause another call to append.
                return new RecordAppendResult(null, false, false, true);
            }
            
            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");

                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    return appendResult;
                }

                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, time.milliseconds());
                FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                        callback, time.milliseconds()));

                dq.addLast(batch);
                incomplete.add(batch);

                // Don't deallocate this buffer in the finally block as it's being used in the record batch
                buffer = null;
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, false);
            }
        } finally {
            if (buffer != null)
                free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }

    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer, byte maxUsableMagic) {
        if (transactionManager != null && maxUsableMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw new UnsupportedVersionException("Attempting to use idempotence with a broker which does not " +
                "support the required message format (v2). The broker must be version 0.11 or later.");
        }
        return MemoryRecords.builder(buffer, maxUsableMagic, compression, TimestampType.CREATE_TIME, 0L);
    }

    /**
     *  Try to append to a ProducerBatch.
     *
     *  If it is full, we return null and a new batch is created. We also close the batch for record appends to free up
     *  resources like compression buffers. The batch will be fully closed (ie. the record batch headers will be written
     *  and memory records built) in one of the following cases (whichever comes first): right before send,
     *  if it is expired, or when the producer is closed.
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque) {
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, time.milliseconds());
            if (future == null)
                last.closeForRecordAppends();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false);
        }
        return null;
    }

    private boolean isMuted(TopicPartition tp, long now) {
		// 要小心避免不必要的map查找，因为此方法会因为向大规模分区生产消息而产生热区域
		// 获取设置的阈值
        Long throttleUntilTime = muted.get(tp);
		// 不存在阈值的话，直接返回false
        if (throttleUntilTime == null)
            return false;
		// 当前时间超出阈值，返回false，并去除监控
        if (now >= throttleUntilTime) {
            muted.remove(tp);
            return false;
		}
		// 在没有超过阈值的情况下，返回true
        return true;
	}

	/**
	 * 计算下一个batch的失效时间
	 */
	public void resetNextBatchExpiryTime() {
		nextBatchExpiryTimeMs = Long.MAX_VALUE;
	}

	/**
	 * 更新当前batch的下一次失效时间
	 * @param batch
	 */
	public void maybeUpdateNextBatchExpiryTime(ProducerBatch batch) {
		if (batch.createdMs + deliveryTimeoutMs > 0) {
			// 非负数检查是为了保证避免设置的deliveryTimeoutMs的值过大
			nextBatchExpiryTimeMs = Math.min(nextBatchExpiryTimeMs, batch.createdMs + deliveryTimeoutMs);
		} else {
			log.warn("Skipping next batch expiry time update due to addition overflow: "
					+ "batch.createMs={}, deliveryTimeoutMs={}", batch.createdMs, deliveryTimeoutMs);
		}
	}

	/**
	 * 获取在累加器中，存在时间特别长，需要执行过期策略的batch列表集合
     */
    public List<ProducerBatch> expiredBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
			// 遍历所有的batch
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                while (!deque.isEmpty()) {
                    ProducerBatch batch = deque.getFirst();
					// 因为是按照创建时间进行排序的，所以对队列中第一个batch进行判断即可
					// 如果已经发送超时
                    if (batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)) {
						// 去除来，并且中断record追加
                        deque.poll();
                        batch.abortRecordAppends();
						// 添加到失效batch集合中
						expiredBatches.add(batch);
						// 如果没有发送超时
					} else {
						// 更新指定batch的下一次失效时间
                        maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
            }
        }
        return expiredBatches;
    }

    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * Re-enqueue the given record batch in the accumulator. In Sender.completeBatch method, we check
     * whether the batch has reached deliveryTimeoutMs or not. Hence we do not do the delivery timeout check here.
     */
    public void reenqueue(ProducerBatch batch, long now) {
        batch.reenqueued(now);
        Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            if (transactionManager != null)
                insertInSequenceOrder(deque, batch);
            else
                deque.addFirst(batch);
        }
    }

    /**
     * Split the big batch that has been rejected and reenqueue the split batches in to the accumulator.
     * @return the number of split batches.
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
        // Reset the estimated compression ratio to the initial value or the big batch compression ratio, whichever
        // is bigger. There are several different ways to do the reset. We chose the most conservative one to ensure
        // the split doesn't happen too often.
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression,
                                                Math.max(1.0f, (float) bigBatch.compressionRatio()));
        Deque<ProducerBatch> dq = bigBatch.split(this.batchSize);
        int numSplitBatches = dq.size();
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);
        while (!dq.isEmpty()) {
            ProducerBatch batch = dq.pollLast();
            incomplete.add(batch);
            // We treat the newly split batches as if they are not even tried.
            synchronized (partitionDequeue) {
                if (transactionManager != null) {
                    // We should track the newly created batches since they already have assigned sequences.
                    transactionManager.addInFlightBatch(batch);
                    insertInSequenceOrder(partitionDequeue, batch);
                } else {
                    partitionDequeue.addFirst(batch);
                }
            }
        }
        return numSplitBatches;
    }

    // We will have to do extra work to ensure the queue is in order when requests are being retried and there are
    // multiple requests in flight to that partition. If the first in flight request fails to append, then all the
    // subsequent in flight requests will also fail because the sequence numbers will not be accepted.
    //
    // Further, once batches are being retried, we are reduced to a single in flight request for that partition. So when
    // the subsequent batches come back in sequence order, they will have to be placed further back in the queue.
    //
    // Note that this assumes that all the batches in the queue which have an assigned sequence also have the current
    // producer id. We will not attempt to reorder messages if the producer id has changed, we will throw an
    // IllegalStateException instead.
    private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
        // When we are requeing and have enabled idempotence, the reenqueued batch must always have a sequence.
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
                "though idempotency is enabled.");

        if (transactionManager.nextBatchBySequence(batch.topicPartition) == null)
            throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
                "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());

        ProducerBatch firstBatchInQueue = deque.peekFirst();
        if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
            // The incoming batch can't be inserted at the front of the queue without violating the sequence ordering.
            // This means that the incoming batch should be placed somewhere further back.
            // We need to find the right place for the incoming batch and insert it there.
            // We will only enter this branch if we have multiple inflights sent to different brokers and we need to retry
            // the inflight batches.
            //
            // Since we reenqueue exactly one batch a time and ensure that the queue is ordered by sequence always, it
            // is a simple linear scan of a subset of the in flight batches to find the right place in the queue each time.
            List<ProducerBatch> orderedBatches = new ArrayList<>();
            while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence())
                orderedBatches.add(deque.pollFirst());

            log.debug("Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at " +
                "position {}", batch.baseSequence(), batch.topicPartition, orderedBatches.size());
            // Either we have reached a point where there are batches without a sequence (ie. never been drained
            // and are hence in order by default), or the batch at the front of the queue has a sequence greater
            // than the incoming batch. This is the right place to add the incoming batch.
            deque.addFirst(batch);

            // Now we have to re insert the previously queued batches in the right order.
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));
            }

            // At this point, the incoming batch has been queued in the correct place according to its sequence.
        } else {
            deque.addFirst(batch);
		}
	}

	/**
	 * 获取准备就绪的分区节点列表，以及最早的不可发送分区的准备就绪时间，并且返回累加的分区batch中，是否有不清楚首领分区节点
	 * 作为目的地的节点，知道满足以下情况才算准备就绪
	 * 至少有一个分区没有后退其发送，并且这些分区都不是未完成的（避免重新排序，如果{@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}设置为1）
	 * 并且满足如下的任一条件：
	 * 	1. record集合已经满了
	 * 	2. record集合已经放入到累加器中
	 * 	3. 累加器已经超出内存，线程正在阻塞等待空间（此时所有的分区立即会被确认为就绪状态）
	 * 	4. 累加器已经关闭
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();
		// 是否有等待获取内存的线程
        boolean exhausted = this.free.queued() > 0;
		// 遍历每个分区
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
			// 获取每个分区的record双向队列
            Deque<ProducerBatch> deque = entry.getValue();
			// 对双向队列进行同步操作
			synchronized (deque) {
				// 当向比较多的分区生产消息时，当前调用是个热区域
				// 我们通常校验第一个batch，用于避免更昂贵的校验
				// 获取队列中第一个封装好的batch
                ProducerBatch batch = deque.peekFirst();
				if (batch != null) {
					// 获取分区信息
                    TopicPartition part = entry.getKey();
					// 获取当前分区的首领节点
                    Node leader = cluster.leaderFor(part);
					if (leader == null) {
						// 如果当前分区不存在首领节点，添加到未知手里能节点topic集合中
						// 首领节点不存在的情况下，也是可以进行消息发送的
						// 需要注意的是，即使双向队列为空，也不会将元素从batch中移除
                        unknownLeaderTopics.add(part.topic());
						// 此时分区存在首领节点
						// 如果就绪节点集合中没有当前节点，并且当且节点处于
                    } else if (!readyNodes.contains(leader) && !isMuted(part, nowMs)) {
						// 获取当前batch的等待时间
                        long waitedTimeMs = batch.waitedTimeMs(nowMs);
						// 是否需要进行反馈
                        boolean backingOff = batch.attempts() > 0 && waitedTimeMs < retryBackoffMs;
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
						// 判断batch是否处于满的状态，半满也算满
                        boolean full = deque.size() > 1 || batch.isFull();
						// 是否已经过期
                        boolean expired = waitedTimeMs >= timeToWaitMs;
						// 判断是否可发送
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
						// 如果可发送，且不需要进行反馈，则当前节点处于就绪状态
                        if (sendable && !backingOff) {
                            readyNodes.add(leader);
						} else {
							// 计算剩余等待时间
                            long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
							// 备注，这是一个保守的估计结果，因为一个不可发送数据的分区可能会有一个手里能借点，不久之后会发现拥有可发送的数据
							// 然而这已经足够好了，因为我们只是唤醒一下，然后就会继续sleep剩余的持续时间
							// 取剩余等待时间和下一次就绪状态检查时间的最小值
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
				}
			}
		}
		// 包装就绪检查结果
		// 参数包括，就绪的节点，下一次就绪检查的时间，以及没有首领节点的topic集合
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * Check whether there are any batches which haven't been drained
     */
    public boolean hasUndrained() {
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch = null;
        if (transactionManager != null) {
            if (!transactionManager.isSendToPartitionAllowed(tp))
                return true;

            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
            if (!producerIdAndEpoch.isValid())
                // we cannot send the batch until we have refreshed the producer id
                return true;

            if (!first.hasSequence() && transactionManager.hasUnresolvedSequence(first.topicPartition))
                // Don't drain any new batches while the state of previous sequence numbers
                // is unknown. The previous batches would be unknown if they were aborted
                // on the client after being sent to the broker at least once.
                return true;

            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
            if (firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                && first.baseSequence() != firstInFlightSequence)
                // If the queued batch already has an assigned sequence, then it is being retried.
                // In this case, we wait until the next immediate batch is ready and drain that.
                // We only move on when the next in line batch is complete (either successfully or due to
                // a fatal broker error). This effectively reduces our in flight request count to 1.
                return true;
		}
		return false;
	}

	/**
	 * 排空单个节点的batch
	 * @param cluster 指定集群
	 * @param node    指定节点
	 * @param maxSize 排空数据的最大字节数
	 * @param now     当前时间戳
	 * @return 指定节点需要排空的batch列表集合
	 */
	private List<ProducerBatch> drainBatchesForOneNode(Cluster cluster, Node node, int maxSize, long now) {
		int size = 0;
		// 获取当前节点的分区信息
		List<PartitionInfo> parts = cluster.partitionsForNode(node.id());

		List<ProducerBatch> ready = new ArrayList<>();
		// 为了减少竞争，循环不从索引0开始
		int start = drainIndex = drainIndex % parts.size();
		do {
			// 获取指定索引的分区
			PartitionInfo part = parts.get(drainIndex);
			// 创建topic-partition对象，存储关联信息
			TopicPartition tp = new TopicPartition(part.topic(), part.partition());
			// 计算新的排空索引
			this.drainIndex = (this.drainIndex + 1) % parts.size();

			// 如果当前分区请求处于已关闭状态，继续遍历下一个partition
			if (isMuted(tp, now))
				continue;
			// 获取当前partition的batch队列
			Deque<ProducerBatch> deque = getDeque(tp);
			if (deque == null)
				// 如果队列为空，继续遍历下一个partition
				continue;
			// 队列不为空的情况下，需要对队列进行同步操作
			synchronized (deque) {
				// 获取当前partition batch队列的第一个batch
				ProducerBatch first = deque.peekFirst();
				// 如果不存在第一个batch，继续遍历下一个partition
				if (first == null)
					continue;

				// 如果存在第一个batch
				// 判断是否需要进行失败重试
				boolean backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs;
				// 需要进行失败重试，继续遍历下一个partition
				if (backoff)
					continue;
				// 如果大小总和已经超过当次发送总大小的阈值，并且节点就绪batch列表不为空
				if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
					// 不需要继续发送了，跳出循环
					break;
				} else {
					// 如果没有超出发送总大小阈值
					// 首先对是否停止排空进行验证
					if (shouldStopDrainBatchesForPartition(first, tp))
						break;
					// 判断当前partition是否处于事务状态
					boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
					// 是否需要producer id以及epoch
					ProducerIdAndEpoch producerIdAndEpoch =
							transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
					// 在把当前batch放回到队列中
					ProducerBatch batch = deque.pollFirst();
					// 如果需要produce id以及epoch，同时batch不需要序列号
					if (producerIdAndEpoch != null && !batch.hasSequence()) {
						// 如果当前batch已经分配了一个序列号，我们就不要去修改produce id和序列号，因为可能会产生重复的情况
						// 尤其是在前一次尚持可能被接受的情况，并且如果我们修改了producer id和序列号，本次尝试也可能会被接受，引发了重复
						// 除此之外，我们会更新为当前分区更新下一个序列号，同时也可以用事务管理器记录batch，这样即使在接收到顺序之外的请求，我们也能根据序列号继续确保维持序列号的顺序
						batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
						transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
						log.debug("Assigned producerId {} and producerEpoch {} to batch with base sequence " +
										"{} being sent to partition {}", producerIdAndEpoch.producerId,
								producerIdAndEpoch.epoch, batch.baseSequence(), tp);

						transactionManager.addInFlightBatch(batch);
					}
					// 关闭batch
					batch.close();
					// 修改传输的字节数大小
					size += batch.records().sizeInBytes();
					// 在就绪batch集合中添加当前batch
					ready.add(batch);
					// 设置当前的排空时间
					batch.drained(now);
				}
			}
		} while (start != drainIndex);
		return ready;
	}

	/**
	 * 排空给定节点集合的数据，将它们居合道一个batch列表中
	 * batch集合将会在每个节点的基础上，适配特定的大小
	 * 方法的目的在于避免重复选择同样的topic节点
	 * @param cluster 当前集群
	 * @param nodes 需要排空的节点集合
	 * @param maxSize 需要排空的最大字节数
	 * @param now 当前时间戳
	 * @return key: node id
	 * 			value: 一个 {@link ProducerBatch} 的列表集合，每个节点的数据总大小小于请求的总大小
	 * A list of {@link ProducerBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<ProducerBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
		for (Node node : nodes) {
			// 汇总每个节点需要发送的batch列表
            List<ProducerBatch> ready = drainBatchesForOneNode(cluster, node, maxSize, now);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    /**
     * The earliest absolute time a batch will expire (in milliseconds)
     */
    public Long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

    private Deque<ProducerBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        Deque<ProducerBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        d = new ArrayDeque<>();
        Deque<ProducerBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            return previous;
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(ProducerBatch batch) {
        incomplete.remove(batch);
        // Only deallocate the batch if it is not a split batch because split batch are allocated outside the
        // buffer pool.
        if (!batch.isSplitBatch())
            free.deallocate(batch.buffer(), batch.initialCapacity());
    }

    /**
     * Package private for unit test. Get the buffer pool remaining size in bytes.
     */
    long bufferPoolAvailableMemory() {
        return free.availableMemory();
    }

    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<ProducerBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (ProducerBatch batch : this.incomplete.copyAll())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * Check whether there are any pending batches (whether sent or unsent).
     */
    public boolean hasIncomplete() {
        return !this.incomplete.isEmpty();
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        abortBatches(new KafkaException("Producer is closed forcefully."));
    }

	/**
	 * 中断所有未完成batch
     */
    void abortBatches(final RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
			// 获取到指定分区对应的分去
            synchronized (dq) {
				batch.abortRecordAppends();
				// 从当前队列中删除
				dq.remove(batch);
			}
			// 设置中断原因
			batch.abort(reason);
			// 取消分配batch
			deallocate(batch);
		}
	}

	/**
	 * 中断还没有发送出去的batch
	 * @param reason 中断的原因，一般是运行时异常
     */
    void abortUndrainedBatches(RuntimeException reason) {
		// 复制一份还未收到ack的batch
        for (ProducerBatch batch : incomplete.copyAll()) {
			// 由于存储的是batch，我们还需要获取存储batch的双向队列
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
			boolean aborted = false;
			// 对batch所在的双向队列进行同步操作
			synchronized (dq) {
				// 如果拥有事务管理器的情况下，batch没有设置序列号
				// 或者在没有事务管理的情况下，batch没有处于关闭状态
                if ((transactionManager != null && !batch.hasSequence()) || (transactionManager == null && !batch.isClosed())) {
					// 可以中断
					aborted = true;
					// 中断并移除batch
                    batch.abortRecordAppends();
					dq.remove(batch);
				}
			}
			// 如果当前batch执行了中断，记录中断原因，释放batch
            if (aborted) {
                batch.abort(reason);
                deallocate(batch);
            }
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.put(tp, Long.MAX_VALUE);
    }

    public void unmutePartition(TopicPartition tp, long throttleUntilTimeMs) {
        muted.put(tp, throttleUntilTimeMs);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;
        public final boolean abortForNewBatch;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated, boolean abortForNewBatch) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.abortForNewBatch = abortForNewBatch;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
}
