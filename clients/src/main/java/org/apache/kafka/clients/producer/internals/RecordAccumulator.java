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
 * 用于累加客户端发送的消息，等到一个batch满了之后，再用prodcuer的sender线程进行统一的发送
 * 累加器使用了一个有限的内存空间，如果资源被耗尽，那么继续添加会引发阻塞，除非这个功能被明确地禁止
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
	/**
	 * key: partition信息
	 * value: 存储batch的双向队列
	 * 用于存储每个partition双向队列batch的并发容器
	 */
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
	 * 添加一条记录到记录累加器，返回追加结果
	 * 追加结果包括future元数据，是否batch已满，或者一个新的batch已经创建
	 *
	 * @param tp 记录要发送到的partition
	 * @param timestamp 记录的时间戳
	 * @param key 记录的key
	 * @param value 记录的value
	 * @param headers 记录的headers
	 * @param callback 请求完成时，用户提供的callback
	 * @param maxTimeToBlock 缓冲存储器可用的最大时间，以毫秒为单位进行阻塞
	 * @param abortOnNewBatch 用于证明是否需要在一个新的batch创建后再返回，再次追加时，使用partition的新batch
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     Callback callback,
                                     long maxTimeToBlock,
                                     boolean abortOnNewBatch) throws InterruptedException {
		// 累加器的计数器操作
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
		// 默认是一个空header
        if (headers == null) headers = Record.EMPTY_HEADERS;
		try {
			// 校验我们是已有一个在运行中的batch deque
            Deque<ProducerBatch> dq = getOrCreateDeque(tp);
			// 进行同步操作
            synchronized (dq) {
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
				// 尝试写入到batch中
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                if (appendResult != null)
                    return appendResult;
			}

			// 此时为没有追加record成功
			// 如果可以在创建一个新的batch时中断当前的追加操作
            if (abortOnNewBatch) {
				// 返回null结果，让其他调用继续追加
                return new RecordAppendResult(null, false, false, true);
			}
			// 获取producer最大的batch大小
            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
			// 估算消息的字节数大小
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
			// 申请一块用于追加当前record的对外内存
            buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // 获取batch队列锁之后，需要重新校验producer的状态
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
				// 再度尝试追加record
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                if (appendResult != null) {
                    // 此时可能是Sender线程将batch发送，多出一个空间
                    return appendResult;
                }
				// 此时仍没有可追加的空间
				// 则使用给定内存创建一个新的batch，并向新batch中追加record
                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, time.milliseconds());
                FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                        callback, time.milliseconds()));
				// 将batch添加到当前partition的batch队列中
                dq.addLast(batch);
				// 此batch为新batch，必定为未发送的batch
                incomplete.add(batch);

				// 由于在锁中，batch还在使用这块对外内存，所以此时不要进行对外内存的接触分配
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
	 *  尝试将record写入到ProducerBatch中
	 *  如果deque已经满了，我们会return null，并且创建一个新的batch，我们会关闭指定写入的deque，然后做一些压缩buffer等释放资源的操作
	 *  在以下情况下，deque会完全关闭：
	 *  1. 等待发送
	 *  2. 失效
	 *  3. producer已经关闭
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque) {
		// 获取最后一个ProducerBatch，最后一个是要写入的batch
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, time.milliseconds());
			// 如果返回null，证明最后一个deque已经写不下了
            if (future == null)
				// 关闭当前batch集合的写入
                last.closeForRecordAppends();
			else
				// 组装追加结果
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false);
        }
        return null;
    }

    private boolean isMuted(TopicPartition tp, long now) {
		// 要小心避免不必要的map查找，因为此方法会因为向大规模partition生产消息而产生热区域
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
	 * 将生产请求拒绝的大batch分割为小batch，并重新入队
     * @return 分割的小batch数量
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
		// 将估计压缩率重置为初始值或大batch的压缩率
		// 有几种不同的方法来做重置，我们选择了最保守的一个方法来确认分割操作不会经常发生
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression,
                                                Math.max(1.0f, (float) bigBatch.compressionRatio()));
		// 根据每个batch大小配置对batch进行分割
        Deque<ProducerBatch> dq = bigBatch.split(this.batchSize);
        int numSplitBatches = dq.size();
		// 获取partition的消息队列
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);
        while (!dq.isEmpty()) {
			// 以栈的方式，后进先出，放入到消息队列的头部
            ProducerBatch batch = dq.pollLast();
            incomplete.add(batch);
			// 我们将新分割出的子batch视为还没有发送过的batch
            synchronized (partitionDequeue) {
                if (transactionManager != null) {
					// 由于已经分配了sequence序号，所以我们不能打破之前的分配顺序
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
	 * 获取准备就绪的partition节点列表，以及最早的不可发送partition的准备就绪时间，并且返回累加的partitionbatch中，是否有不清楚首领partition节点
	 * 作为目的地的节点，知道满足以下情况才算准备就绪
	 * 至少有一个partition没有后退其发送，并且这些partition都不是未完成的（避免重新排序，如果{@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}设置为1）
	 * 并且满足如下的任一条件：
	 * 	1. 消息队列已经满了
	 * 	2. 消息队列已经放入到累加器中
	 * 	3. 累加器已经超出内存，线程正在阻塞等待空间（此时所有的partition立即会被确认为就绪状态）
	 * 	4. 累加器已经关闭
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();
		// 是否有等待获取内存的线程
        boolean exhausted = this.free.queued() > 0;
		// 遍历每个partition的消息队列
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
			// 对双向队列进行同步操作，避免还有追加的操作
			synchronized (deque) {
				// 当向比较多的partition生产消息时，当前调用是个热区域
				// 我们通常校验第一个batch，用于避免更昂贵代价的校验
				// 获取队列中第一个封装好的batch
                ProducerBatch batch = deque.peekFirst();
				if (batch != null) {
					// 获取partition信息
                    TopicPartition part = entry.getKey();
					// 从集群信息中获取当前partition的leader节点
                    Node leader = cluster.leaderFor(part);
					if (leader == null) {
						// 如果当前partition不存在leader节点，添加到未知leader节点的topic集合中
						// leader节点不存在的情况下，也是可以进行消息发送的
						// 需要注意的是，即使双向队列为空，也不会将元素从batch中移除
                        unknownLeaderTopics.add(part.topic());
                    } else if (!readyNodes.contains(leader) && !isMuted(part, nowMs)) {
						// 此时partition存在leader节点
						// 如果就绪节点集合中没有当前leader节点，并且当前partition不处于静默状态

						// 获取当前batch的已经等待的时间
                        long waitedTimeMs = batch.waitedTimeMs(nowMs);
						// 是否需要进行重试
                        boolean backingOff = batch.attempts() > 0 && waitedTimeMs < retryBackoffMs;
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
						// 判断batch是否处于满的状态，需要消息队列有至少两个batch
                        boolean full = deque.size() > 1 || batch.isFull();
						// 是否超过等待时间
                        boolean expired = waitedTimeMs >= timeToWaitMs;
						// 判断是否可发送
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
						// 如果可发送，且无需进行重试，则将此partition对应的leader节点置为准备就绪状态
                        if (sendable && !backingOff) {
							readyNodes.add(leader);
						} else {
							// 如果不可发送，或者需要进行重试
							// 计算剩余等待时间
                            long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
							// 注意：这是一个保守的估计结果
							// 因为一个不可发送数据的partition可能会有一个临界点，不久之后会发现拥有可发送的数据
							// 然而这已经足够好了，因为我们只是唤醒一下，然后就会继续sleep剩余的持续时间
							// 所以取剩余等待时间和下一次就绪状态检查时间的最小值
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
		// 获取当前节点的partition信息
		List<PartitionInfo> parts = cluster.partitionsForNode(node.id());

		List<ProducerBatch> ready = new ArrayList<>();
		// 为了减少竞争，循环不从索引0开始
		int start = drainIndex = drainIndex % parts.size();
		do {
			// 获取指定索引的partition
			PartitionInfo part = parts.get(drainIndex);
			// 创建topic-partition对象，用于存储关联信息
			TopicPartition tp = new TopicPartition(part.topic(), part.partition());
			// 计算下一个需要进行排空的索引
			this.drainIndex = (this.drainIndex + 1) % parts.size();

			// 如果当前partition请求处于静默状态，继续遍历下一个partition
			if (isMuted(tp, now))
				continue;
			// 获取当前partition的batch队列
			Deque<ProducerBatch> deque = getDeque(tp);
			if (deque == null)
				// 如果队列为空，继续遍历下一个partition
				continue;
			// 队列不为空的情况下，需要对队列进行同步操作
			synchronized (deque) {
				// 获取当前partition batch队列的第一个batch，但不取出
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
				// 如果大小总和已经超过当次发送总大小的阈值
				if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
					// 不需要继续发送了，跳出循环
					break;
				} else {
					// 如果没有超出发送总大小阈值
					// 首先对是否停止排空进行验证，主要是对事务状态进行校验
					if (shouldStopDrainBatchesForPartition(first, tp))
						break;
					// 判断当前partition是否处于事务状态
					boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
					// 生成Kafka集群需要用到的producer id和epoch版本
					ProducerIdAndEpoch producerIdAndEpoch =
							transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
					// 此时需要获取第一个batch
					ProducerBatch batch = deque.pollFirst();
					// 如果需要produce id以及epoch，同时batch不需要序列号
					if (producerIdAndEpoch != null && !batch.hasSequence()) {
						// 如果当前batch已经分配了一个序列号，我们就不要去修改produce id和序列号，因为可能会产生重复的情况
						// 尤其是在前一次存在可能被接受的情况，并且如果我们修改了producer id和序列号，本次尝试也可能会被接受，引发了重复
						// 除此之外，我们会更新为当前partition更新下一个序列号，同时也可以用事务管理器记录batch，这样即使在接收到顺序之外的请求，我们也能根据序列号继续确保维持序列号的顺序
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
	 * 从指定的partition中获取存储record的双向队列，如有必要，创建一个
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
		// 从缓存中获取
        Deque<ProducerBatch> d = this.batches.get(tp);
        if (d != null)
			return d;
		// 没有指定的partition的batch deque，创建一个新的
        d = new ArrayDeque<>();
		// 避免出现并发的情况，获取覆盖的值，如果其他线程已经创建了新deque，则使用此deque，否则使用当前线程创建的deque
        Deque<ProducerBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            return previous;
    }

    /**
	 * 回收batch的内存
	 * @param batch 指定batch
     */
    public void deallocate(ProducerBatch batch) {
		// 从未完成batch缓存中移除指定batch
        incomplete.remove(batch);
		// 如果不是拆分的batch，则取消分配的batch
		// 因为拆分的batch已经在buffer池外进行了分配
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
			// 获取到指定partition对应的分去
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
	 * record追加结果
	 * 通常代表的是将record发送到Kafka服务端后，服务端返回的结果
     */
    public final static class RecordAppendResult {
		// 存储的记录元数据
		public final FutureRecordMetadata future;
		// batch是否已满
		public final boolean batchIsFull;
		// 是否创建了新的batch
		public final boolean newBatchCreated;
		// 创建新的batch是否中断
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
