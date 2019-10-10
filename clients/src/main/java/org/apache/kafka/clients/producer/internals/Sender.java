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
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * 用于处理发送生产请求到Kafka集群的后台线程
 * 后台线程用于刷新cluster的视图信息，并且将生产请求发送到合适的节点
 */
public class Sender implements Runnable {

    private final Logger log;

	/**
	 * 连接节点的客户端
	 */
    private final KafkaClient client;

	/**
	 * 记录record的累加器
	 */
    private final RecordAccumulator accumulator;

	/**
	 <<<<<<< HEAD
	 * client的元数据
	 =======
	 * 连接client的元数据
	 >>>>>>> d2b03115f7683352eaef1621489e84ff96f79799
	 */
    private final ProducerMetadata metadata;

	/**
	 * 是否需要保证生产时的顺序
	 */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;

    /* metrics */
    private final SenderMetrics sensors;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeoutMs;

	/**
	 * 在失败的情况下，火鹤等待或尝试下一个请求的时间
	 */
    private final long retryBackoffMs;

    /* current request API versions supported by the known brokers */
    private final ApiVersions apiVersions;

	/**
	 * 和事务有关系的所有状态
	 * 尤其是producer id, producer epoch, sequence numbers
	 */
    private final TransactionManager transactionManager;

	// 针对每个分区的ProducerBatch列表
	// ProducerBatch列表是根据创建时间进行排序的
    private final Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    public Sender(LogContext logContext,
                  KafkaClient client,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager,
                  ApiVersions apiVersions) {
        this.log = logContext.logger(Sender.class);
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        this.inFlightBatches = new HashMap<>();
    }

    public List<ProducerBatch> inFlightBatches(TopicPartition tp) {
        return inFlightBatches.containsKey(tp) ? inFlightBatches.get(tp) : new ArrayList<>();
    }

    public void maybeRemoveFromInflightBatches(ProducerBatch batch) {
        List<ProducerBatch> batches = inFlightBatches.get(batch.topicPartition);
        if (batches != null) {
            batches.remove(batch);
            if (batches.isEmpty()) {
                inFlightBatches.remove(batch.topicPartition);
            }
		}
	}

	/**
	 *  获取已经发送超时的正在运行的batch集合
     */
    private List<ProducerBatch> getExpiredInflightBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();
		// 使用迭代器进行遍历
        for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext();) {
            Map.Entry<TopicPartition, List<ProducerBatch>> entry = batchIt.next();
            List<ProducerBatch> partitionInFlightBatches = entry.getValue();
            if (partitionInFlightBatches != null) {
				// 遍历每个partition的ProducerBatch
                Iterator<ProducerBatch> iter = partitionInFlightBatches.iterator();
                while (iter.hasNext()) {
                    ProducerBatch batch = iter.next();
					// 是否已经超过发送超时时长
                    if (batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now)) {
                        iter.remove();
						// 已经过期的batch，不能继续在正常队列中处理，需要调用#sendProducerData()方法进行处理
						// batch必须是未发送的
						// 如果是已发送的，将会抛出异常
                        if (!batch.isDone()) {
                            expiredBatches.add(batch);
                        } else {
                            throw new IllegalStateException(batch.topicPartition + " batch created at " +
                                batch.createdMs + " gets unexpected final state " + batch.finalState());
                        }
                    } else {
						// 由于是按时间顺序进行排序的，如果当前batch没有过期，证明此索引以后的batch也未过期
                        accumulator.maybeUpdateNextBatchExpiryTime(batch);
                        break;
					}
				}
				// 如果batch已经发送，移除当前的partition分类
                if (partitionInFlightBatches.isEmpty()) {
                    batchIt.remove();
                }
            }
        }
        return expiredBatches;
    }

    private void addToInflightBatches(List<ProducerBatch> batches) {
        for (ProducerBatch batch : batches) {
            List<ProducerBatch> inflightBatchList = inFlightBatches.get(batch.topicPartition);
            if (inflightBatchList == null) {
                inflightBatchList = new ArrayList<>();
                inFlightBatches.put(batch.topicPartition, inflightBatchList);
            }
            inflightBatchList.add(batch);
		}
	}

	/**
	 * 添加到运行中的batch
	 * @param batches 汇总的每个node的batch列表映射
	 */
	public void addToInflightBatches(Map<Integer, List<ProducerBatch>> batches) {
		for (List<ProducerBatch> batchList : batches.values()) {
			addToInflightBatches(batchList);
		}
	}

    private boolean hasPendingTransactionalRequests() {
        return transactionManager != null && transactionManager.hasPendingRequests() && transactionManager.hasOngoingTransaction();
    }

	/**
	 * sender线程核心工作线程
     */
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

		// 主要循环，循环到调用#close()方法
        while (running) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

		// 我们停止接收请求，但是可能还会有请求在事务管理器中、累加器中，或者等待ack
		// 当前发送线程将一直等待所有请求完成
        while (!forceClose && ((this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0) || hasPendingTransactionalRequests())) {
			// 所有请求完成之后，再执行一次
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // Abort the transaction if any commit or abort didn't go through the transaction manager's queue
		// 在任何提交或者中断，没有经过事务管理器的队列情况下，中断事务
        while (!forceClose && transactionManager != null && transactionManager.hasOngoingTransaction()) {
			// 事务管理器没有完成的情况下，进行中断
            if (!transactionManager.isCompleting()) {
                log.info("Aborting incomplete transaction due to shutdown");
                transactionManager.beginAbort();
			}
			try {
				// 再执行一次发送
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
			}
		}
		// 如果开启了强制关闭
        if (forceClose) {
			// 存在事务管理器，则关闭事务管理器
			// 此时，我们需要让所有未完成的事务请求或者batch失败，然后唤醒因为异步请求，现在仍然等待的获取结果的线程
            if (transactionManager != null) {
                log.debug("Aborting incomplete transactional requests due to forced shutdown");
                transactionManager.close();
            }
            log.debug("Aborting incomplete batches due to forced shutdown");
			// 中断没有完成的batch
            this.accumulator.abortIncompleteBatches();
        }
		try {
			// 关闭client
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
	}

	/**
	 * 执行一次发送迭代
     */
    void runOnce() {
		// 如果需要进行事务处理
        if (transactionManager != null) {
			try {
				// 在需要的情况下，重置producerId
                transactionManager.resetProducerIdIfNeeded();
				// 如果事务管理器没有处于事务状态
                if (!transactionManager.isTransactional()) {
					// 这是一个等幂的producer，确保要有producerId
                    maybeWaitForProducerId();
					// 如果事务管理已经解决了异常状态，并且事务管理器没有致命的错误
                } else if (transactionManager.hasUnresolvedSequences() && !transactionManager.hasFatalError()) {

                    transactionManager.transitionToFatalError(
                        new KafkaException("The client hasn't received acknowledgment for " +
                            "some previously sent messages and can no longer retry them. It isn't safe to continue."));
					// 判断是发送还是拉取事务消息
                } else if (maybeSendAndPollTransactionalRequest()) {
					return;
				}

				// 在事务管理器主语失败状态下，或者在等幂的案例情况下没有producer id，不继续发送
                if (transactionManager.hasFatalError() || !transactionManager.hasProducerId()) {
                    RuntimeException lastError = transactionManager.lastError();
					// 获取事务管理器最后一次发生的错误
                    if (lastError != null)
						// 可能会中止batch
                        maybeAbortBatches(lastError);
					// 客户端拉取信息
                    client.poll(retryBackoffMs, time.milliseconds());
					return;
					// 如果当前事务管理器有中断错误
                } else if (transactionManager.hasAbortableError()) {
					// record累加器中断还没有发送出去的batch
                    accumulator.abortUndrainedBatches(transactionManager.lastError());
                }
            } catch (AuthenticationException e) {
				// 已经以error的方式记录，但是传播到这里来进行一些清理
                log.trace("Authentication exception while processing transactional request: {}", e);
				// 事务管理器校验失败
                transactionManager.authenticationFailed(e);
			}
		}
		// 获取当前时间戳
        long currentTimeMs = time.milliseconds();

        long pollTimeout = sendProducerData(currentTimeMs);
        client.poll(pollTimeout, currentTimeMs);
	}

	/**
	 * 发送producer数据
	 * @param now 方法调用时间戳
	 * @return
	 */
	private long sendProducerData(long now) {
		// 从连接metadata中获取集群信息
		Cluster cluster = metadata.fetch();
		// 从累加器中获取处于准备发送状态的分区record累加器
		RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

		// 存在首领分区不清楚的情况下，强制更新metadata
		if (!result.unknownLeaderTopics.isEmpty()) {
			// 这个集合既包括了正在选举主节点的topic，也包括已经失效的topic，
			// 将它们继续添加到metadata中，以确保被metadata包含在内，然后进行一次metadata更新，因为有消息仍需要发送到这些topic上
			for (String topic : result.unknownLeaderTopics)
				this.metadata.add(topic);

			log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
					result.unknownLeaderTopics);
			// 进行一次metadata更新
			this.metadata.requestUpdate();
		}

		// 去除一些没有准备就绪的节点
		Iterator<Node> iter = result.readyNodes.iterator();
		long notReadyTimeout = Long.MAX_VALUE;
		while (iter.hasNext()) {
			Node node = iter.next();
			if (!this.client.ready(node, now)) {
				iter.remove();
				notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
			}
		}

		// 创建生产请求，汇总每个节点需要发送的batch列表
		Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(cluster, result.readyNodes, this.maxRequestSize, now);
		// 添加到运行中的batch集合中
		addToInflightBatches(batches);
		// 在需要保证消息顺序的情况下
		if (guaranteeMessageOrder) {
			// 关闭所有排空的partition
			for (List<ProducerBatch> batchList : batches.values()) {
				for (ProducerBatch batch : batchList)
					this.accumulator.mutePartition(batch.topicPartition);
			}
		}
		// 重置下一个batch的失效桑蝉爱你
		accumulator.resetNextBatchExpiryTime();
		// 获取已经发送超时的正在运行的batch集合
		List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
		// 获取当前累加器的所有失效的batch集合
		List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
		// 汇总两种情况的失效集合
		expiredBatches.addAll(expiredInflightBatches);

		// 如果一个失效的batch先前发送过record给broker，重置producer id，同时更新失效batch的统计数据
		// 请了解@TransactionState.resetProducerId来理解为什么我们在这里需要重置producer id
		if (!expiredBatches.isEmpty())
			log.trace("Expired {} batches in accumulator", expiredBatches.size());
		for (ProducerBatch expiredBatch : expiredBatches) {
			String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
					+ ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
			failBatch(expiredBatch, -1, NO_TIMESTAMP, new TimeoutException(errorMessage), false);
			if (transactionManager != null && expiredBatch.inRetry()) {
				// This ensures that no new batches are drained until the current in flight batches are fully resolved.
				transactionManager.markSequenceUnresolved(expiredBatch.topicPartition);
			}
		}
		sensors.updateProduceRequestMetrics(batches);

		// If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
		// loop and try sending more data. Otherwise, the timeout will be the smaller value between next batch expiry
		// time, and the delay time for checking data availability. Note that the nodes may have data that isn't yet
		// sendable due to lingering, backing off, etc. This specifically does not include nodes with sendable data
		// that aren't ready to send since they would cause busy looping.
		long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
		pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
		pollTimeout = Math.max(pollTimeout, 0);
		if (!result.readyNodes.isEmpty()) {
			log.trace("Nodes with data ready to send: {}", result.readyNodes);
			// if some partitions are already ready to be sent, the select time would be 0;
			// otherwise if some partition already has some data accumulated but not ready yet,
			// the select time will be the time difference between now and its linger expiry time;
			// otherwise the select time will be the time difference between now and the metadata expiry time;
			pollTimeout = 0;
		}
		sendProduceRequests(batches, now);
		return pollTimeout;
	}

	/**
	 * 如果一个事务request被发送或者获取走，或者一个FindCoordinator请求入队，返回true
     */
    private boolean maybeSendAndPollTransactionalRequest() {
		// 事务管理器有正在发送中的事务请求
        if (transactionManager.hasInFlightTransactionalRequest()) {
            // as long as there are outstanding transactional requests, we simply wait for them to return
			// 只要有未完成的事务请求，client就会一直等待#poll()返回
            client.poll(retryBackoffMs, time.milliseconds());
			return true;
		}
		// 如果事务管理器处于完成状态，并且计数器已经完成
        if (transactionManager.isCompleting() && accumulator.hasIncomplete()) {
			// 中断事务管理器
            if (transactionManager.isAborting())
                accumulator.abortUndrainedBatches(new KafkaException("Failing batch since transaction was aborted"));
			// 在充实之后，可能还会有请求遗留，我们无法得知追加到Broker日志的操作是成功还是失败，我们需要在最终状态确认的情况下，重新发送
			// 如果已经追加到Broker日志，但是没有收到指定的错误，我们的sequence number也是不正确的，我们可能会被引导至OutOfSequenceException异常
            if (!accumulator.flushInProgress())
                accumulator.beginFlush();
        }

        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequestHandler(accumulator.hasIncomplete());
        if (nextRequestHandler == null)
            return false;

        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        Node targetNode = null;
        try {
            targetNode = awaitNodeReady(nextRequestHandler.coordinatorType());
            if (targetNode == null) {
                lookupCoordinatorAndRetry(nextRequestHandler);
                return true;
            }

            if (nextRequestHandler.isRetry())
                time.sleep(nextRequestHandler.retryBackoffMs());
            long currentTimeMs = time.milliseconds();
            ClientRequest clientRequest = client.newClientRequest(
                targetNode.idString(), requestBuilder, currentTimeMs, true, requestTimeoutMs, nextRequestHandler);
            log.debug("Sending transactional request {} to node {}", requestBuilder, targetNode);
            client.send(clientRequest, currentTimeMs);
            transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        } catch (IOException e) {
            log.debug("Disconnect from {} while trying to send request {}. Going " +
                    "to back off and retry.", targetNode, requestBuilder, e);
            // We break here so that we pick up the FindCoordinator request immediately.
            lookupCoordinatorAndRetry(nextRequestHandler);
            return true;
        }
    }

    private void lookupCoordinatorAndRetry(TransactionManager.TxnRequestHandler nextRequestHandler) {
        if (nextRequestHandler.needsCoordinator()) {
            transactionManager.lookupCoordinator(nextRequestHandler);
        } else {
            // For non-coordinator requests, sleep here to prevent a tight loop when no node is available
            time.sleep(retryBackoffMs);
            metadata.requestUpdate();
        }

        transactionManager.retry(nextRequestHandler);
    }

    private void maybeAbortBatches(RuntimeException exception) {
		// 如果已经完成，则准备真正中断batch
        if (accumulator.hasIncomplete()) {
            log.error("Aborting producer batches due to fatal error", exception);
            accumulator.abortBatches(exception);
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    public boolean isRunning() {
        return running;
    }

    private ClientResponse sendAndAwaitInitProducerIdRequest(Node node) throws IOException {
        String nodeId = node.idString();
        InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                .setTransactionalId(null)
                .setTransactionTimeoutMs(Integer.MAX_VALUE);
        InitProducerIdRequest.Builder builder = new InitProducerIdRequest.Builder(requestData);
        ClientRequest request = client.newClientRequest(nodeId, builder, time.milliseconds(), true, requestTimeoutMs, null);
        return NetworkClientUtils.sendAndReceive(client, request, time);
    }

    private Node awaitNodeReady(FindCoordinatorRequest.CoordinatorType coordinatorType) throws IOException {
        Node node = coordinatorType != null ?
                transactionManager.coordinator(coordinatorType) :
                client.leastLoadedNode(time.milliseconds());

        if (node != null && NetworkClientUtils.awaitReady(client, node, time, requestTimeoutMs)) {
            return node;
        }
        return null;
    }

    private void maybeWaitForProducerId() {
        while (!forceClose && !transactionManager.hasProducerId() && !transactionManager.hasError()) {
            Node node = null;
            try {
                node = awaitNodeReady(null);
                if (node != null) {
                    ClientResponse response = sendAndAwaitInitProducerIdRequest(node);
                    InitProducerIdResponse initProducerIdResponse = (InitProducerIdResponse) response.responseBody();
                    Errors error = initProducerIdResponse.error();
                    if (error == Errors.NONE) {
                        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(
                                initProducerIdResponse.data.producerId(), initProducerIdResponse.data.producerEpoch());
                        transactionManager.setProducerIdAndEpoch(producerIdAndEpoch);
                        return;
                    } else if (error.exception() instanceof RetriableException) {
                        log.debug("Retriable error from InitProducerId response", error.message());
                    } else {
                        transactionManager.transitionToFatalError(error.exception());
                        break;
                    }
                } else {
                    log.debug("Could not find an available broker to send InitProducerIdRequest to. Will back off and retry.");
                }
            } catch (UnsupportedVersionException e) {
                transactionManager.transitionToFatalError(e);
                break;
            } catch (IOException e) {
                log.debug("Broker {} disconnected while awaiting InitProducerId response", node, e);
            }
            log.trace("Retry InitProducerIdRequest in {}ms.", retryBackoffMs);
            time.sleep(retryBackoffMs);
			metadata.requestUpdate();
		}
	}

	/**
	 * 构建一个生产请求的相应
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, long now) {
        RequestHeader requestHeader = response.requestHeader();
        long receivedTimeMs = response.receivedTimeMs();
        int correlationId = requestHeader.correlationId();
        if (response.wasDisconnected()) {
            log.trace("Cancelled request with header {} due to node {} being disconnected",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION), correlationId, now, 0L);
        } else if (response.versionMismatch() != null) {
            log.warn("Cancelled request {} due to a version mismatch with node {}",
                    response, response.destination(), response.versionMismatch());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now, 0L);
        } else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // if we have a response, parse it
            if (response.hasResponse()) {
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : produceResponse.responses().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    ProduceResponse.PartitionResponse partResp = entry.getValue();
                    ProducerBatch batch = batches.get(tp);
                    completeBatch(batch, partResp, correlationId, now, receivedTimeMs + produceResponse.throttleTimeMs());
                }
                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
            } else {
                // this is the acks = 0 case, just complete all requests
                for (ProducerBatch batch : batches.values()) {
                    completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now, 0L);
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch The record batch
     * @param response The produce response
     * @param correlationId The correlation id for the request
     * @param now The current POSIX timestamp in milliseconds
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
                               long now, long throttleUntilTimeMs) {
        Errors error = response.error;

        if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
                (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
            // If the batch is too large, we split the batch and send the split batches again. We do not decrement
            // the retry attempts in this case.
            log.warn(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlationId,
                batch.topicPartition,
                this.retries - batch.attempts(),
                error);
            if (transactionManager != null)
                transactionManager.removeInFlightBatch(batch);
            this.accumulator.splitAndReenqueue(batch);
            this.accumulator.deallocate(batch);
            this.sensors.recordBatchSplit();
        } else if (error != Errors.NONE) {
            if (canRetry(batch, response, now)) {
                log.warn(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts() - 1,
                    error);
                if (transactionManager == null) {
                    reenqueueBatch(batch, now);
                } else if (transactionManager.hasProducerIdAndEpoch(batch.producerId(), batch.producerEpoch())) {
                    // If idempotence is enabled only retry the request if the current producer id is the same as
                    // the producer id of the batch.
                    log.debug("Retrying batch to topic-partition {}. ProducerId: {}; Sequence number : {}",
                            batch.topicPartition, batch.producerId(), batch.baseSequence());
                    reenqueueBatch(batch, now);
                } else {
                    failBatch(batch, response, new OutOfOrderSequenceException("Attempted to retry sending a " +
                            "batch but the producer id changed from " + batch.producerId() + " to " +
                            transactionManager.producerIdAndEpoch().producerId + " in the mean time. This batch will be dropped."), false);
                }
            } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // If we have received a duplicate sequence error, it means that the sequence number has advanced beyond
                // the sequence of the current batch, and we haven't retained batch metadata on the broker to return
                // the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not return a valid offset and timestamp.
                completeBatch(batch, response);
            } else {
                final RuntimeException exception;
                if (error == Errors.TOPIC_AUTHORIZATION_FAILED)
                    exception = new TopicAuthorizationException(Collections.singleton(batch.topicPartition.topic()));
                else if (error == Errors.CLUSTER_AUTHORIZATION_FAILED)
                    exception = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
                else
                    exception = error.exception();
                // tell the user the result of their request. We only adjust sequence numbers if the batch didn't exhaust
                // its retries -- if it did, we don't know whether the sequence number was accepted or not, and
                // thus it is not safe to reassign the sequence.
                failBatch(batch, response, exception, batch.attempts() < this.retries);
            }
            if (error.exception() instanceof InvalidMetadataException) {
                if (error.exception() instanceof UnknownTopicOrPartitionException) {
                    log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                            "topic-partition may not exist or the user may not have Describe access to it",
                        batch.topicPartition);
                } else {
                    log.warn("Received invalid metadata error in produce request on partition {} due to {}. Going " +
                            "to request metadata update now", batch.topicPartition, error.exception().toString());
                }
                metadata.requestUpdate();
            }
        } else {
            completeBatch(batch, response);
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition, throttleUntilTimeMs);
    }

    private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
        this.accumulator.reenqueue(batch, currentTimeMs);
        maybeRemoveFromInflightBatches(batch);
        this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
    }

    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        if (transactionManager != null) {
            transactionManager.handleCompletedBatch(batch, response);
        }

        if (batch.done(response.baseOffset, response.logAppendTime, null)) {
            maybeRemoveFromInflightBatches(batch);
            this.accumulator.deallocate(batch);
        }
    }

    private void failBatch(ProducerBatch batch,
                           ProduceResponse.PartitionResponse response,
                           RuntimeException exception,
                           boolean adjustSequenceNumbers) {
        failBatch(batch, response.baseOffset, response.logAppendTime, exception, adjustSequenceNumbers);
    }

    private void failBatch(ProducerBatch batch,
                           long baseOffset,
                           long logAppendTime,
                           RuntimeException exception,
                           boolean adjustSequenceNumbers) {
        if (transactionManager != null) {
            transactionManager.handleFailedBatch(batch, exception, adjustSequenceNumbers);
        }

        this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);

        if (batch.done(baseOffset, logAppendTime, exception)) {
            maybeRemoveFromInflightBatches(batch);
            this.accumulator.deallocate(batch);
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed.
     * We can also retry OutOfOrderSequence exceptions for future batches, since if the first batch has failed, the
     * future batches are certain to fail with an OutOfOrderSequence exception.
     */
    private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response, long now) {
        return !batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now) &&
            batch.attempts() < this.retries &&
            !batch.isDone() &&
            ((response.error.exception() instanceof RetriableException) ||
                (transactionManager != null && transactionManager.canRetry(response, batch)));
	}

	/**
	 * 以每个节点为基础，将数据通过生产请求的媒介下，将数据传输过去
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
	}

	/**
	 * 根据给定的record batch集合，创建生产请求
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
		if (batches.isEmpty())
			return;
		// 分区以及分区需要传输的record集合
        Map<TopicPartition, MemoryRecords> produceRecordsByPartition = new HashMap<>(batches.size());
		// 分区以及分区传输的batch集合
        final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());

		// 获取版本号
        byte minUsedMagic = apiVersions.maxUsableProduceMagic();
        for (ProducerBatch batch : batches) {
			// 向后兼容
            if (batch.magic() < minUsedMagic)
                minUsedMagic = batch.magic();
		}
		// 遍历每个batch
        for (ProducerBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
			// 获取每个batch的record
            MemoryRecords records = batch.records();

			// 通常，在producer构建batch和发送写入请求之间，是允许出现一段延迟的，我们还可能会基于过时的metadata选择消息的格式
			// 在最坏的情况下，我们将会乐观的选择去用新的消息格式，但是发现broker并不支持，所以我们就需要向下转换，以便broker能支持
			// 向下转换已在处理在集群中中出现的边界case，因为broker可能在不同版本中并不支持同样的消息格式
			// 举个例子，如果一个partition从一个支持的版本，迁移到一个不支持的版本，我们就需要进行转换
            if (!records.hasMatchingMagic(minUsedMagic))
                records = batch.records().downConvert(minUsedMagic, 0, time).records();
			// 向两种集合中添加不同分类的待发送记录
            produceRecordsByPartition.put(tp, records);
            recordsByPartition.put(tp, batch);
		}
		// 获取transaction id
        String transactionalId = null;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
		}
		// 根据版本，构建request
        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forMagic(minUsedMagic, acks, timeout,
                produceRecordsByPartition, transactionalId);
		// 构建请求完成后的回调任务
		RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());

        String nodeId = Integer.toString(destination);
		// 创建请求
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,
                requestTimeoutMs, callback);
		// 发送请求，请求完成后，会执行回调
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    public static Sensor throttleTimeSensor(SenderMetricsRegistry metrics) {
        Sensor produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, new Avg());
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, new Max());
        return produceThrottleTimeSensor;
    }

    /**
     * A collection of sensors for the sender
     */
    private static class SenderMetrics {
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor batchSplitSensor;
        private final SenderMetricsRegistry metrics;
        private final Time time;

        public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
            this.metrics = metrics;
            this.time = time;

            this.batchSizeSensor = metrics.sensor("batch-size");
            this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
            this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
            this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
            this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
            this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

            this.errorSensor = metrics.sensor("errors");
            this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

            this.maxRecordSizeSensor = metrics.sensor("record-size");
            this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
            this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

            this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
            this.metrics.addMetric(metrics.metadataAge,
                (config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

            this.batchSplitSensor = metrics.sensor("batch-split-rate");
            this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
                MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
                topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                rateMetricName = this.metrics.topicByteRate(metricTags);
                totalMetricName = this.metrics.topicByteTotal(metricTags);
                topicByteRate.add(new Meter(rateMetricName, totalMetricName));

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                MetricName m = this.metrics.topicCompressionRate(metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
                totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
                topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
                totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
                topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
            long now = time.milliseconds();
            for (List<ProducerBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (ProducerBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Objects.requireNonNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Objects.requireNonNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.estimatedSizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Objects.requireNonNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRatio());

                    // global metrics
                    this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);
                    this.queueTimeSensor.record(batch.queueTimeMs(), now);
                    this.compressionRateSensor.record(batch.compressionRatio());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        void recordBatchSplit() {
            this.batchSplitSensor.record();
        }
    }

}
