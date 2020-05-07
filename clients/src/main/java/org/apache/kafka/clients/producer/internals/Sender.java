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
	 * 连接client的元数据
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

	/**
	 * Sender发送数据线程专属计数器
	 */
	private final SenderMetrics sensors;

	/* the max time to wait for the server to respond to the request*/
	private final int requestTimeoutMs;

	/**
	 * 在失败的情况下，等待或尝试下一个请求的时间
	 */
	private final long retryBackoffMs;

	/* current request API versions supported by the known brokers */
	private final ApiVersions apiVersions;

	/**
	 * 和事务有关系的所有状态
	 * 尤其是producer id, producer epoch, sequence numbers
	 */
	private final TransactionManager transactionManager;

	// 针对每个partition的ProducerBatch列表
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
		// 同Kafka服务端进行交互的客户端
		this.client = client;
		// 关联的RecordAccumulator
		this.accumulator = accumulator;
		// KafkaProducer的元数据
		this.metadata = metadata;
		// 是否需要保证消息的顺序性，与max.in.flight.requests.per.connection参数设置有关
		this.guaranteeMessageOrder = guaranteeMessageOrder;
		// 请求允许的最大字节数
		this.maxRequestSize = maxRequestSize;
		// 线程的运行状态
		this.running = true;
		// 集群副本的必须响应数量
		this.acks = acks;
		// 消息发送失败的重试次数
		this.retries = retries;
		// 创建时间戳
		this.time = time;
		// 发送线程计数器
		this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
		// 发送请求的等待时间
		this.requestTimeoutMs = requestTimeoutMs;
		// 发送失败后的等待时间
		this.retryBackoffMs = retryBackoffMs;
		// KafkaProducer的版本
		this.apiVersions = apiVersions;
		// 如果开启了，则需要传递事务管理器
		this.transactionManager = transactionManager;
		// 初始化Kafka集群未响应的batch集合
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
	 * 获取已经发送超时的正在运行的batch集合
	 */
	private List<ProducerBatch> getExpiredInflightBatches(long now) {
		List<ProducerBatch> expiredBatches = new ArrayList<>();
		// 使用迭代器进行遍历
		for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext(); ) {
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
					// 此时是一个幂等的producer，确保要有producerId
					maybeWaitForProducerId();
				} else if (transactionManager.hasUnresolvedSequences() && !transactionManager.hasFatalError()) {
					// 如果当前事务管理器中还有未发送完的sequence，并且事务管理器的状态不是FETAL_ERROR，则将事务管理器状态更新值FATAL_ERROR
					transactionManager.transitionToFatalError(
							new KafkaException("The client hasn't received acknowledgment for " +
									"some previously sent messages and can no longer retry them. It isn't safe to continue."));
				} else if (maybeSendAndPollTransactionalRequest()) {
					// 需要发送和拉取事务请求，则直接返回，等待下一次请求发送
					return;
				}

				// 在事务管理器状态为FATAL_ERROR，或者在开启幂等功能条件下没有producer id，则无法继续发送
				if (transactionManager.hasFatalError() || !transactionManager.hasProducerId()) {
					RuntimeException lastError = transactionManager.lastError();
					// 获取事务管理器最后一次发生的错误
					if (lastError != null)
						// 如果累加器还有未发送的batch，则中断这些batch的发送
						maybeAbortBatches(lastError);
					// 拉取Kafka集群返回的响应，并结束本次发送请求
					client.poll(retryBackoffMs, time.milliseconds());
					return;
				} else if (transactionManager.hasAbortableError()) {
					// 如果当前事务管理器有中断错误
					// 中断RecordAccumulator中还没有发送出去的batch
					accumulator.abortUndrainedBatches(transactionManager.lastError());
				}
			} catch (AuthenticationException e) {
				// 已经以error的方式记录，但是传播到这里来进行一些清理
				log.trace("Authentication exception while processing transactional request: {}", e);
				// 设置事务管理器校验失败及异常信息
				transactionManager.authenticationFailed(e);
			}
		}
		// 获取当前时间戳
		long currentTimeMs = time.milliseconds();
		// 发送生产数据，这里这是构建发送数据请求，然后将请求放入到Channel中，等待执行
		long pollTimeout = sendProducerData(currentTimeMs);
		// 真正的执行请求
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
		// 从累加器中获取处于准备发送状态的partitionrecord累加器
		RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

		// 存在topic leader节点未知的情况下，强制更新metadata
		if (!result.unknownLeaderTopics.isEmpty()) {
			// 这个集合既包括了正在选举leader节点的topic，也包括已经失效的topic，
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

		// 创建生产请求，汇总每个leader节点需要发送的batch列表
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
		// 重置下一个batch的失效时间
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
		// 遍历所有失效的batch，将它们置为失效
		for (ProducerBatch expiredBatch : expiredBatches) {
			String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
					+ ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
			// 对失效batch执行失败操作
			failBatch(expiredBatch, -1, NO_TIMESTAMP, new TimeoutException(errorMessage), false);
			// 在拥有事务管理器的情况下，并且失效的batch进入了重试
			if (transactionManager != null && expiredBatch.inRetry()) {
				// 标记一下batch还没有发送成功，不允许接着分配sequence序号
				transactionManager.markSequenceUnresolved(expiredBatch.topicPartition);
			}
		}
		// Sender发送数据线程专属计数器，统计发送的batch
		sensors.updateProduceRequestMetrics(batches);

		// 如果我们拥有一些节点，这些节点已经准备好发送或者拥有可发送的数据，那么调用poll()方法的时间为0，这样可以立即循环，并且尝试发送更多的数据
		// 其他情况下，poll()方法的超时时间将会是下一个batch的失效时间和检查数据可用性的延长时间，二者的最小值
		// 请注意，节点可能存在延迟，撤回等问题，导致无法发送数据
		// 这种特殊的情况不包括拥有可发送数据，但是还没有准备好发送的节点，这样可能导致频繁的循环
		long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
		pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
		// 等待时间必须大于0
		pollTimeout = Math.max(pollTimeout, 0);
		// 如果有准备就绪的节点，
		if (!result.readyNodes.isEmpty()) {
			log.trace("Nodes with data ready to send: {}", result.readyNodes);
			// 如果一些partition已经准备好发送数据了，select时间为0
			// 其它情况下，如果一些partition拥有累加的数据，但是还没有就绪，那么select时间为当前时间戳和过期时间之差
			// 在上述情况的其它情况下，select时间是当前时间戳和集群metadata过期时间之差
			pollTimeout = 0;
		}
		// 构建生产消息请求，返回获取结果的等待时间
		sendProduceRequests(batches, now);
		return pollTimeout;
	}

	/**
	 * 如果一个事务request被发送或者获取走，或者一个FindCoordinator请求入队，返回true
	 */
	private boolean maybeSendAndPollTransactionalRequest() {
		// 事务管理器有正在发送中的事务请求
		if (transactionManager.hasInFlightTransactionalRequest()) {
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
		// 如果累加器还有未发送的batch，则中断这些batch的发送
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
	 * 构建返回的生产响应的回调任务
	 */
	private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, long now) {
		RequestHeader requestHeader = response.requestHeader();
		long receivedTimeMs = response.receivedTimeMs();
		int correlationId = requestHeader.correlationId();
		if (response.wasDisconnected()) {
			// 响应返回的内容是网络连接断开，则以网络连接异常的失败的原因，取消batch的发送请求
			log.trace("Cancelled request with header {} due to node {} being disconnected",
					requestHeader, response.destination());
			for (ProducerBatch batch : batches.values())
				completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION), correlationId, now, 0L);
		} else if (response.versionMismatch() != null) {
			// 响应返回的内容是客户端版本不匹配，则以不支持的API版本原因，取消batch的发送请求
			log.warn("Cancelled request {} due to a version mismatch with node {}",
					response, response.destination(), response.versionMismatch());
			for (ProducerBatch batch : batches.values())
				completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now, 0L);
		} else {
			log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
			if (response.hasResponse()) {
				// 如果存在响应对象，则对响应对象进行解析
				ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
				// 响应中的信息维度是：topic-partition => partitionResponse
				for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : produceResponse.responses().entrySet()) {
					TopicPartition tp = entry.getKey();
					ProduceResponse.PartitionResponse partResp = entry.getValue();
					// 从回调任务传入的topic-partition=>batch列表集合中获取对应的batch列表，并完成每个batch
					ProducerBatch batch = batches.get(tp);
					completeBatch(batch, partResp, correlationId, now, receivedTimeMs + produceResponse.throttleTimeMs());
				}
				this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
			} else {
				// 此时属于acks=0的条件，完成所有请求即可
				for (ProducerBatch batch : batches.values()) {
					completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now, 0L);
				}
			}
		}
	}

	/**
	 * 完成或重试指定的batch
	 * @param batch         存储record的batch
	 * @param response      生产请求的相应
	 * @param correlationId 请求的唯一ID
	 * @param now           当前时间戳
	 */
	private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
							   long now, long throttleUntilTimeMs) {
		Errors error = response.error;
		// 如果返回的错误信息是消息过大，并且当前batch中有多条消息，并且当前batch并没有终结状态，并且消息版本为V2版本或消息已经压缩
		if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
				(batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
			// 如果batch过大，我们需要将batch分割为多个小batch，并重新进行发送
			// 在这种情况下，我们不会递减重试发送的次数
			log.warn(
					"Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
					correlationId,
					batch.topicPartition,
					this.retries - batch.attempts(),
					error);
			if (transactionManager != null)
				transactionManager.removeInFlightBatch(batch);
			// 将大batch分割为小batch，重新放入到消息队列中
			this.accumulator.splitAndReenqueue(batch);
			// 回收分配的内存
			this.accumulator.deallocate(batch);
			// 记录分割出的batch
			this.sensors.recordBatchSplit();
		} else if (error != Errors.NONE) {
			// 如果提交的offset超出了允许的offset范围
			if (canRetry(batch, response, now)) {
				// 允许重试的情况下，尝试重新发送batch
				log.warn(
						"Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
						correlationId,
						batch.topicPartition,
						this.retries - batch.attempts() - 1,
						error);
				if (transactionManager == null) {
					// 非事务情况下，重新入队，等待发送
					reenqueueBatch(batch, now);
				} else if (transactionManager.hasProducerIdAndEpoch(batch.producerId(), batch.producerEpoch())) {
					// 开启了幂等功能，只在当前producer id和batch的produce id相等的情况下，重新发送
					log.debug("Retrying batch to topic-partition {}. ProducerId: {}; Sequence number : {}",
							batch.topicPartition, batch.producerId(), batch.baseSequence());
					reenqueueBatch(batch, now);
				} else {
					// 其他情况下，不进行重新发送，直接失败
					failBatch(batch, response, new OutOfOrderSequenceException("Attempted to retry sending a " +
							"batch but the producer id changed from " + batch.producerId() + " to " +
							transactionManager.producerIdAndEpoch().producerId + " in the mean time. This batch will be dropped."), false);
				}
			} else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
				// 收到了重复sequence序号的请求
				// 此时意味着，sequence序号已经超过当前batch的序号，我们还没有保留broker上的batch元数据，来返回正确的offset和时间戳
				// 现在能做的唯一一件事情是返回success给用户，并不要返回一个合法的offset和的时间戳
				completeBatch(batch, response);
			} else {
				// 接下来将会使对失败响应的异常处理
				final RuntimeException exception;
				// 包装不同类型的异常，如topic身份验证异常，集群身份验证异常，以及其他异常
				if (error == Errors.TOPIC_AUTHORIZATION_FAILED)
					exception = new TopicAuthorizationException(Collections.singleton(batch.topicPartition.topic()));
				else if (error == Errors.CLUSTER_AUTHORIZATION_FAILED)
					exception = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
				else
					exception = error.exception();
				// 告诉开发者，请求的真实结果
				// 如果batch没有用完它们的重试机会，我们仅会对sequence序号进行调整
				// 如果batch用完了它们的重试机会，我们并不清楚序号是否被接受，而此时，重新分配sequence序号也是不安全的，索性直接返回发送失败
				failBatch(batch, response, exception, batch.attempts() < this.retries);
			}
			// 如果返回了非法的元数据信息，则需要重新更新元数据，之后不需要再处理，等待batch的重新发送
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
			// 此时，响应既不是batch过大，也不没有返回异常信息，视为生产请求成功，正常处理batch的响应
			completeBatch(batch, response);
		}

		// 在需要保证消息的顺序的情况下，由于已经处理完了partition的batch，则取消partition的静默状态，
		if (guaranteeMessageOrder)
			this.accumulator.unmutePartition(batch.topicPartition, throttleUntilTimeMs);
	}

	private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
		this.accumulator.reenqueue(batch, currentTimeMs);
		maybeRemoveFromInflightBatches(batch);
		this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
	}

	/**
	 * 完成指定batch的生产请求
	 * @param batch    指定batch
	 * @param response batch生产请求响应
	 */
	private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
		// 开启事务的情况下，首先处理事务管理器的当前batch
		if (transactionManager != null) {
			transactionManager.handleCompletedBatch(batch, response);
		}
		// 完成batch，同时移除正在发送缓存中的batch，并释放batch所使用的的内存空间
		if (batch.done(response.baseOffset, response.logAppendTime, null)) {
			maybeRemoveFromInflightBatches(batch);
			this.accumulator.deallocate(batch);
		}
	}

	/**
	 * batch发送失败
	 * @param batch 指定batch
	 * @param response 失败的相应内容
	 * @param exception 异常
	 * @param adjustSequenceNumbers
	 */
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
			// 开启事务的情况下，由事务管理器处理失败的batch
			transactionManager.handleFailedBatch(batch, exception, adjustSequenceNumbers);
		}
		// Sender线程记录发送失败信息
		this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);
		// 完成batch，同时移除正在发送缓存中的batch，并释放batch所使用的的内存空间
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
		// partition以及partition需要传输的batch集合
		Map<TopicPartition, MemoryRecords> produceRecordsByPartition = new HashMap<>(batches.size());
		// partition以及partition传输的batch集合
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
