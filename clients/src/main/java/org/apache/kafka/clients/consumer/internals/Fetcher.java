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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.MetadataCache;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * Broker信息拉取器
 * 是线程安全的
 * <p>
 * Requests and responses of Fetcher may be processed by different threads since heartbeat
 * thread may process responses. Other operations are single-threaded and invoked only from
 * the thread polling the consumer.
 * <ul>
 *     <li>If a response handler accesses any shared state of the Fetcher (e.g. FetchSessionHandler),
 *     all access to that state must be synchronized on the Fetcher instance.</li>
 *     <li>If a response handler accesses any shared state of the coordinator (e.g. SubscriptionState),
 *     it is assumed that all access to that state is synchronized on the coordinator instance by
 *     the caller.</li>
 *     <li>Responses that collate partial responses from multiple brokers (e.g. to list offsets) are
 *     synchronized on the response future.</li>
 *     <li>At most one request is pending for each node at any time. Nodes with pending requests are
 *     tracked and updated after processing the response. This ensures that any state (e.g. epoch)
 *     updated while processing responses on one thread are visible while creating the subsequent request
 *     on a different thread.</li>
 * </ul>
 */
public class Fetcher<K, V> implements Closeable {
	private final Logger log;
	private final LogContext logContext;
	private final ConsumerNetworkClient client;
	private final Time time;
	private final int minBytes;
	private final int maxBytes;
	private final int maxWaitMs;
	/**
	 * 拉取的数据大小
	 */
	private final int fetchSize;
	private final long retryBackoffMs;
	private final long requestTimeoutMs;
	private final int maxPollRecords;
	private final boolean checkCrcs;
	private final String clientRackId;
	private final ConsumerMetadata metadata;
	private final FetchManagerMetrics sensors;
	/**
	 * 当前消费者的订阅信息
	 */
	private final SubscriptionState subscriptions;
	/**
	 * 通过sendFetches()拉取到的partition records，用于进一步的分离
	 */
	private final ConcurrentLinkedQueue<PartitionRecords> completedFetches;
	private final BufferSupplier decompressionBufferSupplier = BufferSupplier.create();
	private final Deserializer<K> keyDeserializer;
	private final Deserializer<V> valueDeserializer;
	private final IsolationLevel isolationLevel;
	/**
	 * node-会话处理器集合缓存
	 */
	private final Map<Integer, FetchSessionHandler> sessionHandlers;
	private final AtomicReference<RuntimeException> cachedListOffsetsException = new AtomicReference<>();
	private final AtomicReference<RuntimeException> cachedOffsetForLeaderException = new AtomicReference<>();
	private final OffsetsForLeaderEpochClient offsetsForLeaderEpochClient;
	/**
	 * 在发送请求时，将node id置为挂起请求状态
	 */
	private final Set<Integer> nodesWithPendingFetchRequests;
	private final ApiVersions apiVersions;
	/**
	 * 下一个需要处理的partition records
	 */
	private PartitionRecords nextInLineRecords = null;

	public Fetcher(LogContext logContext,
				   ConsumerNetworkClient client,
				   int minBytes,
				   int maxBytes,
				   int maxWaitMs,
				   int fetchSize,
				   int maxPollRecords,
				   boolean checkCrcs,
				   String clientRackId,
				   Deserializer<K> keyDeserializer,
				   Deserializer<V> valueDeserializer,
				   ConsumerMetadata metadata,
				   SubscriptionState subscriptions,
				   Metrics metrics,
				   FetcherMetricsRegistry metricsRegistry,
				   Time time,
				   long retryBackoffMs,
				   long requestTimeoutMs,
				   IsolationLevel isolationLevel,
				   ApiVersions apiVersions) {
		this.log = logContext.logger(Fetcher.class);
		this.logContext = logContext;
		this.time = time;
		this.client = client;
		this.metadata = metadata;
		this.subscriptions = subscriptions;
		this.minBytes = minBytes;
		this.maxBytes = maxBytes;
		this.maxWaitMs = maxWaitMs;
		this.fetchSize = fetchSize;
		this.maxPollRecords = maxPollRecords;
		this.checkCrcs = checkCrcs;
		this.clientRackId = clientRackId;
		this.keyDeserializer = keyDeserializer;
		this.valueDeserializer = valueDeserializer;
		this.completedFetches = new ConcurrentLinkedQueue<>();
		this.sensors = new FetchManagerMetrics(metrics, metricsRegistry);
		this.retryBackoffMs = retryBackoffMs;
		this.requestTimeoutMs = requestTimeoutMs;
		this.isolationLevel = isolationLevel;
		this.apiVersions = apiVersions;
		this.sessionHandlers = new HashMap<>();
		this.offsetsForLeaderEpochClient = new OffsetsForLeaderEpochClient(client, logContext);
		this.nodesWithPendingFetchRequests = new HashSet<>();
	}

	public static Sensor throttleTimeSensor(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
		Sensor fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
		fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg), new Avg());

		fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax), new Max());

		return fetchThrottleTimeSensor;
	}

	/**
	 * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
	 * visibility for testing.
	 * @return true if there are completed fetches, false otherwise
	 */
	protected boolean hasCompletedFetches() {
		return !completedFetches.isEmpty();
	}

	/**
	 * 判断我们是否有可取的任何已经完成的fetch
	 * 这个方法是线程安全的
	 * @return 有已完成的fetch，返回true，其他情况，返回false
	 */
	public boolean hasAvailableFetches() {
		return completedFetches.stream().anyMatch(fetch -> subscriptions.isFetchable(fetch.partition));
	}

	/**
	 * 为那些已经分配分区，但是还没有运行中的fetch或者待处理的fetch数据的任何节点构建一个fetch请求
	 * 简单来说，就是请求broker拉取records
	 * @return 构建的fetch请求节点个数
	 */
	public synchronized int sendFetches() {
		// 更新计数器，防止分配发生变化
		sensors.maybeUpdateAssignment(subscriptions);
		// 准备拉取请求，节点-节点需要请求的数据
		Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();

		// 遍历每个节点需要请求的数据
		for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
			// 请求数据节点
			final Node fetchTarget = entry.getKey();
			// 请求数据
			final FetchSessionHandler.FetchRequestData data = entry.getValue();
			// 根据请求数据构建拉取请求
			final FetchRequest.Builder request = FetchRequest.Builder
					// 消费者
					.forConsumer(this.maxWaitMs, this.minBytes, data.toSend())
					// 事务等级
					.isolationLevel(isolationLevel)
					// 最大拉取字节数
					.setMaxBytes(this.maxBytes)
					// 元数据
					.metadata(data.metadata())
					// 需要忽略的分区
					.toForget(data.toForget())
					// 客户端标识ID
					.rackId(clientRackId);

			if (log.isDebugEnabled()) {
				log.debug("Sending {} {} to broker {}", isolationLevel, data.toString(), fetchTarget);
			}
			// ConsumerNetworkClient发送请求，并添加请求回调任务
			client.send(fetchTarget, request)
					.addListener(new RequestFutureListener<ClientResponse>() {
						/**
						 * 请求成功
						 * @param resp 响应
						 */
						@Override
						public void onSuccess(ClientResponse resp) {
							synchronized (Fetcher.this) {
								try {
									@SuppressWarnings("unchecked")
									// 获取响应body
											FetchResponse<Records> response = (FetchResponse<Records>) resp.responseBody();
									// 获取拉取会话处理器
									FetchSessionHandler handler = sessionHandler(fetchTarget.id());
									if (handler == null) {
										// 如果拉取会话处理器已经失效，处理响应，直接返回
										log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
												fetchTarget.id());
										return;
									}
									// 处理响应失败，主要是对session的状态分满载，增量进行判断
									if (!handler.handleResponse(response)) {
										return;
									}
									// 返回的响应数据也是根据topic-partition进行分布的，将topic-partition提取出来
									Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
									FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);
									// 遍历每个分区的及拉取的数据
									for (Map.Entry<TopicPartition, FetchResponse.PartitionData<Records>> entry : response.responseData().entrySet()) {
										TopicPartition partition = entry.getKey();
										// 获取请求的数据
										FetchRequest.PartitionData requestData = data.sessionPartitions().get(partition);
										if (requestData == null) {
											// 如果请求时的数据为空
											String message;
											// 构建错误消息内容
											if (data.metadata().isFull()) {
												// 如果是一次完整的拉取请求
												message = MessageFormatter.arrayFormat(
														"Response for missing full request partition: partition={}; metadata={}",
														new Object[]{partition, data.metadata()}).getMessage();
											} else {
												// 增量拉取请求
												message = MessageFormatter.arrayFormat(
														"Response for missing session request partition: partition={}; metadata={}; toSend={}; toForget={}",
														new Object[]{partition, data.metadata(), data.toSend(), data.toForget()}).getMessage();
											}

											// 收到了遗失的会话partition响应
											throw new IllegalStateException(message);
										} else {
											// 获取请求时拉取的offset
											long fetchOffset = requestData.fetchOffset;
											// 获取拉取到的分区数据
											FetchResponse.PartitionData<Records> fetchData = entry.getValue();

											log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
													isolationLevel, fetchOffset, partition, fetchData);
											// 构建当前处理的已完成fetch对象
											CompletedFetch completedFetch = new CompletedFetch(partition, fetchOffset,
													fetchData, metricAggregator, resp.requestHeader().apiVersion());
											// 添加到已拉取的fetch列表中
											completedFetches.add(parseCompletedFetch(completedFetch));
										}
									}
									// 计数器进行计数
									sensors.fetchLatency.record(resp.requestLatencyMs());
								} finally {
									// 移除当前node节点的挂起请求状态
									nodesWithPendingFetchRequests.remove(fetchTarget.id());
								}
							}
						}

						/**
						 * 响应失败
						 * @param e
						 */
						@Override
						public void onFailure(RuntimeException e) {
							// 请求失败，进行同步操作
							synchronized (Fetcher.this) {
								try {
									// 获取发送请求节点拉取会话处理器，用处理器处理错误
									FetchSessionHandler handler = sessionHandler(fetchTarget.id());
									if (handler != null) {
										handler.handleError(e);
									}
								} finally {
									// 同时也移除挂起请求的状态
									nodesWithPendingFetchRequests.remove(fetchTarget.id());
								}
							}
						}
					});
			// 添加节点挂起请求状态
			this.nodesWithPendingFetchRequests.add(entry.getKey().id());
		}
		// 返回参与请求的节点数量
		return fetchRequestMap.size();
	}

	/**
	 * Get topic metadata for all topics in the cluster
	 * @param timer Timer bounding how long this method can block
	 * @return The map of topics with their partition information
	 */
	public Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer) {
		return getTopicMetadata(MetadataRequest.Builder.allTopics(), timer);
	}

	/**
	 * Get metadata for all topics present in Kafka cluster
	 * @param request The MetadataRequest to send
	 * @param timer   Timer bounding how long this method can block
	 * @return The map of topics with their partition information
	 */
	public Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, Timer timer) {
		// Save the round trip if no topics are requested.
		if (!request.isAllTopics() && request.emptyTopicList())
			return Collections.emptyMap();

		do {
			RequestFuture<ClientResponse> future = sendMetadataRequest(request);
			client.poll(future, timer);

			if (future.failed() && !future.isRetriable())
				throw future.exception();

			if (future.succeeded()) {
				MetadataResponse response = (MetadataResponse) future.value().responseBody();
				Cluster cluster = response.cluster();

				Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
				if (!unauthorizedTopics.isEmpty())
					throw new TopicAuthorizationException(unauthorizedTopics);

				boolean shouldRetry = false;
				Map<String, Errors> errors = response.errors();
				if (!errors.isEmpty()) {
					// if there were errors, we need to check whether they were fatal or whether
					// we should just retry

					log.debug("Topic metadata fetch included errors: {}", errors);

					for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
						String topic = errorEntry.getKey();
						Errors error = errorEntry.getValue();

						if (error == Errors.INVALID_TOPIC_EXCEPTION)
							throw new InvalidTopicException("Topic '" + topic + "' is invalid");
						else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
							// if a requested topic is unknown, we just continue and let it be absent
							// in the returned map
							continue;
						else if (error.exception() instanceof RetriableException)
							shouldRetry = true;
						else
							throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
									error.exception());
					}
				}

				if (!shouldRetry) {
					HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
					for (String topic : cluster.topics())
						topicsPartitionInfos.put(topic, cluster.partitionsForTopic(topic));
					return topicsPartitionInfos;
				}
			}

			timer.sleep(retryBackoffMs);
		} while (timer.notExpired());

		throw new TimeoutException("Timeout expired while fetching topic metadata");
	}

	/**
	 * Send Metadata Request to least loaded node in Kafka cluster asynchronously
	 * @return A future that indicates result of sent metadata request
	 */
	private RequestFuture<ClientResponse> sendMetadataRequest(MetadataRequest.Builder request) {
		final Node node = client.leastLoadedNode();
		if (node == null)
			return RequestFuture.noBrokersAvailable();
		else
			return client.send(node, request);
	}

	/**
	 * 计算重置策略时间戳
	 * @param partition 指定partition
	 * @return 重置策略时间戳
	 */
	private Long offsetResetStrategyTimestamp(final TopicPartition partition) {
		// 获取指定partition的重置策略
		OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
		// 根据不同的策略，返回相应的时间戳
		if (strategy == OffsetResetStrategy.EARLIEST)
			// 最早重置策略
			return ListOffsetRequest.EARLIEST_TIMESTAMP;
		else if (strategy == OffsetResetStrategy.LATEST)
			// 最近重置策略
			return ListOffsetRequest.LATEST_TIMESTAMP;
		else
			return null;
	}

	private OffsetResetStrategy timestampToOffsetResetStrategy(long timestamp) {
		if (timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
			return OffsetResetStrategy.EARLIEST;
		else if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP)
			return OffsetResetStrategy.LATEST;
		else
			return null;
	}

	/**
	 * 重置需要的已分配的partition的offset
	 * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException 如果没有重置策略，或者至少一个partition没有在等待seekToBeginning()或者seekToEnd()
	 */
	public void resetOffsetsIfNeeded() {
		// 抛出上一次拉取offset产生的错误
		RuntimeException exception = cachedListOffsetsException.getAndSet(null);
		if (exception != null)
			throw exception;
		// 构建需要重置offset的集合
		Set<TopicPartition> partitions = subscriptions.partitionsNeedingReset(time.milliseconds());
		if (partitions.isEmpty())
			return;

		final Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<>();
		for (final TopicPartition partition : partitions) {
			// 获取重置策略时间戳
			Long timestamp = offsetResetStrategyTimestamp(partition);
			if (timestamp != null)
				offsetResetTimestamps.put(partition, timestamp);
		}

		resetOffsetsAsync(offsetResetTimestamps);
	}

	/**
	 * 如果检测到发生了leader节点变更，需要校验已分配的所有offset
	 */
	public void validateOffsetsIfNeeded() {
		// 出现了leader异常，直接抛出leader异常
		RuntimeException exception = cachedOffsetForLeaderException.getAndSet(null);
		if (exception != null)
			throw exception;

		// 通过当前的leader和leader epoch，校验每个分区的信息
		subscriptions.assignedPartitions().forEach(topicPartition -> {
			ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.leaderAndEpoch(topicPartition);
			// 判断当前partition是否需要进行校验
			subscriptions.maybeValidatePositionForCurrentLeader(topicPartition, leaderAndEpoch);
		});

		// 收集以partition维度，需要校验的position，并设置失败重试时间间隔
		Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate = subscriptions
				.partitionsNeedingValidation(time.milliseconds())
				.stream()
				.collect(Collectors.toMap(Function.identity(), subscriptions::position));
		// 异步校验offset
		validateOffsetsAsync(partitionsToValidate);
	}

	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
																   Timer timer) {
		metadata.addTransientTopics(topicsForPartitions(timestampsToSearch.keySet()));

		try {
			Map<TopicPartition, ListOffsetData> fetchedOffsets = fetchOffsetsByTimes(timestampsToSearch,
					timer, true).fetchedOffsets;

			HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(timestampsToSearch.size());
			for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
				offsetsByTimes.put(entry.getKey(), null);

			for (Map.Entry<TopicPartition, ListOffsetData> entry : fetchedOffsets.entrySet()) {
				// 'entry.getValue().timestamp' will not be null since we are guaranteed
				// to work with a v1 (or later) ListOffset request
				ListOffsetData offsetData = entry.getValue();
				offsetsByTimes.put(entry.getKey(), new OffsetAndTimestamp(offsetData.offset, offsetData.timestamp,
						offsetData.leaderEpoch));
			}

			return offsetsByTimes;
		} finally {
			metadata.clearTransientTopics();
		}
	}

	private ListOffsetResult fetchOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
												 Timer timer,
												 boolean requireTimestamps) {
		ListOffsetResult result = new ListOffsetResult();
		if (timestampsToSearch.isEmpty())
			return result;

		Map<TopicPartition, Long> remainingToSearch = new HashMap<>(timestampsToSearch);
		do {
			RequestFuture<ListOffsetResult> future = sendListOffsetsRequests(remainingToSearch, requireTimestamps);
			client.poll(future, timer);

			if (!future.isDone())
				break;

			if (future.succeeded()) {
				ListOffsetResult value = future.value();
				result.fetchedOffsets.putAll(value.fetchedOffsets);
				if (value.partitionsToRetry.isEmpty())
					return result;

				remainingToSearch.keySet().retainAll(value.partitionsToRetry);
			} else if (!future.isRetriable()) {
				throw future.exception();
			}

			if (metadata.updateRequested())
				client.awaitMetadataUpdate(timer);
			else
				timer.sleep(retryBackoffMs);
		} while (timer.notExpired());

		throw new TimeoutException("Failed to get offsets by times in " + timer.elapsedMs() + "ms");
	}

	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Timer timer) {
		return beginningOrEndOffset(partitions, ListOffsetRequest.EARLIEST_TIMESTAMP, timer);
	}

	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Timer timer) {
		return beginningOrEndOffset(partitions, ListOffsetRequest.LATEST_TIMESTAMP, timer);
	}

	private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
														   long timestamp,
														   Timer timer) {
		metadata.addTransientTopics(topicsForPartitions(partitions));
		try {
			Map<TopicPartition, Long> timestampsToSearch = partitions.stream()
					.collect(Collectors.toMap(Function.identity(), tp -> timestamp));

			ListOffsetResult result = fetchOffsetsByTimes(timestampsToSearch, timer, false);

			return result.fetchedOffsets.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset));
		} finally {
			metadata.clearTransientTopics();
		}
	}

	/**
	 * 返回取到的record，清除record buffer，并更新已消费的位置
	 * 注意：返回空的record集合，保证了已消费位置没有被更新
	 * @return 分区维度取到的record
	 * @throws OffsetOutOfRangeException   FetchResponse返回OffsetOutOfRange，并且默认的重置策略为空
	 * @throws TopicAuthorizationException FetchResponse返回TopicAuthorization错误
	 */
	public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
		Map<TopicPartition, List<ConsumerRecord<K, V>>> fetched = new HashMap<>();
		Queue<PartitionRecords> pausedCompletedFetches = new ArrayDeque<>();
		// 通过max.poll.records设置的最大轮询数量
		int recordsRemaining = maxPollRecords;

		try {
			while (recordsRemaining > 0) {
				// 如果下一个需要处理的partition records为null或者状态为已取
				if (nextInLineRecords == null || nextInLineRecords.isFetched) {
					// 继续从已完成的fetch队列中获取
					PartitionRecords records = completedFetches.peek();
					if (records == null) break;
					// 如果records还没有实例化
					if (records.notInitialized()) {
						try {
							// 进行实例化，并赋值给下一个需要处理的records
							nextInLineRecords = initializePartitionRecords(records);
						} catch (Exception e) {
							// 在以下的情况下，如果在解析过程中出现了异常，我们就需要移除这个completedFetch
							// 1. 它没有包含任何records
							// 2. 在此异常之前，没有获取到任何实际的record
							// 第一个条件确认了completedFetches集合不会相同被相同TopicAuthorizationException异常completedFetch阻塞住
							// 第二个条件确认了没有由于后续record的异常而导致前面部分的record丢失
							FetchResponse.PartitionData partition = records.completedFetch.partitionData;
							if (fetched.isEmpty() && (partition.records == null || partition.records.sizeInBytes() == 0)) {
								completedFetches.poll();
							}
							throw e;
						}
					} else {
						// 已经实例化的records直接赋值给下一个需要处理的records
						nextInLineRecords = records;
					}
					// 继续等待获取已完成的fetch
					completedFetches.poll();
				} else if (subscriptions.isPaused(nextInLineRecords.partition)) {
					// 如果开发者暂停了当前分区，我们可以将records添加到completedFetches队列中来代替排空records
					// 以便于他们可以在partition恢复后，随后的poll()方法中返回
					log.debug("Skipping fetching records for assigned partition {} because it is paused", nextInLineRecords.partition);
					// 等待恢复的completedFetches中添加当前处理的records
					pausedCompletedFetches.add(nextInLineRecords);
					nextInLineRecords = null;
				} else {
					// nextInLineRecords处于正常需要处理的状态
					// 转换拉取到的数据
					List<ConsumerRecord<K, V>> records = fetchRecords(nextInLineRecords, recordsRemaining);
					// 如果需要处理拉取到的数据
					if (!records.isEmpty()) {
						// 获取当前的topic-partition信息
						TopicPartition partition = nextInLineRecords.partition;
						// 检查当前topic-partition是否还有没有处理完成的record
						List<ConsumerRecord<K, V>> currentRecords = fetched.get(partition);
						// 如果当前topic-partition没有需要处理的record，处理本次拉取到的record
						if (currentRecords == null) {
							fetched.put(partition, records);
						} else {
							// 当前topic-partition存在没有需要处理的record
							// 这种情况不经常发生，因为我们在同一时间对每个分区只会发送一个fetch请求
							// 但是它却能让人信服地发生在一些罕见的场合，比如partition的leader节点发生了变化
							// 我们必须拷贝到一个新的list，因为旧的list可能是不可变的
							// 创建一个新的用于存放record的集合
							List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(records.size() + currentRecords.size());
							newRecords.addAll(currentRecords);
							newRecords.addAll(records);
							// 新、旧需要处理的record全部添加进去，并覆盖已有的信息
							fetched.put(partition, newRecords);
						}
						// 递减需要处理的record，进行下一个PartitionRecords的处理
						recordsRemaining -= records.size();
					}
				}
			}
		} catch (KafkaException e) {
			if (fetched.isEmpty())
				throw e;
		} finally {
			// 将轮询到的已完成的，但是因为种种原因partition处于暂停状态的fetch，重新放回completedFetches中，等待在下一次轮询中重新进行评估
			completedFetches.addAll(pausedCompletedFetches);
		}

		return fetched;
	}

	/**
	 * 从已经拉取到的records中处理Record，并返回供消费者处理的record
	 * @param partitionRecords 当前正在处理的分区record
	 * @param maxRecords       最大拉取数量
	 * @return 供消费者处理的record集合
	 */
	private List<ConsumerRecord<K, V>> fetchRecords(PartitionRecords partitionRecords, int maxRecords) {
		// 校验partition状态是否为已分配
		if (!subscriptions.isAssigned(partitionRecords.partition)) {
			// 可能会发生在拉取的records返回之前触发了一次分区再平衡
			log.debug("Not returning fetched records for partition {} since it is no longer assigned",
					partitionRecords.partition);
		} else if (!subscriptions.isFetchable(partitionRecords.partition)) {
			// 校验partition状态是否可取
			// 可能发生于一个partition在拉取records之前暂停，或者offset正在重置
			log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable",
					partitionRecords.partition);
		} else {
			// 正常的拉取操作
			// 获取当前分区的拉取位置信息
			SubscriptionState.FetchPosition position = subscriptions.position(partitionRecords.partition);
			// 如果下一次获取到的fetch offset等于当前consumer存储的offset
			// partitionRecords中的nextFetchOffset是在收到成功响应时，请求数据的值
			// position.offset是broker提供的offset
			if (partitionRecords.nextFetchOffset == position.offset) {
				// 获取指定数目的records
				List<ConsumerRecord<K, V>> partRecords = partitionRecords.fetchRecords(maxRecords);
				// 如果请求时的offset大于订阅列表已有的offset
				if (partitionRecords.nextFetchOffset > position.offset) {
					// 设置新的读取offset位置，覆盖订阅列表中的记录的offset
					SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
							partitionRecords.nextFetchOffset,
							partitionRecords.lastEpoch,
							position.currentLeader);
					log.trace("Returning fetched records at offset {} for assigned partition {} and update " +
							"position to {}", position, partitionRecords.partition, nextPosition);
					// 更新订阅的position位置
					subscriptions.position(partitionRecords.partition, nextPosition);
				}
				//
				Long partitionLag = subscriptions.partitionLag(partitionRecords.partition, isolationLevel);
				if (partitionLag != null)
					// 存在消息积压的情况，使用计数器记录消息积压情况
					this.sensors.recordPartitionLag(partitionRecords.partition, partitionLag);
				// 获取当前consumer已经消费的offset
				Long lead = subscriptions.partitionLead(partitionRecords.partition);
				// 记录当前consumer已经消费的offset
				if (lead != null) {
					this.sensors.recordPartitionLead(partitionRecords.partition, lead);
				}
				// 返回已经解析完成的ConsumerRecord集合
				return partRecords;
			} else {
				// 这些records并不是基于上一次已消费的位置来进行处理，忽略它们
				// 这些records一定是来自过时的请求
				log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
						partitionRecords.partition, partitionRecords.nextFetchOffset, position);
			}
		}
		// 走到这里，证明命中了上面的异常情况，不处理当前拉取到的PartitionRecord，返回空ConsumerRecord
		log.trace("Draining fetched records for partition {}", partitionRecords.partition);
		partitionRecords.drain();

		return emptyList();
	}

	/**
	 * 重置offset
	 * @param partition 需要重置offset的partition
	 * @param requestedResetStrategy 请求重置时的策略
	 * @param offsetData 进行重置的数据
	 */
	private void resetOffsetIfNeeded(TopicPartition partition, OffsetResetStrategy requestedResetStrategy, ListOffsetData offsetData) {
		// 根据新的offset和leader epoch构建最新的position
		SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
				offsetData.offset, offsetData.leaderEpoch, metadata.leaderAndEpoch(partition));
		// 更新拉取的最新的leader epoch数据
		offsetData.leaderEpoch.ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(partition, epoch));
		// 手动修改offset
		subscriptions.maybeSeekUnvalidated(partition, position.offset, requestedResetStrategy);
	}

	/**
	 * 异步重置offset
	 * @param partitionResetTimestamps partition重置集合
	 */
	private void resetOffsetsAsync(Map<TopicPartition, Long> partitionResetTimestamps) {
		// 将以partition维度的重置，转换为node维度的
		Map<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> timestampsToSearchByNode =
				groupListOffsetRequests(partitionResetTimestamps, new HashSet<>());
		// 遍历每个node
		for (Map.Entry<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> entry : timestampsToSearchByNode.entrySet()) {
			Node node = entry.getKey();
			// 获取每个node的partition-重置时间戳集合
			final Map<TopicPartition, ListOffsetRequest.PartitionData> resetTimestamps = entry.getValue();
			// 设置下一次该node下partition重试时间
			subscriptions.setNextAllowedRetry(resetTimestamps.keySet(), time.milliseconds() + requestTimeoutMs);
			// 发送重置请求，并添加响应listener
			RequestFuture<ListOffsetResult> future = sendListOffsetRequest(node, resetTimestamps, false);
			// 发送请求后，通过回调任务，已经将合法的offset封装进future中，此时将会处理future任务
			future.addListener(new RequestFutureListener<ListOffsetResult>() {
				/**
				 * future完成回调成功任务
				 * @param result
				 */
				@Override
				public void onSuccess(ListOffsetResult result) {
					// 处理需要重试重置的partition
					if (!result.partitionsToRetry.isEmpty()) {
						subscriptions.requestFailed(result.partitionsToRetry, time.milliseconds() + retryBackoffMs);
						// 更新metadata
						metadata.requestUpdate();
					}
					// 遍历每个partition最新的offset数据，进行重置
					for (Map.Entry<TopicPartition, ListOffsetData> fetchedOffset : result.fetchedOffsets.entrySet()) {
						TopicPartition partition = fetchedOffset.getKey();
						ListOffsetData offsetData = fetchedOffset.getValue();
						ListOffsetRequest.PartitionData requestedReset = resetTimestamps.get(partition);
						resetOffsetIfNeeded(partition, timestampToOffsetResetStrategy(requestedReset.timestamp), offsetData);
					}
				}

				@Override
				public void onFailure(RuntimeException e) {
					subscriptions.requestFailed(resetTimestamps.keySet(), time.milliseconds() + retryBackoffMs);
					metadata.requestUpdate();

					if (!(e instanceof RetriableException) && !cachedListOffsetsException.compareAndSet(null, e))
						log.error("Discarding error in ListOffsetResponse because another error is pending", e);
				}
			});
		}
	}

	private boolean hasUsableOffsetForLeaderEpochVersion(NodeApiVersions nodeApiVersions) {
		ApiVersionsResponse.ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
		if (apiVersion == null)
			return false;

		return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion);
	}

	/**
	 * 对于每一个需要校验的partition，创建一个异步请求，来获得每个partition的尾端点的offset
	 * 只有在leader epoch小于等于上一次更新的epoch的情况下，才会请求
	 *
	 * 以节点的方式进行请求，可以提高效率
	 */
	private void validateOffsetsAsync(Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate) {
		// 将以partition为维度的更新信息，转换成以node为维度的更新信息
		final Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regrouped =
				regroupFetchPositionsByLeader(partitionsToValidate);

		regrouped.forEach((node, fetchPostitions) -> {
			// 遍历每个节点，构建请求
			if (node.isEmpty()) {
				metadata.requestUpdate();
				return;
			}

			NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
			if (nodeApiVersions == null) {
				client.tryConnect(node);
				return;
			}
			// 如果在当前的leader epoch版本下，没有可用的offset，不请求更新
			if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
				log.debug("Skipping validation of fetch offsets for partitions {} since the broker does not " +
								"support the required protocol version (introduced in Kafka 2.3)",
						fetchPostitions.keySet());
				for (TopicPartition partition : fetchPostitions.keySet()) {
					// 修改拉取状态
					subscriptions.completeValidation(partition);
				}
				return;
			}
			// 设置下一次可进行重试的时间戳
			subscriptions.setNextAllowedRetry(fetchPostitions.keySet(), time.milliseconds() + requestTimeoutMs);
			// 发送异步请求，并添加响应事件的listener
			RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future = offsetsForLeaderEpochClient.sendAsyncRequest(node, partitionsToValidate);
			future.addListener(new RequestFutureListener<OffsetsForLeaderEpochClient.OffsetForEpochResult>() {
				/**
				 * 异步获取更新的offset成功
				 * @param offsetsResult 更新offset结果
				 */
				@Override
				public void onSuccess(OffsetsForLeaderEpochClient.OffsetForEpochResult offsetsResult) {
					Map<TopicPartition, OffsetAndMetadata> truncationWithoutResetPolicy = new HashMap<>();
					// 处理需要重试的partition
					if (!offsetsResult.partitionsToRetry().isEmpty()) {
						subscriptions.setNextAllowedRetry(offsetsResult.partitionsToRetry(), time.milliseconds() + retryBackoffMs);
						metadata.requestUpdate();
					}

					// 对于每一个OffsetsForLeader响应，首先校验传回来的末尾offset是否小于当前consumer存储的offset
					// 如果小于，意味着broker经历了日志文件截断，我们需要重新定位offset
					offsetsResult.endOffsets().forEach((respTopicPartition, respEndOffset) -> {
						SubscriptionState.FetchPosition requestPosition = fetchPostitions.get(respTopicPartition);
						Optional<OffsetAndMetadata> divergentOffsetOpt = subscriptions.maybeCompleteValidation(
								respTopicPartition, requestPosition, respEndOffset);
						// 需要重新处理的offset
						divergentOffsetOpt.ifPresent(divergentOffset -> {
							truncationWithoutResetPolicy.put(respTopicPartition, divergentOffset);
						});
					});
					// 如果存在需要继续处理的offset，则抛出异常
					if (!truncationWithoutResetPolicy.isEmpty()) {
						throw new LogTruncationException(truncationWithoutResetPolicy);
					}
				}

				/**
				 * 异步获取更新的offset失败
				 * @param e
				 */
				@Override
				public void onFailure(RuntimeException e) {
					// 更新失败，计算下一次重试的时间
					subscriptions.requestFailed(fetchPostitions.keySet(), time.milliseconds() + retryBackoffMs);
					// 更新拉取器的metadata
					metadata.requestUpdate();

					if (!(e instanceof RetriableException) && !cachedOffsetForLeaderException.compareAndSet(null, e)) {
						log.error("Discarding error in OffsetsForLeaderEpoch because another error is pending", e);
					}
				}
			});
		});
	}

	/**
	 * Search the offsets by target times for the specified partitions.
	 * @param timestampsToSearch the mapping between partitions and target time
	 * @param requireTimestamps  true if we should fail with an UnsupportedVersionException if the broker does
	 *                           not support fetching precise timestamps for offsets
	 * @return A response which can be polled to obtain the corresponding timestamps and offsets.
	 */
	private RequestFuture<ListOffsetResult> sendListOffsetsRequests(final Map<TopicPartition, Long> timestampsToSearch,
																	final boolean requireTimestamps) {
		final Set<TopicPartition> partitionsToRetry = new HashSet<>();
		Map<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> timestampsToSearchByNode =
				groupListOffsetRequests(timestampsToSearch, partitionsToRetry);
		if (timestampsToSearchByNode.isEmpty())
			return RequestFuture.failure(new StaleMetadataException());

		final RequestFuture<ListOffsetResult> listOffsetRequestsFuture = new RequestFuture<>();
		final Map<TopicPartition, ListOffsetData> fetchedTimestampOffsets = new HashMap<>();
		final AtomicInteger remainingResponses = new AtomicInteger(timestampsToSearchByNode.size());

		for (Map.Entry<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> entry : timestampsToSearchByNode.entrySet()) {
			RequestFuture<ListOffsetResult> future =
					sendListOffsetRequest(entry.getKey(), entry.getValue(), requireTimestamps);
			future.addListener(new RequestFutureListener<ListOffsetResult>() {
				@Override
				public void onSuccess(ListOffsetResult partialResult) {
					synchronized (listOffsetRequestsFuture) {
						fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
						partitionsToRetry.addAll(partialResult.partitionsToRetry);

						if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone()) {
							ListOffsetResult result = new ListOffsetResult(fetchedTimestampOffsets, partitionsToRetry);
							listOffsetRequestsFuture.complete(result);
						}
					}
				}

				@Override
				public void onFailure(RuntimeException e) {
					synchronized (listOffsetRequestsFuture) {
						if (!listOffsetRequestsFuture.isDone())
							listOffsetRequestsFuture.raise(e);
					}
				}
			});
		}
		return listOffsetRequestsFuture;
	}

	/**
	 * 以node维度，聚合需要重置策略的partition的时间戳，前提是leader节点可用
	 * 如果partition所在的集群的没有leader节点，将添加到partitionsToRetry集合中，等待重试
	 * @param timestampsToSearch 需要进新转换的时间戳集合
	 * @param partitionsToRetry  需要进行更新metadata或者重新连接leader节点的partition集合
	 */
	private Map<Node, Map<TopicPartition, ListOffsetRequest.PartitionData>> groupListOffsetRequests(
			Map<TopicPartition, Long> timestampsToSearch,
			Set<TopicPartition> partitionsToRetry) {
		final Map<TopicPartition, ListOffsetRequest.PartitionData> partitionDataMap = new HashMap<>();
		// 以下遍历主要进行了状态校验判断
		for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
			TopicPartition tp = entry.getKey();
			Long offset = entry.getValue();
			// 获取当前partition的metadata
			Optional<MetadataCache.PartitionInfoAndEpoch> currentInfo = metadata.partitionInfoIfCurrent(tp);
			if (!currentInfo.isPresent()) {
				log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
				// 不存在当前partition的metadata，申请进行更新，并等待重试
				metadata.requestUpdate();
				partitionsToRetry.add(tp);
			} else if (currentInfo.get().partitionInfo().leader() == null) {
				// 如果当前partition所在集群的leader节点为null
				log.debug("Leader for partition {} is unavailable for fetching offset {}", tp, offset);
				// 也申请更新metadata，并等待重试
				metadata.requestUpdate();
				partitionsToRetry.add(tp);
			} else if (client.isUnavailable(currentInfo.get().partitionInfo().leader())) {
				client.maybeThrowAuthFailure(currentInfo.get().partitionInfo().leader());

				// 连接是失败了，我们需要等待一段真空期，知道我们我们可以重试，
				// 我们不需申请更新metadata，因为断连时会更新metadata
				log.debug("Leader {} for partition {} is unavailable for fetching offset until reconnect backoff expires",
						currentInfo.get().partitionInfo().leader(), tp);
				partitionsToRetry.add(tp);
			} else {
				partitionDataMap.put(tp,
						new ListOffsetRequest.PartitionData(offset, Optional.of(currentInfo.get().epoch())));
			}
		}
		// 以node为维度，重新聚合
		return regroupPartitionMapByNode(partitionDataMap);
	}

	/**
	 * 向指定broker发送重置offset请求
	 * @param node               需要请求到的broker
	 * @param timestampsToSearch 需要发送的请求partition信息集合
	 * @param requireTimestamp   True if we require a timestamp in the response.
	 * @return A response which can be polled to obtain the corresponding timestamps and offsets.
	 */
	private RequestFuture<ListOffsetResult> sendListOffsetRequest(final Node node,
																  final Map<TopicPartition, ListOffsetRequest.PartitionData> timestampsToSearch,
																  boolean requireTimestamp) {
		// 构建请求
		ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
				.forConsumer(requireTimestamp, isolationLevel)
				.setTargetTimes(timestampsToSearch);
		// 发送请求
		log.debug("Sending ListOffsetRequest {} to broker {}", builder, node);
		return client.send(node, builder)
				.compose(new RequestFutureAdapter<ClientResponse, ListOffsetResult>() {
					@Override
					public void onSuccess(ClientResponse response, RequestFuture<ListOffsetResult> future) {
						ListOffsetResponse lor = (ListOffsetResponse) response.responseBody();
						log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
						// 处理重置offset响应
						handleListOffsetResponse(timestampsToSearch, lor, future);
					}
				});
	}

	/**
	 * 重置offset响应的回调任务
	 * @param timestampsToSearch partition-目标时间戳的映射集合，请求时的partition-目标时间戳集合
	 * @param listOffsetResponse broker返回的重置offset响应
	 * @param future             响应返回时，需要完成的异步任务
	 *                           需要注意的是，任何partition级别的错误都会失败掉整个异步任务结果
	 *                           一个异常是UNSUPPORTED_FOR_MESSAGE_FORMAT，证明broker不支持v1版本的消息格式
	 *                           如果分区有此特定的错误，需要离开异步任务映射集合
	 *                           还需要注意的是，每个partition对应的时间戳在v0版本的消息格式中可能为null，在v1及以后的版本中，ListOffset API不会返回null时间戳（可能会返回-1）
	 */
	@SuppressWarnings("deprecation")
	private void handleListOffsetResponse(Map<TopicPartition, ListOffsetRequest.PartitionData> timestampsToSearch,
										  ListOffsetResponse listOffsetResponse,
										  RequestFuture<ListOffsetResult> future) {
		Map<TopicPartition, ListOffsetData> fetchedOffsets = new HashMap<>();
		Set<TopicPartition> partitionsToRetry = new HashSet<>();
		Set<String> unauthorizedTopics = new HashSet<>();

		for (Map.Entry<TopicPartition, ListOffsetRequest.PartitionData> entry : timestampsToSearch.entrySet()) {
			TopicPartition topicPartition = entry.getKey();
			// 获取指定partition的partition数据
			ListOffsetResponse.PartitionData partitionData = listOffsetResponse.responseData().get(topicPartition);
			Errors error = partitionData.error;
			// 排除异常情况
			if (error == Errors.NONE) {
				// 如果partition数据中包含了offset数据
				if (partitionData.offsets != null) {
					// 处理v0版本消息格式的响应内容
					long offset;
					// 不允许存在一个partition有多个offset的情况
					if (partitionData.offsets.size() > 1) {
						future.raise(new IllegalStateException("Unexpected partitionData response of length " +
								partitionData.offsets.size()));
						return;
						// 没有offset，返回的是一个emptyList
					} else if (partitionData.offsets.isEmpty()) {
						// 返回未知的offset结果
						offset = ListOffsetResponse.UNKNOWN_OFFSET;
					} else {
						// 获取这个唯一的offset
						offset = partitionData.offsets.get(0);
					}
					log.debug("Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
							topicPartition, offset);
					// 在非位置offset的情况下
					if (offset != ListOffsetResponse.UNKNOWN_OFFSET) {
						// 构建offset数据，放入到缓存已拉取的partition-offset数据映射集合中
						ListOffsetData offsetData = new ListOffsetData(offset, null, Optional.empty());
						fetchedOffsets.put(topicPartition, offsetData);
					}
				} else {
					// 处理v1版本及以后的响应内容
					log.debug("Handling ListOffsetResponse response for {}. Fetched offset {}, timestamp {}",
							topicPartition, partitionData.offset, partitionData.timestamp);
					if (partitionData.offset != ListOffsetResponse.UNKNOWN_OFFSET) {
						// 此时是从ListOffsetResponse.PartitionData的offset中获取offset，是一个基本类型成员变量
						ListOffsetData offsetData = new ListOffsetData(partitionData.offset, partitionData.timestamp,
								partitionData.leaderEpoch);
						// 也构建offset数据，放入到缓存已拉取的partition-offset数据映射集合中
						fetchedOffsets.put(topicPartition, offsetData);
					}
				}
			} else if (error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT) {
				// 如果响应返回了UNSUPPORTED_FOR_MESSAGE_FORMAT异常
				// broker端的消息格式版本小于0.10.0，意味着不能支持时间戳格式
				// 我们把这种情况视为没有找到offset，不计入最终结果
				log.debug("Cannot search by timestamp for partition {} because the message format version " +
						"is before 0.10.0", topicPartition);
			} else if (error == Errors.NOT_LEADER_FOR_PARTITION ||
					error == Errors.REPLICA_NOT_AVAILABLE ||
					error == Errors.KAFKA_STORAGE_ERROR ||
					error == Errors.OFFSET_NOT_AVAILABLE ||
					error == Errors.LEADER_NOT_AVAILABLE) {
				// 出现NOT_LEADER_FOR_PARTITION、REPLICA_NOT_AVAILABLE、KAFKA_STORAGE_ERROR、OFFSET_NOT_AVAILABLE、LEADER_NOT_AVAILABLE异常，需要进行重试
				log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
						topicPartition, error);
				partitionsToRetry.add(topicPartition);
			} else if (error == Errors.FENCED_LEADER_EPOCH ||
					error == Errors.UNKNOWN_LEADER_EPOCH) {
				// 出现FENCED_LEADER_EPOCH、UNKNOWN_LEADER_EPOCH，也需要进行重试
				log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
						topicPartition, error);
				partitionsToRetry.add(topicPartition);
			} else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
				// 出现UNKNOWN_TOPIC_OR_PARTITION，也需要进行重试
				log.warn("Received unknown topic or partition error in ListOffset request for partition {}", topicPartition);
				partitionsToRetry.add(topicPartition);
			} else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
				// 出现TOPIC_AUTHORIZATION_FAILED，视为校验失败
				unauthorizedTopics.add(topicPartition.topic());
			} else {
				log.warn("Attempt to fetch offsets for partition {} failed due to: {}, retrying.", topicPartition, error.message());
				// 出现了未知异常，也需要进行重试
				partitionsToRetry.add(topicPartition);
			}
		}
		// 如果存在身份验证问题，抛出身份验证异常
		if (!unauthorizedTopics.isEmpty())
			future.raise(new TopicAuthorizationException(unauthorizedTopics));
		else
			// 否则包装重置offset成功结果
			future.complete(new ListOffsetResult(fetchedOffsets, partitionsToRetry));
	}

	/**
	 * 获取所有可进行拉取操作的partition
	 * @return 需要拉取的分区
	 */
	private List<TopicPartition> fetchablePartitions() {
		Set<TopicPartition> exclude = new HashSet<>();
		// 如果下一个需要处理的records不是可拉取的
		if (nextInLineRecords != null && !nextInLineRecords.isFetched) {
			// 将其排除在外
			exclude.add(nextInLineRecords.partition);
		}
		// 遍历剩余的已经拉取的还没处理的fetch
		for (PartitionRecords completedFetch : completedFetches) {
			exclude.add(completedFetch.partition);
		}
		// 排除不需要继续拉取的分区
		return subscriptions.fetchablePartitions(tp -> !exclude.contains(tp));
	}

	/**
	 * 确认从哪个副本节点读取数据
	 */
	Node selectReadReplica(TopicPartition partition, Node leaderReplica, long currentTimeMs) {
		// 获取首选的副本节点id
		Optional<Integer> nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs);
		// 如果设置了首选的副本节点id
		if (nodeId.isPresent()) {
			// 获取此副本节点，在上线的状态下
			Optional<Node> node = nodeId.flatMap(id -> metadata.fetch().nodeIfOnline(partition, id));
			if (node.isPresent()) {
				return node.get();
			} else {
				log.trace("Not fetching from {} for partition {} since it is marked offline or is missing from our metadata," +
						" using the leader instead.", nodeId, partition);
				subscriptions.clearPreferredReadReplica(partition);
				// 返回leader节点
				return leaderReplica;
			}
		} else {
			// 没有设置首选的副本节点，使用leader节点
			return leaderReplica;
		}
	}

	/**
	 * 针对我们已有的已分配的分区，创建拉取请求，排除那些处于发送中请求的分区
	 */
	private Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
		Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();

		// 确认每个订阅的每个分区都有最新的leader
		subscriptions.assignedPartitions().forEach(
				tp -> subscriptions.maybeValidatePositionForCurrentLeader(tp, metadata.leaderAndEpoch(tp)));

		long currentTimeMs = time.milliseconds();
		// 遍历所有需要拉取的分区
		for (TopicPartition partition : fetchablePartitions()) {
			// 使用首选的已设置的只读副本，或者leader节点
			// 获取上一次拉取的位置
			SubscriptionState.FetchPosition position = this.subscriptions.position(partition);
			// 选择可读副本，没有设置首选节点的情况，会默认使用leader节点
			Node node = selectReadReplica(partition, position.currentLeader.leader, currentTimeMs);
			// 如果没有找到可读节点，需要更新cluster metadata
			if (node == null || node.isEmpty()) {
				metadata.requestUpdate();
			} else if (client.isUnavailable(node)) {
				// 如果找到可读节点，但是客户端无法连接
				// 判断是不是验证失败导致的无法连接
				client.maybeThrowAuthFailure(node);

				// 如果我们尝试在重新来南车中断窗口期间发送，那么无论如何该请在发送之前都会失败，因此暂时跳过此次的请求发送
				log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
			} else if (this.nodesWithPendingFetchRequests.contains(node.id())) {
				// 如果可读节点处于挂起拉取请求中，也就是正在拉取，也跳过次次发送
				log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, node);
			} else {
				// 排除异常状态，可以构建发送请求了
				// 判断是否重复构建节点拉取会话处理器
				FetchSessionHandler.Builder builder = fetchable.get(node);
				if (builder == null) {
					// 没有构建过，构建一个新的
					int id = node.id();
					// 从缓存中获取会话处理器
					FetchSessionHandler handler = sessionHandler(id);
					if (handler == null) {
						// 此次请求没有构建，缓存中也没有，只能创建一个新的
						handler = new FetchSessionHandler(logContext, id);
						sessionHandlers.put(id, handler);
					}
					builder = handler.newBuilder();
					fetchable.put(node, builder);
				}
				// 处理器添加partition信息
				builder.add(partition, new FetchRequest.PartitionData(position.offset,
						FetchRequest.INVALID_LOG_START_OFFSET, this.fetchSize, position.currentLeader.epoch));

				log.debug("Added {} fetch request for partition {} at position {} to node {}", isolationLevel,
						partition, position, node);
			}
		}

		Map<Node, FetchSessionHandler.FetchRequestData> reqs = new LinkedHashMap<>();
		// 遍历所有的需要拉取请求的节点和拉取会话处理器，构建一下，返回拉取请求数据
		for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet()) {
			reqs.put(entry.getKey(), entry.getValue().build());
		}
		// 返回节点-节点需要进行的请求数据集合
		return reqs;
	}

	private Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regroupFetchPositionsByLeader(
			Map<TopicPartition, SubscriptionState.FetchPosition> partitionMap) {
		return partitionMap.entrySet()
				.stream()
				.collect(Collectors.groupingBy(entry -> entry.getValue().currentLeader.leader,
						Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
	}

	/**
	 * 使用lambda进行重新聚合，partition->node维度
	 * @param partitionMap 重新聚合的partition-时间戳集合
	 * @param <T>          聚合泛型
	 * @return node维度的时间戳集合
	 */
	private <T> Map<Node, Map<TopicPartition, T>> regroupPartitionMapByNode(Map<TopicPartition, T> partitionMap) {
		return partitionMap.entrySet()
				.stream()
				.collect(Collectors.groupingBy(entry -> metadata.fetch().leaderFor(entry.getKey()),
						Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
	}

	/**
	 * 处理拉取到的records
	 * 将CompletedFetch->PartitionRecords
	 */
	private PartitionRecords parseCompletedFetch(CompletedFetch completedFetch) {
		TopicPartition tp = completedFetch.partition;
		FetchResponse.PartitionData<Records> partition = completedFetch.partitionData;
		// 获取到producer发送消息到累加器，然后封装好的RecordBatch
		Iterator<? extends RecordBatch> batches = partition.records.batches().iterator();
		return new PartitionRecords(tp, completedFetch, batches);
	}

	/**
	 * Initialize a PartitionRecords object.
	 */
	private PartitionRecords initializePartitionRecords(PartitionRecords partitionRecordsToInitialize) {
		CompletedFetch completedFetch = partitionRecordsToInitialize.completedFetch;
		TopicPartition tp = completedFetch.partition;
		FetchResponse.PartitionData<Records> partition = completedFetch.partitionData;
		long fetchOffset = completedFetch.fetchedOffset;
		PartitionRecords partitionRecords = null;
		Errors error = partition.error;

		try {
			if (!subscriptions.hasValidPosition(tp)) {
				// this can happen when a rebalance happened while fetch is still in-flight
				log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
			} else if (error == Errors.NONE) {
				// we are interested in this fetch only if the beginning offset matches the
				// current consumed position
				SubscriptionState.FetchPosition position = subscriptions.position(tp);
				if (position == null || position.offset != fetchOffset) {
					log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
							"the expected offset {}", tp, fetchOffset, position);
					return null;
				}

				log.trace("Preparing to read {} bytes of data for partition {} with offset {}",
						partition.records.sizeInBytes(), tp, position);
				Iterator<? extends RecordBatch> batches = partition.records.batches().iterator();
				partitionRecords = partitionRecordsToInitialize;

				if (!batches.hasNext() && partition.records.sizeInBytes() > 0) {
					if (completedFetch.responseVersion < 3) {
						// Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
						Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
						throw new RecordTooLargeException("There are some messages at [Partition=Offset]: " +
								recordTooLargePartitions + " whose size is larger than the fetch size " + this.fetchSize +
								" and hence cannot be returned. Please considering upgrading your broker to 0.10.1.0 or " +
								"newer to avoid this issue. Alternately, increase the fetch size on the client (using " +
								ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG + ")",
								recordTooLargePartitions);
					} else {
						// This should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
						throw new KafkaException("Failed to make progress reading messages at " + tp + "=" +
								fetchOffset + ". Received a non-empty fetch response from the server, but no " +
								"complete records were found.");
					}
				}

				if (partition.highWatermark >= 0) {
					log.trace("Updating high watermark for partition {} to {}", tp, partition.highWatermark);
					subscriptions.updateHighWatermark(tp, partition.highWatermark);
				}

				if (partition.logStartOffset >= 0) {
					log.trace("Updating log start offset for partition {} to {}", tp, partition.logStartOffset);
					subscriptions.updateLogStartOffset(tp, partition.logStartOffset);
				}

				if (partition.lastStableOffset >= 0) {
					log.trace("Updating last stable offset for partition {} to {}", tp, partition.lastStableOffset);
					subscriptions.updateLastStableOffset(tp, partition.lastStableOffset);
				}

				if (partition.preferredReadReplica.isPresent()) {
					subscriptions.updatePreferredReadReplica(partitionRecords.partition, partition.preferredReadReplica.get(), () -> {
						long expireTimeMs = time.milliseconds() + metadata.metadataExpireMs();
						log.debug("Updating preferred read replica for partition {} to {}, set to expire at {}",
								tp, partition.preferredReadReplica.get(), expireTimeMs);
						return expireTimeMs;
					});
				}


				partitionRecordsToInitialize.initialized = true;
			} else if (error == Errors.NOT_LEADER_FOR_PARTITION ||
					error == Errors.REPLICA_NOT_AVAILABLE ||
					error == Errors.KAFKA_STORAGE_ERROR ||
					error == Errors.FENCED_LEADER_EPOCH ||
					error == Errors.OFFSET_NOT_AVAILABLE) {
				log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
				this.metadata.requestUpdate();
			} else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
				log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
				this.metadata.requestUpdate();
			} else if (error == Errors.OFFSET_OUT_OF_RANGE) {
				Optional<Integer> clearedReplicaId = subscriptions.clearPreferredReadReplica(tp);
				if (!clearedReplicaId.isPresent()) {
					// If there's no preferred replica to clear, we're fetching from the leader so handle this error normally
					if (fetchOffset != subscriptions.position(tp).offset) {
						log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
								"does not match the current offset {}", tp, fetchOffset, subscriptions.position(tp));
					} else if (subscriptions.hasDefaultOffsetResetPolicy()) {
						log.info("Fetch offset {} is out of range for partition {}, resetting offset", fetchOffset, tp);
						subscriptions.requestOffsetReset(tp);
					} else {
						throw new OffsetOutOfRangeException(Collections.singletonMap(tp, fetchOffset));
					}
				} else {
					log.debug("Unset the preferred read replica {} for partition {} since we got {} when fetching {}",
							clearedReplicaId.get(), tp, error, fetchOffset);
				}
			} else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
				//we log the actual partition and not just the topic to help with ACL propagation issues in large clusters
				log.warn("Not authorized to read from partition {}.", tp);
				throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
			} else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
				log.debug("Received unknown leader epoch error in fetch for partition {}", tp);
			} else if (error == Errors.UNKNOWN_SERVER_ERROR) {
				log.warn("Unknown error fetching data for topic-partition {}", tp);
			} else {
				throw new IllegalStateException("Unexpected error code " + error.code() + " while fetching from partition " + tp);
			}
		} finally {
			if (partitionRecords == null)
				completedFetch.metricAggregator.record(tp, 0, 0);

			if (error != Errors.NONE)
				// we move the partition to the end if there was an error. This way, it's more likely that partitions for
				// the same topic can remain together (allowing for more efficient serialization).
				subscriptions.movePartitionToEnd(tp);
		}

		return partitionRecords;
	}

	/**
	 * 解析record元素，进行反序列化
	 */
	private ConsumerRecord<K, V> parseRecord(TopicPartition partition,
											 RecordBatch batch,
											 Record record) {
		try {
			// 获取offset和时间戳
			long offset = record.offset();
			long timestamp = record.timestamp();
			// 获取对应的leader版本号
			Optional<Integer> leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch());
			// 获取batch的时间戳类型
			TimestampType timestampType = batch.timestampType();
			// 获取record的请求头部
			Headers headers = new RecordHeaders(record.headers());
			// 获取record的请求key
			ByteBuffer keyBytes = record.key();
			// 转换成字节数组
			byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
			// 进行反序列化
			K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), headers, keyByteArray);
			// 获取record的value
			ByteBuffer valueBytes = record.value();
			// 转换成字节数组，进行反序列化
			byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
			V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), headers, valueByteArray);
			// 构建消费者消费的Record
			return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
					timestamp, timestampType, record.checksumOrNull(),
					keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
					valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
					key, value, headers, leaderEpoch);
		} catch (RuntimeException e) {
			throw new SerializationException("Error deserializing key/value for partition " + partition +
					" at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
		}
	}

	private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
		return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
	}

	/**
	 * 释放不属于新的分配分区的缓冲区数据
	 * @param assignedPartitions 新分配的分区数据
	 */
	public void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> assignedPartitions) {
		Iterator<PartitionRecords> completedFetchesItr = completedFetches.iterator();
		while (completedFetchesItr.hasNext()) {
			PartitionRecords records = completedFetchesItr.next();
			TopicPartition tp = records.partition;
			// 排除不在新订阅分区的分区record
			if (!assignedPartitions.contains(tp)) {
				records.drain();
				completedFetchesItr.remove();
			}
		}
		if (nextInLineRecords != null && !assignedPartitions.contains(nextInLineRecords.partition)) {
			nextInLineRecords.drain();
			nextInLineRecords = null;
		}
	}

	/**
	 * 清除没有新订阅topic的分区的buffer数据
	 * @param assignedTopics 指定的topic集合
	 */
	public void clearBufferedDataForUnassignedTopics(Collection<String> assignedTopics) {
		Set<TopicPartition> currentTopicPartitions = new HashSet<>();
		// 过滤没有订阅topic的分区
		for (TopicPartition tp : subscriptions.assignedPartitions()) {
			if (assignedTopics.contains(tp.topic())) {
				// 过滤topic
				currentTopicPartitions.add(tp);
			}
		}
		clearBufferedDataForUnassignedPartitions(currentTopicPartitions);
	}

	/**
	 * 构建向节点拉取任务的会话处理器
	 * @param node 指定节点
	 * @return 会话处理器
	 */
	protected FetchSessionHandler sessionHandler(int node) {
		return sessionHandlers.get(node);
	}

	@Override
	public void close() {
		if (nextInLineRecords != null)
			nextInLineRecords.drain();
		decompressionBufferSupplier.close();
	}

	private Set<String> topicsForPartitions(Collection<TopicPartition> partitions) {
		return partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
	}

	/**
	 * Represents data about an offset returned by a broker.
	 */
	private static class ListOffsetData {
		final long offset;
		final Long timestamp; //  null if the broker does not support returning timestamps
		final Optional<Integer> leaderEpoch; // empty if the leader epoch is not known

		ListOffsetData(long offset, Long timestamp, Optional<Integer> leaderEpoch) {
			this.offset = offset;
			this.timestamp = timestamp;
			this.leaderEpoch = leaderEpoch;
		}
	}

	static class ListOffsetResult {
		private final Map<TopicPartition, ListOffsetData> fetchedOffsets;
		private final Set<TopicPartition> partitionsToRetry;

		public ListOffsetResult(Map<TopicPartition, ListOffsetData> fetchedOffsets, Set<TopicPartition> partitionsNeedingRetry) {
			this.fetchedOffsets = fetchedOffsets;
			this.partitionsToRetry = partitionsNeedingRetry;
		}

		public ListOffsetResult() {
			this.fetchedOffsets = new HashMap<>();
			this.partitionsToRetry = new HashSet<>();
		}
	}

	private static class CompletedFetch {
		private final TopicPartition partition;
		private final long fetchedOffset;
		private final FetchResponse.PartitionData<Records> partitionData;
		private final FetchResponseMetricAggregator metricAggregator;
		private final short responseVersion;

		private CompletedFetch(TopicPartition partition,
							   long fetchedOffset,
							   FetchResponse.PartitionData<Records> partitionData,
							   FetchResponseMetricAggregator metricAggregator,
							   short responseVersion) {
			this.partition = partition;
			this.fetchedOffset = fetchedOffset;
			this.partitionData = partitionData;
			this.metricAggregator = metricAggregator;
			this.responseVersion = responseVersion;
		}
	}

	/**
	 * Since we parse the message data for each partition from each fetch response lazily, fetch-level
	 * metrics need to be aggregated as the messages from each partition are parsed. This class is used
	 * to facilitate this incremental aggregation.
	 */
	private static class FetchResponseMetricAggregator {
		private final FetchManagerMetrics sensors;
		private final Set<TopicPartition> unrecordedPartitions;

		private final FetchMetrics fetchMetrics = new FetchMetrics();
		private final Map<String, FetchMetrics> topicFetchMetrics = new HashMap<>();

		private FetchResponseMetricAggregator(FetchManagerMetrics sensors,
											  Set<TopicPartition> partitions) {
			this.sensors = sensors;
			this.unrecordedPartitions = partitions;
		}

		/**
		 * After each partition is parsed, we update the current metric totals with the total bytes
		 * and number of records parsed. After all partitions have reported, we write the metric.
		 */
		public void record(TopicPartition partition, int bytes, int records) {
			this.unrecordedPartitions.remove(partition);
			this.fetchMetrics.increment(bytes, records);

			// collect and aggregate per-topic metrics
			String topic = partition.topic();
			FetchMetrics topicFetchMetric = this.topicFetchMetrics.get(topic);
			if (topicFetchMetric == null) {
				topicFetchMetric = new FetchMetrics();
				this.topicFetchMetrics.put(topic, topicFetchMetric);
			}
			topicFetchMetric.increment(bytes, records);

			if (this.unrecordedPartitions.isEmpty()) {
				// once all expected partitions from the fetch have reported in, record the metrics
				this.sensors.bytesFetched.record(this.fetchMetrics.fetchBytes);
				this.sensors.recordsFetched.record(this.fetchMetrics.fetchRecords);

				// also record per-topic metrics
				for (Map.Entry<String, FetchMetrics> entry : this.topicFetchMetrics.entrySet()) {
					FetchMetrics metric = entry.getValue();
					this.sensors.recordTopicFetchMetrics(entry.getKey(), metric.fetchBytes, metric.fetchRecords);
				}
			}
		}

		private static class FetchMetrics {
			private int fetchBytes;
			private int fetchRecords;

			protected void increment(int bytes, int records) {
				this.fetchBytes += bytes;
				this.fetchRecords += records;
			}
		}
	}

	private static class FetchManagerMetrics {
		private final Metrics metrics;
		private final Sensor bytesFetched;
		private final Sensor recordsFetched;
		private final Sensor fetchLatency;
		private final Sensor recordsFetchLag;
		private final Sensor recordsFetchLead;
		private FetcherMetricsRegistry metricsRegistry;
		private int assignmentId = 0;
		private Set<TopicPartition> assignedPartitions = Collections.emptySet();

		private FetchManagerMetrics(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
			this.metrics = metrics;
			this.metricsRegistry = metricsRegistry;

			this.bytesFetched = metrics.sensor("bytes-fetched");
			this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeAvg), new Avg());
			this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeMax), new Max());
			this.bytesFetched.add(new Meter(metrics.metricInstance(metricsRegistry.bytesConsumedRate),
					metrics.metricInstance(metricsRegistry.bytesConsumedTotal)));

			this.recordsFetched = metrics.sensor("records-fetched");
			this.recordsFetched.add(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg), new Avg());
			this.recordsFetched.add(new Meter(metrics.metricInstance(metricsRegistry.recordsConsumedRate),
					metrics.metricInstance(metricsRegistry.recordsConsumedTotal)));

			this.fetchLatency = metrics.sensor("fetch-latency");
			this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyAvg), new Avg());
			this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyMax), new Max());
			this.fetchLatency.add(new Meter(new WindowedCount(), metrics.metricInstance(metricsRegistry.fetchRequestRate),
					metrics.metricInstance(metricsRegistry.fetchRequestTotal)));

			this.recordsFetchLag = metrics.sensor("records-lag");
			this.recordsFetchLag.add(metrics.metricInstance(metricsRegistry.recordsLagMax), new Max());

			this.recordsFetchLead = metrics.sensor("records-lead");
			this.recordsFetchLead.add(metrics.metricInstance(metricsRegistry.recordsLeadMin), new Min());
		}

		private static String partitionLagMetricName(TopicPartition tp) {
			return tp + ".records-lag";
		}

		private static String partitionLeadMetricName(TopicPartition tp) {
			return tp + ".records-lead";
		}

		private void recordTopicFetchMetrics(String topic, int bytes, int records) {
			// record bytes fetched
			String name = "topic." + topic + ".bytes-fetched";
			Sensor bytesFetched = this.metrics.getSensor(name);
			if (bytesFetched == null) {
				Map<String, String> metricTags = Collections.singletonMap("topic", topic.replace('.', '_'));

				bytesFetched = this.metrics.sensor(name);
				bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeAvg,
						metricTags), new Avg());
				bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeMax,
						metricTags), new Max());
				bytesFetched.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicBytesConsumedRate, metricTags),
						this.metrics.metricInstance(metricsRegistry.topicBytesConsumedTotal, metricTags)));
			}
			bytesFetched.record(bytes);

			// record records fetched
			name = "topic." + topic + ".records-fetched";
			Sensor recordsFetched = this.metrics.getSensor(name);
			if (recordsFetched == null) {
				Map<String, String> metricTags = new HashMap<>(1);
				metricTags.put("topic", topic.replace('.', '_'));

				recordsFetched = this.metrics.sensor(name);
				recordsFetched.add(this.metrics.metricInstance(metricsRegistry.topicRecordsPerRequestAvg,
						metricTags), new Avg());
				recordsFetched.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedRate, metricTags),
						this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedTotal, metricTags)));
			}
			recordsFetched.record(records);
		}

		private void maybeUpdateAssignment(SubscriptionState subscription) {
			int newAssignmentId = subscription.assignmentId();
			if (this.assignmentId != newAssignmentId) {
				Set<TopicPartition> newAssignedPartitions = subscription.assignedPartitions();
				for (TopicPartition tp : this.assignedPartitions) {
					if (!newAssignedPartitions.contains(tp)) {
						metrics.removeSensor(partitionLagMetricName(tp));
						metrics.removeSensor(partitionLeadMetricName(tp));
						metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp));
					}
				}

				for (TopicPartition tp : newAssignedPartitions) {
					if (!this.assignedPartitions.contains(tp)) {
						MetricName metricName = partitionPreferredReadReplicaMetricName(tp);
						if (metrics.metric(metricName) == null) {
							metrics.addMetric(metricName, (Gauge<Integer>) (config, now) ->
									subscription.preferredReadReplica(tp, 0L).orElse(-1));
						}
					}
				}

				this.assignedPartitions = newAssignedPartitions;
				this.assignmentId = newAssignmentId;
			}
		}

		private void recordPartitionLead(TopicPartition tp, long lead) {
			this.recordsFetchLead.record(lead);

			String name = partitionLeadMetricName(tp);
			Sensor recordsLead = this.metrics.getSensor(name);
			if (recordsLead == null) {
				Map<String, String> metricTags = topicPartitionTags(tp);

				recordsLead = this.metrics.sensor(name);

				recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLead, metricTags), new Value());
				recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLeadMin, metricTags), new Min());
				recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLeadAvg, metricTags), new Avg());
			}
			recordsLead.record(lead);
		}

		private void recordPartitionLag(TopicPartition tp, long lag) {
			this.recordsFetchLag.record(lag);

			String name = partitionLagMetricName(tp);
			Sensor recordsLag = this.metrics.getSensor(name);
			if (recordsLag == null) {
				Map<String, String> metricTags = topicPartitionTags(tp);
				recordsLag = this.metrics.sensor(name);

				recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLag, metricTags), new Value());
				recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLagMax, metricTags), new Max());
				recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLagAvg, metricTags), new Avg());
			}
			recordsLag.record(lag);
		}

		private MetricName partitionPreferredReadReplicaMetricName(TopicPartition tp) {
			Map<String, String> metricTags = topicPartitionTags(tp);
			return this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags);
		}

		private Map<String, String> topicPartitionTags(TopicPartition tp) {
			Map<String, String> metricTags = new HashMap<>(2);
			metricTags.put("topic", tp.topic().replace('.', '_'));
			metricTags.put("partition", String.valueOf(tp.partition()));
			return metricTags;
		}
	}

	private class PartitionRecords {
		private final TopicPartition partition;
		private final CompletedFetch completedFetch;
		/**
		 * record的迭代器
		 */
		private final Iterator<? extends RecordBatch> batches;
		private final Set<Long> abortedProducerIds;
		private final PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions;

		private int recordsRead;
		private int bytesRead;
		private RecordBatch currentBatch;
		private Record lastRecord;
		/**
		 * record的闭环迭代器
		 */
		private CloseableIterator<Record> records;
		private long nextFetchOffset;
		private Optional<Integer> lastEpoch;
		private boolean isFetched = false;
		private Exception cachedRecordException = null;
		private boolean corruptLastRecord = false;
		private boolean initialized = false;

		private PartitionRecords(TopicPartition partition,
								 CompletedFetch completedFetch,
								 Iterator<? extends RecordBatch> batches) {
			this.partition = partition;
			this.completedFetch = completedFetch;
			this.batches = batches;
			this.nextFetchOffset = completedFetch.fetchedOffset;
			this.lastEpoch = Optional.empty();
			this.abortedProducerIds = new HashSet<>();
			this.abortedTransactions = abortedTransactions(completedFetch.partitionData);
		}

		/**
		 * 排空record
		 */
		private void drain() {
			// 还没有拉取完
			if (!isFetched) {
				// 校验是否需要关闭闭环迭代器
				maybeCloseRecordStream();
				cachedRecordException = null;
				// 当前partition-record置为拉取完成
				this.isFetched = true;
				// 对完成的fetch进行计数
				this.completedFetch.metricAggregator.record(partition, bytesRead, recordsRead);

				// 如果我们接收到了字节，我们就把分区移动到订阅列表的尾端，通过这种方法，同一个主题的分区更有可能保持在一起（云讯更有效的序列化）
				if (bytesRead > 0)
					subscriptions.movePartitionToEnd(partition);
			}
		}

		private Optional<Integer> preferredReadReplica() {
			return completedFetch.partitionData.preferredReadReplica;
		}

		/**
		 * 确认batch是否可用
		 * @param batch 需要确认的batch
		 */
		private void maybeEnsureValid(RecordBatch batch) {
			if (checkCrcs && currentBatch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
				try {
					batch.ensureValid();
				} catch (InvalidRecordException e) {
					throw new KafkaException("Record batch for partition " + partition + " at offset " +
							batch.baseOffset() + " is invalid, cause: " + e.getMessage());
				}
			}
		}

		/**
		 * 校验record是否合法
		 * @param record 需要校验的record
		 */
		private void maybeEnsureValid(Record record) {
			if (checkCrcs) {
				try {
					record.ensureValid();
				} catch (InvalidRecordException e) {
					throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
							+ " is invalid, cause: " + e.getMessage());
				}
			}
		}

		private void maybeCloseRecordStream() {
			if (records != null) {
				records.close();
				records = null;
			}
		}

		/**
		 * 获取下一个需要处理的record
		 * @return 下一个需要处理的record
		 */
		private Record nextFetchedRecord() {
			while (true) {
				// 如果没有后续record，证明已经读取完了
				if (records == null || !records.hasNext()) {
					// 判断是否需要关闭闭环迭代器
					maybeCloseRecordStream();
					// 如果record中也没有后续record
					if (!batches.hasNext()) {
						// v2版本的消息格式保留了上一次的offset，即使通过压缩删除了最后一条record
						// 通过使用batch中的上一次的offset计算出的下一次offset，我们确认了下一次fetch的offset会指向下一个batch，这避免了不必要的重新拉取相同的batch
						// 在最坏的情况下，消费者可能会在重复拉取相同batch时出现阻塞
						if (currentBatch != null)
							nextFetchOffset = currentBatch.nextOffset();
						// 排空当前partition-records，并返回null
						drain();
						return null;
					}
					// 获取下一个需要处理的batch
					currentBatch = batches.next();
					// 获取上一次处理的版本号
					lastEpoch = currentBatch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
							Optional.empty() : Optional.of(currentBatch.partitionLeaderEpoch());
					// 确认当前batch是否可用
					maybeEnsureValid(currentBatch);
					// 如果事务等级是可重读，并且发送消息时，通过事务管理器，产生了producerId
					if (isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
						// 从中止的事务队列中删除在当前批次的最后一个偏移之前开始的所有中止的事务，并将关联的生产者ID添加到中止的生产者集合上
						consumeAbortedTransactionsUpTo(currentBatch.lastOffset());
						// 获取producerId
						long producerId = currentBatch.producerId();
						// 判断是否需要中断
						if (containsAbortMarker(currentBatch)) {
							// 需要中断，添加到中断集合中
							abortedProducerIds.remove(producerId);
						} else if (isBatchAborted(currentBatch)) {
							// 如果batch已经中断
							log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
											"offsets {} to {}",
									partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
							// 越过当前的offset
							nextFetchOffset = currentBatch.nextOffset();
							continue;
						}
					}
					// 继续获取record迭代
					records = currentBatch.streamingIterator(decompressionBufferSupplier);
				} else {
					// 从闭环迭代器中获取下一个需要迭代的record
					Record record = records.next();
					// 如果record的offset超过了需要处理的offset
					if (record.offset() >= nextFetchOffset) {
						// 校验record是否合法
						maybeEnsureValid(record);

						// 返回非control record
						if (!currentBatch.isControlBatch()) {
							return record;
						} else {
							// 如果是control record，则移动offset，不进行处理
							nextFetchOffset = record.offset() + 1;
						}
					}
				}
			}
		}

		/**
		 * 从拉取到的partition record中获取指定数目的record，并封装成供消费者消费的record集合
		 * @param maxRecords 消费的数目
		 * @return 供消费者消费的record集合
		 */
		private List<ConsumerRecord<K, V>> fetchRecords(int maxRecords) {
			// 在反序列化之前出现中断错误
			if (corruptLastRecord)
				throw new KafkaException("Received exception when fetching the next record from " + partition
						+ ". If needed, please seek past the record to "
						+ "continue consumption.", cachedRecordException);
			// 已经拉取走了，返回空集合
			if (isFetched)
				return Collections.emptyList();

			List<ConsumerRecord<K, V>> records = new ArrayList<>();
			try {
				// 原始计数器进行遍历，真·每次循环的计数操作
				for (int i = 0; i < maxRecords; i++) {
					// 如果在上一次fetch中没有出现异常，仅仅移动一个record
					// 其他情况下，我们应该使用上一次的record再进行一次反序列化
					if (cachedRecordException == null) {
						corruptLastRecord = true;
						lastRecord = nextFetchedRecord();
						corruptLastRecord = false;
					}
					// 如果没有上一次需要拉取的record，证明已经拉取完了，跳出循环
					if (lastRecord == null)
						break;
					// 构建ConsumerRecord
					records.add(parseRecord(partition, currentBatch, lastRecord));
					// 读取的计数器+1
					recordsRead++;
					// 累加读取的字节数
					bytesRead += lastRecord.sizeInBytes();
					// 累加offset
					nextFetchOffset = lastRecord.offset() + 1;
					// In some cases, the deserialization may have thrown an exception and the retry may succeed,
					// we allow user to move forward in this case.
					// 在某些场景下，反序列化可能会抛出异常，并且重试可能会成功
					// 在这写场景下，我们允许开发者继续进行下去
					cachedRecordException = null;
				}
			} catch (SerializationException se) {
				cachedRecordException = se;
				if (records.isEmpty())
					throw se;
			} catch (KafkaException e) {
				cachedRecordException = e;
				if (records.isEmpty())
					throw new KafkaException("Received exception when fetching the next record from " + partition
							+ ". If needed, please seek past the record to "
							+ "continue consumption.", e);
			}
			return records;
		}

		/**
		 * 中止事务到指定的offset上
		 * @param offset 事务中止指定的offset
		 */
		private void consumeAbortedTransactionsUpTo(long offset) {
			if (abortedTransactions == null)
				return;
			// 遍历，添加到中断集合中，等待中断
			while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset <= offset) {
				FetchResponse.AbortedTransaction abortedTransaction = abortedTransactions.poll();
				abortedProducerIds.add(abortedTransaction.producerId);
			}
		}

		private boolean isBatchAborted(RecordBatch batch) {
			return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
		}

		private PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions(FetchResponse.PartitionData<?> partition) {
			if (partition.abortedTransactions == null || partition.abortedTransactions.isEmpty())
				return null;

			PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
					partition.abortedTransactions.size(), Comparator.comparingLong(o -> o.firstOffset)
			);
			abortedTransactions.addAll(partition.abortedTransactions);
			return abortedTransactions;
		}

		/**
		 * 是否包含中断制造者
		 * @param batch 指定的batch
		 * @return 当前batch是否是中断制造者
		 */
		private boolean containsAbortMarker(RecordBatch batch) {
			if (!batch.isControlBatch())
				return false;

			Iterator<Record> batchIterator = batch.iterator();
			if (!batchIterator.hasNext())
				return false;

			Record firstRecord = batchIterator.next();
			// 根据key的表示为来判断record的控制类型
			return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
		}

		private boolean notInitialized() {
			return !this.initialized;
		}
	}

}
