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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * 这个类负载管理consumer的协调进程
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
	/**
	 * 消费组再平衡配置
	 */
	private final GroupRebalanceConfig rebalanceConfig;
	/**
	 * 日志记录器
	 */
	private final Logger log;
	/**
	 * consumer partition分配器
	 */
	private final List<ConsumerPartitionAssignor> assignors;
	/**
	 * consumer的metadata
	 */
	private final ConsumerMetadata metadata;
	/**
	 * consumer协调者的计数器
	 */
	private final ConsumerCoordinatorMetrics sensors;
	/**
	 * 订阅状态
	 */
	private final SubscriptionState subscriptions;
	/**
	 * 提交offset的回调任务
	 */
    private final OffsetCommitCallback defaultOffsetCommitCallback;
	/**
	 * 是否开启自动提交
	 */
	private final boolean autoCommitEnabled;
	/**
	 * 自动提交的时间间隔
	 */
	private final int autoCommitIntervalMs;
	/**
	 * 提交成功后执行的拦截器操作
	 */
	private final ConsumerInterceptors<?, ?> interceptors;
	/**
	 * 挂起的异步提交数量
	 */
	private final AtomicInteger pendingAsyncCommits;

	/**
	 * 已完成的offset提交
	 * 队列必须是线程安全的，因为他会被多个提交offset请求的响应处理器修改，这些多个处理器可能是由心跳线程触发的
	 */
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;
	/**
	 * 是否是leader节点
	 */
    private boolean isLeader = false;

    private Set<String> joinedSubscription;
	/**
	 * metadata的当前快照
	 */
	private MetadataSnapshot metadataSnapshot;
	/**
	 * 分配的当前快照
	 */
	private MetadataSnapshot assignmentSnapshot;
	/**
	 * 进行下一次提交的计时器
	 */
	private Timer nextAutoCommitTimer;
	/**
	 * 异步提交栅栏
	 */
	private AtomicBoolean asyncCommitFenced;

    // hold onto request&future for committed offset requests to enable async calls.
	/**
	 * 开启异步调用的情况下，挂起提交offset的request和future
	 */
	private PendingCommittedOffsetRequest pendingCommittedOffsetRequest = null;

    private static class PendingCommittedOffsetRequest {
        private final Set<TopicPartition> requestedPartitions;
        private final Generation requestedGeneration;
        private final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response;

        private PendingCommittedOffsetRequest(final Set<TopicPartition> requestedPartitions,
                                              final Generation generationAtRequestTime,
                                              final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response) {
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            this.response = Objects.requireNonNull(response);
            this.requestedGeneration = generationAtRequestTime;
        }

        private boolean sameRequest(final Set<TopicPartition> currentRequest, final Generation currentGeneration) {
            return Objects.equals(requestedGeneration, currentGeneration) && requestedPartitions.equals(currentRequest);
        }
    }

    private final RebalanceProtocol protocol;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               List<ConsumerPartitionAssignor> assignors,
                               ConsumerMetadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors) {
        super(rebalanceConfig,
              logContext,
              client,
              metrics,
              metricGrpPrefix,
              time);
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(ConsumerCoordinator.class);
        this.metadata = metadata;
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch(), metadata.updateVersion());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.pendingAsyncCommits = new AtomicInteger();
        this.asyncCommitFenced = new AtomicBoolean(false);
		// 如果开启了自动提交offset
        if (autoCommitEnabled)
			// 初始化下一次进行自动提交的计时器
            this.nextAutoCommitTimer = time.timer(autoCommitIntervalMs);

        // select the rebalance protocol such that:
        //   1. only consider protocols that are supported by all the assignors. If there is no common protocols supported
        //      across all the assignors, throw an exception.
        //   2. if there are multiple protocols that are commonly supported, select the one with the highest id (i.e. the
        //      id number indicates how advanced the protocol is).
        // we know there are at least one assignor in the list, no need to double check for NPE
        if (!assignors.isEmpty()) {
            List<RebalanceProtocol> supportedProtocols = new ArrayList<>(assignors.get(0).supportedProtocols());

            for (ConsumerPartitionAssignor assignor : assignors) {
                supportedProtocols.retainAll(assignor.supportedProtocols());
            }

            if (supportedProtocols.isEmpty()) {
                throw new IllegalArgumentException("Specified assignors " +
                    assignors.stream().map(ConsumerPartitionAssignor::name).collect(Collectors.toSet()) +
                    " do not have commonly supported rebalance protocol");
            }

            Collections.sort(supportedProtocols);

            protocol = supportedProtocols.get(supportedProtocols.size() - 1);
        } else {
            protocol = null;
        }

        this.metadata.requestUpdate();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        log.debug("Joining group with current subscription: {}", subscriptions.subscription());
        this.joinedSubscription = subscriptions.subscription();
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocolSet = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();

        List<String> topics = new ArrayList<>(joinedSubscription);
        for (ConsumerPartitionAssignor assignor : assignors) {
            Subscription subscription = new Subscription(topics,
                                                         assignor.subscriptionUserData(joinedSubscription),
                                                         subscriptions.assignedPartitionsList());
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);

            protocolSet.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(assignor.name())
                    .setMetadata(Utils.toArray(metadata)));
        }
        return protocolSet;
    }

	/**
	 * 更新正则表达式订阅模式下的订阅信息
	 * @param cluster 集群信息
	 */
	public void updatePatternSubscription(Cluster cluster) {
		// 获取根据正在表达式筛选的需要订阅的topic列表
		final Set<String> topicsToSubscribe = cluster.topics().stream()
				.filter(subscriptions::matchesSubscribedPattern)
				.collect(Collectors.toSet());
		// 更新订阅信息，相当于直接替换当前订阅的topic列表
		if (subscriptions.subscribeFromPattern(topicsToSubscribe))
			// 请求更新新的topic的metadata
			metadata.requestUpdateForNewTopics();
	}

    private ConsumerPartitionAssignor lookupAssignor(String name) {
        for (ConsumerPartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    private void maybeUpdateJoinedSubscription(Set<TopicPartition> assignedPartitions) {
        if (subscriptions.hasPatternSubscription()) {
            // Check if the assignment contains some topics that were not in the original
            // subscription, if yes we will obey what leader has decided and add these topics
            // into the subscriptions as long as they still match the subscribed pattern

            Set<String> addedTopics = new HashSet<>();
            // this is a copy because its handed to listener below
            for (TopicPartition tp : assignedPartitions) {
                if (!joinedSubscription.contains(tp.topic()))
                    addedTopics.add(tp.topic());
            }

            if (!addedTopics.isEmpty()) {
                Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
                Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
                newSubscription.addAll(addedTopics);
                newJoinedSubscription.addAll(addedTopics);

                if (this.subscriptions.subscribeFromPattern(newSubscription))
                    metadata.requestUpdateForNewTopics();
                this.joinedSubscription = newJoinedSubscription;
            }
        }
    }

    private Exception invokePartitionsAssigned(final Set<TopicPartition> assignedPartitions) {
        log.info("Adding newly assigned partitions: {}", Utils.join(assignedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            listener.onPartitionsAssigned(assignedPartitions);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsAssigned for partitions {}",
                listener.getClass().getName(), assignedPartitions, e);
            return e;
        }

        return null;
    }

    private Exception invokePartitionsRevoked(final Set<TopicPartition> revokedPartitions) {
        log.info("Revoke previously assigned partitions {}", Utils.join(revokedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            listener.onPartitionsRevoked(revokedPartitions);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsRevoked for partitions {}",
                listener.getClass().getName(), revokedPartitions, e);
            return e;
        }

        return null;
	}

	/**
	 * 撤销指定的partition
	 * @param lostPartitions 需要撤销的partition
	 * @return 撤销中出现的异常
	 */
	private Exception invokePartitionsLost(final Set<TopicPartition> lostPartitions) {
		log.info("Lost previously assigned partitions {}", Utils.join(lostPartitions, ", "));
		// 再平衡事件的listener
		ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
		try {
			// 使用listener执行撤销操作
			listener.onPartitionsLost(lostPartitions);
			// 出现异常返回异常，正常情况下返回null
		} catch (WakeupException | InterruptException e) {
			throw e;
		} catch (Exception e) {
			log.error("User provided listener {} failed on invocation of onPartitionsLost for partitions {}",
					listener.getClass().getName(), lostPartitions, e);
			return e;
		}

		return null;
	}

    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        log.debug("Executing onJoinComplete with generation {} and memberId {}", generation, memberId);

        // only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        if (!isLeader)
            assignmentSnapshot = null;

        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        Set<TopicPartition> assignedPartitions = new HashSet<>(assignment.partitions());

        if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
            log.warn("We received an assignment {} that doesn't match our current subscription {}; it is likely " +
                "that the subscription has changed since we joined the group. Will try re-join the group with current subscription",
                assignment.partitions(), subscriptions.prettyString());

            requestRejoin();

            return;
        }

        // The leader may have assigned partitions which match our subscription pattern, but which
        // were not explicitly requested, so we update the joined subscription here.
        maybeUpdateJoinedSubscription(assignedPartitions);

        // give the assignor a chance to update internal state based on the received assignment
        ConsumerGroupMetadata metadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, generation, memberId, rebalanceConfig.groupInstanceId);
        assignor.onAssignment(assignment, metadata);

        // reschedule the auto commit starting from now
        if (autoCommitEnabled)
            this.nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);

        // execute the user's callback after rebalance
        final AtomicReference<Exception> firstException = new AtomicReference<>(null);
        Set<TopicPartition> addedPartitions = new HashSet<>(assignedPartitions);
        addedPartitions.removeAll(ownedPartitions);

        switch (protocol) {
            case EAGER:
                // assign partitions that are not yet owned
                subscriptions.assignFromSubscribed(assignedPartitions);

                firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions));

                break;

            case COOPERATIVE:
                Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
                revokedPartitions.removeAll(assignedPartitions);

                log.info("Updating with newly assigned partitions: {}, compare with already owned partitions: {}, " +
                        "newly added partitions: {}, revoking partitions: {}",
                    Utils.join(assignedPartitions, ", "),
                    Utils.join(ownedPartitions, ", "),
                    Utils.join(addedPartitions, ", "),
                    Utils.join(revokedPartitions, ", "));

                // revoke partitions that was previously owned but no longer assigned;
                // note that we should only change the assignment AFTER we've triggered
                // the revoke callback
                if (!revokedPartitions.isEmpty()) {
                    firstException.compareAndSet(null, invokePartitionsRevoked(revokedPartitions));
                }

                subscriptions.assignFromSubscribed(assignedPartitions);

                // add partitions that were not previously owned but are now assigned
                firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions));

                // if revoked any partitions, need to re-join the group afterwards
                if (!revokedPartitions.isEmpty()) {
                    requestRejoin();
                }

                break;
        }

        if (firstException.get() != null)
            throw new KafkaException("User rebalance callback throws an error", firstException.get());
	}

	/**
	 * 更新订阅的metadata
	 */
	void maybeUpdateSubscriptionMetadata() {
		// 获取更新版本
		int version = metadata.updateVersion();
		// 版本控制，避免回退
		if (version > metadataSnapshot.version) {
			// 拉拉取集群信息
			Cluster cluster = metadata.fetch();
			// 如果开启的订阅模式是根据正则表达式分配分区
			if (subscriptions.hasPatternSubscription())
				// 更新正则表达式的订阅内容
				updatePatternSubscription(cluster);

			// 更新当前的快照
			// 快照用于检查需要再平衡的订阅变更（比如出现了新分区）
			metadataSnapshot = new MetadataSnapshot(subscriptions, cluster, version);
		}
	}

	/**
	 * 拉取协调者的事件，它确认了协调者是被集群知道的，consumer已经加入到了消费组中（如果开启了消费组管理）
	 * 它同时也处理了定期的offset提交
	 * 如果超过了超时时间，则提早返回
	 * @param timer poll()方法的等待时间
	 * @throws KafkaException 如果再平衡器的回调任务出现了异常
	 * @return poll()结果
     */
    public boolean poll(Timer timer) {
		// 更新订阅的metadata
        maybeUpdateSubscriptionMetadata();
		// 调用已完成的提交offset任务回调
        invokeCompletedOffsetCommitCallbacks();
		// 如果是自动分配partition
        if (subscriptions.partitionsAutoAssigned()) {
			// 但是partition的协议为null，抛出异常
            if (protocol == null) {
                throw new IllegalStateException("User configured " + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG +
                    " to empty while trying to subscribe for group protocol to auto assign partitions");
			}
			// 自动分配的情况下，需要每次更新最近一次心跳拉取的时间，因此即使找不到协调器，心跳线程也不会由于应用程序不活动而主动离开
            pollHeartbeat(timer.currentTimeMs());
			// 如果当前协调器处于未知状态，或者在规定时间内没有处于就绪状态，直接返回轮询失败
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
			}
			// 是否需要重新加入消费组或者挂起
            if (rejoinNeededOrPending()) {
				// 订阅模式是正则表达式匹配模式
				// 由于处于初始化拉取metadata和初始化再平衡器之间的条件竞争，我们需要确认在加入消费组初始化之前，metadata是最新的
				// 将会确保在加入之前，模式和topic进行了至少一次匹配
                if (subscriptions.hasPatternSubscription()) {
					// 基于消费组采用的是正则表达式匹配的订阅模式，在一个topic被创建后，任何一个consumer在metadata刷新后刷新后发现了新的topic都会触发再平衡操作
					// 如果consumer在截然不同的时间进行多次刷新metadata，那么就会产生多次再平衡
					// 我们可以显示地减少因为简单的topic创建而产生的再平衡操作，通过要求consumer在重新加入消费组之前书安心metadata，只要刷新的下一次充时间已经过去
                    if (this.metadata.timeToAllowUpdate(timer.currentTimeMs()) == 0) {
						// 正则表达式订阅模式下，允许请求更新的情况下，请求更新metadata
                        this.metadata.requestUpdate();
					}
					// 如果并非是最新的metadata，返回false
                    if (!client.ensureFreshMetadata(timer)) {
                        return false;
					}
					// 更新订阅信息中的metadata
                    maybeUpdateSubscriptionMetadata();
				}
				// 并非是最新的活跃
                if (!ensureActiveGroup(timer)) {
                    return false;
                }
            }
        } else {
            // For manually assigned partitions, if there are no ready nodes, await metadata.
            // If connections to all nodes fail, wakeups triggered while attempting to send fetch
            // requests result in polls returning immediately, causing a tight loop of polls. Without
            // the wakeup, poll() with no channels would block for the timeout, delaying re-connection.
            // awaitMetadataUpdate() initiates new connections with configured backoff and avoids the busy loop.
            // When group management is used, metadata wait is already performed for this scenario as
            // coordinator is unknown, hence this check is not required.
            if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
                client.awaitMetadataUpdate(timer);
            }
        }

        maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
        return true;
    }

    /**
     * Return the time to the next needed invocation of {@link #poll(Timer)}.
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        return Math.min(nextAutoCommitTimer.remainingMs(), timeToNextHeartbeat(now));
    }

    private void updateGroupSubscription(Set<String> topics) {
        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        if (this.subscriptions.groupSubscribe(topics))
            metadata.requestUpdateForNewTopics();

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        if (!client.ensureFreshMetadata(time.timer(Long.MAX_VALUE)))
            throw new TimeoutException();

        maybeUpdateSubscriptionMetadata();
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        List<JoinGroupResponseData.JoinGroupResponseMember> allSubscriptions) {
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();

        // collect all the owned partitions
        Map<String, List<TopicPartition>> ownedPartitions = new HashMap<>();

        for (JoinGroupResponseData.JoinGroupResponseMember memberSubscription : allSubscriptions) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(memberSubscription.metadata()));
            subscription.setGroupInstanceId(Optional.ofNullable(memberSubscription.groupInstanceId()));
            subscriptions.put(memberSubscription.memberId(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
            ownedPartitions.put(memberSubscription.memberId(), subscription.ownedPartitions());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        updateGroupSubscription(allSubscribedTopics);

        isLeader = true;

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignor.name(), subscriptions);

        Map<String, Assignment> assignments = assignor.assign(metadata.fetch(), new GroupSubscription(subscriptions)).groupAssignment();

        if (protocol == RebalanceProtocol.COOPERATIVE) {
            validateCooperativeAssignment(ownedPartitions, assignments);
        }

        // user-customized assignor may have created some topics that are not in the subscription list
        // and assign their partitions to the members; in this case we would like to update the leader's
        // own metadata with the newly added topics so that it will not trigger a subsequent rebalance
        // when these topics gets updated from metadata refresh.
        //
        // TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
        //       we may need to modify the PartitionAssignor API to better support this case.
        Set<String> assignedTopics = new HashSet<>();
        for (Assignment assigned : assignments.values()) {
            for (TopicPartition tp : assigned.partitions())
                assignedTopics.add(tp.topic());
        }

        if (!assignedTopics.containsAll(allSubscribedTopics)) {
            Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
            notAssignedTopics.removeAll(assignedTopics);
            log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
        }

        if (!allSubscribedTopics.containsAll(assignedTopics)) {
            Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
            newlyAddedTopics.removeAll(allSubscribedTopics);
            log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

            allSubscribedTopics.addAll(assignedTopics);
            updateGroupSubscription(allSubscribedTopics);
        }

        assignmentSnapshot = metadataSnapshot;

        log.debug("Finished assignment for group: {}", assignments);

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignments.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    /**
     * Used by COOPERATIVE rebalance protocol only.
     *
     * Validate the assignments returned by the assignor such that no owned partitions are going to
     * be reassigned to a different consumer directly: if the assignor wants to reassign an owned partition,
     * it must first remove it from the new assignment of the current owner so that it is not assigned to any
     * member, and then in the next rebalance it can finally reassign those partitions not owned by anyone to consumers.
     */
    private void validateCooperativeAssignment(final Map<String, List<TopicPartition>> ownedPartitions,
                                               final Map<String, Assignment> assignments) {
        Set<TopicPartition> totalRevokedPartitions = new HashSet<>();
        Set<TopicPartition> totalAddedPartitions = new HashSet<>();
        for (final Map.Entry<String, Assignment> entry : assignments.entrySet()) {
            final Assignment assignment = entry.getValue();
            final Set<TopicPartition> addedPartitions = new HashSet<>(assignment.partitions());
            addedPartitions.removeAll(ownedPartitions.get(entry.getKey()));
            final Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions.get(entry.getKey()));
            revokedPartitions.removeAll(assignment.partitions());

            totalAddedPartitions.addAll(addedPartitions);
            totalRevokedPartitions.addAll(revokedPartitions);
        }

        // if there are overlap between revoked partitions and added partitions, it means some partitions
        // immediately gets re-assigned to another member while it is still claimed by some member
        totalAddedPartitions.retainAll(totalRevokedPartitions);
        if (!totalAddedPartitions.isEmpty()) {
            log.error("With the COOPERATIVE protocol, owned partitions cannot be " +
                "reassigned to other members; however the assignor has reassigned partitions {} which are still owned " +
                "by some members; return the error code to all members to let them stop", totalAddedPartitions);

            throw new IllegalStateException("Assignor supporting the COOPERATIVE protocol violates its requirements");
		}
	}

	/**
	 * 加入前的准备工作
	 * @param generation 上一代协调器的版本信息，如果是首次加入准备，返回-1
	 * @param memberId 该consumer在先前消费组的身份标识，如果是首次创建的consumer，身份标识为""
	 */
	@Override
	protected void onJoinPrepare(int generation, String memberId) {
		log.debug("Executing onJoinPrepare with generation {} and memberId {}", generation, memberId);
		// 开启自动提交的情况下，在再平衡之前提交offset
		maybeAutoCommitOffsetsSync(time.timer(rebalanceConfig.rebalanceTimeoutMs));

		// 在获取到错误或者心跳超时的情况下，心跳线程可以重置generation和memberId
		// 在这种情况下，无论之前拥有的什么partition都会丢失，我们将会触发回调任务，并且清空已分配的partition
		// 否则我们可以正常运行并撤销依赖协议的partition，并在这种情况下我们只能在撤销回调任务之后，修改分配方法
		// 以此，开发者仍可以访问之前拥有的partition，并提交offset
		Exception exception = null;
		// 需要撤销的partition
		final Set<TopicPartition> revokedPartitions;
		// 如果既没有generation，也没有memberId
		if (generation == Generation.NO_GENERATION.generationId &&
				memberId.equals(Generation.NO_GENERATION.memberId)) {
			// 所有已分配的partition都将被撤销
			revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());
			// 需要撤销的partition集合不为空，撤销partition
			if (!revokedPartitions.isEmpty()) {
				log.info("Giving away all assigned partitions as lost since generation has been reset," +
						"indicating that consumer is no longer part of the group");
				// 撤销partition
				exception = invokePartitionsLost(revokedPartitions);
				// 取消所有已订阅
				subscriptions.assignFromSubscribed(Collections.emptySet());
			}
		} else {
			// 存在generation或者memberId的情况下
			// 根据协议进行撤销
			switch (protocol) {
				// 独占模式
				case EAGER:
					// 撤销所有的partition
					revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());
					exception = invokePartitionsRevoked(revokedPartitions);

					subscriptions.assignFromSubscribed(Collections.emptySet());

					break;
				// 协作模式
				case COOPERATIVE:
					// 只会撤销那些不在当前订阅信息中的partition
					Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
					revokedPartitions = ownedPartitions.stream()
							.filter(tp -> !subscriptions.subscription().contains(tp.topic()))
							.collect(Collectors.toSet());

					if (!revokedPartitions.isEmpty()) {
						exception = invokePartitionsRevoked(revokedPartitions);

						ownedPartitions.removeAll(revokedPartitions);
						subscriptions.assignFromSubscribed(ownedPartitions);
					}

					break;
			}
		}


		isLeader = false;
		// 重置消费组订阅
		subscriptions.resetGroupSubscription();

		if (exception != null) {
			throw new KafkaException("User rebalance callback throws an error", exception);
		}
	}

    @Override
    public void onLeavePrepare() {
        // we should reset assignment and trigger the callback before leaving group
        Set<TopicPartition> droppedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        if (subscriptions.partitionsAutoAssigned() && !droppedPartitions.isEmpty()) {
            final Exception e = invokePartitionsRevoked(droppedPartitions);

            subscriptions.assignFromSubscribed(Collections.emptySet());

            if (e != null) {
                throw new KafkaException("User rebalance callback throws an error", e);
            }
        }
	}

	/**
	 * 是否需要重新加入消费或是挂起线程
	 * @throws KafkaException 回调任务抛出异常
     */
    @Override
    public boolean rejoinNeededOrPending() {
        if (!subscriptions.partitionsAutoAssigned())
            return false;

		// 如果是开发者手动进行分配，并且metadata发生了变化，我们需要重新加入消费组
		// 对于那些已经拥有，但是实际上不存在的partition，也需要丢弃它们
        if (assignmentSnapshot != null && !assignmentSnapshot.matches(metadataSnapshot))
            return true;

		// 在上次加入到消费组之后，订阅的数据发生了变化
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription())) {
            return true;
		}
		// 使用基本判断是否需要重新加入
        return super.rejoinNeededOrPending();
	}

	/**
	 * 刷新partition已经提交的commit
	 * @param timer 当前方法可以阻塞等待的时间
	 * @return 如果在等待时间内执行完成，返回true
     */
    public boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> missingFetchPositions = subscriptions.missingFetchPositions();

        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(missingFetchPositions, timer);
        if (offsets == null) return false;

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetAndMetadata offsetAndMetadata = entry.getValue();
            final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.leaderAndEpoch(tp);
            final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                    offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),
                    leaderAndEpoch);

            log.info("Setting offset for partition {} to the committed offset {}", tp, position);
            entry.getValue().leaderEpoch().ifPresent(epoch -> this.metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));
            this.subscriptions.seekUnvalidated(tp, position);
        }
        return true;
    }

    /**
	 * 使用协调者拉取指定partition集合提交的offset
	 * @param partitions 需要拉取的partition集合
	 * @return partition和提交的offset和metadata映射集合，如果超过了指定的等待时间，返回null
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(final Set<TopicPartition> partitions,
                                                                        final Timer timer) {
		// 如果需要拉取的partition集合为空，不进行拉取
        if (partitions.isEmpty()) return Collections.emptyMap();
		// 获取当前拉取的版本
        final Generation generation = generation();
        if (pendingCommittedOffsetRequest != null && !pendingCommittedOffsetRequest.sameRequest(partitions, generation)) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null;
        }

		// 在未超时的情况下，进行轮询
        do {
			// 确认协调者处于就绪状态
            if (!ensureCoordinatorReady(timer)) return null;

			// 构建拉取offset请求，并向协调者发起请求
            final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future;
			// 如果已经发起了一次拉取offset请求，直接等待此请求的响应即可
            if (pendingCommittedOffsetRequest != null) {
                future = pendingCommittedOffsetRequest.response;
            } else {
				// 之前没有发起请求，直接发起请求
                future = sendOffsetFetchRequest(partitions);
                pendingCommittedOffsetRequest = new PendingCommittedOffsetRequest(partitions, generation, future);
            }
			// 轮询拉取offset响应
            client.poll(future, timer);
			// 如果响应完成
            if (future.isDone()) {
				// 重置挂起等待拉取offset响应的请求
                pendingCommittedOffsetRequest = null;
				// 如果响应是成功的结果，返回拉取的内容
                if (future.succeeded()) {
                    return future.value();
                } else if (!future.isRetriable()) {
					// 如果响应是失败的结果，并且不可以进行重试，抛出响应返回的异常
                    throw future.exception();
                } else {
					// 如果响应时失败的结果，并且可以进行重试，计时器等待到下一次进行重试的时间
                    timer.sleep(rebalanceConfig.retryBackoffMs);
                }
            } else {
                return null;
            }
        } while (timer.notExpired());
        return null;
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public void close(final Timer timer) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync(timer);
            while (pendingAsyncCommits.get() > 0 && timer.notExpired()) {
                ensureCoordinatorReady(timer);
                client.poll(timer);
                invokeCompletedOffsetCommitCallbacks();
            }
        } finally {
            super.close(timer);
        }
    }

    // visible for testing

	/**
	 * 调用已完成offset提交的回调任务
	 */
	void invokeCompletedOffsetCommitCallbacks() {
		// 获取异步提交栅栏，如果存在栅栏，视为不正常情况，抛出异常
		if (asyncCommitFenced.get()) {
			throw new FencedInstanceIdException("Get fenced exception for group.instance.id "
					+ rebalanceConfig.groupInstanceId.orElse("unset_instance_id")
					+ ", current member.id is " + memberId());
		}
		// 不存在提交栅栏的情况下，处理已完成的offset提交的任务的回调
		while (true) {
			OffsetCommitCompletion completion = completedOffsetCommits.poll();
			if (completion == null) {
				break;
			}
			completion.invoke();
		}
	}

	/**
	 * 异步提交offset
	 * @param offsets  以topic-partition为维度，需要进行提交的offset集合
	 * @param callback 提交后的回调任务
	 */
	public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
		// 调用已完成offset提交的回调任务
		invokeCompletedOffsetCommitCallbacks();
		// 如果协调器状态不处于未知状态
		if (!coordinatorUnknown()) {
			// 执行异步提交offset
			doCommitOffsetsAsync(offsets, callback);
		} else {
			// 如果没有协调者，也就代表没有提交offset的请求目标
			// 我们需要尝试去寻找协调者，或者执行失败操作
			// 我们不希望递归重试操作，因为这可能导致offset提交的顺序打乱
			// 需要注意的是，对于同一个协调者，可能会有多个offset提交链
			// 这是可以接受的，因为listener的顺序就是提交的顺序
			// 还需要注意的是，AbstractCoordinator会保护多个协调者的并发请求
			pendingAsyncCommits.incrementAndGet();
			// 向查找协调者的异步结果中添加listener
			lookupCoordinator().addListener(new RequestFutureListener<Void>() {
				/**
				 * 请求成功
				 * @param value
				 */
				@Override
				public void onSuccess(Void value) {
					// 挂起的异步提交数量-1
					pendingAsyncCommits.decrementAndGet();
					// 异步执行commit提交
					doCommitOffsetsAsync(offsets, callback);
					// 不触发唤醒的情况下，进行一次轮询
					client.pollNoWakeup();
				}

				/**
				 * 请求失败
				 * @param e
				 */
				@Override
				public void onFailure(RuntimeException e) {
					// 也进行挂起的异步提交数量-1
					pendingAsyncCommits.decrementAndGet();
					// 同时添加已完成的offset提交任务
					completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
							new RetriableCommitFailedException(e)));
				}
			});
		}

		// 确认当前提交有机会进行传输，避免阻塞
		// 需要注意的是，提交将会被协调者判定为心跳连接，因为无需通过延迟任务的执行来明确允许心跳
		client.pollNoWakeup();
	}

	/**
	 * 执行异步提交offset
	 * @param offsets  需要提交变异量的集合
	 * @param callback 提交后需要执行的回调任务
	 */
	private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
		// 发送提交offset请求
		RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
		// 没有指定提交后的回调任务，使用默认的offset提交回调任务
		final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
		future.addListener(new RequestFutureListener<Void>() {
			/**
			 * 提交成功处理
			 * @param value
			 */
			@Override
			public void onSuccess(Void value) {
				// 执行拦截器操作
				if (interceptors != null)
					interceptors.onCommit(offsets);
				// 已完成的offset提交添加新处理完成的offset提交
				completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
			}

			/**
			 * 提交失败处理
			 * @param e
			 */
			@Override
			public void onFailure(RuntimeException e) {
				Exception commitException = e;

				if (e instanceof RetriableException) {
					commitException = new RetriableCommitFailedException(e);
				}
				// 提交失败，也放入到已完成提交的offset集合中
				completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
				// 如果返回FencedInstanceIdException异常，设置异步提交栅栏
				if (commitException instanceof FencedInstanceIdException) {
					asyncCommitFenced.set(true);
				}
			}
		});
	}

	/**
	 * 同步提交offset
	 * 当前方法在提交成功之前一直重试，或者出现了一个不可恢复的错误
	 * @param offsets 需要提交的偏移量集合
	 * @return 如果提交offset任务成功发送，并且从协调器接收到了成功的响应
	 * @throws org.apache.kafka.common.errors.AuthorizationException 如果consumer没有被消费组或者其他任何指定的partition认证
	 * @throws CommitFailedException                                 在提交offset任务之前发生了不可恢复的错误
	 * @throws FencedInstanceIdException                             静态成员收到了栅栏
	 */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
		// 调用上次完成提交offset的回调任务
        invokeCompletedOffsetCommitCallbacks();
		// 如果需要提交的offset为空，直接返回提交成功
        if (offsets.isEmpty())
            return true;
		// 在计时器范围内，进行轮询
		do {
			// 如果协调者处于未知状态
			// 或者协调者状态没有处于未知状态，但是没有处于就绪状态
			// 返回false
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
			}
			// 发送提交offset请求，并在剩余等待时间内轮询结果
            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, timer);

			// 再次调用提交提交offset的回调任务
			// 前一次调用为了保证这一次调用的顺序
            invokeCompletedOffsetCommitCallbacks();

            if (future.succeeded()) {
				// 响应成功，执行拦截器完成提交操作
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return true;
			}
			// 响应返回失败，并且处于不可重试状态，直接抛出响应返回的异常
            if (future.failed() && !future.isRetriable())
                throw future.exception();
			// 响应返回失败，但可以重试，等待到下次重试的时间节点，进行重试
            timer.sleep(rebalanceConfig.retryBackoffMs);
        } while (timer.notExpired());

		return false;
	}

	/**
	 * 异步提交offset
	 * @param now 当前时间戳
	 */
	public void maybeAutoCommitOffsetsAsync(long now) {
		// 在开启自动提交的情况下
		if (autoCommitEnabled) {
			//
			nextAutoCommitTimer.update(now);
			if (nextAutoCommitTimer.isExpired()) {
				nextAutoCommitTimer.reset(autoCommitIntervalMs);
				doAutoCommitOffsetsAsync();
			}
		}
	}

	/**
	 * 执行异步自动提交offset
	 */
	private void doAutoCommitOffsetsAsync() {
		// 获取所有已消费的分区offset记录
		Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
		log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);
		// 异步提交offset，已经异常的回调处理
		commitOffsetsAsync(allConsumedOffsets, (offsets, exception) -> {
			if (exception != null) {
				if (exception instanceof RetriableException) {
					log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", offsets,
							exception);
					nextAutoCommitTimer.updateAndReset(rebalanceConfig.retryBackoffMs);
				} else {
					log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, exception.getMessage());
				}
			} else {
				log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
			}
		});
	}

	/**
	 * 在开启自动提交的前提下，异步自动提交offset
	 * @param timer 等待时间计时器
	 */
	private void maybeAutoCommitOffsetsSync(Timer timer) {
		if (autoCommitEnabled) {
			// 获取所有进行消费的offset和metadata信息
			Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
			try {
				log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets);
				// 提交进行消费的offset和metadata信息
				if (!commitOffsetsSync(allConsumedOffsets, timer))
					log.debug("Auto-commit of offsets {} timed out before completion", allConsumedOffsets);
			} catch (WakeupException | InterruptException e) {
				log.debug("Auto-commit of offsets {} was interrupted before completion", allConsumedOffsets);
				// rethrow wakeups since they are triggered by the user
				throw e;
			} catch (Exception e) {
				// consistent with async auto-commit failures, we do not propagate the exception
				log.warn("Synchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, e.getMessage());
			}
		}
	}

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
	}

	/**
	 * 提交指定partition的offset，返回一个请求的异步结果，在同步提交的情况下可以轮询这个请求，在异步的情况下可以将其忽略
	 * 是一个非阻塞的调用，会返回一个
	 * @param offsets 需要提交的partition的offset
	 * @return 请求提交的异步任务
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
		// 如果没有需要提交的offset
        if (offsets.isEmpty())
			// 直接返回请求成功的异步结果
			return RequestFuture.voidSuccess();
		// 获取协调者节点
        Node coordinator = checkAndGetCoordinator();
		if (coordinator == null)
			// 不存在协调者，返回协调者不可用异步结果
			return RequestFuture.coordinatorNotAvailable();

		// 创建offset提交请求
		// topic维度，记录的partition的offset提交数据信息
        Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
			if (offsetAndMetadata.offset() < 0) {
				// 对于异常的offset情况，返回IllegalArgumentException异常异步结果
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
			}
			// 以topic维度，获取需要提交topic的partition列表
			// 如果没有指定topic，则创建对应topic的第一个partition提交offset信息，放入的map中
            OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(),
                            new OffsetCommitRequestData.OffsetCommitRequestTopic()
									.setName(topicPartition.topic())
					);
			// 添加需要提交的数据信息，是向列表中增量添加提交的数据信息
            topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
					// partition索引
					.setPartitionIndex(topicPartition.partition())
					// 需要提交的partition的offset
					.setCommittedOffset(offsetAndMetadata.offset())
					// 设置提交的leader epoch
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
					// 设置提交metadata
                    .setCommittedMetadata(offsetAndMetadata.metadata())
			);
			// 重新覆盖
            requestTopicDataMap.put(topicPartition.topic(), topic);
		}
		// 获取提交版本
		final Generation generation;
		// 如果是动态分配partition
		if (subscriptions.partitionsAutoAssigned()) {
			// 获取当前消费组的版本
			generation = generation();
			// 如果消费组的版本为null，证明consumer不在集群中，或者消费组处于再平衡状态
			// 我们唯一能做的就是失败掉当前的commit，然后让开发者在poll()方法中重新加入消费组
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group");
                return RequestFuture.failure(new CommitFailedException());
			}
        } else
			// 如果是手动非配的，不需要有版本的概念
			generation = Generation.NO_GENERATION;
		// 构建offset提交请求
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                new OffsetCommitRequestData()
                        .setGroupId(this.rebalanceConfig.groupId)
                        .setGenerationId(generation.generationId)
                        .setMemberId(generation.memberId)
                        .setGroupInstanceId(rebalanceConfig.groupInstanceId.orElse(null))
                        .setTopics(new ArrayList<>(requestTopicDataMap.values()))
        );

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);
		// 向指定的协调者发送提交offset请求，并声明offset提交响应的处理器
        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                    OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);

                    long offset = offsetAndMetadata.offset();

                    Errors error = Errors.forCode(partition.errorCode());
                    if (error == Errors.NONE) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                    } else {
                        if (error.exception() instanceof RetriableException) {
                            log.warn("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        } else {
                            log.error("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        }

                        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                            future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                            return;
                        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                            unauthorizedTopics.add(tp.topic());
                        } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                                || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                            // raise the error to the user
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS
                                || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                            // just retry
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                                || error == Errors.NOT_COORDINATOR
                                || error == Errors.REQUEST_TIMED_OUT) {
                            markCoordinatorUnknown();
                            future.raise(error);
                            return;
                        } else if (error == Errors.FENCED_INSTANCE_ID) {
                            log.error("Received fatal exception: group.instance.id gets fenced");
                            future.raise(error);
                            return;
                        } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                            /* Consumer never tries to commit offset in between join-group and sync-group,
                             * and hence on broker-side it is not expected to see a commit offset request
                             * during CompletingRebalance phase; if it ever happens then broker would return
                             * this error. In this case we should just treat as a fatal CommitFailed exception.
                             * However, we do not need to reset generations and just request re-join, such that
                             * if the caller decides to proceed and poll, it would still try to proceed and re-join normally.
                             */
                            requestRejoin();
                            future.raise(new CommitFailedException());
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            // need to reset generation and re-join group
                            resetGenerationOnResponseError(ApiKeys.OFFSET_COMMIT, error);
                            future.raise(new CommitFailedException());
                            return;
                        } else {
                            future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                            return;
                        }
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {}", unauthorizedTopics);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder = new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId,
                new ArrayList<>(partitions));

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            if (response.hasError()) {
                Errors error = response.error();
                log.debug("Offset fetch failed: {}", error.message());

                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(error);
                } else if (error == Errors.NOT_COORDINATOR) {
                    // re-discover the coordinator and retry
                    markCoordinatorUnknown();
                    future.raise(error);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                }
                return;
            }

            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    Errors error = data.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response for partition " +
                            tp + ": " + error.message()));
                        return;
                    }
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.leaderEpoch, data.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                }
            }

            if (unauthorizedTopics != null) {
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(offsets);
            }
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitLatency;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final int version;
        private final Map<String, Integer> partitionsPerTopic;

        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster, int version) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription()) {
                Integer numPartitions = cluster.partitionCountForTopic(topic);
                if (numPartitions != null)
                    partitionsPerTopic.put(topic, numPartitions);
            }
            this.partitionsPerTopic = partitionsPerTopic;
            this.version = version;
        }

        boolean matches(MetadataSnapshot other) {
            return version == other.version || partitionsPerTopic.equals(other.partitionsPerTopic);
        }

        Map<String, Integer> partitionsPerTopic() {
            return partitionsPerTopic;
        }
    }

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }

    /* test-only classes below */
    RebalanceProtocol getProtocol() {
        return protocol;
    }
}
