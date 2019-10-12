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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seekValidated(TopicPartition, FetchPosition)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * Thread Safety: this class is thread-safe.
 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    private final Logger log;

	/**
	 * 消费组需要注册的主题，可能会包括一些在订阅topic列表中不存在的topic
	 * 这样做的原因是它有责任在进行一个消费组的再平衡时，保护元数据的变化
	 */
	private Set<String> groupSubscription;

    /* the type of subscription */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    private Set<String> subscription;

	public synchronized boolean subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
		// 注册再平衡listener
		registerRebalanceListener(listener);
		// 设置订阅类型为自动注册指定的topic
		setSubscriptionType(SubscriptionType.AUTO_TOPICS);
		// 更新订阅的topic
		return changeSubscription(topics);
    }

	/**
	 * 当前已分配的partition状态集合
	 * 需要注意的是，分区的顺序很重要
	 * 详细请看FetchBuilder
	 */
    private final PartitionStates<TopicPartitionState> assignment;

    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;

    /* User-provided listener to be invoked when assignment changes */
    private ConsumerRebalanceListener rebalanceListener;

    private int assignmentId = 0;

    @Override
    public synchronized String toString() {
        return "SubscriptionState{" +
            "type=" + subscriptionType +
            ", subscribedPattern=" + subscribedPattern +
            ", subscription=" + String.join(",", subscription) +
            ", groupSubscription=" + String.join(",", groupSubscription) +
            ", defaultResetStrategy=" + defaultResetStrategy +
            ", assignment=" + assignment.partitionStateValues() + " (id=" + assignmentId + ")}";
    }

    public synchronized String prettyString() {
        switch (subscriptionType) {
            case NONE:
                return "None";
            case AUTO_TOPICS:
                return "Subscribe(" + String.join(",", subscription) + ")";
            case AUTO_PATTERN:
                return "Subscribe(" + subscribedPattern + ")";
            case USER_ASSIGNED:
                return "Assign(" + assignedPartitions() + " , id=" + assignmentId + ")";
            default:
                throw new IllegalStateException("Unrecognized subscription type: " + subscriptionType);
        }
    }

    public SubscriptionState(LogContext logContext, OffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * Monotonically increasing id which is incremented after every assignment change. This can
     * be used to check when an assignment has changed.
     *
     * @return The current assignment Id
     */
    synchronized int assignmentId() {
        return assignmentId;
    }

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
	}

	/**
	 * 更新订阅的topic集合
	 * @param topicsToSubscribe 需要订阅的topic集合
	 * @return 是否发生更新
	 */
	private boolean changeSubscription(Set<String> topicsToSubscribe) {
		// 如果订阅topic没有发生变化，返回false
		if (subscription.equals(topicsToSubscribe))
			return false;
		// 置换需要注册的topic
		subscription = topicsToSubscribe;
		// 如果订阅类型是Kafka分配的模式
		if (subscriptionType != SubscriptionType.USER_ASSIGNED) {
			// 增量添加新订阅的topic，不进行冗余处理
			groupSubscription = new HashSet<>(groupSubscription);
			groupSubscription.addAll(topicsToSubscribe);
		} else {
			// 手动模式下，直接替换为新订阅的topic
			groupSubscription = new HashSet<>(topicsToSubscribe);
		}
		return true;
	}

	public synchronized void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
		registerRebalanceListener(listener);
		setSubscriptionType(SubscriptionType.AUTO_PATTERN);
		this.subscribedPattern = pattern;
	}

	public synchronized boolean subscribeFromPattern(Set<String> topics) {
		if (subscriptionType != SubscriptionType.AUTO_PATTERN)
			throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
					subscriptionType);

		return changeSubscription(topics);
	}

	/**
	 * 当前消费者订阅的消费分区的拷贝
	 * @return 消费分区信息的拷贝
	 */
	public synchronized Set<TopicPartition> assignedPartitions() {
		return new HashSet<>(this.assignment.partitionSet());
	}

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     * @param topics The topics to add to the group subscription
     */
    synchronized boolean groupSubscribe(Collection<String> topics) {
        if (!partitionsAutoAssigned())
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        groupSubscription = new HashSet<>(groupSubscription);
        return groupSubscription.addAll(topics);
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    synchronized void resetGroupSubscription() {
        groupSubscription = subscription;
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public synchronized boolean assignFromUser(Set<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        if (this.assignment.partitionSet().equals(partitions))
            return false;

        assignmentId++;

        Set<String> manualSubscribedTopics = new HashSet<>();
        Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
        for (TopicPartition partition : partitions) {
            TopicPartitionState state = assignment.stateValue(partition);
            if (state == null)
                state = new TopicPartitionState();
            partitionToState.put(partition, state);
            manualSubscribedTopics.add(partition.topic());
        }
        this.assignment.set(partitionToState);
        return changeSubscription(manualSubscribedTopics);
    }

    /**
     * @return true if assignments matches subscription, otherwise false
     */
    public synchronized boolean checkAssignmentMatchedSubscription(Collection<TopicPartition> assignments) {
        for (TopicPartition topicPartition : assignments) {
            if (this.subscribedPattern != null) {
                if (!this.subscribedPattern.matcher(topicPartition.topic()).matches()) {
                    log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                        topicPartition,
                        this.subscribedPattern);

                    return false;
                }
            } else {
                if (!this.subscription.contains(topicPartition.topic())) {
                    log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator, note this is
     * different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs.
     */
    public synchronized void assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.partitionsAutoAssigned())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");


        Map<TopicPartition, TopicPartitionState> assignedPartitionStates = partitionToStateMap(assignments);
        assignmentId++;
        this.assignment.set(assignedPartitionStates);
    }

    private void registerRebalanceListener(ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        this.rebalanceListener = listener;
    }

    /**
     * Check whether pattern subscription is in use.
     *
     */
    synchronized boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    public synchronized void unsubscribe() {
        this.subscription = Collections.emptySet();
        this.groupSubscription = Collections.emptySet();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
        this.assignmentId++;
    }

    /**
     * Check whether a topic matches a subscribed pattern.
     *
     * @return true if pattern subscription is in use and the topic matches the subscribed pattern, false otherwise
     */
    synchronized boolean matchesSubscribedPattern(String topic) {
        Pattern pattern = this.subscribedPattern;
        if (hasPatternSubscription() && pattern != null)
            return pattern.matcher(topic).matches();
        return false;
    }

    public synchronized Set<String> subscription() {
        if (partitionsAutoAssigned())
            return this.subscription;
        return Collections.emptySet();
    }

    public synchronized Set<TopicPartition> pausedPartitions() {
        return collectPartitions(TopicPartitionState::isPaused, Collectors.toSet());
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    synchronized Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    synchronized boolean isGroupSubscribed(String topic) {
        return groupSubscription.contains(topic);
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    private TopicPartitionState assignedStateOrNull(TopicPartition tp) {
        return this.assignment.stateValue(tp);
    }

    public synchronized void seekValidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekValidated(position);
    }

    public void seek(TopicPartition tp, long offset) {
        seekValidated(tp, new FetchPosition(offset));
	}

	/**
	 * 在不进行校验的情况下，进行提交
	 * @param tp
	 * @param position
	 */
	public void seekUnvalidated(TopicPartition tp, FetchPosition position) {
		// 获取当前当前的partition状态，在不进行校验的情况下，进行提交
		assignedState(tp).seekUnvalidated(position);
	}

    synchronized void maybeSeekUnvalidated(TopicPartition tp, long offset, OffsetResetStrategy requestedResetStrategy) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping reset of partition {} since it is no longer assigned", tp);
        } else if (!state.awaitingReset()) {
            log.debug("Skipping reset of partition {} since reset is no longer needed", tp);
        } else if (requestedResetStrategy != state.resetStrategy) {
            log.debug("Skipping reset of partition {} since an alternative reset has been requested", tp);
        } else {
            log.info("Resetting offset for partition {} to offset {}.", tp, offset);
            state.seekUnvalidated(new FetchPosition(offset));
		}
	}

	private enum SubscriptionType {
		NONE,
		/**
		 * 根据指定的topic进行订阅，自动分配分区
		 */
		AUTO_TOPICS,
		/**
		 * 按照指定的正则表达式匹配topic订阅，自动分配分区
		 */
		AUTO_PATTERN,
		/**
		 * 用户指定consumer的topic和分区
		 */
		USER_ASSIGNED
    }

    /**
     * @return a modifiable copy of the currently assigned partitions as a list
     */
    public synchronized List<TopicPartition> assignedPartitionsList() {
        return new ArrayList<>(this.assignment.partitionSet());
    }

    /**
     * Provides the number of assigned partitions in a thread safe manner.
     * @return the number of assigned partitions.
     */
    synchronized int numAssignedPartitions() {
        return this.assignment.size();
    }

    synchronized List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        return assignment.stream()
                .filter(tpState -> isAvailable.test(tpState.topicPartition()) && tpState.value().isFetchable())
                .map(PartitionStates.PartitionState::topicPartition)
                .collect(Collectors.toList());
    }

    synchronized boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized void position(TopicPartition tp, FetchPosition position) {
        assignedState(tp).position(position);
    }

    public synchronized boolean maybeValidatePositionForCurrentLeader(TopicPartition tp, Metadata.LeaderAndEpoch leaderAndEpoch) {
        return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
    }

    /**
     * Attempt to complete validation with the end offset returned from the OffsetForLeaderEpoch request.
     * @return The diverging offset if truncation was detected and no reset policy is defined.
     */
    public synchronized Optional<OffsetAndMetadata> maybeCompleteValidation(TopicPartition tp,
                                                                            FetchPosition requestPosition,
                                                                            EpochEndOffset epochEndOffset) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping completed validation for partition {} which is not currently assigned.", tp);
        } else if (!state.awaitingValidation()) {
            log.debug("Skipping completed validation for partition {} which is no longer expecting validation.", tp);
        } else {
            SubscriptionState.FetchPosition currentPosition = state.position;
            if (!currentPosition.equals(requestPosition)) {
                log.debug("Skipping completed validation for partition {} since the current position {} " +
                                "no longer matches the position {} when the request was sent",
                        tp, currentPosition, requestPosition);
            } else if (epochEndOffset.endOffset() < currentPosition.offset) {
                if (hasDefaultOffsetResetPolicy()) {
                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                            epochEndOffset.endOffset(), Optional.of(epochEndOffset.leaderEpoch()),
                            currentPosition.currentLeader);
                    log.info("Truncation detected for partition {} at offset {}, resetting offset to " +
                            "the first offset known to diverge {}", tp, currentPosition, newPosition);
                    state.seekValidated(newPosition);
                } else {
                    log.warn("Truncation detected for partition {} at offset {} (the end offset from the " +
                                    "broker is {}), but no reset policy is set",
                            tp, currentPosition, epochEndOffset);
                    return Optional.of(new OffsetAndMetadata(epochEndOffset.endOffset(),
                            Optional.of(epochEndOffset.leaderEpoch()), null));
                }
            } else {
                state.completeValidation();
            }
        }

        return Optional.empty();
    }

    public synchronized boolean awaitingValidation(TopicPartition tp) {
        return assignedState(tp).awaitingValidation();
    }

    public synchronized void completeValidation(TopicPartition tp) {
        assignedState(tp).completeValidation();
    }

    public synchronized FetchPosition validPosition(TopicPartition tp) {
        return assignedState(tp).validPosition();
    }

    public synchronized FetchPosition position(TopicPartition tp) {
        return assignedState(tp).position;
    }

	/**
	 * 获取消费积压情况
	 * @param tp             需要查看消费积压的分区
	 * @param isolationLevel 事务等级
	 * @return 消费积压情况
	 */
	synchronized Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
		// 获取当前topic-partition的状态
		TopicPartitionState topicPartitionState = assignedState(tp);
		// 如果开启了可重读事务等级
		if (isolationLevel == IsolationLevel.READ_COMMITTED)
			// 如果不存在上一次提交的offset，则没有积压
			// 如果存在上一次提交的offset，则积压数=上一次提交的offset-当前订阅中记录的offset（在fetch的时候，已经被更新过）
			return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
		else
			// 如果没有开启事务，则使用highWaterMark进行计算
			// 如果没有highWaterMark，则返回没有积压
			// 如果有highWaterMark，则积压数=当前消费者得到的highWaterMark-当前订阅中记录的offset（在fetch的时候，已经被更新过）
			return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
	}

	/**
	 * 计算目前为止，当前consumer已经消费指定topic-partition的offset
	 * @param tp 指定的topic-partition
	 * @return 当前consumer已经消费的offset
	 */
	synchronized Long partitionLead(TopicPartition tp) {
		TopicPartitionState topicPartitionState = assignedState(tp);
		return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
	}

    synchronized void updateHighWatermark(TopicPartition tp, long highWatermark) {
        assignedState(tp).highWatermark(highWatermark);
    }

    synchronized void updateLogStartOffset(TopicPartition tp, long logStartOffset) {
        assignedState(tp).logStartOffset(logStartOffset);
    }

    synchronized void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        assignedState(tp).lastStableOffset(lastStableOffset);
    }

    /**
     * Set the preferred read replica with a lease timeout. After this time, the replica will no longer be valid and
     * {@link #preferredReadReplica(TopicPartition, long)} will return an empty result.
     *
     * @param tp The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs The time at which this preferred replica is no longer valid
     */
    public synchronized void updatePreferredReadReplica(TopicPartition tp, int preferredReadReplicaId, Supplier<Long> timeMs) {
        assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
    }

    /**
     * Get the preferred read replica
     *
     * @param tp The topic partition
     * @param timeMs The current time
     * @return Returns the current preferred read replica, if it has been set and if it has not expired.
     */
    public synchronized Optional<Integer> preferredReadReplica(TopicPartition tp, long timeMs) {
        return assignedState(tp).preferredReadReplica(timeMs);
    }

    /**
     * Unset the preferred read replica. This causes the fetcher to go back to the leader for fetches.
     *
     * @param tp The topic partition
     * @return true if the preferred read replica was set, false otherwise.
     */
    public synchronized Optional<Integer> clearPreferredReadReplica(TopicPartition tp) {
        return assignedState(tp).clearPreferredReadReplica();
	}

	/**
	 * 获取所有参与消费的OffsetAndMetadata信息
	 * @return topic-partition维度的OffsetAndMetadata信息
	 */
	public synchronized Map<TopicPartition, OffsetAndMetadata> allConsumed() {
		Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
		// 遍历已经分配的partition，不能并发遍历，顺序很重要
		assignment.stream().forEach(state -> {
			TopicPartitionState partitionState = state.value();
			// 如果有合法的消费为止
			if (partitionState.hasValidPosition())
				// 构建offset信息，放入到已消费的集群中
				allConsumed.put(state.topicPartition(), new OffsetAndMetadata(partitionState.position.offset,
						partitionState.position.offsetEpoch, ""));
		});
		return allConsumed;
	}

    public synchronized void requestOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).reset(offsetResetStrategy);
    }

    public synchronized void requestOffsetReset(Collection<TopicPartition> partitions, OffsetResetStrategy offsetResetStrategy) {
        partitions.forEach(tp -> {
            log.info("Seeking to {} offset of partition {}", offsetResetStrategy, tp);
            assignedState(tp).reset(offsetResetStrategy);
        });
    }

    public void requestOffsetReset(TopicPartition partition) {
        requestOffsetReset(partition, defaultResetStrategy);
    }

    synchronized void setNextAllowedRetry(Set<TopicPartition> partitions, long nextAllowResetTimeMs) {
        for (TopicPartition partition : partitions) {
            assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs);
        }
    }

    boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public synchronized boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public synchronized OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy();
    }

    public synchronized boolean hasAllFetchPositions() {
        return assignment.stream().allMatch(state -> state.value().hasValidPosition());
    }

    public synchronized Set<TopicPartition> missingFetchPositions() {
        return collectPartitions(state -> !state.hasPosition(), Collectors.toSet());
    }

    private <T extends Collection<TopicPartition>> T collectPartitions(Predicate<TopicPartitionState> filter, Collector<TopicPartition, ?, T> collector) {
        return assignment.stream()
                .filter(state -> filter.test(state.value()))
                .map(PartitionStates.PartitionState::topicPartition)
                .collect(collector);
    }


    public synchronized void resetMissingPositions() {
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        assignment.stream().forEach(state -> {
            TopicPartition tp = state.topicPartition();
            TopicPartitionState partitionState = state.value();
            if (!partitionState.hasPosition()) {
                if (defaultResetStrategy == OffsetResetStrategy.NONE)
                    partitionsWithNoOffsets.add(tp);
                else
                    requestOffsetReset(tp);
            }
        });

        if (!partitionsWithNoOffsets.isEmpty())
            throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
    }

    public synchronized Set<TopicPartition> partitionsNeedingReset(long nowMs) {
        return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs),
                Collectors.toSet());
    }

    public synchronized Set<TopicPartition> partitionsNeedingValidation(long nowMs) {
        return collectPartitions(state -> state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs),
                Collectors.toSet());
    }

    public synchronized boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public synchronized boolean isPaused(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isPaused();
    }

    synchronized boolean isFetchable(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isFetchable();
    }

    public synchronized boolean hasValidPosition(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.hasValidPosition();
    }

    public synchronized void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public synchronized void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    synchronized void requestFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        for (TopicPartition partition : partitions) {
            // by the time the request failed, the assignment may no longer
            // contain this partition any more, in which case we would just ignore.
            final TopicPartitionState state = assignedStateOrNull(partition);
            if (state != null)
                state.requestFailed(nextRetryTimeMs);
        }
    }

    synchronized void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public synchronized ConsumerRebalanceListener rebalanceListener() {
        return rebalanceListener;
    }

    private static Map<TopicPartition, TopicPartitionState> partitionToStateMap(Collection<TopicPartition> assignments) {
        Map<TopicPartition, TopicPartitionState> map = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments)
            map.put(tp, new TopicPartitionState());
		return map;
	}

	/**
	 * An enumeration of all the possible fetch states. The state transitions are encoded in the values returned by
	 * {@link FetchState#validTransitions}.
	 */
	enum FetchStates implements FetchState {
		INITIALIZING() {
			@Override
			public Collection<FetchState> validTransitions() {
				return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
			}

			@Override
			public boolean hasPosition() {
				return false;
			}

			@Override
			public boolean hasValidPosition() {
				return false;
			}
		},

		FETCHING() {
			@Override
			public Collection<FetchState> validTransitions() {
				return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
			}

			@Override
			public boolean hasPosition() {
				return true;
			}

			@Override
			public boolean hasValidPosition() {
				return true;
			}
		},

		AWAIT_RESET() {
			@Override
			public Collection<FetchState> validTransitions() {
				return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET);
			}

			@Override
			public boolean hasPosition() {
				return true;
			}

			@Override
			public boolean hasValidPosition() {
				return false;
			}
		},

		AWAIT_VALIDATION() {
			@Override
			public Collection<FetchState> validTransitions() {
				return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
			}

			@Override
			public boolean hasPosition() {
				return true;
			}

			@Override
			public boolean hasValidPosition() {
				return false;
			}
		}
	}

	/**
	 * 一个partition的fetch状态，这个类用来确认可供传输的合法的状态，并公开当前获取状态的一些行为
	 * 真正的状态变量存储于{@link TopicPartitionState}中
	 */
	interface FetchState {
		default FetchState transitionTo(FetchState newState) {
			// 如果在可进行传输的状态中包含次状态，则返回此状态
			if (validTransitions().contains(newState)) {
				return newState;
			} else {
				// 否则返回当前状态
				return this;
			}
		}

		Collection<FetchState> validTransitions();

		boolean hasPosition();

		boolean hasValidPosition();
	}

	private static class TopicPartitionState {

		private FetchState fetchState;
		/**
		 * 上一次消费到的位置
		 */
		private FetchPosition position;
		/**
		 * 消费者可以看到的最高的日志记录offset
		 * 比如一个topic有三个broker，三个broker每个都有自己的日志文件
		 * highWaterMark则是这几个日志文件末尾offset的最小值
		 * 消费者只能拉取到highWaterMark之前的消息
		 */
        private Long highWatermark; // the high watermark from last fetch
		/**
		 * 记录的起始offset
		 */
		private Long logStartOffset; // the log start offset
		/**
		 * 最后一次提交的offset
		 */
		private Long lastStableOffset;
		/**
		 * 开发者是否暂停了当前分区
		 */
        private boolean paused;
		/**
		 * offset重置策略
		 */
		private OffsetResetStrategy resetStrategy;
        private Long nextRetryTimeMs;
        private Integer preferredReadReplica;
        private Long preferredReadReplicaExpireTimeMs;

        TopicPartitionState() {
            this.paused = false;
            this.fetchState = FetchStates.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
            this.preferredReadReplica = null;
		}

		/**
		 * 传输fetch状态
		 * @param newState          新的fetch状态
		 * @param runIfTransitioned
		 */
		private void transitionState(FetchState newState, Runnable runIfTransitioned) {
			FetchState nextState = this.fetchState.transitionTo(newState);
			if (nextState.equals(newState)) {
				this.fetchState = nextState;
				// 更新状态后，执行任务
				runIfTransitioned.run();
			}
		}

        private Optional<Integer> preferredReadReplica(long timeMs) {
            if (preferredReadReplicaExpireTimeMs != null && timeMs > preferredReadReplicaExpireTimeMs) {
                preferredReadReplica = null;
                return Optional.empty();
            } else {
                return Optional.ofNullable(preferredReadReplica);
            }
        }

        private void updatePreferredReadReplica(int preferredReadReplica, Supplier<Long> timeMs) {
            if (this.preferredReadReplica == null || preferredReadReplica != this.preferredReadReplica) {
                this.preferredReadReplica = preferredReadReplica;
                this.preferredReadReplicaExpireTimeMs = timeMs.get();
            }
        }

        private Optional<Integer> clearPreferredReadReplica() {
            if (preferredReadReplica != null) {
                int removedReplicaId = this.preferredReadReplica;
                this.preferredReadReplica = null;
                this.preferredReadReplicaExpireTimeMs = null;
                return Optional.of(removedReplicaId);
            } else {
                return Optional.empty();
            }
        }

        private void reset(OffsetResetStrategy strategy) {
            transitionState(FetchStates.AWAIT_RESET, () -> {
                this.resetStrategy = strategy;
                this.nextRetryTimeMs = null;
            });
        }

        private boolean maybeValidatePosition(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (this.fetchState.equals(FetchStates.AWAIT_RESET)) {
                return false;
            }

            if (currentLeaderAndEpoch.equals(Metadata.LeaderAndEpoch.noLeaderOrEpoch())) {
                // Ignore empty LeaderAndEpochs
                return false;
            }

            if (position != null && !position.currentLeader.equals(currentLeaderAndEpoch)) {
                FetchPosition newPosition = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                validatePosition(newPosition);
                preferredReadReplica = null;
            }
			return this.fetchState.equals(FetchStates.AWAIT_VALIDATION);
		}

		/**
		 * 校验fetch时的位置
		 * @param position
		 */
		private void validatePosition(FetchPosition position) {
			// 如果fetch时获取到了leader的epoch，并且也拥有当前leader的epoch
			if (position.offsetEpoch.isPresent() && position.currentLeader.epoch.isPresent()) {

				transitionState(FetchStates.AWAIT_VALIDATION, () -> {
					this.position = position;
					this.nextRetryTimeMs = null;
				});
			} else {
				// If we have no epoch information for the current position, then we can skip validation
				transitionState(FetchStates.FETCHING, () -> {
					this.position = position;
					this.nextRetryTimeMs = null;
				});
			}
		}

        /**
         * Clear the awaiting validation state and enter fetching.
         */
        private void completeValidation() {
            if (hasPosition()) {
                transitionState(FetchStates.FETCHING, () -> {
                    this.nextRetryTimeMs = null;
                });
            }
        }

        private boolean awaitingValidation() {
            return fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        private boolean awaitingRetryBackoff(long nowMs) {
            return nextRetryTimeMs != null && nowMs < nextRetryTimeMs;
        }

        private boolean awaitingReset() {
            return fetchState.equals(FetchStates.AWAIT_RESET);
        }

        private void setNextAllowedRetry(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private void requestFailed(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private boolean hasValidPosition() {
            return fetchState.hasValidPosition();
        }

        private boolean hasPosition() {
            return fetchState.hasPosition();
        }

        private boolean isPaused() {
            return paused;
        }

        private void seekValidated(FetchPosition position) {
            transitionState(FetchStates.FETCHING, () -> {
                this.position = position;
                this.resetStrategy = null;
                this.nextRetryTimeMs = null;
            });
        }

        private void seekUnvalidated(FetchPosition fetchPosition) {
            seekValidated(fetchPosition);
            validatePosition(fetchPosition);
        }

        private void position(FetchPosition position) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = position;
        }

        private FetchPosition validPosition() {
            if (hasValidPosition()) {
                return position;
            } else {
                return null;
            }
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

        private void highWatermark(Long highWatermark) {
            this.highWatermark = highWatermark;
        }

        private void logStartOffset(Long logStartOffset) {
            this.logStartOffset = logStartOffset;
        }

        private void lastStableOffset(Long lastStableOffset) {
            this.lastStableOffset = lastStableOffset;
        }

        private OffsetResetStrategy resetStrategy() {
			return resetStrategy;
        }
    }

    /**
     * partition的分区订阅位置信息
     * This includes the offset and epoch from the last record in
     * the batch from a FetchResponse. It also includes the leader epoch at the time the batch was consumed.
	 *
	 * The last fetch epoch is used to
	 */
	public static class FetchPosition {
		/**
		 * 偏移量
		 */
		public final long offset;

		final Optional<Integer> offsetEpoch;
        final Metadata.LeaderAndEpoch currentLeader;

        FetchPosition(long offset) {
            this(offset, Optional.empty(), new Metadata.LeaderAndEpoch(Node.noNode(), Optional.empty()));
        }

        public FetchPosition(long offset, Optional<Integer> offsetEpoch, Metadata.LeaderAndEpoch currentLeader) {
            this.offset = offset;
            this.offsetEpoch = Objects.requireNonNull(offsetEpoch);
            this.currentLeader = Objects.requireNonNull(currentLeader);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchPosition that = (FetchPosition) o;
            return offset == that.offset &&
                    offsetEpoch.equals(that.offsetEpoch) &&
                    currentLeader.equals(that.currentLeader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, offsetEpoch, currentLeader);
        }

        @Override
        public String toString() {
            return "FetchPosition{" +
                    "offset=" + offset +
                    ", offsetEpoch=" + offsetEpoch +
                    ", currentLeader=" + currentLeader +
                    '}';
        }
    }
}
