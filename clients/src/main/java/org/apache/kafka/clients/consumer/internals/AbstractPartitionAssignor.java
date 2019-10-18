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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 抽象分配策略实现了一些需要共同执行的方法，尤其是收集partition数量
 */
public abstract class AbstractPartitionAssignor implements ConsumerPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);

    /**
	 * 具体分配策略的分配实现
	 * @param partitionsPerTopic 每个topic的partition数量，没有在metadata中的topic不会包含在内
	 * @param subscriptions      memberId-订阅信息
	 * @return memberId-partition集合映射信息
     */
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, Subscription> subscriptions);

	/**
	 * partition分配
	 * @param metadata          consumer拥有的当前的topic/broker的metadata
	 * @param groupSubscription 所有成员的订阅信息
	 * @return 集群分配策略
	 */
	@Override
	public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
		// 获取memberId-订阅信息维度的映射集合
		Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
		// 所有已订阅的topic
		Set<String> allSubscribedTopics = new HashSet<>();
		// 遍历所有的订阅信息，获取每个consumer订阅的topic
		for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
			allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());
		// 另一个维度，以topic为维度的topic-partition个数映射集合
		Map<String, Integer> partitionsPerTopic = new HashMap<>();
		for (String topic : allSubscribedTopics) {
			Integer numPartitions = metadata.partitionCountForTopic(topic);
			if (numPartitions != null && numPartitions > 0)
				partitionsPerTopic.put(topic, numPartitions);
			else
				log.debug("Skipping assignment for topic {} since no metadata is available", topic);
		}
		// 进行原生未加工的分配，根据指定的分配策略
		Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, subscriptions);

		// 当前类没有包含开发者数据，所以只包装这些结果
		Map<String, Assignment> assignments = new HashMap<>();
		for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
			// 将分配给当前member的partition列表包装成Assignment
			assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
		return new GroupAssignment(assignments);
	}

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(value);
    }

    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new TopicPartition(topic, i));
        return partitions;
	}

	/**
	 * 消费组成员信息
	 */
	public static class MemberInfo implements Comparable<MemberInfo> {
		/**
		 * memberId
		 */
		public final String memberId;
		/**
		 * group.instance.id
		 */
		public final Optional<String> groupInstanceId;

		public MemberInfo(String memberId, Optional<String> groupInstanceId) {
			this.memberId = memberId;
			this.groupInstanceId = groupInstanceId;
		}

		@Override
		public int compareTo(MemberInfo otherMemberInfo) {
			if (this.groupInstanceId.isPresent() &&
					otherMemberInfo.groupInstanceId.isPresent()) {
				return this.groupInstanceId.get()
						.compareTo(otherMemberInfo.groupInstanceId.get());
			} else if (this.groupInstanceId.isPresent()) {
				return -1;
			} else if (otherMemberInfo.groupInstanceId.isPresent()) {
				return 1;
			} else {
				return this.memberId.compareTo(otherMemberInfo.memberId);
			}
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof MemberInfo && this.memberId.equals(((MemberInfo) o).memberId);
		}

		/**
		 * We could just use member.id to be the hashcode, since it's unique
		 * across the group.
		 */
		@Override
		public int hashCode() {
			return memberId.hashCode();
		}

		@Override
		public String toString() {
			return "MemberInfo [member.id: " + memberId
					+ ", group.instance.id: " + groupInstanceId.orElse("{}")
					+ "]";
		}
	}
}
