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
package org.apache.kafka.common;

import java.net.InetSocketAddress;
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

/**
 * An immutable representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
 */
public final class Cluster {

	private final boolean isBootstrapConfigured;
	private final List<Node> nodes;
	private final Set<String> unauthorizedTopics;
	private final Set<String> invalidTopics;
	private final Set<String> internalTopics;
	private final Node controller;
	private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
	/**
	 * key: topicName
	 * value: topic的partition列表
	 */
	private final Map<String, List<PartitionInfo>> partitionsByTopic;
	/**
	 * key: topicName
	 * value: topic可用的partition集合
	 */
	private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
	private final Map<Integer, List<PartitionInfo>> partitionsByNode;
	private final Map<Integer, Node> nodesById;
	private final ClusterResource clusterResource;

	/**
	 * Create a new cluster with the given id, nodes and partitions
	 * @param nodes      The nodes in the cluster
	 * @param partitions Information about a subset of the topic-partitions this cluster hosts
	 */
	public Cluster(String clusterId,
				   Collection<Node> nodes,
				   Collection<PartitionInfo> partitions,
				   Set<String> unauthorizedTopics,
				   Set<String> internalTopics) {
		this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, null);
	}

	/**
	 * Create a new cluster with the given id, nodes and partitions
	 * @param nodes      The nodes in the cluster
	 * @param partitions Information about a subset of the topic-partitions this cluster hosts
	 */
	public Cluster(String clusterId,
				   Collection<Node> nodes,
				   Collection<PartitionInfo> partitions,
				   Set<String> unauthorizedTopics,
				   Set<String> internalTopics,
				   Node controller) {
		this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, controller);
	}

	/**
	 * Create a new cluster with the given id, nodes and partitions
	 * @param nodes      The nodes in the cluster
	 * @param partitions Information about a subset of the topic-partitions this cluster hosts
	 */
	public Cluster(String clusterId,
				   Collection<Node> nodes,
				   Collection<PartitionInfo> partitions,
				   Set<String> unauthorizedTopics,
				   Set<String> invalidTopics,
				   Set<String> internalTopics,
				   Node controller) {
		this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller);
	}

	private Cluster(String clusterId,
					boolean isBootstrapConfigured,
					Collection<Node> nodes,
					Collection<PartitionInfo> partitions,
					Set<String> unauthorizedTopics,
					Set<String> invalidTopics,
					Set<String> internalTopics,
					Node controller) {
		this.isBootstrapConfigured = isBootstrapConfigured;
		this.clusterResource = new ClusterResource(clusterId);
		// make a randomized, unmodifiable copy of the nodes
		List<Node> copy = new ArrayList<>(nodes);
		Collections.shuffle(copy);
		this.nodes = Collections.unmodifiableList(copy);

		// Index the nodes for quick lookup
		Map<Integer, Node> tmpNodesById = new HashMap<>();
		Map<Integer, List<PartitionInfo>> tmpPartitionsByNode = new HashMap<>(nodes.size());
		for (Node node : nodes) {
			tmpNodesById.put(node.id(), node);
			// Populate the map here to make it easy to add the partitions per node efficiently when iterating over
			// the partitions
			tmpPartitionsByNode.put(node.id(), new ArrayList<>());
		}
		this.nodesById = Collections.unmodifiableMap(tmpNodesById);

		// index the partition infos by topic, topic+partition, and node
		// note that this code is performance sensitive if there are a large number of partitions so we are careful
		// to avoid unnecessary work
		Map<TopicPartition, PartitionInfo> tmpPartitionsByTopicPartition = new HashMap<>(partitions.size());
		Map<String, List<PartitionInfo>> tmpPartitionsByTopic = new HashMap<>();
		for (PartitionInfo p : partitions) {
			tmpPartitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
			List<PartitionInfo> partitionsForTopic = tmpPartitionsByTopic.get(p.topic());
			if (partitionsForTopic == null) {
				partitionsForTopic = new ArrayList<>();
				tmpPartitionsByTopic.put(p.topic(), partitionsForTopic);
			}
			partitionsForTopic.add(p);
			if (p.leader() != null) {
				// The broker guarantees that if a partition has a non-null leader, it is one of the brokers returned
				// in the metadata response
				List<PartitionInfo> partitionsForNode = Objects.requireNonNull(tmpPartitionsByNode.get(p.leader().id()));
				partitionsForNode.add(p);
			}
		}

		// Update the values of `tmpPartitionsByNode` to contain unmodifiable lists
		for (Map.Entry<Integer, List<PartitionInfo>> entry : tmpPartitionsByNode.entrySet()) {
			tmpPartitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
		}

		// Populate `tmpAvailablePartitionsByTopic` and update the values of `tmpPartitionsByTopic` to contain
		// unmodifiable lists
		Map<String, List<PartitionInfo>> tmpAvailablePartitionsByTopic = new HashMap<>(tmpPartitionsByTopic.size());
		for (Map.Entry<String, List<PartitionInfo>> entry : tmpPartitionsByTopic.entrySet()) {
			String topic = entry.getKey();
			List<PartitionInfo> partitionsForTopic = Collections.unmodifiableList(entry.getValue());
			tmpPartitionsByTopic.put(topic, partitionsForTopic);
			// Optimise for the common case where all partitions are available
			boolean foundUnavailablePartition = partitionsForTopic.stream().anyMatch(p -> p.leader() == null);
			List<PartitionInfo> availablePartitionsForTopic;
			if (foundUnavailablePartition) {
				availablePartitionsForTopic = new ArrayList<>(partitionsForTopic.size());
				for (PartitionInfo p : partitionsForTopic) {
					if (p.leader() != null)
						availablePartitionsForTopic.add(p);
				}
				availablePartitionsForTopic = Collections.unmodifiableList(availablePartitionsForTopic);
			} else {
				availablePartitionsForTopic = partitionsForTopic;
			}
			tmpAvailablePartitionsByTopic.put(topic, availablePartitionsForTopic);
		}

		this.partitionsByTopicPartition = Collections.unmodifiableMap(tmpPartitionsByTopicPartition);
		this.partitionsByTopic = Collections.unmodifiableMap(tmpPartitionsByTopic);
		this.availablePartitionsByTopic = Collections.unmodifiableMap(tmpAvailablePartitionsByTopic);
		this.partitionsByNode = Collections.unmodifiableMap(tmpPartitionsByNode);

		this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
		this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
		this.internalTopics = Collections.unmodifiableSet(internalTopics);
		this.controller = controller;
	}

	/**
	 * Create an empty cluster instance with no nodes and no topic-partitions.
	 */
	public static Cluster empty() {
		return new Cluster(null, new ArrayList<>(0), new ArrayList<>(0), Collections.emptySet(),
				Collections.emptySet(), null);
	}

	/**
	 * Create a "bootstrap" cluster using the given list of host/ports
	 * @param addresses The addresses
	 * @return A cluster for these hosts/ports
	 */
	public static Cluster bootstrap(List<InetSocketAddress> addresses) {
		List<Node> nodes = new ArrayList<>();
		int nodeId = -1;
		for (InetSocketAddress address : addresses)
			nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
		return new Cluster(null, true, nodes, new ArrayList<>(0),
				Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
	}

	/**
	 * Return a copy of this cluster combined with `partitions`.
	 */
	public Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions) {
		Map<TopicPartition, PartitionInfo> combinedPartitions = new HashMap<>(this.partitionsByTopicPartition);
		combinedPartitions.putAll(partitions);
		return new Cluster(clusterResource.clusterId(), this.nodes, combinedPartitions.values(),
				new HashSet<>(this.unauthorizedTopics), new HashSet<>(this.invalidTopics),
				new HashSet<>(this.internalTopics), this.controller);
	}

	/**
	 * @return The known set of nodes
	 */
	public List<Node> nodes() {
		return this.nodes;
	}

	/**
	 * Get the node by the node id (or null if no such node exists)
	 * @param id The id of the node
	 * @return The node, or null if no such node exists
	 */
	public Node nodeById(int id) {
		return this.nodesById.get(id);
	}

	/**
	 * Get the node by node id if the replica for the given partition is online
	 * @param partition
	 * @param id
	 * @return the node
	 */
	public Optional<Node> nodeIfOnline(TopicPartition partition, int id) {
		Node node = nodeById(id);
		if (node != null && !Arrays.asList(partition(partition).offlineReplicas()).contains(node)) {
			return Optional.of(node);
		} else {
			return Optional.empty();
		}
	}

	/**
	 * Get the current leader for the given topic-partition
	 * @param topicPartition The topic and partition we want to know the leader for
	 * @return The node that is the leader for this topic-partition, or null if there is currently no leader
	 */
	public Node leaderFor(TopicPartition topicPartition) {
		PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
		if (info == null)
			return null;
		else
			return info.leader();
	}

	/**
	 * 获取指定的分区的元数据
	 * @param topicPartition 指定的topic分区
	 * @return 指定topic分区的元数据，如果没有此分区，返回null
	 */
	public PartitionInfo partition(TopicPartition topicPartition) {
		return partitionsByTopicPartition.get(topicPartition);
	}

	/**
	 * 获取指定topic的所有partition信息
	 * @param topic 指定的topic
	 * @return 指定topic的所有partition信息
	 */
	public List<PartitionInfo> partitionsForTopic(String topic) {
		return partitionsByTopic.getOrDefault(topic, Collections.emptyList());
	}

	/**
	 * 获取指定topic的partition的总数量
	 * @param topic 指定的topic
	 * @return 指定topic的partition的总数量
	 */
	public Integer partitionCountForTopic(String topic) {
		// 获取指定topic的所有partition信息
		List<PartitionInfo> partitions = this.partitionsByTopic.get(topic);
		// 返回当前topic的partition的总数量
		return partitions == null ? null : partitions.size();
	}

	/**
	 * 获取指定topic的可用partition的列表
	 * @param topic 指定的topic
	 * @return 指定topic的可用partition的列表
	 */
	public List<PartitionInfo> availablePartitionsForTopic(String topic) {
		return availablePartitionsByTopic.getOrDefault(topic, Collections.emptyList());
	}

	/**
	 * 获取主节点为指定节点的partition列表
	 * @param nodeId 节点ID
	 * @return 主节点为指定节点的partition列表
	 */
	public List<PartitionInfo> partitionsForNode(int nodeId) {
		return partitionsByNode.getOrDefault(nodeId, Collections.emptyList());
	}

	/**
	 * 获取所有的topic
	 * @return topic集合
	 */
	public Set<String> topics() {
		return partitionsByTopic.keySet();
	}

	public Set<String> unauthorizedTopics() {
		return unauthorizedTopics;
	}

	public Set<String> invalidTopics() {
		return invalidTopics;
	}

	public Set<String> internalTopics() {
		return internalTopics;
	}

	public boolean isBootstrapConfigured() {
		return isBootstrapConfigured;
	}

	public ClusterResource clusterResource() {
		return clusterResource;
	}

	public Node controller() {
		return controller;
	}

	@Override
	public String toString() {
		return "Cluster(id = " + clusterResource.clusterId() + ", nodes = " + this.nodes +
				", partitions = " + this.partitionsByTopicPartition.values() + ", controller = " + controller + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Cluster cluster = (Cluster) o;
		return isBootstrapConfigured == cluster.isBootstrapConfigured &&
				Objects.equals(nodes, cluster.nodes) &&
				Objects.equals(unauthorizedTopics, cluster.unauthorizedTopics) &&
				Objects.equals(invalidTopics, cluster.invalidTopics) &&
				Objects.equals(internalTopics, cluster.internalTopics) &&
				Objects.equals(controller, cluster.controller) &&
				Objects.equals(partitionsByTopicPartition, cluster.partitionsByTopicPartition) &&
				Objects.equals(clusterResource, cluster.clusterResource);
	}

	@Override
	public int hashCode() {
		return Objects.hash(isBootstrapConfigured, nodes, unauthorizedTopics, invalidTopics, internalTopics, controller,
				partitionsByTopicPartition, clusterResource);
	}
}
