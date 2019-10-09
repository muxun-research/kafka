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

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * 默认的消息分区策略：
 * 如果记录已经指定了分区，使用指定的分区编号
 * 如果没有指定分区，但是可以通过hash的方式，将key转换为选择分区的代表
 * 如果没有指定分区，或者在批处理任务已经满了的时候，key将会选择当前的粘性分区
 */
public class DefaultPartitioner implements Partitioner {

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    public void configure(Map<String, ?> configs) {}

    /**
	 * 为给定的记录计算出一个分区索引
	 * @param topic 主题名称
	 * @param key 需要进行分区的key
	 * @param keyBytes 已经序列化过后的分区key
	 * @param value 需要分区存储的value
	 * @param valueBytes 已经序列化后的分区存储的value
	 * @param cluster 当前集群的元数据
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// 如果key为null，则使用轮询的方式获取分区
        if (keyBytes == null) {
            return stickyPartitionCache.partition(topic, cluster);
		}
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();
		// 如果key不为null，则采用murmur2的hash算法来计算key的分区索引
		return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    public void close() {}
    
    /**
	 * 如果对当前的粘性分区来说，一个批处理任务已经完成了，将会变更这个粘性的分区
	 * 轮流地，如果没有粘性的分区，将会创建一个
     */
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        stickyPartitionCache.nextPartition(topic, cluster, prevPartition);
    }
}
