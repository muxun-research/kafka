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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 内部类，实现partition粘性缓存
 * 缓存追踪了当前粘性partition
 * 此类不对外使用
 */
public class StickyPartitionCache {
    private final ConcurrentMap<String, Integer> indexCache;
    public StickyPartitionCache() {
        this.indexCache = new ConcurrentHashMap<>();
    }

    public int partition(String topic, Cluster cluster) {
        Integer part = indexCache.get(topic);
        if (part == null) {
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }

    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		Integer oldPart = indexCache.get(topic);
		Integer newPart = oldPart;
		// 校验当前粘性的状态，可能有两种状态:
		// 1. 没有设置此topic的粘性状态
		// 2. 触发创建新batch，需要切换粘性分区
		if (oldPart == null || oldPart == prevPartition) {
			// 获取topic下可用的partition
			List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
			if (availablePartitions.size() < 1) {
				// 无可用partition，随机返回一个partition
				Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
				newPart = random % partitions.size();
			} else if (availablePartitions.size() == 1) {
				// 只有一个partition，则使用此partition
				newPart = availablePartitions.get(0).partition();
			} else {
				// 多个可用的partition，则随机选择一个partition
				while (newPart == null || newPart.equals(oldPart)) {
					// 计算并获取下一个partition的分区号
					Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
					newPart = availablePartitions.get(random % availablePartitions.size()).partition();
				}
			}
			// 如果旧的partition为null，或者上一个粘性partition匹配了当前的partition，只更新粘性partition，
			if (oldPart == null) {
				indexCache.putIfAbsent(topic, newPart);
			} else {
				indexCache.replace(topic, prevPartition, newPart);
			}
			// 获取更新后的partition
			return indexCache.get(topic);
		}
        return indexCache.get(topic);
    }

}