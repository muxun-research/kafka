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

package org.apache.kafka.clients;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * 保存了连接broker的拉取会话状态
 * 使用了KIP-227这种新的前缀通配符访问控制协议，客户端可以增量的创建拉取会话
 * 这些拉去会话允许客户端反复拉取partition的信息，而不需要明确枚举请求或者响应中的分区
 *
 * 拉取会话处理器记录了会话中参与的分区，它还确认了每个fetch请求中需要包含的分区，以及每个请求附加的fetch会话元数据信息
 * 在broker端对应的类是FetchManager
 */
public class FetchSessionHandler {
    private final Logger log;

    private final int node;

    /**
	 * 下一次fetch请求的metadata
     */
    private FetchMetadata nextMetadata = FetchMetadata.INITIAL;

    public FetchSessionHandler(LogContext logContext, int node) {
        this.log = logContext.logger(FetchSessionHandler.class);
        this.node = node;
    }

    /**
	 * 所有处于fetch请求会话中的分区及分区信息
     */
    private LinkedHashMap<TopicPartition, PartitionData> sessionPartitions =
        new LinkedHashMap<>(0);

	/**
	 * 处理拉取响应
	 * @param response 拉取响应
	 * @return 如果格式良好，返回true，如果不能处理，返回false
	 * 因为缺失或者返回了不期望的partition
	 */
	public boolean handleResponse(FetchResponse<?> response) {
		// 返回了失败信息
		if (response.error() != Errors.NONE) {
			log.info("Node {} was unable to process the fetch request with {}: {}.",
					node, nextMetadata, response.error());
			// 没有找到对应的session id，下一次将会重新拉取
			if (response.error() == Errors.FETCH_SESSION_ID_NOT_FOUND) {
				nextMetadata = FetchMetadata.INITIAL;
			} else {
				// 否则重新创建session
				nextMetadata = nextMetadata.nextCloseExisting();
			}
			return false;
		} else if (nextMetadata.isFull()) {
			// 如果本次fetch请求已经满了
			// 验证本次满载的请求信息
			String problem = verifyFullFetchResponsePartitions(response);
			// 存在异常，则重新创建session
			if (problem != null) {
				log.info("Node {} sent an invalid full fetch response with {}", node, problem);
				nextMetadata = FetchMetadata.INITIAL;
				return false;
			} else if (response.sessionId() == INVALID_SESSION_ID) {
				log.debug("Node {} sent a full fetch response{}",
						node, responseDataToLogString(response));
				nextMetadata = FetchMetadata.INITIAL;
				return true;
			} else {
				log.debug("Node {} sent a full fetch response that created a new incremental " +
						"fetch session {}{}", node, response.sessionId(), responseDataToLogString(response));
				// 根据响应的sessionId重新生成下一次fetch请求的metadata
				nextMetadata = FetchMetadata.newIncremental(response.sessionId());
				return true;
			}
		} else {
			// 一次普通的fetch请求
			// 需要验证增量的fetch响应信息
			String problem = verifyIncrementalFetchResponsePartitions(response);
			if (problem != null) {
				log.info("Node {} sent an invalid incremental fetch response with {}", node, problem);
				nextMetadata = nextMetadata.nextCloseExisting();
				return false;
			} else if (response.sessionId() == INVALID_SESSION_ID) {
				// 增量fetch会话已经被服务端关闭
				log.debug("Node {} sent an incremental fetch response closing session {}{}",
						node, nextMetadata.sessionId(), responseDataToLogString(response));
				nextMetadata = FetchMetadata.INITIAL;
				return true;
			} else {
				log.debug("Node {} sent an incremental fetch response for session {}{}",
						node, response.sessionId(), responseDataToLogString(response));
				// session继续使用，更新为下一代版本的session
				nextMetadata = nextMetadata.nextIncremental();
				return true;
			}
		}
	}

	public static class FetchRequestData {
		/**
		 * 需要发送的请求
		 */
		private final Map<TopicPartition, PartitionData> toSend;

		/**
		 * 需要移除的请求
		 */
		private final List<TopicPartition> toForget;

		/**
		 * 当前正在会话中处理的请求
		 */
		private final Map<TopicPartition, PartitionData> sessionPartitions;

		/**
		 * 请求拉取的元数据
		 */
		private final FetchMetadata metadata;

		FetchRequestData(Map<TopicPartition, PartitionData> toSend,
						 List<TopicPartition> toForget,
						 Map<TopicPartition, PartitionData> sessionPartitions,
						 FetchMetadata metadata) {
			this.toSend = toSend;
			this.toForget = toForget;
			this.sessionPartitions = sessionPartitions;
			this.metadata = metadata;
		}

		/**
		 * Get the set of partitions to send in this fetch request.
		 */
		public Map<TopicPartition, PartitionData> toSend() {
			return toSend;
		}

		/**
		 * Get a list of partitions to forget in this fetch request.
		 */
		public List<TopicPartition> toForget() {
			return toForget;
		}

		/**
		 * Get the full set of partitions involved in this fetch request.
		 */
		public Map<TopicPartition, PartitionData> sessionPartitions() {
			return sessionPartitions;
		}

		public FetchMetadata metadata() {
			return metadata;
		}

		@Override
		public String toString() {
			if (metadata.isFull()) {
				StringBuilder bld = new StringBuilder("FullFetchRequest(");
				String prefix = "";
				for (TopicPartition partition : toSend.keySet()) {
					bld.append(prefix);
					bld.append(partition);
					prefix = ", ";
				}
				bld.append(")");
				return bld.toString();
			} else {
				StringBuilder bld = new StringBuilder("IncrementalFetchRequest(toSend=(");
				String prefix = "";
				for (TopicPartition partition : toSend.keySet()) {
					bld.append(prefix);
					bld.append(partition);
					prefix = ", ";
				}
				bld.append("), toForget=(");
				prefix = "";
				for (TopicPartition partition : toForget) {
					bld.append(prefix);
					bld.append(partition);
					prefix = ", ";
				}
				bld.append("), implied=(");
				prefix = "";
				for (TopicPartition partition : sessionPartitions.keySet()) {
					if (!toSend.containsKey(partition)) {
						bld.append(prefix);
						bld.append(partition);
						prefix = ", ";
					}
				}
				bld.append("))");
				return bld.toString();
			}
		}
	}

    public Builder newBuilder() {
        return new Builder();
    }

    private String partitionsToLogString(Collection<TopicPartition> partitions) {
        if (!log.isTraceEnabled()) {
            return String.format("%d partition(s)", partitions.size());
        }
        return "(" + Utils.join(partitions, ", ") + ")";
    }

    /**
     * Return some partitions which are expected to be in a particular set, but which are not.
     *
     * @param toFind    The partitions to look for.
     * @param toSearch  The set of partitions to search.
     * @return          null if all partitions were found; some of the missing ones
     *                  in string form, if not.
     */
    static Set<TopicPartition> findMissing(Set<TopicPartition> toFind, Set<TopicPartition> toSearch) {
        Set<TopicPartition> ret = new LinkedHashSet<>();
        for (TopicPartition partition : toFind) {
            if (!toSearch.contains(partition)) {
                ret.add(partition);
            }
        }
        return ret;
    }

    /**
     * Verify that a full fetch response contains all the partitions in the fetch session.
     *
     * @param response  The response.
     * @return          True if the full fetch response partitions are valid.
     */
    private String verifyFullFetchResponsePartitions(FetchResponse<?> response) {
        StringBuilder bld = new StringBuilder();
        Set<TopicPartition> omitted =
            findMissing(response.responseData().keySet(), sessionPartitions.keySet());
        Set<TopicPartition> extra =
            findMissing(sessionPartitions.keySet(), response.responseData().keySet());
        if (!omitted.isEmpty()) {
            bld.append("omitted=(").append(Utils.join(omitted, ", ")).append(", ");
        }
        if (!extra.isEmpty()) {
            bld.append("extra=(").append(Utils.join(extra, ", ")).append(", ");
        }
        if ((!omitted.isEmpty()) || (!extra.isEmpty())) {
            bld.append("response=(").append(Utils.join(response.responseData().keySet(), ", "));
            return bld.toString();
        }
        return null;
    }

    /**
     * Verify that the partitions in an incremental fetch response are contained in the session.
     *
     * @param response  The response.
     * @return          True if the incremental fetch response partitions are valid.
     */
    private String verifyIncrementalFetchResponsePartitions(FetchResponse<?> response) {
        Set<TopicPartition> extra =
            findMissing(response.responseData().keySet(), sessionPartitions.keySet());
        if (!extra.isEmpty()) {
            StringBuilder bld = new StringBuilder();
            bld.append("extra=(").append(Utils.join(extra, ", ")).append("), ");
            bld.append("response=(").append(
                Utils.join(response.responseData().keySet(), ", ")).append("), ");
            return bld.toString();
        }
        return null;
    }

    /**
     * Create a string describing the partitions in a FetchResponse.
     *
     * @param response  The FetchResponse.
     * @return          The string to log.
     */
    private String responseDataToLogString(FetchResponse<?> response) {
        if (!log.isTraceEnabled()) {
            int implied = sessionPartitions.size() - response.responseData().size();
            if (implied > 0) {
                return String.format(" with %d response partition(s), %d implied partition(s)",
                    response.responseData().size(), implied);
            } else {
                return String.format(" with %d response partition(s)",
                    response.responseData().size());
            }
        }
        StringBuilder bld = new StringBuilder();
        bld.append(" with response=(").
            append(Utils.join(response.responseData().keySet(), ", ")).
            append(")");
        String prefix = ", implied=(";
        String suffix = "";
        for (TopicPartition partition : sessionPartitions.keySet()) {
            if (!response.responseData().containsKey(partition)) {
                bld.append(prefix);
                bld.append(partition);
                prefix = ", ";
                suffix = ")";
			}
		}
		bld.append(suffix);
		return bld.toString();
	}

	public class Builder {
		/**
		 * 下一次fetch的partition集合
		 * 使用LinkedHashMap目的是为了保证插入时的顺序
		 * 需要保证顺序的原因是，在我们除了满额的fetch请求时，如果没有足够的响应空间来返回数据，那么服务端仅会返回在集合中的先插入的部分
		 * 另一个原因是，我们可以利用排序来优化增量fetch请求
		 */
		private LinkedHashMap<TopicPartition, PartitionData> next = new LinkedHashMap<>();

		/**
		 * 标记在接下来的fetch中我们需要获取数据的分区
		 */
		public void add(TopicPartition topicPartition, PartitionData data) {
			next.put(topicPartition, data);
		}

		/**
		 * 根据拉取线程处理器构建拉取请求数据
		 * @return 拉取请求数据
		 */
		public FetchRequestData build() {
			// 如果下一次fetch请求的metadata已经满了
			if (nextMetadata.isFull()) {
				if (log.isDebugEnabled()) {
					log.debug("Built full fetch {} for node {} with {}.",
							nextMetadata, node, partitionsToLogString(next.keySet()));
				}
				// 直接使用下一次fetch的partition集合
				sessionPartitions = next;
				next = null;
				// 构建partiton信息和partition数据集合，并构建拉取请求数据
				Map<TopicPartition, PartitionData> toSend =
						Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
				return new FetchRequestData(toSend, Collections.emptyList(), toSend, nextMetadata);
			}

			// 新增的
			List<TopicPartition> added = new ArrayList<>();
			// 移除的
			List<TopicPartition> removed = new ArrayList<>();
			// 修改的
			List<TopicPartition> altered = new ArrayList<>();
			// 遍历在筛选节点中，筛选出来的分区信息和分区拉取数据
			for (Iterator<Entry<TopicPartition, PartitionData>> iter =
				 sessionPartitions.entrySet().iterator(); iter.hasNext(); ) {
				Entry<TopicPartition, PartitionData> entry = iter.next();
				TopicPartition topicPartition = entry.getKey();
				PartitionData prevData = entry.getValue();
				PartitionData nextData = next.get(topicPartition);
				if (nextData != null) {
					if (prevData.equals(nextData)) {
						// 如果前后拉取数据没有发生变化，下一次不需要进行处理
						next.remove(topicPartition);
					} else {
						// 将修改的分区数据移动到下一次需要处理的分区数据列表中
						next.remove(topicPartition);
						next.put(topicPartition, nextData);
						entry.setValue(nextData);
						altered.add(topicPartition);
					}
				} else {
					// 从当前会话中移除次topic-partition信息
					iter.remove();
					// 证明我们不需要继续监听此partition
					removed.add(topicPartition);
				}
			}
			// 添加新partition到会话中
			for (Entry<TopicPartition, PartitionData> entry : next.entrySet()) {
				TopicPartition topicPartition = entry.getKey();
				PartitionData nextData = entry.getValue();
				if (sessionPartitions.containsKey(topicPartition)) {
					// 再上一次轮询中，所有既存在于sessionPartitions集合，也存在于next集合中的分区，已经移动到next的尾部，或者移出next集合
					// 因此，如果我们命中了它们中的一个，我们就可以知道接下来没有更多在next中查不到的元素了
					break;
				}
				sessionPartitions.put(topicPartition, nextData);
				added.add(topicPartition);
			}
			if (log.isDebugEnabled()) {
				log.debug("Built incremental fetch {} for node {}. Added {}, altered {}, removed {} " +
								"out of {}", nextMetadata, node, partitionsToLogString(added),
						partitionsToLogString(altered), partitionsToLogString(removed),
						partitionsToLogString(sessionPartitions.keySet()));
			}
			// 需要发送的，即下一次需要处理
			Map<TopicPartition, PartitionData> toSend =
					Collections.unmodifiableMap(new LinkedHashMap<>(next));
			// 当前会话分区的，视为不需要请求的
			Map<TopicPartition, PartitionData> curSessionPartitions =
					Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
			next = null;
			return new FetchRequestData(toSend, Collections.unmodifiableList(removed),
					curSessionPartitions, nextMetadata);
        }
    }

    /**
     * Handle an error sending the prepared request.
     *
     * When a network error occurs, we close any existing fetch session on our next request,
     * and try to create a new session.
     *
     * @param t     The exception.
     */
    public void handleError(Throwable t) {
        log.info("Error sending fetch request {} to node {}: {}.", nextMetadata, node, t.toString());
        nextMetadata = nextMetadata.nextCloseExisting();
    }
}
