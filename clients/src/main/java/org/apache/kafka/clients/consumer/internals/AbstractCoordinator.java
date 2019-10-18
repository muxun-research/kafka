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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.MemberIdRequiredException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, List)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 */
public abstract class AbstractCoordinator implements Closeable {
    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";

	/**
	 * 当前consumer的状态
	 */
	private MemberState state = MemberState.UNJOINED;

    private final Logger log;
    private final GroupCoordinatorMetrics sensors;
    private final Heartbeat heartbeat;
    private final GroupRebalanceConfig rebalanceConfig;
    protected final ConsumerNetworkClient client;
    protected final Time time;

    private HeartbeatThread heartbeatThread = null;
    private boolean rejoinNeeded = true;
	/**
	 * 默认情况下，需要加入前准备
	 */
	private boolean needsJoinPrepare = true;

	/**
	 * 将心跳线程置为无效
	 */
	private synchronized void disableHeartbeatThread() {
		if (heartbeatThread != null)
			heartbeatThread.disable();
    }
    private RequestFuture<ByteBuffer> joinFuture = null;
    private Node coordinator = null;
    private Generation generation = Generation.NO_GENERATION;

    private RequestFuture<Void> findCoordinatorFuture = null;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time) {
        Objects.requireNonNull(rebalanceConfig.groupId,
                               "Expected a non-null group id for coordinator construction");
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(AbstractCoordinator.class);
        this.client = client;
        this.time = time;
        this.heartbeat = new Heartbeat(rebalanceConfig, time);
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param protocol The protocol selected by the coordinator
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group. If this call fails with an exception,
     * then it will be retried using the same assignment state on the next call to {@link #ensureActiveGroup()}.
     *
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Invoked prior to each leave group event. This is typically used to cleanup assigned partitions;
     * note it is triggered by the consumer's API caller thread (i.e. background heartbeat thread would
     * not trigger it even if it tries to force leaving group upon heartbeat session expiration)
     */
    protected void onLeavePrepare() {}

    /**
	 * 确认协调者已经处于就绪状态，准备好接收请求
	 * @param timer 指定的等待时间计时器
	 * @return 如果协调者处于可发现状态，并且初始化连接已经成功，返回true，其他情况返回false
     */
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
		// 如果协调者没有处于未知状态，返回true
        if (!coordinatorUnknown())
            return true;

		// 在协调者处于未知状态，并且在剩余等待时间内进行轮询
        do {
			// 查找当前consumer的协调者信息
            final RequestFuture<Void> future = lookupCoordinator();
            client.poll(future, timer);
			// 没有查找完成，跳出轮旋，根据当前的协调者状态进行判断
            if (!future.isDone()) {
				// 可能是超出等待时间了
                break;
            }
			// 查找失败
            if (future.failed()) {
				// 如果出现的是可重试错误
                if (future.isRetriable()) {
                    log.debug("Coordinator discovery failed, refreshing metadata");
					// 证明协调者发现失败，刷新metadata
                    client.awaitMetadataUpdate(timer);
                } else
					// 非重试性错误，直接抛出异常
                    throw future.exception();
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
				// 如果当前的协调者不为null，但是consumer client和协调者之间是不可用的
				// 证明我们已经找到了协调者，但是连接失败了，所以标记这个协调者状态为未知，然后重试进行发现
                markCoordinatorUnknown();
				// 计时器sleep到下一次进行重试的时间
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }
        } while (coordinatorUnknown() && timer.notExpired());
		// 返回协调者状态是否处于就绪状态
        return !coordinatorUnknown();
    }

	/**
	 * 查询当前consumer的协调者
	 * 是线程安全的
	 * @return 包装了协调者的异步结果
	 */
	protected synchronized RequestFuture<Void> lookupCoordinator() {
		if (findCoordinatorFuture == null) {
			// 找到最近工作的node节点，向它询问现在的协调者是哪个节点
			Node node = this.client.leastLoadedNode();
			if (node == null) {
				log.debug("No broker available to send FindCoordinator request");
				// 如果连这样的节点都没有找到，那真只能失败了，返回没有可用的broker结果
				return RequestFuture.noBrokersAvailable();
			} else
				// 向当前节点咨询当前的协调者是哪个节点
				findCoordinatorFuture = sendFindCoordinatorRequest(node);
		}
		// 返回包装了协调者的异步结果
		return findCoordinatorFuture;
	}

    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

	/**
	 * 判断是否需要重新加入到消费组中（比如metadata发生变化）或者一个重新加入请求已经在运行中，正等待响应
	 * @return 需要重新加入，返回true，其他情况，返回false
     */
    protected synchronized boolean rejoinNeededOrPending() {
		// 如果有一个正在挂起的加入异步任务，我们需要尝试完成处理它
        return rejoinNeeded || joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     *
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }
            // Awake the heartbeat thread if needed
            if (heartbeat.shouldHeartbeat(now)) {
                notify();
            }
            heartbeat.poll(now);
        }
    }

    protected synchronized long timeToNextHeartbeat(long now) {
        // if we have not joined the group, we don't need to send heartbeats
        if (state == MemberState.UNJOINED)
            return Long.MAX_VALUE;
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    public void ensureActiveGroup() {
        while (!ensureActiveGroup(time.timer(Long.MAX_VALUE))) {
            log.warn("still waiting to ensure active group");
        }
    }

	/**
	 * 确认消费组是不是处于活跃状态，比如已经加入并同步
	 * @param timer 等待的计时器
	 * @throws KafkaException 回调任务出现了异常
	 * @return 消费组处于活跃状态
     */
    boolean ensureActiveGroup(final Timer timer) {
		// 必须要确认协调器是否处于就绪状态，因为我们可能会在发送心跳时断开连接，并且不需要我们重新加入消费组
        if (!ensureCoordinatorReady(timer)) {
            return false;
		}
		// 开启心跳线程
        startHeartbeatThreadIfNeeded();
		// 在协调器处于就绪状态下，加入消费组
        return joinGroupIfNeeded(timer);
	}

	/**
	 * 当当前没有心跳线程的情况下，创建并启动一个新的心跳线程
	 */
	private synchronized void startHeartbeatThreadIfNeeded() {
		if (heartbeatThread == null) {
			heartbeatThread = new HeartbeatThread();
			heartbeatThread.start();
		}
	}

	/**
	 * 加入消费组
	 * @param timer 等待时间计数器
	 * @throws KafkaException 如果回调任务出现了异常
	 * @return 加入消费组结果
     */
    boolean joinGroupIfNeeded(final Timer timer) {
		// 在需要重新加入或者挂起的情况下
        while (rejoinNeededOrPending()) {
			// 如果协调者没有处于就绪状态，直接返回false
            if (!ensureCoordinatorReady(timer)) {
                return false;
			}

			// 是否需要加入前准备，我们设置了一个标识位，来确认我们不会进行二次调用，如果client在一个挂起的再平衡完成之前被唤醒
			// 必须在当前轮询的每次迭代中调用，因为每个事件都需要一个再平衡（比如一个改变了匹配的订阅信息集合的metadata刷新）会触发
			// 当另一个再平衡才做仍然在处理中
			// 需要加入前准备
            if (needsJoinPrepare) {
				// 有需要准备的线程，先将标识位设为false
				// 因为开发者的回调任务可能会抛出异常，在这种情况下，重试时我们也不应该在onJoinPrepare上重试
                needsJoinPrepare = false;
                onJoinPrepare(generation.generationId, generation.memberId);
			}
			// 初始化加入消费组，并轮训加入结果
            final RequestFuture<ByteBuffer> future = initiateJoinGroup();
            client.poll(future, timer);
			// 加入任务没有完成，返回false
            if (!future.isDone()) {
				// 没有完成的原因可能是超时
                return false;
			}
			// 请求加入消费组成功
            if (future.succeeded()) {
				// 复制一份buffer，防止onJoinComplete()没有完成，进行重试
                ByteBuffer memberAssignment = future.value().duplicate();
				// 执行加入完成操作
                onJoinComplete(generation.generationId, generation.memberId, generation.protocol, memberAssignment);

				// 只有在回调任务完成之后，才会重置加入的消费组，它确认了如果回调任务被唤醒，我们将会在下一次需要加入时进行重试
                resetJoinGroupFuture();
                needsJoinPrepare = true;
            } else {
				// 加入失败
				// 重置回调加入回调任务
                resetJoinGroupFuture();
				// 获取失败异常，针对类型进行不同的判断
                final RuntimeException exception = future.exception();
                if (exception instanceof UnknownMemberIdException ||
                        exception instanceof RebalanceInProgressException ||
                        exception instanceof IllegalGenerationException ||
                        exception instanceof MemberIdRequiredException)
					// 在UnknownMemberIdException、RebalanceInProgressException、IllegalGenerationException、MemberIdRequiredException异常情况下，继续下一次轮询
                    continue;
					// 如果在失败的情况下，不允许重试，直接抛出指定异常
                else if (!future.isRetriable())
                    throw exception;
				// 在失败情况下，允许重试，等待进行下一次重试
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }
		}
		// 加入成功，返回true
        return true;
	}

	private void closeHeartbeatThread() {
		HeartbeatThread thread = null;
		synchronized (this) {
			if (heartbeatThread == null)
				return;
			heartbeatThread.close();
			thread = heartbeatThread;
			heartbeatThread = null;
		}
		try {
			thread.join();
		} catch (InterruptedException e) {
			log.warn("Interrupted while waiting for consumer heartbeat thread to close");
			throw new InterruptException(e);
		}
    }

	/**
	 * 初始化消费组
	 * @return 初始化消费组的异步结果
	 */
	private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
		// 我们需要存储一份加入异步任务备份，以防开发者唤醒，然后执行
		// 它将确保我们不会在未完成再平衡之前错误地尝试重新加入
		if (joinFuture == null) {
			// 如果没有需要完成的加入异步回调任务，就暂时屏蔽掉心跳检测线程，以便它不会干扰加入消费组
			// 需要住的是，必须发生在onJoinPrepare方法之后调用，因为如果回调任务的处理需要一些时间，我们必须还能持续发送心跳
			// 将心跳线程置为无效
			disableHeartbeatThread();
			// 状态置为正在进行再平衡
			state = MemberState.REBALANCING;
			// 发送加入消费组请求
			joinFuture = sendJoinGroupRequest();
			joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
				@Override
				public void onSuccess(ByteBuffer value) {
					// 处理加入完成任务，
					synchronized (AbstractCoordinator.this) {
						log.info("Successfully joined group with generation {}", generation.generationId);
						state = MemberState.STABLE;
						rejoinNeeded = false;

						if (heartbeatThread != null)
							heartbeatThread.enable();
					}
				}

				@Override
				public void onFailure(RuntimeException e) {
					// we handle failures below after the request finishes. if the join completes
					// after having been woken up, the exception is ignored and we will rejoin
					synchronized (AbstractCoordinator.this) {
						state = MemberState.UNJOINED;
					}
				}
			});
		}
		return joinFuture;
	}

	private synchronized void resetJoinGroupFuture() {
		this.joinFuture = null;
	}

	/**
	 * 加入消费组并返回下一generation的分配模式
	 * 这个函数处理了加入消费组和同步消费组两项工作，由协调器选举leader时委托给{@link #performAssignment(String, String, List)}方法
	 * @return 主消费者返回的包装了分配模式的请求回调异步任务
     */
    RequestFuture<ByteBuffer> sendJoinGroupRequest() {
		// 如果协调器处于未知状态，返回协调器不可用的异步任务
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

		// 构建加入消费组的请求
        log.info("(Re-)joining group");
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                new JoinGroupRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setSessionTimeoutMs(this.rebalanceConfig.sessionTimeoutMs)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setProtocolType(protocolType())
                        .setProtocols(metadata())
                        .setRebalanceTimeoutMs(this.rebalanceConfig.rebalanceTimeoutMs)
        );

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);

		// 获取请求的等待时间
		// 取设置的再平衡的超时时间，和再平衡超时时间+5s之间的最大值
		// 需要注意的是，使用设置的再平衡的超时时间重写了请求的超时时间，因为可能需要在协调器上阻塞一会
		// 我们又添加了5s的小等待
        int joinGroupTimeoutMs = Math.max(rebalanceConfig.rebalanceTimeoutMs, rebalanceConfig.rebalanceTimeoutMs + 5000);
		// 发送加入消费组请求，并等待回调异步任务
        return client.send(coordinator, requestBuilder, joinGroupTimeoutMs)
                .compose(new JoinGroupResponseHandler());
	}

	/**
	 * Follower节点发送同步消费组订阅信息请求
	 * @return 返回节点加入结果
	 */
	private RequestFuture<ByteBuffer> onJoinFollower() {
		// 如果是follower节点的情况，那么不进行分配策略，发送空的分配策略
		SyncGroupRequest.Builder requestBuilder =
				new SyncGroupRequest.Builder(
						new SyncGroupRequestData()
								.setGroupId(rebalanceConfig.groupId)
								.setMemberId(generation.memberId)
								.setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
								.setGenerationId(generation.generationId)
								// 空的分配策略
								.setAssignments(Collections.emptyList())
				);
		log.debug("Sending follower SyncGroup to coordinator {}: {}", this.coordinator, requestBuilder);
		return sendSyncGroupRequest(requestBuilder);
	}

	/**
	 * 主消费者在收到加入消费组响应后，执行完分区分配工作，才会发送同步组请求
	 * @param joinResponse 收到的加入消费组响应
	 * @return
	 */
	private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
		try {
			// 执行消费组级别的任务分配工作，响应结果中有分配的协议、所有consumer的全局信息
			Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.data().leader(),
					joinResponse.data().protocolName(),
					joinResponse.data().members());
			// 遍历包装成同步消费组请求分配中
			List<SyncGroupRequestData.SyncGroupRequestAssignment> groupAssignmentList = new ArrayList<>();
			for (Map.Entry<String, ByteBuffer> assignment : groupAssignment.entrySet()) {
				groupAssignmentList.add(new SyncGroupRequestData.SyncGroupRequestAssignment()
						.setMemberId(assignment.getKey())
						.setAssignment(Utils.toArray(assignment.getValue()))
				);
			}
			// 构建同步请求
			SyncGroupRequest.Builder requestBuilder =
					new SyncGroupRequest.Builder(
							new SyncGroupRequestData()
									.setGroupId(rebalanceConfig.groupId)
									.setMemberId(generation.memberId)
									.setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
									.setGenerationId(generation.generationId)
									.setAssignments(groupAssignmentList)
					);
			log.debug("Sending leader SyncGroup to coordinator {}: {}", this.coordinator, requestBuilder);
			// 发送同步请出去
			return sendSyncGroupRequest(requestBuilder);
		} catch (RuntimeException e) {
			return RequestFuture.failure(e);
		}
	}

	/**
	 * 发送集群同步请求给协调器
	 * @param requestBuilder 同步请求构造器
	 * @return 发送请求的回调任务
	 */
	private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
		if (coordinatorUnknown())
			return RequestFuture.coordinatorNotAvailable();
		return client.send(coordinator, requestBuilder)
				.compose(new SyncGroupResponseHandler());
	}

	/**
	 * 设置需要重新加入消费组的需求为true
	 */
	protected synchronized void requestRejoin() {
		this.rejoinNeeded = true;
	}

	private enum MemberState {
		UNJOINED,    // client还没有在消费组中
		REBALANCING, // client开始进行再平衡
		STABLE,      // client已经加入消费组，并且开始发送心跳
	}

	/**
	 * 加入消费组响应处理器
	 */
	private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
		@Override
		public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
			Errors error = joinResponse.error();
			if (error == Errors.NONE) {
				log.debug("Received successful JoinGroup response: {}", joinResponse);
				sensors.joinLatency.record(response.requestLatencyMs());

				synchronized (AbstractCoordinator.this) {
					// 如果当前consumer的状态不是再平衡
					if (state != MemberState.REBALANCING) {
						// 如果consumer在再平衡完成之前被唤醒，代表可能已经离开消费组
						// 代表可能不需要继续同步数据，直接铲产生没有加入到消费组异常
						future.raise(new UnjoinedGroupException());
					} else {
						// 在consumer处于再平衡状态下
						// 根据响应信息构建当前的generation信息
						AbstractCoordinator.this.generation = new Generation(joinResponse.data().generationId(),
								joinResponse.data().memberId(), joinResponse.data().protocolName());
						// 如果当前consumer被选为leader
						if (joinResponse.isLeader()) {
							// 构建leader节点调用链，再添加同步的listener
							onJoinLeader(joinResponse).chain(future);
						} else {
							// 否则构建follower节点调用链，再添加的同步的listener
							onJoinFollower().chain(future);
						}
					}
				}
				// 出现异常，产生错误
			} else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
				log.debug("Attempt to join group rejected since coordinator {} is loading the group.", coordinator());
				// backoff and retry
				future.raise(error);
			} else if (error == Errors.UNKNOWN_MEMBER_ID) {
				// reset the member id and retry immediately
				resetGenerationOnResponseError(ApiKeys.JOIN_GROUP, error);
				log.debug("Attempt to join group failed due to unknown member id.");
				future.raise(error);
			} else if (error == Errors.COORDINATOR_NOT_AVAILABLE
					|| error == Errors.NOT_COORDINATOR) {
				// re-discover the coordinator and retry with backoff
				markCoordinatorUnknown();
				log.debug("Attempt to join group failed due to obsolete coordinator information: {}", error.message());
				future.raise(error);
			} else if (error == Errors.FENCED_INSTANCE_ID) {
				log.error("Received fatal exception: group.instance.id gets fenced");
				future.raise(error);
			} else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
					|| error == Errors.INVALID_SESSION_TIMEOUT
					|| error == Errors.INVALID_GROUP_ID
					|| error == Errors.GROUP_AUTHORIZATION_FAILED
					|| error == Errors.GROUP_MAX_SIZE_REACHED) {
				// log the error and re-throw the exception
				log.error("Attempt to join group failed due to fatal error: {}", error.message());
				if (error == Errors.GROUP_MAX_SIZE_REACHED) {
					future.raise(new GroupMaxSizeReachedException("Consumer group " + rebalanceConfig.groupId +
							" already has the configured maximum number of members."));
				} else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
					future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
				} else {
					future.raise(error);
				}
			} else if (error == Errors.UNSUPPORTED_VERSION) {
				log.error("Attempt to join group failed due to unsupported version error. Please unset field group.instance.id and retry" +
						"to see if the problem resolves");
				future.raise(error);
			} else if (error == Errors.MEMBER_ID_REQUIRED) {
				// 错误是没有传memberId，但是broker需要memberId，更新memberId，然后再下一次循环中发送下一个加入消费组的请求
				synchronized (AbstractCoordinator.this) {
					AbstractCoordinator.this.generation = new Generation(OffsetCommitRequest.DEFAULT_GENERATION_ID,
							joinResponse.data().memberId(), null);
					AbstractCoordinator.this.rejoinNeeded = true;
					AbstractCoordinator.this.state = MemberState.UNJOINED;
				}
				future.raise(error);
			} else {
				// 未知错误，包装成KafkaException异常
				log.error("Attempt to join group failed due to unexpected error: {}", error.message());
				future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
			}
		}
	}

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending FindCoordinator request to broker {}", node);
        FindCoordinatorRequest.Builder requestBuilder =
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                            .setKeyType(CoordinatorType.GROUP.id())
                            .setKey(this.rebalanceConfig.groupId));
        return client.send(node, requestBuilder)
                .compose(new FindCoordinatorResponseHandler());
    }

    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received FindCoordinator response {}", resp);
            clearFindCoordinatorFuture();

            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
            Errors error = findCoordinatorResponse.error();
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {
                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer
                    int coordinatorConnectionId = Integer.MAX_VALUE - findCoordinatorResponse.data().nodeId();

                    AbstractCoordinator.this.coordinator = new Node(
                            coordinatorConnectionId,
                            findCoordinatorResponse.data().host(),
                            findCoordinatorResponse.data().port());
                    log.info("Discovered group coordinator {}", coordinator);
                    client.tryConnect(coordinator);
                    heartbeat.resetSessionTimeout();
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                log.debug("Group coordinator lookup failed: {}", findCoordinatorResponse.data().errorMessage());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            clearFindCoordinatorFuture();
            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator() == null;
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node checkAndGetCoordinator() {
        if (coordinator != null && client.isUnavailable(coordinator)) {
            markCoordinatorUnknown(true);
            return null;
        }
        return this.coordinator;
    }

    private synchronized Node coordinator() {
        return this.coordinator;
    }

	/**
	 * 在不断开连接的情况下，标记协调者的状态为未知状态
	 */
	protected synchronized void markCoordinatorUnknown() {
		markCoordinatorUnknown(false);
	}

	/**
	 * 标记协调者的状态为未知状态
	 * @param isDisconnected 是否已经断开连接
	 */
	protected synchronized void markCoordinatorUnknown(boolean isDisconnected) {
		if (this.coordinator != null) {
			log.info("Group coordinator {} is unavailable or invalid, will attempt rediscovery", this.coordinator);
			Node oldCoordinator = this.coordinator;

			// 先转移协调者，以便执行后续的回调任务，同时也阻止了新的请求仍然继续发送到协调者
			this.coordinator = null;

			// 没有断开连接，执行异步断开连接操作
			if (!isDisconnected)
				client.disconnectAsync(oldCoordinator);
		}
	}

	/**
	 * 获取稳定消费组的当前版本
	 * @return 获取当前的版本，如果消费组处于未加入或者再平衡，返回null
     */
    protected synchronized Generation generation() {
        if (this.state != MemberState.STABLE)
            return null;
        return generation;
    }

    protected synchronized String memberId() {
        return generation == null ? JoinGroupRequest.UNKNOWN_MEMBER_ID :
                generation.memberId;
    }

    /**
     * Check whether given generation id is matching the record within current generation.
     * Only using in unit tests.
     * @param generationId generation id
     * @return true if the two ids are matching.
     */
    final synchronized boolean hasMatchingGenerationId(int generationId) {
        return generation != null && generation.generationId == generationId;
    }

    /**
     * @return true if the current generation's member ID is valid, false otherwise
     */
    // Visible for testing
    final synchronized boolean hasValidMemberId() {
        return generation != null && generation.hasMemberId();
    }

    private synchronized void resetGeneration() {
        this.generation = Generation.NO_GENERATION;
        this.state = MemberState.UNJOINED;
        this.rejoinNeeded = true;
    }

    synchronized void resetGenerationOnResponseError(ApiKeys api, Errors error) {
        log.debug("Resetting generation after encountering " + error + " from " + api + " response");
        resetGeneration();
    }

    synchronized void resetGenerationOnLeaveGroup() {
        log.debug("Resetting generation due to consumer pro-actively leaving the group");
		resetGeneration();
	}

	/**
	 * 同步消费组信息响应处理器
	 */
	private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
		@Override
		public void handle(SyncGroupResponse syncResponse,
						   RequestFuture<ByteBuffer> future) {
			Errors error = syncResponse.error();
			if (error == Errors.NONE) {
				// 记录的请求时间
				sensors.syncLatency.record(response.requestLatencyMs());
				// 包装分配的任务，将分配partition的数据加入到future中
				future.complete(ByteBuffer.wrap(syncResponse.data.assignment()));
			} else {
				// 出现异常，首先再次发送加入消费组请求
				requestRejoin();
				// 然后接着处理异常
				if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
					future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
				} else if (error == Errors.REBALANCE_IN_PROGRESS) {
					log.debug("SyncGroup failed because the group began another rebalance");
					future.raise(error);
				} else if (error == Errors.FENCED_INSTANCE_ID) {
					log.error("Received fatal exception: group.instance.id gets fenced");
					future.raise(error);
				} else if (error == Errors.UNKNOWN_MEMBER_ID
						|| error == Errors.ILLEGAL_GENERATION) {
					log.debug("SyncGroup failed: {}", error.message());
					resetGenerationOnResponseError(ApiKeys.SYNC_GROUP, error);
					future.raise(error);
				} else if (error == Errors.COORDINATOR_NOT_AVAILABLE
						|| error == Errors.NOT_COORDINATOR) {
					log.debug("SyncGroup failed: {}", error.message());
					markCoordinatorUnknown();
					future.raise(error);
				} else {
					future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public final void close() {
        close(time.timer(0));
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    protected void close(Timer timer) {
        try {
            closeHeartbeatThread();
        } finally {
            // Synchronize after closing the heartbeat thread since heartbeat thread
            // needs this lock to complete and terminate after close flag is set.
            synchronized (this) {
                if (rebalanceConfig.leaveGroupOnClose) {
                    onLeavePrepare();
                    maybeLeaveGroup("the consumer is being closed");
                }

                // At this point, there may be pending commits (async commits or sync commits that were
                // interrupted using wakeup) and the leave group request which have been queued, but not
                // yet sent to the broker. Wait up to close timeout for these pending requests to be processed.
                // If coordinator is not known, requests are aborted.
                Node coordinator = checkAndGetCoordinator();
                if (coordinator != null && !client.awaitPendingRequests(coordinator, timer))
                    log.warn("Close timed out with {} pending requests to coordinator, terminating client connections",
                            client.pendingRequestCount(coordinator));
            }
        }
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public synchronized RequestFuture<Void> maybeLeaveGroup(String leaveReason) {
        RequestFuture<Void> future = null;

        // Starting from 2.3, only dynamic members will send LeaveGroupRequest to the broker,
        // consumer with valid group.instance.id is viewed as static member that never sends LeaveGroup,
        // and the membership expiration is only controlled by session timeout.
        if (isDynamicMember() && !coordinatorUnknown() &&
            state != MemberState.UNJOINED && generation.hasMemberId()) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            log.info("Member {} sending LeaveGroup request to coordinator {} due to {}",
                generation.memberId, coordinator, leaveReason);
            LeaveGroupRequest.Builder request = new LeaveGroupRequest.Builder(
                rebalanceConfig.groupId,
                Collections.singletonList(new MemberIdentity().setMemberId(generation.memberId))
            );

            future = client.send(coordinator, request).compose(new LeaveGroupResponseHandler());
            client.pollNoWakeup();
        }

        resetGenerationOnLeaveGroup();

        return future;
    }

    protected boolean isDynamicMember() {
        return !rebalanceConfig.groupInstanceId.isPresent();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            final List<MemberResponse> members = leaveResponse.memberResponses();
            if (members.size() > 1) {
                future.raise(new IllegalStateException("The expected leave group response " +
                                                           "should only contain no more than one member info, however get " + members));
            }

            final Errors error = leaveResponse.error();
            if (error == Errors.NONE) {
                log.debug("LeaveGroup request returned successfully");
                future.complete(null);
            } else {
                log.error("LeaveGroup request failed with error: {}", error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request to coordinator {}", coordinator);
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(new HeartbeatRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setGenerationId(this.generation.generationId));
        return client.send(coordinator, requestBuilder)
                .compose(new HeartbeatResponseHandler());
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            Errors error = heartbeatResponse.error();
            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response");
                future.complete(null);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid.",
                        coordinator());
                markCoordinatorUnknown();
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                log.info("Attempt to heartbeat failed since group is rebalancing");
                requestRejoin();
                future.raise(error);
            } else if (error == Errors.ILLEGAL_GENERATION) {
                log.info("Attempt to heartbeat failed since generation {} is not current", generation.generationId);
                resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                future.raise(error);
            } else if (error == Errors.FENCED_INSTANCE_ID) {
                log.error("Received fatal exception: group.instance.id gets fenced");
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                log.info("Attempt to heartbeat failed for since member id {} is not valid.", generation.memberId);
                resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                future.raise(error);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                markCoordinatorUnknown(true);
            }
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

    }

    protected Meter createMeter(Metrics metrics, String groupName, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
                metrics.metricName(baseName + "-rate", groupName,
                        String.format("The number of %s per second", descriptiveName)),
                metrics.metricName(baseName + "-total", groupName,
                        String.format("The total number of %s", descriptiveName)));
    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                    this.metricGrpName,
                    "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(createMeter(metrics, metricGrpName, "heartbeat", "heartbeats"));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Max());
            this.joinLatency.add(createMeter(metrics, metricGrpName, "join", "group joins"));


            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Max());
            this.syncLatency.add(createMeter(metrics, metricGrpName, "sync", "group syncs"));

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last coordinator heartbeat was sent"),
                lastHeartbeat);
        }
    }

    private class HeartbeatThread extends KafkaThread implements AutoCloseable {
        private boolean enabled = false;
        private boolean closed = false;
        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        private HeartbeatThread() {
            super(HEARTBEAT_THREAD_PREFIX + (rebalanceConfig.groupId.isEmpty() ? "" : " | " + rebalanceConfig.groupId), true);
        }

        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Enabling heartbeat thread");
                this.enabled = true;
                heartbeat.resetTimeouts();
                AbstractCoordinator.this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Disabling heartbeat thread");
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread started");
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed)
                            return;

                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        if (state != MemberState.STABLE) {
                            // the group is not stable (perhaps because we left the group or because the coordinator
                            // kicked us out), so disable heartbeats and wait for the main thread to rejoin.
                            disable();
                            continue;
                        }

                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture != null || lookupCoordinator().failed())
                                // the immediate future check ensures that we backoff properly in the case that no
                                // brokers are available to connect to.
                                AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            markCoordinatorUnknown();
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll().
                            String leaveReason = "consumer poll timeout has expired. This means the time between subsequent calls to poll() " +
                                                    "was longer than the configured max.poll.interval.ms, which typically implies that " +
                                                    "the poll loop is spending too much time processing messages. " +
                                                    "You can address this either by increasing max.poll.interval.ms or by reducing " +
                                                    "the maximum size of batches returned in poll() with max.poll.records.";
                            maybeLeaveGroup(leaveReason);
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);

                            sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat();
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat();
                                        } else if (e instanceof FencedInstanceIdException) {
                                            log.error("Caught fenced group.instance.id {} error in heartbeat thread", rebalanceConfig.groupInstanceId);
                                            heartbeatThread.failed.set(e);
                                            heartbeatThread.disable();
                                        } else {
                                            heartbeat.failHeartbeat();
                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (AuthenticationException e) {
                log.error("An authentication error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (GroupAuthorizationException e) {
                log.error("A group authorization error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread", e);
                this.failed.set(new RuntimeException(e));
            } catch (Throwable e) {
                log.error("Heartbeat thread failed due to unexpected error", e);
                if (e instanceof RuntimeException)
                    this.failed.set((RuntimeException) e);
                else
                    this.failed.set(new RuntimeException(e));
            } finally {
                log.debug("Heartbeat thread has closed");
            }
        }

    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupResponse.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocol;

        public Generation(int generationId, String memberId, String protocol) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocol = protocol;
        }

        /**
         * @return true if this generation has a valid member id, false otherwise. A member might have an id before
         * it becomes part of a group generation.
         */
        public boolean hasMemberId() {
            return !memberId.isEmpty();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Generation that = (Generation) o;
            return generationId == that.generationId &&
                    Objects.equals(memberId, that.memberId) &&
                    Objects.equals(protocol, that.protocol);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generationId, memberId, protocol);
        }

        @Override
        public String toString() {
            return "Generation{" +
                    "generationId=" + generationId +
                    ", memberId='" + memberId + '\'' +
                    ", protocol='" + protocol + '\'' +
                    '}';
        }
    }

    private static class UnjoinedGroupException extends RetriableException {

    }

    // For testing only
    public Heartbeat heartbeat() {
        return heartbeat;
    }
}
