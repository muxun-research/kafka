/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.group

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch.{NO_PRODUCER_EPOCH, NO_PRODUCER_ID}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, immutable}
import scala.math.max

/**
 * GroupCoordinator处理一般的消费组consumer关系和offset管理
 *
 * 每个Kafka服务端都会实例化一个协调起来负责一个消费组集合，消费组基于消费组名称的方式分配给协调器
 *
 * <b>Delayed operation locking notes:</b>
 * 延迟操作锁提示
 * 在消费组协调器中的延迟操作使用"group"作为延迟操作锁，在持有消费组锁时，ReplicaManager.appendRecords可能会被调用
 * 延迟的毁掉任务可能需要持有消费组锁，因为延迟的操作之后再占有消费组锁的前提下，才会完成
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time,
                       metrics: Metrics) extends Logging {

  import GroupCoordinator._

  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = SyncGroupResult => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true): Unit = {
    info("Starting up.")
    groupManager.startup(enableMetadataExpiration)
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown(): Unit = {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  /**
   * 处理加入组请求
   * @param groupId
   * @param memberId
   * @param groupInstanceId
   * @param requireKnownMemberId
   * @param clientId
   * @param clientHost
   * @param rebalanceTimeoutMs
   * @param sessionTimeoutMs
   * @param protocolType
   * @param protocols
   * @param responseCallback
   */
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      requireKnownMemberId: Boolean,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    // 首先校验消费组的状态
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      // 异常状态下抛出异常
      responseCallback(joinError(memberId, error))
      return
    }

    // 请求会话超时时间小于集群设定的最小会话请求超时时间，或者大于集群设定的最大会话超时时间
    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      // 返回异常超时时间错误
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      // 请求成员是否是未知成员
      val isUnknownMember = memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID
      // 遍历消费组列表，是否有指定的消费组
      groupManager.getGroup(groupId) match {
        case None =>
          // 如果消费组位置并且member.id位置，创建消费组
          // 如果指定了成员但是消费组却不存在，拒绝加入消费组请求
          if (isUnknownMember) {
            // 创建新的消费组
            val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
            // 执行加入未知消费组
            doUnknownJoinGroup(group, groupInstanceId, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          } else {
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          }
        case Some(group) =>
          group.inLock {
            if ((groupIsOverCapacity(group)
              && group.has(memberId) && !group.get(memberId).isAwaitingJoin) // oversized group, need to shed members that haven't joined yet
              || (isUnknownMember && group.size >= groupConfig.groupMaxSize)) {
              group.remove(memberId)
              group.removeStaticMember(groupInstanceId)
              responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED))
            } else if (isUnknownMember) {
              doUnknownJoinGroup(group, groupInstanceId, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            } else {
              doJoinGroup(group, memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            }

            // attempt to complete JoinGroup
            if (group.is(PreparingRebalance)) {
              joinPurgatory.checkAndComplete(GroupKey(group.groupId))
            }
          }
      }
    }
  }

  def handleLeaveGroup(groupId: String,
                       leavingMembers: List[MemberIdentity],
                       responseCallback: LeaveGroupResult => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP) match {
      case Some(error) =>
        responseCallback(leaveError(error, List.empty))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            responseCallback(leaveError(Errors.NONE, leavingMembers.map { leavingMember =>
              memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
            }))
          case Some(group) =>
            group.inLock {
              if (group.is(Dead)) {
                responseCallback(leaveError(Errors.COORDINATOR_NOT_AVAILABLE, List.empty))
              } else {
                val memberErrors = leavingMembers.map { leavingMember =>
                  val memberId = leavingMember.memberId
                  val groupInstanceId = Option(leavingMember.groupInstanceId)
                  if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID
                    && group.isStaticMemberFenced(memberId, groupInstanceId)) {
                    memberLeaveError(leavingMember, Errors.FENCED_INSTANCE_ID)
                  } else if (group.isPendingMember(memberId)) {
                    if (groupInstanceId.isDefined) {
                      throw new IllegalStateException(s"the static member $groupInstanceId was not expected to be leaving " +
                        s"from pending member bucket with member id $memberId")
                    } else {
                      // if a pending member is leaving, it needs to be removed from the pending list, heartbeat cancelled
                      // and if necessary, prompt a JoinGroup completion.
                      info(s"Pending member $memberId is leaving group ${group.groupId}.")
                      removePendingMemberAndUpdateGroup(group, memberId)
                      heartbeatPurgatory.checkAndComplete(MemberKey(group.groupId, memberId))
                      memberLeaveError(leavingMember, Errors.NONE)
                    }
                  } else if (!group.has(memberId) && !group.hasStaticMember(groupInstanceId)) {
                    memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
                  } else {
                    val member = if (group.hasStaticMember(groupInstanceId))
                      group.get(group.getStaticMemberId(groupInstanceId))
                    else
                      group.get(memberId)
                    removeHeartbeatForLeavingMember(group, member)
                    info(s"Member[group.instance.id ${member.groupInstanceId}, member.id ${member.memberId}] " +
                      s"in group ${group.groupId} has left, removing it from the group")
                    removeMemberAndUpdateGroup(group, member, s"removing member $memberId on LeaveGroup")
                    memberLeaveError(leavingMember, Errors.NONE)
                  }
                }
                responseCallback(leaveError(Errors.NONE, memberErrors))
              }
            }
        }
    }
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      generationId: Int,
                      responseCallback: Errors => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.HEARTBEAT).foreach { error =>
      if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
      // the group is still loading, so respond just blindly
        responseCallback(Errors.NONE)
      else
        responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) => group.inLock {
        if (group.is(Dead)) {
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the member retry
          // finding the correct coordinator and rejoin.
          responseCallback(Errors.COORDINATOR_NOT_AVAILABLE)
        } else if (group.isStaticMemberFenced(memberId, groupInstanceId)) {
          responseCallback(Errors.FENCED_INSTANCE_ID)
        } else if (!group.has(memberId)) {
          responseCallback(Errors.UNKNOWN_MEMBER_ID)
        } else if (generationId != group.generationId) {
          responseCallback(Errors.ILLEGAL_GENERATION)
        } else {
          group.currentState match {
            case Empty =>
              responseCallback(Errors.UNKNOWN_MEMBER_ID)

            case CompletingRebalance =>
              responseCallback(Errors.REBALANCE_IN_PROGRESS)

            case PreparingRebalance =>
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.REBALANCE_IN_PROGRESS)

            case Stable =>
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.NONE)

            case Dead =>
              throw new IllegalStateException(s"Reached unexpected condition for Dead group $groupId")
          }
        }
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupInstanceId: Option[String],
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback): Unit = {
    validateGroupStatus(groupId, ApiKeys.SYNC_GROUP) match {
      case Some(error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS =>
        // The coordinator is loading, which means we've lost the state of the active rebalance and the
        // group will need to start over at JoinGroup. By returning rebalance in progress, the consumer
        // will attempt to rejoin without needing to rediscover the coordinator. Note that we cannot
        // return COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect the error.
        responseCallback(SyncGroupResult(Array.empty, Errors.REBALANCE_IN_PROGRESS))

      case Some(error) => responseCallback(SyncGroupResult(Array.empty, error))

      case None =>
        groupManager.getGroup(groupId) match {
          case None => responseCallback(SyncGroupResult(Array.empty, Errors.UNKNOWN_MEMBER_ID))
          case Some(group) => doSyncGroup(group, generation, memberId, groupInstanceId, groupAssignment, responseCallback)
        }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupInstanceId: Option[String],
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(SyncGroupResult(Array.empty, Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId)) {
        responseCallback(SyncGroupResult(Array.empty, Errors.FENCED_INSTANCE_ID))
      } else if (!group.has(memberId)) {
        responseCallback(SyncGroupResult(Array.empty, Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        responseCallback(SyncGroupResult(Array.empty, Errors.ILLEGAL_GENERATION))
      } else {
        group.currentState match {
          case Empty =>
            responseCallback(SyncGroupResult(Array.empty, Errors.UNKNOWN_MEMBER_ID))

          case PreparingRebalance =>
            responseCallback(SyncGroupResult(Array.empty, Errors.REBALANCE_IN_PROGRESS))

          case CompletingRebalance =>
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            if (group.isLeader(memberId)) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the CompletingRebalance state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group, s"error when storing group assignment during SyncGroup (member: $memberId)")
                    } else {
                      setAndPropagateAssignment(group, assignment)
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(SyncGroupResult(memberMetadata.assignment, Errors.NONE))
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))

          case Dead =>
            throw new IllegalStateException(s"Reached unexpected condition for Dead group ${group.groupId}")
        }
      }
    }
  }

  /**
   * 校验group.id是否合法，已分配给当前协调器，并且消费组是否已经加载
   */
  private def validateGroupStatus(groupId: String, api: ApiKeys): Option[Errors] = {
    // 校验group.id
    if (!isValidGroupId(groupId, api))
      Some(Errors.INVALID_GROUP_ID)
    // 消费组协调器是否处于活跃状态
    else if (!isActive.get)
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    // 消费组协调是否已经加载指定group.id的partition
    else if (isCoordinatorLoadInProgress(groupId))
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    // 当前消费组协调器是否是负责当前消费组的
    else if (!isCoordinatorForGroup(groupId))
      Some(Errors.NOT_COORDINATOR)
    else
      None
  }

  def handleDeleteGroups(groupIds: Set[String]): Map[String, Errors] = {
    var groupErrors: Map[String, Errors] = Map()
    var groupsEligibleForDeletion: Seq[GroupMetadata] = Seq()

    groupIds.foreach { groupId =>
      validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS) match {
        case Some(error) =>
          groupErrors += groupId -> error

        case None =>
          groupManager.getGroup(groupId) match {
            case None =>
              groupErrors += groupId ->
                (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
            case Some(group) =>
              group.inLock {
                group.currentState match {
                  case Dead =>
                    groupErrors += groupId ->
                      (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
                  case Empty =>
                    group.transitionTo(Dead)
                    groupsEligibleForDeletion :+= group
                  case Stable | PreparingRebalance | CompletingRebalance =>
                    groupErrors += groupId -> Errors.NON_EMPTY_GROUP
                }
              }
          }
      }
    }

    if (groupsEligibleForDeletion.nonEmpty) {
      val offsetsRemoved = groupManager.cleanupGroupMetadata(groupsEligibleForDeletion, _.removeAllOffsets())
      groupErrors ++= groupsEligibleForDeletion.map(_.groupId -> Errors.NONE).toMap
      info(s"The following groups were deleted: ${groupsEligibleForDeletion.map(_.groupId).mkString(", ")}. " +
        s"A total of $offsetsRemoved offsets were removed.")
    }

    groupErrors
  }

  /**
   * 执行加入未知消费组
   * @param group
   * @param groupInstanceId
   * @param requireKnownMemberId
   * @param clientId
   * @param clientHost
   * @param rebalanceTimeoutMs
   * @param sessionTimeoutMs
   * @param protocolType
   * @param protocols
   * @param responseCallback
   */
  private def doUnknownJoinGroup(group: GroupMetadata,
                                 groupInstanceId: Option[String],
                                 requireKnownMemberId: Boolean,
                                 clientId: String,
                                 clientHost: String,
                                 rebalanceTimeoutMs: Int,
                                 sessionTimeoutMs: Int,
                                 protocolType: String,
                                 protocols: List[(String, Array[Byte])],
                                 responseCallback: JoinCallback): Unit = {
    // 加锁
    group.inLock {
      // 如果消费组已经死掉，返回协调器不可用异常
      if (group.is(Dead)) {
        // 证明其他线程刚刚已经将消费组从协调器metadata中移除去，可能消费组已经迁移到其他的协调器中或者消费组处于不稳定状态
        // 成员会通过重试来获取正确的协调器并重新加入消费组
        responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        // 当前消费组不支持此分配协议，返回不支持此分配协议错误
        responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else {
        // 根据group.instance.id和client.id生成当前成员在消费组中唯一的member.id
        val newMemberId = group.generateMemberId(clientId, groupInstanceId)
        // 如果当前消费组已经拥有此静态成员
        if (group.hasStaticMember(groupInstanceId)) {
          // 获取旧的成员member.id
          val oldMemberId = group.getStaticMemberId(groupInstanceId)
          info(s"Static member $groupInstanceId with unknown member id rejoins, assigning new member id $newMemberId, while " +
            s"old member $oldMemberId will be removed.")
          // 获取当前消费组的leader consumer
          val currentLeader = group.leaderOrNull
          // 更新静态成员信息
          val member = group.replaceGroupInstance(oldMemberId, newMemberId, groupInstanceId)
          // 老member的心跳会失效，因为老member.id已经不存在于消费组中
          // 新心跳机制会使用新的member.id继续定时进行
          completeAndScheduleNextHeartbeatExpiration(group, member)
          // 从消费组中获取指定的成员信息
          val knownStaticMember = group.get(newMemberId)
          // 更新消费组中的成员信息
          group.updateMember(knownStaticMember, protocols, responseCallback)
          // 过滤当前集群的状态
          group.currentState match {
            // 稳定或者正在完成再平衡
            case Stable | CompletingRebalance =>
              // 静态成员在此时加入消费组不会触发再平衡
              info(s"Static member joins during ${group.currentState} stage will not trigger rebalance.")
              group.maybeInvokeJoinCallback(member, JoinGroupResult(
                members = List.empty,
                memberId = newMemberId,
                generationId = group.generationId,
                subProtocol = group.protocolOrNull,
                // 需要避免当前leader consumer在消费组处于稳定/等待同步的状态时，执行琐碎的分配操作
                // 可以保证每次都返回老的leader id以便当前leader consumer不会消费根据返回的消息将自己假设为leader consumer
                // 因为新的member.id与返回的leader id不匹配，因此不会执行任何分配
                leaderId = currentLeader,
                error = Errors.NONE))
            // 死亡状态
            case Empty | Dead =>
              throw new IllegalStateException(s"Group ${group.groupId} was not supposed to be " +
                s"in the state ${group.currentState} when the unknown static member $groupInstanceId rejoins.")
            // 准备进行再平衡，不进行任何回调
            case PreparingRebalance =>
          }
        } else if (requireKnownMemberId) {
          // 如果是动态成员，并且有已知的member.id，注册到挂起成员列表中，并且返回响应
          debug(s"Dynamic member with unknown member id rejoins group ${group.groupId} in " +
            s"${group.currentState} state. Created a new member id $newMemberId and request the member to rejoin with this id.")
          group.addPendingMember(newMemberId)
          // 设置新成员的失效机制（心跳）
          addPendingMemberExpiration(group, newMemberId, sessionTimeoutMs)
          // 返回响应
          responseCallback(joinError(newMemberId, Errors.MEMBER_ID_REQUIRED))
        } else {
          debug(s"Dynamic member with unknown member id rejoins group ${group.groupId} in " +
            s"${group.currentState} state. Created a new member id $newMemberId for this member and add to the group.")
          // 如果动态成员是新成员，需要加入消费组
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, newMemberId, groupInstanceId,
            clientId, clientHost, protocolType, protocols, group, responseCallback)
        }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.TXN_OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.map { case (k, _) => k -> error })
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse {
          groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
        }
        doCommitOffsets(group, NoMemberId, None, NoGeneration, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          groupInstanceId: Option[String],
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.map { case (k, _) => k -> error })
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
              doCommitOffsets(group, memberId, groupInstanceId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
            }

          case Some(group) =>
            doCommitOffsets(group, memberId, groupInstanceId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
              offsetMetadata, responseCallback)
        }
    }
  }

  def scheduleHandleTxnCompletion(producerId: Long,
                                  offsetsPartitions: Iterable[TopicPartition],
                                  transactionResult: TransactionResult): Unit = {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.scheduleHandleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              groupInstanceId: Option[String],
                              generationId: Int,
                              producerId: Long,
                              producerEpoch: Short,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.COORDINATOR_NOT_AVAILABLE })
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId)) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.FENCED_INSTANCE_ID })
      } else if ((generationId < 0 && group.is(Empty)) || (producerId != NO_PRODUCER_ID)) {
        // The group is only using Kafka to store offsets.
        // Also, for transactional offset commits we don't need to validate group membership and the generation.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.UNKNOWN_MEMBER_ID })
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
      } else {
        group.currentState match {
          case Stable | PreparingRebalance =>
            // During PreparingRebalance phase, we still allow a commit request since we rely
            // on heartbeat response to eventually notify the rebalance in progress signal to the consumer
            val member = group.get(memberId)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)

          case CompletingRebalance =>
            // We should not receive a commit request if the group has not completed rebalance;
            // but since the consumer's member.id and generation is valid, it means it has received
            // the latest group generation information from the JoinResponse.
            // So let's return a REBALANCE_IN_PROGRESS to let consumer handle it gracefully.
            responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.REBALANCE_IN_PROGRESS })

          case _ =>
            throw new RuntimeException(s"Logic error: unexpected group state ${group.currentState}")
        }
      }
    }
  }

  def handleFetchOffsets(groupId: String, partitions: Option[Seq[TopicPartition]] = None):
  (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {

    validateGroupStatus(groupId, ApiKeys.OFFSET_FETCH) match {
      case Some(error) => error -> Map.empty
      case None =>
        // return offsets blindly regardless the current group state since the group may be using
        // Kafka commit storage without automatic group management
        (Errors.NONE, groupManager.getOffsets(groupId, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS) match {
      case Some(error) => (error, GroupCoordinator.EmptyGroup)
      case None =>
        groupManager.getGroup(groupId) match {
          case None => (Errors.NONE, GroupCoordinator.DeadGroup)
          case Some(group) =>
            group.inLock {
              (Errors.NONE, group.summary)
            }
        }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]): Unit = {
    val offsetsRemoved = groupManager.cleanupGroupMetadata(groupManager.currentGroups, group => {
      group.removeOffsets(topicPartitions)
    })
    info(s"Removed $offsetsRemoved offsets associated with deleted partitions: ${topicPartitions.mkString(", ")}.")
  }

  private def isValidGroupId(groupId: String, api: ApiKeys): Boolean = {
    api match {
      case ApiKeys.OFFSET_COMMIT | ApiKeys.OFFSET_FETCH | ApiKeys.DESCRIBE_GROUPS | ApiKeys.DELETE_GROUPS =>
        // For backwards compatibility, we support the offset commit APIs for the empty groupId, and also
        // in DescribeGroups and DeleteGroups so that users can view and delete state of all groups.
        groupId != null
      case _ =>
        // The remaining APIs are groups using Kafka for group coordination and must have a non-empty groupId
        groupId != null && !groupId.isEmpty
    }
  }

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          groupInstanceId: Option[String],
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(joinError(memberId, Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (group.isPendingMember(memberId)) {
        // A rejoining pending member will be accepted. Note that pending member will never be a static member.
        if (groupInstanceId.isDefined) {
          throw new IllegalStateException(s"the static member $groupInstanceId was not expected to be assigned " +
            s"into pending member bucket with member id $memberId")
        } else {
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, memberId, groupInstanceId,
            clientId, clientHost, protocolType, protocols, group, responseCallback)
        }
      } else {
        val groupInstanceIdNotFound = groupInstanceId.isDefined && !group.hasStaticMember(groupInstanceId)
        if (group.isStaticMemberFenced(memberId, groupInstanceId)) {
          // given member id doesn't match with the groupInstanceId. Inform duplicate instance to shut down immediately.
          responseCallback(joinError(memberId, Errors.FENCED_INSTANCE_ID))
        } else if (!group.has(memberId) || groupInstanceIdNotFound) {
          // If the dynamic member trying to register with an unrecognized id, or
          // the static member joins with unknown group instance id, send the response to let
          // it reset its member id and retry.
          responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
        } else {
          val member = group.get(memberId)

          group.currentState match {
            case PreparingRebalance =>
              updateMemberAndRebalance(group, member, protocols, responseCallback)

            case CompletingRebalance =>
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                responseCallback(JoinGroupResult(
                  members = if (group.isLeader(memberId)) {
                    group.currentMemberMetadata
                  } else {
                    List.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocolOrNull,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              } else {
                // member has changed metadata, so force a rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }

            case Stable =>
              val member = group.get(memberId)
              if (group.isLeader(memberId) || !member.matches(protocols)) {
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                responseCallback(JoinGroupResult(
                  members = List.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocolOrNull,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              }

            case Empty | Dead =>
              // Group reaches unexpected state. Let the joining member reset their generation and rejoin.
              warn(s"Attempt to add rejoining member $memberId of group ${group.groupId} in " +
                s"unexpected group state ${group.currentState}")
              responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          }
        }
      }
    }
  }

  private def onGroupUnloaded(group: GroupMetadata): Unit = {
    group.inLock {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      group.transitionTo(Dead)

      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeJoinCallback(member, joinError(member.memberId, Errors.NOT_COORDINATOR))
          }

          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | CompletingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeSyncCallback(member, SyncGroupResult(Array.empty, Errors.NOT_COORDINATOR))
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata): Unit = {
    group.inLock {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))
      if (groupIsOverCapacity(group)) {
        prepareRebalance(group, s"Freshly-loaded group is over capacity ($groupConfig.groupMaxSize). Rebalacing in order to give a chance for consumers to commit offsets")
      }

      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int): Unit = {
    groupManager.scheduleLoadGroupAndOffsets(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int): Unit = {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]): Unit = {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors): Unit = {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(_.assignment = Array.empty)
    propagateAssignment(group, error)
  }

  private def propagateAssignment(group: GroupMetadata, error: Errors): Unit = {
    for (member <- group.allMemberMetadata) {
      if (group.maybeInvokeSyncCallback(member, SyncGroupResult(member.assignment, error))) {
        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata): Unit = {
    completeAndScheduleNextExpiration(group, member, member.sessionTimeoutMs)
  }

  /**
   * 完成之前成员剩余的任务，并重新计划进行下一次心跳的最晚时间
   * @param group     消费组metadata
   * @param member    成员metadata
   * @param timeoutMs 超时时间
   */
  private def completeAndScheduleNextExpiration(group: GroupMetadata, member: MemberMetadata, timeoutMs: Long): Unit = {
    // 设置最近一次心跳请求的时间戳
    member.latestHeartbeat = time.milliseconds()
    // 构建memberKey
    val memberKey = MemberKey(member.groupId, member.memberId)
    // 完成监听的任务
    heartbeatPurgatory.checkAndComplete(memberKey)

    // 重新计划进行下一次心跳的deadline时间节点
    val deadline = member.latestHeartbeat + timeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member.memberId, isPending = false, deadline, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  /**
   * Add pending member expiration to heartbeat purgatory
   */
  private def addPendingMemberExpiration(group: GroupMetadata, pendingMemberId: String, timeoutMs: Long): Unit = {
    val pendingMemberKey = MemberKey(group.groupId, pendingMemberId)
    val deadline = time.milliseconds() + timeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, pendingMemberId, isPending = true, deadline, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(pendingMemberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata): Unit = {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  /**
   * 动态成员加入消费组，加入到消费组中，并触发再平衡操作
   * @param rebalanceTimeoutMs 再平衡等待时间
   * @param sessionTimeoutMs   会话等待时间
   * @param memberId           member.id
   * @param groupInstanceId    group.instance.id
   * @param clientId           client.id
   * @param clientHost         客户端地址
   * @param protocolType       客户端协议类型
   * @param protocols          客户端协议内容
   * @param group              消费组metadata
   * @param callback           执行完成后回调任务
   */
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    memberId: String,
                                    groupInstanceId: Option[String],
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback): Unit = {
    // 创建新成员的metadata
    val member = new MemberMetadata(memberId, group.groupId, groupInstanceId,
      clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)
    // 新成员的状态是新的
    member.isNew = true

    // 更新新成员添加的标识，证明加入消费组可以进一步延迟
    if (group.is(PreparingRebalance) && group.generationId == 0)
      group.newMemberAdded = true
    // 添加新成员和添加成的回调任务
    group.add(member, callback)

    // 当前会话超时时间，不会影响创建的新的成员，因为它们没有member.id，也不能发送心跳请求
    // 还有，我们不能检测到断开连接，因为socket在加入消费组请求是可变的
    // 如果客户端执行了断开连接（比如一个请求在一个长再平衡过程中出现了超时情况），它们可能简单的进行重试，这样在再平衡重导致很多的失效成员
    // 为了避免这种情况经常发生，我们将对新成员加入消费组的请求进行等待，如果新的成员仍然在，我们就执行重试
    completeAndScheduleNextExpiration(group, member, NewMemberJoinTimeoutMs)
    // 如果当前成员是静态成员
    if (member.isStaticMember)
    // 添加为静态成员
      group.addStaticMember(groupInstanceId, memberId)
    else
    // 非静态成员取消挂起状态
      group.removePendingMember(memberId)
    // 准备进行再平衡
    maybePrepareRebalance(group, s"Adding new member $memberId with group instanceid $groupInstanceId")
  }

  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback): Unit = {
    group.updateMember(member, protocols, callback)
    maybePrepareRebalance(group, s"Updating metadata for member ${member.memberId}")
  }

  /**
   * 准备进行再平衡
   * @param group
   * @param reason
   */
  private def maybePrepareRebalance(group: GroupMetadata, reason: String): Unit = {
    group.inLock {
      // 在消费组可以进行再平衡的情况下
      if (group.canRebalance)
      // 准备进行再平衡
        prepareRebalance(group, reason)
    }
  }

  /**
   * 准备进行再平衡
   * @param group  消费组metadata
   * @param reason 加入消费组的原因
   */
  private def prepareRebalance(group: GroupMetadata, reason: String): Unit = {
    // 如果有成员在等待同步，取消它们，让它们进行重新加入
    if (group.is(CompletingRebalance))
    // 重置并传播分配错误
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)

    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} in state ${group.currentState} with old generation " +
      s"${group.generationId} (${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)}) (reason: $reason)")
    // 创建消费组key
    val groupKey = GroupKey(group.groupId)

    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata, reason: String): Unit = {
    // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
    // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
    // will retry the JoinGroup request if is still active.
    group.maybeInvokeJoinCallback(member, joinError(NoMemberId, Errors.UNKNOWN_MEMBER_ID))

    group.remove(member.memberId)
    group.removeStaticMember(member.groupInstanceId)

    group.currentState match {
      case Dead | Empty =>
      case Stable | CompletingRebalance => maybePrepareRebalance(group, reason)
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  private def removePendingMemberAndUpdateGroup(group: GroupMetadata, memberId: String): Unit = {
    group.removePendingMember(memberId)

    if (group.is(PreparingRebalance)) {
      joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group.inLock {
      if (group.hasAllMembersJoined)
        forceComplete()
      else false
    }
  }

  def onExpireJoin(): Unit = {
    // TODO: add metrics for restabilize timeouts
  }

  def onCompleteJoin(group: GroupMetadata): Unit = {
    group.inLock {
      // remove dynamic members who haven't joined the group yet
      group.notYetRejoinedMembers.filterNot(_.isStaticMember) foreach { failedMember =>
        removeHeartbeatForLeavingMember(group, failedMember)
        group.remove(failedMember.memberId)
        group.removeStaticMember(failedMember.groupInstanceId)
        // TODO: cut the socket connection to the client
      }

      if (group.is(Dead)) {
        info(s"Group ${group.groupId} is dead, skipping rebalance stage")
      } else if (!group.maybeElectNewJoinedLeader() && group.allMembers.nonEmpty) {
        // If all members are not rejoining, we will postpone the completion
        // of rebalance preparing stage, and send out another delayed operation
        // until session timeout removes all the non-responsive members.
        error(s"Group ${group.groupId} could not complete rebalance because no members rejoined")
        joinPurgatory.tryCompleteElseWatch(
          new DelayedJoin(this, group, group.rebalanceTimeoutMs),
          Seq(GroupKey(group.groupId)))
      } else {
        group.initNextGeneration()
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          // trigger the awaiting join group response callback for all the members after rebalancing
          for (member <- group.allMemberMetadata) {
            val joinResult = JoinGroupResult(
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                List.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              subProtocol = group.protocolOrNull,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)

            group.maybeInvokeJoinCallback(member, joinResult)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            member.isNew = false
          }
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group.inLock {
      if (isPending) {
        // complete the heartbeat if the member has joined the group
        if (group.has(memberId)) {
          forceComplete()
        } else false
      }
      else {
        val member = group.get(memberId)
        if (member.shouldKeepAlive(heartbeatDeadline) || member.isLeaving) {
          forceComplete()
        } else false
      }
    }
  }

  def onExpireHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean, heartbeatDeadline: Long): Unit = {
    group.inLock {
      if (isPending) {
        info(s"Pending member $memberId in group ${group.groupId} has been removed after session timeout expiration.")
        removePendingMemberAndUpdateGroup(group, memberId)
      } else if (!group.has(memberId)) {
        debug(s"Member $memberId has already been removed from the group.")
      } else {
        val member = group.get(memberId)
        if (!member.shouldKeepAlive(heartbeatDeadline)) {
          info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
          removeMemberAndUpdateGroup(group, member, s"removing member ${member.memberId} on heartbeat expiration")
        }
      }
    }
  }

  def onCompleteHeartbeat(): Unit = {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def groupIsOverCapacity(group: GroupMetadata): Boolean = {
    group.size > groupConfig.groupMaxSize
  }

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMemberId = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)
  val NewMemberJoinTimeoutMs: Int = 5 * 60 * 1000

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            time: Time,
            metrics: Metrics): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, time, metrics)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time,
            metrics: Metrics): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupMaxSize = config.groupMaxSize,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkClient, time, metrics)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time, metrics)
  }

  def joinError(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = List.empty,
      memberId = memberId,
      generationId = GroupCoordinator.NoGeneration,
      subProtocol = GroupCoordinator.NoProtocol,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }

  private def memberLeaveError(memberIdentity: MemberIdentity,
                               error: Errors): LeaveMemberResponse = {
    LeaveMemberResponse(
      memberId = memberIdentity.memberId,
      groupInstanceId = Option(memberIdentity.groupInstanceId),
      error = error)
  }

  private def leaveError(topLevelError: Errors,
                         memberResponses: List[LeaveMemberResponse]): LeaveGroupResult = {
    LeaveGroupResult(
      topLevelError = topLevelError,
      memberResponses = memberResponses)
  }
}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int,
                       groupMaxSize: Int,
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: List[JoinGroupResponseMember],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           error: Errors)

case class SyncGroupResult(memberAssignment: Array[Byte],
                           error: Errors)

case class LeaveMemberResponse(memberId: String,
                               groupInstanceId: Option[String],
                               error: Errors)

case class LeaveGroupResult(topLevelError: Errors,
                            memberResponses: List[LeaveMemberResponse])
