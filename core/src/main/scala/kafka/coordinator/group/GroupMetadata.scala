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
package kafka.coordinator.group

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import kafka.common.OffsetAndMetadata
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time

import scala.collection.{Seq, immutable, mutable}

private[group] sealed trait GroupState

/**
 * 准备再平衡状态
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 * respond to sync group with REBALANCE_IN_PROGRESS
 * remove member on leave group request
 * park join group requests from new or existing members until all expected members have joined
 * allow offset commits from previous generation
 * allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 * all members have left the group => Empty
 * group is removed by partition emigration => Dead
 */
private[group] case object PreparingRebalance extends GroupState

/**
 * 等待完成再平衡状态
 * 消费组正在等待leader consumer分配状态
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 * respond to offset commits with REBALANCE_IN_PROGRESS
 * park sync group requests from followers until transition to Stable
 * allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 * join group from new member or existing member with updated metadata => PreparingRebalance
 * leave group from existing member => PreparingRebalance
 * member failure detected => PreparingRebalance
 * group is removed by partition emigration => Dead
 */
private[group] case object CompletingRebalance extends GroupState

/**
 * Group is stable
 * 稳定状态
 * action: respond to member heartbeats normally
 * respond to sync group from any member with current assignment
 * respond to join group from followers with matching metadata with current group metadata
 * allow offset commits from member of current generation
 * allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 * leave group from existing member => PreparingRebalance
 * leader join-group received => PreparingRebalance
 * follower join-group with new metadata => PreparingRebalance
 * group is removed by partition emigration => Dead
 */
private[group] case object Stable extends GroupState

/**
 * Group has no more members and its metadata is being removed
 * 失效状态
 * 消费组没有member，并且消费组metadata正在被移除
 * action: respond to join group with UNKNOWN_MEMBER_ID
 * respond to sync group with UNKNOWN_MEMBER_ID
 * respond to heartbeat with UNKNOWN_MEMBER_ID
 * respond to leave group with UNKNOWN_MEMBER_ID
 * respond to offset commit with UNKNOWN_MEMBER_ID
 * allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[group] case object Dead extends GroupState

/**
 * 空置状态
 * 消费组没有更多的member，但是等待所有offset都失效之后才消失，这个状态也代表了仅仅使用Kafka作为一个offset提交的来使用
 * action: respond normally to join group from new members
 * respond to sync group with UNKNOWN_MEMBER_ID
 * respond to heartbeat with UNKNOWN_MEMBER_ID
 * respond to leave group with UNKNOWN_MEMBER_ID
 * respond to offset commit with UNKNOWN_MEMBER_ID
 * allow offset fetch requests
 * transition: last offsets removed in periodic expiration task => Dead
 * join group from a new member => PreparingRebalance
 * group is removed by partition emigration => Dead
 * group is removed by expiration => Dead
 */
private[group] case object Empty extends GroupState


private object GroupMetadata {
  private val validPreviousStates: Map[GroupState, Set[GroupState]] =
    Map(Dead -> Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead),
      CompletingRebalance -> Set(PreparingRebalance),
      Stable -> Set(CompletingRebalance),
      PreparingRebalance -> Set(Stable, CompletingRebalance, Empty),
      Empty -> Set(PreparingRebalance))

  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocol: String,
                leaderId: String,
                currentStateTimestamp: Option[Long],
                members: Iterable[MemberMetadata],
                time: Time): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState, time)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocol = Option(protocol)
    group.leaderId = Option(leaderId)
    group.currentStateTimestamp = currentStateTimestamp
    members.foreach(member => {
      group.add(member, null)
      if (member.isStaticMember) {
        group.addStaticMember(member.groupInstanceId, member.memberId)
      }
    })
    group
  }

  private val MemberIdDelimiter = "-"
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String,
                         protocolType: String)

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String,
                        protocolType: String,
                        protocol: String,
                        members: List[MemberSummary])

/**
 * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
 * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
 * information of the commit record offset, compaction of the offsets topic it self may result in the wrong offset commit
 * being materialized.
 */
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset): Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

/**
 * Group contains the following metadata:
 *
 * Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 * State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 */
@nonthreadsafe
private[group] class GroupMetadata(val groupId: String, initialState: GroupState, time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit

  private[group] val lock = new ReentrantLock

  /**
   * 成员信息
   * key:
   */
  private val members = new mutable.HashMap[String, MemberMetadata]
  /**
   *
   */
  var currentStateTimestamp: Option[Long] = Some(time.milliseconds())
  var protocolType: Option[String] = None
  /**
   * generation编号
   */
  var generationId = 0
  /**
   * 消费组的状态，初始时为稳定状态
   */
  private var state: GroupState = initialState
  /**
   * leader node id
   * 每个消费组有且仅有一个leader consumer
   */
  private var leaderId: Option[String] = None
  /**
   * 协议名称
   * 每个消费组有且仅有一个protocol
   */
  private var protocol: Option[String] = None
  /**
   * Kafka 2.3的新特性，静态成员
   * key: group.instance.id
   * value: member.id
   */
  private val staticMembers = new mutable.HashMap[String, String]
  private val pendingMembers = new mutable.HashSet[String]
  private var numMembersAwaitingJoin = 0
  private val supportedProtocols = new mutable.HashMap[String, Integer]().withDefaultValue(0)
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  private var receivedConsumerOffsetCommits = false

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def is(groupState: GroupState) = state == groupState

  def not(groupState: GroupState) = state != groupState

  def has(memberId: String) = members.contains(memberId)

  def get(memberId: String) = members(memberId)

  def size = members.size

  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)

  def leaderOrNull: String = leaderId.orNull

  def protocolOrNull: String = protocol.orNull

  def currentStateTimestampOrDefault: Long = currentStateTimestamp.getOrElse(-1)

  /**
   * 添加成员到消费组中
   * @param member   需要添加的成员
   * @param callback 回调任务
   */
  def add(member: MemberMetadata, callback: JoinCallback = null): Unit = {
    if (members.isEmpty)
      this.protocolType = Some(member.protocolType)

    assert(groupId == member.groupId)
    assert(this.protocolType.orNull == member.protocolType)
    assert(supportsProtocols(member.protocolType, MemberMetadata.plainProtocolSet(member.supportedProtocols)))
    // 如果当前成员是第一个加入消费组的成员
    if (leaderId.isEmpty)
    // 设置当前成员为leader consumer
      leaderId = Some(member.memberId)
    // 添加当前成员
    members.put(member.memberId, member)
    // 添加需要支持的协议
    member.supportedProtocols.foreach { case (protocol, _) => supportedProtocols(protocol) += 1 }
    // 设置添加成功的回调任务
    member.awaitingJoinCallback = callback
    // 等待加入的成员数量+1
    if (member.isAwaitingJoin)
      numMembersAwaitingJoin += 1
  }

  def remove(memberId: String): Unit = {
    members.remove(memberId).foreach { member =>
      member.supportedProtocols.foreach { case (protocol, _) => supportedProtocols(protocol) -= 1 }
      if (member.isAwaitingJoin)
        numMembersAwaitingJoin -= 1
    }

    if (isLeader(memberId))
      leaderId = members.keys.headOption
  }

  /**
   * Check whether current leader is rejoined. If not, try to find another joined member to be
   * new leader. Return false if
   *   1. the group is currently empty (has no designated leader)
   *   2. no member rejoined
   */
  def maybeElectNewJoinedLeader(): Boolean = {
    leaderId.exists { currentLeaderId =>
      val currentLeader = get(currentLeaderId)
      if (!currentLeader.isAwaitingJoin) {
        members.find(_._2.isAwaitingJoin) match {
          case Some((anyJoinedMemberId, anyJoinedMember)) =>
            leaderId = Option(anyJoinedMemberId)
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, while new leader $anyJoinedMember was elected.")
            true

          case None =>
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, and the group couldn't proceed to next generation" +
              s"because no member joined.")
            false
        }
      } else {
        true
      }
    }
  }

  /**
   * 仅针对静态成员部分
   * 使用新member.id代替老的member.id
   * 保持其他的事情不变，返回更新的成员信息
   */
  def replaceGroupInstance(oldMemberId: String,
                           newMemberId: String,
                           groupInstanceId: Option[String]): MemberMetadata = {
    if (groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in replaceGroupInstance")
    }
    val oldMember = members.remove(oldMemberId)
      .getOrElse(throw new IllegalArgumentException(s"Cannot replace non-existing member id $oldMemberId"))

    // 马上屏蔽潜在的重复成员，如果其他成员在等待加入/同步的回调任务
    // 根据等待的不同状态，添加不同的任务
    maybeInvokeJoinCallback(oldMember, JoinGroupResult(
      members = List.empty,
      memberId = oldMemberId,
      generationId = GroupCoordinator.NoGeneration,
      subProtocol = GroupCoordinator.NoProtocol,
      leaderId = GroupCoordinator.NoLeader,
      error = Errors.FENCED_INSTANCE_ID))

    maybeInvokeSyncCallback(oldMember, SyncGroupResult(
      Array.empty, Errors.FENCED_INSTANCE_ID
    ))
    // 更新member.id
    oldMember.memberId = newMemberId
    members.put(newMemberId, oldMember)
    // 当前节点是否是leader consumer
    if (isLeader(oldMemberId))
      leaderId = Some(newMemberId)
    // 添加静态成员
    addStaticMember(groupInstanceId, newMemberId)
    // 返回更新的成员信息
    oldMember
  }

  def isPendingMember(memberId: String): Boolean = pendingMembers.contains(memberId) && !has(memberId)

  def addPendingMember(memberId: String) = pendingMembers.add(memberId)

  def removePendingMember(memberId: String) = pendingMembers.remove(memberId)

  def hasStaticMember(groupInstanceId: Option[String]) = groupInstanceId.isDefined && staticMembers.contains(groupInstanceId.get)

  /**
   * 添加静态成员对象
   * @param groupInstanceId group.instance.id 静态成员对象唯一标识
   * @param newMemberId     新生成的member.id
   * @return
   */
  def addStaticMember(groupInstanceId: Option[String], newMemberId: String) = {
    if (groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in addStaticMember")
    }
    staticMembers.put(groupInstanceId.get, newMemberId)
  }

  /**
   * @return true if a sync callback actually performs.
   */
  def maybeInvokeSyncCallback(member: MemberMetadata,
                              syncGroupResult: SyncGroupResult): Boolean = {
    if (member.isAwaitingSync) {
      member.awaitingSyncCallback(syncGroupResult)
      member.awaitingSyncCallback = null
      true
    } else {
      false
    }
  }

  def removeStaticMember(groupInstanceId: Option[String]) = {
    if (groupInstanceId.isDefined) {
      staticMembers.remove(groupInstanceId.get)
    }
  }

  def currentState = state

  /**
   * 还没发送"重新加入消费组"的member，已经在members中，但是还没有回调对象
   * @return 没发送"重新加入消费组"的member
   */
  def notYetRejoinedMembers = members.values.filter(!_.isAwaitingJoin).toList

  def hasAllMembersJoined = members.size == numMembersAwaitingJoin && pendingMembers.isEmpty

  def allMembers = members.keySet

  def allStaticMembers = staticMembers.keySet

  def numPending = pendingMembers.size

  def allMemberMetadata = members.values.toList

  /**
   * 消费组进行再平衡的超时时间
   * @return
   */
  def rebalanceTimeoutMs = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs)
  }

  /**
   * 生成消费组中的member.id
   * @param clientId        客户端ID
   * @param groupInstanceId group.instance.id
   * @return 生成的成员member.id
   */
  def generateMemberId(clientId: String,
                       groupInstanceId: Option[String]): String = {
    groupInstanceId match {
      case None =>
        // 没有指定group.instance.id的情况下，默认是"client.id-uuid"
        clientId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
      case Some(instanceId) =>
        // 在有指定group.instance.id的情况下，默认是"group.instance.id-uuid"
        instanceId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
    }
  }

  /**
   * 验证member.id是最新的静态成员，下面两个条件全部满足，则返回true：
   * 1. 指定的member.id是消费组已知的静态成员
   * 2. 消费组已存储的member.id没有匹配到指定的member.id
   */
  def isStaticMemberFenced(memberId: String,
                           groupInstanceId: Option[String]): Boolean = {
    if (hasStaticMember(groupInstanceId)
      && getStaticMemberId(groupInstanceId) != memberId) {
      error(s"given member.id $memberId is identified as a known static member ${groupInstanceId.get}," +
        s"but not matching the expected member.id ${getStaticMemberId(groupInstanceId)}")
      true
    } else
      false
  }

  def canRebalance = GroupMetadata.validPreviousStates(PreparingRebalance).contains(state)

  def transitionTo(groupState: GroupState): Unit = {
    assertValidTransition(groupState)
    state = groupState
    currentStateTimestamp = Some(time.milliseconds())
  }

  def selectProtocol: String = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    val votes: List[(String, Int)] = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .mapValues(_.size)
      .toList

    votes.maxBy(_._2)._1
  }

  private def candidateProtocols = {
    // get the set of protocols that are commonly supported by all members
    val numMembers = members.size
    supportedProtocols.filter(_._2 == numMembers).map(_._1).toSet
  }

  def supportsProtocols(memberProtocolType: String, memberProtocols: Set[String]) = {
    if (is(Empty))
      !memberProtocolType.isEmpty && memberProtocols.nonEmpty
    else
      protocolType.contains(memberProtocolType) && memberProtocols.exists(supportedProtocols(_) == members.size)
  }

  def getStaticMemberId(groupInstanceId: Option[String]) = {
    if (groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in getStaticMemberId")
    }
    staticMembers(groupInstanceId.get)
  }

  def maybeInvokeJoinCallback(member: MemberMetadata,
                              joinGroupResult: JoinGroupResult): Unit = {
    if (member.isAwaitingJoin) {
      member.awaitingJoinCallback(joinGroupResult)
      member.awaitingJoinCallback = null
      numMembersAwaitingJoin -= 1
    }
  }

  /**
   * 更新消费组中的成员信息
   * @param member
   * @param protocols
   * @param callback
   */
  def updateMember(member: MemberMetadata,
                   protocols: List[(String, Array[Byte])],
                   callback: JoinCallback) = {
    member.supportedProtocols.foreach { case (protocol, _) => supportedProtocols(protocol) -= 1 }
    protocols.foreach { case (protocol, _) => supportedProtocols(protocol) += 1 }
    member.supportedProtocols = protocols

    if (callback != null && !member.isAwaitingJoin) {
      numMembersAwaitingJoin += 1
    } else if (callback == null && member.isAwaitingJoin) {
      numMembersAwaitingJoin -= 1
    }
    member.awaitingJoinCallback = callback
  }

  def initNextGeneration() = {
    if (members.nonEmpty) {
      generationId += 1
      protocol = Some(selectProtocol)
      transitionTo(CompletingRebalance)
    } else {
      generationId += 1
      protocol = None
      transitionTo(Empty)
    }
    receivedConsumerOffsetCommits = false
    receivedTransactionalOffsetCommits = false
  }

  def currentMemberMetadata: List[JoinGroupResponseMember] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map { case (memberId, memberMetadata) => new JoinGroupResponseMember()
      .setMemberId(memberId)
      .setGroupInstanceId(memberMetadata.groupInstanceId.orNull)
      .setMetadata(memberMetadata.metadata(protocol.get))
    }.toList
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val protocol = protocolOrNull
      if (protocol == null)
        throw new IllegalStateException("Invalid null group protocol for stable group")

      val members = this.members.values.map { member => member.summary(protocol) }
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members.toList)
    } else {
      val members = this.members.values.map { member => member.summaryNoMetadata() }
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members.toList)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType.getOrElse(""))
  }

  def initializeOffsets(offsets: collection.Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTxnOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]): Unit = {
    this.offsets ++= offsets
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets
  }

  def onOffsetCommitAppend(topicPartition: TopicPartition, offsetWithCommitRecordMetadata: CommitRecordMetadataAndOffset): Unit = {
    if (pendingOffsetCommits.contains(topicPartition)) {
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +
          "in the log.")
      if (!offsets.contains(topicPartition) || offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata))
        offsets.put(topicPartition, offsetWithCommitRecordMetadata)
    }

    pendingOffsetCommits.get(topicPartition) match {
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition)
      case _ =>
      // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
      // its entries would be removed from the cache by the `removeOffsets` method.
    }
  }

  def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    receivedConsumerOffsetCommits = true
    pendingOffsetCommits ++= offsets
  }

  def prepareTxnOffsetCommit(producerId: Long, offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])

    offsets.foreach { case (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata))
    }
  }

  def hasReceivedConsistentOffsetCommits: Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
      // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
      // We may hit this case if the partition in question has emigrated.
    }
  }

  /* Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   */
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.foreach { case (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long) =
    pendingTransactionalOffsetCommits.contains(producerId)

  def removeAllOffsets(): immutable.Map[TopicPartition, OffsetAndMetadata] = removeOffsets(offsets.keySet.toSeq)

  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.foreach { case (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  def removeExpiredOffsets(currentTimestamp: Long, offsetRetentionMs: Long): Map[TopicPartition, OffsetAndMetadata] = {

    def getExpiredOffsets(baseTimestamp: CommitRecordMetadataAndOffset => Long): Map[TopicPartition, OffsetAndMetadata] = {
      offsets.filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          !pendingOffsetCommits.contains(topicPartition) && {
            commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp match {
              case None =>
                // current version with no per partition retention
                currentTimestamp - baseTimestamp(commitRecordMetadataAndOffset) >= offsetRetentionMs
              case Some(expireTimestamp) =>
                // older versions with explicit expire_timestamp field => old expiration semantics is used
                currentTimestamp >= expireTimestamp
            }
          }
      }.map {
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }.toMap
    }

    val expiredOffsets: Map[TopicPartition, OffsetAndMetadata] = protocolType match {
      case Some(_) if is(Empty) =>
        // no consumer exists in the group =>
        // - if current state timestamp exists and retention period has passed since group became Empty,
        //   expire all offsets with no pending offset commit;
        // - if there is no current state timestamp (old group metadata schema) and retention period has passed
        //   since the last commit timestamp, expire the offset
        getExpiredOffsets(commitRecordMetadataAndOffset =>
          currentStateTimestamp.getOrElse(commitRecordMetadataAndOffset.offsetAndMetadata.commitTimestamp))

      case None =>
        // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage only
        // expire offsets with no pending offset commit that retention period has passed since their last commit
        getExpiredOffsets(_.offsetAndMetadata.commitTimestamp)

      case _ =>
        Map()
    }

    if (expiredOffsets.nonEmpty)
      debug(s"Expired offsets from group '$groupId': ${expiredOffsets.keySet}")

    offsets --= expiredOffsets.keySet
    expiredOffsets
  }

  def allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[group] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets = offsets.size

  def hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  private def assertValidTransition(targetState: GroupState): Unit = {
    if (!GroupMetadata.validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
  }

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}

