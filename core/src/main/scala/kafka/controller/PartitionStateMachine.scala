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
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.controller.Election._
import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{Map, Seq, mutable}

/**
 * partition状态机
 * @param controllerContext
 */
abstract class PartitionStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * 在主控制器选举成功之后调用
   */
  def startup(): Unit = {
    info("Initializing partition state")
    initializePartitionState()
    info("Triggering online partition state changes")
    triggerOnlinePartitionStateChange()
    debug(s"Started partition state machine with initial state -> ${controllerContext.partitionStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped partition state machine")
  }

  /**
   * 调用基于所有partition的OnlinePartition状态变化的API，基于的partition是NewPartition状态或OfflinePartition状态
   * 在一次成功的控制器选举和broker发生变化时调用
   */
  def triggerOnlinePartitionStateChange(): Unit = {
    // 获取OfflinePartition、NewPartition两种状态的分区
    val partitions = controllerContext.partitionsInStates(Set(OfflinePartition, NewPartition))
    // 触发Online状态变化
    triggerOnlineStateChangeForPartitions(partitions)
  }

  def triggerOnlinePartitionStateChange(topic: String): Unit = {
    val partitions = controllerContext.partitionsInStates(topic, Set(OfflinePartition, NewPartition))
    triggerOnlineStateChangeForPartitions(partitions)
  }

  /**
   * 触发Online状态变化
   * @param partitions OfflinePartition、NewPartition状态的partition
   */
  private def triggerOnlineStateChangeForPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    // 尝试将所有处于NewPartition或OfflinePartition的partition状态转换到OnlinePartition状态，除了需要进行删除的topic的partition
    val partitionsToTrigger = partitions.filter { partition =>
      !controllerContext.isTopicQueuedUpForDeletion(partition.topic)
    }.toSeq
    // 处理状态变化事件
    handleStateChanges(partitionsToTrigger, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  /**
   * 调用并启动partition状态机，并设置所有ZK中已存在的partition的初始状态
   */
  private def initializePartitionState(): Unit = {
    // 获取控制器从ZK中的读取的partition信息
    for (topicPartition <- controllerContext.allPartitions) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      // 判断leader
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // leader is alive
            controllerContext.putPartitionState(topicPartition, OnlinePartition)
          else
            controllerContext.putPartitionState(topicPartition, OfflinePartition)
        case None =>
          controllerContext.putPartitionState(topicPartition, NewPartition)
      }
    }
  }

  def handleStateChanges(
                          partitions: Seq[TopicPartition],
                          targetState: PartitionState
                        ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    handleStateChanges(partitions, targetState, None)
  }

  def handleStateChanges(
                          partitions: Seq[TopicPartition],
                          targetState: PartitionState,
                          leaderElectionStrategy: Option[PartitionLeaderElectionStrategy]
                        ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]]

}

/**
 * ZkPartitionStateMachine代表partition的状态机
 * 它声明了partition可以处于的状态，并且将partition转换到另一个合法的状态
 * partition状态如下：
 * 1. NonExistentPartition: 此状态证明partition没有创建或者创建后删除，如果需要合法的前置状态，是OfflinePartition
 * 2. NewPartition: 经过创建，partition处于NewPartition状态，在这种状态下，partition需要分配副本节点，但是此时还没有leader/isr，合法的前置状态是NonExistentPartition
 * 3. OnlinePartition: 当partition选举出leader，那么partition就会处于OnlinePartition状态，合法的前置状态是NewPartition/OfflinePartition
 * 4. OfflinePartition: 如果在leader选举成功后，partition的leader节点挂掉，partition会迁转换到OfflinePartition状态，前置合法状态是NewPartition/OnlinePartition
 */
class ZkPartitionStateMachine(config: KafkaConfig,
                              stateChangeLogger: StateChangeLogger,
                              controllerContext: ControllerContext,
                              zkClient: KafkaZkClient,
                              controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends PartitionStateMachine(controllerContext) {

  private val controllerId = config.brokerId
  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * 将给定的partition从当前状态转换为目标状态，
   * 如果需要leader选举，使用给定的partition的leader选举策略
   * @param partitions                         需要进行状态变更的partition
   * @param targetState                        目标转换状态
   * @param partitionLeaderElectionStrategyOpt 需要leader选举的情况下，leader的选举策略
   * @return 当目标状态为OnlinePartitions时，状态转换失败或者成功选举的字典表
   *         key: topic-partition
   *         value: {
   *         "失败": exception,
   *         "成功": 新leader节点和ISR
   *         }
   */
  override def handleStateChanges(
                                   partitions: Seq[TopicPartition],
                                   targetState: PartitionState,
                                   partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
                                 ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    if (partitions.nonEmpty) {
      try {
        controllerBrokerRequestBatch.newBatch()
        // 执行partition状态变更
        val result = doHandleStateChanges(
          partitions,
          targetState,
          partitionLeaderElectionStrategyOpt
        )
        // 控制器发起，广播LeaderAndIsrRequest、UpdateMetadataRequest、StopRelica事件
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        result
      } catch {
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some partitions to $targetState state", e)
          throw e
        case e: Throwable =>
          error(s"Error while moving some partitions to $targetState state", e)
          partitions.iterator.map(_ -> Left(e)).toMap
      }
    } else {
      Map.empty
    }
  }

  private def partitionState(partition: TopicPartition): PartitionState = {
    controllerContext.partitionState(partition)
  }

  /**
   * 这个API实现了partition的状态机，它确保了每个状态从合法的先前状态到目标状态的转换，合法的状态转换如下：
   * 1. NonExistentPartition -> NewPartition: 将从ZK获取的分配的副本节点信息加载到控制器缓存中
   * 2. NewPartition -> OnlinePartition:
   * ① 分配第一个处于存活状态的副本节点为leader节点，其他存活的副本节点即为ISR，将leader节点和ISR写入到ZK中
   * ② 向每个存活的副本节点发送LeaderAndIsrRequest，向每个存活的broker发送UpdateMetadataRequest
   * 3. OnlinePartition,OfflinePartition -> OnlinePartition:
   * ① 选举partition新的leader节点和ISR，以及需要接收LeaderAndIsrRequest的副本节点集合，并将新的选举信息写入到ZK中
   * ② 对于此partition，向每个接收的副本节点发送LeaderAndIsrRequest，向每个broker发送UpdateMetadataRequest
   * 4. NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition: 只将partition的状态变更为OfflinePartition
   * 5. OfflinePartition -> NonExistentPartition: 只将partition的状态的变更为NonExistentPartition
   * @param partitions  需要进行状态转换的partition集合
   * @param targetState 转换的目标状态
   * @return 目标状态为OnlinePartition时，返回的是选举成功或者失败的字典表
   *         key: topic-partition
   *         value: 抛出的异常或者新的leader节点、ISR集合
   */
  private def doHandleStateChanges(
                                    partitions: Seq[TopicPartition],
                                    targetState: PartitionState,
                                    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
                                  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    // 创建partition状态变更日志
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)

    partitions.foreach(partition => controllerContext.putPartitionStateIfNotExists(partition, NonExistentPartition))
    val (validPartitions, invalidPartitions) = controllerContext.checkValidPartitionStateChange(partitions, targetState)
    // 记录非法的状态的转换，这种类型不进行状态转换
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState))
    // 目标状态过滤
    targetState match {
      case NewPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          // 直接将状态变更为NewPartition
          controllerContext.putPartitionState(partition, NewPartition)
        }
        Map.empty
      case OnlinePartition =>
        // 没有进行初始化的partition
        // 条件：partition的当前状态是NewPartition
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)
        // 需要进行leader选举的partition
        // 条件：partition的当前状态是OfflinePartition或者OnlinePartition
        val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)
        // 处理未进行初始化的partition
        if (uninitializedPartitions.nonEmpty) {
          // 进行初始化，也就是leader和ISR选举
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.trace(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).leaderAndIsr}")
            // 将ISR集合中的partition状态转换为OnlinePartition
            controllerContext.putPartitionState(partition, OnlinePartition)
          }
        }
        // 如果有需要进行选举leader节点的partition
        if (partitionsToElectLeader.nonEmpty) {
          // 选举leader节点
          val electionResults = electLeaderForPartitions(
            partitionsToElectLeader,
            partitionLeaderElectionStrategyOpt.getOrElse(
              throw new IllegalArgumentException("Election strategy is a required field when the target state is OnlinePartition")
            )
          )

          electionResults.foreach {
            case (partition, Right(leaderAndIsr)) =>
              stateChangeLog.trace(
                s"Changed partition $partition from ${partitionState(partition)} to $targetState with state $leaderAndIsr"
              )
              controllerContext.putPartitionState(partition, OnlinePartition)
            case (_, Left(_)) => // Ignore; no need to update partition state on election error
          }

          electionResults
        } else {
          Map.empty
        }
      case OfflinePartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          // 直接将状态转换为OfflinePartition
          controllerContext.putPartitionState(partition, OfflinePartition)
        }
        Map.empty
      case NonExistentPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          // 直接将状态转换为NonExistentPartition
          controllerContext.putPartitionState(partition, NonExistentPartition)
        }
        Map.empty
    }
  }

  /**
   * 在Zookeeper中实现leader和ISR
   * @param partitions 尝试初始化的partition
   * @return 成功初始化的partition
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    // 成功初始化的partition集合
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    // 获取每个partition所在的副本节点
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
    // 获取每个partition存活的副本节点
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
      val liveReplicasForPartition = replicas.filter(
        // 副本节点处于在线状态
        replica => controllerContext.isReplicaOnline(replica, partition))
      partition -> liveReplicasForPartition
    }
    // 对partition是否拥有存活的副本节点进行分区
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }
    // 对于没有存活副本节点的partition，记录失败的状态转换，不进行状态转换
    partitionsWithoutLiveReplicas.foreach { case (partition, replicas) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${replicas.mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }
    // 对于存活副本节点的partition
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      // 创建LeaderAndIsr
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
      // 创建新的LeaderAndIsrControllerEpoch
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap
    // 创建状态变更响应
    val createResponses = try {
      // 在ZK上创建
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs, controllerContext.epochZkVersion)
    } catch {
      case e: ControllerMovedException =>
        error("Controller moved to another broker when trying to create the topic partition state znode", e)
        throw e
      case e: Exception =>
        partitionsWithLiveReplicas.foreach { case (partition, _) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    // 返回成功实例化的partition
    successfulInitializations
  }

  /**
   * 为多个partition选举leader节点
   * 重复进行，直到没有未选举出的leader节点的partition
   * @param partitions                      需要进行选举leader节点的partition集合
   * @param partitionLeaderElectionStrategy 需要使用的选举策略
   * @return 返回的是选举成功或者失败的字典表
   *         key: topic-partition
   *         value: 抛出的异常或者新的leader节点、ISR集合
   */
  private def electLeaderForPartitions(
                                        partitions: Seq[TopicPartition],
                                        partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
                                      ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    var remaining = partitions
    val finishedElections = mutable.Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]]
    // 循环尝试选举
    while (remaining.nonEmpty) {
      // 进行一次选举
      val (finished, updatesToRetry) = doElectLeaderForPartitions(remaining, partitionLeaderElectionStrategy)
      // 置换需要进行重试选举的集合
      remaining = updatesToRetry
      // 完成选举leader节点的partition
      finished.foreach {
        case (partition, Left(e)) =>
          // 记录失败的状态变化的partition及状态
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
        case (_, Right(_)) =>
      }
      // 添加到返回结果中
      finishedElections ++= finished
    }

    finishedElections.toMap
  }

  /**
   * 为partition集合选举leader节点
   * 选举partition的leader节点并在ZK中更新partition的状态
   * @param partitions                      需要进行选举的partition集合
   * @param partitionLeaderElectionStrategy 选举策略
   * @return 两个值的tuple:
   *         1. partition和期望的leader和ISR成功获取了一个leader选举，失败的选举异常不会进行重试
   *         2. 由于ZK版本冲突导致的选举异常，需要进行重试，版本冲突可以发生在leader partition在控制器尝试更新partition状态时自己先更新了partition状态
   */
  private def doElectLeaderForPartitions(
                                          partitions: Seq[TopicPartition],
                                          partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
                                        ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    // 获取给定partition的状态
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }
    val failedElections = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]
    val validLeaderAndIsrs = mutable.Buffer.empty[(TopicPartition, LeaderAndIsr)]
    // 遍历每一个partition
    getDataResponses.foreach { getDataResponse =>
      // 获取topic-partition及状态
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      val currState = partitionState(partition)
      // 如果响应时成功，证明包含了
      if (getDataResponse.resultCode == Code.OK) {
        // 解析响应中的数据和状态
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          // 首先进行控制器的版本校验
          case Some(leaderIsrAndControllerEpoch) =>
            // 如果当前leaderIsrAndControllerEpoch版本比当前控制器上下文的版本要大，证明选举失败，需要重新进行选举
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val failMsg = s"Aborted leader election for partition $partition since the LeaderAndIsr path was " +
                s"already written by another controller. This probably means that the current controller $controllerId went through " +
                s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
              failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
            } else {
              // 获取新的leader和ISR集合
              validLeaderAndIsrs += partition -> leaderIsrAndControllerEpoch.leaderAndIsr
            }
          // 如果没有leaderIsrAndControllerEpoch信息，证明还处于选举状态中，稍后重新获取最新的选举信息
          case None =>
            val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
            failedElections.put(partition, Left(exception))
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        // 如果响应码是没有节点，稍后进行重试
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, Left(exception))
      } else {
        // 其他未知情况，也进行重试
        failedElections.put(partition, Left(getDataResponse.resultException.get))
      }
    }
    // 如果没有合法的leader和ISR集合，则全部partition都需要进行重试，等待进行下一次重试
    if (validLeaderAndIsrs.isEmpty) {
      return (failedElections.toMap, Seq.empty)
    }
    // 根据选举策略，对响应响应分区进行leader节点划分
    // 所有的结果都会根据返回结果中新的LeaderAndIsr是否为空进行分区，分为有leader的partition集合和没有leader的partition集合
    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      // OfflinePartition的leader选举，无论是否是clean的
      case OfflinePartitionLeaderElectionStrategy(allowUnclean) =>
        val partitionsWithUncleanLeaderElectionState = collectUncleanLeaderElectionState(
          validLeaderAndIsrs,
          allowUnclean
        )
        // 从NewPartition和OfflinePartition状态的partition中选举leader节点
        leaderForOffline(controllerContext, partitionsWithUncleanLeaderElectionState).partition(_.leaderAndIsr.isEmpty)
      case ReassignPartitionLeaderElectionStrategy =>
        // 重分配的leader选举策略
        leaderForReassign(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        // 首选副本节点leader选举策略
        leaderForPreferredReplica(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        // 正在关闭broker是触发的leader选举
        leaderForControlledShutdown(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
    }
    // 遍历所有没有选举出leader的partition集合
    partitionsWithoutLeaders.foreach { electionResult =>
      val partition = electionResult.topicPartition
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      // 记录失败选举信息
      failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
    }
    // topic-partition->liveReplicas字典表
    val recipientsPerPartition = partitionsWithLeaders.map(result => result.topicPartition -> result.liveReplicas).toMap
    // topic-partition->leaderAndIsr字典表
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map(result => result.topicPartition -> result.leaderAndIsr.get).toMap
    // 更新ZK中存储的leaderAndIsr信息
    val UpdateLeaderAndIsrResult(finishedUpdates, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)
    // 遍历更新成功的响应
    finishedUpdates.foreach { case (partition, result) =>
      result.right.foreach { leaderAndIsr =>
        val replicas = controllerContext.partitionReplicaAssignment(partition)
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
        // 更新完ZK后，才更新控制器中缓存的数据
        controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
        // 发送UpdateMetadataRequest给对应的broker集合
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipientsPerPartition(partition), partition,
          leaderIsrAndControllerEpoch, replicas, isNew = false)
      }
    }
    // 返回成功更新和选举失败的集合+需要进行重试的集合
    (finishedUpdates ++ failedElections, updatesToRetry)
  }

  /* For the provided set of topic partition and partition sync state it attempts to determine if unclean
   * leader election should be performed. Unclean election should be performed if there are no live
   * replica which are in sync and unclean leader election is allowed (allowUnclean parameter is true or
   * the topic has been configured to allow unclean election).
   *
   * @param leaderIsrAndControllerEpochs set of partition to determine if unclean leader election should be
   *                                     allowed
   * @param allowUnclean whether to allow unclean election without having to read the topic configuration
   * @return a sequence of three element tuple:
   *         1. topic partition
   *         2. leader, isr and controller epoc. Some means election should be performed
   *         3. allow unclean
   */
  private def collectUncleanLeaderElectionState(
                                                 leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
                                                 allowUnclean: Boolean
                                               ): Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderAndIsrs.partition {
      case (partition, leaderAndIsr) =>
        val liveInSyncReplicas = leaderAndIsr.isr.filter(controllerContext.isReplicaOnline(_, partition))
        liveInSyncReplicas.isEmpty
    }

    val electionForPartitionWithoutLiveReplicas = if (allowUnclean) {
      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), true)
      }
    } else {
      val (logConfigs, failed) = zkClient.getLogConfigs(
        partitionsWithNoLiveInSyncReplicas.iterator.map { case (partition, _) => partition.topic }.toSet,
        config.originals()
      )

      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        if (failed.contains(partition.topic)) {
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
          (partition, None, false)
        } else {
          (
            partition,
            Option(leaderAndIsr),
            logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue()
          )
        }
      }
    }

    electionForPartitionWithoutLiveReplicas ++
      partitionsWithLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), false)
      }
  }

  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  /**
   * 记录失败的partition状态转换
   * @param partition   状态转换失败的partition
   * @param currState   当前状态
   * @param targetState 目标状态
   * @param t           转换失败抛出的异常
   */
  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

/**
 * partition leader节点选举算法
 * 选举策略：在正存活的副本节点和ISR副本节点中选举共同的副本节点
 */
object PartitionLeaderElectionAlgorithms {

  /**
   * 从NewPartition和OfflinePartition状态的partition中选举leader节点
   * @param assignment                   已分配的partition分区
   * @param isr                          ZK中记录的ISR集合
   * @param liveReplicas                 目前已存活的副本节点
   * @param uncleanLeaderElectionEnabled unclean选举策论是否开启
   * @param controllerContext            集群当前状态的上下文
   * @return 选举出的leader节点，可能返回None
   */
  def offlinePartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], uncleanLeaderElectionEnabled: Boolean, controllerContext: ControllerContext): Option[Int] = {
    // 选举策略：从已分配的partition中获取存在于存活副本节点和ISR中的节点
    // 如果没有根据直接策略选举出，则在开启unclean策略的情况下，进行第二次选举
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      // 开启unclean选举策略的情况下
      if (uncleanLeaderElectionEnabled) {
        // 找到assignment和liveReplicas的第一个共同节点
        val leaderOpt = assignment.find(liveReplicas.contains)
        if (leaderOpt.isDefined)
        // 标记unclean选举的比例
          controllerContext.stats.uncleanLeaderElectionRate.mark()
        // 返回unclean状态下的leader节点
        leaderOpt
      } else {
        None
      }
    }
  }

  /**
   * 重分配partition的leader选举
   * 选举策略：从重分配的partition节点集合中选举
   * @param reassignment 重分配的新副本节点集合
   * @param isr          ZK中记录的ISR集合
   * @param liveReplicas 目前存活状态的副本节点
   * @return 选举出的leader节点，可能返回None
   */
  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }

  /**
   * 首选副本节点partition的leader选举
   * 选举选举策略的第一个节点
   * @param assignment   已分配的partition节点集合
   * @param isr          ZK中记录的ISR集合
   * @param liveReplicas 目前存活状态的副本节点
   * @return 选举出的leader节点，可能返回None
   */
  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }

  /**
   * 关闭的broker partition的leader选举
   * 公共选举策略额外刨除处于正在关闭的broker节点
   * @param assignment          已分配的partition节点集合
   * @param isr                 ZK中记录的ISR集合
   * @param liveReplicas        目前存活状态的副本节点
   * @param shuttingDownBrokers 正在关闭的broker节点集合
   * @return
   */
  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}

sealed trait PartitionLeaderElectionStrategy

final case class OfflinePartitionLeaderElectionStrategy(allowUnclean: Boolean) extends PartitionLeaderElectionStrategy

final case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

final case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

final case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

sealed trait PartitionState {
  def state: Byte

  def validPreviousStates: Set[PartitionState]
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
