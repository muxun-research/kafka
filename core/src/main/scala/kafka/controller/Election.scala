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
package kafka.controller

import kafka.api.LeaderAndIsr
import org.apache.kafka.common.TopicPartition

import scala.collection.Seq

case class ElectionResult(topicPartition: TopicPartition, leaderAndIsr: Option[LeaderAndIsr], liveReplicas: Seq[Int])

object Election {

  /**
   * 从NewPartition和OfflinePartition状态的partition中选举leader节点
   * @param controllerContext                        集群当前状态的上下文
   * @param partitionsWithUncleanLeaderElectionState 代表需要选的partition的tuple序列，其leader/ISR状态以及是否启用clean的leader选举策略
   * @return 选举结果
   */
  def leaderForOffline(
                        controllerContext: ControllerContext,
                        partitionsWithUncleanLeaderElectionState: Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)]
                      ): Seq[ElectionResult] = {
    partitionsWithUncleanLeaderElectionState.map {
      case (partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled) =>
        leaderForOffline(partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled, controllerContext)
    }
  }

  /**
   * 从NewPartition和OfflinePartition状态的partition中选举leader节点
   * @param partition                    进行选举的partition
   * @param leaderAndIsrOpt              partition所在的leader和ISR集合
   * @param uncleanLeaderElectionEnabled 是否开启unclean选举策略
   * @param controllerContext            集群当前状态的上下文
   * @return
   */
  private def leaderForOffline(partition: TopicPartition,
                               leaderAndIsrOpt: Option[LeaderAndIsr],
                               uncleanLeaderElectionEnabled: Boolean,
                               controllerContext: ControllerContext): ElectionResult = {
    // 已分配的partition
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    // 从已分配的partition中获取所有处于在线的副本节点
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        // 获取ZK中记录的ISR集合
        val isr = leaderAndIsr.isr
        // 根据选举策略获取leader节点
        // 会根据unclean的策略的开启额外进行一次选举
        val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(
          assignment, isr, liveReplicas.toSet, uncleanLeaderElectionEnabled, controllerContext)
        val newLeaderAndIsrOpt = leaderOpt.map { leader =>
          // 对新leader节点是否处于ISR中进行不同的包装
          val newIsr = if (isr.contains(leader)) isr.filter(replica => controllerContext.isReplicaOnline(replica, partition))
          else List(leader)
          leaderAndIsr.newLeaderAndIsr(leader, newIsr)
        }
        ElectionResult(partition, newLeaderAndIsrOpt, liveReplicas)

      case None =>
        ElectionResult(partition, None, liveReplicas)
    }
  }

  /**
   * 获取正处于重分配阶段的partition集合的leader节点
   * @param controllerContext 集群当前状态的上下文
   * @param leaderAndIsrs     partition所在的leader和ISR集合
   * @return partition首选leader节点和新ISR信息
   */
  def leaderForReassign(controllerContext: ControllerContext,
                        leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForReassign(partition, leaderAndIsr, controllerContext)
    }
  }

  /**
   * 获取正处于重分配阶段的partition集合的leader节点
   * @param partition         进行选举的partition
   * @param leaderAndIsr      partition所在的leader和ISR集合
   * @param controllerContext 集群当前状态的上下文
   * @return partition首选leader节点和新ISR信息
   */
  private def leaderForReassign(partition: TopicPartition,
                                leaderAndIsr: LeaderAndIsr,
                                controllerContext: ControllerContext): ElectionResult = {
    // 获取正在重分配的partition中新的副本节点
    val reassignment = controllerContext.partitionsBeingReassigned(partition).newReplicas
    // 获取所有存活状态的副本节点
    val liveReplicas = reassignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    // 获取ZK中记录的ISR集合信息
    val isr = leaderAndIsr.isr
    // 在重分配的副本中进行选举
    // 从重分配的新副本节点中获取既是存活副本节点又是ISR中的节点作为leader节点
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(reassignment, isr, liveReplicas.toSet)
    // 包装为新选举结果
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, reassignment)
  }

  /**
   * 选举首选的leader节点
   * @param controllerContext 集群当前状态的上下文
   * @param leaderAndIsrs     需要进行选举的partition和它们各自的leader和ISR状态
   * @return 选举结果
   */
  def leaderForPreferredReplica(controllerContext: ControllerContext,
                                leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    // 遍历每一个leader和ISR
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForPreferredReplica(partition, leaderAndIsr, controllerContext)
    }
  }

  /**
   * 获取首选节点作为leader节点
   * @param partition         进行选举的partition
   * @param leaderAndIsr      partition所在的leader和ISR集合
   * @param controllerContext 集群当前状态的上下文
   * @return partition首选leader节点和新ISR信息
   */
  private def leaderForPreferredReplica(partition: TopicPartition,
                                        leaderAndIsr: LeaderAndIsr,
                                        controllerContext: ControllerContext): ElectionResult = {
    // 获取partition的副本分配情况
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    // 获取当前存活的副本节点
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    // 获取ZK中存储的ISR集合
    val isr = leaderAndIsr.isr
    // 进行首选节点的选举
    // 在存活的liveReplicas中和ISR中都存在的节点即视为leader节点
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
    // 包装为新选举结果
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, assignment)
  }

  /**
   * 为当前leader节点正在关闭的partition进行leader选举
   * @param controllerContext 集群当前状态的上下文
   * @param leaderAndIsrs     需要进行选举的partition和它们各自的leader和ISR状态
   * @return 选举结果
   */
  def leaderForControlledShutdown(controllerContext: ControllerContext,
                                  leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    // 获取当前集群中正在关闭的broker节点集合
    val shuttingDownBrokerIds = controllerContext.shuttingDownBrokerIds.toSet
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      // 进行选举
      leaderForControlledShutdown(partition, leaderAndIsr, shuttingDownBrokerIds, controllerContext)
    }
  }

  /**
   * 为当前leader节点正在关闭的partition进行leader选举
   * @param partition             进行选举的partition
   * @param leaderAndIsr          partition所在的leader和ISR集合
   * @param shuttingDownBrokerIds 当前集群中正在关闭的broker节点集合
   * @param controllerContext     集群当前状态的上下文
   * @return
   */
  private def leaderForControlledShutdown(partition: TopicPartition,
                                          leaderAndIsr: LeaderAndIsr,
                                          shuttingDownBrokerIds: Set[Int],
                                          controllerContext: ControllerContext): ElectionResult = {
    // 获取partition已分配的副本节点
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    // 获取当前正处于存活状态的副本节点，包含处于正在关闭的副本节点
    val liveOrShuttingDownReplicas = assignment.filter(replica =>
      controllerContext.isReplicaOnline(replica, partition, includeShuttingDownBrokers = true))
    // 获取ZK中记录的ISR集合
    val isr = leaderAndIsr.isr
    // 进行选举
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment, isr,
      liveOrShuttingDownReplicas.toSet, shuttingDownBrokerIds)
    // 新的ISR去除正在关闭的副本节点
    val newIsr = isr.filter(replica => !shuttingDownBrokerIds.contains(replica))
    // 构建leader选举结果
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeaderAndIsr(leader, newIsr))
    ElectionResult(partition, newLeaderAndIsrOpt, liveOrShuttingDownReplicas)
  }
}
