/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import kafka.server.{DelayedOperation, DelayedOperationPurgatory, GroupKey}

import scala.math.{max, min}

/**
 * 延迟在平衡操作
 *
 * 当前消费组准备进行再平衡时，延迟操作会被添加到集合中
 * 每当收到一个加入消费组的请求是，检查是否所有一直的消费组member已经请求了重新加入消费组的请求
 * 如果已经进行了重新请求，完成这个操作并执行再平衡
 *
 * 当操作失效时，任何没有请求重新加入消费组的已知member将会标记为失败，并完成当前操作来进行剩余member的再平衡操作
 */
private[group] class DelayedJoin(coordinator: GroupCoordinator,
                                 group: GroupMetadata,
                                 rebalanceTimeout: Long) extends DelayedOperation(rebalanceTimeout, Some(group.lock)) {

  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)

  override def onExpiration(): Unit = {
    // try to complete delayed actions introduced by coordinator.onCompleteJoin
    tryToCompleteDelayedAction()
  }

  override def onComplete(): Unit = coordinator.onCompleteJoin(group)

  // TODO: remove this ugly chain after we move the action queue to handler thread
  private def tryToCompleteDelayedAction(): Unit = coordinator.groupManager.replicaManager.tryCompleteActions()
}

/**
  * Delayed rebalance operation that is added to the purgatory when a group is transitioning from
  * Empty to PreparingRebalance
  *
  * When onComplete is triggered we check if any new members have been added and if there is still time remaining
  * before the rebalance timeout. If both are true we then schedule a further delay. Otherwise we complete the
  * rebalance.
  */
private[group] class InitialDelayedJoin(coordinator: GroupCoordinator,
                                        purgatory: DelayedOperationPurgatory[DelayedJoin],
                                        group: GroupMetadata,
                                        configuredRebalanceDelay: Int,
                                        delayMs: Int,
                                        remainingMs: Int) extends DelayedJoin(coordinator, group, delayMs) {

  override def tryComplete(): Boolean = false

  override def onComplete(): Unit = {
    group.inLock {
      if (group.newMemberAdded && remainingMs != 0) {
        group.newMemberAdded = false
        val delay = min(configuredRebalanceDelay, remainingMs)
        val remaining = max(remainingMs - delayMs, 0)
        purgatory.tryCompleteElseWatch(new InitialDelayedJoin(coordinator,
          purgatory,
          group,
          configuredRebalanceDelay,
          delay,
          remaining
        ), Seq(GroupKey(group.groupId)))
      } else
        super.onComplete()
    }
  }

}
