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

package kafka.server


import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = s"[acksPending: $acksPending, error: ${responseStatus.error.code}, " +
    s"startOffset: ${responseStatus.baseOffset}, requiredOffset: $requiredOffset]"
}

/**
 * 创建延迟生产操作metadata
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {
  override def toString = s"[requiredAcks: $produceRequiredAcks, partitionStatus: $produceStatus]"
}

/**
 * 有副本管理器创建的延迟生产请求，并生产操作缓存中进行观察
 * 其实也是一个延迟操作
 */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                     lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  // 首先，根据error code更新acks待定变量
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    if (status.responseStatus.error == Errors.NONE) {
      // 超时错误状态将会在收到需要的acks后清除
      status.acksPending = true
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      status.acksPending = false
    }

    trace(s"Initial partition status for $topicPartition is $status")
  }

  /**
   * 延迟生产操作可以在每个partition在满足下列条件后后完成：
   * Case A: broker不再是leader节点，在响应中设置错误信息
   * Case B: broker仍然是leader节点:
   *   B.1 - 如果在检查是否需要required时抛出了本地错误，则ACK副本已经赶上此操作，在响应中设置错误
   *   B.2 - 其他情况，不在响应中设置错误
   */
  override def tryComplete(): Boolean = {
    // 检查每个partition是否还有待定的acks
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // 忽略那些已经完成的partition
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
          case HostedPartition.Online(partition) =>
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)

          case HostedPartition.Offline =>
            (false, Errors.KAFKA_STORAGE_ERROR)

          case HostedPartition.None =>
            // Case A
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
        // Case B.1 || B.2
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    // 在case A或者case B的情况下，每个partition是否已经满足条件
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
    // 强制执行
      forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        debug(s"Expiring produce request for partition $topicPartition with status $status")
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete(): Unit = {
    val responseStatus = produceMetadata.produceStatus.map { case (k, status) => k -> status.responseStatus }
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition): Unit = {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

