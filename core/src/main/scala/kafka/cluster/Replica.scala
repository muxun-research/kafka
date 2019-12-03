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

package kafka.cluster

import kafka.log.Log
import kafka.server.LogOffsetMetadata
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

class Replica(val brokerId: Int, val topicPartition: TopicPartition) extends Logging {
  /**
   * 偏移量元数据，在所有节点中都会进行存储
   * 对于本地副本来说，是日志文件的结束偏移量
   * 对于远程副本来说，只会由follower拉取时进行更新
   */
  @volatile private[this] var _logEndOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata
  // for local replica it is the log's start offset, for remote replicas its value is only updated by follower fetch
  /**
   * log起始偏移量，在所有节点中都会进行存储
   * 对于本地副本，是日志的起始偏移量
   * 对于远程副本，只会在follower拉取时进行更新
   */
  @volatile private[this] var _logStartOffset = Log.UnknownOffset

  /**
   * 当前leader节点收到follower最近一次的FetchRequest的logEndOffset
   * 用于确认follower节点的"lastCaughtUpTimeMs"
   */
  @volatile private[this] var lastFetchLeaderLogEndOffset = 0L

  /**
   * leader节点收到此follower节点最近一次FetchRequest的时间戳
   * 用于确认follower节点的"lastCaughtUpTimeMs"
   */
  @volatile private[this] var lastFetchTimeMs = 0L

  /**
   * 是此follower节点大多数FetchRequest的offset ≥ leader节点的logEndOffset的最大时间
   * 用于确认follower滞后的进度和分区的ISR
   */
  @volatile private[this] var _lastCaughtUpTimeMs = 0L

  /**
   * 最高水位是在follower节点的大多数FetchRequest之后，leader节点的最高水位
   * 用于确认follower已知的最大高水位
   * 请看KIP-392
   */
  @volatile private[this] var _lastSentHighWatermark = 0L

  def logStartOffset: Long = _logStartOffset

  def logEndOffsetMetadata: LogOffsetMetadata = _logEndOffsetMetadata

  def logEndOffset: Long = logEndOffsetMetadata.messageOffset

  def lastCaughtUpTimeMs: Long = _lastCaughtUpTimeMs

  def lastSentHighWatermark: Long = _lastSentHighWatermark

  /*
   * 如果在收到当前FetchRequest时，FetchRequest读取到leader节点的logEndOffset位置，设置"lastCaughtUpTimeMs"时间为收到当前FetchRequest的时间
   *
   * 或者如果在收到上一个FetchRequest时，FetchRequest读取到leader节点的logEndOffset位置，设置"lastCaughtUpTimeMs"时间为收到上一个FetchRequest的时间
   *
   * 用于强制执行ISR，比如，当且仅当副本节点最多落后于leader节点的logEndOffset（LEO）达到"replicaLagTimeMaxMs"时，副本节点才处于ISR
   * 这些语义允许将关注者添加到ISR，即使它的FetchRequest的offset总是比leader的logEndOffset小，这可以发生在高频次接收生产请求时
   */
  def updateFetchState(followerFetchOffsetMetadata: LogOffsetMetadata,
                       followerStartOffset: Long,
                       followerFetchTimeMs: Long,
                       leaderEndOffset: Long): Unit = {
    // 如果follower节点发送的FetchRequest请求的offset ≥ leader节点的logEndOffset
    if (followerFetchOffsetMetadata.messageOffset >= leaderEndOffset)
    // 设置"lastCaughtUpTimeMs" ＝ "_lastCaughtUpTimeMs"和"follower节点FetchRequest时间"的最大值
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, followerFetchTimeMs)
    else if (followerFetchOffsetMetadata.messageOffset >= lastFetchLeaderLogEndOffset)
    // 如果follower节点发送的FetchRequest请求的offset ≥ leader节点收到follower最近一次的FetchRequest的logEndOffset
    // 设置"lastCaughtUpTimeMs" ＝ "_lastCaughtUpTimeMs"和"lastFetchTimeMs"的最大值
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, lastFetchTimeMs)
    // 更新logStartOffset、logEndOffsetMetadata
    // lastFetchLeaderLogEndOffset、lastFetchTimeMs
    _logStartOffset = followerStartOffset
    _logEndOffsetMetadata = followerFetchOffsetMetadata
    lastFetchLeaderLogEndOffset = leaderEndOffset
    lastFetchTimeMs = followerFetchTimeMs
    trace(s"Updated state of replica to $this")
  }

  /**
   * 更新远程副本的高水位
   * 用于追踪已知最新的远程副本的高水位
   * 当发送响应时进行记录，并不保证follower节点实际会收到高水位，所以可以认为这是一个follower节点已知的上限水位
   *
   * 在处理拉取请求时，回去检查最近一次发送的高水位来得出是否需要立即返回响应，为了迅速的传播高水位
   * 具体请看KIP-392
   */
  def updateLastSentHighWatermark(highWatermark: Long): Unit = {
    // 更新为最近一次发送的高水位
    _lastSentHighWatermark = highWatermark
    trace(s"Updated HW of replica to $highWatermark")
  }

  /**
   * 重置_lastCaughtUpTimeMs
   * @param curLeaderLogEndOffset 进行更新的最近一次FetchRequest的logEndOffset
   * @param curTimeMs             当前时间戳
   * @param lastCaughtUpTimeMs    follower节点 ≥ leader节点的logEndOffset的最大时间
   */
  def resetLastCaughtUpTime(curLeaderLogEndOffset: Long, curTimeMs: Long, lastCaughtUpTimeMs: Long): Unit = {
    // 更新leader节点收到follower最近一次的FetchRequest的logEndOffset
    lastFetchLeaderLogEndOffset = curLeaderLogEndOffset
    // 更新leader节点收到此follower节点最近一次FetchRequest的时间戳
    // 为当前时间戳
    lastFetchTimeMs = curTimeMs
    // 更新大多数FetchRequest的offset ≥ leader节点的logEndOffset的最大时间
    _lastCaughtUpTimeMs = lastCaughtUpTimeMs
    trace(s"Reset state of replica to $this")
  }

  override def toString: String = {
    val replicaString = new StringBuilder
    replicaString.append("Replica(replicaId=" + brokerId)
    replicaString.append(s", topic=${topicPartition.topic}")
    replicaString.append(s", partition=${topicPartition.partition}")
    replicaString.append(s", lastCaughtUpTimeMs=$lastCaughtUpTimeMs")
    replicaString.append(s", logStartOffset=$logStartOffset")
    replicaString.append(s", logEndOffset=$logEndOffset")
    replicaString.append(s", logEndOffsetMetadata=$logEndOffsetMetadata")
    replicaString.append(s", lastFetchLeaderLogEndOffset=$lastFetchLeaderLogEndOffset")
    replicaString.append(s", lastFetchTimeMs=$lastFetchTimeMs")
    replicaString.append(s", lastSentHighWatermark=$lastSentHighWatermark")
    replicaString.append(")")
    replicaString.toString
  }

  override def equals(that: Any): Boolean = that match {
    case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
    case _ => false
  }

  override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId
}
