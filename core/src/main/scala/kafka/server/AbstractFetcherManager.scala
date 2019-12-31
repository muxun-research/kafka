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

package kafka.server

import com.yammer.metrics.core.Gauge
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.{Map, Set, mutable}

abstract class AbstractFetcherManager[T <: AbstractFetcherThread](val name: String, clientId: String, numFetchers: Int)
  extends Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher.
  // package private for test
  private[server] val fetcherThreadMap = new mutable.HashMap[BrokerIdAndFetcherId, T]
  private val lock = new Object
  private var numFetchersPerBroker = numFetchers
  val failedPartitions = new FailedPartitions
  this.logIdent = "[" + name + "] "

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value: Long = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          curMaxThread.max(fetcherLagStatsEntry._2.lag)
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> clientId)
  )

  newGauge(
    "MinFetchRate", {
      new Gauge[Double] {
        // current min fetch rate across all fetchers/topics/partitions
        def value: Double = {
          val headRate: Double =
            fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

          fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
            fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
          })
        }
      }
    },
    Map("clientId" -> clientId)
  )

  val failedPartitionsCount = newGauge(
    "FailedPartitionsCount", {
      new Gauge[Int] {
        def value: Int = failedPartitions.size
      }
    },
    Map("clientId" -> clientId)
  )

  def resizeThreadPool(newSize: Int): Unit = {
    def migratePartitions(newSize: Int): Unit = {
      fetcherThreadMap.foreach { case (id, thread) =>
        val removedPartitions = thread.partitionsAndOffsets
        removeFetcherForPartitions(removedPartitions.keySet)
        if (id.fetcherId >= newSize)
          thread.shutdown()
        addFetcherForPartitions(removedPartitions)
      }
    }

    lock synchronized {
      val currentSize = numFetchersPerBroker
      info(s"Resizing fetcher thread pool size from $currentSize to $newSize")
      numFetchersPerBroker = newSize
      if (newSize != currentSize) {
        // We could just migrate some partitions explicitly to new threads. But this is currently
        // reassigning all partitions using the new thread size so that hash-based allocation
        // works with partition add/delete as it did before.
        migratePartitions(newSize)
      }
      shutdownIdleFetcherThreads()
    }
  }

  // Visible for testing
  private[server] def getFetcher(topicPartition: TopicPartition): Option[T] = {
    lock synchronized {
      fetcherThreadMap.values.find { fetcherThread =>
        fetcherThread.fetchState(topicPartition).isDefined
      }
    }
  }

  // Visibility for testing
  private[server] def getFetcherId(topicPartition: TopicPartition): Int = {
    lock synchronized {
      Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchersPerBroker
    }
  }

  // This method is only needed by ReplicaAlterDirManager
  def markPartitionsForTruncation(brokerId: Int, topicPartition: TopicPartition, truncationOffset: Long): Unit = {
    lock synchronized {
      val fetcherId = getFetcherId(topicPartition)
      val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerId, fetcherId)
      fetcherThreadMap.get(brokerIdAndFetcherId).foreach { thread =>
        thread.markPartitionsForTruncation(topicPartition, truncationOffset)
      }
    }
  }

  /**
   * 为partition添加拉取器
   * @param partitionAndOffsets topic-partition及offset信息
   */
  def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    // 使用同步机制
    lock synchronized {
      // fetcher-partition维度
      val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
        BrokerAndFetcherId(brokerAndInitialFetchOffset.leader, getFetcherId(topicPartition))
      }

      /**
       * 添加并启动拉取线程
       * @param brokerAndFetcherId   Broker和Fetcher ID
       * @param brokerIdAndFetcherId BrokerID和Fetcher ID
       * @return 拉取线程
       */
      def addAndStartFetcherThread(brokerAndFetcherId: BrokerAndFetcherId, brokerIdAndFetcherId: BrokerIdAndFetcherId): AbstractFetcherThread = {
        // 创建拉取线程
        val fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
        // 以brokerId和FetcherId为key放入到已创建拉取线程集合中
        fetcherThreadMap.put(brokerIdAndFetcherId, fetcherThread)
        // 启动新创建的拉取线程
        fetcherThread.start()
        fetcherThread
      }

      // 对于每个拉取线程的partition信息
      for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
        // 构建BrokerIdAndFetcherId对象，作为获取拉取线程的key
        val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
        val fetcherThread = fetcherThreadMap.get(brokerIdAndFetcherId) match {
          case Some(currentFetcherThread) if currentFetcherThread.sourceBroker == brokerAndFetcherId.broker =>
            // 如果当前的拉取线程的源broker是当前的broker，则复用当前的拉取线程
            currentFetcherThread
          case Some(f) =>
            // 否则停止并创建一个新的拉取线程
            f.shutdown()
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
          case None =>
            // 没有拉取线程直接创建一个新的拉取线程
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
        }
        // 组装初始化拉取的offset和当前leader epoch对象
        val initialOffsetAndEpochs = initialFetchOffsets.map { case (tp, brokerAndInitOffset) =>
          tp -> OffsetAndEpoch(brokerAndInitOffset.initOffset, brokerAndInitOffset.currentLeaderEpoch)
        }
        // 添加拉取线程关联的partition
        fetcherThread.addPartitions(initialOffsetAndEpochs)
        info(s"Added fetcher to broker ${brokerAndFetcherId.broker} for partitions $initialOffsetAndEpochs")
        // 移除失败的partition
        failedPartitions.removeAll(partitionAndOffsets.keySet)
      }
    }
  }

  /**
   * 抽象方法，由子类实现创建特定的拉取器
   * @param fetcherId    拉取ID
   * @param sourceBroker 源broker
   * @return
   */
  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): T

  def removeFetcherForPartitions(partitions: Set[TopicPartition]): Unit = {
    lock synchronized {
      for (fetcher <- fetcherThreadMap.values)
        fetcher.removePartitions(partitions)
      failedPartitions.removeAll(partitions)
    }
    if (partitions.nonEmpty)
      info(s"Removed fetcher for partitions $partitions")
  }

  def shutdownIdleFetcherThreads(): Unit = {
    lock synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerIdAndFetcherId]
      for ((key, fetcher) <- fetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  def closeAllFetchers(): Unit = {
    lock synchronized {
      for ((_, fetcher) <- fetcherThreadMap) {
        fetcher.initiateShutdown()
      }

      for ((_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
    }
  }
}

/**
 * The class FailedPartitions would keep a track of partitions marked as failed either during truncation or appending
 * resulting from one of the following errors -
 * <ol>
 * <li> Storage exception
 * <li> Fenced epoch
 * <li> Unexpected errors
 * </ol>
 * The partitions which fail due to storage error are eventually removed from this set after the log directory is
 * taken offline.
 */
class FailedPartitions {
  private val failedPartitionsSet = new mutable.HashSet[TopicPartition]

  def size: Int = synchronized {
    failedPartitionsSet.size
  }

  def add(topicPartition: TopicPartition): Unit = synchronized {
    failedPartitionsSet += topicPartition
  }

  def removeAll(topicPartitions: Set[TopicPartition]): Unit = synchronized {
    failedPartitionsSet --= topicPartitions
  }

  def contains(topicPartition: TopicPartition): Boolean = synchronized {
    failedPartitionsSet.contains(topicPartition)
  }
}

case class BrokerAndFetcherId(broker: BrokerEndPoint, fetcherId: Int)

case class InitialFetchState(leader: BrokerEndPoint, currentLeaderEpoch: Int, initOffset: Long)

case class BrokerIdAndFetcherId(brokerId: Int, fetcherId: Int)
