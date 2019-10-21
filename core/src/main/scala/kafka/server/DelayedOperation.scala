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

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * 延迟操作
 * 一个操作需要进行延迟处理
 *
 * 在逻辑上，一个延迟操作完成的任务生命在onComplete()方法中，而且只会被调用一次
 * 如果一个操作完成了，isCompleted()会返回true
 * onComplete()可以被forceComplete()在等待时间后但是操作还没有完成的情况下强制触发
 * 或者通过tryComplete()方法，首先校验当前操作是否可以完成，然后再调用forceComplete()方法
 *
 * DelayedOperation子类需要同时提供onComplete()或者tryComplete()的实现
 */
abstract class DelayedOperation(override val delayMs: Long,
                                lockOpt: Option[Lock] = None)
  extends TimerTask with Logging {

  private val completed = new AtomicBoolean(false)
  private val tryCompletePending = new AtomicBoolean(false)
  // Visible for testing
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /**
   * 当延迟的操作到期并因此被强制完成时执行回调
   */
  def onExpiration(): Unit

  /**
   * 一个完成任务的进程，需要在子类中实现，并且只会在forceComplete()中调用一次
   */
  def onComplete(): Unit

  /**
   * 尝试完成延迟操作，首先需要检查当前是否可完成
   * 如果可完成，则调用forceComplete()完成任务，并返回true
   * 其他情况，返回false
   * 需要在子类中实现
   */
  def tryComplete(): Boolean

  /*
   * run()方法声明了一个用于执行等待的任务
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }

  /*
   * 强制完成延迟的操作，如果还没有完成，函数可以在以下情况下被触发
   * 1. 操作已经在tryComplete()方法中被验证可以完成
   * 2. 操作已经失效，需要立即完成
   * 如果调用者完成了任务，返回true
   * 需要注意的是，存在并发下完成任务的情况，但是仅有第一个线程可以完成任务，并返回true，其他线程将会返回false
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // 取消等待计时器
      cancel()
      // 完成任务
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * tryComplete()的线程安全的变体，用于仅在可以占有锁的情况下，尝试完成，不会进行阻塞
   * 确保每次maybeTryComplete()都会在至少一次tryComplete()调用之后调用，知道操作真正完成
   */
  private[server] def maybeTryComplete(): Boolean = {
    var retry = false
    var done = false
    do {
      if (lock.tryLock()) {
        try {
          tryCompletePending.set(false)
          done = tryComplete()
        } finally {
          lock.unlock()
        }
        // 当前已经占有了锁，另一个线程调用了maybeTryComplete()并设置了tryCompletePending，就需要进行重试
        retry = tryCompletePending.get()
      } else {
        // 另一线程占有了锁，如果已经设置了tryCompletePending并且当前线程尝试获取锁失败了，那么持有锁的线程就会保证看到标识，并进行重试
        // 否则，应该设置标识并在当前线程上重试，因为持有锁的线程可能已经释放了锁，并返回设置标识的时间
        retry = !tryCompletePending.getAndSet(true)
      }
    } while (!isCompleted && retry)
    done
  }

  /**
   * 检查当前任务是否已完成
   */
  def isCompleted: Boolean = completed.get()
}

object DelayedOperationPurgatory {

  private val Shards = 512 // Shard the watcher list to reduce lock contention

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {
  /* a list of operation watching keys */
  private class WatcherList {
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     */
    def allWatchers = {
      watchersByKey.values
    }
  }

  private val watcherLists = Array.fill[WatcherList](DelayedOperationPurgatory.Shards)(new WatcherList)
  private def watcherList(key: Any): WatcherList = {
    watcherLists(Math.abs(key.hashCode() % watcherLists.length))
  }

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value: Int = watched
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value: Int = numDelayed
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    // At this point the only thread that can attempt this operation is this current thread
    // Hence it is safe to tryComplete() without a lock
    var isCompletedByMe = operation.tryComplete()
    if (isCompletedByMe)
      return true

    var watchCreated = false
    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (operation.isCompleted)
        return false
      watchForOperation(key, operation)

      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }

    isCompletedByMe = operation.maybeTryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    if (!operation.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(operation)
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * 指定的key是否有一些延迟的操作需要完成，如果有，完成它们
   * @return 完成的项目数量
   */
  def checkAndComplete(key: Any): Int = {
    val wl = watcherList(key)
    // 需要完成在同步状态下获取watcher
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    val numCompleted = if (watchers == null)
      0
    else
    // 完成watcher任务
      watchers.tryCompleteWatched()
    debug(s"Request key $key unblocked $numCompleted $purgatoryName operations")
    // 返回完成的任务数量
    numCompleted
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = {
    watcherLists.foldLeft(0) { case (sum, watcherList) => sum + watcherList.allWatchers.map(_.countWatched).sum }
  }

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def numDelayed: Int = timeoutTimer.size

  /**
    * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
    */
  def cancelForKey(key: Any): List[T] = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watchers = wl.watchersByKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watcher = wl.watchersByKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (wl.watchersByKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        wl.watchersByKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown(): Unit = {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: T): Unit = {
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          iter.remove()
        } else if (curr.maybeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long): Unit = {
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(numDelayed)
      debug("Begin purging watch lists")
      val purged = watcherLists.foldLeft(0) {
        case (sum, watcherList) => sum + watcherList.allWatchers.map(_.purgeCompleted()).sum
      }
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork(): Unit = {
      advanceClock(200L)
    }
  }
}
