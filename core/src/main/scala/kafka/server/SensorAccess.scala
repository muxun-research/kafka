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

import java.util.concurrent.locks.ReadWriteLock

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{MeasurableStat, MetricConfig, Metrics, Sensor}

/**
 * 创建/访问传感器的逻辑类
 * 可以将配额包装在传递的MetricConfig中来更新配额
 * 后面的参数作为方法传递，因为它们仅在实例化传感器时才被调用
 */
class SensorAccess(lock: ReadWriteLock, metrics: Metrics) {

  /**
   * 获取或创建传感器
   * @param sensorName     传感器名称
   * @param expirationTime 传感器失效时间
   * @param metricName     计数器名称
   * @param config         计数器配置，配额可以通过此种方式传入
   * @param measure
   * @return
   */
  def getOrCreate(sensorName: String, expirationTime: Long,
                  metricName: => MetricName, config: => Option[MetricConfig], measure: => MeasurableStat): Sensor = {
    var sensor: Sensor = null

    /*
     * 获取读锁来获取sensor，对于多线程来说，调用getSensor是线程安全的
     * read lock允许一个线程在隔离中创建sensor，创建sensor的线程会获取write lock并阻止传感器在创建过程中被读取
     * 只需要获取sensor是否为空而不获取read lock就足够了，但是已经存在传感器并不意味着它完全初始化，所有的指标还没有添加
     * read lock会进行等待直到写线程释放writer lock，完全初始化sensor这个节点是，代表读取是安全的
     */
    lock.readLock().lock()
    try sensor = metrics.getSensor(sensorName)
    finally lock.readLock().unlock()

    /*
     * 如果sensor为null，尝试创建sensor
     * 否则返回已存在的sensor
     * sensor可能为null，需要进行空校验
     */
    if (sensor == null) {
      /*
       * 获取write lock
       */
      lock.writeLock().lock()
      try {
        // 为两个传感器设置变量，避免另一个线程赢得了争夺了写锁定的资源变更
        // 会用于确认通过非空参数来初始化ClientSensors
        sensor = metrics.getSensor(sensorName)
        if (sensor == null) {
          // 获取已有的sensor或者创建新sensor返回的sensor
          sensor = metrics.sensor(sensorName, config.orNull, expirationTime)
          // 添加度量标准和数据分析
          sensor.add(metricName, measure)
        }
      } finally {
        // 释放write lock
        lock.writeLock().unlock()
      }
    }
    sensor
  }
}
