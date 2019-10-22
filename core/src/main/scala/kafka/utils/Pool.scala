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

package kafka.utils

import java.util.concurrent._

import org.apache.kafka.common.KafkaException

import scala.collection.JavaConverters._
import scala.collection.mutable

class Pool[K,V](valueFactory: Option[K => V] = None) extends Iterable[(K, V)] {

  private val pool: ConcurrentMap[K, V] = new ConcurrentHashMap[K, V]
  
  def put(k: K, v: V): V = pool.put(k, v)
  
  def putIfNotExists(k: K, v: V): V = pool.putIfAbsent(k, v)

  /**
   * 获取给定key有关的value
   * 如果没有关联的value，就会使用pool的value工厂创建一个value，并关联这个key，返回此value
   * 开发者可以声明工厂方法为懒加载，如果需要避免副作用的情况下
   * @param key The key to lookup.
   * @return The final value associated with the key.
   */
  def getAndMaybePut(key: K): V = {
    // 没有value工厂的情况下，抛出异常
    if (valueFactory.isEmpty)
      throw new KafkaException("Empty value factory in pool.")
    getAndMaybePut(key, valueFactory.get(key))
  }

  /**
   * 获取给定key有关的value
   * 如果没有关联的value，就会使用createValue创建一个value，并关联这个key，返回此value
   * @param key         需要查找的key
   * @param createValue value工厂创造value函数
   * @return 最终与key进行关联的value
   */
  def getAndMaybePut(key: K, createValue: => V): V =
    pool.computeIfAbsent(key, new java.util.function.Function[K, V] {
      override def apply(k: K): V = createValue
    })

  def contains(id: K): Boolean = pool.containsKey(id)
  
  def get(key: K): V = pool.get(key)
  
  def remove(key: K): V = pool.remove(key)

  def remove(key: K, value: V): Boolean = pool.remove(key, value)

  def keys: mutable.Set[K] = pool.keySet.asScala

  def values: Iterable[V] = pool.values.asScala

  def clear(): Unit = { pool.clear() }
  
  override def size: Int = pool.size
  
  override def iterator: Iterator[(K, V)] = new Iterator[(K,V)]() {
    
    private val iter = pool.entrySet.iterator
    
    def hasNext: Boolean = iter.hasNext
    
    def next: (K, V) = {
      val n = iter.next
      (n.getKey, n.getValue)
    }
    
  }
    
}
