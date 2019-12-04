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

package kafka.common

/**
 * 证明follower或者future副本收到了主副本（或当前副本）的firstOffset小于期望的offset
 * @param firstOffset 需要追加的第一个offset
 * @param lastOffset  要追加的记录的最后偏移量
 */
class UnexpectedAppendOffsetException(val message: String,
                                      val firstOffset: Long,
                                      val lastOffset: Long) extends RuntimeException(message) {
}
