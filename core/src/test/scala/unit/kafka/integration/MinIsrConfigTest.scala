/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.integration

import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

class MinIsrConfigTest extends KafkaServerTestHarness {
  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.MinInSyncReplicasProp, "5")

  def generateConfigs: Seq[KafkaConfig] = TestUtils.createBrokerConfigs(1, zkConnectOrNull).map(KafkaConfig.fromProps(_, overridingProps))

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDefaultKafkaConfig(quorum: String): Unit = {
    assert(brokers.head.logManager.initialDefaultConfig.minInSyncReplicas == 5)
  }
}
