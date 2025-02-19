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

import kafka.utils.TestUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import scala.collection.mutable.ArrayBuffer

class AdvertiseBrokerTest extends QuorumTestHarness {
  val servers = ArrayBuffer[KafkaServer]()

  val brokerId = 0

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testBrokerAdvertiseListenersToZK(): Unit = {
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect, enableControlledShutdown = false)
    props.put("advertised.listeners", "PLAINTEXT://routable-listener:3334")
    servers += TestUtils.createServer(KafkaConfig.fromProps(props))

    val brokerInfo = zkClient.getBroker(brokerId).get
    assertEquals(1, brokerInfo.endPoints.size)
    val endpoint = brokerInfo.endPoints.head
    assertEquals("routable-listener", endpoint.host)
    assertEquals(3334, endpoint.port)
    assertEquals(SecurityProtocol.PLAINTEXT, endpoint.securityProtocol)
    assertEquals(SecurityProtocol.PLAINTEXT.name, endpoint.listenerName.value)
  }

  @Test
  def testBrokerAdvertiseListenersWithCustomNamesToZK(): Unit = {
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect, enableControlledShutdown = false)
    props.put("listeners", "INTERNAL://:0,EXTERNAL://:0")
    props.put("advertised.listeners", "EXTERNAL://external-listener:9999,INTERNAL://internal-listener:10999")
    props.put("listener.security.protocol.map", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT")
    props.put("inter.broker.listener.name", "INTERNAL")
    servers += TestUtils.createServer(KafkaConfig.fromProps(props))

    val brokerInfo = zkClient.getBroker(brokerId).get
    assertEquals(2, brokerInfo.endPoints.size)
    val endpoint = brokerInfo.endPoints.head
    assertEquals("external-listener", endpoint.host)
    assertEquals(9999, endpoint.port)
    assertEquals(SecurityProtocol.PLAINTEXT, endpoint.securityProtocol)
    assertEquals("EXTERNAL", endpoint.listenerName.value)
    val endpoint2 = brokerInfo.endPoints(1)
    assertEquals("internal-listener", endpoint2.host)
    assertEquals(10999, endpoint2.port)
    assertEquals(SecurityProtocol.PLAINTEXT, endpoint.securityProtocol)
    assertEquals("INTERNAL", endpoint2.listenerName.value)
  }
  
}
