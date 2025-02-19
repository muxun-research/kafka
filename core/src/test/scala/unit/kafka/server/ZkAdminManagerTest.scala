/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.server.metadata.ZkConfigRepository
import kafka.utils.TestUtils
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.Mockito.{mock, when}

import java.util.Properties
import scala.jdk.CollectionConverters._

class ZkAdminManagerTest {

  private val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val topic = "topic-1"
  private val metadataCache: MetadataCache = mock(classOf[MetadataCache])

  @AfterEach
  def tearDown(): Unit = {
    metrics.close()
  }

  def createConfigHelper(metadataCache: MetadataCache, zkClient: KafkaZkClient): ConfigHelper = {
    val props = TestUtils.createBrokerConfig(brokerId, "zk")
    new ConfigHelper(metadataCache, KafkaConfig.fromProps(props), new ZkConfigRepository(new AdminZkClient(zkClient)))
  }

  @Test
  def testClientQuotasToProps(): Unit = {
    val emptyProps = ZkAdminManager.clientQuotaPropsToDoubleMap(Map.empty)
    assertEquals(0, emptyProps.size)

    val oneProp = ZkAdminManager.clientQuotaPropsToDoubleMap(Map("foo" -> "1234"))
    assertEquals(1, oneProp.size)
    assertEquals(1234.0, oneProp("foo"))

    // This is probably not desired, but kept for compatibility with existing usages
    val emptyKey = ZkAdminManager.clientQuotaPropsToDoubleMap(Map("" -> "-42.1"))
    assertEquals(1, emptyKey.size)
    assertEquals(-42.1, emptyKey(""))

    val manyProps = ZkAdminManager.clientQuotaPropsToDoubleMap(Map("foo" -> "1234", "bar" -> "0", "spam" -> "-1234.56"))
    assertEquals(3, manyProps.size)

    assertThrows(classOf[NullPointerException], () => ZkAdminManager.clientQuotaPropsToDoubleMap(Map("foo" -> null)))
    assertThrows(classOf[IllegalStateException], () => ZkAdminManager.clientQuotaPropsToDoubleMap(Map("foo" -> "bar")))
    assertThrows(classOf[IllegalStateException], () => ZkAdminManager.clientQuotaPropsToDoubleMap(Map("foo" -> "")))
  }

  @Test
  def testDescribeConfigsWithNullConfigurationKeys(): Unit = {
    when(zkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    when(metadataCache.contains(topic)).thenReturn(true)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setConfigurationKeys(null))
    val configHelper = createConfigHelper(metadataCache, zkClient)
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    assertFalse(results.head.configs().isEmpty, "Should return configs")
  }

  @Test
  def testDescribeConfigsWithEmptyConfigurationKeys(): Unit = {
    when(zkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    when(metadataCache.contains(topic)).thenReturn(true)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id))
    val configHelper = createConfigHelper(metadataCache, zkClient)
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    assertFalse(results.head.configs().isEmpty, "Should return configs")
  }

  @Test
  def testDescribeConfigsWithConfigurationKeys(): Unit = {
    when(zkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(TestUtils.createBrokerConfig(brokerId, "zk"))
    when(metadataCache.contains(topic)).thenReturn(true)

    val resources = List(new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceName(topic)
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setConfigurationKeys(List("retention.ms", "retention.bytes", "segment.bytes").asJava)
    )
    val configHelper = createConfigHelper(metadataCache, zkClient)
    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(Errors.NONE.code, results.head.errorCode())
    val resultConfigKeys = results.head.configs().asScala.map(r => r.name()).toSet
    assertEquals(Set("retention.ms", "retention.bytes", "segment.bytes"), resultConfigKeys)
  }

  @Test
  def testDescribeConfigsWithDocumentation(): Unit = {
    when(zkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(new Properties)
    when(zkClient.getEntityConfigs(ConfigType.Broker, brokerId.toString)).thenReturn(new Properties)
    when(metadataCache.contains(topic)).thenReturn(true)

    val configHelper = createConfigHelper(metadataCache, zkClient)

    val resources = List(
      new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(topic)
        .setResourceType(ConfigResource.Type.TOPIC.id),
      new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(brokerId.toString)
        .setResourceType(ConfigResource.Type.BROKER.id))

    val results: List[DescribeConfigsResponseData.DescribeConfigsResult] = configHelper.describeConfigs(resources, true, true)
    assertEquals(2, results.size)
    results.foreach(r => {
      assertEquals(Errors.NONE.code, r.errorCode)
      assertFalse(r.configs.isEmpty, "Should return configs")
      r.configs.forEach(c => {
        assertNotNull(c.documentation, s"Config ${c.name} should have non null documentation")
        assertNotEquals(s"Config ${c.name} should have non blank documentation", "", c.documentation.trim)
      })
    })
  }
}
