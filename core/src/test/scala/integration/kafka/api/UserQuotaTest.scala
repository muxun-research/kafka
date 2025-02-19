/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.api

import kafka.server.KafkaBroker
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

class UserQuotaTest extends BaseQuotaTest with SaslSetup {

  override protected def securityProtocol = SecurityProtocol.SASL_SSL

  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))
  private val kafkaServerSaslMechanisms = Seq("GSSAPI")
  private val kafkaClientSaslMechanism = "GSSAPI"
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some("GSSAPI"), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    super.setUp(testInfo)
    quotaTestClients.alterClientQuotas(
      quotaTestClients.clientQuotaAlteration(
        quotaTestClients.clientQuotaEntity(Some(QuotaTestClients.DefaultEntity), None),
        Some(defaultProducerQuota), Some(defaultConsumerQuota), Some(defaultRequestQuota)
      )
    )
    quotaTestClients.waitForQuotaUpdate(defaultProducerQuota, defaultConsumerQuota, defaultRequestQuota)
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  override def createQuotaTestClients(topic: String, leaderNode: KafkaBroker): QuotaTestClients = {
    val producer = createProducer()
    val consumer = createConsumer()
    val adminClient = createAdminClient()

    new QuotaTestClients(topic, leaderNode, producerClientId, consumerClientId, producer, consumer, adminClient) {
      override val userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaClientPrincipalUnqualifiedName2)

      override def quotaMetricTags(clientId: String): Map[String, String] = {
        Map("user" -> userPrincipal.getName, "client-id" -> "")
      }

      override def overrideQuotas(producerQuota: Long, consumerQuota: Long, requestQuota: Double): Unit = {
        alterClientQuotas(
          clientQuotaAlteration(
            clientQuotaEntity(Some(userPrincipal.getName), None),
            Some(producerQuota), Some(consumerQuota), Some(requestQuota)
          )
        )
      }

      override def removeQuotaOverrides(): Unit = {
        alterClientQuotas(
          clientQuotaAlteration(
            clientQuotaEntity(Some(userPrincipal.getName), None),
            None, None, None
          )
        )
      }
    }
  }
}
