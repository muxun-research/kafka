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

package kafka.tools

import java.time.Duration
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.regex.Pattern
import java.util.{Collections, Properties}

import com.yammer.metrics.core.Gauge
import kafka.consumer.BaseConsumerRecord
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors.{TimeoutException, WakeupException}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.control.ControlThrowable
import scala.util.{Failure, Success, Try}

/**
 * Mirror Maker有一下的结构：
 * - 有N个mirror maker线程，每个mirror maker线程都有一个分离的KafkaConsumer实例
 * - 所有的mirror maker现场共享同一个producer
 * - 每个mirro maker线程会周期性地刷新producer，提交所有的offset
 * @note    对于mirror maker，为了避免数据丢失，以下设置为默认设置：
 *       1. Producer使用下列配置：
 *          acks=all、delivery.timeout.ms=max integer、max.block.ms=max long、max.in.flight.requests.per.connection=1
 *       2. Consumer配置：
 *          enable.auto.commit=false
 *       3. Mirror Maker设置
 *          abort.on.send.failure=true
 */
object MirrorMaker extends Logging with KafkaMetricsGroup {
  /**
   * mirror maker进程是否处于正在关闭状态
   */
  private val isShuttingDown: AtomicBoolean = new AtomicBoolean(false)
  /**
   * 记录下mirror maker没有发送成功的消息数量
   */
  private val numDroppedMessages: AtomicInteger = new AtomicInteger(0)
  /**
   * mirror maker关联的Producer
   */
  private[tools] var producer: MirrorMakerProducer = null
  // Track the messages not successfully sent by mirror maker.
  /**
   * mirror maker线程数
   */
  private var mirrorMakerThreads: Seq[MirrorMakerThread] = null
  /**
   * mirror maker消息处理器
   */
  private var messageHandler: MirrorMakerMessageHandler = null
  private var offsetCommitIntervalMs = 0
  private var abortOnSendFailure: Boolean = true
  @volatile private var exitingOnSendFailure: Boolean = false
  private var lastSuccessfulCommitTime = -1L
  private val time = Time.SYSTEM

  // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
  // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
  // message was not really acked, but was skipped. This metric records the number of skipped offsets.
  newGauge("MirrorMaker-numDroppedMessages",
    new Gauge[Int] {
      def value = numDroppedMessages.get()
    })

  /**
   * Mirror Maker进程启动器
   * @param args 启动参数
   */
  def main(args: Array[String]): Unit = {

    info("Starting mirror maker")
    try {
      // 根据启动参数构建内部MirrorMaker操作对象
      val opts = new MirrorMakerOptions(args)
      // 命令行参数是否包含帮助、版本号信息
      CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to continuously copy data between two Kafka clusters.")
      // 校验参数
      opts.checkArgs()
    } catch {
      case ct: ControlThrowable => throw ct
      case t: Throwable =>
        error("Exception when starting mirror maker.", t)
    }
    // 启动所有的mirror maker线程
    mirrorMakerThreads.foreach(_.start())
    // 启动所有mirror maker线程的等待关闭任务
    mirrorMakerThreads.foreach(_.awaitShutdown())
  }

  /**
   * 为每个mirror maker线程创建Consumer
   * @param numStreams              需要创建的mirror maker线程数量
   * @param consumerConfigProps     Consumer配置属性
   * @param customRebalanceListener Consumer重分配监听器
   * @param whitelist               白名单
   * @return 创建的Consumer列表
   */
  def createConsumers(numStreams: Int,
                      consumerConfigProps: Properties,
                      customRebalanceListener: Option[ConsumerRebalanceListener],
                      whitelist: Option[String]): Seq[ConsumerWrapper] = {
    // 默认关闭Consumer的自动提交功能
    maybeSetDefaultProperty(consumerConfigProps, "enable.auto.commit", "false")
    // 硬编码key和value的反序列化器
    consumerConfigProps.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
    consumerConfigProps.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
    // 默认的client.id是group.id，默认将client.id设置为groupId-index来避免数据统计冲突
    val groupIdString = consumerConfigProps.getProperty("group.id")
    val consumers = (0 until numStreams) map { i =>
      // 设置client.id并创建KafkaConsumer
      consumerConfigProps.setProperty("client.id", groupIdString + "-" + i.toString)
      new KafkaConsumer[Array[Byte], Array[Byte]](consumerConfigProps)
    }
    // 白名单不能为空
    whitelist.getOrElse(throw new IllegalArgumentException("White list cannot be empty"))
    // 将Consumer包装为ConsumerWrapper
    consumers.map(consumer => new ConsumerWrapper(consumer, customRebalanceListener, whitelist))
  }

  /**
   * 手动提交偏移量
   * @param consumerWrapper 每个Consumer的包装类
   */
  def commitOffsets(consumerWrapper: ConsumerWrapper): Unit = {
    if (!exitingOnSendFailure) {
      // 重试次数
      var retry = 0
      // 是否需要重试
      var retryNeeded = true
      // 如果需要进行重试，则进入死循环
      while (retryNeeded) {
        trace("Committing offsets.")
        try {
          // 手动提交offset
          consumerWrapper.commit()
          // 记录最近一次成功提交的时间
          lastSuccessfulCommitTime = time.milliseconds
          // 提交成功，不需要继续重试
          retryNeeded = false
        } catch {
          case e: WakeupException =>
            // 仅有在关闭consumer时才会调用wakeup()方法
            // 可以在提交的时候捕获这个异常，可以进行更安全的重试
            // 也可以通过抛出这个异常来中断循环
            commitOffsets(consumerWrapper)
            throw e

          case _: TimeoutException =>
            // 提交超时异常，consumer线程停止工作100ms后继续重试
            Try(consumerWrapper.consumer.listTopics) match {
              case Success(visibleTopics) =>
                consumerWrapper.offsets.retain((tp, _) => visibleTopics.containsKey(tp.topic))
              case Failure(e) =>
                warn("Failed to list all authorized topics after committing offsets timed out: ", e)
            }

            retry += 1
            warn("Failed to commit offsets because the offset commit request processing can not be completed in time. " +
              s"If you see this regularly, it could indicate that you need to increase the consumer's ${ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG} " +
              s"Last successful offset commit timestamp=$lastSuccessfulCommitTime, retry count=$retry")
            Thread.sleep(100)

          case _: CommitFailedException =>
            // 提交失败异常，不进行重试
            retryNeeded = false
            warn("Failed to commit offsets because the consumer group has rebalanced and assigned partitions to " +
              "another instance. If you see this regularly, it could indicate that you need to either increase " +
              s"the consumer's ${ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG} or reduce the number of records " +
              s"handled on each iteration with ${ConsumerConfig.MAX_POLL_RECORDS_CONFIG}")
        }
      }
    } else {
      info("Exiting on send failure, skip committing offsets.")
    }
  }

  def cleanShutdown(): Unit = {
    if (isShuttingDown.compareAndSet(false, true)) {
      info("Start clean shutdown.")
      // Shutdown consumer threads.
      info("Shutting down consumer threads.")
      if (mirrorMakerThreads != null) {
        mirrorMakerThreads.foreach(_.shutdown())
        mirrorMakerThreads.foreach(_.awaitShutdown())
      }
      info("Closing producer.")
      producer.close()
      info("Kafka mirror maker shutdown successfully")
    }
  }

  private def maybeSetDefaultProperty(properties: Properties, propertyName: String, defaultValue: String): Unit = {
    val propertyValue = properties.getProperty(propertyName)
    properties.setProperty(propertyName, Option(propertyValue).getOrElse(defaultValue))
    if (properties.getProperty(propertyName) != defaultValue)
      info("Property %s is overridden to %s - data loss or message reordering is possible.".format(propertyName, propertyValue))
  }

  /**
   * mirror maker线程
   * @param consumerWrapper mirror maker关联的consumer
   * @param threadId        线程ID
   */
  class MirrorMakerThread(consumerWrapper: ConsumerWrapper,
                          val threadId: Int) extends Thread with Logging with KafkaMetricsGroup {
    /**
     * 统一线程名称
     */
    private val threadName = "mirrormaker-thread-" + threadId
    /**
     * 线程关闭栅栏
     */
    private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
    /**
     * 上一次提交的时间戳
     */
    private var lastOffsetCommitMs = System.currentTimeMillis()
    /**
     * mirror maker线程是否正在关闭
     */
    @volatile private var shuttingDown: Boolean = false
    this.logIdent = "[%s] ".format(threadName)

    /**
     * 设置线程名称
     */
    setName(threadName)

    /**
     * mirror maker线程任务执行
     */
    override def run(): Unit = {
      info(s"Starting mirror maker thread $threadName")
      try {
        // 初始化关联的Consumer
        consumerWrapper.init()

        // 为了兼容旧版本的KafkaConsumer，此处需要两种循环
        // 现在可以进行简化
        // 对于新KafkaConsumer，hasData()方法总是返回true，但是轮询时没有拉取到数据，会抛出ConsumerTimeoutException异常
        // 第一层循环即使在有异常的情况下，消费者线程还会进行轮询
        // 对于旧消费者，hasData()方法可能返回false，第一层循环也要能保证获取到新的消息
        // 没有发送失败和mirror maker线程没有关闭的情况下，进行双重循环
        while (!exitingOnSendFailure && !shuttingDown) {
          try {
            while (!exitingOnSendFailure && !shuttingDown) {
              // 从KafkaConsumer中接收消息
              val data = consumerWrapper.receive()
              if (data.value != null) {
                trace("Sending message with value size %d and offset %d.".format(data.value.length, data.offset))
              } else {
                trace("Sending message with null value and offset %d.".format(data.offset))
              }
              // 通过MirrorMakerMessageHandler处理从KafkaConsumer中的消息
              // 处理完成后，是List[ProduceRecord]类型，方便消费者进行发送
              val records = messageHandler.handle(toBaseConsumerRecord(data))
              // 遍历所有的Base
              records.asScala.foreach(producer.send)
              // 发送到KafkaProducer的缓冲区后，立即调用KakfaProducer#flush()方法，将消息写入新集群，并手动提交offset
              maybeFlushAndCommitOffsets()
            }
          } catch {
            case _: NoRecordsException =>
              trace("Caught NoRecordsException, continue iteration.")
            case _: WakeupException =>
              trace("Caught WakeupException, continue iteration.")
            case e: KafkaException if (shuttingDown || exitingOnSendFailure) =>
              trace(s"Ignoring caught KafkaException during shutdown. sendFailure: $exitingOnSendFailure.", e)
          }
          // 可能会抛出异常，也进行一次KakfaProducer#flush()和手动提交offset
          maybeFlushAndCommitOffsets()
        }
      } catch {
        case t: Throwable =>
          exitingOnSendFailure = true
          fatal("Mirror maker thread failure due to ", t)
      } finally {
        CoreUtils.swallow({
          info("Flushing producer.")
          // 无论成功还是失败，都再次清空一次KafkaProducer的缓冲区，使消息及时送达到代理节点
          producer.flush()

          // 需要注意的是，如果flush()调用失败，这个commit会被跳过，这确保了不会丢失消息
          info("Committing consumer offsets.")
          // 手动提交offset
          commitOffsets(consumerWrapper)
        }, this)

        info("Shutting down consumer connectors.")
        // 调用KafkaConsumer的wakeup()方法，准备关闭KafkaConsumer
        CoreUtils.swallow(consumerWrapper.wakeup(), this)
        // 关闭KafkaConsumer
        CoreUtils.swallow(consumerWrapper.close(), this)
        // 关闭栅栏-1
        shutdownLatch.countDown()
        info("Mirror maker thread stopped")
        // 如果是意外退出的，需要完全停止mirror maker
        if (!isShuttingDown.get()) {
          fatal("Mirror maker thread exited abnormally, stopping the whole mirror maker.")
          sys.exit(-1)
        }
      }
    }

    /**
     * 将ConsumerRecord转换为BaseConsumerRecord
     * @param record 新KafkaConsumer的ConsumerRecord
     * @return 旧版本客户端的BaseConsumerRecord
     */
    private def toBaseConsumerRecord(record: ConsumerRecord[Array[Byte], Array[Byte]]): BaseConsumerRecord =
      BaseConsumerRecord(record.topic,
        record.partition,
        record.offset,
        record.timestamp,
        record.timestampType,
        record.key,
        record.value,
        record.headers)

    /**
     * 可能需要调用KafkaProducer#flush()方法及手动提交offset
     */
    def maybeFlushAndCommitOffsets(): Unit = {
      // 在超过了提交时间间隔的情况
      // 用KafkaProducer#flush()方法及手动提交offset
      if (System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
        debug("Committing MirrorMaker state.")
        producer.flush()
        commitOffsets(consumerWrapper)
        lastOffsetCommitMs = System.currentTimeMillis()
      }
    }

    def shutdown(): Unit = {
      try {
        info(s"$threadName shutting down")
        shuttingDown = true
        consumerWrapper.wakeup()
      }
      catch {
        case _: InterruptedException =>
          warn("Interrupt during shutdown of the mirror maker thread")
      }
    }

    def awaitShutdown(): Unit = {
      try {
        shutdownLatch.await()
        info("Mirror maker thread shutdown complete")
      } catch {
        case _: InterruptedException =>
          warn("Shutdown of the mirror maker thread interrupted")
      }
    }
  }

  // Visible for testing
  private[tools] class ConsumerWrapper(private[tools] val consumer: Consumer[Array[Byte], Array[Byte]],
                                       customRebalanceListener: Option[ConsumerRebalanceListener],
                                       whitelistOpt: Option[String]) {
    val regex = whitelistOpt.getOrElse(throw new IllegalArgumentException("New consumer only supports whitelist."))
    var recordIter: java.util.Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = null

    // We manually maintain the consumed offsets for historical reasons and it could be simplified
    // Visible for testing
    private[tools] val offsets = new HashMap[TopicPartition, Long]()

    def init(): Unit = {
      debug("Initiating consumer")
      // 创建Consumer再平衡监听器
      val consumerRebalanceListener = new InternalRebalanceListener(this, customRebalanceListener)
      // 遍历所有的白名单
      whitelistOpt.foreach { whitelist =>
        try {
          // Consumer使用正则表达式和再平衡监听器订阅对应的主题
          consumer.subscribe(Pattern.compile(Whitelist(whitelist).regex), consumerRebalanceListener)
        } catch {
          case pse: RuntimeException =>
            error(s"Invalid expression syntax: $whitelist")
            throw pse
        }
      }
    }

    /**
     * 轮询获取消息
     * @return
     */
    def receive(): ConsumerRecord[Array[Byte], Array[Byte]] = {
      // 没有消息的时候，调用消费者的轮询方法
      if (recordIter == null || !recordIter.hasNext) {
        // 轮询1s，获取拉取的消息record集合
        // 在这种场景下，数据在offsetCommitIntervalMs没有到达，并且offsetCommitIntervalMs要比轮询的等待时间要短
        // 自上次轮询依赖，所有未提交的offset都会被延迟
        // 使用1s作为轮询的等待时间来确保offsetCommitIntervalMs如果大于1s，将不会再提交offset中看到延迟
        recordIter = consumer.poll(Duration.ofSeconds(1L)).iterator
        // 没有拉取到record则抛出NoRecordsException异常
        if (!recordIter.hasNext)
          throw new NoRecordsException
      }
      // 获取拉取回来的record内容，进行处理
      val record = recordIter.next()
      val tp = new TopicPartition(record.topic, record.partition)

      offsets.put(tp, record.offset + 1)
      record
    }

    def wakeup(): Unit = {
      consumer.wakeup()
    }

    def close(): Unit = {
      consumer.close()
    }

    def commit(): Unit = {
      consumer.commitSync(offsets.map { case (tp, offset) => (tp, new OffsetAndMetadata(offset)) }.asJava)
      offsets.clear()
    }
  }

  private class InternalRebalanceListener(consumerWrapper: ConsumerWrapper,
                                          customRebalanceListener: Option[ConsumerRebalanceListener])
    extends ConsumerRebalanceListener {

    override def onPartitionsLost(partitions: util.Collection[TopicPartition]): Unit = {}

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      producer.flush()
      commitOffsets(consumerWrapper)
      customRebalanceListener.foreach(_.onPartitionsRevoked(partitions))
    }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
      customRebalanceListener.foreach(_.onPartitionsAssigned(partitions))
    }
  }

  private[tools] class MirrorMakerProducer(val sync: Boolean, val producerProps: Properties) {

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def send(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (sync) {
        this.producer.send(record).get()
      } else {
        this.producer.send(record,
          new MirrorMakerProducerCallback(record.topic(), record.key(), record.value()))
      }
    }

    def flush(): Unit = {
      this.producer.flush()
    }

    def close(): Unit = {
      this.producer.close()
    }

    def close(timeout: Long): Unit = {
      this.producer.close(Duration.ofMillis(timeout))
    }
  }

  private class MirrorMakerProducerCallback(topic: String, key: Array[Byte], value: Array[Byte])
    extends ErrorLoggingCallback(topic, key, value, false) {

    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception != null) {
        // Use default call back to log error. This means the max retries of producer has reached and message
        // still could not be sent.
        super.onCompletion(metadata, exception)
        // If abort.on.send.failure is set, stop the mirror maker. Otherwise log skipped message and move on.
        if (abortOnSendFailure) {
          info("Closing producer due to send failure.")
          exitingOnSendFailure = true
          producer.close(0)
        }
        numDroppedMessages.incrementAndGet()
      }
    }
  }

  /**
   * If message.handler.args is specified. A constructor that takes in a String as argument must exist.
   */
  trait MirrorMakerMessageHandler {
    def handle(record: BaseConsumerRecord): util.List[ProducerRecord[Array[Byte], Array[Byte]]]
  }

  private[tools] object defaultMirrorMakerMessageHandler extends MirrorMakerMessageHandler {
    override def handle(record: BaseConsumerRecord): util.List[ProducerRecord[Array[Byte], Array[Byte]]] = {
      val timestamp: java.lang.Long = if (record.timestamp == RecordBatch.NO_TIMESTAMP) null else record.timestamp
      Collections.singletonList(new ProducerRecord(record.topic, null, timestamp, record.key, record.value, record.headers))
    }
  }

  // package-private for tests
  private[tools] class NoRecordsException extends RuntimeException

  class MirrorMakerOptions(args: Array[String]) extends CommandDefaultOptions(args) {

    val consumerConfigOpt = parser.accepts("consumer.config",
      "Embedded consumer config for consuming from the source cluster.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    parser.accepts("new.consumer",
      "DEPRECATED Use new consumer in mirror maker (this is the default so this option will be removed in " +
        "a future version).")

    val producerConfigOpt = parser.accepts("producer.config",
      "Embedded producer config.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val numStreamsOpt = parser.accepts("num.streams",
      "Number of consumption streams.")
      .withRequiredArg()
      .describedAs("Number of threads")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)

    val whitelistOpt = parser.accepts("whitelist",
      "Whitelist of topics to mirror.")
      .withRequiredArg()
      .describedAs("Java regex (String)")
      .ofType(classOf[String])

    val offsetCommitIntervalMsOpt = parser.accepts("offset.commit.interval.ms",
      "Offset commit interval in ms.")
      .withRequiredArg()
      .describedAs("offset commit interval in millisecond")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(60000)

    val consumerRebalanceListenerOpt = parser.accepts("consumer.rebalance.listener",
      "The consumer rebalance listener to use for mirror maker consumer.")
      .withRequiredArg()
      .describedAs("A custom rebalance listener of type ConsumerRebalanceListener")
      .ofType(classOf[String])

    val rebalanceListenerArgsOpt = parser.accepts("rebalance.listener.args",
      "Arguments used by custom rebalance listener for mirror maker consumer.")
      .withRequiredArg()
      .describedAs("Arguments passed to custom rebalance listener constructor as a string.")
      .ofType(classOf[String])

    val messageHandlerOpt = parser.accepts("message.handler",
      "Message handler which will process every record in-between consumer and producer.")
      .withRequiredArg()
      .describedAs("A custom message handler of type MirrorMakerMessageHandler")
      .ofType(classOf[String])

    val messageHandlerArgsOpt = parser.accepts("message.handler.args",
      "Arguments used by custom message handler for mirror maker.")
      .withRequiredArg()
      .describedAs("Arguments passed to message handler constructor.")
      .ofType(classOf[String])

    val abortOnSendFailureOpt = parser.accepts("abort.on.send.failure",
      "Configure the mirror maker to exit on a failed send.")
      .withRequiredArg()
      .describedAs("Stop the entire mirror maker when a send failure occurs")
      .ofType(classOf[String])
      .defaultsTo("true")

    options = parser.parse(args: _*)

    def checkArgs() = {
      CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt)
      val consumerProps = Utils.loadProps(options.valueOf(consumerConfigOpt))

      if (!options.has(whitelistOpt)) {
        error("whitelist must be specified")
        sys.exit(1)
      }

      if (!consumerProps.containsKey(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG))
        System.err.println("WARNING: The default partition assignment strategy of the mirror maker will " +
          "change from 'range' to 'roundrobin' in an upcoming release (so that better load balancing can be achieved). If " +
          "you prefer to make this switch in advance of that release add the following to the corresponding " +
          "config: 'partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor'")

      abortOnSendFailure = options.valueOf(abortOnSendFailureOpt).toBoolean
      offsetCommitIntervalMs = options.valueOf(offsetCommitIntervalMsOpt).intValue()
      val numStreams = options.valueOf(numStreamsOpt).intValue()

      Runtime.getRuntime.addShutdownHook(new Thread("MirrorMakerShutdownHook") {
        override def run(): Unit = {
          cleanShutdown()
        }
      })

      // create producer
      val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))
      val sync = producerProps.getProperty("producer.type", "async").equals("sync")
      producerProps.remove("producer.type")
      // Defaults to no data loss settings.
      maybeSetDefaultProperty(producerProps, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Int.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all")
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
      // Always set producer key and value serializer to ByteArraySerializer.
      producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producer = new MirrorMakerProducer(sync, producerProps)

      // Create consumers
      val customRebalanceListener: Option[ConsumerRebalanceListener] = {
        val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
        if (customRebalanceListenerClass != null) {
          val rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt)
          if (rebalanceListenerArgs != null)
            Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass, rebalanceListenerArgs))
          else
            Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass))
        } else {
          None
        }
      }
      val mirrorMakerConsumers = createConsumers(
        numStreams,
        consumerProps,
        customRebalanceListener,
        Option(options.valueOf(whitelistOpt)))

      // Create mirror maker threads.
      mirrorMakerThreads = (0 until numStreams) map (i =>
        new MirrorMakerThread(mirrorMakerConsumers(i), i))

      // Create and initialize message handler
      val customMessageHandlerClass = options.valueOf(messageHandlerOpt)
      val messageHandlerArgs = options.valueOf(messageHandlerArgsOpt)
      messageHandler = {
        if (customMessageHandlerClass != null) {
          if (messageHandlerArgs != null)
            CoreUtils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass, messageHandlerArgs)
          else
            CoreUtils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass)
        } else {
          defaultMirrorMakerMessageHandler
        }
      }
    }
  }

}
