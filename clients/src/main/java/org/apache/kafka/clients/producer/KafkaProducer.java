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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Kafka客户端发布record到Kafka集群
 * <P>
 * Kafka producer是线程安全的，并且共享了一个简单的producer实例来进行跨线程操作，这通常比多实例更快
 * <p>
 * 这有一个简单的使用producer的示例了来发送带有包含序号作为key/value的string信息的records
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * producer包含了用于持有records的buffer空间，这些records还没有被后台线程传输到server
 * 以及负责将这些records转换为请求并传输它们到集群的后台线程，如果在使用后无法关闭producer，则会泄露这些资源
 * <p>
 * {@link #send(ProducerRecord) send()}方法是异步的，调用此方法时，将会把record添加到缓存区域中的record追加队列中，并且立即返回
 * 这使生产者可以将record进行批处理以提高效率
 * <p>
 * code>acks</code>配置用于控制将请求视为完成的条件
 * "all"意味着在返回record的全局提交结果之前，将会一直处于阻塞状态，最慢但是最持久化的设定
 * <p>
 * 如果请求失败了，producer会自动进行重试，如果指定的<code>retries</code>为0，那么不会进行重试
 * 开启<code>retries</code>也意味着开启了重复的可能性（<a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a>提供了更多信息）
 * producer为每个partition均维护了一个未发送记录的缓冲区，用<code>batch.size</code>来配置缓冲区的大小
 * 增大batch大小可以进行更大的批处理，但是需要更多的内存（因为每个处于活跃的partition均有一个这样的缓冲区）
 * <p>
 * 默认情况下，即使缓冲区还有其他未使用的空间，缓冲区也可以立即发送
 * 然而，如果你需要减少请求的数量，你可以将<code>linger.ms</code>属性配置为＞0
 * 这个配置用于指定生产者进行等待的毫秒数，这样，更多的record会到达并填充同一个batch，这类似于Nagle在TCP中的算法
 * 举个例子，在上面的代码片段中，一个简单的请求将会发送近100个record，因为设置的<code>linger.ms</code>属性为1ms，如果没有填满缓冲区，则会增加1ms的延迟来等待更多的记录到达
 * 需要注意的是，时间仅靠的记录通常会与<code>linger.ms=0</code>一起进行batch操作，所有到高负载的情况下，批处理不管延迟配置，都会进行
 * 然而如果设置<code>linger.ms>0</code>，当不在最大负载的情况下，以少量的延迟为代价，将会有更少、更有效率的请求，
 * <p>
 * <code>buffer.memory</code>属性控制了producer用于缓冲的总内存大小，如果records的发送速度超过了records传输到服务端的速度，缓冲区将会被耗尽
 * 如果缓冲区域被耗尽，那么其他的send()方法调用将会被阻塞，如果等待时间超过了<code>max.block.ms</code>，那么将会抛出TimeoutException
 * <p>
 * <code>key.serializer</code>和<code>value.serializer</code>指示如何将用户通过其<code>ProducerRecord</code>提供的键值对转换为字节
 * 你可以使用内置的{@link org.apache.kafka.common.serialization.ByteArraySerializer}或者{@link org.apache.kafka.common.serialization.StringSerializer}用于简单的string或字节类型
 * <p>
 * 从Kafka 0.11开始，KafkaProducer提供了两种额外的模式，幂等的producer和事务producer
 * 幂等的producer将Kafka的语义从至少一次提高到一次
 * 事务的producer允许应用原子性的发送消息到多个partition或者topic上
 * </p>
 * <p>
 * 使用<code>enable.idempotence=true</code>来开启幂等策略
 * 如果开启了幂等策略，<code>retries</code>配置将会默认设置为<code>Integer.MAX_VALUE</code>，<code>acks</code>配置将会设置为<code>all</code>
 * 幂等性的producer没有任何的API变化，所有已有的应用不需要因为开启幂等策略导致需要修改代码
 * </p>
 * <p>
 * 利用幂等producer的策略，必须避免应用级别的重发送，因为无法进行重复数据删除
 * 就此而言，如果一个应用开启了幂等性，则无需设置<code>retries</code>配置，除此之外，如果{@link #send(ProducerRecord)}即使在无限重试的情况下返回了一个错误（例如，如果消息在发送之前已经在缓冲区过期）
 * 那么建议关闭producer，并检查最后生产的消息内容来确认没有重复的
 * </p>
 * 使用事务性的producer和附带的API，必须要设置<code>transactional.id</code>配置属性
 * 如果设置了<code>transactional.id</code>配置属性，幂等性将会随着producer配置自动开启，包含在事务中的topic需要配置为持久性
 * 通常，<code>replication.factor</code>需要配置为至少3个，<code>min.insync.replicas</code>ISR集合的大小需要设置为2
 * 最后，为了从端到端实现事务担保，consumers必须配置为仅读取已提交的消息
 * </p>
 * <p>
 * <code>transactional.id</code>目的是开启在跨单个生产者实例的多个会话的事务恢复，它通常从在分区的有状态的应用标识符派生
 * </p>
 * 所有的新事务API都是阻塞的，并且在失败时会抛出异常，下面是说明如何使用新API的示例，和上面的例子近似，除了所有的100条消息都是单个事务的一部分
 * </p>
 * <p>
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("transactional.id", "my-transactional-id");
 * Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 * producer.initTransactions();
 *
 * try {
 *     producer.beginTransaction();
 *     for (int i = 0; i < 100; i++)
 *         producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *     producer.commitTransaction();
 * } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *     // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *     producer.close();
 * } catch (KafkaException e) {
 *     // For all other exceptions, just abort the transaction and try again.
 *     producer.abortTransaction();
 * }
 * producer.close();
 * } </pre>
 * </p>
 * <p>
 * 如例子所示，每个producer仅会开启一个事务，在{@link #beginTransaction()}和{@link #commitTransaction()}之间发送的所有消息都是一个事务的组成部分
 * 如果指定了<code>transactional.id</code>，该producer发送的所有消息都会是事务的一部分
 * </p>
 * <p>
 * 事务producer使用异常来沟通错误状态
 * 通常，不需要为<code>producer.send()</code>或在返回的Future上调用get()指定回调方法
 * 如果<code>producer.send()</code>或者事务调用在事务中发生了不可恢复的错误，那么会抛出<code>KafkaException</code>
 * 请查阅{@link #send(ProducerRecord)}来获取更多有关从事务中检测错误的信息
 * </p>
 * </p>By calling
 * 通过调用<code>producer.abortTransaction()</code>收到<code>KafkaException</code>异常后，可以确认任何成功的写入均可以视为可中断的，因此需要保留事务担保
 * </p>
 * <p>
 * 在Kafka 0.10.0及更新的版本，客户端可以和broker进行通信，新旧版本的broker可能不支持某些客户端功能
 * 举个例子，事务API需要broker版本为0.11.0之上，如果你使用了broker不支持的API版本，你会收到一个<code>UnsupportedVersionException</code>异常
 * </p>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private final Logger log;
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    private final String clientId;
	/**
	 * 用于测试
	 */
    final Metrics metrics;
	/**
	 * 分区器
	 */
	private final Partitioner partitioner;

    private final int maxRequestSize;
    private final long totalMemorySize;
    private final ProducerMetadata metadata;
    private final RecordAccumulator accumulator;
	/**
	 * 处理生产请求的后台线程
	 */
	private final Sender sender;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;

	/**
	 * 使用键值对提供配置信息的生产者
	 * 值既可以是字符串，也可以是Object的泛型
	 * 创建一个KafkaProducer之后，你必须关闭它，避免产生资源泄露
	 * @param configs 生产者配置
	 */
    public KafkaProducer(final Map<String, Object> configs) {
        this(configs, null, null, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(configs, keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(propsToMap(properties), null, null, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(propsToMap(properties), keySerializer, valueSerializer, null, null, null,
                Time.SYSTEM);
    }

    // visible for testing
    @SuppressWarnings("unchecked")
    KafkaProducer(Map<String, Object> configs,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors interceptors,
                  Time time) {
        ProducerConfig config = new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer,
                valueSerializer));
        try {
            Map<String, Object> userProvidedConfigs = config.originals();
            this.producerConfig = config;
            this.time = time;
            String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            this.clientId = clientId;

            String transactionalId = userProvidedConfigs.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG) ?
                    (String) userProvidedConfigs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) : null;
            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));
            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class,
                    Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            reporters.add(new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time);
            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                                                                         Serializer.class);
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                                           Serializer.class);
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            // load interceptors and make sure they get clientId
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            ProducerConfig configWithClientId = new ProducerConfig(userProvidedConfigs, false);
            List<ProducerInterceptor<K, V>> interceptorList = (List) configWithClientId.getConfiguredInstances(
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptor.class);
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList);
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keySerializer,
                    valueSerializer, interceptorList, reporters);
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));

            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            this.transactionManager = configureTransactionState(config, logContext, log);
            int deliveryTimeoutMs = configureDeliveryTimeout(config, log);

            this.apiVersions = new ApiVersions();
            this.accumulator = new RecordAccumulator(logContext,
                    config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                    this.compressionType,
                    lingerMs(config),
                    retryBackoffMs,
                    deliveryTimeoutMs,
                    metrics,
                    PRODUCER_METRIC_GROUP_NAME,
                    time,
                    apiVersions,
                    transactionManager,
                    new BufferPool(this.totalMemorySize, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG), metrics, time, PRODUCER_METRIC_GROUP_NAME));
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG));
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                this.metadata = new ProducerMetadata(retryBackoffMs,
                        config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                        logContext,
                        clusterResourceListeners,
                        Time.SYSTEM);
                this.metadata.bootstrap(addresses, time.milliseconds());
            }
            this.errors = this.metrics.sensor("errors");
            this.sender = newSender(logContext, kafkaClient, this.metadata);
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();
            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(Duration.ofMillis(0), true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    // visible for testing
    Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
        int maxInflightRequests = configureInflightRequests(producerConfig, transactionManager != null);
        int requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(producerConfig, time);
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        KafkaClient client = kafkaClient != null ? kafkaClient : new NetworkClient(
                new Selector(producerConfig.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                        this.metrics, time, "producer", channelBuilder, logContext),
                metadata,
                clientId,
                maxInflightRequests,
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                producerConfig.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                producerConfig.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                requestTimeoutMs,
                ClientDnsLookup.forConfig(producerConfig.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                time,
                true,
                apiVersions,
                throttleTimeSensor,
                logContext);
        int retries = configureRetries(producerConfig, transactionManager != null, log);
        short acks = configureAcks(producerConfig, transactionManager != null, log);
        return new Sender(logContext,
                client,
                metadata,
                this.accumulator,
                maxInflightRequests == 1,
                producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                acks,
                retries,
                metricsRegistry.senderMetrics,
                time,
                requestTimeoutMs,
                producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                this.transactionManager,
                apiVersions);
    }

    private static int lingerMs(ProducerConfig config) {
        return (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), Integer.MAX_VALUE);
    }

    private static int configureDeliveryTimeout(ProducerConfig config, Logger log) {
        int deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        int lingerMs = lingerMs(config);
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int lingerAndRequestTimeoutMs = (int) Math.min((long) lingerMs + requestTimeoutMs, Integer.MAX_VALUE);

        if (deliveryTimeoutMs < Integer.MAX_VALUE && deliveryTimeoutMs < lingerAndRequestTimeoutMs) {
            if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                // throw an exception if the user explicitly set an inconsistent value
                throw new ConfigException(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                    + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                    + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            } else {
                // override deliveryTimeoutMs default value to lingerMs + requestTimeoutMs for backward compatibility
                deliveryTimeoutMs = lingerAndRequestTimeoutMs;
                log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
            }
        }
        return deliveryTimeoutMs;
    }

    private static TransactionManager configureTransactionState(ProducerConfig config, LogContext logContext, Logger log) {

        TransactionManager transactionManager = null;

        boolean userConfiguredIdempotence = false;
        if (config.originals().containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))
            userConfiguredIdempotence = true;

        boolean userConfiguredTransactions = false;
        if (config.originals().containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
            userConfiguredTransactions = true;

        boolean idempotenceEnabled = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);

        if (!idempotenceEnabled && userConfiguredIdempotence && userConfiguredTransactions)
            throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");

        if (userConfiguredTransactions)
            idempotenceEnabled = true;

        if (idempotenceEnabled) {
            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            transactionManager = new TransactionManager(logContext, transactionalId, transactionTimeoutMs, retryBackoffMs);
            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        }

        return transactionManager;
    }

    private static int configureRetries(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredRetries = false;
        if (config.originals().containsKey(ProducerConfig.RETRIES_CONFIG)) {
            userConfiguredRetries = true;
        }
        if (idempotenceEnabled && !userConfiguredRetries) {
            // We recommend setting infinite retries when the idempotent producer is enabled, so it makes sense to make
            // this the default.
            log.info("Overriding the default retries config to the recommended value of {} since the idempotent " +
                    "producer is enabled.", Integer.MAX_VALUE);
            return Integer.MAX_VALUE;
        }
        if (idempotenceEnabled && config.getInt(ProducerConfig.RETRIES_CONFIG) == 0) {
            throw new ConfigException("Must set " + ProducerConfig.RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
        }
        return config.getInt(ProducerConfig.RETRIES_CONFIG);
    }

    private static int configureInflightRequests(ProducerConfig config, boolean idempotenceEnabled) {
        if (idempotenceEnabled && 5 < config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            throw new ConfigException("Must set " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " to at most 5" +
                    " to use the idempotent producer.");
        }
        return config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    }

    private static short configureAcks(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredAcks = false;
        short acks = (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG));
        if (config.originals().containsKey(ProducerConfig.ACKS_CONFIG)) {
            userConfiguredAcks = true;
        }

        if (idempotenceEnabled && !userConfiguredAcks) {
            log.info("Overriding the default {} to all since idempotence is enabled.", ProducerConfig.ACKS_CONFIG);
            return -1;
        }

        if (idempotenceEnabled && acks != -1) {
            throw new ConfigException("Must set " + ProducerConfig.ACKS_CONFIG + " to all in order to use the idempotent " +
                    "producer. Otherwise we cannot guarantee idempotence.");
        }
        return acks;
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     *
     * This method does the following:
     *   1. Ensures any transactions initiated by previous instances of the producer with the same
     *      transactional.id are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.
     *   2. Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.
     *
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot
     * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
     * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
     * initialized, this method should no longer be used.
     *
     * @throws IllegalStateException if no transactional.id has been configured
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void initTransactions() {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        TransactionalRequestResult result = transactionManager.initializeTransactions();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     *
     * @throws IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        transactionManager.beginTransaction();
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
     * {@link KafkaConsumer consumer}. Note, that the consumer should have {@code enable.auto.commit=false}
     * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException  fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);
        sender.wakeup();
        result.await();
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
     *
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     *
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be committed before expiration
     * of {@code max.block.ms}. Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as abortTransaction)
     * since the commit may already be in the progress of completing. If not retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * @throws TimeoutException if the time taken for committing the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made.
     * This call will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link ProducerFencedException} or an instance of {@link org.apache.kafka.common.errors.AuthorizationException}.
     *
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be aborted before expiration
     * of {@code max.block.ms}. Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as commitTransaction)
     * since the abort may already be in the progress of completing. If not retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for aborting the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void abortTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        TransactionalRequestResult result = transactionManager.beginAbort();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
    }

	/**
	 * 异步发送记录到topic，相当于#send(record, null)方法
	 * {@link #send(ProducerRecord, Callback)}
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

	/**
	 * 异步发送记录到topic，在有回调时，调用提供的callback方法
	 *
	 * 发送的动作是异步的，当记录已经存储在等待发送的记录buffer中时，方法就会返回结果，所以它允许一次并发发送多条记录，避免因为等待每一条记录的返回结果而阻塞
	 *
	 * 发送消息返回的结果是{@link RecordMetadata}，它指定了记录被发送到了哪个分区
	 * 时间戳既可以是开发者提供的时间戳，也可以是记录发送的时间戳
	 * 如果使用了{@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}，时间戳将会采用Kafka Broker的本地时间
	 *
	 * 发送消息的返回结果是{@link Future}，存储了{@link RecordMetadata}，调用{@link java.util.concurrent.Future#get()}方法会一直阻塞到请求完成并返回，或者抛出异常
	 * 我们可以使用{@link Callback}进行同步非阻塞的使用，当请求完成时，就会执行{@link Callback}
	 *
	 * 发送至相同分区的记录，callback的是按照发送的优先级顺序执行的，
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
	 * }
	 * 我们使用了一部分的事务，用声明一个callback，或者检查Future#get()返回值方式来发现send()方法的错误是没有必要的，如果send()方法的调用，产生了一个无法恢复的错误
	 * 最后再调用{@link #commitTransaction()}时会失败并抛出异常
	 * 当出现{@link #commitTransaction()}调用异常时，我们应该重置状态并重新发送消息
	 *
	 * 一些事务发送错误并不能通过调用{@link #abortTransaction()}来解决，尤其是在一个事务发送结束于一连串的错误异常，那么此时剩下的唯一解决方案就是调用close()方法，将KafkaProducer进行关闭
	 * 致命错误可能会导致producer进入一个不再起作用的状态，继续调用future的api，将会导致产生更多的相同的错误
	 *
	 * 可能会存在已经开启幂等，但是transactional.id还是没有配置的情况
	 * 在这种情况下，{@link UnsupportedVersionException}、{@link AuthorizationException}被认为是致命错误
	 * 然而{@link ProducerFencedException}不需要进行处理。除此之外，也可以在收到{@link OutOfOrderSequenceException}后继续发送记录
	 * 但是这样做可能会导致挂起的消息，处理的不正确
	 * 为了确保正确的排序，我们需要重新实例化一个producer实例
	 *
	 * 如果目标topic的消息格式没有升级到0.11.0.0版本，幂等和事务的特性，将会因为{@link UnsupportedForMessageFormatException}而失效
	 * 如果是在事务过程中出现了异常错误，可以中断然后继续，但是需要注意的是，直到升级topic之前，我们还是会收到相同的异常
	 *
	 * 需要注意的是，callback的执行，将会依赖与producer的io线程，所以callback的执行应该很快，否则就会阻塞后续的其他线程的执行
	 * 如果你想要执行一些阻塞，或者具有昂贵计算代价的callback，建议使用线程池来执行
	 *
	 * @param record 需要发送的记录
	 * @param callback 用户提供的callback，在记录被服务端接收之后执行，null意味着没有callback
	 *
	 * @throws AuthenticationException 身份验证失败
	 * @throws AuthorizationException producer不允许写入
	 * @throws IllegalStateException 如果已经配置了一个transaction.id，但是没有任何事务启动
	 * 								 或者在producer已经关闭的情况下，继续调用send()方法
	 * @throws InterruptException Thread在阻塞过程中被中断
	 * @throws SerializationException key或者value不是给定配置的序列化器的合法对象
	 * @throws TimeoutException 取回元数据超过了max.block.ms
	 * 						    获取为记录申请的内存已经超过阈值
	 * @throws KafkaException 发生了非具体制定的Kafka异常
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
		// 拦截记录，进行一些Kafka的修改
		// 这个方法不会抛出异常
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
	}

	/**
	 * 验证producer是否已经被关闭
	 * @throws IllegalStateException 在producer关闭的情况下抛出异常
	 */
    private void throwIfProducerClosed() {
        if (sender == null || !sender.isRunning())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

	/**
	 * 异步发送消息
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
		try {
			// producer状态校验
            throwIfProducerClosed();
			// 首先确认topic的元数据是可用的
            ClusterAndWaitTime clusterAndWaitTime;
            try {
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
			}
			// 计算剩余的等待时间
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
			// 获取集群信息
            Cluster cluster = clusterAndWaitTime.cluster;
            byte[] serializedKey;
			try {
				// 对记录的topic、header、key使用keySerializer进行序列化
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            byte[] serializedValue;
			try {
				// 使用valueSerializer对record的topic、header、value进行序列化
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
			}
			// 获取消息将要发往的分区编号
            int partition = partition(record, serializedKey, serializedValue, cluster);
			// 创建将要发往的分区信息
            tp = new TopicPartition(record.topic(), partition);
			// 指定记录，只读设置
            setReadOnly(record.headers());
			// 获取请求的Header信息
            Header[] headers = record.headers().toArray();
			// 获取序列化之后，整个记录的大小
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(),
                    compressionType, serializedKey, serializedValue, headers);
			// 确保记录的大小处于有效范围内
            ensureValidRecordSize(serializedSize);
			// 记录的时间戳配置
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            if (log.isTraceEnabled()) {
                log.trace("Attempting to append record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
			}
			// 创建拦截器的callback，其中会包装我们自定义的callback
            Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);
			// 如果开启了事务，会对事务需要的状态进行校验
            if (transactionManager != null && transactionManager.isTransactional()) {
                transactionManager.failIfNotReadyForSend();
			}
			// 尝试record累加器中追加record，获取追加结果
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
                    serializedValue, headers, interceptCallback, remainingWaitMs, true);
			// 如果是因为需要创建新batch而中断追加
            if (result.abortForNewBatch) {
				// 上一次计算的分区索引
                int prevPartition = partition;
				// 在分区上创建新的粘性分区
                partitioner.onNewBatch(record.topic(), cluster, prevPartition);
				// 创建新的分区，返回的是新分区的编号
                partition = partition(record, serializedKey, serializedValue, cluster);
				// 新的分区
                tp = new TopicPartition(record.topic(), partition);
                if (log.isTraceEnabled()) {
                    log.trace("Retrying append due to new batch creation for topic {} partition {}. The old partition was {}", record.topic(), partition, prevPartition);
				}
				// 创建拦截器callback和开发者自定义callback
                interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);
				// 继续追加一次record
                result = accumulator.append(tp, timestamp, serializedKey,
                    serializedValue, headers, interceptCallback, remainingWaitMs, false);
			}
			// 如果需要进行事务
            if (transactionManager != null && transactionManager.isTransactional())
				// 将分区添加到事务中
                transactionManager.maybeAddPartitionToTransaction(tp);
			// 如果追加的结果是batch已经满了，或者新的batch已经创建
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
				// 唤醒sender线程，发送缓冲区存储的record
                this.sender.wakeup();
			}
			// 返回追加的任务
            return result.future;
			// 处理异常并记录错误
			// 对于API型异常，将在future中进行返回
			// 其他异常，将直接抛出异常
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

	/**
	 * 等待集群的元数据，包括给定的topic的分区是否是可用的
	 * @param topic 我们想要获取元数据的来源topic
	 * @param partition 一个希望存在元数据的分区，如果没有指定，则为null
	 * @param maxWaitMs 等待元数据的最大时间
	 * @return 集群及包含topic元数据的信息，以及等待获取元数据的实际时间
	 * @throws KafkaException 执行异常
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {
		// add topic to metadata topic list if it is not there already and reset expiry
		// 将指定topic添加到topic元数据列表中
		// 如果不存在指定的topic，会重新更新它的失效时间
        Cluster cluster = metadata.fetch();

		// topic是不可用的
        if (cluster.invalidTopics().contains(topic))
            throw new InvalidTopicException(topic);

		// 添加topic
        metadata.add(topic);
		// 获取指定topic的partition数量
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
		// 返回缓存元数据，如果指定记录的partition既没有声明，或者在已知的partition范围内
		// 返回集群等待时间为0的ClusterAndWaitTime
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
			// 返回新的集群元数据及等待时间
            return new ClusterAndWaitTime(cluster, 0);

		// 当前时间节点
        long begin = time.milliseconds();
		// 持续等待时间
        long remainingWaitMs = maxWaitMs;
        long elapsed;
		// 持续发送metadata请求，自导我们获取到指定topic的metadata
        // Issue metadata requests until we have metadata for the topic and the requested partition,
        // or until maxWaitTimeMs is exceeded. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        do {
            if (partition != null) {
                log.trace("Requesting metadata update for partition {} of topic {}.", partition, topic);
            } else {
                log.trace("Requesting metadata update for topic {}.", topic);
			}
			// 向元数据中添加topic信息
            metadata.add(topic);
			// 更新集群信息
            int version = metadata.requestUpdate();
			// 唤醒select线程模型
            sender.wakeup();
			try {
				// 等待更新元数据
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                throw new TimeoutException(
                        String.format("Topic %s not present in metadata after %d ms.",
                                topic, maxWaitMs));
			}
			// 从缓存中获取cluster信息
            cluster = metadata.fetch();
			// 计算等待时间
            elapsed = time.milliseconds() - begin;
			// 如果已经超出等待时间，则直接抛出异常
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException(partitionsCount == null ?
                        String.format("Topic %s not present in metadata after %d ms.",
                                topic, maxWaitMs) :
                        String.format("Partition %d of topic %s with partition count %d is not present in metadata after %d ms.",
                                partition, topic, partitionsCount, maxWaitMs));
            }
            metadata.maybeThrowExceptionForTopic(topic);
            remainingWaitMs = maxWaitMs - elapsed;
			// 获取指定topic的partition的总数量
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null || (partition != null && partition >= partitionsCount));

        return new ClusterAndWaitTime(cluster, elapsed);
    }

	/**
	 * 校验记录的总大小
     */
    private void ensureValidRecordSize(int size) {
		// 请求最大大小校验
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the maximum request size you have configured with the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                    " configuration.");
		// 总共占用内存大小校验
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()} will
     * flush all buffered records before performing the commit. This ensures that all the {@link #send(ProducerRecord)}
     * calls made since the previous {@link #beginTransaction()} are completed before the commit.
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately. It will also abort the ongoing transaction if it's not
     * already completing.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(Duration.ofMillis(0))</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     *
     */
    @Override
    public void close(Duration timeout) {
        close(timeout, false);
    }

    private void close(Duration timeout, boolean swallowException) {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeoutMs);

        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeoutMs > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.",
                        timeoutMs);
            } else {
                // Try to close gracefully.
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeoutMs);
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeoutMs);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        Utils.closeQuietly(interceptors, "producer interceptors", firstException);
        Utils.closeQuietly(metrics, "producer metrics", firstException);
        Utils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        Utils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        Utils.closeQuietly(partitioner, "producer partitioner", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
        log.debug("Kafka producer has been closed");
    }

    private static Map<String, Object> propsToMap(Properties properties) {
        Map<String, Object> map = new HashMap<>(properties.size());
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getKey() instanceof String) {
                String k = (String) entry.getKey();
                map.put(k, properties.get(k));
            } else {
                throw new ConfigException(entry.getKey().toString(), entry.getValue(), "Key must be a string.");
            }
        }
        return map;
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer, Serializer<V> valueSerializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

	/**
	 * 为指定的记录计算分区索引
	 * 开发者指定了分区，则使用指定的分区
	 * 如果没有指定分区，使用开发者指定的策略来计算分区，默认的分区策略时DefaultPartitioner
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
		// 如果开发者已经指定了的分区编号，使用自定义的分区编号
		// 否则kafka会自己选择一个分区编号
        Integer partition = record.partition();
        return partition != null ?
                partition :
                partitioner.partition(
                        record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    // Visible for testing
    String getClientId() {
        return clientId;
    }

    private static class ClusterAndWaitTime {
		/**
		 * 集群
		 */
		final Cluster cluster;
		/**
		 * 等待元数据的时间
		 */
		final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

	/**
	 * 在请求完成之后的callback任务，它会反过来调用开发者提供的callback，然后通知producer的拦截器，请求已经完成了
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        private InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors, TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            metadata = metadata != null ? metadata : new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, Long.valueOf(-1L), -1, -1);
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}
