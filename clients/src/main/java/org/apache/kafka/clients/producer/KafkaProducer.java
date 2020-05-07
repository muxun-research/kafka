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
 * Kafka客户端，用于向Kafka集群发布record
 * <p>
 * Producer是一个线程安全，并且跨线程共享单个生产者实例，这要比多实例更快
 * <p>
 * 这有一个使用producer来发送string record的简单示例，包含有序数字作为键值对
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
 * Producer由持有record的缓冲池组成，缓冲池中的record还没有传输到Kafka集群
 * 后台线程负责将record组装成request请求，然后将请求发送到Kafka集群
 * 如果在使用后无法正常关闭Producer，将会泄露这些record
 * <p>
 * {@link #send(ProducerRecord) send()}方法是异步的
 * 我们调用这个方法时，其实它是将我们的内容组装成一个record，然后将record添加到缓冲池中，相当于是一个追加的行为，此时会立即返回追加的结果
 * 这样做，Producer可以将record作为集合，分批次处理，提高效率
 * <p>
 * <code>acks</code>配置，用于控制请求完成的标准
 * * all: 所有Kafka集群节点都需要接收到这个record，最慢但是持久化最强的配置
 * <p>
 * 如果请求失败，Producer会自动进行重试，除非由于我们已经指定了<code>retries</code>次数为0
 * 允许重试，同时也意味着开启了record重复的可能性
 * <p>
 * Producer保存了每个partition没有发送的record的缓存，每个缓存的大小由<code>batch.size</code>大小进行配置
 * 配置的缓冲大小越大，会带来更多的批处理，但需要的内存越多(因为我们通常会为每个活跃partition发送这些的缓存)
 * <p>
 * 默认地，即使缓存仍有未使用的空间，缓存在可以发送的情况下，也会发送
 * 然而，如果你需要减少请求的次数，你可以将<code>linger.ms</code>配置设置为超过0的值
 * 这个配置会命令Producer等待<code>linger.ms</code>的时间，然后再发送请求，这样可以让更多的record填充batch
 * 这类似于TCP中的Nagle的算法
 * 举例，在上面的代码片段中，我们将会发送100个record，但是我们设置了<code>linger.ms</code>时间为1ms，所有100个record会在一个请求中发送
 * 然而我们如果没有填充满缓存，那么<code>linger.ms</code>会让Producer等待1ms来获取更多的record
 * 注意，时间接近的record，通常会与<code>linger.ms=0</code>的record一起进行批处理，因此在高负载的情况下，无论是否进行了延迟配置，都会进行批处理
 * 然而，在非高负载的情况下，如果配置<code>linger.ms>0</code>，可以带来更少，更高效的请求，代价仅仅是少量的等待时间
 * <p>
 * <code>buffer.memory</code>配置用于控制Producer可以进行缓存的内存大小
 * 如果record发送的要比传输到Kafka集群的速度要快，那么这部分缓存将会被耗尽
 * 如果缓存已满，那么其他的{@link #send(ProducerRecord) send()}方法调用将会被阻塞
 * 我们可以设置Producer的阻塞时间，配置项为<code>max.block.ms</code>，超过设置的等待时间吗，将会抛出TimeoutException异常
 * <p>
 * <code>key.serializer</code>和<code>value.serializer</code>用于命令如何将使用者通过<code>ProducerRecord</code>提供的键值对，转化为字节
 * 你可以使用Kafka内置的{@link org.apache.kafka.common.serialization.ByteArraySerializer}或{@link org.apache.kafka.common.serialization.StringSerializer}
 * 用作简单的string或字节类型
 * <p>
 * 从Kafka 0.11版本开始，KafkaProducer提供了两种额外的模式，幂等Producer和事务Producer
 * 幂等Producer将Kafka的发送语义，从至少一次提升到了准确一次，尤其是在Producer在进行重试时，不会产生重复的现象
 * 事务Producer允许应用原子性地向多个partition或者topic发送消息
 * </p>
 * <p>
 * 通过<code>enable.idempotence=true</code>配置开启Producer的幂等特性
 * 如果开启幂等特性，那么会自动设置为<code>retries=Integer.MAX_VALUE</code>，<code>acks=all</code>
 * 幂等Producer的API没有任何变化，所有使用这个特性的应用不用做出增量改变
 * </p>
 * <p>
 * 如果要使用幂等Producer，就需要避免应用程序级别的重发送，因为幂等性保证的就是消息不重复
 * 就此而言，如果应用允许幂等，推荐不对<code>retries</code>进行配置，使<code>retries</code>默认为<code>Integer.MAX_VALUE</code>
 * 除此之外，如果{@link #send(ProducerRecord)}方法在无限重试的情况下返回了错误(举例：如果消息在等待发送时已经过期)
 * 推荐关闭Producer并检查最后一次生产的消息，来确认是否是重复的
 * 最后，幂等Producer仅能够保证在一个会话中的消息是幂等的
 * </p>
 * 如果需要使用事务Producer和附属API，你必须要设置<code>transactional.id</code>配置属性
 * 如果设置了<code>transactional.id</code>配置，就会一并的开启幂等的特性，因为Kafka的事务特性需要依赖幂等特性
 * 更远地，处于事务中的topic也应该配置为持久化的。
 * 尤其， topic的集群配置<code>replication.factor</code>配置必须至少是3，<code>min.insync.replicas</code>最小ISR集合副本节点数量必须为2
 * 最后，为了从头到尾实现事务特性，Consumer也必须要配置为仅允许获取已提交的消息
 * </p>
 * <p>
 * 使用<code>transactional.id</code>目的在于在同一个Producer实例下的多个会话中允许事务恢复
 * 通常从分区的，稳定的应用中的分片符中派生
 * 就此而言，<code>transactional.id</code>对于每个在分区应用程序中运行的Producer实例来说必须是唯一的
 * </p>
 * 所有的新事务API都是阻塞的，并且在失败的情况下会抛出异常
 * 下面的实例向大家展示了新API的使用方式，其实和上面的API示例使用类型，区别在于100条消息是一个事务的一部分
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
 * 如示例所示，每个Producer仅允许开启一个事务，所有在{@link #beginTransaction()}和{@link #commitTransaction()}之间发送的消息都属于一个事务的一部分
 * 当指定<code>transactional.id</code>时，Producer发送的所有消息都会属于同一个事务
 * </p>
 * <p>
 * 事务Producer使用异常来传递错误状态，尤其，不要为<code>producer.send()</code>方法指定回调方法，或者在返回的Future对象上调用<code>.get()</code>方法
 * 如果<code>producer.send()</code>方法或者事务调用在事务中遇到一个不可恢复的错误，会抛出<code>KafkaException</code>异常
 * 查看{@link #send(ProducerRecord)}方法的文档来获取更多的有关从事务发送中的发现错误的详细信息
 * </p>
 * </p>By calling
 * 通过调用<code>producer.abortTransaction()</code>之上收到的<code>KafkaException</code>异常，我们可以确认
 * 任何成功的写入被标记为终端，因此保留事务担保
 * </p>
 * <p>
 * 在Broker 0.10.0及之上的版本，Producer可以和Broker进行交流，但是比0.10.0低的版本，Broker可能不支持这个功能
 * 举例：事务API需要Broker的版本在0.11.0版本以上
 * 如果你在调用事务API时，Broker的版本不符合API的规定，你的Producer将会收到<code>UnsupportedVersionException</code>异常
 * </p>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

	private final Logger log;
	/**
	 * Producer客户端ID初始化序号
	 */
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
	/**
	 * record累加器
	 */
	private final RecordAccumulator accumulator;
	/**
	 * 处理生产请求的后台线程
	 */
	private final Sender sender;
	private final Thread ioThread;
	/**
	 * 消息压缩类型
	 */
	private final CompressionType compressionType;
	private final Sensor errors;
	private final Time time;
	/**
	 * 消息key序列化器
	 */
	private final Serializer<K> keySerializer;
	/**
	 * 消息value序列化器
	 */
	private final Serializer<V> valueSerializer;
	/**
	 * Producer配置
	 */
	private final ProducerConfig producerConfig;
	/**
	 * 最长等待时间
	 */
	private final long maxBlockTimeMs;
	private final ProducerInterceptors<K, V> interceptors;
	/**
	 * API版本信息
	 */
	private final ApiVersions apiVersions;
	/**
	 * Producer的事务管理器
	 */
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
	 * @param configs         The producer configs
	 * @param keySerializer   The serializer for key that implements {@link Serializer}. The configure() method won't be
	 *                        called in the producer when the serializer is passed in directly.
	 * @param valueSerializer The serializer for value that implements {@link Serializer}. The configure() method won't
	 *                        be called in the producer when the serializer is passed in directly.
	 */
	public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		this(configs, keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
	}

	/**
	 * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
	 * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
	 * <p>
	 * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
	 * @param properties The producer configs
	 */
	public KafkaProducer(Properties properties) {
		this(propsToMap(properties), null, null, null, null, null, Time.SYSTEM);
	}

	/**
	 * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
	 * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
	 * <p>
	 * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
	 * @param properties      The producer configs
	 * @param keySerializer   The serializer for key that implements {@link Serializer}. The configure() method won't be
	 *                        called in the producer when the serializer is passed in directly.
	 * @param valueSerializer The serializer for value that implements {@link Serializer}. The configure() method won't
	 *                        be called in the producer when the serializer is passed in directly.
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
		// 配置的发送请求超时时间
		int requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
		// 构建
		ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(producerConfig, time);
		// 生产者的计数器
		ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
		Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
		// 如果已有和Kafka服务端建立连接的客户端，则使用此客户端，否则新创建一个客户端
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
		// 是否需要进行重试
		int retries = configureRetries(producerConfig, transactionManager != null, log);
		// 发送请求需要的Kafka服务端副本的响应数量
		short acks = configureAcks(producerConfig, transactionManager != null, log);
		// 构建发送Sender线程
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
	 * <p>
	 * This method does the following:
	 * 1. Ensures any transactions initiated by previous instances of the producer with the same
	 * transactional.id are completed. If the previous instance had failed with a transaction in
	 * progress, it will be aborted. If the last transaction had begun completion,
	 * but not yet finished, this method awaits its completion.
	 * 2. Gets the internal producer id and epoch, used in all future transactional
	 * messages issued by the producer.
	 * <p>
	 * Note that this method will raise {@link TimeoutException} if the transactional state cannot
	 * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
	 * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
	 * initialized, this method should no longer be used.
	 * @throws IllegalStateException                                      if no transactional.id has been configured
	 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
	 *                                                                    does not support transactions (i.e. if its version is lower than 0.11.0.0)
	 * @throws org.apache.kafka.common.errors.AuthorizationException      fatal error indicating that the configured
	 *                                                                    transactional.id is not authorized. See the exception for more details
	 * @throws KafkaException                                             if the producer has encountered a previous fatal error or for any other unexpected error
	 * @throws TimeoutException                                           if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
	 * @throws InterruptException                                         if the thread is interrupted while blocked
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
	 * @throws IllegalStateException                                      if no transactional.id has been configured or if {@link #initTransactions()}
	 *                                                                    has not yet been invoked
	 * @throws ProducerFencedException                                    if another producer with the same transactional.id is active
	 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
	 *                                                                    does not support transactions (i.e. if its version is lower than 0.11.0.0)
	 * @throws org.apache.kafka.common.errors.AuthorizationException      fatal error indicating that the configured
	 *                                                                    transactional.id is not authorized. See the exception for more details
	 * @throws KafkaException                                             if the producer has encountered a previous fatal error or for any other unexpected error
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
	 * @throws IllegalStateException                                               if no transactional.id has been configured or no transaction has been started
	 * @throws ProducerFencedException                                             fatal error indicating another producer with the same transactional.id is active
	 * @throws org.apache.kafka.common.errors.UnsupportedVersionException          fatal error indicating the broker
	 *                                                                             does not support transactions (i.e. if its version is lower than 0.11.0.0)
	 * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
	 *                                                                             format used for the offsets topic on the broker does not support transactions
	 * @throws org.apache.kafka.common.errors.AuthorizationException               fatal error indicating that the configured
	 *                                                                             transactional.id is not authorized. See the exception for more details
	 * @throws KafkaException                                                      if the producer has encountered a previous fatal or abortable error, or for any
	 *                                                                             other unexpected error
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
	 * <p>
	 * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
	 * errors, this method will throw the last received exception immediately and the transaction will not be committed.
	 * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
	 * <p>
	 * Note that this method will raise {@link TimeoutException} if the transaction cannot be committed before expiration
	 * of {@code max.block.ms}. Additionally, it will raise {@link InterruptException} if interrupted.
	 * It is safe to retry in either case, but it is not possible to attempt a different operation (such as abortTransaction)
	 * since the commit may already be in the progress of completing. If not retrying, the only option is to close the producer.
	 * @throws IllegalStateException                                      if no transactional.id has been configured or no transaction has been started
	 * @throws ProducerFencedException                                    fatal error indicating another producer with the same transactional.id is active
	 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
	 *                                                                    does not support transactions (i.e. if its version is lower than 0.11.0.0)
	 * @throws org.apache.kafka.common.errors.AuthorizationException      fatal error indicating that the configured
	 *                                                                    transactional.id is not authorized. See the exception for more details
	 * @throws KafkaException                                             if the producer has encountered a previous fatal or abortable error, or for any
	 *                                                                    other unexpected error
	 * @throws TimeoutException                                           if the time taken for committing the transaction has surpassed <code>max.block.ms</code>.
	 * @throws InterruptException                                         if the thread is interrupted while blocked
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
	 * <p>
	 * Note that this method will raise {@link TimeoutException} if the transaction cannot be aborted before expiration
	 * of {@code max.block.ms}. Additionally, it will raise {@link InterruptException} if interrupted.
	 * It is safe to retry in either case, but it is not possible to attempt a different operation (such as commitTransaction)
	 * since the abort may already be in the progress of completing. If not retrying, the only option is to close the producer.
	 * @throws IllegalStateException                                      if no transactional.id has been configured or no transaction has been started
	 * @throws ProducerFencedException                                    fatal error indicating another producer with the same transactional.id is active
	 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
	 *                                                                    does not support transactions (i.e. if its version is lower than 0.11.0.0)
	 * @throws org.apache.kafka.common.errors.AuthorizationException      fatal error indicating that the configured
	 *                                                                    transactional.id is not authorized. See the exception for more details
	 * @throws KafkaException                                             if the producer has encountered a previous fatal error or for any other unexpected error
	 * @throws TimeoutException                                           if the time taken for aborting the transaction has surpassed <code>max.block.ms</code>.
	 * @throws InterruptException                                         if the thread is interrupted while blocked
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
	 * <p>
	 * 发送的动作是异步的，当记录已经存储在等待发送的记录buffer中时，方法就会返回结果，所以它允许一次并发发送多条记录，避免因为等待每一条记录的返回结果而阻塞
	 * <p>
	 * 发送消息返回的结果是{@link RecordMetadata}，它指定了记录被发送到了哪个分区
	 * 时间戳既可以是开发者提供的时间戳，也可以是记录发送的时间戳
	 * 如果使用了{@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}，时间戳将会采用Kafka Broker的本地时间
	 * <p>
	 * 发送消息的返回结果是{@link Future}，存储了{@link RecordMetadata}，调用{@link java.util.concurrent.Future#get()}方法会一直阻塞到请求完成并返回，或者抛出异常
	 * 我们可以使用{@link Callback}进行同步非阻塞的使用，当请求完成时，就会执行{@link Callback}
	 * <p>
	 * 发送至相同分区的记录，callback的是按照发送的优先级顺序执行的，
	 * {@code
	 * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
	 * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
	 * }
	 * 我们使用了一部分的事务，用声明一个callback，或者检查Future#get()返回值方式来发现send()方法的错误是没有必要的，如果send()方法的调用，产生了一个无法恢复的错误
	 * 最后再调用{@link #commitTransaction()}时会失败并抛出异常
	 * 当出现{@link #commitTransaction()}调用异常时，我们应该重置状态并重新发送消息
	 * <p>
	 * 一些事务发送错误并不能通过调用{@link #abortTransaction()}来解决，尤其是在一个事务发送结束于一连串的错误异常，那么此时剩下的唯一解决方案就是调用close()方法，将KafkaProducer进行关闭
	 * 致命错误可能会导致producer进入一个不再起作用的状态，继续调用future的api，将会导致产生更多的相同的错误
	 * <p>
	 * 可能会存在已经开启幂等，但是transactional.id还是没有配置的情况
	 * 在这种情况下，{@link UnsupportedVersionException}、{@link AuthorizationException}被认为是致命错误
	 * 然而{@link ProducerFencedException}不需要进行处理。除此之外，也可以在收到{@link OutOfOrderSequenceException}后继续发送记录
	 * 但是这样做可能会导致挂起的消息，处理的不正确
	 * 为了确保正确的排序，我们需要重新实例化一个producer实例
	 * <p>
	 * 如果目标topic的消息格式没有升级到0.11.0.0版本，幂等和事务的特性，将会因为{@link UnsupportedForMessageFormatException}而失效
	 * 如果是在事务过程中出现了异常错误，可以中断然后继续，但是需要注意的是，直到升级topic之前，我们还是会收到相同的异常
	 * <p>
	 * 需要注意的是，callback的执行，将会依赖与producer的io线程，所以callback的执行应该很快，否则就会阻塞后续的其他线程的执行
	 * 如果你想要执行一些阻塞，或者具有昂贵计算代价的callback，建议使用线程池来执行
	 * @param record   需要发送的记录
	 * @param callback 用户提供的callback，在记录被服务端接收之后执行，null意味着没有callback
	 * @throws AuthenticationException 身份验证失败
	 * @throws AuthorizationException  producer不允许写入
	 * @throws IllegalStateException   如果已经配置了一个transaction.id，但是没有任何事务启动
	 *                                 或者在producer已经关闭的情况下，继续调用send()方法
	 * @throws InterruptException      Thread在阻塞过程中被中断
	 * @throws SerializationException  key或者value不是给定配置的序列化器的合法对象
	 * @throws TimeoutException        取回元数据超过了max.block.ms
	 *                                 获取为记录申请的内存已经超过阈值
	 * @throws KafkaException          发生了非具体制定的Kafka异常
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
			// 获取消息将要发往的partition编号
			int partition = partition(record, serializedKey, serializedValue, cluster);
			// 创建将要发往的partition信息
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
			// sticky的实现方式可以让每个batch的消息足够多，并降低请求频次，提升整体发送延迟
			if (result.abortForNewBatch) {
				// 记录存储上一次计算的partition序号
				int prevPartition = partition;
				// 使用sticky实现获取新的发送到的partition序号
				partitioner.onNewBatch(record.topic(), cluster, prevPartition);
				partition = partition(record, serializedKey, serializedValue, cluster);
				// 创建新的topic-partition
				tp = new TopicPartition(record.topic(), partition);
				if (log.isTraceEnabled()) {
					log.trace("Retrying append due to new batch creation for topic {} partition {}. The old partition was {}", record.topic(), partition, prevPartition);
				}
				// 重新创建拦截器callback和开发者自定义callback
				interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);
				// 继续追加一次record
				result = accumulator.append(tp, timestamp, serializedKey,
						serializedValue, headers, interceptCallback, remainingWaitMs, false);
			}
			// 如果需要进行事务
			if (transactionManager != null && transactionManager.isTransactional())
				// 将topic-partition对象添加到事务中
				transactionManager.maybeAddPartitionToTransaction(tp);
			// 如果追加的结果是batch已经满了，或者新的batch已经创建
			if (result.batchIsFull || result.newBatchCreated) {
				log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
				// 唤醒sender线程，发送缓冲区存储的record
				this.sender.wakeup();
			}
			// 返回追加record的异步结果
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
	 * @param topic     我们想要获取元数据的来源topic
	 * @param partition 一个希望存在元数据的分区，如果没有指定，则为null
	 * @param maxWaitMs 等待元数据的最大时间
	 * @return 集群及包含topic元数据的信息，以及等待获取元数据的实际时间
	 * @throws KafkaException 执行异常
	 */
	private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {
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
	 * 调用此方法将会使所有在缓冲区的record立即变为可以发送的状态(即使等待<code>linger.ms</code>大于0)，然后阻塞等待发送请求的完成
	 * flush()方法的后置条件是先前发送的任何record都将完成(比如：Future.isDone() == true)
	 * 成功确认请求后，该请求即视为已完成，还取决于<code>acks</code>的配置
	 * 如果没有成功确认请求，则会产生错误
	 * <p>
	 * 如果一个线程在等待flush()调用的完成，其他线程仍可以继续发送record
	 * 然而不保证flush调用开始之后发送的record是否完整
	 * <p>
	 * 此方法在消费一些输入系统并将内容写入Kafka等场景下比较适用，<code>flush()</code>调用是一种确认之前所有消息均已完成的快捷方式
	 * <p>
	 * 下面示例代码演示了从Kafka topic消费消息后，写入另一个Kafka topic
	 * <pre>
	 * {@code
	 * for(ConsumerRecord<String, String> record: consumer.poll(100))
	 *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
	 *     producer.flush();
	 *     consumer.commit();
	 * }
	 * </pre>
	 * 注意：如果生产请求失败，则上面的例子可能丢弃record，如果我们需要确认是否发生丢弃record的现象，就需要提高配置中<code>retries</code>的重试此时
	 * </p>
	 * <p>
	 * 应用程序无需为事务型producer实例调用此方法，因为{@link #commitTransaction()}方法将会在执行commit之前flush掉所有的缓存record
	 * 也就是在commit之前，可以确保所有的{@link #send(ProducerRecord)}调用将会在上一个{@link #beginTransaction()}完成之后进行
	 * </p>
	 * @throws InterruptException 线程在阻塞等待过程中被中断
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
	 * @throws AuthorizationException  if not authorized to the specified topic. See the exception for more details
	 * @throws InterruptException      if the thread is interrupted while blocked
	 * @throws TimeoutException        if metadata could not be refreshed within {@code max.block.ms}
	 * @throws KafkaException          for all Kafka-related exceptions, including the case where this method is called after producer close
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
	 * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
	 *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
	 * @throws InterruptException       If the thread is interrupted while blocked
	 * @throws IllegalArgumentException If the <code>timeout</code> is negative.
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
		for (List<?> candidateList : candidateLists)
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
