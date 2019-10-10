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
package org.apache.kafka.common.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 * <p>
 * A connection can be added to the nioSelector associated with an integer id by doing
 *
 * <pre>
 * nioSelector.connect(&quot;42&quot;, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 * The nioSelector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 *
 * This class is not thread safe!
 */
public class Selector implements Selectable, AutoCloseable {

    public static final long NO_IDLE_TIMEOUT_MS = -1;
    public static final int NO_FAILED_AUTHENTICATION_DELAY = 0;

    private enum CloseMode {
        GRACEFUL(true),            // process outstanding staged receives, notify disconnect
        NOTIFY_ONLY(true),         // discard any outstanding receives, notify disconnect
        DISCARD_NO_NOTIFY(false);  // discard any outstanding receives, no disconnect notification

        boolean notifyDisconnect;

        CloseMode(boolean notifyDisconnect) {
            this.notifyDisconnect = notifyDisconnect;
        }
    }

    private final Logger log;
	/**
	 * NIO选择器
	 */
	private final java.nio.channels.Selector nioSelector;
    private final Map<String, KafkaChannel> channels;
    private final Set<KafkaChannel> explicitlyMutedChannels;
	/**
	 * 已完成发送请求的集合
	 */
    private final List<Send> completedSends;
	/**
	 * 已完成接受请求的集合
	 */
	private final List<NetworkReceive> completedReceives;
	/**
	 * 断开连接的node id channel集合
	 */
	private final Map<String, ChannelState> disconnected;
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;
    private final Set<SelectionKey> immediatelyConnectedKeys;
    private final Map<String, KafkaChannel> closingChannels;
    private Set<SelectionKey> keysWithBufferedRead;
	/**
	 * 进行连接的node id集合
	 */
    private final List<String> connected;
	/**
	 * 内存溢出标识
	 */
	private boolean outOfMemory;
    private final List<String> failedSends;
    private final Time time;
    private final SelectorMetrics sensors;
    private final ChannelBuilder channelBuilder;
    private final int maxReceiveSize;
    private final boolean recordTimePerConnection;
    private final IdleExpiryManager idleExpiryManager;
    private final LinkedHashMap<String, DelayedAuthenticationFailureClose> delayedClosingChannels;
    private final MemoryPool memoryPool;
    private final long lowMemThreshold;
    private final int failedAuthenticationDelayMs;

    //indicates if the previous call to poll was able to make progress in reading already-buffered data.
    //this is used to prevent tight loops when memory is not available to read any more data
    private boolean madeReadProgressLastPoll = true;

    /**
     * Create a new nioSelector
     * @param maxReceiveSize Max size in bytes of a single network receive (use {@link NetworkReceive#UNLIMITED} for no limit)
     * @param connectionMaxIdleMs Max idle connection time (use {@link #NO_IDLE_TIMEOUT_MS} to disable idle timeout)
     * @param failedAuthenticationDelayMs Minimum time by which failed authentication response and channel close should be delayed by.
     *                                    Use {@link #NO_FAILED_AUTHENTICATION_DELAY} to disable this delay.
     * @param metrics Registry for Selector metrics
     * @param time Time implementation
     * @param metricGrpPrefix Prefix for the group of metrics registered by Selector
     * @param metricTags Additional tags to add to metrics registered by Selector
     * @param metricsPerConnection Whether or not to enable per-connection metrics
     * @param channelBuilder Channel builder for every new connection
     * @param logContext Context for logging with additional info
     */
    public Selector(int maxReceiveSize,
            long connectionMaxIdleMs,
            int failedAuthenticationDelayMs,
            Metrics metrics,
            Time time,
            String metricGrpPrefix,
            Map<String, String> metricTags,
            boolean metricsPerConnection,
            boolean recordTimePerConnection,
            ChannelBuilder channelBuilder,
            MemoryPool memoryPool,
            LogContext logContext) {
        try {
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        this.maxReceiveSize = maxReceiveSize;
        this.time = time;
        this.channels = new HashMap<>();
        this.explicitlyMutedChannels = new HashSet<>();
        this.outOfMemory = false;
        this.completedSends = new ArrayList<>();
        this.completedReceives = new ArrayList<>();
        this.stagedReceives = new HashMap<>();
        this.immediatelyConnectedKeys = new HashSet<>();
        this.closingChannels = new HashMap<>();
        this.keysWithBufferedRead = new HashSet<>();
        this.connected = new ArrayList<>();
        this.disconnected = new HashMap<>();
        this.failedSends = new ArrayList<>();
        this.sensors = new SelectorMetrics(metrics, metricGrpPrefix, metricTags, metricsPerConnection);
        this.channelBuilder = channelBuilder;
        this.recordTimePerConnection = recordTimePerConnection;
        this.idleExpiryManager = connectionMaxIdleMs < 0 ? null : new IdleExpiryManager(time, connectionMaxIdleMs);
        this.memoryPool = memoryPool;
        this.lowMemThreshold = (long) (0.1 * this.memoryPool.size());
        this.log = logContext.logger(Selector.class);
        this.failedAuthenticationDelayMs = failedAuthenticationDelayMs;
        this.delayedClosingChannels = (failedAuthenticationDelayMs > NO_FAILED_AUTHENTICATION_DELAY) ? new LinkedHashMap<String, DelayedAuthenticationFailureClose>() : null;
    }

    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    boolean recordTimePerConnection,
                    ChannelBuilder channelBuilder,
                    MemoryPool memoryPool,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags,
                metricsPerConnection, recordTimePerConnection, channelBuilder, memoryPool, logContext);
    }

    public Selector(int maxReceiveSize,
                long connectionMaxIdleMs,
                int failedAuthenticationDelayMs,
                Metrics metrics,
                Time time,
                String metricGrpPrefix,
                Map<String, String> metricTags,
                boolean metricsPerConnection,
                ChannelBuilder channelBuilder,
                LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, false, channelBuilder, MemoryPool.NONE, logContext);
    }


    public Selector(int maxReceiveSize,
            long connectionMaxIdleMs,
            Metrics metrics,
            Time time,
            String metricGrpPrefix,
            Map<String, String> metricTags,
            boolean metricsPerConnection,
            ChannelBuilder channelBuilder,
            LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, channelBuilder, logContext);
    }

    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, Collections.emptyMap(), true, channelBuilder, logContext);
    }

    public Selector(long connectionMaxIdleMS, int failedAuthenticationDelayMs, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, Collections.<String, String>emptyMap(), true, channelBuilder, logContext);
    }

    /**
     * Begin connecting to the given address and add the connection to this nioSelector associated with the given id
     * number.
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        ensureNotRegistered(id);
        SocketChannel socketChannel = SocketChannel.open();
        SelectionKey key = null;
        try {
            configureSocketChannel(socketChannel, sendBufferSize, receiveBufferSize);
            boolean connected = doConnect(socketChannel, address);
            key = registerChannel(id, socketChannel, SelectionKey.OP_CONNECT);

            if (connected) {
                // OP_CONNECT won't trigger for immediately connected channels
                log.debug("Immediately connected to node {}", id);
                immediatelyConnectedKeys.add(key);
                key.interestOps(0);
            }
        } catch (IOException | RuntimeException e) {
            if (key != null)
                immediatelyConnectedKeys.remove(key);
            channels.remove(id);
            socketChannel.close();
            throw e;
        }
    }

    // Visible to allow test cases to override. In particular, we use this to implement a blocking connect
    // in order to simulate "immediately connected" sockets.
    protected boolean doConnect(SocketChannel channel, InetSocketAddress address) throws IOException {
        try {
            return channel.connect(address);
        } catch (UnresolvedAddressException e) {
            throw new IOException("Can't resolve address: " + address, e);
        }
    }

    private void configureSocketChannel(SocketChannel socketChannel, int sendBufferSize, int receiveBufferSize)
            throws IOException {
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(true);
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setSendBufferSize(sendBufferSize);
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setReceiveBufferSize(receiveBufferSize);
        socket.setTcpNoDelay(true);
    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * <p>
     * If a connection already exists with the same connection id in `channels` or `closingChannels`,
     * an exception is thrown. Connection ids must be chosen to avoid conflict when remote ports are reused.
     * Kafka brokers add an incrementing index to the connection id to avoid reuse in the timing window
     * where an existing connection may not yet have been closed by the broker when a new connection with
     * the same remote host:port is processed.
     * </p><p>
     * If a `KafkaChannel` cannot be created for this connection, the `socketChannel` is closed
     * and its selection key cancelled.
     * </p>
     */
    public void register(String id, SocketChannel socketChannel) throws IOException {
        ensureNotRegistered(id);
        registerChannel(id, socketChannel, SelectionKey.OP_READ);
        this.sensors.connectionCreated.record();
    }

    private void ensureNotRegistered(String id) {
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);
        if (this.closingChannels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id + " that is still being closed");
    }

    protected SelectionKey registerChannel(String id, SocketChannel socketChannel, int interestedOps) throws IOException {
        SelectionKey key = socketChannel.register(nioSelector, interestedOps);
        KafkaChannel channel = buildAndAttachKafkaChannel(socketChannel, id, key);
        this.channels.put(id, channel);
        if (idleExpiryManager != null)
            idleExpiryManager.update(channel.id(), time.nanoseconds());
        return key;
    }

    private KafkaChannel buildAndAttachKafkaChannel(SocketChannel socketChannel, String id, SelectionKey key) throws IOException {
        try {
            KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize, memoryPool);
            key.attach(channel);
            return channel;
        } catch (Exception e) {
            try {
                socketChannel.close();
            } finally {
                key.cancel();
            }
            throw new IOException("Channel could not be created for socket " + socketChannel, e);
        }
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        try {
            for (String id : connections)
                close(id);
        } finally {
            // If there is any exception thrown in close(id), we should still be able
            // to close the remaining objects, especially the sensors because keeping
            // the sensors may lead to failure to start up the ReplicaFetcherThread if
            // the old sensors with the same names has not yet been cleaned up.
            AtomicReference<Throwable> firstException = new AtomicReference<>();
            Utils.closeQuietly(nioSelector, "nioSelector", firstException);
            Utils.closeQuietly(sensors, "sensors", firstException);
            Utils.closeQuietly(channelBuilder, "channelBuilder", firstException);
            Throwable exception = firstException.get();
            if (exception instanceof RuntimeException && !(exception instanceof SecurityException)) {
                throw (RuntimeException) exception;
            }

        }
    }

    /**
     * Queue the given request for sending in the subsequent {@link #poll(long)} calls
	 *
     * @param send The request to send
     */
    public void send(Send send) {
		// 获取目的地的ID
        String connectionId = send.destination();
		// 获取传输数据的Channel
        KafkaChannel channel = openOrClosingChannelOrFail(connectionId);
		// 如果当前Channel处于正在关闭状态
        if (closingChannels.containsKey(connectionId)) {
			// 在失败发送中添加当前node id，并保持Channel的当前状态
            this.failedSends.add(connectionId);
        } else {
			try {
				// 设置需要发送的模型
				// 并注册NIO网络模型的写事件
                channel.setSend(send);
            } catch (Exception e) {
				// 如果出现异常
				// 并发更新Channel状态，channel将会在#close()后被废弃掉
                channel.state(ChannelState.FAILED_SEND);
				// 在下一次#poll()处理#failedSends()时，确保#disconnected()的通知
                this.failedSends.add(connectionId);
				// 关闭异常channel
                close(channel, CloseMode.DISCARD_NO_NOTIFY);
				// 非 {@link CancelledKeyException} 异常记录日志，并抛出
                if (!(e instanceof CancelledKeyException)) {
                    log.error("Unexpected exception during send, closing connection {} and rethrowing exception {}",
                            connectionId, e);
                    throw e;
                }
            }
		}
	}

	/**
	 * 在非阻塞的情况下，做IO所能做的事情，包括完成连接，完成断连，初始化新的数据发送，或者进行数据发送和接收数据
	 *
	 * 当本次调用完成时，开发者可以使用{@link #completedSends()}检查发送是否完成，{@link #completedReceives()}检查响应是否接收
	 * {@link #connected()}检查连接是否连接，{@link #disconnected()}检查连接是否断开
	 * 这些数据会在每次poll()方法调用之前被清除，然后在调用时通过IO操作重新填充
	 *
	 * 在明文设置中，我们使用SocketChannel来进行网络读写，但是对于SSL设置，我们需要在使用SocketChannel向网络写数据之前，对数据进行加密，在返回响应的时候进行解密
	 * 这需要额外的buffer来保证我们从网络中读取到的数据，由于网路上的数据已经加密，所以我们无法通过Kafka的协议需求来读取真正的字节数据
	 * 在不超过SSL引擎阈值的情况下，我们可以读取尽可能多的数据，这意味着我们可能会读取比请求大小更多的数据
	 * 如果没有要从SocketChannel Selector中读取的其他数据，则不会继续调用该Channel，并且buffer中还有额外字节
	 * 我们通过添加stagedReceives map来避免这个问题，这个集合中包含了每个channel的双向队列
	 * 当我们从一个channel中读取数据时，我们尽可能多的读取响应，然后将它们存储在stagedReceives中，然后在poll()方法调用过程中，pop出一个放入到completedReceives集合中
	 * 如果在stagedReceives中存在任何活跃的channel，我们将等待时间设置为0，并且pop出响应信息，放入到completedReceives中
	 *
	 * 在每次poll()方法调用时，每个channel最多只会添加一个元素到completedReceives列表中，这是必要的，以确保来自channel的请求会在broker上按照顺序进行处理
	 * 因为SocketServer添加到请求队列中的未完成的请求，可能会被不同的请求处理线程进行处理，所以每个channel上的请求必须保证每次处理一个来保证顺序性
	 * @param timeout 等待时间，单位毫秒，非负数
	 * @throws IllegalArgumentException timeout为负数时抛出的异常
	 * @throws IllegalStateException 一个给定的发送没有连接，或者已经处于发送状态
     */
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");
		// 上一次是否进行读操作调用
        boolean madeReadProgressLastCall = madeReadProgressLastPoll;
		// 发送之前，先清除内部集合
		clear();
		// buffer中是否还有数据
        boolean dataInBuffers = !keysWithBufferedRead.isEmpty();
		// 如果channel处于分阶段提交状态，或者需要立即连接的selection key，或者buffer中有数据并且上次调用进行过读取操作
		// 上述三种情况满足之一，超时时间设置为0
        if (hasStagedReceives() || !immediatelyConnectedKeys.isEmpty() || (madeReadProgressLastCall && dataInBuffers))
			timeout = 0;
		// 如果内存池没有超出内存，但是当前Selector线程超出了内存
        if (!memoryPool.isOutOfMemory() && outOfMemory) {
			// 我们已经从内存压力中恢复过来，将因为不明确地原因mute掉的channel进行unmute
            log.trace("Broker no longer low on memory - unmuting incoming sockets");
            for (KafkaChannel channel : channels.values()) {
                if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                    channel.maybeUnmute();
				}
			}
			// 已经恢复了，内存溢出标识设为false
            outOfMemory = false;
		}

		// 获取开始进行选取的时间戳
        long startSelect = time.nanoseconds();
		// 获取处于就绪状态的selection key
        int numReadyKeys = select(timeout);
		// 获得结束选取的时间戳
        long endSelect = time.nanoseconds();
		// 记录选择器选取的时间
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());
		// 如果有selection key处于就绪状态，或者有selection key需要立即连接，或者buffer中有数据的情况下
        if (numReadyKeys > 0 || !immediatelyConnectedKeys.isEmpty() || dataInBuffers) {
            Set<SelectionKey> readyKeys = this.nioSelector.selectedKeys();

			// 在buffer中有数据的情况下，从channel中轮询数据
            if (dataInBuffers) {
                keysWithBufferedRead.removeAll(readyKeys); //so no channel gets polled twice
                Set<SelectionKey> toPoll = keysWithBufferedRead;
                keysWithBufferedRead = new HashSet<>(); //poll() calls will repopulate if needed
                pollSelectionKeys(toPoll, false, endSelect);
			}

			// 轮询那些潜在拥有更多数据的channel
            pollSelectionKeys(readyKeys, false, endSelect);
			// 就绪状态的selection key轮询完毕，清除就绪状态的selection key
            readyKeys.clear();
			// 需要进行立即连接的selection key轮询完毕，清除需要进行立即连接的selection key
            pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
            immediatelyConnectedKeys.clear();
		} else {
			// 如果没有轮询，记为本次轮询状态为读状态
			madeReadProgressLastPoll = true;
        }

        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());

        // Close channels that were delayed and are now ready to be closed
        completeDelayedChannelClose(endIo);

        // we use the time at the end of select to ensure that we don't close any connections that
        // have just been processed in pollSelectionKeys
        maybeCloseOldestConnection(endSelect);

        // Add to completedReceives after closing expired connections to avoid removing
        // channels with completed receives until all staged receives are completed.
        addToCompletedReceives();
	}

	/**
	 * 处理selection key集合中的IO操作
	 * @param selectionKeys 需要处理的selection key
	 * @param isImmediatelyConnected 是否是立即连接
	 * @param currentTimeNanos 当前的纳秒时间戳
     */
    void pollSelectionKeys(Set<SelectionKey> selectionKeys,
                           boolean isImmediatelyConnected,
                           long currentTimeNanos) {
		// selection key是否需要进行重新洗牌
        for (SelectionKey key : determineHandlingOrder(selectionKeys)) {
			// 获取selection key关联的channel
            KafkaChannel channel = channel(key);
			// channel开始连接时的时间戳
            long channelStartTimeNanos = recordTimePerConnection ? time.nanoseconds() : 0;
			// 发送失败标识
            boolean sendFailed = false;

			// 根据channel id注册每个连接的计数器
            sensors.maybeRegisterConnectionMetrics(channel.id());
            if (idleExpiryManager != null)
                idleExpiryManager.update(channel.id(), currentTimeNanos);

            try {
                /* complete any connections that have finished their handshake (either normally or immediately) */
				// 在需要立即连接的情况下，或者selection key处于可连接状态
                if (isImmediatelyConnected || key.isConnectable()) {
					// 如果channel已经完成连接
                    if (channel.finishConnect()) {
						// 已连接channel中添加当前的channel
                        this.connected.add(channel.id());
						// Selector计数器记录当前channel已经创建
                        this.sensors.connectionCreated.record();
						// 获取selection key的channel，并日志记录信息
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        log.debug("Created socket with SO_RCVBUF = {}, SO_SNDBUF = {}, SO_TIMEOUT = {} to node {}",
                                socketChannel.socket().getReceiveBufferSize(),
                                socketChannel.socket().getSendBufferSize(),
                                socketChannel.socket().getSoTimeout(),
								channel.id());
						// 还没有完成连接，遍历下一个selection key
                    } else {
                        continue;
                    }
				}

				// 如果channel已经连接，但是还没有就绪
                if (channel.isConnected() && !channel.ready()) {
					// 完成channel的准备，正常执行下，channel会处于就绪状态
                    channel.prepare();
					// 如果此时channel处于就绪状态
                    if (channel.ready()) {
						// 获取channel就绪的时间戳
                        long readyTimeMs = time.milliseconds();
						// channel是否进行了多次验证
                        boolean isReauthentication = channel.successfulAuthentications() > 1;
                        if (isReauthentication) {
							// 计数器记录成功重复验证的数据
                            sensors.successfulReauthentication.record(1.0, readyTimeMs);
                            if (channel.reauthenticationLatencyMs() == null)
                                log.warn(
                                    "Should never happen: re-authentication latency for a re-authenticated channel was null; continuing...");
                            else
                                sensors.reauthenticationLatency
                                    .record(channel.reauthenticationLatencyMs().doubleValue(), readyTimeMs);
                        } else {
                            sensors.successfulAuthentication.record(1.0, readyTimeMs);
                            if (!channel.connectedClientSupportsReauthentication())
                                sensors.successfulAuthenticationNoReauth.record(1.0, readyTimeMs);
                        }
                        log.debug("Successfully {}authenticated with {}", isReauthentication ?
                            "re-" : "", channel.socketDescription());
					}
					// 如果在验证过程中收到了响应，则添加将channel和NetwordReceive添加到分阶段接收集合中
                    List<NetworkReceive> responsesReceivedDuringReauthentication = channel
                            .getAndClearResponsesReceivedDuringReauthentication();
                    responsesReceivedDuringReauthentication.forEach(receive -> addToStagedReceives(channel, receive));
				}
				// 尝试读取数据
                attemptRead(key, channel);
				// 如果channel的buffer还有需要读取的数据
                if (channel.hasBytesBuffered()) {
					// channel中的中间层有我们读取不了的字节（可能是因为没有内存），这可能会导致潜在的socket在下一次轮询中也不会出现
					// 所以我们就需要记住这个channel，在下一次poll的时候
					// 如果我们在尝试处理buffer中的数据，但是没有进展，我们就会清除channel的buffer数据，以防每次都需要检查
					// 向存在buffer的selection key集合中添加此selection key
                    keysWithBufferedRead.add(key);
				}

				// 如果channel准备就绪可写，并且buffer中还有写入空间，并且我们也有写入的数据
                if (channel.ready() && key.isWritable() && !channel.maybeBeginClientReauthentication(
                    () -> channelStartTimeNanos != 0 ? channelStartTimeNanos : currentTimeNanos)) {
                    Send send;
					try {
						// 写入channel
						// 如果写入成功，则发送时构建的Send对象，就是接收的Send对象
                        send = channel.write();
                    } catch (Exception e) {
						// 写入异常，则将写入失败标识置为true
                        sendFailed = true;
                        throw e;
                    }
                    if (send != null) {
						// 如果发送成功，证明当前发送已经完成，计数器进行记录
                        this.completedSends.add(send);
                        this.sensors.recordBytesSent(channel.id(), send.size());
                    }
				}

				// 取消已失效的socket
                if (!key.isValid())
                    close(channel, CloseMode.GRACEFUL);

            } catch (Exception e) {
				// 获取当前channel的socket连接地址
                String desc = channel.socketDescription();
                if (e instanceof IOException) {
                    log.debug("Connection with {} disconnected", desc, e);
                } else if (e instanceof AuthenticationException) {
                    boolean isReauthentication = channel.successfulAuthentications() > 0;
					// 根据验证情况进行记录
                    if (isReauthentication)
                        sensors.failedReauthentication.record();
                    else
                        sensors.failedAuthentication.record();
                    String exceptionMessage = e.getMessage();
					// 日志记录异常信息
                    if (e instanceof DelayedResponseAuthenticationException)
                        exceptionMessage = e.getCause().getMessage();
                    log.info("Failed {}authentication with {} ({})", isReauthentication ? "re-" : "",
                        desc, exceptionMessage);
                } else {
                    log.warn("Unexpected error from {}; closing connection", desc, e);
				}
				// 如果是延迟响应验证异常
                if (e instanceof DelayedResponseAuthenticationException)
					// 执行
                    maybeDelayCloseOnAuthenticationFailure(channel);
				else
					// 其他情况下，关闭channel
                    close(channel, sendFailed ? CloseMode.NOTIFY_ONLY : CloseMode.GRACEFUL);
			} finally {
				// 记录每次连接的时间
                maybeRecordTimePerConnection(channel, channelStartTimeNanos);
            }
		}
	}

	/**
	 * 确定需要处理的顺序
	 * @param selectionKeys 需要处理的selection key的顺序
	 * @return selection key按照处理顺序排序后的集合
	 */
	private Collection<SelectionKey> determineHandlingOrder(Set<SelectionKey> selectionKeys) {
		// 需要保证的是，在每次调用下，产生的排序相同的
		// 在内存不足时，这可能会导致读取不足，为了解决这个问题，我们会在内存不足时，进行重新洗牌
		// 在没有超出内存的情况下，剩余内存已经超过最低内存的阈值时，需要进行重新洗牌
		if (!outOfMemory && memoryPool.availableMemory() < lowMemThreshold) {
			List<SelectionKey> shuffledKeys = new ArrayList<>(selectionKeys);
			// 随机改变排序顺序
			Collections.shuffle(shuffledKeys);
			return shuffledKeys;
		} else {
			// 返回相同的排序
			return selectionKeys;
		}
	}

	/**
	 * 尝试读取数据
	 * @param key 指定的selection key
	 * @param channel 指定的channel
	 * @throws IOException 读取异常，抛出IO异常
	 */
	private void attemptRead(SelectionKey key, KafkaChannel channel) throws IOException {
		// 如果channel处于就绪状态，并且可以从socket或者buffer中读取字节，并且此前没有进行分阶段接收，或者channel存在于mute集合中
		if (channel.ready() && (key.isReadable() || channel.hasBytesBuffered()) && !hasStagedReceive(channel)
				&& !explicitlyMutedChannels.contains(channel)) {
			NetworkReceive networkReceive;
			// 读取内容
			while ((networkReceive = channel.read()) != null) {
				// 上一次轮询执行的读取操作为true
				madeReadProgressLastPoll = true;
				// 将接收到的数据添加到分阶段接收中
				addToStagedReceives(channel, networkReceive);
			}
			// 如果channel状态变为mute
			if (channel.isMute()) {
				// 因为内存压力，将内存溢出标识置为true
				outOfMemory = true;
			} else {
				// 如果channel没有处于mute状态，将上一次轮询为读状态的标识置为true
				madeReadProgressLastPoll = true;
			}
		}
	}

    // Record time spent in pollSelectionKeys for channel (moved into a method to keep checkstyle happy)
    private void maybeRecordTimePerConnection(KafkaChannel channel, long startTimeNanos) {
        if (recordTimePerConnection)
            channel.addNetworkThreadTimeNanos(time.nanoseconds() - startTimeNanos);
    }

    @Override
    public List<Send> completedSends() {
        return this.completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
        explicitlyMutedChannels.add(channel);
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        // Remove the channel from explicitlyMutedChannels only if the channel has been actually unmuted.
        if (channel.maybeUnmute()) {
            explicitlyMutedChannels.remove(channel);
        }
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    // package-private for testing
    void completeDelayedChannelClose(long currentTimeNanos) {
        if (delayedClosingChannels == null)
            return;

        while (!delayedClosingChannels.isEmpty()) {
            DelayedAuthenticationFailureClose delayedClose = delayedClosingChannels.values().iterator().next();
            if (!delayedClose.tryClose(currentTimeNanos))
                break;
        }
    }

    private void maybeCloseOldestConnection(long currentTimeNanos) {
        if (idleExpiryManager == null)
            return;

        Map.Entry<String, Long> expiredConnection = idleExpiryManager.pollExpiredConnection(currentTimeNanos);
        if (expiredConnection != null) {
            String connectionId = expiredConnection.getKey();
            KafkaChannel channel = this.channels.get(connectionId);
            if (channel != null) {
                if (log.isTraceEnabled())
                    log.trace("About to close the idle connection from {} due to being idle for {} millis",
                            connectionId, (currentTimeNanos - expiredConnection.getValue()) / 1000 / 1000);
                channel.state(ChannelState.EXPIRED);
                close(channel, CloseMode.GRACEFUL);
            }
		}
	}

	/**
	 * 清除上一次poll()调用的结果
     */
    private void clear() {
		// 清除已完成发送请求、已完成接收请求、进行连接的node id集合、断开连接的node id集合
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();

		// 移除已经关闭的channel，在这些channel中所有的分阶段请求已经处理或者已经发送任务已经请求的情况下
        for (Iterator<Map.Entry<String, KafkaChannel>> it = closingChannels.entrySet().iterator(); it.hasNext(); ) {
            KafkaChannel channel = it.next().getValue();
            Deque<NetworkReceive> deque = this.stagedReceives.get(channel);
            boolean sendFailed = failedSends.remove(channel.id());
            if (deque == null || deque.isEmpty() || sendFailed) {
                doClose(channel, true);
                it.remove();
			}
		}
		// 断开所有发送数据失败的channel
        for (String channel : this.failedSends)
            this.disconnected.put(channel, ChannelState.FAILED_SEND);
        this.failedSends.clear();
        this.madeReadProgressLastPoll = false;
	}

	/**
	 * 在给定的时间内，选座写入就绪的通道
	 * @param timeoutMs 等待时间，单位ms，必须为非负数
	 * @return 处于就绪状态的selection key的个数
     */
    private int select(long timeoutMs) throws IOException {
        if (timeoutMs < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");

        if (timeoutMs == 0L)
            return this.nioSelector.selectNow();
        else
            return this.nioSelector.select(timeoutMs);
    }

    /**
     * Close the connection identified by the given id
     */
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null) {
            // There is no disconnect notification for local close, but updating
            // channel state here anyway to avoid confusion.
            channel.state(ChannelState.LOCAL_CLOSE);
            close(channel, CloseMode.DISCARD_NO_NOTIFY);
        } else {
            KafkaChannel closingChannel = this.closingChannels.remove(id);
            // Close any closing channel, leave the channel in the state in which closing was triggered
            if (closingChannel != null)
                doClose(closingChannel, false);
		}
	}

	/**
	 * 关闭channel
	 * @param channel 需要关闭的channel
	 */
	private void maybeDelayCloseOnAuthenticationFailure(KafkaChannel channel) {
		// 封装成一个延迟关闭channel的对象，用于延迟关闭
		DelayedAuthenticationFailureClose delayedClose = new DelayedAuthenticationFailureClose(channel, failedAuthenticationDelayMs);
		if (delayedClosingChannels != null)
			delayedClosingChannels.put(channel.id(), delayedClose);
		else
			delayedClose.closeNow();
	}

    private void handleCloseOnAuthenticationFailure(KafkaChannel channel) {
        try {
            channel.completeCloseOnAuthenticationFailure();
        } catch (Exception e) {
            log.error("Exception handling close on authentication failure node {}", channel.id(), e);
        } finally {
            close(channel, CloseMode.GRACEFUL);
        }
    }

    /**
     * Begin closing this connection.
     * If 'closeMode' is `CloseMode.GRACEFUL`, the channel is disconnected here, but staged receives
     * are processed. The channel is closed when there are no outstanding receives or if a send is
     * requested. For other values of `closeMode`, outstanding receives are discarded and the channel
     * is closed immediately.
     *
     * The channel will be added to disconnect list when it is actually closed if `closeMode.notifyDisconnect`
     * is true.
     */
    private void close(KafkaChannel channel, CloseMode closeMode) {
        channel.disconnect();

        // Ensure that `connected` does not have closed channels. This could happen if `prepare` throws an exception
        // in the `poll` invocation when `finishConnect` succeeds
        connected.remove(channel.id());

        // Keep track of closed channels with pending receives so that all received records
        // may be processed. For example, when producer with acks=0 sends some records and
        // closes its connections, a single poll() in the broker may receive records and
        // handle close(). When the remote end closes its connection, the channel is retained until
        // a send fails or all outstanding receives are processed. Mute state of disconnected channels
        // are tracked to ensure that requests are processed one-by-one by the broker to preserve ordering.
        Deque<NetworkReceive> deque = this.stagedReceives.get(channel);
        if (closeMode == CloseMode.GRACEFUL && deque != null && !deque.isEmpty()) {
            // stagedReceives will be moved to completedReceives later along with receives from other channels
            closingChannels.put(channel.id(), channel);
            log.debug("Tracking closing connection {} to process outstanding requests", channel.id());
        } else {
            doClose(channel, closeMode.notifyDisconnect);
        }
        this.channels.remove(channel.id());

        if (delayedClosingChannels != null)
            delayedClosingChannels.remove(channel.id());

        if (idleExpiryManager != null)
            idleExpiryManager.remove(channel.id());
    }

    private void doClose(KafkaChannel channel, boolean notifyDisconnect) {
        SelectionKey key = channel.selectionKey();
        try {
            immediatelyConnectedKeys.remove(key);
            keysWithBufferedRead.remove(key);
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", channel.id(), e);
        } finally {
            key.cancel();
            key.attach(null);
        }
        this.sensors.connectionClosed.record();
        this.stagedReceives.remove(channel);
        this.explicitlyMutedChannels.remove(channel);
        if (notifyDisconnect)
            this.disconnected.put(channel.id(), channel.state());
    }

    /**
     * check if channel is ready
	 * 判断Channel状态是否就绪
     */
    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
	}

	/**
	 * 从正在运行中的Channel集合中获取Channel，或者从正在关闭的Channel中获取指定Channel
	 * @param id 目的地的node id
	 * @return 指定node id的KafkaChannel
	 */
	private KafkaChannel openOrClosingChannelOrFail(String id) {
		// 从缓存的连接Channel中，获取KafkaChannel
		KafkaChannel channel = this.channels.get(id);
		if (channel == null)
			// 当前正常连接的Channel中，没有目的地Channel，则从正在关闭的Channel中获取指定Channel
			channel = this.closingChannels.get(id);
		// 没有获取到Channel，则抛出异常
		if (channel == null)
			throw new IllegalStateException("Attempt to retrieve channel for which there is no connection. Connection id " + id + " existing connections " + channels.keySet());
		return channel;
	}

    /**
     * Return the selector channels.
     */
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel associated with the
     * connection.
     */
    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    /**
     * Return the channel with the specified id if it was disconnected, but not yet closed
     * since there are outstanding messages to be processed.
     */
    public KafkaChannel closingChannel(String id) {
        return closingChannels.get(id);
    }

    /**
     * Returns the lowest priority channel chosen using the following sequence:
     *   1) If one or more channels are in closing state, return any one of them
     *   2) If idle expiry manager is enabled, return the least recently updated channel
     *   3) Otherwise return any of the channels
     *
     * This method is used to close a channel to accommodate a new channel on the inter-broker listener
     * when broker-wide `max.connections` limit is enabled.
     */
    public KafkaChannel lowestPriorityChannel() {
        KafkaChannel channel = null;
        if (!closingChannels.isEmpty()) {
            channel = closingChannels.values().iterator().next();
        } else if (idleExpiryManager != null && !idleExpiryManager.lruConnections.isEmpty()) {
            String channelId = idleExpiryManager.lruConnections.keySet().iterator().next();
            channel = channel(channelId);
        } else if (!channels.isEmpty()) {
            channel = channels.values().iterator().next();
        }
        return channel;
    }

    /**
     * Get the channel associated with selectionKey
     */
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    /**
     * Check if given channel has a staged receive
     */
    private boolean hasStagedReceive(KafkaChannel channel) {
        return stagedReceives.containsKey(channel);
	}

	/**
	 * 判断是否有分阶段的请求，这些分阶段请求的channel是否处于muted状态
     */
    private boolean hasStagedReceives() {
        for (KafkaChannel channel : this.stagedReceives.keySet()) {
            if (!channel.isMute())
                return true;
        }
        return false;
    }


    /**
     * adds a receive to staged receives
     */
    private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
        if (!stagedReceives.containsKey(channel))
            stagedReceives.put(channel, new ArrayDeque<>());

        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        deque.add(receive);
    }

    /**
     * checks if there are any staged receives and adds to completedReceives
     */
    private void addToCompletedReceives() {
        if (!this.stagedReceives.isEmpty()) {
            Iterator<Map.Entry<KafkaChannel, Deque<NetworkReceive>>> iter = this.stagedReceives.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<KafkaChannel, Deque<NetworkReceive>> entry = iter.next();
                KafkaChannel channel = entry.getKey();
                if (!explicitlyMutedChannels.contains(channel)) {
                    Deque<NetworkReceive> deque = entry.getValue();
                    addToCompletedReceives(channel, deque);
                    if (deque.isEmpty())
                        iter.remove();
                }
            }
        }
    }

    private void addToCompletedReceives(KafkaChannel channel, Deque<NetworkReceive> stagedDeque) {
        NetworkReceive networkReceive = stagedDeque.poll();
        this.completedReceives.add(networkReceive);
        this.sensors.recordBytesReceived(channel.id(), networkReceive.size());
    }

    // only for testing
    public Set<SelectionKey> keys() {
        return new HashSet<>(nioSelector.keys());
    }

    // only for testing
    public int numStagedReceives(KafkaChannel channel) {
        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        return deque == null ? 0 : deque.size();
    }

    private class SelectorMetrics implements AutoCloseable {
        private final Metrics metrics;
        private final String metricGrpPrefix;
        private final Map<String, String> metricTags;
        private final boolean metricsPerConnection;

        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor successfulAuthentication;
        public final Sensor successfulReauthentication;
        public final Sensor successfulAuthenticationNoReauth;
        public final Sensor reauthenticationLatency;
        public final Sensor failedAuthentication;
        public final Sensor failedReauthentication;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor bytesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();
        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection) {
            this.metrics = metrics;
            this.metricGrpPrefix = metricGrpPrefix;
            this.metricTags = metricTags;
            this.metricsPerConnection = metricsPerConnection;
            String metricGrpName = metricGrpPrefix + "-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix);
            this.connectionClosed.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-close", "connections closed"));

            this.connectionCreated = sensor("connections-created:" + tagsSuffix);
            this.connectionCreated.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-creation", "new connections established"));

            this.successfulAuthentication = sensor("successful-authentication:" + tagsSuffix);
            this.successfulAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-authentication", "connections with successful authentication"));

            this.successfulReauthentication = sensor("successful-reauthentication:" + tagsSuffix);
            this.successfulReauthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-reauthentication", "successful re-authentication of connections"));

            this.successfulAuthenticationNoReauth = sensor("successful-authentication-no-reauth:" + tagsSuffix);
            MetricName successfulAuthenticationNoReauthMetricName = metrics.metricName(
                    "successful-authentication-no-reauth-total", metricGrpName,
                    "The total number of connections with successful authentication where the client does not support re-authentication",
                    metricTags);
            this.successfulAuthenticationNoReauth.add(successfulAuthenticationNoReauthMetricName, new CumulativeSum());

            this.failedAuthentication = sensor("failed-authentication:" + tagsSuffix);
            this.failedAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-authentication", "connections with failed authentication"));

            this.failedReauthentication = sensor("failed-reauthentication:" + tagsSuffix);
            this.failedReauthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-reauthentication", "failed re-authentication of connections"));

            this.reauthenticationLatency = sensor("reauthentication-latency:" + tagsSuffix);
            MetricName reauthenticationLatencyMaxMetricName = metrics.metricName("reauthentication-latency-max",
                    metricGrpName, "The max latency observed due to re-authentication",
                    metricTags);
            this.reauthenticationLatency.add(reauthenticationLatencyMaxMetricName, new Max());
            MetricName reauthenticationLatencyAvgMetricName = metrics.metricName("reauthentication-latency-avg",
                    metricGrpName, "The average latency observed due to re-authentication",
                    metricTags);
            this.reauthenticationLatency.add(reauthenticationLatencyAvgMetricName, new Avg());

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix);
            bytesTransferred.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(),
                    "network-io", "network operations (reads or writes) on all connections"));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix, bytesTransferred);
            this.bytesSent.add(createMeter(metrics, metricGrpName, metricTags,
                    "outgoing-byte", "outgoing bytes sent to all servers"));
            this.bytesSent.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(),
                    "request", "requests sent"));
            MetricName metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of requests sent.", metricTags);
            this.bytesSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent.", metricTags);
            this.bytesSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix, bytesTransferred);
            this.bytesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    "incoming-byte", "bytes read off all sockets"));
            this.bytesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    new WindowedCount(), "response", "responses received"));

            this.selectTime = sensor("select-time:" + tagsSuffix);
            this.selectTime.add(createMeter(metrics, metricGrpName, metricTags,
                    new WindowedCount(), "select", "times the I/O layer checked for new I/O to perform"));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            this.selectTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io-wait", "waiting"));

            this.ioTime = sensor("io-time:" + tagsSuffix);
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            this.ioTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io", "doing I/O"));

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, (config, now) -> channels.size());
        }

        private Meter createMeter(Metrics metrics, String groupName, Map<String, String> metricTags,
                SampledStat stat, String baseName, String descriptiveName) {
            MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
                            String.format("The number of %s per second", descriptiveName), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
                            String.format("The total number of %s", descriptiveName), metricTags);
            if (stat == null)
                return new Meter(rateMetricName, totalMetricName);
            else
                return new Meter(stat, rateMetricName, totalMetricName);
        }

        private Meter createMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String descriptiveName) {
            return createMeter(metrics, groupName, metricTags, null, baseName, descriptiveName);
        }

        private Meter createIOThreadRatioMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String action) {
            MetricName rateMetricName = metrics.metricName(baseName + "-ratio", groupName,
                    String.format("The fraction of time the I/O thread spent %s", action), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "time-total", groupName,
                    String.format("The total time the I/O thread spent %s", action), metricTags);
            return new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName);
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    String metricGrpName = metricGrpPrefix + "-node-metrics";

                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    nodeRequest.add(createMeter(metrics, metricGrpName, tags, "outgoing-byte", "outgoing bytes"));
                    nodeRequest.add(createMeter(metrics, metricGrpName, tags, new WindowedCount(), "request", "requests sent"));
                    MetricName metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of requests sent.", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent.", tags);
                    nodeRequest.add(metricName, new Max());

                    String nodeResponseName = "node-" + connectionId + ".bytes-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    nodeResponse.add(createMeter(metrics, metricGrpName, tags, "incoming-byte", "incoming bytes"));
                    nodeResponse.add(createMeter(metrics, metricGrpName, tags, new WindowedCount(), "response", "responses received"));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes) {
            long now = time.milliseconds();
            this.bytesSent.record(bytes, now);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void recordBytesReceived(String connection, int bytes) {
            long now = time.milliseconds();
            this.bytesReceived.record(bytes, now);
            if (!connection.isEmpty()) {
                String nodeRequestName = "node-" + connection + ".bytes-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
		}
	}

	/**
	 * 将channel封装成DelayedAuthenticationFailureClose，用于因为验证失败，需要在一段指定的时间后，才能关闭channel
     */
    private class DelayedAuthenticationFailureClose {
        private final KafkaChannel channel;
        private final long endTimeNanos;
        private boolean closed;

        /**
         * @param channel The channel whose close is being delayed
         * @param delayMs The amount of time by which the operation should be delayed
         */
        public DelayedAuthenticationFailureClose(KafkaChannel channel, int delayMs) {
            this.channel = channel;
            this.endTimeNanos = time.nanoseconds() + (delayMs * 1000L * 1000L);
            this.closed = false;
        }

        /**
         * Try to close this channel if the delay has expired.
         * @param currentTimeNanos The current time
         * @return True if the delay has expired and the channel was closed; false otherwise
         */
        public final boolean tryClose(long currentTimeNanos) {
            if (endTimeNanos <= currentTimeNanos)
                closeNow();
            return closed;
        }

        /**
         * Close the channel now, regardless of whether the delay has expired or not.
         */
        public final void closeNow() {
            if (closed)
                throw new IllegalStateException("Attempt to close a channel that has already been closed");
            handleCloseOnAuthenticationFailure(channel);
            closed = true;
        }
    }

    // helper class for tracking least recently used connections to enable idle connection closing
    private static class IdleExpiryManager {
        private final Map<String, Long> lruConnections;
        private final long connectionsMaxIdleNanos;
        private long nextIdleCloseCheckTime;

        public IdleExpiryManager(Time time, long connectionsMaxIdleMs) {
            this.connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;
            // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
            this.lruConnections = new LinkedHashMap<>(16, .75F, true);
            this.nextIdleCloseCheckTime = time.nanoseconds() + this.connectionsMaxIdleNanos;
        }

        public void update(String connectionId, long currentTimeNanos) {
            lruConnections.put(connectionId, currentTimeNanos);
        }

        public Map.Entry<String, Long> pollExpiredConnection(long currentTimeNanos) {
            if (currentTimeNanos <= nextIdleCloseCheckTime)
                return null;

            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
                return null;
            }

            Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
            Long connectionLastActiveTime = oldestConnectionEntry.getValue();
            nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;

            if (currentTimeNanos > nextIdleCloseCheckTime)
                return oldestConnectionEntry;
            else
                return null;
        }

        public void remove(String connectionId) {
            lruConnections.remove(connectionId);
        }
    }

    //package-private for testing
    boolean isOutOfMemory() {
        return outOfMemory;
    }

    //package-private for testing
    boolean isMadeReadProgressLastPoll() {
        return madeReadProgressLastPoll;
    }

    // package-private for testing
    Map<?, ?> delayedClosingChannels() {
        return delayedClosingChannels;
    }
}
