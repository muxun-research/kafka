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

import org.apache.kafka.common.errors.AuthenticationException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.Principal;

/*
 * 核心传输层
 * 在最基本的层次上，它是SocketChannel的包装，并且可以作为SocketChannel的替代品，已经其他Network Channel的实现
 * 我们使用NetworkClient替代了BlockingChannel和其他实现，我们也将使用KafkaChannel作为网络IO的channel
 */
public interface TransportLayer extends ScatteringByteChannel, GatheringByteChannel {

	/**
	 * 在握手及验证完成的情况下，返回true
	 */
	boolean ready();

	/**
	 * 判断是否结束和一个socket channel的连接进程
	 */
	boolean finishConnect() throws IOException;

	/**
	 * 断开和socket channel的链接
	 */
	void disconnect();

	/**
	 * Tells whether or not this channel's network socket is connected.
	 * 判断channel的网络socket是否连接
	 */
	boolean isConnected();

	/**
	 * 返回潜在的socket channel
	 */
	SocketChannel socketChannel();

	/**
	 * 获取潜在的selection key
	 */
	SelectionKey selectionKey();

	/**
	 * 这对于非安全的纯文本来说是一个无操作
	 * 对于SSL，将使用SSL握手方式，在使用{@link org.apache.kafka.common.config.SslConfigs#SSL_CLIENT_AUTH_CONFIG}的情况下，SSL握手方式包括客户端身份验证
	 * @throws AuthenticationException 如果握手失败是由于一个 {@link javax.net.ssl.SSLException}异常，抛出身份验证失败异常
	 * @throws IOException             读写失败抛出IO异常
	 */
	void handshake() throws AuthenticationException, IOException;

	/**
	 * 判断是否存在待定的写入
	 */
	boolean hasPendingWrites();

	/**
	 * 如果是一个SslTransportLayer并且已经拥有一个已验证的节点，返回SSLSession.getPeerPrincipal()结果
	 * 其他情况下返回KafkaPrincipal.ANONYMOUS
	 */
	Principal peerPrincipal() throws IOException;

	void addInterestOps(int ops);

	void removeInterestOps(int ops);

	boolean isMute();

	/**
	 * @return 在channel有需要读取的在中间层buffer的字节时，返回true
	 * 中间层buffer可能不会读取网络传输中的额外数据
	 */
	boolean hasBytesBuffered();

	/**
	 * 将字节从fileChannel传输到当前的TransportLayer
	 * 这个方法将会代理{@link FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)}方法
	 * 但是在可能的情况下，它将会打开目的地的channel，目的是为了利用零拷贝的优势
	 * 这是必需的，因为只有目标缓冲区从内部JDK类继承时，才会执行“transferTo”的快速路径
	 * @param fileChannel 资源的channel
	 * @param position    传输开始的位置，必须为非负数
	 * @param count       传输字节数的最大值，必须为非负数
	 * @return 真是传输的字节数，可能为0
	 * @see FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)
	 */
	long transferFrom(FileChannel fileChannel, long position, long count) throws IOException;
}
