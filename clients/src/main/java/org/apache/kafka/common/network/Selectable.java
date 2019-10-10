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


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * 异步，多Channel的网络IO模型
 */
public interface Selectable {

	/**
	 * See {@link #connect(String, InetSocketAddress, int, int) connect()}
	 */
	int USE_DEFAULT_BUFFER_SIZE = -1;

	/**
	 * 开始建立一个socket连接到给定的地址，
	 * @param id                连接的node id
	 * @param address           连接的网络地址
	 * @param sendBufferSize    需要发送的buffer大小
	 * @param receiveBufferSize 需要接受的buffer大小
	 * @throws IOException 连接失败，抛出IO异常
	 */
	void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException;

	/**
	 * 在Selector出现IO阻塞的情况下，唤醒Selector
	 */
	void wakeup();

	/**
	 * 关闭Selector
	 */
	void close();

	/**
	 * 关闭由给定ID标识的连接
	 */
	void close(String id);

	/**
	 * 将指定请求放入队列，在随后的{@link #poll(long)}方法后调用
	 * @param send 需要发送的请求
	 */
	void send(Send send);

	/**
	 * 进行IO操作，读、写，建立连接等
	 * @param timeout 空闲等待时间
	 * @throws IOException 出现异常，则抛出IO异常
	 */
	void poll(long timeout) throws IOException;

	/**
	 * The list of sends that completed on the last {@link #poll(long) poll()} call.
	 */
	List<Send> completedSends();

	/**
	 * The list of receives that completed on the last {@link #poll(long) poll()} call.
	 */
	List<NetworkReceive> completedReceives();

	/**
	 * The connections that finished disconnecting on the last {@link #poll(long) poll()}
	 * call. Channel state indicates the local channel state at the time of disconnection.
	 */
	Map<String, ChannelState> disconnected();

	/**
	 * The list of connections that completed their connection on the last {@link #poll(long) poll()}
	 * call.
	 */
	List<String> connected();

	/**
	 * Disable reads from the given connection
	 * @param id The id for the connection
	 */
	void mute(String id);

	/**
	 * Re-enable reads from the given connection
	 * @param id The id for the connection
	 */
	void unmute(String id);

	/**
	 * Disable reads from all connections
	 */
	void muteAll();

	/**
	 * Re-enable reads from all connections
	 */
	void unmuteAll();

	/**
	 * returns true  if a channel is ready
	 * @param id The id for the connection
	 */
	boolean isChannelReady(String id);
}
