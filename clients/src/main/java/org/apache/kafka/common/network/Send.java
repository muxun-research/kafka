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
import java.nio.channels.GatheringByteChannel;

/**
 * 将数据发送到指定目的地的接口模型
 */
public interface Send {

	/**
	 * 发送目的地的node id
	 */
	String destination();

	/**
	 * 判断发送是否完成
	 */
	boolean completed();

	/**
	 * 将需要发送的数据写入到channel中
	 * 可能会进行多次调用，直到所有的需要发送的数据都写入完成
	 * @param channel 写入的channel
	 * @return 一共写了多少字节
	 * @throws IOException 写入失败，抛出IO异常
	 */
	long writeTo(GatheringByteChannel channel) throws IOException;

	/**
	 * 发送数据的大小
	 */
	long size();
}
