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

import java.nio.ByteBuffer;

/**
 * 由4字节网络顺序大小n，后跟n字节内容组成的以大小分隔的发送
 */
public class NetworkSend extends ByteBufferSend {

    public NetworkSend(String destination, ByteBuffer buffer) {
        super(destination, sizeBuffer(buffer.remaining()), buffer);
    }

	/**
	 * 使用ByteBuffer创建一个传输数据大小的buffer
	 * @param size 剩余需要发送的buffer字节数大小
	 * @return 传输数据大小的buffer
	 */
	private static ByteBuffer sizeBuffer(int size) {
		// 创建一个4字节的ByteBuffer
		ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
		// 写入需要发送的字节数
		sizeBuffer.putInt(size);
		// 准备写入buffer
		sizeBuffer.rewind();
		return sizeBuffer;
	}

}
