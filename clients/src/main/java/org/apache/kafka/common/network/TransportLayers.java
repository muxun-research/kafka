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

import java.nio.channels.GatheringByteChannel;

public final class TransportLayers {

    private TransportLayers() {}

	// 这是一个临时解决方案，因为发送或者接收接口用于BlockingChannel
	// 我们可以通过使用TransportLayer来移除一个BlockingChannel，这样做会比GatheringByteChannel和ScatteringByteChannel更好
    public static boolean hasPendingWrites(GatheringByteChannel channel) {
        if (channel instanceof TransportLayer)
            return ((TransportLayer) channel).hasPendingWrites();
        return false;
    }
}
