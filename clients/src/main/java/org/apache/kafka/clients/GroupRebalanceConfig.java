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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Locale;
import java.util.Optional;

/**
 * Class to extract group rebalance related configs.
 */
public class GroupRebalanceConfig {

    public enum ProtocolType {
        CONSUMER,
        CONNECT;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public final int sessionTimeoutMs;
    public final int rebalanceTimeoutMs;
    public final int heartbeatIntervalMs;
    public final String groupId;
    public final Optional<String> groupInstanceId;
    public final long retryBackoffMs;
    public final boolean leaveGroupOnClose;

    public GroupRebalanceConfig(AbstractConfig config, ProtocolType protocolType) {
        this.sessionTimeoutMs = config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG);

		// Consumer或者Connect协议，使用的再平衡时间是不同的
        if (protocolType == ProtocolType.CONSUMER) {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        } else {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG);
        }
		// 心跳时间
        this.heartbeatIntervalMs = config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG);
		// 获取配置的group.id属性
        this.groupId = config.getString(CommonClientConfigs.GROUP_ID_CONFIG);

		// 静态成员资格仅在consumer中使用
        if (protocolType == ProtocolType.CONSUMER) {
			// 获取配置的group.instance.id属性
            String groupInstanceId = config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG);
            if (groupInstanceId != null) {
				// 校验开发者配置的group.instance.id属性
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
                this.groupInstanceId = Optional.of(groupInstanceId);
            } else {
                this.groupInstanceId = Optional.empty();
            }
        } else {
            this.groupInstanceId = Optional.empty();
        }
		// 获取配置的retry.backoff.ms属性
        this.retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);

		// Consumer协议的专属配置
        if (protocolType == ProtocolType.CONSUMER) {
			// 获取配置的internal.leave.group.on.close属性
            this.leaveGroupOnClose = config.getBoolean("internal.leave.group.on.close");
        } else {
            this.leaveGroupOnClose = true;
        }
    }

    // For testing purpose.
    public GroupRebalanceConfig(final int sessionTimeoutMs,
                                final int rebalanceTimeoutMs,
                                final int heartbeatIntervalMs,
                                String groupId,
                                Optional<String> groupInstanceId,
                                long retryBackoffMs,
                                boolean leaveGroupOnClose) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.retryBackoffMs = retryBackoffMs;
        this.leaveGroupOnClose = leaveGroupOnClose;
    }
}
