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
package org.apache.kafka.common.requests;

import java.util.List;

/**
 * 类中包含了在LeaderAndIsrRequest.PartitionState和UpdateMetadataRequest.PartitionState之间共享的状态字段
 */
public class BasePartitionState {

	/**
	 * 控制器epoch
	 */
	public final int controllerEpoch;
	/**
	 * leader节点
	 */
	public final int leader;
	/**
	 * leader epoch
	 */
	public final int leaderEpoch;
	/**
	 * ISR集合
	 */
	public final List<Integer> isr;
	/**
	 * ZK版本
	 */
	public final int zkVersion;
	/**
	 * AR集合
	 */
	public final List<Integer> replicas;

	BasePartitionState(int controllerEpoch, int leader, int leaderEpoch, List<Integer> isr, int zkVersion, List<Integer> replicas) {
		this.controllerEpoch = controllerEpoch;
		this.leader = leader;
		this.leaderEpoch = leaderEpoch;
		this.isr = isr;
		this.zkVersion = zkVersion;
		this.replicas = replicas;
	}

}
