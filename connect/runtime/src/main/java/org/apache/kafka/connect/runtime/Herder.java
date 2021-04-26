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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * <p>
 * Header接口用于追踪和管理worker和connectors，它是对外组件的主要接口，用于变更集群的状态，举个例子，在分布式模式下，此类的实现会知道如何去接收一个connector配置
 * 可能需要路由这个connector到当前集群的主worker线程，因此配置可以被持久化存储，以此来确认新的connector正确的初始化于worker之上
 * </p>
 * <p>
 * 此类必须实现集群需要调用的方法，(add/remove connectors, pause/resume tasks,et state of connectors and tasks, 等)
 * 对于集群的非Java接口类型的方法(REST API and CLI)，这个接口提供了简单的函数包装
 * </p>
 * <p>
 * 在单机模式下，此类的实现是微不足道的，因为不需要进行协调，在这种情况下，实现主要是将任务直接委派给其他组件
 * 举个例子，在单机模式下创建一个新的connector，无需持久化存储配置，connector和任务都在同一个进程中，所以单机模式下的header实现可以立即初始化并开启connector和它的任务
 * </p>
 */
public interface Herder {

	void start();

	void stop();

	boolean isRunning();

	/**
	 * Get a list of connectors currently running in this cluster. This is a full list of connectors in the cluster gathered
	 * from the current configuration. However, note
	 * @return A list of connector names
	 * @throws org.apache.kafka.connect.runtime.distributed.RequestTargetException if this node can not resolve the request
	 *                                                                             (e.g., because it has not joined the cluster or does not have configs in sync with the group) and it is
	 *                                                                             not the leader or the task owner (e.g., task restart must be handled by the worker which owns the task)
	 * @throws org.apache.kafka.connect.errors.ConnectException                    if this node is the leader, but still cannot resolve the
	 *                                                                             request (e.g., it is not in sync with other worker's config state)
	 */
	void connectors(Callback<Collection<String>> callback);

	/**
	 * Get the definition and status of a connector.
	 */
	void connectorInfo(String connName, Callback<ConnectorInfo> callback);

	/**
	 * Get the configuration for a connector.
	 * @param connName name of the connector
	 * @param callback callback to invoke with the configuration
	 */
	void connectorConfig(String connName, Callback<Map<String, String>> callback);

	/**
	 * Get the configuration for all tasks.
	 * @param connName name of the connector
	 * @param callback callback to invoke with the configuration
	 */
	void tasksConfig(String connName, Callback<Map<ConnectorTaskId, Map<String, String>>> callback);

	/**
	 * Set the configuration for a connector. This supports creation and updating.
	 * @param connName name of the connector
	 * @param config the connectors configuration, or null if deleting the connector
	 * @param allowReplace if true, allow overwriting previous configs; if false, throw AlreadyExistsException if a connector
	 *                     with the same name already exists
	 * @param callback callback to invoke when the configuration has been written
     */
    void putConnectorConfig(String connName, Map<String, String> config, boolean allowReplace, Callback<Created<ConnectorInfo>> callback);

    /**
     * Delete a connector and its configuration.
     * @param connName name of the connector
     * @param callback callback to invoke when the configuration has been written
     */
    void deleteConnectorConfig(String connName, Callback<Created<ConnectorInfo>> callback);

    /**
     * Requests reconfiguration of the task. This should only be triggered by
     * {@link HerderConnectorContext}.
     *
     * @param connName name of the connector that should be reconfigured
     */
    void requestTaskReconfiguration(String connName);

    /**
     * Get the configurations for the current set of tasks of a connector.
	 * @param connName connector to update
	 * @param callback callback to invoke upon completion
	 */
	void taskConfigs(String connName, Callback<List<TaskInfo>> callback);

	/**
	 * Set the configurations for the tasks of a connector. This should always include all tasks in the connector; if
	 * there are existing configurations and fewer are provided, this will reduce the number of tasks, and if more are
	 * provided it will increase the number of tasks.
	 * @param connName connector to update
	 * @param configs list of configurations
	 * @param callback callback to invoke upon completion
	 * @param requestSignature the signature of the request made for this task (re-)configuration;
	 *                         may be null if no signature was provided
	 */
	void putTaskConfigs(String connName, List<Map<String, String>> configs, Callback<Void> callback, InternalRequestSignature requestSignature);

    /**
	 * Get a list of connectors currently running in this cluster.
	 * @return A list of connector names
	 */
	Collection<String> connectors();

	/**
	 * Get the definition and status of a connector.
	 * @param connName name of the connector
	 */
	ConnectorInfo connectorInfo(String connName);

	/**
	 * Lookup the current status of a connector.
	 * @param connName name of the connector
	 */
	ConnectorStateInfo connectorStatus(String connName);

	/**
	 * Lookup the set of topics currently used by a connector.
	 * @param connName name of the connector
	 * @return the set of active topics
	 */
	ActiveTopicsInfo connectorActiveTopics(String connName);

	/**
	 * Request to asynchronously reset the active topics for the named connector.
	 * @param connName name of the connector
	 */
	void resetConnectorActiveTopics(String connName);

	/**
	 * Return a reference to the status backing store used by this herder.
	 * @return the status backing store used by this herder
	 */
	StatusBackingStore statusBackingStore();

	/**
	 * Lookup the status of the a task.
	 * @param id id of the task
	 */
	ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id);

	/**
	 * Validate the provided connector config values against the configuration definition.
	 * @param connectorConfig the provided connector config values
	 * @param callback        the callback to invoke after validation has completed (successfully or not)
	 */
	void validateConnectorConfig(Map<String, String> connectorConfig, Callback<ConfigInfos> callback);

	/**
	 * Validate the provided connector config values against the configuration definition.
	 * @param connectorConfig the provided connector config values
	 * @param callback        the callback to invoke after validation has completed (successfully or not)
	 * @param doLog           if true log all the connector configurations at INFO level; if false, no connector configurations are logged.
	 *                        Note that logging of configuration is not necessary in every endpoint that uses this method.
	 */
	default void validateConnectorConfig(Map<String, String> connectorConfig, Callback<ConfigInfos> callback, boolean doLog) {
		validateConnectorConfig(connectorConfig, callback);
	}

	/**
	 * Restart the task with the given id.
     * @param id id of the task
     * @param cb callback to invoke upon completion
     */
    void restartTask(ConnectorTaskId id, Callback<Void> cb);

	/**
	 * Restart the connector.
	 * @param connName name of the connector
	 * @param cb       callback to invoke upon completion
	 */
	void restartConnector(String connName, Callback<Void> cb);

    /**
     * Restart the connector.
     * @param delayMs delay before restart
     * @param connName name of the connector
     * @param cb callback to invoke upon completion
     * @return The id of the request
     */
    HerderRequest restartConnector(long delayMs, String connName, Callback<Void> cb);

    /**
     * Pause the connector. This call will asynchronously suspend processing by the connector and all
     * of its tasks.
     * @param connector name of the connector
     */
    void pauseConnector(String connector);

    /**
     * Resume the connector. This call will asynchronously start the connector and its tasks (if
     * not started already).
     * @param connector name of the connector
     */
    void resumeConnector(String connector);

    /**
     * Returns a handle to the plugin factory used by this herder and its worker.
     *
     * @return a reference to the plugin factory.
     */
    Plugins plugins();

    /**
     * Get the cluster ID of the Kafka cluster backing this Connect cluster.
     * @return the cluster ID of the Kafka cluster backing this connect cluster
     */
    String kafkaClusterId();

    enum ConfigReloadAction {
        NONE,
        RESTART
    }

    class Created<T> {
        private final boolean created;
        private final T result;

        public Created(boolean created, T result) {
            this.created = created;
            this.result = result;
        }

        public boolean created() {
            return created;
        }

        public T result() {
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Created<?> created1 = (Created<?>) o;
            return Objects.equals(created, created1.created) &&
                    Objects.equals(result, created1.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(created, result);
        }
    }
}
