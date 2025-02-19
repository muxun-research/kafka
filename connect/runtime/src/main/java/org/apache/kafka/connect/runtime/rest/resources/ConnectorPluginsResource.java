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
package org.apache.kafka.connect.runtime.rest.resources;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.PluginInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.tools.*;
import org.apache.kafka.connect.util.FutureCallback;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Path("/connector-plugins")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorPluginsResource implements ConnectResource {

    private static final String ALIAS_SUFFIX = "Connector";
    private final Herder herder;
    private final List<PluginInfo> connectorPlugins;
    private long requestTimeoutMs;

    static final List<Class<? extends SinkConnector>> SINK_CONNECTOR_EXCLUDES = Arrays.asList(VerifiableSinkConnector.class, MockSinkConnector.class);

    static final List<Class<? extends SourceConnector>> SOURCE_CONNECTOR_EXCLUDES = Arrays.asList(VerifiableSourceConnector.class, MockSourceConnector.class, SchemaSourceConnector.class);

    public ConnectorPluginsResource(Herder herder) {
        this.herder = herder;
        this.connectorPlugins = new ArrayList<>();
        this.requestTimeoutMs = DEFAULT_REST_REQUEST_TIMEOUT_MS;

        // TODO: improve once plugins are allowed to be added/removed during runtime.
        addConnectorPlugins(herder.plugins().sinkConnectors(), SINK_CONNECTOR_EXCLUDES);
        addConnectorPlugins(herder.plugins().sourceConnectors(), SOURCE_CONNECTOR_EXCLUDES);
        addConnectorPlugins(herder.plugins().transformations(), Collections.emptySet());
        addConnectorPlugins(herder.plugins().predicates(), Collections.emptySet());
        addConnectorPlugins(herder.plugins().converters(), Collections.emptySet());
        addConnectorPlugins(herder.plugins().headerConverters(), Collections.emptySet());
    }

    private <T> void addConnectorPlugins(Collection<PluginDesc<T>> plugins, Collection<Class<? extends T>> excludes) {
        plugins.stream().filter(p -> !excludes.contains(p.pluginClass())).map(PluginInfo::new).forEach(connectorPlugins::add);
    }

    @Override
    public void requestTimeout(long requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    @PUT
    @Path("/{pluginName}/config/validate")
    @Operation(summary = "Validate the provided configuration against the configuration definition for the specified pluginName")
    public ConfigInfos validateConfigs(final @PathParam("pluginName") String pluginName, final Map<String, String> connectorConfig) throws Throwable {
        String includedConnType = connectorConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
        if (includedConnType != null && !normalizedPluginName(includedConnType).endsWith(normalizedPluginName(pluginName))) {
            throw new BadRequestException("Included connector type " + includedConnType + " does not match request type " + pluginName);
        }

        // the validated configs don't need to be logged
        FutureCallback<ConfigInfos> validationCallback = new FutureCallback<>();
        herder.validateConnectorConfig(connectorConfig, validationCallback, false);

        try {
            return validationCallback.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // This timeout is for the operation itself. None of the timeout error codes are relevant, so internal server
            // error is the best option
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request timed out");
        } catch (InterruptedException e) {
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request interrupted");
        }
    }

    @GET
    @Path("/")
    @Operation(summary = "List all connector plugins installed")
    public List<PluginInfo> listConnectorPlugins(@DefaultValue("true") @QueryParam("connectorsOnly") @Parameter(description = "Whether to list only connectors instead of all plugins") boolean connectorsOnly) {
        synchronized (this) {
            if (connectorsOnly) {
                return Collections.unmodifiableList(connectorPlugins.stream().filter(p -> PluginType.SINK.toString().equals(p.type()) || PluginType.SOURCE.toString().equals(p.type())).collect(Collectors.toList()));
            } else {
                return Collections.unmodifiableList(connectorPlugins);
            }
        }
    }

    @GET
    @Path("/{pluginName}/config")
    @Operation(summary = "Get the configuration definition for the specified pluginName")
    public List<ConfigKeyInfo> getConnectorConfigDef(final @PathParam("pluginName") String pluginName) {
        synchronized (this) {
            return herder.connectorPluginConfig(pluginName);
        }
    }

    private String normalizedPluginName(String pluginName) {
        // Works for both full and simple class names. In the latter case, it generates the alias.
        return pluginName.endsWith(ALIAS_SUFFIX) && pluginName.length() > ALIAS_SUFFIX.length() ? pluginName.substring(0, pluginName.length() - ALIAS_SUFFIX.length()) : pluginName;
    }

}
