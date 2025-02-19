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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class InMemoryTimeOrderedKeyValueChangeBufferTest {

    @Test
    public void bufferShouldAllowCacheEnablement() {
        new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>(null, null, null).withCachingEnabled();
    }

    @Test
    public void bufferShouldAllowCacheDisablement() {
        new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>(null, null, null).withCachingDisabled();
    }

    @Test
    public void bufferShouldAllowLoggingEnablement() {
        final String expect = "3";
        final Map<String, String> logConfig = new HashMap<>();
        logConfig.put("min.insync.replicas", expect);
        final StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<Object, Object, Change<Object>>> builder = new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>(null, null, null).withLoggingEnabled(logConfig);

        assertThat(builder.logConfig(), is(singletonMap("min.insync.replicas", expect)));
        assertThat(builder.loggingEnabled(), is(true));
    }

    @Test
    public void bufferShouldAllowLoggingDisablement() {
        final StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<Object, Object, Change<Object>>> builder = new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>(null, null, null).withLoggingDisabled();

        assertThat(builder.logConfig(), is(emptyMap()));
        assertThat(builder.loggingEnabled(), is(false));
    }
}
