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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.api.Record;

import java.util.HashMap;
import java.util.Map;

public class CacheFlushListenerStub<K, V> implements CacheFlushListener<byte[], byte[]> {
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    final Map<K, Change<V>> forwarded = new HashMap<>();

    CacheFlushListenerStub(final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void apply(final Record<byte[], Change<byte[]>> record) {
        forwarded.put(keyDeserializer.deserialize(null, record.key()), new Change<>(valueDeserializer.deserialize(null, record.value().newValue), valueDeserializer.deserialize(null, record.value().oldValue), record.value().isLatest));
    }
}
