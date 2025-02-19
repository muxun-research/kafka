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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of OffsetStorageReader. Unlike {@link OffsetStorageWriter} which is implemented
 * directly, the interface is only separate from this implementation because it needs to be
 * included in the public API package.
 */
public class OffsetStorageReaderImpl implements CloseableOffsetStorageReader {
    private static final Logger log = LoggerFactory.getLogger(OffsetStorageReaderImpl.class);

    private final OffsetBackingStore backingStore;
    private final String namespace;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final AtomicBoolean closed;
    private final Set<Future<Map<ByteBuffer, ByteBuffer>>> offsetReadFutures;

    public OffsetStorageReaderImpl(OffsetBackingStore backingStore, String namespace, Converter keyConverter, Converter valueConverter) {
        this.backingStore = backingStore;
        this.namespace = namespace;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.closed = new AtomicBoolean(false);
        this.offsetReadFutures = new HashSet<>();
    }

    @Override
    public <T> Map<String, Object> offset(Map<String, T> partition) {
        return offsets(Collections.singletonList(partition)).get(partition);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
        // Serialize keys so backing store can work with them
        Map<ByteBuffer, Map<String, T>> serializedToOriginal = new HashMap<>(partitions.size());
        for (Map<String, T> key : partitions) {
            try {
                // Offsets are treated as schemaless, their format is only validated here (and the returned value below)
                OffsetUtils.validateFormat(key);
                byte[] keySerialized = keyConverter.fromConnectData(namespace, null, Arrays.asList(namespace, key));
                ByteBuffer keyBuffer = (keySerialized != null) ? ByteBuffer.wrap(keySerialized) : null;
                serializedToOriginal.put(keyBuffer, key);
            } catch (Throwable t) {
                log.error("CRITICAL: Failed to serialize partition key when getting offsets for task with "
                        + "namespace {}. No value for this data will be returned, which may break the "
                        + "task or cause it to skip some data.", namespace, t);
            }
        }

        // Get serialized key -> serialized value from backing store
        Map<ByteBuffer, ByteBuffer> raw;
        try {
            Future<Map<ByteBuffer, ByteBuffer>> offsetReadFuture;
            synchronized (offsetReadFutures) {
                if (closed.get()) {
                    throw new ConnectException("Offset reader is closed. This is likely because the task has already been " + "scheduled to stop but has taken longer than the graceful shutdown " + "period to do so.");
                }
                offsetReadFuture = backingStore.get(serializedToOriginal.keySet());
                offsetReadFutures.add(offsetReadFuture);
            }

            try {
                raw = offsetReadFuture.get();
            } catch (CancellationException e) {
                throw new ConnectException("Offset reader closed while attempting to read offsets. This is likely because " + "the task was been scheduled to stop but has taken longer than the " + "graceful shutdown period to do so.");
            } finally {
                synchronized (offsetReadFutures) {
                    offsetReadFutures.remove(offsetReadFuture);
                }
            }
        } catch (Exception e) {
            log.error("Failed to fetch offsets from namespace {}: ", namespace, e);
            throw new ConnectException("Failed to fetch offsets.", e);
        }

        // Deserialize all the values and map back to the original keys
        Map<Map<String, T>, Map<String, Object>> result = new HashMap<>(partitions.size());
        for (Map.Entry<ByteBuffer, ByteBuffer> rawEntry : raw.entrySet()) {
            try {
                // Since null could be a valid key, explicitly check whether map contains the key
                if (!serializedToOriginal.containsKey(rawEntry.getKey())) {
                    log.error("Should be able to map {} back to a requested partition-offset key, backing " + "store may have returned invalid data", rawEntry.getKey());
                    continue;
                }
                Map<String, T> origKey = serializedToOriginal.get(rawEntry.getKey());
                SchemaAndValue deserializedSchemaAndValue = valueConverter.toConnectData(namespace, rawEntry.getValue() != null ? rawEntry.getValue().array() : null);
                Object deserializedValue = deserializedSchemaAndValue.value();
                OffsetUtils.validateFormat(deserializedValue);

                result.put(origKey, (Map<String, Object>) deserializedValue);
            } catch (Throwable t) {
                log.error("CRITICAL: Failed to deserialize offset data when getting offsets for task with" + " namespace {}. No value for this data will be returned, which may break the " + "task or cause it to skip some data. This could either be due to an error in " + "the connector implementation or incompatible schema.", namespace, t);
            }
        }

        return result;
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            synchronized (offsetReadFutures) {
                for (Future<Map<ByteBuffer, ByteBuffer>> offsetReadFuture : offsetReadFutures) {
                    try {
                        offsetReadFuture.cancel(true);
                    } catch (Throwable t) {
                        log.error("Failed to cancel offset read future", t);
                    }
                }
                offsetReadFutures.clear();
            }
        }
    }
}
