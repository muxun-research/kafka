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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.VersionedKeyValueStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Materializes a key-value store as either a {@link TimestampedKeyValueStoreBuilder} or a
 * {@link VersionedKeyValueStoreBuilder} depending on whether the store is versioned or not.
 */
public class KeyValueStoreMaterializer<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueStoreMaterializer.class);

    private final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized;

    public KeyValueStoreMaterializer(final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        this.materialized = materialized;
    }

    /**
     * @return StoreBuilder
     */
    public StoreBuilder<?> materialize() {
        KeyValueBytesStoreSupplier supplier = (KeyValueBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            switch (materialized.storeType()) {
                case IN_MEMORY:
                    supplier = Stores.inMemoryKeyValueStore(materialized.storeName());
                    break;
                case ROCKS_DB:
                    supplier = Stores.persistentTimestampedKeyValueStore(materialized.storeName());
                    break;
                default:
                    throw new IllegalStateException("Unknown store type: " + materialized.storeType());
            }
        }

        final StoreBuilder<?> builder;
        if (supplier instanceof VersionedBytesStoreSupplier) {
            builder = Stores.versionedKeyValueStoreBuilder((VersionedBytesStoreSupplier) supplier, materialized.keySerde(), materialized.valueSerde());
        } else {
            builder = Stores.timestampedKeyValueStoreBuilder(supplier, materialized.keySerde(), materialized.valueSerde());
        }

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            if (!(builder instanceof VersionedKeyValueStoreBuilder)) {
                builder.withCachingEnabled();
            } else {
                LOG.info("Not enabling caching for store '{}' as versioned stores do not support caching.", supplier.name());
            }
        }
        return builder;
    }
}