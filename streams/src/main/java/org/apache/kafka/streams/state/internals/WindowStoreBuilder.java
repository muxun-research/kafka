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

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class WindowStoreBuilder<K, V> extends AbstractStoreBuilder<K, V, WindowStore<K, V>> {
    private final Logger log = LoggerFactory.getLogger(WindowStoreBuilder.class);

    private final WindowBytesStoreSupplier storeSupplier;

    public WindowStoreBuilder(final WindowBytesStoreSupplier storeSupplier,
                              final Serde<K> keySerde,
                              final Serde<V> valueSerde,
                              final Time time) {
        super(storeSupplier.name(), keySerde, valueSerde, time);
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        this.storeSupplier = storeSupplier;

        if (storeSupplier.retainDuplicates()) {
            this.logConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        }
    }

    @Override
    public WindowStore<K, V> build() {
        if (storeSupplier.retainDuplicates() && enableCaching) {
            log.warn("Disabling caching for {} since store was configured to retain duplicates", storeSupplier.name());
            enableCaching = false;
        }

        return new MeteredWindowStore<>(maybeWrapCaching(maybeWrapLogging(storeSupplier.get())), storeSupplier.windowSize(), storeSupplier.metricsScope(), time, keySerde, valueSerde);
    }

    private WindowStore<Bytes, byte[]> maybeWrapCaching(final WindowStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingWindowStore(
            inner,
            storeSupplier.windowSize(),
            storeSupplier.segmentIntervalMs());
    }

    private WindowStore<Bytes, byte[]> maybeWrapLogging(final WindowStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingWindowBytesStore(inner, storeSupplier.retainDuplicates(), WindowKeySchema::toStoreKeyBinary);
    }

    public long retentionPeriod() {
        return storeSupplier.retentionPeriod();
    }
}
