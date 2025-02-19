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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

class MergedSortedCacheWindowStoreKeyValueIterator extends AbstractMergedSortedCacheStoreIterator<Windowed<Bytes>, Windowed<Bytes>, byte[], byte[]> {

    private final StateSerdes<Bytes, byte[]> serdes;
    private final long windowSize;
    private final SegmentedCacheFunction cacheFunction;
    private final StoreKeyToWindowKey storeKeyToWindowKey;
    private final WindowKeyToBytes windowKeyToBytes;

    MergedSortedCacheWindowStoreKeyValueIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator, final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator, final StateSerdes<Bytes, byte[]> serdes, final long windowSize, final SegmentedCacheFunction cacheFunction, final boolean forward) {
        this(filteredCacheIterator, underlyingIterator, serdes, windowSize, cacheFunction, forward, WindowKeySchema::fromStoreKey, WindowKeySchema::toStoreKeyBinary);
    }

    MergedSortedCacheWindowStoreKeyValueIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator, final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator, final StateSerdes<Bytes, byte[]> serdes, final long windowSize, final SegmentedCacheFunction cacheFunction, final boolean forward, final StoreKeyToWindowKey storeKeyToWindowKey, final WindowKeyToBytes windowKeyToBytes) {
        super(filteredCacheIterator, underlyingIterator, forward);
        this.serdes = serdes;
        this.windowSize = windowSize;
        this.cacheFunction = cacheFunction;
        this.storeKeyToWindowKey = storeKeyToWindowKey;
        this.windowKeyToBytes = windowKeyToBytes;
    }

    @Override
    Windowed<Bytes> deserializeStoreKey(final Windowed<Bytes> key) {
        return key;
    }

    @Override
    KeyValue<Windowed<Bytes>, byte[]> deserializeStorePair(final KeyValue<Windowed<Bytes>, byte[]> pair) {
        return pair;
    }

    @Override
    Windowed<Bytes> deserializeCacheKey(final Bytes cacheKey) {
        final byte[] binaryKey = cacheFunction.key(cacheKey).get();
        return storeKeyToWindowKey.toWindowKey(binaryKey, windowSize, serdes.keyDeserializer(), serdes.topic());
    }

    @Override
    byte[] deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return cacheEntry.value();
    }

    @Override
    int compare(final Bytes cacheKey, final Windowed<Bytes> storeKey) {
        final Bytes storeKeyBytes = windowKeyToBytes.toBytes(storeKey.key(), storeKey.window().start(), 0);
        return cacheFunction.compareSegmentedKeys(cacheKey, storeKeyBytes);
    }

    @FunctionalInterface
    interface StoreKeyToWindowKey {
        Windowed<Bytes> toWindowKey(final byte[] binaryKey, final long windowSize, final Deserializer<Bytes> deserializer, final String topic);
    }

    @FunctionalInterface
    interface WindowKeyToBytes {
        Bytes toBytes(final Bytes key, final long windowStart, final int seqNum);
    }
}
