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

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

public class KTableSourceValueGetterSupplier<K, V> implements KTableValueGetterSupplier<K, V> {
    private final String storeName;

	public KTableSourceValueGetterSupplier(final String storeName) {
		this.storeName = storeName;
	}

    public KTableValueGetter<K, V> get() {
        return new KTableSourceValueGetter();
    }

    @Override
    public String[] storeNames() {
        return new String[]{storeName};
    }

    private class KTableSourceValueGetter implements KTableValueGetter<K, V> {
        private KeyValueStoreWrapper<K, V> store;

        public void init(final ProcessorContext<?, ?> context) {
            store = new KeyValueStoreWrapper<>(context, storeName);
        }

        public ValueAndTimestamp<V> get(final K key) {
            return store.get(key);
        }

        @Override
        public ValueAndTimestamp<V> get(final K key, final long asOfTimestamp) {
            return store.get(key, asOfTimestamp);
        }

        @Override
        public boolean isVersioned() {
            return store.isVersionedStore();
        }
    }
}
