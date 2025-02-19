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

package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionReceiveProcessorSupplier<K, KO> implements ProcessorSupplier<KO, SubscriptionWrapper<K>, CombinedKey<KO, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>> {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionReceiveProcessorSupplier.class);

    private final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>> storeBuilder;
    private final CombinedKeySchema<KO, K> keySchema;

    public SubscriptionReceiveProcessorSupplier(final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>> storeBuilder, final CombinedKeySchema<KO, K> keySchema) {

        this.storeBuilder = storeBuilder;
        this.keySchema = keySchema;
    }

    @Override
    public Processor<KO, SubscriptionWrapper<K>, CombinedKey<KO, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>> get() {

        return new ContextualProcessor<KO, SubscriptionWrapper<K>, CombinedKey<KO, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>>() {

            private TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>> store;
            private Sensor droppedRecordsSensor;

            @Override
            public void init(final ProcessorContext<CombinedKey<KO, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>> context) {
                super.init(context);
                final InternalProcessorContext<?, ?> internalProcessorContext = (InternalProcessorContext<?, ?>) context;

                droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(Thread.currentThread().getName(), internalProcessorContext.taskId().toString(), internalProcessorContext.metrics());
                store = internalProcessorContext.getStateStore(storeBuilder);

                keySchema.init(context);
            }

            @Override
            public void process(final Record<KO, SubscriptionWrapper<K>> record) {
                if (record.key() == null) {
                    if (context().recordMetadata().isPresent()) {
                        final RecordMetadata recordMetadata = context().recordMetadata().get();
                        LOG.warn("Skipping record due to null foreign key. " + "topic=[{}] partition=[{}] offset=[{}]", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                    } else {
                        LOG.warn("Skipping record due to null foreign key. Topic, partition, and offset not known.");
                    }
                    droppedRecordsSensor.record();
                    return;
                }
                if (record.value().getVersion() > SubscriptionWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionWrapper. Need to ensure that there is compatibility
                    //with previous versions to enable rolling upgrades. Must develop a strategy for upgrading
                    //from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionWrapper is of an incompatible version.");
                }

                final Bytes subscriptionKey = keySchema.toBytes(record.key(), record.value().getPrimaryKey());

                final ValueAndTimestamp<SubscriptionWrapper<K>> newValue = ValueAndTimestamp.make(record.value(), record.timestamp());
                final ValueAndTimestamp<SubscriptionWrapper<K>> oldValue = store.get(subscriptionKey);

                //This store is used by the prefix scanner in ForeignTableJoinProcessorSupplier
                if (record.value().getInstruction().equals(SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE) || record.value().getInstruction().equals(SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE)) {
                    store.delete(subscriptionKey);
                } else {
                    store.put(subscriptionKey, newValue);
                }
                final Change<ValueAndTimestamp<SubscriptionWrapper<K>>> change = new Change<>(newValue, oldValue);
                // note: key is non-nullable
                // note: newValue is non-nullable
                context().forward(record.withKey(new CombinedKey<>(record.key(), record.value().getPrimaryKey())).withValue(change).withTimestamp(newValue.timestamp()));
            }
        };
    }
}