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

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

class KStreamFlatMapValues<KIn, VIn, VOut> implements FixedKeyProcessorSupplier<KIn, VIn, VOut> {

    private final ValueMapperWithKey<? super KIn, ? super VIn, ? extends Iterable<? extends VOut>> mapper;

    KStreamFlatMapValues(final ValueMapperWithKey<? super KIn, ? super VIn, ? extends Iterable<? extends VOut>> mapper) {
        this.mapper = mapper;
    }

    @Override
    public FixedKeyProcessor<KIn, VIn, VOut> get() {
        return new KStreamFlatMapValuesProcessor();
    }

    private class KStreamFlatMapValuesProcessor extends ContextualFixedKeyProcessor<KIn, VIn, VOut> {
        @Override
        public void process(final FixedKeyRecord<KIn, VIn> record) {
            final Iterable<? extends VOut> newValues = mapper.apply(record.key(), record.value());
            for (final VOut v : newValues) {
                context().forward(record.withValue(v));
            }
        }
    }
}
