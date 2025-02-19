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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamSlidingWindowAggregate<KIn, VIn, VAgg> implements KStreamAggProcessorSupplier<KIn, VIn, Windowed<KIn>, VAgg> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storeName;
    private final SlidingWindows windows;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> aggregator;
    private final EmitStrategy emitStrategy;

    private boolean sendOldValues = false;

    public KStreamSlidingWindowAggregate(final SlidingWindows windows, final String storeName, final EmitStrategy emitStrategy, final Initializer<VAgg> initializer, final Aggregator<? super KIn, ? super VIn, VAgg> aggregator) {
        this.windows = windows;
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.emitStrategy = emitStrategy;
    }

    @Override
    public Processor<KIn, VIn, Windowed<KIn>, Change<VAgg>> get() {
        return new KStreamSlidingWindowAggregateProcessor(storeName, emitStrategy, sendOldValues);
    }

    public SlidingWindows windows() {
        return windows;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KStreamSlidingWindowAggregateProcessor extends AbstractKStreamTimeWindowAggregateProcessor<KIn, VIn, VAgg> {
        private Boolean reverseIteratorPossible = null;

        protected KStreamSlidingWindowAggregateProcessor(final String storeName, final EmitStrategy emitStrategy, final boolean sendOldValues) {
            super(storeName, emitStrategy, sendOldValues);
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            if (record.key() == null || record.value() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    log.warn("Skipping record due to null key or value. " + "topic=[{}] partition=[{}] offset=[{}]", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                } else {
                    log.warn("Skipping record due to null key or value. Topic, partition, and offset not known.");
                }
                droppedRecordsSensor.record();
                return;
            }

            updateObservedStreamTime(record.timestamp());
            final long windowCloseTime = observedStreamTime - windows.gracePeriodMs();

            final long windowStart = record.timestamp();
            final long windowEnd = record.timestamp() + windows.timeDifferenceMs();
            if (windowEnd < windowCloseTime) {
                final String window = "[" + windowStart + "," + windowEnd + "]";
                logSkippedRecordForExpiredWindow(log, record.timestamp(), windowCloseTime, window);
                return;
            }

            if (record.timestamp() < windows.timeDifferenceMs()) {
                processEarly(record, windowCloseTime);
                return;
            }

            if (reverseIteratorPossible == null) {
                try {
                    try (final WindowStoreIterator<ValueAndTimestamp<VAgg>> iterator = windowStore.backwardFetch(record.key(), 0L, 0L)) {
                        reverseIteratorPossible = true;
                        log.debug("Sliding Windows aggregate using a reverse iterator");
                    }
                } catch (final UnsupportedOperationException e) {
                    reverseIteratorPossible = false;
                    log.debug("Sliding Windows aggregate using a forward iterator");
                }
            }

            if (reverseIteratorPossible) {
                processReverse(record, windowCloseTime);
            } else {
                processInOrder(record, windowCloseTime);
            }

            maybeForwardFinalResult(record, windowCloseTime);
        }

        public void processInOrder(final Record<KIn, VIn> record, final long windowCloseTime) {
            final Set<Long> windowStartTimes = new HashSet<>();

            // aggregate that will go in the current record’s left/right window (if needed)
            ValueAndTimestamp<VAgg> leftWinAgg = null;
            ValueAndTimestamp<VAgg> rightWinAgg = null;

            //if current record's left/right windows already exist
            boolean leftWinAlreadyCreated = false;
            boolean rightWinAlreadyCreated = false;

            Long previousRecordTimestamp = null;

            try (final KeyValueIterator<Windowed<KIn>, ValueAndTimestamp<VAgg>> iterator = windowStore.fetch(record.key(), record.key(), Math.max(0, record.timestamp() - 2 * windows.timeDifferenceMs()),
                    // add 1 to upper bound to catch the current record's right window, if it exists, without more calls to the store
                    record.timestamp() + 1)) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<KIn>, ValueAndTimestamp<VAgg>> windowBeingProcessed = iterator.next();
                    final long startTime = windowBeingProcessed.key.window().start();
                    windowStartTimes.add(startTime);
                    final long endTime = startTime + windows.timeDifferenceMs();
                    final long windowMaxRecordTimestamp = windowBeingProcessed.value.timestamp();

                    if (endTime < record.timestamp()) {
                        leftWinAgg = windowBeingProcessed.value;
                        previousRecordTimestamp = windowMaxRecordTimestamp;
                    } else if (endTime == record.timestamp()) {
                        leftWinAlreadyCreated = true;
                        if (windowMaxRecordTimestamp < record.timestamp()) {
                            previousRecordTimestamp = windowMaxRecordTimestamp;
                        }
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, record, windowCloseTime);
                    } else if (endTime > record.timestamp() && startTime <= record.timestamp()) {
                        rightWinAgg = windowBeingProcessed.value;
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, record, windowCloseTime);
                    } else if (startTime == record.timestamp() + 1) {
                        rightWinAlreadyCreated = true;
                    } else {
                        log.error("Unexpected window with start {} found when processing record at {} in `KStreamSlidingWindowAggregate`.", startTime, record.timestamp());
                        throw new IllegalStateException("Unexpected window found when processing sliding windows");
                    }
                }
            }
            createWindows(record, windowCloseTime, windowStartTimes, rightWinAgg, leftWinAgg, leftWinAlreadyCreated, rightWinAlreadyCreated, previousRecordTimestamp);
        }

        public void processReverse(final Record<KIn, VIn> record, final long windowCloseTime) {
            final Set<Long> windowStartTimes = new HashSet<>();

            // aggregate that will go in the current record’s left/right window (if needed)
            ValueAndTimestamp<VAgg> leftWinAgg = null;
            ValueAndTimestamp<VAgg> rightWinAgg = null;

            //if current record's left/right windows already exist
            boolean leftWinAlreadyCreated = false;
            boolean rightWinAlreadyCreated = false;

            Long previousRecordTimestamp = null;

            try (final KeyValueIterator<Windowed<KIn>, ValueAndTimestamp<VAgg>> iterator = windowStore.backwardFetch(record.key(), record.key(), Math.max(0, record.timestamp() - 2 * windows.timeDifferenceMs()),
                    // add 1 to upper bound to catch the current record's right window, if it exists, without more calls to the store
                    record.timestamp() + 1)) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<KIn>, ValueAndTimestamp<VAgg>> windowBeingProcessed = iterator.next();
                    final long startTime = windowBeingProcessed.key.window().start();
                    windowStartTimes.add(startTime);
                    final long endTime = startTime + windows.timeDifferenceMs();
                    final long windowMaxRecordTimestamp = windowBeingProcessed.value.timestamp();
                    if (startTime == record.timestamp() + 1) {
                        rightWinAlreadyCreated = true;
                    } else if (endTime > record.timestamp()) {
                        if (rightWinAgg == null) {
                            rightWinAgg = windowBeingProcessed.value;
                        }
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, record, windowCloseTime);
                    } else if (endTime == record.timestamp()) {
                        leftWinAlreadyCreated = true;
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, record, windowCloseTime);
                        if (windowMaxRecordTimestamp < record.timestamp()) {
                            previousRecordTimestamp = windowMaxRecordTimestamp;
                        } else {
                            return;
                        }
                    } else if (endTime < record.timestamp()) {
                        leftWinAgg = windowBeingProcessed.value;
                        previousRecordTimestamp = windowMaxRecordTimestamp;
                        break;
                    } else {
                        log.error("Unexpected window with start {} found when processing record at {} in `KStreamSlidingWindowAggregate`.", startTime, record.timestamp());
                        throw new IllegalStateException("Unexpected window found when processing sliding windows");
                    }
                }
            }
            createWindows(record, windowCloseTime, windowStartTimes, rightWinAgg, leftWinAgg, leftWinAlreadyCreated, rightWinAlreadyCreated, previousRecordTimestamp);
        }

        /**
         * Created to handle records where 0 < inputRecordTimestamp < timeDifferenceMs. These records would create
         * windows with negative start times, which is not supported. Instead, we will put them into the [0, timeDifferenceMs]
         * window as a "workaround", and we will update or create their right windows as new records come in later
         */
        private void processEarly(final Record<KIn, VIn> record, final long windowCloseTime) {
            if (record.timestamp() < 0 || record.timestamp() >= windows.timeDifferenceMs()) {
                log.error("Early record for sliding windows must fall between fall between 0 <= inputRecordTimestamp. Timestamp {} does not fall between 0 <= {}", record.timestamp(), windows.timeDifferenceMs());
                throw new IllegalArgumentException("Early record for sliding windows must fall between fall between 0 <= inputRecordTimestamp");
            }

            // A window from [0, timeDifferenceMs] that holds all early records
            KeyValue<Windowed<KIn>, ValueAndTimestamp<VAgg>> combinedWindow = null;
            ValueAndTimestamp<VAgg> rightWinAgg = null;
            boolean rightWinAlreadyCreated = false;
            final Set<Long> windowStartTimes = new HashSet<>();

            Long previousRecordTimestamp = null;

            try (final KeyValueIterator<Windowed<KIn>, ValueAndTimestamp<VAgg>> iterator = windowStore.fetch(record.key(), record.key(), 0,
                    // add 1 to upper bound to catch the current record's right window, if it exists, without more calls to the store
                    record.timestamp() + 1)) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<KIn>, ValueAndTimestamp<VAgg>> windowBeingProcessed = iterator.next();
                    final long startTime = windowBeingProcessed.key.window().start();
                    windowStartTimes.add(startTime);
                    final long windowMaxRecordTimestamp = windowBeingProcessed.value.timestamp();

                    if (startTime == 0) {
                        combinedWindow = windowBeingProcessed;
                        // We don't need to store previousRecordTimestamp if maxRecordTimestamp >= timestamp
                        // because the previous record's right window (if there is a previous record)
                        // would have already been created by maxRecordTimestamp
                        if (windowMaxRecordTimestamp < record.timestamp()) {
                            previousRecordTimestamp = windowMaxRecordTimestamp;
                        }

                    } else if (startTime <= record.timestamp()) {
                        rightWinAgg = windowBeingProcessed.value;
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, record, windowCloseTime);
                    } else if (startTime == record.timestamp() + 1) {
                        rightWinAlreadyCreated = true;
                    } else {
                        log.error("Unexpected window with start {} found when processing record at {} in `KStreamSlidingWindowAggregate`.", startTime, record.timestamp());
                        throw new IllegalStateException("Unexpected window found when processing sliding windows");
                    }
                }
            }

            // If there wasn't a right window agg found and we need a right window for our new record,
            // the current aggregate in the combined window will go in the new record's right window. We can be sure that the combined
            // window only holds records that fall into the current record's right window for two reasons:
            // 1. If there were records earlier than the current record AND later than the current record, there would be a right window found
            // when we looked for right window agg.
            // 2. If there was only a record before the current record, we wouldn't need a right window for the current record and wouldn't update the
            // rightWinAgg value here, as the combinedWindow.value.timestamp() < inputRecordTimestamp
            if (rightWinAgg == null && combinedWindow != null && combinedWindow.value.timestamp() > record.timestamp()) {
                rightWinAgg = combinedWindow.value;
            }

            if (!rightWinAlreadyCreated && rightWindowIsNotEmpty(rightWinAgg, record.timestamp())) {
                createCurrentRecordRightWindow(record.timestamp(), rightWinAgg, record);
            }

            //create the right window for the previous record if the previous record exists and the window hasn't already been created
            if (previousRecordTimestamp != null && !windowStartTimes.contains(previousRecordTimestamp + 1)) {
                createPreviousRecordRightWindow(previousRecordTimestamp + 1, record, windowCloseTime);
            }

            if (combinedWindow == null) {
                final TimeWindow window = new TimeWindow(0, windows.timeDifferenceMs());
                final ValueAndTimestamp<VAgg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), record.timestamp());
                updateWindowAndForward(window, valueAndTime, record, windowCloseTime);

            } else {
                //update the combined window with the new aggregate
                updateWindowAndForward(combinedWindow.key.window(), combinedWindow.value, record, windowCloseTime);
            }

        }

        private void createWindows(final Record<KIn, VIn> record, final long closeTime, final Set<Long> windowStartTimes, final ValueAndTimestamp<VAgg> rightWinAgg, final ValueAndTimestamp<VAgg> leftWinAgg, final boolean leftWinAlreadyCreated, final boolean rightWinAlreadyCreated, final Long previousRecordTimestamp) {
            // create right window for previous record
            if (previousRecordTimestamp != null) {
                final long previousRightWinStart = previousRecordTimestamp + 1;
                if (previousRecordRightWindowDoesNotExistAndIsNotEmpty(windowStartTimes, previousRightWinStart, record.timestamp())) {
                    createPreviousRecordRightWindow(previousRightWinStart, record, closeTime);
                }
            }

            // create left window for new record
            if (!leftWinAlreadyCreated) {
                final ValueAndTimestamp<VAgg> valueAndTime;
                if (leftWindowNotEmpty(previousRecordTimestamp, record.timestamp())) {
                    valueAndTime = ValueAndTimestamp.make(leftWinAgg.value(), record.timestamp());
                } else {
                    valueAndTime = ValueAndTimestamp.make(initializer.apply(), record.timestamp());
                }
                final TimeWindow window = new TimeWindow(record.timestamp() - windows.timeDifferenceMs(), record.timestamp());
                updateWindowAndForward(window, valueAndTime, record, closeTime);
            }

            // create right window for new record, if necessary
            if (!rightWinAlreadyCreated && rightWindowIsNotEmpty(rightWinAgg, record.timestamp())) {
                createCurrentRecordRightWindow(record.timestamp(), rightWinAgg, record);
            }
        }

        private void createCurrentRecordRightWindow(final long inputRecordTimestamp, final ValueAndTimestamp<VAgg> rightWinAgg, final Record<KIn, VIn> record) {
            final TimeWindow window = new TimeWindow(inputRecordTimestamp + 1, inputRecordTimestamp + 1 + windows.timeDifferenceMs());
            windowStore.put(record.key(), rightWinAgg, window.start());
            maybeForwardUpdate(record, window, null, rightWinAgg.value(), rightWinAgg.timestamp());
        }

        private void createPreviousRecordRightWindow(final long windowStart, final Record<KIn, VIn> record, final long closeTime) {
            final TimeWindow window = new TimeWindow(windowStart, windowStart + windows.timeDifferenceMs());
            final ValueAndTimestamp<VAgg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), record.timestamp());
            updateWindowAndForward(window, valueAndTime, record, closeTime);
        }

        // checks if the previous record falls into the current records left window; if yes, the left window is not empty, otherwise it is empty
        private boolean leftWindowNotEmpty(final Long previousRecordTimestamp, final long inputRecordTimestamp) {
            return previousRecordTimestamp != null && inputRecordTimestamp - windows.timeDifferenceMs() <= previousRecordTimestamp;
        }

        // checks if the previous record's right window does not already exist and the current record falls within previous record's right window
        private boolean previousRecordRightWindowDoesNotExistAndIsNotEmpty(final Set<Long> windowStartTimes, final long previousRightWindowStart, final long inputRecordTimestamp) {
            return !windowStartTimes.contains(previousRightWindowStart) && previousRightWindowStart + windows.timeDifferenceMs() >= inputRecordTimestamp;
        }

        // checks if the aggregate we found has records that fall into the current record's right window; if yes, the right window is not empty
        private boolean rightWindowIsNotEmpty(final ValueAndTimestamp<VAgg> rightWinAgg, final long inputRecordTimestamp) {
            return rightWinAgg != null && rightWinAgg.timestamp() > inputRecordTimestamp;
        }

        @Override
        protected long emitRangeLowerBound(final long windowCloseTime) {
            return lastEmitWindowCloseTime == ConsumerRecord.NO_TIMESTAMP ? 0L : Math.max(0L, lastEmitWindowCloseTime - windows.timeDifferenceMs());
        }

        @Override
        protected long emitRangeUpperBound(final long windowCloseTime) {
            // Sliding window's start and end timestamps are inclusive, so
            // we should minus 1 for the inclusive closed window-end upper bound
            return windowCloseTime - windows.timeDifferenceMs() - 1;
        }

        @Override
        protected boolean shouldRangeFetch(final long emitRangeLowerBound, final long emitRangeUpperBound) {
            return true;
        }

        private void updateWindowAndForward(final Window window, final ValueAndTimestamp<VAgg> valueAndTime, final Record<KIn, VIn> record, final long windowCloseTime) {
            final long windowStart = window.start();
            final long windowEnd = window.end();

            if (windowEnd >= windowCloseTime) {
                // get aggregate from existing window
                final VAgg oldAgg = getValueOrNull(valueAndTime);
                final VAgg newAgg = aggregator.apply(record.key(), record.value(), oldAgg);

                final long newTimestamp = oldAgg == null ? record.timestamp() : Math.max(record.timestamp(), valueAndTime.timestamp());
                windowStore.put(record.key(), ValueAndTimestamp.make(newAgg, newTimestamp), windowStart);
                maybeForwardUpdate(record, window, oldAgg, newAgg, newTimestamp);
            } else {
                final String windowString = "[" + windowStart + "," + windowEnd + "]";
                logSkippedRecordForExpiredWindow(log, record.timestamp(), windowCloseTime, windowString);
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<Windowed<KIn>, VAgg> view() {
        return new KTableValueGetterSupplier<Windowed<KIn>, VAgg>() {

            public KTableValueGetter<Windowed<KIn>, VAgg> get() {
                return new KStreamWindowAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KStreamWindowAggregateValueGetter implements KTableValueGetter<Windowed<KIn>, VAgg> {
        private TimestampedWindowStore<KIn, VAgg> windowStore;

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            windowStore = context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<VAgg> get(final Windowed<KIn> windowedKey) {
            final KIn key = windowedKey.key();
            return windowStore.fetch(key, windowedKey.window().start());
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isVersioned() {
            return false;
        }
    }
}
