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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.EmitStrategy.StrategyType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.RocksDbIndexedTimeOrderedWindowBytesStoreSupplier;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.*;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class KStreamWindowAggregateTest {
    private static final String WINDOW_STORE_NAME = "dummy-store-name";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final String threadId = Thread.currentThread().getName();

    @Parameter
    public StrategyType type;

    @Parameter(1)
    public boolean withCache;

    private EmitStrategy emitStrategy;

    private boolean emitFinal;

    @Parameterized.Parameters(name = "{0}_cache:{1}")
    public static Collection<Object[]> getEmitStrategy() {
        return asList(new Object[][]{{StrategyType.ON_WINDOW_UPDATE, true}, {StrategyType.ON_WINDOW_UPDATE, false}, {StrategyType.ON_WINDOW_CLOSE, true}, {StrategyType.ON_WINDOW_CLOSE, false}});
    }

    @Before
    public void before() {
        emitFinal = type.equals(StrategyType.ON_WINDOW_CLOSE);
        emitStrategy = StrategyType.forType(type);
        // Set interval to 0 so that it always tries to emit
        props.setProperty(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, "0");
    }

    @Test
    public void testAggBasic() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<Windowed<String>, String> table2 = builder.stream(topic1, Consumed.with(Serdes.String(), Serdes.String())).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(10), ofMillis(100)).advanceBy(ofMillis(5))).emitStrategy(emitStrategy).aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, setMaterializedCache(Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String())));

        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 = driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            inputTopic1.pipeInput("A", "1", 0L);
            inputTopic1.pipeInput("B", "2", 1L);
            inputTopic1.pipeInput("C", "3", 2L);
            inputTopic1.pipeInput("D", "4", 3L);
            inputTopic1.pipeInput("A", "1", 4L);

            inputTopic1.pipeInput("A", "1", 5L);
            inputTopic1.pipeInput("B", "2", 6L);
            inputTopic1.pipeInput("D", "4", 7L);
            inputTopic1.pipeInput("B", "2", 8L);
            inputTopic1.pipeInput("C", "3", 9L);

            inputTopic1.pipeInput("A", "1", 10L);
            inputTopic1.pipeInput("B", "2", 11L);
            inputTopic1.pipeInput("D", "4", 12L);
            inputTopic1.pipeInput("B", "2", 13L);
            inputTopic1.pipeInput("C", "3", 14L);

            inputTopic1.pipeInput("B", "1", 3L);
            inputTopic1.pipeInput("B", "2", 2L);
            inputTopic1.pipeInput("B", "3", 9L);
        }

        if (emitFinal) {
            // Nothing processed since grace is 100L and no window closes
            assertTrue(supplier.theCapturedProcessor().processed().isEmpty());
        } else {
            assertEquals(asList(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1", 0), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2", 1), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+3", 2), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(0, 10)), "0+4", 3), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1+1", 4), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1+1+1", 5), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1", 5), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2", 6), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2", 6), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(0, 10)), "0+4+4", 7), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+4", 7), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2+2", 8), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2+2", 8), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+3+3", 9), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(5, 15)), "0+3", 9), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1+1", 10), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10, 20)), "0+1", 10), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2+2+2", 11), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10, 20)), "0+2", 11), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+4+4", 12), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(10, 20)), "0+4", 12), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2+2+2+2", 13), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10, 20)), "0+2+2", 13), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(5, 15)), "0+3+3", 14), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(10, 20)), "0+3", 14), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2+2+1", 8), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2+2+1+2", 8), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2+2+1+2+3", 9), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2+2+2+2+3", 13)

            ), supplier.theCapturedProcessor().processed());
        }
    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final String topic2 = "topic2";
        final long grace = emitFinal ? 5L : 100L;

        final KTable<Windowed<String>, String> table1 = builder.stream(topic1, Consumed.with(Serdes.String(), Serdes.String())).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(10), ofMillis(grace)).advanceBy(ofMillis(5))).emitStrategy(emitStrategy).aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, setMaterializedCache(Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String())));

        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table1.toStream().process(supplier);

        final KTable<Windowed<String>, String> table2 = builder.stream(topic2, Consumed.with(Serdes.String(), Serdes.String())).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(10), ofMillis(grace)).advanceBy(ofMillis(5))).emitStrategy(emitStrategy).aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, setMaterializedCache(Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic2-Canonized").withValueSerde(Serdes.String())));
        table2.toStream().process(supplier);

        table1.join(table2, (p1, p2) -> p1 + "%" + p2).toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 = driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> inputTopic2 = driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());

            if (emitFinal) {
                processEmitFinalJoin(inputTopic1, inputTopic2, supplier);
            } else {
                processEmitUpdateJoin(inputTopic1, inputTopic2, supplier);
            }
        }
    }

    private void processEmitFinalJoin(final TestInputTopic<String, String> inputTopic1, final TestInputTopic<String, String> inputTopic2, final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier) {
        inputTopic1.pipeInput("A", "1", 0L);
        inputTopic1.pipeInput("B", "2", 1L);
        inputTopic1.pipeInput("C", "3", 2L);
        inputTopic1.pipeInput("D", "4", 3L);
        inputTopic1.pipeInput("A", "1", 9L);
        inputTopic1.pipeInput("A", "1", 15L);

        final List<MockApiProcessor<Windowed<String>, String, Void, Void>> processors = supplier.capturedProcessors(3);

        processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1+1", 9), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2", 1), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+3", 2), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(0, 10)), "0+4", 3));
        processors.get(1).checkAndClearProcessResult();
        processors.get(2).checkAndClearProcessResult();

        inputTopic1.pipeInput("A", "1", 10L);
        inputTopic1.pipeInput("B", "2", 11L);
        inputTopic1.pipeInput("D", "4", 12L);
        inputTopic1.pipeInput("B", "2", 13L);
        inputTopic1.pipeInput("C", "3", 14L);
        inputTopic1.pipeInput("A", "1", 20L);

        processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1+1", 10), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2+2", 13), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(5, 15)), "0+3", 14), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+4", 12));

        processors.get(1).checkAndClearProcessResult();
        processors.get(2).checkAndClearProcessResult();

        inputTopic2.pipeInput("A", "a", 0L);
        inputTopic2.pipeInput("B", "b", 1L);
        inputTopic2.pipeInput("C", "c", 2L);
        inputTopic2.pipeInput("D", "d", 10L);
        inputTopic2.pipeInput("A", "a", 15L);

        processors.get(0).checkAndClearProcessResult();

        if (withCache) {
            processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+a", 0), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+b", 1), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+c", 2));
            processors.get(2).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1+1%0+a", 9), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2%0+b", 1), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+3%0+c", 2));
        } else {
            processors.get(0).checkAndClearProcessResult();
            processors.get(2).checkAndClearProcessResult();
        }

        inputTopic2.pipeInput("A", "a", 5L);
        inputTopic2.pipeInput("B", "b", 6L);
        inputTopic2.pipeInput("D", "d", 7L);
        inputTopic2.pipeInput("D", "d", 18L);
        inputTopic2.pipeInput("A", "a", 21L);

        processors.get(0).checkAndClearProcessResult();
        if (withCache) {
            processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+a", 5), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+b", 6), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+d+d", 10));
        } else {
            processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+a", 0), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+b", 1), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+c", 2), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+a", 5), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+b", 6), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+d+d", 10));

        }
        processors.get(2).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1+1%0+a", 10), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2+2%0+b", 13), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+4%0+d+d", 12));
    }

    private void processEmitUpdateJoin(final TestInputTopic<String, String> inputTopic1, final TestInputTopic<String, String> inputTopic2, final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier) {
        inputTopic1.pipeInput("A", "1", 0L);
        inputTopic1.pipeInput("B", "2", 1L);
        inputTopic1.pipeInput("C", "3", 2L);
        inputTopic1.pipeInput("D", "4", 3L);
        inputTopic1.pipeInput("A", "1", 9L);

        final List<MockApiProcessor<Windowed<String>, String, Void, Void>> processors = supplier.capturedProcessors(3);

        processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1", 0), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2", 1), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+3", 2), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(0, 10)), "0+4", 3), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1+1", 9), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1", 9));
        processors.get(1).checkAndClearProcessResult();
        processors.get(2).checkAndClearProcessResult();

        inputTopic1.pipeInput("A", "1", 5L);
        inputTopic1.pipeInput("B", "2", 6L);
        inputTopic1.pipeInput("D", "4", 7L);
        inputTopic1.pipeInput("B", "2", 8L);
        inputTopic1.pipeInput("C", "3", 9L);

        processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1+1+1", 9), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1+1", 9), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2", 6), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2", 6), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(0, 10)), "0+4+4", 7), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+4", 7), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2+2", 8), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2+2", 8), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+3+3", 9), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(5, 15)), "0+3", 9));
        processors.get(1).checkAndClearProcessResult();
        processors.get(2).checkAndClearProcessResult();

        inputTopic2.pipeInput("A", "a", 0L);
        inputTopic2.pipeInput("B", "b", 1L);
        inputTopic2.pipeInput("C", "c", 2L);
        inputTopic2.pipeInput("D", "d", 20L);
        inputTopic2.pipeInput("A", "a", 20L);

        processors.get(0).checkAndClearProcessResult();
        processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+a", 0), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+b", 1), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+c", 2), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(15, 25)), "0+d", 20), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(20, 30)), "0+d", 20), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(15, 25)), "0+a", 20), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(20, 30)), "0+a", 20));
        processors.get(2).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1+1+1%0+a", 9), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2+2%0+b", 8), new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(0, 10)), "0+3+3%0+c", 9));

        inputTopic2.pipeInput("A", "a", 5L);
        inputTopic2.pipeInput("B", "b", 6L);
        inputTopic2.pipeInput("D", "d", 7L);
        inputTopic2.pipeInput("D", "d", 18L);
        inputTopic2.pipeInput("A", "a", 21L);

        processors.get(0).checkAndClearProcessResult();
        processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+a+a", 5), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+a", 5), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+b+b", 6), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+b", 6), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(0, 10)), "0+d", 7), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+d", 7), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(10, 20)), "0+d", 18), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(15, 25)), "0+d+d", 20), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(15, 25)), "0+a+a", 21), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(20, 30)), "0+a+a", 21));
        processors.get(2).checkAndClearProcessResult(new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1+1+1%0+a+a", 9), new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1+1%0+a", 9), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0, 10)), "0+2+2+2%0+b+b", 8), new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5, 15)), "0+2+2%0+b", 8), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(0, 10)), "0+4+4%0+d", 7), new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(5, 15)), "0+4%0+d", 7));
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullKey() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String())).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10)).advanceBy(ofMillis(5))).emitStrategy(emitStrategy).aggregate(MockInitializer.STRING_INIT, MockAggregator.toStringInstance("+"), setMaterializedCache(Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonicalized").withValueSerde(Serdes.String())));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamWindowAggregate.class); final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput(null, "1");

            assertThat(appender.getMessages(), hasItem("Skipping record due to null key. topic=[topic] partition=[0] offset=[0]"));
        }
    }

    @Test
    public void shouldLogAndMeterWhenSkippingExpiredWindow() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String())).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(10), ofMillis(90)).advanceBy(ofMillis(5))).emitStrategy(emitStrategy).aggregate(() -> "", MockAggregator.toStringInstance("+"), setMaterializedCache(Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonicalized").withValueSerde(Serdes.String()).withLoggingDisabled().withRetention(Duration.ofMillis(100)))).toStream().map((key, value) -> new KeyValue<>(key.toString(), value)).to("output");

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamWindowAggregate.class); final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("k", "100", 100L);
            inputTopic.pipeInput("k", "0", 0L);
            inputTopic.pipeInput("k", "1", 1L);
            inputTopic.pipeInput("k", "2", 2L);
            inputTopic.pipeInput("k", "3", 3L);
            inputTopic.pipeInput("k", "4", 4L);
            inputTopic.pipeInput("k", "5", 5L);
            inputTopic.pipeInput("k", "6", 6L);
            inputTopic.pipeInput("k", "105", 105L);
            inputTopic.pipeInput("k", "106", 106L);

            assertLatenessMetrics(driver, is(7.0), // how many events get dropped
                    is(100.0), // k:0 is 100ms late, since its time is 0, but it arrives at stream time 100.
                    is(67.9) // (0 + 100 + 99 + 98 + 97 + 96 + 95 + 94 + 0) / 10
            );

            assertThat(appender.getMessages(), hasItems("Skipping record for expired window. topic=[topic] partition=[0] offset=[1] timestamp=[0] window=[0,10) expiration=[10] streamTime=[100]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[2] timestamp=[1] window=[0,10) expiration=[10] streamTime=[100]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[3] timestamp=[2] window=[0,10) expiration=[10] streamTime=[100]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[4] timestamp=[3] window=[0,10) expiration=[10] streamTime=[100]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[5] timestamp=[4] window=[0,10) expiration=[10] streamTime=[100]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[6] timestamp=[5] window=[0,10) expiration=[10] streamTime=[100]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[7] timestamp=[6] window=[0,10) expiration=[10] streamTime=[100]"));

            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic("output", new StringDeserializer(), new StringDeserializer());

            if (emitFinal) {
                // Window close time is 15 when timestamp is 105
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@5/15]", "+5+6", null, 6L)));
                assertEmittedMetrics(driver, is(1.0));
            } else {
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@95/105]", "+100", null, 100L)));
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@100/110]", "+100", null, 100L)));
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@5/15]", "+5", null, 5L)));
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@5/15]", "+5+6", null, 6L)));
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@100/110]", "+100+105", null, 105L)));
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@105/115]", "+105", null, 105L)));
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@100/110]", "+100+105+106", null, 106L)));
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@105/115]", "+105+106", null, 106L)));
            }
            assertTrue(outputTopic.isEmpty());
        }
    }

    @Test
    public void shouldLogAndMeterWhenSkippingExpiredWindowByGrace() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String())).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(10), ofMillis(90)).advanceBy(ofMillis(10))).emitStrategy(emitStrategy).aggregate(() -> "", MockAggregator.toStringInstance("+"), setMaterializedCache(Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonicalized").withValueSerde(Serdes.String()).withLoggingDisabled())).toStream().map((key, value) -> new KeyValue<>(key.toString(), value)).to("output");

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamWindowAggregate.class); final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("k", "100", 200L);
            inputTopic.pipeInput("k", "0", 100L);
            inputTopic.pipeInput("k", "1", 101L);
            inputTopic.pipeInput("k", "2", 102L);
            inputTopic.pipeInput("k", "3", 103L);
            inputTopic.pipeInput("k", "4", 104L);
            inputTopic.pipeInput("k", "5", 105L);
            inputTopic.pipeInput("k", "6", 6L);

            assertLatenessMetrics(driver, is(7.0), is(194.0), is(97.375));

            assertThat(appender.getMessages(), hasItems("Skipping record for expired window. topic=[topic] partition=[0] offset=[1] timestamp=[100] window=[100,110) expiration=[110] streamTime=[200]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[2] timestamp=[101] window=[100,110) expiration=[110] streamTime=[200]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[3] timestamp=[102] window=[100,110) expiration=[110] streamTime=[200]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[4] timestamp=[103] window=[100,110) expiration=[110] streamTime=[200]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[5] timestamp=[104] window=[100,110) expiration=[110] streamTime=[200]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[6] timestamp=[105] window=[100,110) expiration=[110] streamTime=[200]", "Skipping record for expired window. topic=[topic] partition=[0] offset=[7] timestamp=[6] window=[0,10) expiration=[110] streamTime=[200]"));

            if (!emitFinal) {
                final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic("output", new StringDeserializer(), new StringDeserializer());
                assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@200/210]", "+100", null, 200L)));
                assertTrue(outputTopic.isEmpty());
            }
        }
    }

    @Test
    public void shouldNotEmitFinalIfNotProgressEnough() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final long windowSize = 10L;
        final Windows<TimeWindow> windows = TimeWindows.ofSizeAndGrace(ofMillis(windowSize), ofMillis(5)).advanceBy(ofMillis(5));

        try {
            // Always process
            props.put(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0);
            final MockInternalNewProcessorContext<Windowed<String>, Change<String>> context = makeContext(stateDir, windowSize);
            final KStreamWindowAggregate<String, String, String, TimeWindow> processorSupplier = new KStreamWindowAggregate<>(windows, WINDOW_STORE_NAME, emitStrategy, MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER);
            final Processor<String, String, Windowed<String>, Change<String>> processor = processorSupplier.get();
            processor.init(context);

            context.setSystemTimeMs(0L);
            processor.process(new Record<>("A", "1", 0));
            processor.process(new Record<>("B", "2", 5));
            processor.process(new Record<>("C", "3", 15));

            List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> forwarded = context.forwarded();
            List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> expected;
            if (emitFinal) {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1", null), 0)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(0, 10)), new Change<>("0+2", null), 5)));
            } else {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1", null), 0)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(0, 10)), new Change<>("0+2", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(5, 15)), new Change<>("0+2", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(10, 20)), new Change<>("0+3", null), 15)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(15, 25)), new Change<>("0+3", null), 15)));
            }
            assertThat(forwarded, is(expected));
            context.resetForwards();

            processor.process(new Record<>("D", "4", 15));
            forwarded = context.forwarded();
            if (emitFinal) {
                // None emitted because observedTime doesn't progress
                assertTrue(forwarded.isEmpty());
            } else {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("D", new TimeWindow(10, 20)), new Change<>("0+4", null), 15)), new CapturedForward<>(new Record<>(new Windowed<>("D", new TimeWindow(15, 25)), new Change<>("0+4", null), 15)));
                assertThat(forwarded, is(expected));
            }
            context.resetForwards();

            processor.process(new Record<>("E", "5", 19));
            forwarded = context.forwarded();
            if (emitFinal) {
                // None emitted because observedTime doesn't progress enough to cover new windows
                assertTrue(forwarded.isEmpty());
            } else {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("E", new TimeWindow(10, 20)), new Change<>("0+5", null), 19)), new CapturedForward<>(new Record<>(new Windowed<>("E", new TimeWindow(15, 25)), new Change<>("0+5", null), 19)));
                assertThat(forwarded, is(expected));
            }

            context.getStateStore(WINDOW_STORE_NAME).close();
        } finally {
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldEmitWithInterval0() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final long windowSize = 10L;
        final Windows<TimeWindow> windows = TimeWindows.ofSizeAndGrace(ofMillis(windowSize), ofMillis(5)).advanceBy(ofMillis(5));

        try {
            // Always process
            props.put(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0);
            final MockInternalNewProcessorContext<Windowed<String>, Change<String>> context = makeContext(stateDir, windowSize);
            final KStreamWindowAggregate<String, String, String, TimeWindow> processorSupplier = new KStreamWindowAggregate<>(windows, WINDOW_STORE_NAME, emitStrategy, MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER);
            final Processor<String, String, Windowed<String>, Change<String>> processor = processorSupplier.get();
            processor.init(context);

            context.setSystemTimeMs(0L);
            processor.process(new Record<>("A", "1", 0));
            processor.process(new Record<>("A", "1", 5));
            processor.process(new Record<>("B", "2", 10));
            processor.process(new Record<>("C", "3", 15));
            processor.process(new Record<>("D", "4", 20));

            final List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> forwarded = context.forwarded();
            final List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> expected;
            if (emitFinal) {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1+1", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(5, 15)), new Change<>("0+1", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(5, 15)), new Change<>("0+2", null), 10)));
            } else {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1", null), 0)), new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1+1", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(5, 15)), new Change<>("0+1", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(5, 15)), new Change<>("0+2", null), 10)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(10, 20)), new Change<>("0+2", null), 10)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(10, 20)), new Change<>("0+3", null), 15)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(15, 25)), new Change<>("0+3", null), 15)), new CapturedForward<>(new Record<>(new Windowed<>("D", new TimeWindow(15, 25)), new Change<>("0+4", null), 20)), new CapturedForward<>(new Record<>(new Windowed<>("D", new TimeWindow(20, 30)), new Change<>("0+4", null), 20)));
            }
            assertThat(forwarded, is(expected));
            context.getStateStore(WINDOW_STORE_NAME).close();
        } finally {
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldEmitWithLargeInterval() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final long windowSize = 10L;
        final Windows<TimeWindow> windows = TimeWindows.ofSizeAndGrace(ofMillis(windowSize), ofMillis(5)).advanceBy(ofMillis(5));

        try {
            // Emit final every second
            props.put(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 1000L);
            final MockInternalNewProcessorContext<Windowed<String>, Change<String>> context = makeContext(stateDir, windowSize);
            final KStreamWindowAggregate<String, String, String, TimeWindow> processorSupplier = new KStreamWindowAggregate<>(windows, WINDOW_STORE_NAME, emitStrategy, MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER);
            final Processor<String, String, Windowed<String>, Change<String>> processor = processorSupplier.get();
            processor.init(context);

            context.setSystemTimeMs(0L);
            processor.process(new Record<>("A", "1", 0));
            processor.process(new Record<>("A", "1", 5));
            processor.process(new Record<>("B", "2", 10));
            processor.process(new Record<>("C", "3", 15));

            List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> forwarded = context.forwarded();
            if (emitFinal) {
                assertTrue(forwarded.isEmpty());
            } else {
                final List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1", null), 0)), new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1+1", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(5, 15)), new Change<>("0+1", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(5, 15)), new Change<>("0+2", null), 10)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(10, 20)), new Change<>("0+2", null), 10)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(10, 20)), new Change<>("0+3", null), 15)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(15, 25)), new Change<>("0+3", null), 15)));
                assertThat(forwarded, is(expected));
            }
            context.resetForwards();

            // Progress
            context.setSystemTimeMs(10000L);
            processor.process(new Record<>("D", "4", 20));

            forwarded = context.forwarded();
            List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> expected;
            if (emitFinal) {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1+1", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(5, 15)), new Change<>("0+1", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(5, 15)), new Change<>("0+2", null), 10)));
            } else {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("D", new TimeWindow(15, 25)), new Change<>("0+4", null), 20)), new CapturedForward<>(new Record<>(new Windowed<>("D", new TimeWindow(20, 30)), new Change<>("0+4", null), 20)));
            }
            assertThat(forwarded, is(expected));
            context.resetForwards();

            // Progress
            context.setSystemTimeMs(10100L);
            processor.process(new Record<>("E", "5", 40));

            forwarded = context.forwarded();
            if (emitFinal) {
                assertTrue(forwarded.isEmpty());
            } else {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("E", new TimeWindow(35, 45)), new Change<>("0+5", null), 40)), new CapturedForward<>(new Record<>(new Windowed<>("E", new TimeWindow(40, 50)), new Change<>("0+5", null), 40)));
                assertThat(forwarded, is(expected));
            }

            context.getStateStore(WINDOW_STORE_NAME).close();
        } finally {
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldEmitFromLastEmitTime() throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final long windowSize = 10L;
        final Windows<TimeWindow> windows = TimeWindows.ofSizeAndGrace(ofMillis(windowSize), ofMillis(5)).advanceBy(ofMillis(5));

        try {
            // Always process
            props.put(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0);
            final MockInternalNewProcessorContext<Windowed<String>, Change<String>> context = makeContext(stateDir, windowSize);
            final KStreamWindowAggregate<String, String, String, TimeWindow> processorSupplier = new KStreamWindowAggregate<>(windows, WINDOW_STORE_NAME, emitStrategy, MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER);
            final Processor<String, String, Windowed<String>, Change<String>> processor = processorSupplier.get();
            processor.init(context);

            context.setSystemTimeMs(0L);
            processor.process(new Record<>("A", "1", 0));
            processor.process(new Record<>("B", "2", 5));
            processor.process(new Record<>("C", "3", 15));

            List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> forwarded = context.forwarded();
            List<CapturedForward<? extends Windowed<String>, ? extends Change<String>>> expected;
            if (emitFinal) {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1", null), 0)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(0, 10)), new Change<>("0+2", null), 5)));
            } else {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("A", new TimeWindow(0, 10)), new Change<>("0+1", null), 0)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(0, 10)), new Change<>("0+2", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(5, 15)), new Change<>("0+2", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(10, 20)), new Change<>("0+3", null), 15)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(15, 25)), new Change<>("0+3", null), 15)));
            }
            assertThat(forwarded, is(expected));
            context.resetForwards();

            final Processor<String, String, Windowed<String>, Change<String>> newProcessor = processorSupplier.get();
            newProcessor.init(context);
            newProcessor.process(new Record<>("D", "4", 25));
            forwarded = context.forwarded();
            if (emitFinal) {
                // Don't output old windows for new processor
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("B", new TimeWindow(5, 15)), new Change<>("0+2", null), 5)), new CapturedForward<>(new Record<>(new Windowed<>("C", new TimeWindow(10, 20)), new Change<>("0+3", null), 15)));
            } else {
                expected = asList(new CapturedForward<>(new Record<>(new Windowed<>("D", new TimeWindow(20, 30)), new Change<>("0+4", null), 25)), new CapturedForward<>(new Record<>(new Windowed<>("D", new TimeWindow(25, 35)), new Change<>("0+4", null), 25)));
            }
            assertThat(forwarded, is(expected));
            context.resetForwards();

            context.getStateStore(WINDOW_STORE_NAME).close();
        } finally {
            Utils.delete(stateDir);
        }
    }

    @Test
    public void showThrowIfEmitFinalUsedWithUnlimitedWindow() {
        if (emitFinal) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new KStreamWindowAggregate<>(UnlimitedWindows.of(), WINDOW_STORE_NAME, emitStrategy, MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER));
            assertThat(e.getMessage(), is("ON_WINDOW_CLOSE strategy is only supported for " + "TimeWindows and SlidingWindows for TimeWindowedKStream"));
        } else {
            new KStreamWindowAggregate<>(UnlimitedWindows.of(), WINDOW_STORE_NAME, emitStrategy, MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER);
        }
    }

    private TimestampedWindowStore<String, String> getWindowStore(final long windowSize) {
        final WindowBytesStoreSupplier supplier;
        if (emitFinal) {
            supplier = RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(WINDOW_STORE_NAME, Duration.ofDays(1), Duration.ofMillis(windowSize), false, false);
        } else {
            supplier = Stores.persistentTimestampedWindowStore(WINDOW_STORE_NAME, Duration.ofDays(1), Duration.ofMillis(windowSize), false);
        }

        return Stores.timestampedWindowStoreBuilder(supplier, Serdes.String(), Serdes.String()).withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                .withCachingDisabled() // Caching is not supported by MockProcessorContext.
                .build();
    }

    private MockInternalNewProcessorContext<Windowed<String>, Change<String>> makeContext(final File stateDir, final long windowSize) {
        final MockInternalNewProcessorContext<Windowed<String>, Change<String>> context = new MockInternalNewProcessorContext<>(props, new TaskId(0, 0), stateDir);

        context.setCurrentNode(new ProcessorNode("testNode"));

        // Create, initialize, and register the state store.
        final TimestampedWindowStore<String, String> store = getWindowStore(windowSize);
        store.init(context.getStateStoreContext(), store);
        context.getStateStoreContext().register(store, null);

        return context;
    }

    private void assertLatenessMetrics(final TopologyTestDriver driver, final Matcher<Object> dropTotal, final Matcher<Object> maxLateness, final Matcher<Object> avgLateness) {

        final MetricName dropTotalMetric;
        final MetricName dropRateMetric;
        final MetricName latenessMaxMetric;
        final MetricName latenessAvgMetric;
        dropTotalMetric = new MetricName("dropped-records-total", "stream-task-metrics", "The total number of dropped records", mkMap(mkEntry("thread-id", threadId), mkEntry("task-id", "0_0")));
        dropRateMetric = new MetricName("dropped-records-rate", "stream-task-metrics", "The average number of dropped records per second", mkMap(mkEntry("thread-id", threadId), mkEntry("task-id", "0_0")));
        latenessMaxMetric = new MetricName("record-lateness-max", "stream-task-metrics", "The observed maximum lateness of records in milliseconds, measured by comparing the record " + "timestamp with the current stream time", mkMap(mkEntry("thread-id", threadId), mkEntry("task-id", "0_0")));
        latenessAvgMetric = new MetricName("record-lateness-avg", "stream-task-metrics", "The observed average lateness of records in milliseconds, measured by comparing the record " + "timestamp with the current stream time", mkMap(mkEntry("thread-id", threadId), mkEntry("task-id", "0_0")));

        assertThat(driver.metrics().get(dropTotalMetric).metricValue(), dropTotal);
        assertThat(driver.metrics().get(dropRateMetric).metricValue(), not(0.0));
        assertThat(driver.metrics().get(latenessMaxMetric).metricValue(), maxLateness);
        assertThat(driver.metrics().get(latenessAvgMetric).metricValue(), avgLateness);
    }

    private void assertEmittedMetrics(final TopologyTestDriver driver, final Matcher<Object> emittedTotal) {

        final MetricName emittedTotalMetric;
        final MetricName emittedRateMetric;
        emittedTotalMetric = new MetricName("window-aggregate-final-emit-total", "stream-processor-node-metrics", "The total number of emit final records", mkMap(mkEntry("thread-id", threadId), mkEntry("task-id", "0_0"), mkEntry("processor-node-id", "KSTREAM-AGGREGATE-0000000001")));
        emittedRateMetric = new MetricName("window-aggregate-final-emit-rate", "stream-processor-node-metrics", "The average number of emit final records per second", mkMap(mkEntry("thread-id", threadId), mkEntry("task-id", "0_0"), mkEntry("processor-node-id", "KSTREAM-AGGREGATE-0000000001")));

        assertThat(driver.metrics().get(emittedTotalMetric).metricValue(), emittedTotal);
        assertThat(driver.metrics().get(emittedRateMetric).metricValue(), not(0.0));
    }

    private <K, V, S extends StateStore> Materialized<K, V, S> setMaterializedCache(final Materialized<K, V, S> materialized) {
        if (withCache) {
            return materialized.withCachingEnabled();
        }
        return materialized.withCachingDisabled();
    }
}