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
package org.apache.kafka.streams.tests;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

import static org.apache.kafka.streams.tests.SmokeTestUtil.intSerde;
import static org.apache.kafka.streams.tests.SmokeTestUtil.stringSerde;

public class StreamsUpgradeTest {

    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsUpgradeTest requires one argument (properties-file) but provided none");
        }
        final String propFileName = args[0];

        final Properties streamsProperties = Utils.loadProps(propFileName);

        System.out.println("StreamsTest instance started (StreamsUpgradeTest v2.5)");
        System.out.println("props=" + streamsProperties);

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Integer> dataTable = builder.table("data", Consumed.with(stringSerde, intSerde));
        final KStream<String, Integer> dataStream = dataTable.toStream();
        dataStream.process(printProcessorSupplier("data"));
        dataStream.to("echo");

        final boolean runFkJoin = Boolean.parseBoolean(streamsProperties.getProperty("test.run_fk_join", "false"));
        if (runFkJoin) {
            try {
                final KTable<Integer, String> fkTable = builder.table("fk", Consumed.with(intSerde, stringSerde));
                buildFKTable(dataTable, fkTable);
            } catch (final Exception e) {
                System.err.println("Caught " + e.getMessage());
            }
        }

        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsUpgradeTest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        config.putAll(streamsProperties);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            System.out.println("UPGRADE-TEST-CLIENT-CLOSED");
            System.out.flush();
        }));
    }

    private static void buildFKTable(final KTable<String, Integer> primaryTable, final KTable<Integer, String> otherTable) {
        final KStream<String, String> kStream = primaryTable.join(otherTable, v -> v, (k0, v0) -> v0).toStream();
        kStream.process(printProcessorSupplier("fk"));
        kStream.to("fk-result", Produced.with(stringSerde, stringSerde));
    }

    private static <K, V> ProcessorSupplier<K, V> printProcessorSupplier(final String topic) {
        return () -> new AbstractProcessor<K, V>() {
            private int numRecordsProcessed = 0;

            @Override
            public void init(final ProcessorContext context) {
                System.out.println("[2.5] initializing processor: topic=" + topic + " taskId=" + context.taskId());
                numRecordsProcessed = 0;
            }

            @Override
            public void process(final K key, final V value) {
                numRecordsProcessed++;
                if (numRecordsProcessed % 100 == 0) {
                    System.out.println("processed " + numRecordsProcessed + " records from topic=" + topic);
                }
            }

            @Override
            public void close() {
            }
        };
    }
}
