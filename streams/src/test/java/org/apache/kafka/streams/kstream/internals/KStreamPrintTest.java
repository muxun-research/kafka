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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KStreamPrintTest {

    private ByteArrayOutputStream byteOutStream;
    private Processor<Integer, String, Void, Void> printProcessor;

    @Mock
    private ProcessorContext<Void, Void> processorContext;

    @Before
    public void setUp() {
        byteOutStream = new ByteArrayOutputStream();

        final KStreamPrint<Integer, String> kStreamPrint = new KStreamPrint<>(new PrintForeachAction<>(byteOutStream, (key, value) -> String.format("%d, %s", key, value), "test-stream"));

        printProcessor = kStreamPrint.get();

        printProcessor.init(processorContext);
    }

    @Test
    public void testPrintStreamWithProvidedKeyValueMapper() {
        final List<KeyValue<Integer, String>> inputRecords = Arrays.asList(new KeyValue<>(0, "zero"), new KeyValue<>(1, "one"), new KeyValue<>(2, "two"), new KeyValue<>(3, "three"));

        final String[] expectedResult = {"[test-stream]: 0, zero", "[test-stream]: 1, one", "[test-stream]: 2, two", "[test-stream]: 3, three"};

        for (final KeyValue<Integer, String> record: inputRecords) {
            final Record<Integer, String> r = new Record<>(record.key, record.value, 0L);
            printProcessor.process(r);
        }
        printProcessor.close();

        final String[] flushOutDatas = new String(byteOutStream.toByteArray(), StandardCharsets.UTF_8).split("\\r*\\n");
        for (int i = 0; i < flushOutDatas.length; i++) {
            assertEquals(expectedResult[i], flushOutDatas[i]);
        }
    }

}
