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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerRecordsTest {

    @Test
    public void iterator() throws Exception {

        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new LinkedHashMap<>();

        String topic = "topic";
        records.put(new TopicPartition(topic, 0), new ArrayList<>());
        ConsumerRecord<Integer, String> record1 = new ConsumerRecord<>(topic, 1, 0, 0L, TimestampType.CREATE_TIME, 0, 0, 1, "value1", new RecordHeaders(), Optional.empty());
        ConsumerRecord<Integer, String> record2 = new ConsumerRecord<>(topic, 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, 2, "value2", new RecordHeaders(), Optional.empty());
        records.put(new TopicPartition(topic, 1), Arrays.asList(record1, record2));
        records.put(new TopicPartition(topic, 2), new ArrayList<>());

        ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
        Iterator<ConsumerRecord<Integer, String>> iter = consumerRecords.iterator();

        int c = 0;
        for (; iter.hasNext(); c++) {
            ConsumerRecord<Integer, String> record = iter.next();
            assertEquals(1, record.partition());
            assertEquals(topic, record.topic());
            assertEquals(c, record.offset());
        }
        assertEquals(2, c);
    }
}
