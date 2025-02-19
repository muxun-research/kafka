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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DeleteTopicsResultTest {

    @Test
    public void testDeleteTopicsResultWithNames() {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.complete(null);
        Map<String, KafkaFuture<Void>> topicNames = Collections.singletonMap("foo", future);

        DeleteTopicsResult topicNameFutures = DeleteTopicsResult.ofTopicNames(topicNames);

        assertEquals(topicNames, topicNameFutures.topicNameValues());
        assertNull(topicNameFutures.topicIdValues());
        assertTrue(topicNameFutures.all().isDone());
    }

    @Test
    public void testDeleteTopicsResultWithIds() {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.complete(null);
        Map<Uuid, KafkaFuture<Void>> topicIds = Collections.singletonMap(Uuid.randomUuid(), future);

        DeleteTopicsResult topicIdFutures = DeleteTopicsResult.ofTopicIds(topicIds);

        assertEquals(topicIds, topicIdFutures.topicIdValues());
        assertNull(topicIdFutures.topicNameValues());
        assertTrue(topicIdFutures.all().isDone());
    }

    @Test
    public void testInvalidConfigurations() {
        assertThrows(IllegalArgumentException.class, () -> new DeleteTopicsResult(null, null));
        assertThrows(IllegalArgumentException.class, () -> new DeleteTopicsResult(Collections.emptyMap(), Collections.emptyMap()));
    }
}
