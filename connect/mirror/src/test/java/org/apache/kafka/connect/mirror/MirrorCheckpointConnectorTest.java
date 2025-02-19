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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class MirrorCheckpointConnectorTest {

    private static final String CONSUMER_GROUP = "consumer-group-1";

    @Test
    public void testMirrorCheckpointConnectorDisabled() {
        // disable the checkpoint emission
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps("emit.checkpoints.enabled", "false"));

        Set<String> knownConsumerGroups = new HashSet<>();
        knownConsumerGroups.add(CONSUMER_GROUP);
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups, config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size(), "MirrorCheckpointConnector not disabled");
    }

    @Test
    public void testMirrorCheckpointConnectorEnabled() {
        // enable the checkpoint emission
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps("emit.checkpoints.enabled", "true"));

        Set<String> knownConsumerGroups = new HashSet<>();
        knownConsumerGroups.add(CONSUMER_GROUP);
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups, config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect 1 task will be created
        assertEquals(1, output.size(), "MirrorCheckpointConnectorEnabled for " + CONSUMER_GROUP + " has incorrect size");
        assertEquals(CONSUMER_GROUP, output.get(0).get(MirrorCheckpointConfig.TASK_CONSUMER_GROUPS), "MirrorCheckpointConnectorEnabled for " + CONSUMER_GROUP + " failed");
    }

    @Test
    public void testNoConsumerGroup() {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(new HashSet<>(), config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size(), "ConsumerGroup shouldn't exist");
    }

    @Test
    public void testReplicationDisabled() {
        // disable the replication
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps("enabled", "false"));

        Set<String> knownConsumerGroups = new HashSet<>();
        knownConsumerGroups.add(CONSUMER_GROUP);
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups, config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size(), "Replication isn't disabled");
    }

    @Test
    public void testReplicationEnabled() {
        // enable the replication
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps("enabled", "true"));

        Set<String> knownConsumerGroups = new HashSet<>();
        knownConsumerGroups.add(CONSUMER_GROUP);
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups, config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect 1 task will be created
        assertEquals(1, output.size(), "Replication for consumer-group-1 has incorrect size");
        assertEquals(CONSUMER_GROUP, output.get(0).get(MirrorCheckpointConfig.TASK_CONSUMER_GROUPS), "Replication for consumer-group-1 failed");
    }

    @Test
    public void testFindConsumerGroups() throws Exception {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(Collections.emptySet(), config);
        connector = spy(connector);

        Collection<ConsumerGroupListing> groups = Arrays.asList(new ConsumerGroupListing("g1", true), new ConsumerGroupListing("g2", false));
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        doReturn(groups).when(connector).listConsumerGroups();
        doReturn(true).when(connector).shouldReplicateByTopicFilter(anyString());
        doReturn(true).when(connector).shouldReplicateByGroupFilter(anyString());
        doReturn(offsets).when(connector).listConsumerGroupOffsets(anyString());
        Set<String> groupFound = connector.findConsumerGroups();

        Set<String> expectedGroups = groups.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toSet());
        assertEquals(expectedGroups, groupFound, "Expected groups are not the same as findConsumerGroups");

        doReturn(false).when(connector).shouldReplicateByTopicFilter(anyString());
        Set<String> topicFilterGroupFound = connector.findConsumerGroups();
        assertEquals(Collections.emptySet(), topicFilterGroupFound);
    }

    @Test
    public void testFindConsumerGroupsInCommonScenarios() throws Exception {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(Collections.emptySet(), config);
        connector = spy(connector);

        Collection<ConsumerGroupListing> groups = Arrays.asList(new ConsumerGroupListing("g1", true), new ConsumerGroupListing("g2", false), new ConsumerGroupListing("g3", false), new ConsumerGroupListing("g4", false));
        Map<TopicPartition, OffsetAndMetadata> offsetsForGroup1 = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> offsetsForGroup2 = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> offsetsForGroup3 = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> offsetsForGroup4 = new HashMap<>();
        offsetsForGroup1.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        offsetsForGroup1.put(new TopicPartition("t2", 0), new OffsetAndMetadata(0));
        offsetsForGroup2.put(new TopicPartition("t2", 0), new OffsetAndMetadata(0));
        offsetsForGroup2.put(new TopicPartition("t3", 0), new OffsetAndMetadata(0));
        offsetsForGroup3.put(new TopicPartition("t3", 0), new OffsetAndMetadata(0));
        offsetsForGroup4.put(new TopicPartition("t3", 0), new OffsetAndMetadata(0));
        doReturn(groups).when(connector).listConsumerGroups();
        doReturn(false).when(connector).shouldReplicateByTopicFilter("t1");
        doReturn(true).when(connector).shouldReplicateByTopicFilter("t2");
        doReturn(false).when(connector).shouldReplicateByTopicFilter("t3");
        doReturn(true).when(connector).shouldReplicateByGroupFilter("g1");
        doReturn(true).when(connector).shouldReplicateByGroupFilter("g2");
        doReturn(true).when(connector).shouldReplicateByGroupFilter("g3");
        doReturn(false).when(connector).shouldReplicateByGroupFilter("g4");
        doReturn(offsetsForGroup1).when(connector).listConsumerGroupOffsets("g1");
        doReturn(offsetsForGroup2).when(connector).listConsumerGroupOffsets("g2");
        doReturn(offsetsForGroup3).when(connector).listConsumerGroupOffsets("g3");
        doReturn(offsetsForGroup4).when(connector).listConsumerGroupOffsets("g4");

        Set<String> groupFound = connector.findConsumerGroups();
        Set<String> verifiedSet = new HashSet<>();
        verifiedSet.add("g1");
        verifiedSet.add("g2");
        assertEquals(groupFound, verifiedSet);
    }

}
