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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TxnOffsetCommitRequestTest extends OffsetCommitRequestTest {

    private static String transactionalId = "transactionalId";
    private static int producerId = 10;
    private static short producerEpoch = 1;
    private static int generationId = 5;
    private static Map<TopicPartition, CommittedOffset> offsets = new HashMap<>();
    private static TxnOffsetCommitRequest.Builder builder;
    private static TxnOffsetCommitRequest.Builder builderWithGroupMetadata;

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
        offsets.clear();
        offsets.put(new TopicPartition(topicOne, partitionOne), new CommittedOffset(offset, metadata, Optional.of((int) leaderEpoch)));
        offsets.put(new TopicPartition(topicTwo, partitionTwo), new CommittedOffset(offset, metadata, Optional.of((int) leaderEpoch)));

        builder = new TxnOffsetCommitRequest.Builder(transactionalId, groupId, producerId, producerEpoch, offsets);

        builderWithGroupMetadata = new TxnOffsetCommitRequest.Builder(transactionalId, groupId, producerId, producerEpoch, offsets, memberId, generationId, Optional.of(groupInstanceId));
    }

    @Test
    @Override
    public void testConstructor() {

        Map<TopicPartition, Errors> errorsMap = new HashMap<>();
        errorsMap.put(new TopicPartition(topicOne, partitionOne), Errors.NOT_COORDINATOR);
        errorsMap.put(new TopicPartition(topicTwo, partitionTwo), Errors.NOT_COORDINATOR);

        List<TxnOffsetCommitRequestTopic> expectedTopics = Arrays.asList(new TxnOffsetCommitRequestTopic().setName(topicOne).setPartitions(Collections.singletonList(new TxnOffsetCommitRequestPartition().setPartitionIndex(partitionOne).setCommittedOffset(offset).setCommittedLeaderEpoch(leaderEpoch).setCommittedMetadata(metadata))), new TxnOffsetCommitRequestTopic().setName(topicTwo).setPartitions(Collections.singletonList(new TxnOffsetCommitRequestPartition().setPartitionIndex(partitionTwo).setCommittedOffset(offset).setCommittedLeaderEpoch(leaderEpoch).setCommittedMetadata(metadata))));

        for (short version : ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            final TxnOffsetCommitRequest request;
            if (version < 3) {
                request = builder.build(version);
            } else {
                request = builderWithGroupMetadata.build(version);
            }
            assertEquals(offsets, request.offsets());
            assertEquals(expectedTopics, TxnOffsetCommitRequest.getTopics(request.offsets()));

            TxnOffsetCommitResponse response = request.getErrorResponse(throttleTimeMs, Errors.NOT_COORDINATOR.exception());

            assertEquals(errorsMap, response.errors());
            assertEquals(Collections.singletonMap(Errors.NOT_COORDINATOR, 2), response.errorCounts());
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        }
    }
}
