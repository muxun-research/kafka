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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.*;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.image.writer.ImageReWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.FakePartitionRegistrationType.*;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ControllerMetadataMetricsPublisherTest {
    static class TestEnv implements AutoCloseable {
        MockFaultHandler faultHandler = new MockFaultHandler("ControllerMetadataMetricsPublisher");
        ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.empty());
        ControllerMetadataMetricsPublisher publisher = new ControllerMetadataMetricsPublisher(metrics, faultHandler);

        @Override
        public void close() {
            publisher.close();
            faultHandler.maybeRethrowFirstException();
        }
    }

    @Test
    public void testMetricsBeforePublishing() {
        try (TestEnv env = new TestEnv()) {
            assertEquals(0, env.metrics.activeBrokerCount());
            assertEquals(0, env.metrics.globalTopicCount());
            assertEquals(0, env.metrics.globalPartitionCount());
            assertEquals(0, env.metrics.offlinePartitionCount());
            assertEquals(0, env.metrics.preferredReplicaImbalanceCount());
            assertEquals(0, env.metrics.metadataErrorCount());
        }
    }

    static MetadataImage fakeImageFromTopicsImage(TopicsImage topicsImage) {
        return new MetadataImage(MetadataProvenance.EMPTY, FeaturesImage.EMPTY, ClusterImage.EMPTY, topicsImage, ConfigurationsImage.EMPTY, ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY, AclsImage.EMPTY, ScramImage.EMPTY);
    }

    static final TopicsImage TOPICS_IMAGE1;

    static final MetadataImage IMAGE1;

    static {
        TOPICS_IMAGE1 = fakeTopicsImage(fakeTopicImage("foo", Uuid.fromString("JKNp6fQaT-icHxh654ok-w"), fakePartitionRegistration(NORMAL)), fakeTopicImage("bar", Uuid.fromString("pEMSdUVWTXaFQUzLTznFSw"), fakePartitionRegistration(NORMAL), fakePartitionRegistration(NORMAL), fakePartitionRegistration(NON_PREFERRED_LEADER)), fakeTopicImage("quux", Uuid.fromString("zkUT4lyyRke6VIaTw6RQWg"), fakePartitionRegistration(OFFLINE), fakePartitionRegistration(OFFLINE), fakePartitionRegistration(OFFLINE)));
        IMAGE1 = fakeImageFromTopicsImage(TOPICS_IMAGE1);
    }

    @Test
    public void testPublish() {
        try (TestEnv env = new TestEnv()) {
            assertEquals(0, env.metrics.activeBrokerCount());
            assertEquals(0, env.metrics.globalTopicCount());
            assertEquals(0, env.metrics.globalPartitionCount());
            assertEquals(0, env.metrics.offlinePartitionCount());
            assertEquals(0, env.metrics.preferredReplicaImbalanceCount());
            assertEquals(0, env.metrics.metadataErrorCount());
        }
    }

    static LoaderManifest fakeManifest(boolean isSnapshot) {
        if (isSnapshot) {
            return new SnapshotManifest(MetadataProvenance.EMPTY, 0);
        } else {
            return new LogDeltaManifest(MetadataProvenance.EMPTY, LeaderAndEpoch.UNKNOWN, 0, 0, 0);
        }
    }

    @Test
    public void testLoadSnapshot() {
        try (TestEnv env = new TestEnv()) {
            MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
            ImageReWriter writer = new ImageReWriter(delta);
            IMAGE1.write(writer, new ImageWriterOptions.Builder().setMetadataVersion(delta.image().features().metadataVersion()).build());
            env.publisher.onMetadataUpdate(delta, IMAGE1, fakeManifest(true));
            assertEquals(0, env.metrics.activeBrokerCount());
            assertEquals(3, env.metrics.globalTopicCount());
            assertEquals(7, env.metrics.globalPartitionCount());
            assertEquals(3, env.metrics.offlinePartitionCount());
            assertEquals(4, env.metrics.preferredReplicaImbalanceCount());
            assertEquals(0, env.metrics.metadataErrorCount());
        }
    }
}
