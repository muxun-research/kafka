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

package org.apache.kafka.image.publisher;

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.raft.LeaderAndEpoch;


/**
 * Publishes metadata deltas which we have loaded from the log and snapshots.
 * <p>
 * Publishers receive a stream of callbacks from the metadata loader which keeps them notified
 * of the latest cluster metadata. This interface abstracts away some of the complications of
 * following the cluster metadata. For example, if the loader needs to read a snapshot, it will
 * present the contents of the snapshot in the form of a delta from the previous state.
 */
public interface MetadataPublisher extends AutoCloseable {
    /**
     * Returns the name of this publisher.
     * @return The publisher name.
     */
    String name();

    /**
     * Handle a change in the current controller.
     * @param newLeaderAndEpoch The new quorum leader and epoch. The new leader will be
     *                          OptionalInt.empty if there is currently no active controller.
     */
    default void onControllerChange(LeaderAndEpoch newLeaderAndEpoch) {
    }

    /**
     * Publish a new cluster metadata snapshot that we loaded.
     * @param delta    The delta between the previous state and the new one.
     * @param newImage The complete new state.
     * @param manifest A manifest which describes the contents of what was published.
     *                 If we loaded a snapshot, this will be a SnapshotManifest.
     *                 If we loaded a log delta, this will be a LogDeltaManifest.
     */
    void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest);

    /**
     * Close this metadata publisher and free any associated resources.
     */
    default void close() throws Exception {
    }
}
