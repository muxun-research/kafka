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

package org.apache.kafka.streams.processor;

import org.apache.kafka.common.TopicPartition;

/**
 * Class for listening to various states of the restoration process of a StateStore.
 *
 * <p>
 * When calling {@link org.apache.kafka.streams.KafkaStreams#setGlobalStateRestoreListener(StateRestoreListener)}
 * the passed instance is expected to be stateless since the {@code StateRestoreListener} is shared
 * across all {@link org.apache.kafka.streams.processor.internals.StreamThread} instances.
 *
 * <p>
 * Users desiring stateful operations will need to provide synchronization internally in
 * the {@code StateRestorerListener} implementation.
 *
 * <p>
 * Note that this listener is only registered at the per-client level and users can base on the {@code storeName}
 * parameter to define specific monitoring for different {@link StateStore}s. There is another
 * {@link StateRestoreCallback} interface which is registered via the
 * {@link StateStoreContext#register(StateStore, StateRestoreCallback, CommitCallback)}
 * function per-store, and it is used to apply the fetched changelog records into the local state store during restoration.
 * These two interfaces serve different restoration purposes and users should not try to implement both of them in a single
 * class during state store registration.
 *
 * <p>
 * Also note that the update process of standby tasks is not monitored via this interface, since a standby task does
 * note actually <it>restore</it> state, but keeps updating its state from the changelogs written by the active task
 * which does not ever finish.
 *
 * <p>
 * Incremental updates are exposed so users can estimate how much progress has been made.
 */
public interface StateRestoreListener {

    /**
     * Method called at the very beginning of {@link StateStore} restoration.
     *
     * @param topicPartition the TopicPartition containing the values to restore
     * @param storeName      the name of the store undergoing restoration
     * @param startingOffset the starting offset of the entire restoration process for this TopicPartition
     * @param endingOffset   the exclusive ending offset of the entire restoration process for this TopicPartition
     */
    void onRestoreStart(final TopicPartition topicPartition,
                        final String storeName,
                        final long startingOffset,
                        final long endingOffset);

    /**
     * Method called after restoring a batch of records.  In this case the maximum size of the batch is whatever
     * the value of the MAX_POLL_RECORDS is set to.
     *
     * This method is called after restoring each batch and it is advised to keep processing to a minimum.
     * Any heavy processing will hold up recovering the next batch, hence slowing down the restore process as a
     * whole.
     *
     * If you need to do any extended processing or connecting to an external service consider doing so asynchronously.
     *
     * @param topicPartition the TopicPartition containing the values to restore
     * @param storeName the name of the store undergoing restoration
     * @param batchEndOffset the inclusive ending offset for the current restored batch for this TopicPartition
     * @param numRestored the total number of records restored in this batch for this TopicPartition
     */
    void onBatchRestored(final TopicPartition topicPartition,
                         final String storeName,
                         final long batchEndOffset,
                         final long numRestored);

    /**
     * Method called when restoring the {@link StateStore} is complete.
     *
     * @param topicPartition the TopicPartition containing the values to restore
     * @param storeName the name of the store just restored
     * @param totalRestored the total number of records restored for this TopicPartition
     */
    void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored);

    /**
     * Method called when restoring the {@link StateStore} is suspended due to the task being migrated out of the host.
     * If the migrated task is recycled or re-assigned back to the current host, another
     * {@link #onRestoreStart(TopicPartition, String, long, long)} would be called.
     * @param topicPartition the {@link TopicPartition} containing the values to restore
     * @param storeName      the name of the store just restored
     * @param totalRestored  the total number of records restored for this TopicPartition before being paused
     */
    default void onRestoreSuspended(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
    }
}
