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

package org.apache.kafka.server.common;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds a range of Producer IDs used for Transactional and EOS producers.
 * <p>
 * The start and end of the ID block are inclusive.
 */
public class ProducerIdsBlock {
    public static final int PRODUCER_ID_BLOCK_SIZE = 1000;

    public static final ProducerIdsBlock EMPTY = new ProducerIdsBlock(-1, 0, 0);

    private final int assignedBrokerId;
    private final long firstProducerId;
    private final int blockSize;
    private final AtomicLong producerIdCounter;

    public ProducerIdsBlock(int assignedBrokerId, long firstProducerId, int blockSize) {
        this.assignedBrokerId = assignedBrokerId;
        this.firstProducerId = firstProducerId;
        this.blockSize = blockSize;
        producerIdCounter = new AtomicLong(firstProducerId);
    }

    /**
     * Claim the next available producer id from the block.
     * Returns an empty result if there are no more available producer ids in the block.
     */
    public Optional<Long> claimNextId() {
        long nextId = producerIdCounter.getAndIncrement();
        if (nextId > lastProducerId()) {
            return Optional.empty();
        }
        return Optional.of(nextId);
    }

    /**
     * Get the ID of the broker that this block was assigned to.
     */
    public int assignedBrokerId() {
        return assignedBrokerId;
    }

    /**
     * Get the first ID (inclusive) to be assigned from this block.
     */
    public long firstProducerId() {
        return firstProducerId;
    }

    /**
     * Get the number of IDs contained in this block.
     */
    public int size() {
        return blockSize;
    }

    /**
     * Get the last ID (inclusive) to be assigned from this block.
     */
    public long lastProducerId() {
        return firstProducerId + blockSize - 1;
    }

    /**
     * Get the first ID of the next block following this one.
     */
    public long nextBlockFirstId() {
        return firstProducerId + blockSize;
    }

    @Override
    public String toString() {
        return "ProducerIdsBlock(" + "assignedBrokerId=" + assignedBrokerId + ", firstProducerId=" + firstProducerId + ", size=" + blockSize + ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ProducerIdsBlock that = (ProducerIdsBlock) o;
        return assignedBrokerId == that.assignedBrokerId && firstProducerId == that.firstProducerId && blockSize == that.blockSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignedBrokerId, firstProducerId, blockSize);
    }
}
