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
package org.apache.kafka.raft.internals;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;

public class MemoryBatchReader<T> implements BatchReader<T> {
    private final CloseListener<BatchReader<T>> closeListener;
    private final Iterator<Batch<T>> iterator;
    private final long baseOffset;
    private final long lastOffset;

    private MemoryBatchReader(long baseOffset, long lastOffset, Iterator<Batch<T>> iterator, CloseListener<BatchReader<T>> closeListener) {
        this.baseOffset = baseOffset;
        this.lastOffset = lastOffset;
        this.iterator = iterator;
        this.closeListener = closeListener;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Batch<T> next() {
        return iterator.next();
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public OptionalLong lastOffset() {
        return OptionalLong.of(lastOffset);
    }

    @Override
    public void close() {
        closeListener.onClose(this);
    }

    public static <T> MemoryBatchReader<T> empty(long baseOffset, long lastOffset, CloseListener<BatchReader<T>> closeListener) {
        return new MemoryBatchReader<>(baseOffset, lastOffset, Collections.emptyIterator(), closeListener);
    }

    public static <T> MemoryBatchReader<T> of(List<Batch<T>> batches, CloseListener<BatchReader<T>> closeListener) {
        if (batches.isEmpty()) {
            throw new IllegalArgumentException("MemoryBatchReader requires at least " + "one batch to iterate, but an empty list was provided");
        }

        return new MemoryBatchReader<>(batches.get(0).baseOffset(), batches.get(batches.size() - 1).lastOffset(), batches.iterator(), closeListener);
    }
}
