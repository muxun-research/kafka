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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.kafka.streams.state.StateSerdes.TIMESTAMP_SIZE;


public class SessionKeySchema implements SegmentedBytesStore.KeySchema {

    private static final int SUFFIX_SIZE = 2 * TIMESTAMP_SIZE;
    private static final byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

    public static int keyByteLength(final Bytes key) {
        return (key == null ? 0 : key.get().length) + 2 * TIMESTAMP_SIZE;
    }

    @Override
    public Bytes upperRangeFixedSize(final Bytes key, final long to) {
        final Windowed<Bytes> sessionKey = upperRangeFixedWindow(key, to);
        return SessionKeySchema.toBinary(sessionKey);
    }

    public static <K> Windowed<K> upperRangeFixedWindow(final K key, final long to) {
        return new Windowed<K>(key, new SessionWindow(to, Long.MAX_VALUE));
    }

    @Override
    public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
        final Windowed<Bytes> sessionKey = lowerRangeFixedWindow(key, from);
        return SessionKeySchema.toBinary(sessionKey);
    }

    public static <K> Windowed<K> lowerRangeFixedWindow(final K key, final long from) {
        return new Windowed<K>(key, new SessionWindow(0, Math.max(0, from)));
    }

    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        if (key == null) {
            return null;
        }
        final byte[] maxSuffix = ByteBuffer.allocate(SUFFIX_SIZE)
                // the end timestamp can be as large as possible as long as it's larger than start time
                .putLong(Long.MAX_VALUE)
                // this is the start timestamp
                .putLong(to).array();
        return OrderedBytes.upperRange(key, maxSuffix);
    }

    @Override
    public Bytes lowerRange(final Bytes key, final long from) {
        if (key == null) {
            return null;
        }
        return OrderedBytes.lowerRange(key, MIN_SUFFIX);
    }

    @Override
    public long segmentTimestamp(final Bytes key) {
        return SessionKeySchema.extractEndTimestamp(key.get());
    }

    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to, final boolean forward) {
        return iterator -> {
            while (iterator.hasNext()) {
                final Bytes bytes = iterator.peekNextKey();
                final Windowed<Bytes> windowedKey = SessionKeySchema.from(bytes);
                if ((binaryKeyFrom == null || windowedKey.key().compareTo(binaryKeyFrom) >= 0) && (binaryKeyTo == null || windowedKey.key().compareTo(binaryKeyTo) <= 0) && windowedKey.window().end() >= from && windowedKey.window().start() <= to) {
                    return true;
                }
                iterator.next();
            }
            return false;
        };
    }

    @Override
	public <S extends Segment> List<S> segmentsToSearch(final Segments<S> segments,
														final long from,
														final long to,
														final boolean forward) {
		return segments.segments(from, Long.MAX_VALUE, forward);
	}

    private static <K> K extractKey(final byte[] binaryKey,
                                    final Deserializer<K> deserializer,
                                    final String topic) {
        return deserializer.deserialize(topic, extractKeyBytes(binaryKey));
    }

    static byte[] extractKeyBytes(final byte[] binaryKey) {
        final byte[] bytes = new byte[binaryKey.length - 2 * TIMESTAMP_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        return bytes;
    }

    static long extractEndTimestamp(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
    }

    static long extractStartTimestamp(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE);
    }

    static Window extractWindow(final byte[] binaryKey) {
        final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
        final long end = buffer.getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
        return new SessionWindow(start, end);
    }

    public static <K> Windowed<K> from(final byte[] binaryKey,
                                       final Deserializer<K> keyDeserializer,
                                       final String topic) {
        final K key = extractKey(binaryKey, keyDeserializer, topic);
        final Window window = extractWindow(binaryKey);
        return new Windowed<>(key, window);
    }

    public static Windowed<Bytes> from(final Bytes bytesKey) {
        final byte[] binaryKey = bytesKey.get();
        final Window window = extractWindow(binaryKey);
        return new Windowed<>(Bytes.wrap(extractKeyBytes(binaryKey)), window);
    }

    public static <K> Windowed<K> from(final Windowed<Bytes> keyBytes,
                                       final Deserializer<K> keyDeserializer,
                                       final String topic) {
        final K key = keyDeserializer.deserialize(topic, keyBytes.key().get());
        return new Windowed<>(key, keyBytes.window());
    }

    public static <K> byte[] toBinary(final Windowed<K> sessionKey,
                                      final Serializer<K> serializer,
                                      final String topic) {
        final byte[] bytes = serializer.serialize(topic, sessionKey.key());
        return toBinary(Bytes.wrap(bytes), sessionKey.window().start(), sessionKey.window().end()).get();
    }

    public static Bytes toBinary(final Windowed<Bytes> sessionKey) {
        return toBinary(sessionKey.key(), sessionKey.window().start(), sessionKey.window().end());
    }

    public static Bytes toBinary(final Bytes key, final long startTime, final long endTime) {
        final ByteBuffer buf = ByteBuffer.allocate(keyByteLength(key));
        writeBinary(buf, key, startTime, endTime);
        return Bytes.wrap(buf.array());
    }

    public static void writeBinary(final ByteBuffer buf, final Windowed<Bytes> sessionKey) {
        writeBinary(buf, sessionKey.key(), sessionKey.window().start(), sessionKey.window().end());
    }

    public static void writeBinary(final ByteBuffer buf, final Bytes key, final long startTime, final long endTime) {
        // we search for the session window that can overlap with the [ESET, LSST] range
        // since the session window length can vary, we define the search boundary as:
        // lower: [0, ESET]
        // upper: [LSST, INF]
        // and by putting the end time first and then the start time, the serialized search boundary
        // is: [(ESET-0), (INF-LSST)]
        buf.put(key.get());
        buf.putLong(endTime);
        buf.putLong(startTime);
    }
}
