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
package org.apache.kafka.common.utils;

import java.io.Closeable;
import java.util.Iterator;

/**
 * 闭环迭代器
 * 注意，在实现这个接口之前，考虑下是否有更好的选座，避免由于不习惯闭环，而导致滥用
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {
    void close();

    static <R> CloseableIterator<R> wrap(Iterator<R> inner) {
        return new CloseableIterator<>() {
            @Override
            public void close() {}

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public R next() {
                return inner.next();
            }

            @Override
            public void remove() {
                inner.remove();
            }
        };
    }
}
