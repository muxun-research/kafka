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

package org.apache.kafka.controller;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;


@Timeout(value = 40)
public class ResultOrErrorTest {
    @Test
    public void testError() {
        ResultOrError<Integer> resultOrError = new ResultOrError<>(Errors.INVALID_REQUEST, "missing foobar");
        assertTrue(resultOrError.isError());
        assertFalse(resultOrError.isResult());
        assertNull(resultOrError.result());
        assertEquals(new ApiError(Errors.INVALID_REQUEST, "missing foobar"), resultOrError.error());
    }

    @Test
    public void testResult() {
        ResultOrError<Integer> resultOrError = new ResultOrError<>(123);
        assertFalse(resultOrError.isError());
        assertTrue(resultOrError.isResult());
        assertEquals(123, resultOrError.result());
        assertNull(resultOrError.error());
    }

    @Test
    public void testEquals() {
        ResultOrError<String> a = new ResultOrError<>(Errors.INVALID_REQUEST, "missing foobar");
        ResultOrError<String> b = new ResultOrError<>("abcd");
        assertNotEquals(a, b);
        assertNotEquals(b, a);
        assertEquals(a, a);
        assertEquals(b, b);
        ResultOrError<String> c = new ResultOrError<>(Errors.INVALID_REQUEST, "missing baz");
        assertNotEquals(a, c);
        assertNotEquals(c, a);
        assertEquals(c, c);
    }
}
