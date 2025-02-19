/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test


class ThrottlerTest {
  @Test
  def testThrottleDesiredRate(): Unit = {
    val throttleCheckIntervalMs = 100
    val desiredCountPerSec = 1000.0
    val desiredCountPerInterval = desiredCountPerSec * throttleCheckIntervalMs / 1000.0

    val mockTime = new MockTime()
    val throttler = new Throttler(desiredRatePerSec = desiredCountPerSec,
                                  checkIntervalMs = throttleCheckIntervalMs,
                                  time = mockTime)

    // Observe desiredCountPerInterval at t1
    val t1 = mockTime.milliseconds()
    throttler.maybeThrottle(desiredCountPerInterval)
    assertEquals(t1, mockTime.milliseconds())

    // Observe desiredCountPerInterval at t1 + throttleCheckIntervalMs + 1,
    mockTime.sleep(throttleCheckIntervalMs + 1)
    throttler.maybeThrottle(desiredCountPerInterval)
    val t2 = mockTime.milliseconds()
    assertTrue(t2 >= t1 + 2 * throttleCheckIntervalMs)

    // Observe desiredCountPerInterval at t2
    throttler.maybeThrottle(desiredCountPerInterval)
    assertEquals(t2, mockTime.milliseconds())

    // Observe desiredCountPerInterval at t2 + throttleCheckIntervalMs + 1
    mockTime.sleep(throttleCheckIntervalMs + 1)
    throttler.maybeThrottle(desiredCountPerInterval)
    val t3 = mockTime.milliseconds()
    assertTrue(t3 >= t2 + 2 * throttleCheckIntervalMs)

    val elapsedTimeMs = t3 - t1
    val actualCountPerSec = 4 * desiredCountPerInterval * 1000 / elapsedTimeMs
    assertTrue(actualCountPerSec <= desiredCountPerSec)
  }

  @Test
  def testUpdateThrottleDesiredRate(): Unit = {
    val throttleCheckIntervalMs = 100
    val desiredCountPerSec = 1000.0
    val desiredCountPerInterval = desiredCountPerSec * throttleCheckIntervalMs / 1000.0
    val updatedDesiredCountPerSec = 1500.0;
    val updatedDesiredCountPerInterval = updatedDesiredCountPerSec * throttleCheckIntervalMs / 1000.0

    val mockTime = new MockTime()
    val throttler = new Throttler(desiredRatePerSec = desiredCountPerSec,
      checkIntervalMs = throttleCheckIntervalMs,
      time = mockTime)

    // Observe desiredCountPerInterval at t1
    val t1 = mockTime.milliseconds()
    throttler.maybeThrottle(desiredCountPerInterval)
    assertEquals(t1, mockTime.milliseconds())

    // Observe desiredCountPerInterval at t1 + throttleCheckIntervalMs + 1,
    mockTime.sleep(throttleCheckIntervalMs + 1)
    throttler.maybeThrottle(desiredCountPerInterval)
    val t2 = mockTime.milliseconds()
    assertTrue(t2 >= t1 + 2 * throttleCheckIntervalMs)

    val elapsedTimeMs = t2 - t1
    val actualCountPerSec = 2 * desiredCountPerInterval * 1000 / elapsedTimeMs
    assertTrue(actualCountPerSec <= desiredCountPerSec)

    // Update ThrottleDesiredRate
    throttler.updateDesiredRatePerSec(updatedDesiredCountPerSec);

    // Observe updatedDesiredCountPerInterval at t2
    throttler.maybeThrottle(updatedDesiredCountPerInterval)
    assertEquals(t2, mockTime.milliseconds())

    // Observe updatedDesiredCountPerInterval at t2 + throttleCheckIntervalMs + 1
    mockTime.sleep(throttleCheckIntervalMs + 1)
    throttler.maybeThrottle(updatedDesiredCountPerInterval)
    val t3 = mockTime.milliseconds()
    assertTrue(t3 >= t2 + 2 * throttleCheckIntervalMs)

    val updatedElapsedTimeMs = t3 - t2
    val updatedActualCountPerSec = 2 * updatedDesiredCountPerInterval * 1000 / updatedElapsedTimeMs
    assertTrue(updatedActualCountPerSec <= updatedDesiredCountPerSec)
  }
}
