/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeTest {

    public static List<Time> times() {
        return List.of(Time.SYSTEM_TIME, new Time.TestTime());
    }

    @ParameterizedTest
    @MethodSource("times")
    public void testSleepConsistentWithNanoTime(Time time) throws InterruptedException {
        long epsilonNanos = 100_000_000;
        var t0 = time.nanoTime();
        time.sleep(500, 000);
        var elapsedNanos = time.nanoTime() - t0;
        assertTrue(elapsedNanos - epsilonNanos < 500_000_000);
    }
}