/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AlarmTest {

    private final Time.TestTime time = new Time.TestTime();

    @Test
    public void testRemaining() throws InterruptedException, TimeoutException {
        var tx = time.nanoTime();
        var alarm = Alarm.timer(time, 1_000, () -> "message");
        assertEquals(1_000, alarm.remainingMs());
        time.tickNanos(100_000_000);
        assertEquals(900, alarm.remainingMs());
        time.tickNanos(50_000_000);
        assertEquals(850, alarm.remainingMs());

        alarm.sleep(100);
        assertEquals(750, alarm.remainingMs());
        alarm.sleep(700);
        assertEquals(50, alarm.remainingMs());

        // We wake up one final time with 0ns left, even if we would have slept longer
        var t0 = time.nanoTime();
        alarm.sleep(51);
        assertEquals(50_000_000, time.nanoTime() - t0, "Expected to sleep for exactly 5,000,000 ns");
        assertEquals(0, alarm.remainingMs(), "Expect no time remaining");
        assertEquals(1_000_000_000, time.nanoTime() - tx, "Expected timeout after 1,000 ms");

        // Once the time advances past the dealine
        time.tickNanos(1);

        // We expect subsequent attampts to sleep to throw
        assertEquals("message",
                assertThrows(TimeoutException.class, () -> alarm.sleep(0)).getMessage());
        assertEquals("message",
                assertThrows(TimeoutException.class, () -> alarm.sleep(1)).getMessage());
    }

}