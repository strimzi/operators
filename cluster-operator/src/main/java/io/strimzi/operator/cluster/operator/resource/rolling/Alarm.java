/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.common.UncheckedInterruptedException;

import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Timing utility for polling loops which allows to set an alarm (in terms of a duration from "now") and
 * subsequently sleep the executing thread. If the alarm duration is exceeded the call to sleep will throw a
 * {@link TimeoutException}. This can be used to simplify writing polling logic like the following
 * <pre>{@code
 * long timeoutMs = 60_000
 * long pollIntervalMs = 1_000;
 * Alarm alarm = Alarm.start(time, timeoutMs);
 * while (true) {
 *   // do some processing
 *   if (processingSuccess) {
 *       timeoutMs = alarm.remainingMs();
 *       // we might want to use the remaining timeout when
 *       // a single timeout is used for a sequence of polling tasks
 *       break;
 *   }
 *   alarm.sleep(pollIntervalMs);
 * }
 * }</pre>
 * This logic is encapsulated in the {@link #poll(long, BooleanSupplier)} method.
 */
public class Alarm {

    final Time time;
    final long deadline;
    private final Supplier<String> timeoutMessageSupplier;

    private Alarm(Time time, long deadline, Supplier<String> timeoutMessageSupplier) {
        this.time = time;
        this.deadline = deadline;
        this.timeoutMessageSupplier = timeoutMessageSupplier;
    }

    /**
     * Creates an Alerm
     * @param time The source of time
     * @param timeoutMs The timeout for this alarm.
     * @param timeoutMessageSupplier The exception message
     * @return The alarm
     */
    public static Alarm timer(Time time, long timeoutMs, Supplier<String> timeoutMessageSupplier) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException();
        }
        long deadline = time.nanoTime() + 1_000_000 * timeoutMs;
        return new Alarm(time, deadline, timeoutMessageSupplier);
    }

    /**
     * @return The remaining number of milliseconds until the deadline passed
     */
    public long remainingMs() {
        return Math.max(deadline - time.nanoTime(), 0) / 1_000_000L;
    }

    /**
     * Sleep the current thread for at most at least {@code ms} milliseconds, according to
     * (and subject to the precision and accuracy of) the configured {@link Time} instance.
     * The actual sleep time will be less than {@code ms} if using {@code ms} would exceed this
     * alarm's deadline.
     * The thread does not lose ownership of any monitors.
     * @param ms The number of milliseconds to sleep for.
     * @throws TimeoutException If the Alarm's deadline has passed
     * @throws InterruptedException If the current thread is interrupted
     */
    public void sleep(long ms) throws TimeoutException, InterruptedException {
        if (ms < 0) {
            throw new IllegalArgumentException();
        }
        long sleepNs = Math.min(1_000_000L * ms, deadline - time.nanoTime());
        if (sleepNs <= 0) {
            throw new TimeoutException(timeoutMessageSupplier.get());
        }
        time.sleep(sleepNs / 1_000_000L, (int) (sleepNs % 1_000_000L));
    }

    /**
     * Test {@code done} at least once, returning when it returns true, and otherwise sleeping for at most approximately
     * {@code pollIntervalMs} before repeating, throwing {@link TimeoutException} should this
     * alarm expire before {@code done} returns true.
     *
     * @param pollIntervalMs The polling interval
     * @param done           A predicate function to detecting when the polling loop is complete.
     * @return The remaining time left for this alarm, in ms.
     * @throws UncheckedInterruptedException The thread was interrupted
     * @throws TimeoutException     The {@link #remainingMs()} has reached zero.
     */
    public long poll(long pollIntervalMs, BooleanSupplier done) throws TimeoutException {
        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException();
        }
        try {
            while (true) {
                if (done.getAsBoolean()) {
                    return this.remainingMs();
                }
                this.sleep(pollIntervalMs);
            }
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        }
    }
}
