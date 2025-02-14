/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.common.UncheckedInterruptedException;

/**
 * An abstraction of time. 
 * A Time instance represents some way of measuring durations (the {@link #nanoTime()} method) and some way of suspending 
 * the execution of a thread for some duration consistent with such measurements {@link #sleep(long, int)}.
 * <p>In practice a real system would use {@link #SYSTEM_TIME}.</p>
 * <p>In testing you can use {@link Time.TestTime} so that tests don't depend on actually sleeping threads.</p>
 */
public interface Time {

    /** The system's time */
    public final Time SYSTEM_TIME = new Time() {
        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public long systemTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public void sleep(long millis, int nanos) {
            try {
                Thread.sleep(millis, nanos);
            } catch (InterruptedException e) {
                throw new UncheckedInterruptedException(e);
            }
        }

        @Override
        public String toString() {
            return "SYSTEM_TIME";
        }
    };

    /**
     * A {@code Time} implementation used for testing.
     */
    public static class TestTime implements Time {
        private final long autoAdvanceNs;
        long time = 0;

        /**
         * Constructs a new {@code Time} where time will only advance via calls to {@link #tickNanos(long)}.
         */
        public TestTime() {
            this(0);
        }

        /**
         * Constructs a new {@code Time} where time will advance either by calls to {@link #tickNanos(long)} or
         * as a side-effect of calls to {@link #nanoTime()}.
         * This can be useful in tests where the code under test needs to see time advance,
         * does not call {@link #sleep(long, int)}, and difficult or impossible for the test code to call
         * {@link #tickNanos(long)}.
         * @param autoAdvanceNs How much time will advance when {@link #nanoTime()} is called.
         */
        public TestTime(long autoAdvanceNs) {
            if (autoAdvanceNs < 0) {
                throw new IllegalArgumentException();
            }
            this.autoAdvanceNs = autoAdvanceNs;
        }

        /**
         * Advance the time by the given number of nanoseconds.
         * @param advanceNs The number of nanoseconds by which the time should be advanced.
         */
        public void tickNanos(long advanceNs) {
            if (advanceNs < 0) {
                throw new IllegalArgumentException();
            }
            time += advanceNs;
        }

        @Override
        public long nanoTime() {
            var result = time;
            time += autoAdvanceNs;
            return result;
        }

        @Override
        public long systemTimeMillis() {
            return time / 1_000_000;
        }

        @Override
        public void sleep(long millis, int nanos) {
            if (millis < 0 || nanos < 0 || nanos > 999_999) {
                throw new IllegalArgumentException();
            }
            time += 1_000_000 * millis + nanos;
        }

        @Override
        public String toString() {
            return "TestTime";
        }
    }


    /**
     * The number of nanoseconds since some unknown epoch.
     * Different {@code Time} instances may use different epochs.
     * This is only useful for measuring and calculating elapsed time against the same Time instance.
     * The return value is guaranteed to be monotonic.
     *
     * <p>When the instance is {@link Time#SYSTEM_TIME} this corresponds to a call to {@link System#nanoTime()}.</p>
     *
     * @return The number of nanoseconds since some unknown epoch.
     */
    long nanoTime();

    /**
     * The system time, defined as the number of milliseconds since the epoch, midnight, January 1, 1970 UTC.
     * Different {@code Time} instances all use the same epoch.
     *
     * There are no guarantees about precision or accuracy and the return value is not guaranteed to be monotonic.
     *
     * When the instance is {@link Time#SYSTEM_TIME} this corresponds to a call to {@link System#currentTimeMillis()}.
     * @return The number of milliseconds since the epoch.
     */
    long systemTimeMillis();

    /**
     * Causes the current thread to sleep (suspend execution) for the given number of milliseconds
     * plus the given number of nanoseconds, relative to this Time instance and subject to its precision and accuracy.
     * In other words after successful execution of the following code
     * <pre>{@code
     * Time time = ...
     * long start = time.nanoTime();
     * time.sleep(millis, nanos)
     * long end = time.nanoTime();
     * long diff = (1_000_000 * millis + nanos) - (end - start)
     * }</pre>
     * we would expect {@code diff} to be small in magnitude.
     *
     * <p>The thread does not lose ownership of any monitors.</p>
     *
     * <p>When the instance is {@link Time#SYSTEM_TIME} this corresponds to a call to {@link Thread#sleep(long, int)}.</p>
     *
     * @param millis The number of milliseconds
     * @param nanos The number of nanoseconds
     * @throws io.strimzi.operator.common.UncheckedInterruptedException If the sleep was interrupted before the given time has elapsed.
     */
    void sleep(long millis, int nanos);

}

