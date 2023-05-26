/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Used to wrap {@code InterruptedException} within methods that do not declare it in their {@code throws} clause.
 * This can be useful when combining KafkaFuture and java Stream APIs, for example.
 */
public class UncheckedInterruptedException extends RuntimeException {
    /**
     * @param cause The cause.
     */
    public UncheckedInterruptedException(InterruptedException cause) {
        super(cause);
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    @Override
    public synchronized InterruptedException getCause() {
        return (InterruptedException) super.getCause();
    }
}