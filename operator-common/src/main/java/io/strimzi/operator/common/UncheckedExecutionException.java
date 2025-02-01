/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ExecutionException;

/**
 * Used to wrap {@code ExecutionException} within methods that do not declare it in their {@code throws} clause.
 * This can be useful when combining KafkaFuture and java Stream APIs, for example.
 */
public class UncheckedExecutionException extends RuntimeException {

    /**
     * @param cause The cause
     */
    public UncheckedExecutionException(ExecutionException cause) {
        super(cause);
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    @Override
    public synchronized ExecutionException getCause() {
        return (ExecutionException) super.getCause();
    }
}
