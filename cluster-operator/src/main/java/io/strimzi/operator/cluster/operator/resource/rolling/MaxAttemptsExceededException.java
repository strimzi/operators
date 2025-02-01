/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

/**
 * Indicates failure of the roller to make progress
 */
public class MaxAttemptsExceededException extends RuntimeException {
    /**
     * Constructor
     * @param message The message
     */
    public MaxAttemptsExceededException(String message) {
        super(message);
    }
}
