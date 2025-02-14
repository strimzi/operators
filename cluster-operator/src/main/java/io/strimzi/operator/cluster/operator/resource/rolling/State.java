/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

/**
 * Enumerates the possible "rolling states" of a Kafka node
 */
enum State {
    UNKNOWN, // the initial state
    NOT_RUNNING, // The pod/process is not running.
    NOT_READY, // decided to restart right now or broker state < 2 OR == 127
    RECOVERING, // broker state == 2
    READY, // broker state >= 3 AND != 127
}
