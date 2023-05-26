/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource.rolling;


import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the states published by a Kafka node via the broker state metric.
 */
enum BrokerState {
    /**
     * The state the broker is in when it first starts up.
     */
    NOT_RUNNING((byte) 0),

    /**
     * The state the broker is in when it is catching up with cluster metadata.
     */
    STARTING((byte) 1),

    /**
     * The broker has caught up with cluster metadata, but has not yet
     * been unfenced by the controller.
     */
    RECOVERY((byte) 2),

    /**
     * The state the broker is in when it has registered at least once, and is
     * accepting client requests.
     */
    RUNNING((byte) 3),

    /**
     * The state the broker is in when it is attempting to perform a controlled
     * shutdown.
     */
    PENDING_CONTROLLED_SHUTDOWN((byte) 6),

    /**
     * The state the broker is in when it is shutting down.
     */
    SHUTTING_DOWN((byte) 7),

    /**
     * The broker is in an unknown state.
     */
    UNKNOWN((byte) 127);

    private final static Map<Byte, BrokerState> VALUES_TO_ENUMS = new HashMap<>();

    static {
        for (BrokerState state : BrokerState.values()) {
            VALUES_TO_ENUMS.put(state.value(), state);
        }
    }

    private final byte value;
    private int remainingLogsToRecover;
    private int remainingSegmentsToRecover;

    BrokerState(byte value) {
        this.value = value;
    }

    public static BrokerState fromValue(byte value) {
        BrokerState state = VALUES_TO_ENUMS.get(value);
        if (state == null) {
            return UNKNOWN;
        }
        return state;
    }

    public byte value() {
        return value;
    }

    void setRemainingLogsToRecover(int value) {
        remainingLogsToRecover = value;
    }

    void setRemainingSegmentsToRecover(int value) {
        remainingSegmentsToRecover = value;
    }

    int remainingLogsToRecover() {
        return remainingLogsToRecover;
    }

    int remainingSegmentsToRecover() {
        return remainingSegmentsToRecover;
    }
}