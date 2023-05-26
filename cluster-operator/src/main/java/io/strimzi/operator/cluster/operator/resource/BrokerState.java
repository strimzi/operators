/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Java representation of the JSON response from the /v1/broker-state endpoint of the KafkaAgent
 */
public class BrokerState {
    private static final int BROKER_RECOVERY_STATE = 2;

    private final int code;
    private final Map<String, Object> recoveryState;

    /**
     * Constructor
     * @param code Broker state
     * @param recoveryState Map that has the number of remaining logs and segments to recover
     */
    @JsonCreator
    public BrokerState(@JsonProperty("brokerState")int code, @JsonProperty("recoveryState") Map<String, Object> recoveryState) {
        this.code = code;
        this.recoveryState = recoveryState;
    }

    /**
     * Integer that represents the broker state, or -1 if there was an error when getting the broker state.
     * @return integer result
     */
    public int code() {
        return code;
    }

    /**
     * The number of remaining logs to recover
     * @return integer result
     */
    public int remainingLogsToRecover() {
        if (recoveryState != null  && recoveryState.containsKey("remainingLogsToRecover")) {
            return (int) recoveryState.get("remainingLogsToRecover");
        }
        return 0;
    }

    /**
     * The number of remaining segments to recover
     * @return integer result
     */
    public int remainingSegmentsToRecover() {
        if (recoveryState != null  && recoveryState.containsKey("remainingSegmentsToRecover")) {
            return (int) recoveryState.get("remainingSegmentsToRecover");
        }
        return 0;
    }

    @Override
    public String toString() {
        return String.format("Broker state: %d, Recovery state: %s", code, recoveryState);
    }

    /**
     * Returns true if broker state is 2 (RECOVERY)
     * @return boolean result
     */
    public boolean isBrokerInRecovery() {
        return code == BROKER_RECOVERY_STATE;
    }
}
