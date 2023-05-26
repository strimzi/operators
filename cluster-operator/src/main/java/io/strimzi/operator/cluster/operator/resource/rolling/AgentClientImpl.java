/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;

class AgentClientImpl implements AgentClient {
    private final KafkaAgentClient kafkaAgentClient;

    AgentClientImpl(KafkaAgentClient kafkaAgentClient) {
        this.kafkaAgentClient = kafkaAgentClient;

    }

    @Override
    public BrokerState getBrokerState(NodeRef nodeRef) {
        var result = kafkaAgentClient.getBrokerState(nodeRef.podName());
        BrokerState brokerState = BrokerState.fromValue((byte) result.code());
        brokerState.setRemainingSegmentsToRecover(result.remainingSegmentsToRecover());
        brokerState.setRemainingLogsToRecover(result.remainingLogsToRecover());
        return brokerState;
    }
}
