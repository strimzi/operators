/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;

/**
 * An abstraction over a KafkaAgent client.
 */
interface AgentClient {

    /** @return The broker state, according to the Kafka Agent */
    BrokerState getBrokerState(NodeRef nodeRef);
}
