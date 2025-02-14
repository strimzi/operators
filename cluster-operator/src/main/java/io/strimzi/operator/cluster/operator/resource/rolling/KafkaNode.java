/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import java.util.Set;

/**
 * Information about a Kafka node (which may be a broker, controller, or both) and its replicas.
 * @param id The id of the server
 * @param replicas The replicas on this server
 */
record KafkaNode(int id, Set<Replica> replicas) {
}
