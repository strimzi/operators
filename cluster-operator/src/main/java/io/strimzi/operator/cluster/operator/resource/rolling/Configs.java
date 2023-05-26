/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import org.apache.kafka.clients.admin.Config;

/**
 * Holds Kafka node configs and logger configs returned from Kafka Admin API.
 *
 * @param nodeConfigs Broker/Controller configs
 * @param nodeLoggerConfigs Broker/Controller logging config
 */
record Configs(Config nodeConfigs, Config nodeLoggerConfigs) {

}

