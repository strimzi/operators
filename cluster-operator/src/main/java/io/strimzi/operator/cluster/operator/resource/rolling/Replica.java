/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import org.apache.kafka.common.Node;

import java.util.Collection;

/**
 * A replica on a particular {@link KafkaNode}.
 *
 * @param topicName   The name of the topic
 * @param partitionId The partition id
 * @param isrSize     If the broker hosting this replica is in the ISR for the partition of this replica
 *                    this is the size of the ISR.
 *                    If the broker hosting this replica is NOT in the ISR for the partition of this replica
 *                    this is the negative of the size of the ISR.
 *                    In other words, the magnitude is the size of the ISR and the sign will be negative
 *                    if the broker hosting this replica is not in the ISR.
 */
record Replica(String topicName, int partitionId, short isrSize) {

    public Replica(Node broker, String topicName, int partitionId, Collection<Node> isr) {
        this(topicName, partitionId, (short) (isr.contains(broker) ? isr.size() : -isr.size()));
    }

    @Override
    public String toString() {
        return topicName + "-" + partitionId;
    }

    /**
     * @return The size of the ISR for the partition of this replica.
     */
    public short isrSize() {
        return (short) Math.abs(isrSize);
    }

    /**
     * @return true if the broker hosting this replica is in the ISR for the partition of this replica.
     */
    public boolean isInIsr() {
        return isrSize > 0;
    }
}
