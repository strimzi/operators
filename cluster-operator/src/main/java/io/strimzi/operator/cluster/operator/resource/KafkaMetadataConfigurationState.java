/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.model.NodeRef;

/**
 * Represents a desired configuration state needed on nodes (brokers and/or controllers) when transitioning
 * across states in the FSM
 */
public enum KafkaMetadataConfigurationState {

    /**
     * Full ZooKeeper. Brokers should have ZooKeeper only.
     */
    ZK,

    /**
     * Full ZooKeeper. Controllers should have KRaft, ZooKeeper and migration enabled
     */
    PRE_MIGRATION,

    /**
     * Migration going on. Both brokers and controllers should have KRaft, ZooKeeper and migration enabled.
     */
    MIGRATION,

    /**
     * Finalising migration. Brokers don't have ZooKeeper anymore. Controllers still configured with it.
     */
    POST_MIGRATION,

    /**
     * Full KRaft. Both brokers and controllers have KRaft only.
     */
    KRAFT;

    /**
     * @return if the Kafka metadata are fully stored in ZooKeeper
     */
    public boolean isZooKeeper() {
        return ZK.equals(this);
    }

    /**
     * @return if the Kafka metadata configuration state is in pre-migration
     *         Controllers should have KRaft, ZooKeeper and migration enabled
     */
    public boolean isPreMigration() {
        return PRE_MIGRATION.equals(this);
    }

    /**
     * @return if the Kafka metadata configuration state is in migration
     *         Both brokers and controllers should have KRaft, ZooKeeper and migration enabled
     */
    public boolean isMigration() {
        return MIGRATION.equals(this);
    }

    /**
     * @return if the Kafka metadata configuration state is in post-migration
     *         Brokers don't have ZooKeeper anymore. Controllers still configured with it
     */
    public boolean isPostMigration() {
        return POST_MIGRATION.equals(this);
    }

    /**
     * @return if the Kafka metadata are fully stored in KRaft
     */
    public boolean isKRaft() {
        return KRAFT.equals(this);
    }

    /**
     * @return if the Kafka metadata configuration state is from ZooKeeper-based up to KRaft migration (and dual-write) going on
     */
    public boolean isZooKeeperOrMigration() {
        return this.ordinal() <= MIGRATION.ordinal();
    }

    /**
     * @return if the Kafka metadata configuration state is from ZooKeeper-based up to KRaft post-migration
     */
    public boolean isZooKeeperOrPostMigration() {
        return this.ordinal() <= POST_MIGRATION.ordinal();
    }

    /**
     * @return if the Kafka metadata configuration state is from KRaft pre-migration up to the full KRaft-based
     */
    public boolean isPreMigrationOrKRaft() {
        return this.ordinal() >= PRE_MIGRATION.ordinal();
    }

    /**
     * @return if the Kafka metadata configuration state is from KRaft post-migration up to the full KRaft-based
     */
    public boolean isPostMigrationOrKRaft() {
        return this.ordinal() >= POST_MIGRATION.ordinal();
    }

    /**
     * @return if the Kafka metadata configuration state is from KRaft migration up to the full KRaft-based
     */
    public boolean isMigrationOrKRaft() {
        return this.ordinal() >= MIGRATION.ordinal();
    }

    /**
     * Checking KRaft configuration needed depending on the node role
     *
     * @param node node to check to see if it needs KRaft configuration
     * @return if KRaft is going to be configured depending on the node role and the Kafka metadata configuration state
     */
    public boolean isKRaftInConfiguration(NodeRef node) {
        return (node.controller() && this.ordinal() >= PRE_MIGRATION.ordinal()) ||
                (node.broker() && this.ordinal() >= MIGRATION.ordinal());
    }
}
