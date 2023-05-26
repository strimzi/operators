/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

/**
 * Holds the current process roles for a node.
 * This is different from the broker and controller information in NodeRef.
 * NodeRef holds the desired roles for a node rather than the roles currently assigned to the node.
 * @param controller set to true if the node has controller role
 * @param broker set to true if the node has broker role
 */
record NodeRoles(boolean controller, boolean broker) {
}

