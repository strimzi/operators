/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An abstraction over a Kafka Admin client.
 */
interface RollClient {
    /**
     * Sets admin client for brokers.
     **/
    void initialiseBrokerAdmin(Set<NodeRef> brokerNodes);

    /**
     * Sets admin client for controllers.
     **/
    void initialiseControllerAdmin(Set<NodeRef> controllerNodes);

    /**
     * Closes controller admin client
     **/
    void closeControllerAdminClient();

    /**
     * Closes broker admin client
     **/
    void closeBrokerAdminClient();

    /**
     * Checks if node is responsive by connecting to it via Admin API
     * @param nodeRef The node ref
     * @param controller a boolean value informing if it's a controller node
     * @return true if node is responsive, otherwise false
     */
    boolean canConnectToNode(NodeRef nodeRef, boolean controller);

    /**
     * @return All the topics in the cluster, including internal topics.
     * @throws io.strimzi.operator.common.UncheckedExecutionException Execution exception from clients
     * @throws io.strimzi.operator.common.UncheckedInterruptedException The thread was interrupted
     */
    Collection<TopicListing> listTopics();

    /**
     * Describe the topics with the given ids.
     * If the given {@code topicIds} is large, multiple requests (to different brokers) may be used.
     * @param topicIds The topic ids.
     * @return The topic descriptions.
     * @throws io.strimzi.operator.common.UncheckedExecutionException Execution exception from clients
     * @throws io.strimzi.operator.common.UncheckedInterruptedException The thread was interrupted
     */
    List<TopicDescription> describeTopics(List<Uuid> topicIds);

    /**
     * Get the {@code min.insync.replicas} of each of the topics in the given {@code topicNames} list.
     * @param topicNames The names of the topics to get the {@code min.insync.replicas} of.
     * @return A map from topic name to its {@code min.insync.replicas}.
     * @throws io.strimzi.operator.common.UncheckedExecutionException Execution exception from clients
     * @throws io.strimzi.operator.common.UncheckedInterruptedException The thread was interrupted
     */
    Map<String, Integer> describeTopicMinIsrs(List<String> topicNames);

    /**
     * Describe the metadata quorum info.
     *
     * @param activeControllerNodeRef A set containing active controller NodeRef.
     * @return A map from controller quorum followers to their {@code lastCaughtUpTimestamps}.
     * @throws io.strimzi.operator.common.UncheckedExecutionException   Execution exception from clients
     * @throws io.strimzi.operator.common.UncheckedInterruptedException The thread was interrupted
     */
    Map<Integer, Long> quorumLastCaughtUpTimestamps(Set<NodeRef> activeControllerNodeRef);

    /**
     * @return The id of the node that is the active controller of the cluster.
     * If there is no active controller or failed to get quorum information,
     * return -1 as the default value.
     * @throws io.strimzi.operator.common.UncheckedExecutionException Execution exception from clients
     * @throws io.strimzi.operator.common.UncheckedInterruptedException The thread was interrupted
     */
    int activeController();

    /**
     * Reconfigure the given server with the given configs
     *
     * @param nodeRef The node
     * @param kafkaBrokerConfigurationDiff The broker config diff
     * @param kafkaBrokerLoggingConfigurationDiff The broker logging diff
     */
    void reconfigureNode(NodeRef nodeRef, KafkaBrokerConfigurationDiff kafkaBrokerConfigurationDiff, KafkaBrokerLoggingConfigurationDiff kafkaBrokerLoggingConfigurationDiff);

    /**
     * Try to elect the given server as the leader for all the replicas on the server where it's not already
     * the preferred leader.
     * @param nodeRef The node
     * @return The number of replicas on the server which it is not leading, but is preferred leader
     * @throws io.strimzi.operator.common.UncheckedExecutionException Execution exception from clients
     * @throws io.strimzi.operator.common.UncheckedInterruptedException The thread was interrupted
     */
    int tryElectAllPreferredLeaders(NodeRef nodeRef);

    /**
     * Return the Kafka broker configs and logger configs for each of the given nodes
     * @param toList The nodes to get the configs for
     * @return A map from node id to configs
     * @throws io.strimzi.operator.common.UncheckedExecutionException Execution exception from clients
     * @throws io.strimzi.operator.common.UncheckedInterruptedException The thread was interrupted
     */
    Map<Integer, Configs> describeBrokerConfigs(List<NodeRef> toList);

    /**
     * Return the Kafka controller configs for each of the given nodes
     * @param toList The nodes to get the configs for
     * @return A map from node id to configs
     * @throws io.strimzi.operator.common.UncheckedExecutionException Execution exception from clients
     * @throws io.strimzi.operator.common.UncheckedInterruptedException The thread was interrupted
     */
    Map<Integer, Configs> describeControllerConfigs(List<NodeRef> toList);
}
