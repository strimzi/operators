/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

@Tag(REGRESSION)
class RackAwarenessST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(RackAwarenessST.class);
    private static final String TOPOLOGY_KEY = "kubernetes.io/hostname";

    /**
     * @description This test case verifies that Rack awareness configuration works as expected in Kafka Cluster.
     *
     * @steps
     *  1. - Deploy Kafka Clusters with rack topology key and also 'replica.selector.class' configured to rack aware value.
     *     - Kafka Clusters is deployed with according configuration in pod affinity, consumer client rack, and additional expected labels.
     *  2. - Make sure Kafka works as expected by producing and consuming data.
     *     - Data are successfully produced and consumed from Kafka Clusters.
     *
     * @usecase
     *  - rack-awareness
     *  - configuration
     *  - kafka
     *  - labels
     */
    @ParallelNamespaceTest
    void testKafkaRackAwareness() {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
                .editSpec()
                    .editKafka()
                        .withNewRack(TOPOLOGY_KEY)
                        .addToConfig("replica.selector.class", "org.apache.kafka.common.replica.RackAwareReplicaSelector")
                    .endKafka()
                .endSpec()
                .build());

        LOGGER.info("Kafka cluster deployed successfully");
        String podName = PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());
        Pod pod = kubeClient().getPod(testStorage.getNamespaceName(), podName);

        resourceManager.createResourceWithWait(
            AdminClientTemplates.plainAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName(), KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                // TODO: once new test-clients are released (0.10.0), this should be removed
                                .withImage("quay.io/strimzi-test-clients/test-clients:latest-kafka-" + Environment.ST_KAFKA_VERSION)
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build()
        );
        final AdminClient adminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());

        // check that spec matches the actual pod configuration
        Affinity specAffinity = StrimziPodSetUtils.getStrimziPodSetAffinity(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());
        NodeSelectorRequirement specNodeRequirement = specAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        NodeAffinity podAffinity = pod.getSpec().getAffinity().getNodeAffinity();
        NodeSelectorRequirement podNodeRequirement = podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(podNodeRequirement, is(specNodeRequirement));
        assertThat(specNodeRequirement.getKey(), is(TOPOLOGY_KEY));
        assertThat(specNodeRequirement.getOperator(), is("Exists"));

        PodAffinityTerm specPodAntiAffinityTerm = specAffinity.getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getPodAffinityTerm();
        PodAffinityTerm podAntiAffinityTerm = pod.getSpec().getAffinity().getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getPodAffinityTerm();
        assertThat(podAntiAffinityTerm, is(specPodAntiAffinityTerm));
        assertThat(specPodAntiAffinityTerm.getTopologyKey(), is(TOPOLOGY_KEY));
        assertThat(specPodAntiAffinityTerm.getLabelSelector().getMatchLabels(), hasEntry("strimzi.io/cluster", testStorage.getClusterName()));
        assertThat(specPodAntiAffinityTerm.getLabelSelector().getMatchLabels(), hasEntry("strimzi.io/name", KafkaResources.kafkaComponentName(testStorage.getClusterName())));

        // check Kafka rack awareness configuration
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;

        String nodeDescription = adminClient.describeNodes("all");
        assertThat(AdminClientUtils.getRack(nodeDescription, "0").contains(hostname), is(true));

        LOGGER.info("Producing and Consuming data in the Kafka cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    /**
     * @description This test case verifies that Rack awareness configuration works as expected in KafkaConnect. This is done by trying to deploy 2 Kafka Connect
     * Clusters; First one with valid topology key configuration, which will be scheduled and eventually progress into ready state, while invalid configuration
     * of the second Kafka Connect will render second Cluster unschedulable.
     *
     * @steps
     *  1. - Deploy Kafka Clusters, with 1 replica.
     *     - Kafka Clusters and its components are deployed.
     *  2. - Deploy Kafka Connect cluster, with rack topology key set to invalid value.
     *     - Kafka Connect is not scheduled.
     *  3. - Deploy Kafka Connect cluster, with rack topology key set to valid value.
     *     - Kafka Connect cluster is scheduled and eventually becomes ready, with its pod having all expected configuration in place.
     *  4. - Deploy Sink Kafka Connector and Produce 100 messages into the observed topic.
     *     - Messages are handled by Sink Connector successfully, thereby confirming Connect Cluster and Connector work correctly.
     *
     * @usecase
     *  - rack-awareness
     *  - configuration
     *  - connect
     */
    @Tag(CONNECT)
    @ParallelNamespaceTest
    void testConnectRackAwareness() {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String invalidTopologyKey = "invalid-topology-key";
        final String invalidConnectClusterName = testStorage.getClusterName() + "-invalid";

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build());

        LOGGER.info("Deploying unschedulable KafkaConnect: {}/{} with an invalid topology key: {}", testStorage.getNamespaceName(), invalidConnectClusterName, invalidTopologyKey);
        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), invalidConnectClusterName, testStorage.getClusterName(), 1)
                .editSpec()
                    .withNewRack(invalidTopologyKey)
                .endSpec()
                .build());

        LOGGER.info("Deploying KafkaConnect: {}/{} with a valid topology key: {}", testStorage.getNamespaceName(), testStorage.getClusterName(), TOPOLOGY_KEY);
        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .withNewRack(TOPOLOGY_KEY)
            .endSpec()
            .build());

        LOGGER.info("Check that KafkaConnect Pod is unschedulable");
        KafkaConnectUtils.waitForConnectPodCondition(testStorage.getNamespaceName(), "Unschedulable", invalidConnectClusterName, 30_000);

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("KafkaConnect cluster deployed successfully");
        String deployName = KafkaConnectResources.componentName(testStorage.getClusterName());
        String podName = PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), deployName);
        Pod pod = kubeClient().getPod(testStorage.getNamespaceName(), podName);

        // check that spec matches the actual pod configuration
        Affinity specAffinity = StUtils.getDeploymentOrStrimziPodSetAffinity(testStorage.getNamespaceName(), deployName);
        NodeSelectorRequirement specNodeRequirement = specAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        NodeAffinity podAffinity = pod.getSpec().getAffinity().getNodeAffinity();
        NodeSelectorRequirement podNodeRequirement = podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(podNodeRequirement, is(specNodeRequirement));
        assertThat(podNodeRequirement.getKey(), is(TOPOLOGY_KEY));
        assertThat(podNodeRequirement.getOperator(), is("Exists"));

        // check Kafka client rack awareness configuration
        String commandOut = cmdKubeClient(testStorage.getNamespaceName()).execInPod(podName,
                "/bin/bash", "-c", "cat /tmp/strimzi-connect.properties | grep consumer.client.rack").out().trim();
        assertThat(commandOut.contains("consumer.client.rack=${strimzidir:/opt/kafka/init:rack.id}"), is(true));

        // produce data which are to be available in the topic
        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        consumeDataWithNewSinkConnector(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getMessageCount());
    }

    /**
     * @description This test case verifies that Rack awareness configuration works as expected in KafkaMirrorMaker2 by configuring it and also using given CLuster.
     *
     * @steps
     *  1. - Deploy target and source Kafka Clusters, both with 1 broker and 1 controller replica.
     *     - Kafka Clusters and its components are deployed.
     *  2. - Deploy KafkaMirrorMaker2 Cluster with rack configuration set to 'kubernetes.io/hostname'.
     *     - KafkaMirrorMaker2 Cluster is deployed with according configuration in pod affinity, and consumer client rack on a given node.
     *  3. - Produce messages in the source Kafka Cluster and Consuming from mirrored KafkaTopic in the target Kafka Cluster, thus making sure KafkaMirrorMaker2 works as expected.
     *     - Data are produced and consumed from respective Kafka Clusters successfully.
     *
     * @usecase
     *  - rack-awareness
     *  - configuration
     *  - mirror-maker-2
     */
    @Tag(MIRROR_MAKER2)
    @ParallelNamespaceTest
    void testMirrorMaker2RackAwareness() {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build());

        resourceManager.createResourceWithWait(
                KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), 1, false)
                        .editSpec()
                            .withNewRack(TOPOLOGY_KEY)
                            .editFirstMirror()
                                .editSourceConnector()
                                    .addToConfig("refresh.topics.interval.seconds", "1")
                                .endSourceConnector()
                            .endMirror()
                        .endSpec()
                        .build());

        LOGGER.info("MirrorMaker2: {}/{} cluster deployed successfully", testStorage.getNamespaceName(), testStorage.getClusterName());
        String deployName = KafkaMirrorMaker2Resources.componentName(testStorage.getClusterName());
        String podName = PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), deployName);
        Pod pod = kubeClient().getPod(testStorage.getNamespaceName(), podName);

        // check that spec matches the actual pod configuration
        Affinity specAffinity = StUtils.getDeploymentOrStrimziPodSetAffinity(testStorage.getNamespaceName(), deployName);
        NodeSelectorRequirement specNodeRequirement = specAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        NodeAffinity podAffinity = pod.getSpec().getAffinity().getNodeAffinity();
        NodeSelectorRequirement podNodeRequirement = podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(podNodeRequirement, is(specNodeRequirement));
        assertThat(podNodeRequirement.getKey(), is(TOPOLOGY_KEY));
        assertThat(podNodeRequirement.getOperator(), is("Exists"));

        // check Kafka client rack awareness configuration
        String commandOut = cmdKubeClient(testStorage.getNamespaceName()).execInPod(podName, "/bin/bash", "-c", "cat /tmp/strimzi-connect.properties | grep consumer.client.rack").out().trim();
        assertThat(commandOut.contains("${strimzidir:/opt/kafka/init:rack.id}"), is(true));

        // Mirroring messages by: Producing to the Source Kafka Cluster and consuming them from mirrored KafkaTopic in target Kafka Cluster.

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), 3).build());

        LOGGER.info("Producing messages into the source Kafka: {}/{}, Topic: {}", testStorage.getNamespaceName(), testStorage.getSourceClusterName(), testStorage.getTopicName());
        final KafkaClients sourceClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()));
        resourceManager.createResourceWithWait(sourceClients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        LOGGER.info("Consuming messages in the target Kafka: {}/{} mirrored Topic: {}", testStorage.getNamespaceName(), testStorage.getTargetClusterName(), testStorage.getMirroredSourceTopicName());
        final KafkaClients targetClients = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .build();
        resourceManager.createResourceWithWait(targetClients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    private void consumeDataWithNewSinkConnector(String namespaceName, String newConnectorName, String connectClusterName, String topicName, int msgCount) {

        LOGGER.info("Deploying Sink KafkaConnector in KafkaConnect Cluster: {}/{}", namespaceName, newConnectorName);
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("file", TestConstants.DEFAULT_SINK_FILE_PATH);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(namespaceName, newConnectorName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        String kafkaConnectPodName = KubeClusterResource.kubeClient(namespaceName).listPods(connectClusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        LOGGER.info("KafkaConnect Pod: {}/{}", namespaceName, kafkaConnectPodName);
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(namespaceName, kafkaConnectPodName);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(namespaceName, kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, msgCount);
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
