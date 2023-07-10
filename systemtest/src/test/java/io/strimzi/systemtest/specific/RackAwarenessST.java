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
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

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
    void testKafkaRackAwareness(ExtensionContext extensionContext) {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 1, 1)
                .editSpec()
                    .editKafka()
                        .withNewRack(TOPOLOGY_KEY)
                        .addToConfig("replica.selector.class", "org.apache.kafka.common.replica.RackAwareReplicaSelector")
                    .endKafka()
                .endSpec()
                .build());

        LOGGER.info("Kafka cluster deployed successfully");
        String ssName = testStorage.getKafkaStatefulSetName();
        String podName = PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), ssName);
        Pod pod = kubeClient().getPod(testStorage.getNamespaceName(), podName);

        // check that spec matches the actual pod configuration
        Affinity specAffinity = StrimziPodSetUtils.getStrimziPodSetAffinity(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());
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
        assertThat(specPodAntiAffinityTerm.getLabelSelector().getMatchLabels(), hasEntry("strimzi.io/name", KafkaResources.kafkaStatefulSetName(testStorage.getClusterName())));

        // check Kafka rack awareness configuration
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;

        String rackIdOut = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0),
                "/bin/bash", "-c", "cat /opt/kafka/init/rack.id").out().trim();
        String brokerRackOut = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0),
                "/bin/bash", "-c", "cat /tmp/strimzi.properties | grep broker.rack").out().trim();
        assertThat(rackIdOut.trim(), is(hostname));
        assertThat(brokerRackOut.contains("broker.rack=" + hostname), is(true));

        LOGGER.info("Producing and Consuming data in the Kafka cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);
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
    void testConnectRackAwareness(ExtensionContext extensionContext) {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        final TestStorage testStorage = storageMap.get(extensionContext);
        final String invalidTopologyKey = "invalid-topology-key";
        final String invalidConnectClusterName = testStorage.getClusterName() + "-invalid";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 1, 1).build());

        LOGGER.info("Deploying unschedulable KafkaConnect: {}/{} with an invalid topology key: {}", testStorage.getNamespaceName(), invalidConnectClusterName, invalidTopologyKey);
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(invalidConnectClusterName, testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
                .editSpec()
                    .withNewRack(invalidTopologyKey)
                .endSpec()
                .build());

        LOGGER.info("Deploying KafkaConnect: {}/{} with a valid topology key: {}", testStorage.getNamespaceName(), testStorage.getClusterName(), TOPOLOGY_KEY);
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .withNewRack(TOPOLOGY_KEY)
                .endSpec()
                .build());

        LOGGER.info("Check that KafkaConnect Pod is unschedulable");
        KafkaConnectUtils.waitForConnectPodCondition("Unschedulable", testStorage.getNamespaceName(), invalidConnectClusterName, 30_000);

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("KafkaConnect cluster deployed successfully");
        String deployName = KafkaConnectResources.deploymentName(testStorage.getClusterName());
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
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;
        String commandOut = cmdKubeClient(testStorage.getNamespaceName()).execInPod(podName,
                "/bin/bash", "-c", "cat /tmp/strimzi-connect.properties | grep consumer.client.rack").out().trim();
        assertThat(commandOut.equals("consumer.client.rack=" + hostname), is(true));

        // produce data which are to be available in the topic
        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        consumeDataWithNewSinkConnector(extensionContext, testStorage.getClusterName(), testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName());
    }

    /**
     * @description This test case verifies that Rack awareness configuration works as expected in KafkaMirrorMaker2 by configuring it and also using given CLuster.
     *
     * @steps
     *  1. - Deploy target and source Kafka Clusters, both with 1 Kafka and 1 Zookeeper replica.
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
    void testMirrorMaker2RackAwareness(ExtensionContext extensionContext) {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        TestStorage testStorage = storageMap.get(extensionContext);
        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";
        String sourceMirroredTopicName = kafkaClusterSourceName + "." + testStorage.getTopicName();

        resourceManager.createResource(extensionContext,
                KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        resourceManager.createResource(extensionContext,
                KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext,
                KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
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
        String deployName = KafkaMirrorMaker2Resources.deploymentName(testStorage.getClusterName());
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
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;
        String commandOut = cmdKubeClient(testStorage.getNamespaceName()).execInPod(podName, "/bin/bash", "-c", "cat /tmp/strimzi-connect.properties | grep consumer.client.rack").out().trim();
        assertThat(commandOut.equals("consumer.client.rack=" + hostname), is(true));

        // Mirroring messages by: Producing to the Source Kafka Cluster and consuming them from mirrored KafkaTopic in target Kafka Cluster.

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), 3, testStorage.getNamespaceName()).build());

        LOGGER.info("Producing messages into the source Kafka: {}/{}, Topic: {}", testStorage.getNamespaceName(), kafkaClusterSourceName, testStorage.getTopicName());
        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();
        resourceManager.createResource(extensionContext, clients.producerStrimzi());
        ClientUtils.waitForProducerClientSuccess(testStorage);

        LOGGER.info("Consuming messages in the target Kafka: {}/{} mirrored Topic: {}", testStorage.getNamespaceName(), kafkaClusterTargetName, sourceMirroredTopicName);
        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();
        resourceManager.createResource(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    private void consumeDataWithNewSinkConnector(ExtensionContext extensionContext, String newConnectorName, String connectClusterName, String topicName, String namespace) {

        LOGGER.info("Deploying Sink KafkaConnector in KafkaConnect Cluster: {}/{}", namespace, newConnectorName);
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("file", Constants.DEFAULT_SINK_FILE_PATH);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(newConnectorName)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        String kafkaConnectPodName = KubeClusterResource.kubeClient(namespace).listPods(connectClusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        LOGGER.info("KafkaConnect Pod: {}/{}", namespace, kafkaConnectPodName);
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(namespace, kafkaConnectPodName);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(namespace, kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @BeforeEach
    void createTestResources(ExtensionContext extensionContext) {
        storageMap.put(extensionContext, new TestStorage(extensionContext));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation(extensionContext)
                .createInstallation()
                .runInstallation();
    }
}
