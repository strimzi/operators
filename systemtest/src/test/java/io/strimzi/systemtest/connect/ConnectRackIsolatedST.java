/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CO_OPERATION_TIMEOUT_SHORT;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.RACK_AWARENESS;
import static io.strimzi.systemtest.Constants.REGRESSION;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.ResourceManager;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.ScraperUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
@Tag(INTERNAL_CLIENTS_USED)
@Tag(RACK_AWARENESS)
@IsolatedSuite
class ConnectRackIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ConnectRackIsolatedST.class);

    @IsolatedTest
    void testConnectRackAwareness(ExtensionContext extensionContext) {
        assumeFalse(Environment.isNamespaceRbacScope());

        TestStorage storage = new TestStorage(extensionContext);
        String namespace = storage.getNamespaceName();
        String clusterName = storage.getClusterName();
        String topologyKey = "kubernetes.io/hostname";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 1, 1).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 1)
                .editSpec()
                    .withNewRack(topologyKey)
                .endSpec()
                .build());

        LOGGER.info("Connect cluster deployed successfully");
        String deployName = KafkaConnectResources.deploymentName(clusterName);
        String podName = PodUtils.getPodNameByPrefix(namespace, deployName);
        Pod pod = PodUtils.getPodByName(namespace, podName);

        // check that spec matches the actual pod configuration
        Affinity specAffinity = kubeClient().getDeployment(deployName).getSpec().getTemplate().getSpec().getAffinity();
        NodeSelectorRequirement specNodeRequirement = specAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        NodeAffinity podAffinity = pod.getSpec().getAffinity().getNodeAffinity();
        NodeSelectorRequirement podNodeRequirement = podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(podNodeRequirement, is(specNodeRequirement));
        assertThat(podNodeRequirement.getKey(), is(topologyKey));
        assertThat(podNodeRequirement.getOperator(), is("Exists"));

        // check Kafka client rack awareness configuration
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;
        String commandOut = cmdKubeClient().execInPod(podName, "/bin/bash", "-c", "cat /tmp/strimzi-connect.properties | grep consumer.client.rack").out().trim();
        assertThat(commandOut.equals("consumer.client.rack=" + hostname), is(true));
    }

    @IsolatedTest
    void testConnectRackAwarenessCorrectDeployment(ExtensionContext extensionContext) {
        assumeFalse(Environment.isNamespaceRbacScope());
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topologyKey = "kubernetes.io/hostname";

        // We need to update CO configuration to set OPERATION_TIMEOUT to shorter value, because we expect timeout in that test
        Map<String, String> coSnapshot = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName());
        // We have to install CO in class stack, otherwise it will be deleted at the end of test case and all following tests will fail
        clusterOperator.unInstall();
        clusterOperator = clusterOperator.defaultInstallation()
                .withOperationTimeout(CO_OPERATION_TIMEOUT_SHORT)
                .withReconciliationInterval(Constants.RECONCILIATION_INTERVAL)
                .createInstallation()
                .runBundleInstallation();

        coSnapshot = DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coSnapshot);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName,  3)
                .editSpec()
                    .editKafka()
                        .withNewRack(topologyKey)
                        .addToConfig("replica.selector.class", "org.apache.kafka.common.replica.RackAwareReplicaSelector")
                    .endKafka()
                .endSpec()
                .build());

        // we should create topic before KafkaConnect - topic is recreated if we delete it before KafkaConnect
        String topicName = "rw-" + KafkaTopicUtils.generateRandomNameOfTopic();
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Deploy KafkaConnect with correct rack-aware topology key: {}", topologyKey);
        KafkaConnect kc = KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
                .editSpec()
                    .withNewRack(topologyKey)
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .endSpec()
                .build();

        resourceManager.createResource(extensionContext, kc);

        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, kc, KafkaConnectResources.deploymentName(clusterName));

        String connectPodName = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0);

        Affinity connectPodSpecAffinity = kubeClient().getDeployment(KafkaConnectResources.deploymentName(clusterName)).getSpec().getTemplate().getSpec().getAffinity();
        NodeSelectorRequirement connectPodNodeSelectorRequirement = connectPodSpecAffinity.getNodeAffinity()
                .getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        Pod connectPod = kubeClient().getPod(connectPodName);
        NodeAffinity nodeAffinity = connectPod.getSpec().getAffinity().getNodeAffinity();

        LOGGER.info("PodName: {}\nNodeAffinity: {}", connectPodName, nodeAffinity);
        assertThat(connectPodNodeSelectorRequirement.getKey(), is(topologyKey));
        assertThat(connectPodNodeSelectorRequirement.getOperator(), is("Exists"));

        // fetch scraper Pod name
        final String scraperPodName = ScraperUtils.getScraperPod(clusterOperator.getDeploymentNamespace()).getMetadata().getName();

        KafkaConnectUtils.sendReceiveMessagesThroughConnect(connectPodName, topicName, kafkaClientsPodName, scraperPodName, clusterOperator.getDeploymentNamespace(), clusterName);
        // Revert changes for CO deployment
        clusterOperator.unInstall();
        clusterOperator = clusterOperator.defaultInstallation()
                .createInstallation()
                .runInstallation();

        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coSnapshot);
    }

    @IsolatedTest
    void testConnectRackAwarenessWrongDeployment(ExtensionContext extensionContext) {
        assumeFalse(Environment.isNamespaceRbacScope());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        Map<String, String> label = Collections.singletonMap("my-label", "value");
        Map<String, String> anno = Collections.singletonMap("my-annotation", "value");
        String topologyKey = "kubernetes.io/hostname";
        String wrongTopologyKey = "wrong-topology-key";

        // We need to update CO configuration to set OPERATION_TIMEOUT to shorter value, because we expect timeout in that test
        Map<String, String> coSnapshot = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName());
        // We have to install CO in class stack, otherwise it will be deleted at the end of test case and all following tests will fail
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
                .withNamespace(clusterOperator.getDeploymentNamespace())
                .withOperationTimeout(CO_OPERATION_TIMEOUT_SHORT)
                .withReconciliationInterval(Constants.RECONCILIATION_INTERVAL)
                .createInstallation()
                .runBundleInstallation();

        coSnapshot = DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName(), 1, coSnapshot);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
                .editMetadata()
                    .withNamespace(clusterOperator.getDeploymentNamespace())
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withNewRack(topologyKey)
                        .addToConfig("replica.selector.class", "org.apache.kafka.common.replica.RackAwareReplicaSelector")
                    .endKafka()
                .endSpec()
                .build());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(clusterOperator.getDeploymentNamespace(), false, kafkaClientsName).build());
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(clusterOperator.getDeploymentNamespace(), kafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Deploy KafkaConnect with wrong rack-aware topology key: {}", wrongTopologyKey);

        KafkaConnect kc = KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, clusterName, 1)
                .editMetadata()
                    .withNamespace(clusterOperator.getDeploymentNamespace())
                .endMetadata()
                .editSpec()
                    .withNewRack(wrongTopologyKey)
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .editOrNewTemplate()
                        .withNewClusterRoleBinding()
                            .withNewMetadata()
                                .withAnnotations(anno)
                                .withLabels(label)
                            .endMetadata()
                        .endClusterRoleBinding()
                    .endTemplate()
                .endSpec()
                .build();

        resourceManager.createResource(extensionContext, false,  kc);

        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, kc, KafkaConnectResources.deploymentName(clusterName));

        PodUtils.waitForPendingPod(clusterOperator.getDeploymentNamespace(), clusterName + "-connect");
        List<String> connectWrongPods = kubeClient().listPodNames(clusterOperator.getDeploymentNamespace(), clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND);
        String connectWrongPodName = connectWrongPods.get(0);
        LOGGER.info("Waiting for ClusterOperator to get timeout operation of incorrectly set up KafkaConnect");
        KafkaConnectUtils.waitForKafkaConnectCondition("TimeoutException", "NotReady", clusterOperator.getDeploymentNamespace(), clusterName);

        PodStatus kcWrongStatus = kubeClient().getPod(clusterOperator.getDeploymentNamespace(), connectWrongPodName).getStatus();
        assertThat("Unschedulable", is(kcWrongStatus.getConditions().get(0).getReason()));
        assertThat("PodScheduled", is(kcWrongStatus.getConditions().get(0).getType()));

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName, kafkaConnect -> {
            kafkaConnect.getSpec().setRack(new Rack(topologyKey));
        }, clusterOperator.getDeploymentNamespace());
        KafkaConnectUtils.waitForConnectReady(clusterOperator.getDeploymentNamespace(), clusterName);
        LOGGER.info("KafkaConnect is ready with changed rack key: '{}'.", topologyKey);
        LOGGER.info("Verify KafkaConnect rack key update");
        kc = KafkaConnectResource.kafkaConnectClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterName).get();
        assertThat(kc.getSpec().getRack().getTopologyKey(), is(topologyKey));

        final String scraperPodName = ScraperUtils.getScraperPod(clusterOperator.getDeploymentNamespace()).getMetadata().getName();

        List<String> kcPods = kubeClient().listPodNames(clusterOperator.getDeploymentNamespace(), clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND);
        KafkaConnectUtils.sendReceiveMessagesThroughConnect(kcPods.get(0), TOPIC_NAME, kafkaClientsPodName, scraperPodName, clusterOperator.getDeploymentNamespace(), clusterName);

        // check the ClusterRoleBinding annotations and labels in Kafka cluster
        Map<String, String> actualLabel = KafkaConnectResource.kafkaConnectClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterName).get().getSpec().getTemplate().getClusterRoleBinding().getMetadata().getLabels();
        Map<String, String> actualAnno = KafkaConnectResource.kafkaConnectClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterName).get().getSpec().getTemplate().getClusterRoleBinding().getMetadata().getAnnotations();

        assertThat(actualLabel, is(label));
        assertThat(actualAnno, is(anno));

        // revert changes for CO deployment
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
                .withNamespace(clusterOperator.getDeploymentNamespace())
                .createInstallation()
                .runBundleInstallation();

        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName(), 1, coSnapshot);
    }

    @BeforeAll
    void setUp() {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
