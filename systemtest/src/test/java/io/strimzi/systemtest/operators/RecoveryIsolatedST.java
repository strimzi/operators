/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.rollingupdate.KafkaRollerIsolatedST;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.generateRandomNameOfKafka;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
class RecoveryIsolatedST extends AbstractST {

    static String sharedClusterName;
    private static final int KAFKA_REPLICAS = 3;
    private static final int ZOOKEEPER_REPLICAS = KAFKA_REPLICAS;

    private static final Logger LOGGER = LogManager.getLogger(RecoveryIsolatedST.class);

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaStrimziPodSetDeletion() {
        // kafka cluster already deployed
        String kafkaName = KafkaResource.getStrimziPodSetName(sharedClusterName);
        String kafkaUid = StrimziPodSetUtils.getStrimziPodSetUID(clusterOperator.getDeploymentNamespace(), kafkaName);

        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).withName(Constants.STRIMZI_DEPLOYMENT_NAME).withTimeoutInMillis(600_000L).scale(0);
        StrimziPodSetUtils.deleteStrimziPodSet(clusterOperator.getDeploymentNamespace(), kafkaName);

        PodUtils.waitForPodsWithPrefixDeletion(kafkaName);
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).withName(Constants.STRIMZI_DEPLOYMENT_NAME).withTimeoutInMillis(600_000L).scale(1);

        LOGGER.info("Waiting for recovery {}", kafkaName);
        StrimziPodSetUtils.waitForStrimziPodSetRecovery(clusterOperator.getDeploymentNamespace(), kafkaName, kafkaUid);
        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaName, KafkaResources.kafkaStatefulSetName(sharedClusterName), KAFKA_REPLICAS);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test class")
    void testRecoveryFromZookeeperStrimziPodSetDeletion() {
        // kafka cluster already deployed
        String zookeeperName = KafkaResources.zookeeperStatefulSetName(sharedClusterName);
        String zookeeperUid = StrimziPodSetUtils.getStrimziPodSetUID(clusterOperator.getDeploymentNamespace(), zookeeperName);

        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).withName(Constants.STRIMZI_DEPLOYMENT_NAME).withTimeoutInMillis(600_000L).scale(0);
        StrimziPodSetUtils.deleteStrimziPodSet(clusterOperator.getDeploymentNamespace(), zookeeperName);

        PodUtils.waitForPodsWithPrefixDeletion(zookeeperName);
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).withName(Constants.STRIMZI_DEPLOYMENT_NAME).withTimeoutInMillis(600_000L).scale(1);

        LOGGER.info("Waiting for recovery {}", zookeeperName);
        StrimziPodSetUtils.waitForStrimziPodSetRecovery(clusterOperator.getDeploymentNamespace(), zookeeperName, zookeeperUid);
        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(clusterOperator.getDeploymentNamespace(), zookeeperName, zookeeperName, ZOOKEEPER_REPLICAS);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaServiceDeletion(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", sharedClusterName);

        String kafkaServiceName = KafkaResources.bootstrapServiceName(sharedClusterName);
        String kafkaServiceUid = kubeClient().getServiceUid(kafkaServiceName);

        kubeClient().deleteService(kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        ServiceUtils.waitForServiceRecovery(clusterOperator.getDeploymentNamespace(), kafkaServiceName, kafkaServiceUid);
        verifyStabilityBySendingAndReceivingMessages(extensionContext, testStorage);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test class")
    void testRecoveryFromZookeeperServiceDeletion(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", sharedClusterName);

        String zookeeperServiceName = KafkaResources.zookeeperServiceName(sharedClusterName);
        String zookeeperServiceUid = kubeClient().getServiceUid(zookeeperServiceName);

        kubeClient().deleteService(zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        ServiceUtils.waitForServiceRecovery(clusterOperator.getDeploymentNamespace(), zookeeperServiceName, zookeeperServiceUid);

        verifyStabilityBySendingAndReceivingMessages(extensionContext, testStorage);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromKafkaHeadlessServiceDeletion(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", sharedClusterName);

        String kafkaHeadlessServiceName = KafkaResources.brokersServiceName(sharedClusterName);
        String kafkaHeadlessServiceUid = kubeClient().getServiceUid(kafkaHeadlessServiceName);

        kubeClient().deleteService(kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(clusterOperator.getDeploymentNamespace(), kafkaHeadlessServiceName, kafkaHeadlessServiceUid);

        verifyStabilityBySendingAndReceivingMessages(extensionContext, testStorage);
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test class")
    void testRecoveryFromZookeeperHeadlessServiceDeletion(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", sharedClusterName);

        String zookeeperHeadlessServiceName = KafkaResources.zookeeperHeadlessServiceName(sharedClusterName);
        String zookeeperHeadlessServiceUid = kubeClient().getServiceUid(zookeeperHeadlessServiceName);

        kubeClient().deleteService(zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        ServiceUtils.waitForServiceRecovery(clusterOperator.getDeploymentNamespace(), zookeeperHeadlessServiceName, zookeeperHeadlessServiceUid);

        verifyStabilityBySendingAndReceivingMessages(extensionContext, testStorage);
    }

    /**
     * The main difference between this test and KafkaRollerST#testKafkaPodPending()
     * is that in this test, we are deploying Kafka cluster with an impossible memory request,
     * but in the KafkaRollerST#testKafkaPodPending()
     * we first deploy Kafka cluster with a correct configuration, then change the configuration to an unschedulable one, waiting
     * for one Kafka pod to be in the `Pending` phase. In this test, all 3 Kafka pods are `Pending`. After we
     * check that Kafka pods are stable in `Pending` phase (for one minute), we change the memory request so that the pods are again schedulable
     * and wait until the Kafka cluster recovers and becomes `Ready`.
     *
     * @see {@link KafkaRollerIsolatedST#testKafkaPodPending(ExtensionContext)}
     */
    @IsolatedTest("We need for each test case its own Cluster Operator")
    void testRecoveryFromImpossibleMemoryRequest() {
        final String kafkaSsName = KafkaResource.getStrimziPodSetName(sharedClusterName);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(sharedClusterName, KafkaResources.kafkaStatefulSetName(sharedClusterName));
        final Map<String, Quantity> requests = new HashMap<>(1);

        requests.put("memory", new Quantity("465458732Gi"));
        final ResourceRequirements resourceReq = new ResourceRequirementsBuilder()
            .withRequests(requests)
            .build();

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(sharedClusterName), knp ->
                knp.getSpec().setResources(resourceReq), clusterOperator.getDeploymentNamespace());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(sharedClusterName, k -> k.getSpec().getKafka().setResources(resourceReq), clusterOperator.getDeploymentNamespace());
        }

        PodUtils.waitForPendingPod(clusterOperator.getDeploymentNamespace(), kafkaSsName);
        PodUtils.verifyThatPendingPodsAreStable(clusterOperator.getDeploymentNamespace(), kafkaSsName);

        requests.put("memory", new Quantity("512Mi"));
        resourceReq.setRequests(requests);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(sharedClusterName), knp ->
                knp.getSpec().setResources(resourceReq), clusterOperator.getDeploymentNamespace());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(sharedClusterName, k -> k.getSpec().getKafka().setResources(resourceReq), clusterOperator.getDeploymentNamespace());
        }

        RollingUpdateUtils.waitForComponentAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, KAFKA_REPLICAS);
        KafkaUtils.waitForKafkaReady(clusterOperator.getDeploymentNamespace(), sharedClusterName);
    }

    private void verifyStabilityBySendingAndReceivingMessages(ExtensionContext extensionContext, TestStorage testStorage) {
        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(sharedClusterName))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @IsolatedTest
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test class")
    void testRecoveryFromKafkaAndZookeeperPodDeletion() {
        final String kafkaName = KafkaResources.kafkaStatefulSetName(sharedClusterName);
        final String kafkaStrimziPodSet = KafkaResource.getStrimziPodSetName(sharedClusterName);
        final String zkName = KafkaResources.zookeeperStatefulSetName(sharedClusterName);

        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(sharedClusterName, kafkaName);
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(sharedClusterName, zkName);

        LOGGER.info("Deleting most of the Kafka and ZK Pods");
        List<Pod> kafkaPodList = kubeClient().listPods(kafkaSelector);
        List<Pod> zkPodList = kubeClient().listPods(zkSelector);

        kafkaPodList.subList(0, kafkaPodList.size() - 1).forEach(pod -> kubeClient().deletePod(pod));
        zkPodList.subList(0, zkPodList.size() - 1).forEach(pod -> kubeClient().deletePod(pod));

        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaStrimziPodSet, kafkaName, KAFKA_REPLICAS);
        StrimziPodSetUtils.waitForAllStrimziPodSetAndPodsReady(clusterOperator.getDeploymentNamespace(), zkName, zkName, ZOOKEEPER_REPLICAS);
        KafkaUtils.waitForKafkaReady(clusterOperator.getDeploymentNamespace(), sharedClusterName);
    }

    @BeforeEach
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator.defaultInstallation(extensionContext)
            .withReconciliationInterval(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();

        sharedClusterName = generateRandomNameOfKafka("recovery-cluster");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(sharedClusterName, KAFKA_REPLICAS).build());
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(sharedClusterName, KafkaResources.plainBootstrapAddress(sharedClusterName), 1).build());
    }
}
