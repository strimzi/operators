/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.COMPONENT_SCALING;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
class RollingUpdateST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RollingUpdateST.class);
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @KRaftNotSupported("ZooKeeper is not supported by KRaft mode and is used in this test case")
    void testRecoveryDuringZookeeperRollingUpdate(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 2, 2, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        LOGGER.info("Update resources for Pods");

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        }, testStorage.getNamespaceName());

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());
        LOGGER.info("Verifying stability of ZooKeeper Pods except the one, which is in pending phase");
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());

        LOGGER.info("Verifying stability of Kafka Pods");
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build());
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 3);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(newTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testRecoveryDuringKafkaRollingUpdate(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 2, 2, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        LOGGER.info("Update resources for Pods");

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp ->
                knp.getSpec().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build()), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
                k.getSpec()
                    .getKafka()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
            }, testStorage.getNamespaceName());
        }

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());
        LOGGER.info("Verifying stability of Kafka Pods except the one, which is in pending phase");
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Verifying stability of ZooKeeper Pods");
            PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());
        }

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp ->
                knp.getSpec().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build()), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
                k.getSpec()
                    .getKafka()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("200m"))
                        .build());
            }, testStorage.getNamespaceName());
        }

        // This might need to wait for the previous reconciliation to timeout and for the KafkaRoller to timeout.
        // Therefore we use longer timeout.
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(newTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @Tag(COMPONENT_SCALING)
    @KRaftNotSupported("The scaling of the Kafka Pods is not working properly at the moment")
    void testKafkaAndZookeeperScaleUpScaleDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editSpec()
                .editKafka()
                    // Topic Operator doesn't support KRaft, yet, using auto topic creation and default replication factor as workaround
                    .addToConfig(singletonMap("default.replication.factor", Environment.isKRaftModeEnabled() ? 3 : 1))
                    .addToConfig("auto.create.topics.enable", Environment.isKRaftModeEnabled())
                .endKafka()
            .endSpec()
            .build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        testDockerImagesForKafkaCluster(testStorage.getClusterName(), clusterOperator.getDeploymentNamespace(), testStorage.getNamespaceName(), 3, 3, false);

        LOGGER.info("Running kafkaScaleUpScaleDown {}", testStorage.getClusterName());

        final int initialReplicas = kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withLabelSelector(testStorage.getKafkaSelector()).list().getItems().size();
        assertEquals(3, initialReplicas);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 3, initialReplicas, initialReplicas, testStorage.getNamespaceName()).build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        // scale up
        final int scaleTo = initialReplicas + 4;
        LOGGER.info("Scale up Kafka to {}", scaleTo);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp ->
                knp.getSpec().setReplicas(scaleTo), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
                kafka.getSpec().getKafka().setReplicas(scaleTo);
            }, testStorage.getNamespaceName());
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), scaleTo);

        LOGGER.info("Kafka scale up to {} finished", scaleTo);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        assertThat((int) kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            pvc -> pvc.getMetadata().getName().contains(testStorage.getKafkaStatefulSetName())).count(), is(scaleTo));

        if (!Environment.isKRaftModeEnabled()) {
            // try to scale up ZK in ZK mode to make sure everything is good
            final int zookeeperScaleTo = initialReplicas + 2;
            Map<String, String> zooKeeperPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

            LOGGER.info("Scale up ZooKeeper to {}", zookeeperScaleTo);

            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getZookeeper().setReplicas(zookeeperScaleTo), testStorage.getNamespaceName());
            RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), zookeeperScaleTo);

            LOGGER.info("ZooKeeper scale up to {} finished", zookeeperScaleTo);


            clients = new KafkaClientsBuilder(clients)
                    .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
                    .build();

            resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
            ClientUtils.waitForConsumerClientSuccess(testStorage);
        }

        // scale down
        LOGGER.info("Scale down Kafka to {}", initialReplicas);
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp ->
                knp.getSpec().setReplicas(initialReplicas), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getKafka().setReplicas(initialReplicas), testStorage.getNamespaceName());
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), initialReplicas);

        LOGGER.info("Kafka scale down to {} finished", initialReplicas);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        PersistentVolumeClaimUtils.waitForPersistentVolumeClaimDeletion(testStorage, initialReplicas);

        // Create new topic to ensure, that ZK or KRaft is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(newTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(COMPONENT_SCALING)
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test case")
    void testZookeeperScaleUpScaleDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build(),
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", testStorage.getClusterName());
        final int initialZkReplicas = kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withLabelSelector(testStorage.getZookeeperSelector()).list().getItems().size();
        assertThat(initialZkReplicas, is(3));

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        final int scaleZkTo = initialZkReplicas + 4;
        final List<String> newZkPodNames = new ArrayList<String>() {{
                for (int i = initialZkReplicas; i < scaleZkTo; i++) {
                    add(KafkaResources.zookeeperPodName(testStorage.getClusterName(), i));
                }
            }};

        LOGGER.info("Scale up ZooKeeper to {}", scaleZkTo);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getZookeeper().setReplicas(scaleZkTo), testStorage.getNamespaceName());

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), scaleZkTo);
        // check the new node is either in leader or follower state
        KafkaUtils.waitForZkMntr(testStorage.getNamespaceName(), testStorage.getClusterName(), ZK_SERVER_STATE, 0, 1, 2, 3, 4, 5, 6);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String scaleUpTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), scaleUpTopicName, 1, 1, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(scaleUpTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        // scale down
        LOGGER.info("Scale down ZooKeeper to {}", initialZkReplicas);
        // Get zk-3 uid before deletion
        String uid = kubeClient(testStorage.getNamespaceName()).getPodUid(newZkPodNames.get(3));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas), testStorage.getNamespaceName());

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), initialZkReplicas);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        // Wait for one zk pods will became leader and others follower state
        KafkaUtils.waitForZkMntr(testStorage.getNamespaceName(), testStorage.getClusterName(), ZK_SERVER_STATE, 0, 1, 2);

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String scaleDownTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), scaleDownTopicName, 1, 1, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(scaleDownTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        //Test that the second pod has event 'Killing'
        assertThat(kubeClient(testStorage.getNamespaceName()).listEventsByResourceUid(uid), hasAllOfReasons(Killing));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testBrokerConfigurationChangeTriggerRollingUpdate(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> zkPods = null;
        if (!Environment.isKRaftModeEnabled()) {
            zkPods = PodUtils.podSnapshot(namespaceName, zkSelector);
        }

        // Changes to readiness probe should trigger a rolling update
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
        }, namespaceName);

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);
        if (!Environment.isKRaftModeEnabled()) {
            assertThat(PodUtils.podSnapshot(namespaceName, zkSelector), is(zkPods));
        }
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testManualKafkaConfigMapChangeDontTriggerRollingUpdate(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> zkPods = PodUtils.podSnapshot(namespaceName, zkSelector);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(clusterName, 3)) {
            ConfigMap configMap = kubeClient(namespaceName).getConfigMap(namespaceName, cmName);
            configMap.getData().put("new.kafka.config", "new.config.value");
            kubeClient().updateConfigMapInNamespace(namespaceName, configMap);
        }

        PodUtils.verifyThatRunningPodsAreStable(namespaceName, clusterName);

        assertThat(PodUtils.podSnapshot(namespaceName, zkSelector), is(zkPods));
        assertThat(PodUtils.podSnapshot(namespaceName, kafkaSelector), is(kafkaPods));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testExternalLoggingChangeTriggerRollingUpdate(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        // EO dynamic logging is tested in io.strimzi.systemtest.log.LoggingChangeST.testDynamicallySetEOloggingLevels
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> zkPods = null;
        if (!Environment.isKRaftModeEnabled()) {
            zkPods = PodUtils.podSnapshot(namespaceName, zkSelector);
        }

        String loggersConfig = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]\n" +
                "kafka.root.logger.level=INFO\n" +
                "log4j.rootLogger=${kafka.root.logger.level}, CONSOLE\n" +
                "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
                "log4j.logger.org.apache.zookeeper=INFO\n" +
                "log4j.logger.kafka=INFO\n" +
                "log4j.logger.org.apache.kafka=INFO\n" +
                "log4j.logger.kafka.request.logger=WARN, CONSOLE\n" +
                "log4j.logger.kafka.network.Processor=INFO\n" +
                "log4j.logger.kafka.server.KafkaApis=INFO\n" +
                "log4j.logger.kafka.network.RequestChannel$=INFO\n" +
                "log4j.logger.kafka.controller=INFO\n" +
                "log4j.logger.kafka.log.LogCleaner=INFO\n" +
                "log4j.logger.state.change.logger=TRACE\n" +
                "log4j.logger.kafka.authorizer.logger=INFO";

        String configMapLoggersName = "loggers-config-map";
        ConfigMap configMapLoggers = new ConfigMapBuilder()
                .withNewMetadata()
                    .withNamespace(namespaceName)
                    .withName(configMapLoggersName)
                .endMetadata()
                .addToData("log4j-custom.properties", loggersConfig)
                .build();

        ConfigMapKeySelector log4jLoggimgCMselector = new ConfigMapKeySelectorBuilder()
                .withName(configMapLoggersName)
                .withKey("log4j-custom.properties")
                .build();

        kubeClient().createConfigMapInNamespace(namespaceName, configMapLoggers);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setLogging(new ExternalLoggingBuilder()
                    .withNewValueFrom()
                        .withConfigMapKeyRef(log4jLoggimgCMselector)
                    .endValueFrom()
                    .build());
            if (!Environment.isKRaftModeEnabled()) {
                kafka.getSpec().getZookeeper().setLogging(new ExternalLoggingBuilder()
                    .withNewValueFrom()
                        .withConfigMapKeyRef(log4jLoggimgCMselector)
                    .endValueFrom()
                    .build());
            }
        }, namespaceName);

        if (!Environment.isKRaftModeEnabled()) {
            zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(namespaceName, zkSelector, 3, zkPods);
        }
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);

        configMapLoggers.getData().put("log4j-custom.properties", loggersConfig.replace("%p %m (%c) [%t]", "%p %m (%c) [%t]%n"));
        kubeClient().updateConfigMapInNamespace(namespaceName, configMapLoggers);

        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(namespaceName, zkSelector, 3, zkPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);
    }

    @IsolatedTest("Deleting Pod of Shared Cluster Operator")
    @Tag(ROLLING_UPDATE)
    void testClusterOperatorFinishAllRollingUpdates(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), kafkaSelector);
        Map<String, String> zkPods = null;
        if (!Environment.isKRaftModeEnabled()) {
            zkPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zkSelector);
        }

        // Changes to readiness probe should trigger a rolling update
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
            if (!Environment.isKRaftModeEnabled()) {
                kafka.getSpec().getZookeeper().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
            }
        }, clusterOperator.getDeploymentNamespace());

        TestUtils.waitFor("rolling update starts", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient(clusterOperator.getDeploymentNamespace()).listPods(clusterOperator.getDeploymentNamespace()).stream().filter(pod -> pod.getStatus().getPhase().equals("Running"))
                    .map(pod -> pod.getStatus().getPhase()).collect(Collectors.toList()).size() < kubeClient().listPods(clusterOperator.getDeploymentNamespace()).size());

        LabelSelector coLabelSelector = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName()).getSpec().getSelector();
        LOGGER.info("Deleting Cluster Operator Pod with labels {}", coLabelSelector);
        kubeClient(clusterOperator.getDeploymentNamespace()).deletePodsByLabelSelector(coLabelSelector);
        LOGGER.info("Cluster Operator Pod deleted");

        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), zkSelector, 3, zkPods);
        }

        TestUtils.waitFor("rolling update starts", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient(clusterOperator.getDeploymentNamespace()).listPods(clusterOperator.getDeploymentNamespace()).stream().map(pod -> pod.getStatus().getPhase()).collect(Collectors.toList()).contains("Pending"));

        LOGGER.info("Deleting Cluster Operator Pod with labels {}", coLabelSelector);
        kubeClient(clusterOperator.getDeploymentNamespace()).deletePodsByLabelSelector(coLabelSelector);
        LOGGER.info("Cluster Operator Pod deleted");

        RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
    }

    @IsolatedTest
    @Tag(ROLLING_UPDATE)
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test class")
    @SuppressWarnings("checkstyle:MethodLength")
    void testMetricsChange(ExtensionContext extensionContext) throws JsonProcessingException {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        //Kafka
        Map<String, Object> kafkaRule = new HashMap<>();
        kafkaRule.put("pattern", "kafka.(\\w+)<type=(.+), name=(.+)><>Count");
        kafkaRule.put("name", "kafka_$1_$2_$3_count");
        kafkaRule.put("type", "COUNTER");

        Map<String, Object> kafkaMetrics = new HashMap<>();
        kafkaMetrics.put("lowercaseOutputName", true);
        kafkaMetrics.put("rules", Collections.singletonList(kafkaRule));

        String metricsCMNameK = "k-metrics-cm";

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        String yaml = mapper.writeValueAsString(kafkaMetrics);
        ConfigMap metricsCMK = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(metricsCMNameK)
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(singletonMap("metrics-config.yml", yaml))
                .build();

        JmxPrometheusExporterMetrics kafkaMetricsConfig = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                            .withName(metricsCMNameK)
                            .withKey("metrics-config.yml")
                            .withOptional(true)
                            .build())
                .endValueFrom()
                .build();

        //Zookeeper
        Map<String, Object> zookeeperLabels = new HashMap<>();
        zookeeperLabels.put("replicaId", "$2");

        Map<String, Object> zookeeperRule = new HashMap<>();
        zookeeperRule.put("labels", zookeeperLabels);
        zookeeperRule.put("name", "zookeeper_$3");
        zookeeperRule.put("pattern", "org.apache.ZooKeeperService<name0=ReplicatedServer_id(\\d+), name1=replica.(\\d+)><>(\\w+)");

        Map<String, Object> zookeeperMetrics = new HashMap<>();
        zookeeperMetrics.put("lowercaseOutputName", true);
        zookeeperMetrics.put("rules", Collections.singletonList(zookeeperRule));

        String metricsCMNameZk = "zk-metrics-cm";
        ConfigMap metricsCMZk = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(metricsCMNameZk)
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(singletonMap("metrics-config.yml", mapper.writeValueAsString(zookeeperMetrics)))
                .build();

        JmxPrometheusExporterMetrics zkMetricsConfig = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                            .withName(metricsCMNameZk)
                            .withKey("metrics-config.yml")
                            .withOptional(true)
                            .build())
                .endValueFrom()
                .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), metricsCMK);
        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), metricsCMZk);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withMetricsConfig(kafkaMetricsConfig)
                .endKafka()
                .editOrNewZookeeper()
                    .withMetricsConfig(zkMetricsConfig)
                .endZookeeper()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());
        Map<String, String> zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getZookeeperSelector());

        resourceManager.createResource(extensionContext, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        String metricsScraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        MetricsCollector kafkaCollector = new MetricsCollector.Builder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withScraperPodName(metricsScraperPodName)
            .withComponentName(testStorage.getClusterName())
            .withComponentType(ComponentType.Kafka)
            .build();

        MetricsCollector zkCollector = kafkaCollector.toBuilder()
            .withComponentType(ComponentType.Zookeeper)
            .build();

        LOGGER.info("Check if metrics are present in Pod of Kafka and ZooKeeper");
        kafkaCollector.collectMetricsFromPods();
        zkCollector.collectMetricsFromPods();

        assertThat(kafkaCollector.getCollectedData().values().toString().contains("kafka_"), is(true));
        assertThat(zkCollector.getCollectedData().values().toString().contains("replicaId"), is(true));

        LOGGER.info("Changing metrics to something else");

        kafkaRule.replace("pattern", "kafka.(\\w+)<type=(.+), name=(.+)><>Count",
                "kafka.(\\w+)<type=(.+), name=(.+)Percent\\w*><>MeanRate");
        kafkaRule.replace("name", "kafka_$1_$2_$3_count", "kafka_$1_$2_$3_percent");
        kafkaRule.replace("type", "COUNTER", "GAUGE");

        zookeeperRule.replace("pattern",
                "org.apache.ZooKeeperService<name0=ReplicatedServer_id(\\d+), name1=replica.(\\d+)><>(\\w+)",
                "org.apache.ZooKeeperService<name0=StandaloneServer_port(\\d+)><>(\\w+)");
        zookeeperRule.replace("name", "zookeeper_$3", "zookeeper_$2");
        zookeeperRule.replace("labels", zookeeperLabels, null);

        metricsCMZk = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(metricsCMNameZk)
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(singletonMap("metrics-config.yml", mapper.writeValueAsString(zookeeperMetrics)))
                .build();

        metricsCMK = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(metricsCMNameK)
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(singletonMap("metrics-config.yml", mapper.writeValueAsString(kafkaMetrics)))
                .build();

        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), metricsCMK);
        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), metricsCMZk);

        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        LOGGER.info("Check if Kafka and ZooKeeper Pods didn't roll");
        assertThat(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getZookeeperSelector()), is(zkPods));
        assertThat(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector()), is(kafkaPods));

        LOGGER.info("Check if Kafka and ZooKeeper metrics are changed");
        ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        String kafkaMetricsConf = kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(metricsCMNameK).get().getData().get("metrics-config.yml");
        String zkMetricsConf = kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(metricsCMNameZk).get().getData().get("metrics-config.yml");
        Object kafkaMetricsJsonToYaml = yamlReader.readValue(kafkaMetricsConf, Object.class);
        Object zkMetricsJsonToYaml = yamlReader.readValue(zkMetricsConf, Object.class);
        ObjectMapper jsonWriter = new ObjectMapper();
        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(testStorage.getClusterName(), 3)) {
            assertThat(kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(cmName).get().getData().get(Constants.METRICS_CONFIG_JSON_NAME),
                    is(jsonWriter.writeValueAsString(kafkaMetricsJsonToYaml)));
        }
        assertThat(kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.zookeeperMetricsAndLogConfigMapName(testStorage.getClusterName())).get().getData().get(Constants.METRICS_CONFIG_JSON_NAME),
                is(jsonWriter.writeValueAsString(zkMetricsJsonToYaml)));

        LOGGER.info("Check if metrics are present in Pod of Kafka and ZooKeeper");

        kafkaCollector.collectMetricsFromPods();
        zkCollector.collectMetricsFromPods();

        assertThat(kafkaCollector.getCollectedData().values().toString().contains("kafka_"), is(true));
        assertThat(zkCollector.getCollectedData().values().toString().contains("replicaId"), is(true));

        LOGGER.info("Removing metrics from Kafka and ZooKeeper and setting them to null");

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setMetricsConfig(null);
            kafka.getSpec().getZookeeper().setMetricsConfig(null);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for Kafka and ZooKeeper Pods to roll");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 3, zkPods);
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);

        LOGGER.info("Check if metrics do not exist in Pods");

        kafkaCollector.collectMetricsFromPodsWithoutWait().values().forEach(value -> assertThat(value, is("")));
        zkCollector.collectMetricsFromPodsWithoutWait().values().forEach(value -> assertThat(value, is("")));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation(extensionContext)
                .createInstallation()
                .runInstallation();
    }
}
