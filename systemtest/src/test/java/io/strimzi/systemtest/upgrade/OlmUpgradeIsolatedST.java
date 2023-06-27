/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.operator.configuration.OlmConfiguration;
import io.strimzi.systemtest.resources.operator.configuration.OlmConfigurationBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader.ModificationType;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import static io.strimzi.systemtest.Constants.OLM_UPGRADE;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * This test class contains tests for Strimzi downgrade from version X to version X - 1.
 * The difference between this class and {@link StrimziUpgradeIsolatedST} is in cluster operator install type.
 * Tests in this class use OLM for install cluster operator.
 */
@Tag(OLM_UPGRADE)
@KRaftNotSupported("Strimzi and Kafka downgrade is not supported with KRaft mode")
public class OlmUpgradeIsolatedST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(OlmUpgradeIsolatedST.class);
    private final OlmVersionModificationData olmUpgradeData = new VersionModificationDataLoader(ModificationType.OLM_UPGRADE).getOlmUpgradeData();
    @Test
    void testStrimziUpgrade(ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String toVersion = olmUpgradeData.getToVersion();
        final String fromVersion = olmUpgradeData.getFromVersion();

        LOGGER.info("====================================================================================");
        LOGGER.info("---------------- Updating Cluster Operator version " + fromVersion + "=>" + toVersion + " -----------------");
        LOGGER.info("====================================================================================");
        LOGGER.info("-------------------------- Upgrade data used in this test --------------------------");
        LOGGER.info(olmUpgradeData.toString());
        LOGGER.info("====================================================================================");

        // Install operator via Olm
        //  1. Create subscription with manual approval and operator group if needed
        //  2. Approve installation (get install-plan and patch it)
        clusterOperator.runManualOlmInstallation(olmUpgradeData.getFromOlmChannelName(), fromVersion);

        // In this test we intend to setup Kafka once at the beginning and then upgrade it with CO
        File dir = FileUtils.downloadAndUnzip(olmUpgradeData.getFromUrl());
        File kafkaYaml = new File(dir, olmUpgradeData.getFromExamples() + "/examples/kafka/kafka-persistent.yaml");

        LOGGER.info("Deploying Kafka from file: {}", kafkaYaml.getPath());
        KubeClusterResource.cmdKubeClient().create(kafkaYaml);
        waitForReadinessOfKafkaCluster();

        // Create KafkaTopic
        final String topicUpgradeName = "topic-upgrade";
        HashMap<String, Object> topicConfig = new HashMap<String, Object>();
        topicConfig.put("min.insync.replicas", 2);

        KafkaTopic kafkaUpgradeTopic = new YAMLMapper().readValue(new File(dir, olmUpgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml"), KafkaTopic.class);
        kafkaUpgradeTopic = new KafkaTopicBuilder(kafkaUpgradeTopic)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
                .withName(topicUpgradeName)
            .endMetadata()
            .editSpec()
                .withReplicas(3)
                .addToConfig(topicConfig)
            .endSpec()
            .build();

        resourceManager.createResource(extensionContext, kafkaUpgradeTopic);

        KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicUpgradeName)
            .withMessageCount(testStorage.getMessageCount())
            .withDelayMs(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerStrimzi(), kafkaBasicClientJob.consumerStrimzi());

        String clusterOperatorDeploymentName = ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        LOGGER.info("Old deployment name of Cluster Operator is {}", clusterOperatorDeploymentName);

        // ======== Cluster Operator upgrade starts ========
        makeSnapshots();

        OlmConfiguration upgradeOlmConfig = new OlmConfigurationBuilder(clusterOperator.getOlmResource().getOlmConfiguration())
            .withChannelName("stable")
            .withOperatorVersion(toVersion)
            .build();

        // Cluster Operator upgrade
        clusterOperator.upgradeClusterOperator(upgradeOlmConfig);

        clusterOperatorDeploymentName = ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        LOGGER.info("New deployment name of Cluster Operator is {}", clusterOperatorDeploymentName);
        ResourceManager.setCoDeploymentName(clusterOperatorDeploymentName);

        // Verification that Cluster Operator has been upgraded to a correct version
        String afterUpgradeVersionOfCo = kubeClient().getCsvWithPrefix(clusterOperator.getDeploymentNamespace(), upgradeOlmConfig.getOlmAppBundlePrefix()).getSpec().getVersion();
        assertThat(afterUpgradeVersionOfCo, is(toVersion));

        // Wait for Rolling Update to finish
        zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3, zkPods);
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
        // ======== Cluster Operator upgrade ends ========

        // ======== Kafka upgrade starts ========
        logPodImages(clusterName);
        changeKafkaAndLogFormatVersion(olmUpgradeData, extensionContext);
        logPodImages(clusterName);
        // ======== Kafka upgrade ends ========

        // Wait for messages of previously created clients
        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), clusterOperator.getDeploymentNamespace(), testStorage.getMessageCount());

        // Check for errors in Cluster Operator log
        assertNoCoErrorsLogged(clusterOperator.getDeploymentNamespace(), 0);
    }

    @BeforeAll
    void setup(final ExtensionContext extensionContext) {
        clusterOperator = clusterOperator.defaultInstallation(extensionContext)
            .withNamespace(INFRA_NAMESPACE)
            .withBindingsNamespaces(Collections.singletonList(INFRA_NAMESPACE))
            .withWatchingNamespaces(INFRA_NAMESPACE)
            .createInstallation();
    }
}
