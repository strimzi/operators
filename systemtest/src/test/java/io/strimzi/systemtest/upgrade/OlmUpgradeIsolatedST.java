/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.OlmUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static io.strimzi.systemtest.Constants.OLM_UPGRADE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;


/**
 * This test class contains tests for Strimzi downgrade from version X to version X - 1.
 * The difference between this class and {@link StrimziUpgradeIsolatedST} is in cluster operator install type.
 * Tests in this class use OLM for install cluster operator.
 */
@Tag(OLM_UPGRADE)
@IsolatedSuite
public class OlmUpgradeIsolatedST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(OlmUpgradeIsolatedST.class);

    private final String producerName = "producer";
    private final String consumerName = "consumer";
    private final String topicUpgradeName = "topic-upgrade";
    // clusterName has to be same as cluster name in examples
    private final String clusterName = "my-cluster";
    private final int messageUpgradeCount =  600;
    private KafkaClients kafkaBasicClientJob;
    private KafkaTopic kafkaUpgradeTopic;

    @Test
    void testStrimziUpgrade(ExtensionContext extensionContext) throws IOException {
        JsonArray upgradeData = readUpgradeJson(UPGRADE_JSON_FILE);
        JsonObject latestUpgradeData = upgradeData.getJsonObject(upgradeData.size() - 1);

        List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.getSupportedKafkaVersions();
        TestKafkaVersion testKafkaVersion = testKafkaVersions.get(testKafkaVersions.size() - 1);

        // Generate procedures and data for OLM upgrade
        // This is needed, because for upgrade it's generated by method which generated data for parametrized test
        JsonObject procedures = new JsonObject();
        procedures.put("kafkaVersion", testKafkaVersion.version());
        procedures.put("logMessageVersion", testKafkaVersion.messageVersion());
        procedures.put("interBrokerProtocolVersion", testKafkaVersion.protocolVersion());
        latestUpgradeData.put("proceduresAfterOperatorUpgrade", procedures);
        latestUpgradeData.put("toVersion", Environment.OLM_OPERATOR_LATEST_RELEASE_VERSION);
        latestUpgradeData.put("toExamples", "HEAD");
        latestUpgradeData.put("urlTo", "HEAD");

        // perform verification of to version
        performUpgradeVerification(latestUpgradeData, extensionContext);
    }

    private void performUpgradeVerification(JsonObject testParameters, ExtensionContext extensionContext) throws IOException {
        LOGGER.info("Upgrade data: {}", testParameters.toString());
        final String fromVersion = testParameters.getString("fromVersion");
        final String toVersion = testParameters.getString("toVersion");
        LOGGER.info("====================================================================================");
        LOGGER.info("============== Verification version of CO: " + fromVersion + " => " + toVersion);
        LOGGER.info("====================================================================================");

        // 1. Create subscription (+ operator group) with manual approval strategy
        // 2. Approve installation
        //   a) get name of install-plan
        //   b) approve installation
        clusterOperator.runManualOlmInstallation(fromVersion, "strimzi-0.27.x");

        String url = testParameters.getString("urlFrom");
        File dir = FileUtils.downloadAndUnzip(url);

        // In chainUpgrade we want to setup Kafka only at the begging and then upgrade it via CO
        kafkaYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/kafka/kafka-persistent.yaml");
        LOGGER.info("Deploy Kafka from: {}", kafkaYaml.getPath());
        KubeClusterResource.cmdKubeClient().create(kafkaYaml);
        // Wait for readiness
        waitForReadinessOfKafkaCluster();

        clusterOperator.getOlmResource().getClosedMapInstallPlan().put(clusterOperator.getOlmResource().getNonUsedInstallPlan(), Boolean.TRUE);

        this.kafkaUpgradeTopic = new YAMLMapper().readValue(new File(dir, testParameters.getString("fromExamples") + "/examples/topic/kafka-topic.yaml"), KafkaTopic.class);
        this.kafkaUpgradeTopic.getMetadata().setName(topicUpgradeName);
        this.kafkaUpgradeTopic.getSpec().setReplicas(3);
        this.kafkaUpgradeTopic.getSpec().setAdditionalProperty("min.insync.replicas", 2);

        LOGGER.info("Deploy KafkaTopic: {}", this.kafkaUpgradeTopic.toString());

        cmdKubeClient().applyContent(this.kafkaUpgradeTopic.toString());
        ResourceManager.waitForResourceReadiness(KafkaTopic.RESOURCE_PLURAL + "." +
            io.strimzi.api.kafka.model.Constants.V1BETA2 + "." + io.strimzi.api.kafka.model.Constants.RESOURCE_GROUP_NAME, topicUpgradeName);

        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerStrimzi());
        resourceManager.createResource(extensionContext, kafkaBasicClientJob.consumerStrimzi());

        String clusterOperatorDeploymentName = ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        LOGGER.info("Old deployment name of cluster operator is {}", clusterOperatorDeploymentName);

        // ======== Cluster Operator upgrade starts ========
        makeSnapshots();
        // wait until non-used install plan is present (sometimes install-plan did not append immediately and we need to wait for at least 10m)
        clusterOperator.getOlmResource().updateSubscription("stable", toVersion,
            Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL, OlmInstallationStrategy.Manual);
        // wait until non-used install plan is present (sometimes install-plan did not append immediately and we need to wait for at least 10m)
        OlmUtils.waitUntilNonUsedInstallPlanIsPresent(toVersion);

        // Cluster Operator
        clusterOperator.getOlmResource().upgradeClusterOperator();

        // wait until RU is finished
        zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(zkSelector, 3, zkPods);
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(kafkaSelector, 3, kafkaPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
        // ======== Cluster Operator upgrade ends ========

        clusterOperatorDeploymentName = ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        LOGGER.info("New deployment name of cluster operator is {}", clusterOperatorDeploymentName);
        ResourceManager.setCoDeploymentName(clusterOperatorDeploymentName);

        // verification that cluster operator has correct version (install-plan) - strimzi-cluster-operator.v[version]
        String afterUpgradeVersionOfCo = clusterOperator.getOlmResource().getClusterOperatorVersion();

        // if HEAD -> 6.6.6 version
        assertThat(afterUpgradeVersionOfCo, is(Environment.OLM_APP_BUNDLE_PREFIX + ".v" + toVersion));

        // ======== Kafka upgrade starts ========
        logPodImages(clusterName);
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresAfterOperatorUpgrade"), testParameters, extensionContext);
        logPodImages(clusterName);
        // ======== Kafka upgrade ends ========

        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), messageUpgradeCount);
        ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), messageUpgradeCount);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @BeforeAll
    void setup() {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator.defaultInstallation()
            .withNamespace(Constants.INFRA_NAMESPACE)
            .withBindingsNamespaces(Collections.singletonList(Constants.INFRA_NAMESPACE))
            .withWatchingNamespaces(Constants.INFRA_NAMESPACE)
            .createInstallation();

        this.kafkaBasicClientJob = new KafkaClientsBuilder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicUpgradeName)
            .withMessageCount(messageUpgradeCount)
            .withDelayMs(1000)
            .build();
    }
}
