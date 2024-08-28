/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.kraft;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KindIPv6NotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.upgrade.BundleVersionModificationData;
import io.strimzi.systemtest.upgrade.UpgradeKafkaVersion;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Map;

import static io.strimzi.systemtest.Environment.TEST_SUITE_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.KRAFT_UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Class for testing upgrade process of Strimzi with its components when running in KRaft mode
 *      -> KRaft to KRaft upgrades
 * Metadata for the following tests are collected from systemtest/src/test/resources/upgrade/BundleUpgrade.yaml
 */
@Tag(KRAFT_UPGRADE)
public class KRaftStrimziUpgradeST extends AbstractKRaftUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(KRaftStrimziUpgradeST.class);
    private final BundleVersionModificationData acrossUpgradeData = new VersionModificationDataLoader(VersionModificationDataLoader.ModificationType.BUNDLE_UPGRADE).buildDataForUpgradeAcrossVersionsForKRaft();

    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>) ")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlUpgradeDataForKRaft")
    void testUpgradeStrimziVersion(String fromVersion, String toVersion, String fgBefore, String fgAfter, BundleVersionModificationData upgradeData) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(upgradeData.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(upgradeData.getEnvMaxK8sVersion()));

        performUpgrade(CO_NAMESPACE, TEST_SUITE_NAMESPACE, upgradeData);
    }

    @IsolatedTest
    void testUpgradeKafkaWithoutVersion() throws IOException {
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());
        upgradeKafkaVersion.setVersion(null);

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage, upgradeKafkaVersion);

        Map<String, String> controllerSnapshot = PodUtils.podSnapshot(TEST_SUITE_NAMESPACE, controllerSelector);
        Map<String, String> brokerSnapshot = PodUtils.podSnapshot(TEST_SUITE_NAMESPACE, brokerSelector);
        Map<String, String> eoSnapshot = PodUtils.podSnapshot(TEST_SUITE_NAMESPACE, eoSelector);

        // Make snapshots of all Pods
        makeComponentsSnapshots(TEST_SUITE_NAMESPACE);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TEST_SUITE_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData);
        logClusterOperatorPodImage(CO_NAMESPACE);
        logComponentsPodImages(TEST_SUITE_NAMESPACE);

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TEST_SUITE_NAMESPACE, controllerSelector, 3, controllerSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TEST_SUITE_NAMESPACE, brokerSelector, 3, brokerSnapshot);
        DeploymentUtils.waitTillDepHasRolled(TEST_SUITE_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);
        checkAllComponentsImages(TEST_SUITE_NAMESPACE, acrossUpgradeData);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TEST_SUITE_NAMESPACE, clusterName);
        // Verify upgrade
        verifyProcedure(TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);

        String controllerPodName = kubeClient().listPodsByPrefixInName(TEST_SUITE_NAMESPACE, KafkaResource.getStrimziPodSetName(clusterName, CONTROLLER_NODE_NAME)).get(0).getMetadata().getName();
        String brokerPodName = kubeClient().listPodsByPrefixInName(TEST_SUITE_NAMESPACE, KafkaResource.getStrimziPodSetName(clusterName, BROKER_NODE_NAME)).get(0).getMetadata().getName();

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(TEST_SUITE_NAMESPACE, controllerPodName), containsString(acrossUpgradeData.getProcedures().getVersion()));
        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(TEST_SUITE_NAMESPACE, brokerPodName), containsString(acrossUpgradeData.getProcedures().getVersion()));
    }

    @IsolatedTest
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());

        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage, upgradeKafkaVersion);

        // Make snapshots of all Pods
        makeComponentsSnapshots(TEST_SUITE_NAMESPACE);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TEST_SUITE_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData);

        waitForKafkaClusterRollingUpdate(TEST_SUITE_NAMESPACE);

        logPodImages(CO_NAMESPACE);

        // Upgrade kafka
        changeKafkaAndMetadataVersion(TEST_SUITE_NAMESPACE, acrossUpgradeData, true);

        logPodImages(CO_NAMESPACE);

        checkAllComponentsImages(TEST_SUITE_NAMESPACE, acrossUpgradeData);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TEST_SUITE_NAMESPACE, clusterName);

        // Verify upgrade
        verifyProcedure(TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
    }

    @IsolatedTest
    void testUpgradeAcrossVersionsWithNoKafkaVersion() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage, null);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TEST_SUITE_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData);

        // Wait till first upgrade finished
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TEST_SUITE_NAMESPACE, controllerSelector, 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TEST_SUITE_NAMESPACE, brokerSelector, 3, brokerPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(TEST_SUITE_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(CO_NAMESPACE);

        // Upgrade kafka
        changeKafkaAndMetadataVersion(TEST_SUITE_NAMESPACE, acrossUpgradeData);
        logComponentsPodImages(TEST_SUITE_NAMESPACE);
        checkAllComponentsImages(TEST_SUITE_NAMESPACE, acrossUpgradeData);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TEST_SUITE_NAMESPACE, clusterName);

        // Verify upgrade
        verifyProcedure(TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
    }

    @MicroShiftNotSupported("Due to lack of Kafka Connect build feature")
    @KindIPv6NotSupported("Our current CI setup doesn't allow pushing into internal registries that is needed in this test")
    @IsolatedTest
    void testUpgradeOfKafkaConnectAndKafkaConnector(final ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion(acrossUpgradeData.getDefaultKafka());

        doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage, upgradeKafkaVersion);
    }

    private void performUpgrade(String clusterOperatorNamespaceName, String componentsNamespaceName, BundleVersionModificationData upgradeData) throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // leave empty, so the original Kafka version from appropriate Strimzi's yaml will be used
        UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion();

        // Setup env
        setupEnvAndUpgradeClusterOperator(clusterOperatorNamespaceName, componentsNamespaceName, upgradeData, testStorage, upgradeKafkaVersion);

        // Upgrade CO to HEAD
        logClusterOperatorPodImage(clusterOperatorNamespaceName);
        logComponentsPodImages(componentsNamespaceName);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(componentsNamespaceName, eoSelector);

        changeClusterOperator(clusterOperatorNamespaceName, componentsNamespaceName, upgradeData);

        if (TestKafkaVersion.supportedVersionsContainsVersion(upgradeData.getDefaultKafkaVersionPerStrimzi())) {
            waitForKafkaClusterRollingUpdate(componentsNamespaceName);
        }

        logClusterOperatorPodImage(clusterOperatorNamespaceName);
        logComponentsPodImages(componentsNamespaceName);

        // Upgrade kafka
        changeKafkaAndMetadataVersion(componentsNamespaceName, upgradeData, true);
        logComponentsPodImages(componentsNamespaceName);
        checkAllComponentsImages(componentsNamespaceName, upgradeData);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(componentsNamespaceName, clusterName);

        // Verify upgrade
        verifyProcedure(componentsNamespaceName, upgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
    }

    @BeforeEach
    void setupEnvironment() {
        NamespaceManager.getInstance().createNamespaceAndPrepare(CO_NAMESPACE);
    }
}
