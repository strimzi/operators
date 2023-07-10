/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.JvmOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CC_LOG_CONFIG_RELOAD;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CO_OPERATION_TIMEOUT_MEDIUM;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TIMEOUT_FOR_LOG;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@TestMethodOrder(OrderAnnotation.class)
class LogSettingST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(LogSettingST.class);

    private static final String INFO = "INFO";
    private static final String ERROR = "ERROR";
    private static final String WARN = "WARN";
    private static final String TRACE = "TRACE";
    private static final String DEBUG = "DEBUG";
    private static final String FATAL = "FATAL";
    private static final String OFF = "OFF";

    private static final String LOG_SETTING_CLUSTER_NAME = "log-setting-cluster-name";
    private static final String GC_LOGGING_SET_NAME = "gc-set-logging";

    private static final JvmOptions JVM_OPTIONS = new JvmOptionsBuilder()
        .withGcLoggingEnabled(false)
        .build();

    private static final Map<String, String> KAFKA_LOGGERS = new HashMap<>() {
        {
            put("kafka.root.logger.level", INFO);
            put("test.kafka.logger.level", INFO);
            put("log4j.logger.org.I0Itec.zkclient.ZkClient", ERROR);
            put("log4j.logger.org.apache.zookeeper", WARN);
            put("log4j.logger.kafka", TRACE);
            put("log4j.logger.org.apache.kafka", DEBUG);
            put("log4j.logger.kafka.request.logger", FATAL);
            put("log4j.logger.kafka.network.Processor", OFF);

            put("log4j.logger.kafka.server.KafkaApis", INFO);
            put("log4j.logger.kafka.network.RequestChannel$", ERROR);
            put("log4j.logger.kafka.controller", WARN);
            put("log4j.logger.kafka.log.LogCleaner", TRACE);
            put("log4j.logger.state.change.logger", DEBUG);
            put("log4j.logger.kafka.authorizer.logger", FATAL);
        }
    };

    private static final Map<String, String> ZOOKEEPER_LOGGERS = new HashMap<>() {
        {
            put("zookeeper.root.logger", OFF);
            put("test.zookeeper.logger.level", DEBUG);
        }
    };

    private static final Map<String, String> CONNECT_LOGGERS = new HashMap<>() {
        {
            put("connect.root.logger.level", INFO);
            put("test.connect.logger.level", DEBUG);
            put("log4j.logger.org.I0Itec.zkclient", ERROR);
            put("log4j.logger.org.reflections", WARN);
        }
    };

    private static final Map<String, String> OPERATORS_LOGGERS = new HashMap<>() {
        {
            put("rootLogger.level", DEBUG);
            put("test.operator.logger.level", DEBUG);
        }
    };

    private static final Map<String, String> MIRROR_MAKER_LOGGERS = new HashMap<>() {
        {
            put("mirrormaker.root.logger", TRACE);
            put("test.mirrormaker.logger.level", TRACE);
        }
    };

    private static final Map<String, String> BRIDGE_LOGGERS = new HashMap<>() {
        {
            put("logger.createConsumer.name", "http.openapi.operation.createConsumer");
            put("logger.createConsumer.level", INFO);
            put("logger.deleteConsumer.name", "http.openapi.operation.deleteConsumer");
            put("logger.deleteConsumer.level", DEBUG);
            put("logger.subscribe.name", "http.openapi.operation.subscribe");
            put("logger.subscribe.level", TRACE);
            put("logger.unsubscribe.name", "http.openapi.operation.unsubscribe");
            put("logger.unsubscribe.level", DEBUG);
            put("logger.poll.name", "http.openapi.operation.poll");
            put("logger.poll.level", INFO);
            put("logger.assign.name", "http.openapi.operation.assign");
            put("logger.assign.level", TRACE);
            put("logger.commit.name", "http.openapi.operation.commit");
            put("logger.commit.level", DEBUG);
            put("logger.send.name", "http.openapi.operation.send");
            put("logger.send.level", ERROR);
            put("logger.sendToPartition.name", "http.openapi.operation.sendToPartition");
            put("logger.sendToPartition.level", TRACE);
            put("logger.seekToBeginning.name", "http.openapi.operation.seekToBeginning");
            put("logger.seekToBeginning.level", DEBUG);
            put("logger.seekToEnd.name", "http.openapi.operation.seekToEnd");
            put("logger.seekToEnd.level", WARN);
            put("logger.seek.name", "http.openapi.operation.seek");
            put("logger.seek.level", INFO);
            put("logger.healthy.name", "http.openapi.operation.healthy");
            put("logger.healthy.level", ERROR);
            put("logger.ready.name", "http.openapi.operation.ready");
            put("logger.ready.level", WARN);
            put("logger.openapi.name", "http.openapi.operation.openapi");
            put("logger.openapi.level", TRACE);
            put("test.logger.bridge.level", ERROR);
        }
    };

    @IsolatedTest("Using shared Kafka")
    void testKafkaLogSetting(ExtensionContext extensionContext) {
        String kafkaMap = KafkaResources.kafkaMetricsAndLogConfigMapName(LOG_SETTING_CLUSTER_NAME);
        String zookeeperMap = KafkaResources.zookeeperMetricsAndLogConfigMapName(LOG_SETTING_CLUSTER_NAME);
        String topicOperatorMap = String.format("%s-%s", LOG_SETTING_CLUSTER_NAME, "entity-topic-operator-config");
        String userOperatorMap = String.format("%s-%s", LOG_SETTING_CLUSTER_NAME, "entity-user-operator-config");

        String eoDepName = KafkaResources.entityOperatorDeploymentName(LOG_SETTING_CLUSTER_NAME);
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(LOG_SETTING_CLUSTER_NAME);
        String zkSsName = KafkaResources.zookeeperStatefulSetName(LOG_SETTING_CLUSTER_NAME);

        LabelSelector kafkaSelector = KafkaResource.getLabelSelector(LOG_SETTING_CLUSTER_NAME, kafkaSsName);
        LabelSelector zkSelector = KafkaResource.getLabelSelector(LOG_SETTING_CLUSTER_NAME, zkSsName);

        Map<String, String> eoPods = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), eoDepName);
        Map<String, String> kafkaPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), kafkaSelector);
        Map<String, String> zkPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zkSelector);

        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(LOG_SETTING_CLUSTER_NAME, topicName, clusterOperator.getDeploymentNamespace()).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterOperator.getDeploymentNamespace(), LOG_SETTING_CLUSTER_NAME, userName).build());

        LOGGER.info("Checking if Kafka, ZooKeeper, TO and UO of cluster: {} has log level set properly", LOG_SETTING_CLUSTER_NAME);
        StUtils.getKafkaConfigurationConfigMaps(LOG_SETTING_CLUSTER_NAME, 3)
                .forEach(cmName -> {
                    assertThat("Kafka's log level is set properly", checkLoggersLevel(clusterOperator.getDeploymentNamespace(), KAFKA_LOGGERS, cmName), is(true));
                });
        if (!Environment.isKRaftModeEnabled()) {
            assertThat("ZooKeeper's log level is set properly", checkLoggersLevel(clusterOperator.getDeploymentNamespace(), ZOOKEEPER_LOGGERS, zookeeperMap), is(true));
            assertThat("Topic Operator's log level is set properly", checkLoggersLevel(clusterOperator.getDeploymentNamespace(), OPERATORS_LOGGERS, topicOperatorMap), is(true));
        }
        assertThat("User operator's log level is set properly", checkLoggersLevel(clusterOperator.getDeploymentNamespace(), OPERATORS_LOGGERS, userOperatorMap), is(true));

        LOGGER.info("Checking if Kafka, ZooKeeper, TO and UO of cluster: {} has GC logging enabled in stateful sets/deployments", LOG_SETTING_CLUSTER_NAME);
        checkGcLoggingPods(clusterOperator.getDeploymentNamespace(), kafkaSelector, true);
        if (!Environment.isKRaftModeEnabled()) {
            checkGcLoggingPods(clusterOperator.getDeploymentNamespace(), zkSelector, true);
            assertThat("TO GC logging is enabled", checkGcLoggingDeployments(clusterOperator.getDeploymentNamespace(), eoDepName, "topic-operator"), is(true));
        }
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(clusterOperator.getDeploymentNamespace(), eoDepName, "user-operator"), is(true));

        LOGGER.info("Changing JVM options - setting GC logging to false");
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(LOG_SETTING_CLUSTER_NAME), knp ->
                knp.getSpec().setJvmOptions(JVM_OPTIONS), clusterOperator.getDeploymentNamespace());
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(LOG_SETTING_CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getZookeeper().setJvmOptions(JVM_OPTIONS);
            if (!Environment.isKRaftModeEnabled()) {
                kafka.getSpec().getEntityOperator().getTopicOperator().setJvmOptions(JVM_OPTIONS);
            }
            kafka.getSpec().getEntityOperator().getUserOperator().setJvmOptions(JVM_OPTIONS);
        }, clusterOperator.getDeploymentNamespace());

        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 1, zkPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), eoDepName, 1, eoPods);

        LOGGER.info("Checking if Kafka, ZooKeeper, TO and UO of cluster: {} has GC logging disabled in stateful sets/deployments", LOG_SETTING_CLUSTER_NAME);
        checkGcLoggingPods(clusterOperator.getDeploymentNamespace(), kafkaSelector, false);
        if (!Environment.isKRaftModeEnabled()) {
            checkGcLoggingPods(clusterOperator.getDeploymentNamespace(), zkSelector, false);
            assertThat("TO GC logging is disabled", checkGcLoggingDeployments(clusterOperator.getDeploymentNamespace(), eoDepName, "topic-operator"), is(false));
        }
        assertThat("UO GC logging is disabled", checkGcLoggingDeployments(clusterOperator.getDeploymentNamespace(), eoDepName, "user-operator"), is(false));

        LOGGER.info("Checking if Kafka, ZooKeeper, TO and UO of cluster: {} has GC logging disabled in stateful sets/deployments", GC_LOGGING_SET_NAME);
        checkGcLoggingPods(clusterOperator.getDeploymentNamespace(), kafkaSelector, false);
        if (!Environment.isKRaftModeEnabled()) {
            checkGcLoggingPods(clusterOperator.getDeploymentNamespace(), zkSelector, false);
            assertThat("TO GC logging is enabled", checkGcLoggingDeployments(clusterOperator.getDeploymentNamespace(), eoDepName, "topic-operator"), is(false));
        }
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(clusterOperator.getDeploymentNamespace(), eoDepName, "user-operator"), is(false));

        kubectlGetStrimziUntilOperationIsSuccessful(clusterOperator.getDeploymentNamespace(), LOG_SETTING_CLUSTER_NAME);
        kubectlGetStrimziUntilOperationIsSuccessful(clusterOperator.getDeploymentNamespace(), GC_LOGGING_SET_NAME);

        checkContainersHaveProcessOneAsTini(clusterOperator.getDeploymentNamespace(), LOG_SETTING_CLUSTER_NAME);
        checkContainersHaveProcessOneAsTini(clusterOperator.getDeploymentNamespace(), GC_LOGGING_SET_NAME);
    }

    @ParallelTest
    @Tag(CONNECT)
    void testConnectLogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectClusterName = clusterName + "-connect";

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(connectClusterName, clusterOperator.getDeploymentNamespace(), LOG_SETTING_CLUSTER_NAME, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        final String connectDepName = KafkaConnectResources.deploymentName(connectClusterName);
        final LabelSelector labelSelector = KafkaConnectResource.getLabelSelector(connectClusterName, KafkaConnectResources.deploymentName(connectClusterName));
        final String connectMap = KafkaConnectResources.metricsAndLogConfigMapName(connectClusterName);
        final Map<String, String> connectPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), labelSelector);

        LOGGER.info("Checking if Connect has log level set properly");
        assertThat("KafkaConnect's log level is set properly", checkLoggersLevel(clusterOperator.getDeploymentNamespace(), CONNECT_LOGGERS, connectMap), is(true));
        this.checkGcLogging(clusterOperator.getDeploymentNamespace(), labelSelector, connectDepName, true);

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(connectClusterName, kc -> kc.getSpec().setJvmOptions(JVM_OPTIONS), clusterOperator.getDeploymentNamespace());
        StUtils.waitTillStrimziPodSetOrDeploymentRolled(clusterOperator.getDeploymentNamespace(), connectDepName, 1, connectPods, labelSelector);
        this.checkGcLogging(clusterOperator.getDeploymentNamespace(), labelSelector, connectDepName, false);

        kubectlGetStrimziUntilOperationIsSuccessful(clusterOperator.getDeploymentNamespace(), connectClusterName);
        checkContainersHaveProcessOneAsTini(clusterOperator.getDeploymentNamespace(), connectClusterName);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER)
    void testMirrorMakerLogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMakerName = clusterName + "-mirror-maker";

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(mirrorMakerName, LOG_SETTING_CLUSTER_NAME, GC_LOGGING_SET_NAME, "my-group", 1, false)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String mmDepName = KafkaMirrorMakerResources.deploymentName(mirrorMakerName);
        Map<String, String> mmPods = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), mmDepName);
        String mirrorMakerMap = KafkaMirrorMakerResources.metricsAndLogConfigMapName(mirrorMakerName);

        LOGGER.info("Checking if MirrorMaker has log level set properly");
        assertThat("KafkaMirrorMaker's log level is set properly", checkLoggersLevel(clusterOperator.getDeploymentNamespace(), MIRROR_MAKER_LOGGERS, mirrorMakerMap), is(true));
        checkGcLoggingDeployments(clusterOperator.getDeploymentNamespace(), mmDepName, true);

        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(mirrorMakerName, mm -> mm.getSpec().setJvmOptions(JVM_OPTIONS), clusterOperator.getDeploymentNamespace());
        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), mmDepName, 1, mmPods);
        checkGcLoggingDeployments(clusterOperator.getDeploymentNamespace(), mmDepName, false);

        kubectlGetStrimziUntilOperationIsSuccessful(clusterOperator.getDeploymentNamespace(), mirrorMakerName);
        checkContainersHaveProcessOneAsTini(clusterOperator.getDeploymentNamespace(), mirrorMakerName);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    void testMirrorMaker2LogSetting(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, LOG_SETTING_CLUSTER_NAME, GC_LOGGING_SET_NAME, 1, false)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        final String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(clusterName);
        final String mirrorMakerMap = KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(clusterName);
        final LabelSelector labelSelector = KafkaMirrorMaker2Resource.getLabelSelector(clusterName, KafkaMirrorMaker2Resources.deploymentName(clusterName));
        final Map<String, String> mm2Pods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), labelSelector);

        LOGGER.info("Checking if MirrorMaker2 has log level set properly");
        assertThat("KafkaMirrorMaker2's log level is set properly", checkLoggersLevel(clusterOperator.getDeploymentNamespace(), MIRROR_MAKER_LOGGERS, mirrorMakerMap), is(true));
        this.checkGcLoggingPods(clusterOperator.getDeploymentNamespace(), labelSelector, true);
        this.checkGcLogging(clusterOperator.getDeploymentNamespace(), labelSelector, mm2DepName, true);

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(clusterName, mm2 -> mm2.getSpec().setJvmOptions(JVM_OPTIONS), clusterOperator.getDeploymentNamespace());
        StUtils.waitTillStrimziPodSetOrDeploymentRolled(clusterOperator.getDeploymentNamespace(), mm2DepName, 1, mm2Pods, labelSelector);

        this.checkGcLogging(clusterOperator.getDeploymentNamespace(), labelSelector, mm2DepName,  false);

        kubectlGetStrimziUntilOperationIsSuccessful(clusterOperator.getDeploymentNamespace(), clusterName);
        checkContainersHaveProcessOneAsTini(clusterOperator.getDeploymentNamespace(), clusterName);
    }

    @ParallelTest
    @Tag(BRIDGE)
    void testBridgeLogSetting(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String bridgeName = clusterName + "-bridge";

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, LOG_SETTING_CLUSTER_NAME, KafkaResources.plainBootstrapAddress(LOG_SETTING_CLUSTER_NAME), 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(BRIDGE_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        final String bridgeDepName = KafkaBridgeResources.deploymentName(bridgeName);
        final Map<String, String> bridgePods = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), bridgeDepName);
        final String bridgeMap = KafkaBridgeResources.metricsAndLogConfigMapName(bridgeName);
        final LabelSelector labelSelector = KafkaBridgeResource.getLabelSelector(bridgeDepName, KafkaMirrorMaker2Resources.deploymentName(bridgeDepName));

        LOGGER.info("Checking if Bridge has log level set properly");
        assertThat("Bridge's log level is set properly", checkLoggersLevel(clusterOperator.getDeploymentNamespace(), BRIDGE_LOGGERS, bridgeMap), is(true));

        this.checkGcLogging(clusterOperator.getDeploymentNamespace(), labelSelector, bridgeDepName, true);

        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName, bridge -> bridge.getSpec().setJvmOptions(JVM_OPTIONS), clusterOperator.getDeploymentNamespace());
        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), bridgeDepName, 1, bridgePods);

        this.checkGcLogging(clusterOperator.getDeploymentNamespace(), labelSelector, bridgeDepName, false);

        kubectlGetStrimziUntilOperationIsSuccessful(clusterOperator.getDeploymentNamespace(), bridgeName);
        checkContainersHaveProcessOneAsTini(clusterOperator.getDeploymentNamespace(), bridgeName);
    }

    @IsolatedTest("Updating shared Kafka")
    // This test might be flaky, as it gets real logs from CruiseControl pod
    void testCruiseControlLogChange(ExtensionContext extensionContext) {
        final String debugText = " DEBUG ";
        String cruiseControlPodName = PodUtils.getPodNameByPrefix(clusterOperator.getDeploymentNamespace(), LOG_SETTING_CLUSTER_NAME + "-" + Constants.CRUISE_CONTROL_CONTAINER_NAME);
        LOGGER.info("Check that default/actual root logging level is info");
        String containerLogLevel = cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(cruiseControlPodName, "grep", "-i", "rootlogger.level",
                Constants.CRUISE_CONTROL_LOG_FILE_PATH).out().trim().split("=")[1];
        assertThat(containerLogLevel.toUpperCase(Locale.ENGLISH), is(not(debugText.strip())));

        LOGGER.info("Checking logs in CruiseControl - make sure no DEBUG is found there");
        String logOut = StUtils.getLogFromPodByTime(clusterOperator.getDeploymentNamespace(), cruiseControlPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME, "20s");
        assertThat(logOut.toUpperCase(Locale.ENGLISH), not(containsString(debugText)));

        InlineLogging logging = new InlineLogging();
        logging.setLoggers(Collections.singletonMap("rootLogger.level", debugText.strip()));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(LOG_SETTING_CLUSTER_NAME, kafka -> kafka.getSpec().getCruiseControl().setLogging(logging), clusterOperator.getDeploymentNamespace());

        LOGGER.info("Waiting for change of root logger in {}", cruiseControlPodName);
        TestUtils.waitFor(" for log to be changed", CC_LOG_CONFIG_RELOAD, CO_OPERATION_TIMEOUT_MEDIUM, () -> {
            String line = StUtils.getLineFromPodContainer(clusterOperator.getDeploymentNamespace(), cruiseControlPodName, null, Constants.CRUISE_CONTROL_LOG_FILE_PATH, "rootlogger.level");
            return line.toUpperCase(Locale.ENGLISH).contains(debugText.strip());
        });

        LOGGER.info("Check CruiseControl logs in Pod: {}/{} and it's container {}", clusterOperator.getDeploymentNamespace(), cruiseControlPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME);
        TestUtils.waitFor("debug log line to be present in logs", CC_LOG_CONFIG_RELOAD, TIMEOUT_FOR_LOG, () -> {
            String log = StUtils.getLogFromPodByTime(clusterOperator.getDeploymentNamespace(), cruiseControlPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME, "20s");
            return log.toUpperCase(Locale.ENGLISH).contains(debugText);
        });
    }

    // only one thread can access (eliminate data-race)
    private synchronized void kubectlGetStrimziUntilOperationIsSuccessful(String namespaceName, String resourceName) {
        TestUtils.waitFor("Checking if kubectl get strimzi contains:" + resourceName, Duration.ofSeconds(10).toMillis(),
            Constants.GLOBAL_TIMEOUT, () -> cmdKubeClient().namespace(namespaceName).execInCurrentNamespace("get", "strimzi").out().contains(resourceName));
    }

    // only one thread can access (eliminate data-race)
    private synchronized void checkContainersHaveProcessOneAsTini(String namespaceName, String resourceClusterName) {
        //Used [/] in the grep command so that grep process does not return itself
        String command = "cat /proc/1/cmdline";

        for (Pod pod : kubeClient(namespaceName).listPods(Labels.STRIMZI_CLUSTER_LABEL, resourceClusterName)) {
            String podName = pod.getMetadata().getName();
            if (!podName.contains("build") && !podName.contains("deploy") && !podName.contains("kafka-clients")) {
                for (Container container : pod.getSpec().getContainers()) {
                    String containerName = container.getName();

                    PodUtils.waitForPodContainerReady(namespaceName, podName, containerName);
                    LOGGER.info("Checking tini process for Pod: {}/{} with container {}", namespaceName, podName, containerName);
                    String processOne = cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.DEBUG, podName, containerName, "/bin/bash", "-c", command).out().trim();
                    assertThat(processOne, startsWith("/usr/bin/tini"));
                }
            }
        }
    }

    private synchronized String configMap(String namespaceName, String configMapName) {
        Map<String, String> configMapData = kubeClient(namespaceName).getConfigMap(configMapName).getData();
        // tries to get a log4j2 configuration file first (operator, bridge, ...) otherwise log4j one (kafka, zookeeper, ...)
        String configMapKey = configMapData.keySet()
                .stream()
                .filter(key -> key.equals("log4j2.properties") || key.equals("log4j.properties"))
                .findAny()
                .orElseThrow();
        return configMapData.get(configMapKey);
    }

    private synchronized boolean checkLoggersLevel(String namespaceName, Map<String, String> loggers, String configMapName) {
        boolean result = false;
        String configMap = configMap(namespaceName, configMapName);
        for (Map.Entry<String, String> entry : loggers.entrySet()) {
            LOGGER.info("Check log level setting for logger: {} Expected: {}", entry.getKey(), entry.getValue());
            String loggerConfig = String.format("%s=%s", entry.getKey(), entry.getValue());
            result = configMap.contains(loggerConfig);

            // Validation failed
            if (!result) {
                break;
            }
        }

        return result;
    }

    private synchronized Boolean checkGcLoggingDeployments(String namespaceName, String deploymentName, String containerName) {
        LOGGER.info("Checking deployment: {}", deploymentName);
        List<Container> containers = kubeClient(namespaceName).getDeployment(namespaceName, deploymentName).getSpec().getTemplate().getSpec().getContainers();
        Container container = getContainerByName(containerName, containers);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }
    private synchronized void checkGcLogging(final String namespaceName, final LabelSelector selector,
                                                final String deploymentName, boolean exceptedValue) {
        if (Environment.isStableConnectIdentitiesEnabled()) {
            this.checkGcLoggingPods(namespaceName, selector, exceptedValue);
        } else {
            this.checkGcLoggingDeployments(namespaceName, deploymentName, exceptedValue);
        }
    }
    private synchronized void checkGcLoggingDeployments(String namespaceName, String deploymentName, boolean expectedValue) {
        LOGGER.info("Checking deployment: {}", deploymentName);
        Container container = kubeClient(namespaceName).getDeployment(namespaceName, deploymentName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());

        assertThat(checkEnvVarValue(container), is(expectedValue));
    }

    private synchronized void checkGcLoggingPods(String namespaceName, LabelSelector selector, boolean expectedValue) {
        LOGGER.info("Checking Pods with selector: {}", selector);
        List<Pod> pods = kubeClient(namespaceName).getClient().pods().inNamespace(namespaceName).withLabelSelector(selector).list().getItems();

        for (Pod pod : pods)    {
            LOGGER.info("Checking Pod: {}/{}, container: {}", namespaceName, pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getName());
            assertThat("Kafka GC logging in Pod: "  + pod.getMetadata().getName() + " has wrong value", checkEnvVarValue(pod.getSpec().getContainers().get(0)), is(expectedValue));
        }
    }

    private synchronized Container getContainerByName(String containerName, List<Container> containers) {
        return containers.stream().filter(c -> c.getName().equals(containerName)).findFirst().orElse(null);
    }

    private synchronized Boolean checkEnvVarValue(Container container) {
        assertThat("Container is null!", container, is(notNullValue()));

        List<EnvVar> loggingEnvVar = container.getEnv().stream().filter(envVar -> envVar.getName().contains("GC_LOG_ENABLED")).collect(Collectors.toList());
        LOGGER.info("{}={}", loggingEnvVar.get(0).getName(), loggingEnvVar.get(0).getValue());
        return loggingEnvVar.get(0).getValue().contains("true");
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaPersistent(LOG_SETTING_CLUSTER_NAME, 3, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewInlineLogging()
                        .withLoggers(KAFKA_LOGGERS)
                    .endInlineLogging()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withNewInlineLogging()
                        .withLoggers(ZOOKEEPER_LOGGERS)
                    .endInlineLogging()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endZookeeper()
                .editEntityOperator()
                    .editOrNewUserOperator()
                        .withNewInlineLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLogging()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                    .endUserOperator()
                    .editOrNewTopicOperator()
                        .withNewInlineLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLogging()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                    .endTopicOperator()
                .endEntityOperator()
                .withNewCruiseControl()
                .endCruiseControl()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build());

//         deploying second Kafka here because of MM and MM2 tests
        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaPersistent(GC_LOGGING_SET_NAME, 1, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endZookeeper()
                .editEntityOperator()
                    .editTopicOperator()
                        .withNewJvmOptions()
                        .endJvmOptions()
                    .endTopicOperator()
                    .editUserOperator()
                        .withNewJvmOptions()
                        .endJvmOptions()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        // sync point wait for all resources
        KafkaUtils.waitForKafkaReady(clusterOperator.getDeploymentNamespace(), LOG_SETTING_CLUSTER_NAME);
        KafkaUtils.waitForKafkaReady(clusterOperator.getDeploymentNamespace(), GC_LOGGING_SET_NAME);
    }
}
