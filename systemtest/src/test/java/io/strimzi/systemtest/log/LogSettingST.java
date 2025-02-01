/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.JvmOptionsBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestConstants.CC_LOG_CONFIG_RELOAD;
import static io.strimzi.systemtest.TestConstants.CO_OPERATION_TIMEOUT_MEDIUM;
import static io.strimzi.systemtest.TestConstants.TIMEOUT_FOR_LOG;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.REGRESSION;
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
@SuiteDoc(
    description = @Desc("This suite verifies logging behavior under various configurations and scenarios."),
    beforeTestSteps = {
        @Step(value = "Deploy the Cluster Operator.", expected = "Cluster Operator is deployed successfully.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA),
        @Label(value = TestDocsLabels.LOGGING)
    }
)
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

    private static final Map<String, String> CONNECT_LOGGERS = new HashMap<>() {
        {
            put("connect.root.logger.level", INFO);
            put("test.connect.logger.level", DEBUG);
            put("log4j.logger.org.reflections", WARN);
        }
    };

    private static final Map<String, String> OPERATORS_LOGGERS = new HashMap<>() {
        {
            put("rootLogger.level", DEBUG);
            put("test.operator.logger.level", DEBUG);
        }
    };

    private static final Map<String, String> MIRROR_MAKER_2_LOGGERS = new HashMap<>() {
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
    @TestDoc(
        description = @Desc("Test verifies Kafka logging configuration (both inline and GC logging) and dynamic changes in Kafka components."),
        steps = {
            @Step(value = "Deploy a Kafka cluster with specified inline logging configurations and GC logging enabled.", expected = "Kafka cluster is deployed successfully with correct log and GC settings."),
            @Step(value = "Check Kafka logging levels in the generated ConfigMaps.", expected = "Logging levels match the expected configuration."),
            @Step(value = "Verify that GC logging is enabled in Kafka and Entity Operator components.", expected = "GC logging is confirmed to be enabled."),
            @Step(value = "Change JVM options to disable GC logging.", expected = "Kafka resources are updated to disable GC logging."),
            @Step(value = "Wait for rolling updates (if any) and verify that GC logging is disabled.", expected = "No unexpected rolling updates occur, and GC logging is now disabled."),
            @Step(value = "Ensure that the changes do not break logging hierarchy or default loggers.", expected = "Logging functions normally and retains specified levels.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testKafkaLogSetting() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        String topicOperatorMap = String.format("%s-%s", LOG_SETTING_CLUSTER_NAME, "entity-topic-operator-config");
        String userOperatorMap = String.format("%s-%s", LOG_SETTING_CLUSTER_NAME, "entity-user-operator-config");

        String eoDepName = KafkaResources.entityOperatorDeploymentName(LOG_SETTING_CLUSTER_NAME);
        String brokerComponentName = StrimziPodSetResource.getBrokerComponentName(LOG_SETTING_CLUSTER_NAME);
        String controllerComponentName = StrimziPodSetResource.getControllerComponentName(LOG_SETTING_CLUSTER_NAME);

        LabelSelector brokerSelector = KafkaResource.getLabelSelector(LOG_SETTING_CLUSTER_NAME, brokerComponentName);
        LabelSelector controllerSelector = KafkaResource.getLabelSelector(LOG_SETTING_CLUSTER_NAME, controllerComponentName);

        Map<String, String> eoPods = DeploymentUtils.depSnapshot(Environment.TEST_SUITE_NAMESPACE, eoDepName);
        Map<String, String> brokerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, brokerSelector);
        Map<String, String> controllerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, controllerSelector);

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), LOG_SETTING_CLUSTER_NAME).build());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername(), LOG_SETTING_CLUSTER_NAME).build());

        LOGGER.info("Checking if Kafka, TO and UO of cluster: {} has log level set properly", LOG_SETTING_CLUSTER_NAME);
        StUtils.getKafkaConfigurationConfigMaps(Environment.TEST_SUITE_NAMESPACE, LOG_SETTING_CLUSTER_NAME)
                .forEach(cmName -> {
                    assertThat("Kafka's log level is set properly", checkLoggersLevel(Environment.TEST_SUITE_NAMESPACE, KAFKA_LOGGERS, cmName), is(true));
                });
        assertThat("Topic Operator's log level is set properly", checkLoggersLevel(Environment.TEST_SUITE_NAMESPACE, OPERATORS_LOGGERS, topicOperatorMap), is(true));
        assertThat("User operator's log level is set properly", checkLoggersLevel(Environment.TEST_SUITE_NAMESPACE, OPERATORS_LOGGERS, userOperatorMap), is(true));

        LOGGER.info("Checking if Kafka, TO and UO of cluster: {} has GC logging enabled in StrimziPodSets/Deployments", LOG_SETTING_CLUSTER_NAME);
        checkGcLoggingPods(Environment.TEST_SUITE_NAMESPACE, brokerSelector, true);
        checkGcLoggingPods(Environment.TEST_SUITE_NAMESPACE, controllerSelector, true);

        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(Environment.TEST_SUITE_NAMESPACE, eoDepName, "topic-operator"), is(true));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(Environment.TEST_SUITE_NAMESPACE, eoDepName, "user-operator"), is(true));

        LOGGER.info("Changing JVM options - setting GC logging to false");
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getBrokerPoolName(LOG_SETTING_CLUSTER_NAME),
            knp -> knp.getSpec().setJvmOptions(JVM_OPTIONS));
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getControllerPoolName(LOG_SETTING_CLUSTER_NAME),
            knp -> knp.getSpec().setJvmOptions(JVM_OPTIONS));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, LOG_SETTING_CLUSTER_NAME, kafka -> {
            kafka.getSpec().getEntityOperator().getTopicOperator().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getEntityOperator().getUserOperator().setJvmOptions(JVM_OPTIONS);
        });

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(Environment.TEST_SUITE_NAMESPACE, controllerSelector, 1, controllerPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(Environment.TEST_SUITE_NAMESPACE, brokerSelector, 3, brokerPods);
        DeploymentUtils.waitTillDepHasRolled(Environment.TEST_SUITE_NAMESPACE, eoDepName, 1, eoPods);

        LOGGER.info("Checking if Kafka, TO and UO of cluster: {} has GC logging disabled in stateful sets/deployments", LOG_SETTING_CLUSTER_NAME);
        checkGcLoggingPods(Environment.TEST_SUITE_NAMESPACE, brokerSelector, false);
        checkGcLoggingPods(Environment.TEST_SUITE_NAMESPACE, controllerSelector, false);

        assertThat("TO GC logging is disabled", checkGcLoggingDeployments(Environment.TEST_SUITE_NAMESPACE, eoDepName, "topic-operator"), is(false));
        assertThat("UO GC logging is disabled", checkGcLoggingDeployments(Environment.TEST_SUITE_NAMESPACE, eoDepName, "user-operator"), is(false));

        LOGGER.info("Checking if Kafka, TO and UO of cluster: {} has GC logging disabled in StrimziPodSets/Deployments", GC_LOGGING_SET_NAME);
        checkGcLoggingPods(Environment.TEST_SUITE_NAMESPACE, brokerSelector, false);
        checkGcLoggingPods(Environment.TEST_SUITE_NAMESPACE, controllerSelector, false);

        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(Environment.TEST_SUITE_NAMESPACE, eoDepName, "topic-operator"), is(false));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(Environment.TEST_SUITE_NAMESPACE, eoDepName, "user-operator"), is(false));

        kubectlGetStrimziUntilOperationIsSuccessful(Environment.TEST_SUITE_NAMESPACE, LOG_SETTING_CLUSTER_NAME);
        kubectlGetStrimziUntilOperationIsSuccessful(Environment.TEST_SUITE_NAMESPACE, GC_LOGGING_SET_NAME);

        checkContainersHaveProcessOneAsTini(Environment.TEST_SUITE_NAMESPACE, LOG_SETTING_CLUSTER_NAME);
        checkContainersHaveProcessOneAsTini(Environment.TEST_SUITE_NAMESPACE, GC_LOGGING_SET_NAME);
    }

    @ParallelTest
    @Tag(CONNECT)
    @TestDoc(
        description = @Desc("Test verifies Kafka Connect logging configuration, dynamic GC logging changes, and inline logging modifications."),
        steps = {
            @Step(value = "Deploy a KafkaConnect instance with specified inline logging and GC logging enabled.", expected = "KafkaConnect is deployed with correct log levels and GC logging."),
            @Step(value = "Verify log settings in the ConfigMap and confirm GC logging is enabled in the Connect pod.", expected = "Log configuration is correct and GC logging is initially enabled."),
            @Step(value = "Update KafkaConnect JVM options to disable GC logging.", expected = "KafkaConnect configuration is updated successfully."),
            @Step(value = "Wait for any rolling updates and confirm GC logging is disabled afterwards.", expected = "KafkaConnect pods reflect disabled GC logging."),
            @Step(value = "Check that the configured log levels remain consistent after the JVM option changes.", expected = "Logging remains functional and at the expected levels.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testConnectLogSetting() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnect(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), LOG_SETTING_CLUSTER_NAME, 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        final String connectDepName = KafkaConnectResources.componentName(testStorage.getClusterName());
        final String connectMap = KafkaConnectResources.configMapName(testStorage.getClusterName());
        final Map<String, String> connectPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaConnectSelector());

        LOGGER.info("Checking if Connect has log level set properly");
        assertThat("KafkaConnect's log level is set properly", checkLoggersLevel(Environment.TEST_SUITE_NAMESPACE, CONNECT_LOGGERS, connectMap), is(true));
        this.checkGcLogging(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaConnectSelector(), true);

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), kc -> kc.getSpec().setJvmOptions(JVM_OPTIONS));
        StUtils.waitTillStrimziPodSetOrDeploymentRolled(Environment.TEST_SUITE_NAMESPACE, connectDepName, 1, connectPods, testStorage.getKafkaConnectSelector());
        this.checkGcLogging(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaConnectSelector(), false);

        kubectlGetStrimziUntilOperationIsSuccessful(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        checkContainersHaveProcessOneAsTini(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @TestDoc(
        description = @Desc("Test verifies Kafka MirrorMaker2 logging configuration, inline logging updates, and toggling of GC logging."),
        steps = {
            @Step(value = "Deploy Kafka MirrorMaker2 with inline logging set and GC logging enabled.", expected = "MirrorMaker2 is deployed with desired log configuration and GC logging."),
            @Step(value = "Verify that logs in MirrorMaker2 match the configured levels and GC logging status.", expected = "Log checks confirm correct levels and GC logging enabled."),
            @Step(value = "Update MirrorMaker2 JVM options to disable GC logging.", expected = "Configuration is updated and applied to MirrorMaker2."),
            @Step(value = "Wait for any rolling updates and check that GC logging is disabled now.", expected = "MirrorMaker2 pods reflect disabled GC logging."),
            @Step(value = "Confirm that the log levels remain correct and that no unexpected changes occurred.", expected = "MirrorMaker2 runs with correct logging hierarchy and no issues.")
        },
        labels = {
            @Label(value = TestDocsLabels.LOGGING),
            @Label(value = TestDocsLabels.MIRROR_MAKER_2)
        }
    )
    @ParallelTest
    @Tag(MIRROR_MAKER2)
    void testMirrorMaker2LogSetting() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), GC_LOGGING_SET_NAME, LOG_SETTING_CLUSTER_NAME, 1, false)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_2_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        final String mm2DepName = KafkaMirrorMaker2Resources.componentName(testStorage.getClusterName());
        final String mirrorMakerMap = KafkaMirrorMaker2Resources.configMapName(testStorage.getClusterName());
        final Map<String, String> mm2Pods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getMM2Selector());

        LOGGER.info("Checking if MirrorMaker2 has log level set properly");
        assertThat("KafkaMirrorMaker2's log level is set properly", checkLoggersLevel(Environment.TEST_SUITE_NAMESPACE, MIRROR_MAKER_2_LOGGERS, mirrorMakerMap), is(true));
        this.checkGcLoggingPods(Environment.TEST_SUITE_NAMESPACE, testStorage.getMM2Selector(), true);
        this.checkGcLogging(Environment.TEST_SUITE_NAMESPACE, testStorage.getMM2Selector(), true);

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), mm2 -> mm2.getSpec().setJvmOptions(JVM_OPTIONS));
        StUtils.waitTillStrimziPodSetOrDeploymentRolled(Environment.TEST_SUITE_NAMESPACE, mm2DepName, 1, mm2Pods, testStorage.getMM2Selector());

        this.checkGcLogging(Environment.TEST_SUITE_NAMESPACE, testStorage.getMM2Selector(), false);

        kubectlGetStrimziUntilOperationIsSuccessful(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        checkContainersHaveProcessOneAsTini(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @ParallelTest
    @Tag(BRIDGE)
    @TestDoc(
        description = @Desc("Test verifies Kafka Bridge logging configuration including inline logging changes and enabling/disabling GC logging."),
        steps = {
            @Step(value = "Deploy Kafka Bridge with custom inline logging and GC logging enabled.", expected = "Bridge is deployed with the desired log settings and GC logging on."),
            @Step(value = "Check the associated ConfigMap to ensure logging levels match expectations.", expected = "Log levels are applied correctly in the Bridge."),
            @Step(value = "Update JVM options to disable GC logging for the Bridge.", expected = "Bridge configuration is updated to turn off GC logging."),
            @Step(value = "Confirm that pods, after any rolling updates, now run without GC logging.", expected = "Bridge runs with GC logging disabled and correct log levels."),
            @Step(value = "Ensure the logging hierarchy and inline logging adjustments persist and function correctly.", expected = "Bridge continues to operate with the desired logging setup.")
        },
        labels = {
            @Label(value = TestDocsLabels.BRIDGE),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testBridgeLogSetting() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(LOG_SETTING_CLUSTER_NAME), 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(BRIDGE_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        final String bridgeDepName = KafkaBridgeResources.componentName(testStorage.getClusterName());
        final Map<String, String> bridgePods = DeploymentUtils.depSnapshot(Environment.TEST_SUITE_NAMESPACE, bridgeDepName);
        final String bridgeMap = KafkaBridgeResources.configMapName(testStorage.getClusterName());
        final LabelSelector labelSelector = KafkaBridgeResource.getLabelSelector(bridgeDepName, KafkaMirrorMaker2Resources.componentName(bridgeDepName));

        LOGGER.info("Checking if Bridge has log level set properly");
        assertThat("Bridge's log level is set properly", checkLoggersLevel(Environment.TEST_SUITE_NAMESPACE, BRIDGE_LOGGERS, bridgeMap), is(true));

        this.checkGcLogging(Environment.TEST_SUITE_NAMESPACE, labelSelector, true);

        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), bridge -> bridge.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(Environment.TEST_SUITE_NAMESPACE, bridgeDepName, 1, bridgePods);

        this.checkGcLogging(Environment.TEST_SUITE_NAMESPACE, labelSelector, false);

        kubectlGetStrimziUntilOperationIsSuccessful(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        checkContainersHaveProcessOneAsTini(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @IsolatedTest("Updating shared Kafka")
    // This test might be flaky, as it gets real logs from CruiseControl pod
    @TestDoc(
        description = @Desc("Test verifies that changing the Cruise Control logging configuration dynamically applies without requiring a full restart."),
        steps = {
            @Step(value = "Deploy Kafka with Cruise Control enabled and default logging settings.", expected = "Kafka (with Cruise Control) is deployed and running normally."),
            @Step(value = "Check the Cruise Control log level and confirm it's at the default (INFO) level.", expected = "Cruise Control logs confirm the current default logging level."),
            @Step(value = "Change the Cruise Control logging level to DEBUG via inline logging updates.", expected = "Logging configuration changes are applied to Cruise Control dynamically."),
            @Step(value = "Wait for Cruise Control to detect and apply the new logging configuration.", expected = "Cruise Control logs now show DEBUG level messages."),
            @Step(value = "Verify no unnecessary restarts occur and Cruise Control continues to function properly.", expected = "Cruise Control remains stable and reflects the new DEBUG logging level.")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
            @Label(value = TestDocsLabels.LOGGING),
        }
    )
    void testCruiseControlLogChange() {
        final String debugText = " DEBUG ";
        String cruiseControlPodName = PodUtils.getPodNameByPrefix(Environment.TEST_SUITE_NAMESPACE, LOG_SETTING_CLUSTER_NAME + "-" + TestConstants.CRUISE_CONTROL_CONTAINER_NAME);
        LOGGER.info("Check that default/actual root logging level is info");
        String containerLogLevel = cmdKubeClient().namespace(Environment.TEST_SUITE_NAMESPACE).execInPod(cruiseControlPodName, "grep", "-i", "rootlogger.level",
                TestConstants.CRUISE_CONTROL_LOG_FILE_PATH).out().trim().split("=")[1];
        assertThat(containerLogLevel.toUpperCase(Locale.ENGLISH), is(not(debugText.strip())));

        LOGGER.info("Checking logs in CruiseControl - make sure no DEBUG is found there");
        String logOut = StUtils.getLogFromPodByTime(Environment.TEST_SUITE_NAMESPACE, cruiseControlPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME, "20s");
        assertThat(logOut.toUpperCase(Locale.ENGLISH), not(containsString(debugText)));

        InlineLogging logging = new InlineLogging();
        logging.setLoggers(Collections.singletonMap("rootLogger.level", debugText.strip()));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, LOG_SETTING_CLUSTER_NAME, kafka -> kafka.getSpec().getCruiseControl().setLogging(logging));

        LOGGER.info("Waiting for change of root logger in {}", cruiseControlPodName);
        TestUtils.waitFor(" for log to be changed", CC_LOG_CONFIG_RELOAD, CO_OPERATION_TIMEOUT_MEDIUM, () -> {
            String line = StUtils.getLineFromPodContainer(Environment.TEST_SUITE_NAMESPACE, cruiseControlPodName, null, TestConstants.CRUISE_CONTROL_LOG_FILE_PATH, "rootlogger.level");
            return line.toUpperCase(Locale.ENGLISH).contains(debugText.strip());
        });

        LOGGER.info("Check CruiseControl logs in Pod: {}/{} and it's container {}", Environment.TEST_SUITE_NAMESPACE, cruiseControlPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME);
        TestUtils.waitFor("debug log line to be present in logs", CC_LOG_CONFIG_RELOAD, TIMEOUT_FOR_LOG, () -> {
            String log = StUtils.getLogFromPodByTime(Environment.TEST_SUITE_NAMESPACE, cruiseControlPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME, "20s");
            return log.toUpperCase(Locale.ENGLISH).contains(debugText);
        });
    }

    // only one thread can access (eliminate data-race)
    private synchronized void kubectlGetStrimziUntilOperationIsSuccessful(String namespaceName, String resourceName) {
        TestUtils.waitFor("Checking if kubectl get strimzi contains:" + resourceName, Duration.ofSeconds(10).toMillis(),
            TestConstants.GLOBAL_TIMEOUT, () -> cmdKubeClient().namespace(namespaceName).execInCurrentNamespace("get", "strimzi").out().contains(resourceName));
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
        // tries to get a log4j2 configuration file first (operator, bridge, ...) otherwise log4j one (kafka, ...)
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

    private synchronized void checkGcLogging(final String namespaceName, final LabelSelector selector, boolean exceptedValue) {
        this.checkGcLoggingPods(namespaceName, selector, exceptedValue);
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
    void setup() {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getBrokerPoolName(LOG_SETTING_CLUSTER_NAME), LOG_SETTING_CLUSTER_NAME, 3)
                .editSpec()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getControllerPoolName(LOG_SETTING_CLUSTER_NAME), LOG_SETTING_CLUSTER_NAME, 1)
                .editSpec()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getBrokerPoolName(GC_LOGGING_SET_NAME), GC_LOGGING_SET_NAME, 1)
                .editSpec()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getControllerPoolName(GC_LOGGING_SET_NAME), GC_LOGGING_SET_NAME, 1)
                .editSpec()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endSpec()
                .build()
        );

        Kafka logSettingKafka = KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, LOG_SETTING_CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withNewInlineLogging()
                        .withLoggers(KAFKA_LOGGERS)
                    .endInlineLogging()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endKafka()
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
            .build();

//         deploying second Kafka here because of MM and MM2 tests
        Kafka gcLoggingKafka = KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, GC_LOGGING_SET_NAME, 1)
            .editSpec()
                .editKafka()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endKafka()
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
            .build();

        resourceManager.createResourceWithoutWait(logSettingKafka, gcLoggingKafka);

        // sync point wait for all resources
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, LOG_SETTING_CLUSTER_NAME);
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, GC_LOGGING_SET_NAME);
    }
}
