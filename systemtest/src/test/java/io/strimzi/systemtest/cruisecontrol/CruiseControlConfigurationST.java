/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@ParallelSuite
public class CruiseControlConfigurationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlConfigurationST.class);

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(CruiseControlConfigurationST.class.getSimpleName()).stream().findFirst().get();

    @ParallelNamespaceTest
    void testCapacityFile(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        double disk = 100_000;
        double cpu = 100;
        String inboundNetwork = "10000KiB/s";
        String outboundNetwork = "10000KiB/s";
        String inboundNetworkOverride0 = "20000KiB/s";
        String inboundNetworkOverride1 = "40000KiB/s";
        String outboundNetworkOverride1 = "400000KiB/s";

        int broker0 = 0;
        int broker1 = 1;
        int broker2 = 2;

        List<Integer> overrideList0 = List.of(broker0, broker1, broker2, broker0);
        List<Integer> overrideList1 = List.of(broker1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3)
            .editOrNewSpec()
                .editCruiseControl()
                    .withNewBrokerCapacity()
                        .withInboundNetwork(inboundNetwork)
                        .addNewOverride()
                            .withBrokers(overrideList0)
                            .withInboundNetwork(inboundNetworkOverride0)
                        .endOverride()
                        .addNewOverride()
                            .withBrokers(overrideList1)
                            .withInboundNetwork(inboundNetworkOverride1)
                            .withOutboundNetwork(outboundNetworkOverride1)
                        .endOverride()
                    .endBrokerCapacity()
                .endCruiseControl()
            .endSpec()
            .build());

        String cruiseControlPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0).getMetadata().getName();

        JsonObject cruiseControlCapacityFileContent =
            new JsonObject(cmdKubeClient(namespaceName).execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + Constants.CRUISE_CONTROL_CAPACITY_FILE_PATH).out());

        LOGGER.info("Verifying cruise control configuration.");
        JsonArray brokerCapacities =  cruiseControlCapacityFileContent.getJsonArray("brokerCapacities");
        assertThat(brokerCapacities, not(nullValue()));

        LOGGER.info("We have 4 entries in broker-capacities");
        assertThat(brokerCapacities.size(), is(4));

        JsonObject defaultBroker = brokerCapacities.getJsonObject(0);
        assertThat(defaultBroker.getString("brokerId"), is("-1"));
        assertThat(defaultBroker.getString("doc"), notNullValue());

        LOGGER.info("Verifying default cruise control capacities");
        JsonObject defaultBrokerCapacity = defaultBroker.getJsonObject("capacity");
        assertThat(Double.valueOf(defaultBrokerCapacity.getString("DISK")), is(disk));
        assertThat(Double.valueOf(defaultBrokerCapacity.getString("CPU")), is(cpu));
        assertThat(Double.valueOf(defaultBrokerCapacity.getString("NW_IN")), is(CruiseControlUtils.removeNetworkCapacityKibSuffix(inboundNetwork)));
        assertThat(Double.valueOf(defaultBrokerCapacity.getString("NW_OUT")), is(CruiseControlUtils.removeNetworkCapacityKibSuffix(outboundNetwork)));

        LOGGER.info("Verifying cruise control capacity overrides");
        JsonObject broker0Capacity = brokerCapacities.getJsonObject(1).getJsonObject("capacity");
        JsonObject broker1Capacity = brokerCapacities.getJsonObject(2).getJsonObject("capacity");
        JsonObject broker2Capacity = brokerCapacities.getJsonObject(3).getJsonObject("capacity");

        assertThat(Double.valueOf(broker0Capacity.getString("NW_IN")), is(CruiseControlUtils.removeNetworkCapacityKibSuffix(inboundNetworkOverride0)));
        assertThat(Double.valueOf(broker0Capacity.getString("NW_OUT")), is(CruiseControlUtils.removeNetworkCapacityKibSuffix(outboundNetwork)));

        assertThat(Double.valueOf(broker1Capacity.getString("NW_IN")), is(CruiseControlUtils.removeNetworkCapacityKibSuffix(inboundNetworkOverride0)));
        assertThat(Double.valueOf(broker1Capacity.getString("NW_OUT")), is(CruiseControlUtils.removeNetworkCapacityKibSuffix(outboundNetwork)));

        assertThat(Double.valueOf(broker2Capacity.getString("NW_IN")), is(CruiseControlUtils.removeNetworkCapacityKibSuffix(inboundNetworkOverride0)));
        assertThat(Double.valueOf(broker2Capacity.getString("NW_OUT")), is(CruiseControlUtils.removeNetworkCapacityKibSuffix(outboundNetwork)));
    }

    @ParallelNamespaceTest
    void testDeployAndUnDeployCruiseControl(ExtensionContext extensionContext) throws IOException {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Removing Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(null);
        }, namespaceName);

        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);

        LOGGER.info("Verifying that in {} is not present in the Kafka cluster", Constants.CRUISE_CONTROL_NAME);
        assertThat(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getCruiseControl(), nullValue());

        LOGGER.info("Verifying that {} pod is not present", clusterName + "-cruise-control-");
        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, clusterName + "-cruise-control-", 0);

        LOGGER.info("Verifying that in Kafka config map there is no configuration to cruise control metric reporter");
        assertThrows(WaitException.class, () -> CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(namespaceName, clusterName)));

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Cruise Control topics will not be deleted and will stay in the Kafka cluster");
            CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Adding Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(new CruiseControlSpec());
        }, namespaceName);

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);

        LOGGER.info("Verifying that in Kafka config map there is configuration to cruise control metric reporter");
        CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(namespaceName, clusterName));

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Verifying that {} topics are created after CC is instantiated.", Constants.CRUISE_CONTROL_NAME);

            CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);
        }
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testConfigurationDiskChangeDoNotTriggersRollingUpdateOfKafkaPods(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Map<String, String> kafkaSnapShot = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> cruiseControlSnapShot = DeploymentUtils.depSnapshot(namespaceName, CruiseControlResources.deploymentName(clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {

            LOGGER.info("Changing the broker capacity of the cruise control");

            CruiseControlSpec cruiseControl = new CruiseControlSpecBuilder()
                .withNewBrokerCapacity()
                    .withOutboundNetwork("20KB/s")
                .endBrokerCapacity()
                .build();

            kafka.getSpec().setCruiseControl(cruiseControl);
        }, namespaceName);

        LOGGER.info("Verifying that CC pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(namespaceName, CruiseControlResources.deploymentName(clusterName), 1, cruiseControlSnapShot);

        LOGGER.info("Verifying that Kafka pods did not roll");
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaSnapShot);

        LOGGER.info("Verifying new configuration in the Kafka CR");

        assertThat(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec()
            .getCruiseControl().getBrokerCapacity().getOutboundNetwork(), is("20KB/s"));

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(namespaceName);
    }

    @ParallelNamespaceTest
    void testConfigurationReflection(ExtensionContext extensionContext) throws IOException {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Pod cruiseControlPod = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0);

        String cruiseControlPodName = cruiseControlPod.getMetadata().getName();

        String configurationFileContent = cmdKubeClient(namespaceName).execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

        InputStream configurationFileStream = new ByteArrayInputStream(configurationFileContent.getBytes(StandardCharsets.UTF_8));

        Properties fileConfiguration = new Properties();
        fileConfiguration.load(configurationFileStream);

        Container cruiseControlContainer = null;

        for (Container container : cruiseControlPod.getSpec().getContainers()) {
            if (container.getName().equals("cruise-control")) {
                cruiseControlContainer = container;
            }
        }

        EnvVar cruiseControlConfiguration = null;

        for (EnvVar envVar : Objects.requireNonNull(cruiseControlContainer).getEnv()) {
            if (envVar.getName().equals(Constants.CRUISE_CONTROL_CONFIGURATION_ENV)) {
                cruiseControlConfiguration = envVar;
            }
        }

        InputStream configurationContainerStream = new ByteArrayInputStream(Objects.requireNonNull(cruiseControlConfiguration).getValue().getBytes(StandardCharsets.UTF_8));

        Properties containerConfiguration = new Properties();
        containerConfiguration.load(configurationContainerStream);

        LOGGER.info("Verifying that all configuration in the cruise control container matching the cruise control file {} properties", Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH);
        List<String> checkCCProperties = Arrays.asList(
                CruiseControlConfigurationParameters.PARTITION_METRICS_WINDOW_NUM_CONFIG_KEY.getValue(),
                CruiseControlConfigurationParameters.PARTITION_METRICS_WINDOW_MS_CONFIG_KEY.getValue(),
                CruiseControlConfigurationParameters.BROKER_METRICS_WINDOW_NUM_CONFIG_KEY.getValue(),
                CruiseControlConfigurationParameters.BROKER_METRICS_WINDOW_MS_CONFIG_KEY.getValue(),
                CruiseControlConfigurationParameters.COMPLETED_USER_TASK_RETENTION_MS_CONFIG_KEY.getValue(),
                "goals", "default.goals");

        for (String propertyName : checkCCProperties) {
            assertThat(containerConfiguration.stringPropertyNames(), hasItem(propertyName));
            assertThat(containerConfiguration.getProperty(propertyName), is(fileConfiguration.getProperty(propertyName)));
        }
    }

    @ParallelNamespaceTest
    void testConfigurationFileIsCreated(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        String cruiseControlPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0).getMetadata().getName();

        String cruiseControlConfigurationFileContent = cmdKubeClient(namespaceName).execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

        assertThat(cruiseControlConfigurationFileContent, not(nullValue()));
    }

    @ParallelNamespaceTest
    void testConfigurationPerformanceOptions(ExtensionContext extensionContext) throws IOException {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        Container cruiseControlContainer;
        EnvVar cruiseControlConfiguration;

        Map<String, String> kafkaSnapShot = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> cruiseControlSnapShot = DeploymentUtils.depSnapshot(namespaceName, CruiseControlResources.deploymentName(clusterName));
        Map<String, Object> performanceTuningOpts = new HashMap<String, Object>() {{
                put(CruiseControlConfigurationParameters.CONCURRENT_INTRA_PARTITION_MOVEMENTS.getValue(), 2);
                put(CruiseControlConfigurationParameters.CONCURRENT_PARTITION_MOVEMENTS.getValue(), 5);
                put(CruiseControlConfigurationParameters.CONCURRENT_LEADER_MOVEMENTS.getValue(), 1000);
                put(CruiseControlConfigurationParameters.REPLICATION_THROTTLE.getValue(), -1);
            }};

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Changing cruise control performance tuning options");
            kafka.getSpec().setCruiseControl(new CruiseControlSpecBuilder()
                    .addToConfig(performanceTuningOpts)
                    .build());
        }, namespaceName);

        LOGGER.info("Verifying that CC pod is rolling, after changing options");
        DeploymentUtils.waitTillDepHasRolled(namespaceName, CruiseControlResources.deploymentName(clusterName), 1, cruiseControlSnapShot);

        LOGGER.info("Verifying that Kafka pods did not roll");
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaSnapShot);

        LOGGER.info("Verifying new configuration in the Kafka CR");
        Pod cruiseControlPod = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-cruise-control-").get(0);

        // Get CruiseControl resource properties
        cruiseControlContainer = cruiseControlPod.getSpec().getContainers().stream()
                .filter(container -> container.getName().equals(Constants.CRUISE_CONTROL_CONTAINER_NAME))
                .findFirst().orElse(null);

        cruiseControlConfiguration = Objects.requireNonNull(cruiseControlContainer).getEnv().stream()
                .filter(envVar -> envVar.getName().equals(Constants.CRUISE_CONTROL_CONFIGURATION_ENV))
                .findFirst().orElse(null);

        InputStream configurationContainerStream = new ByteArrayInputStream(
                Objects.requireNonNull(cruiseControlConfiguration).getValue().getBytes(StandardCharsets.UTF_8));
        Properties containerConfiguration = new Properties();
        containerConfiguration.load(configurationContainerStream);

        LOGGER.info("Verifying Cruise control performance options are set in Kafka CR");
        performanceTuningOpts.forEach((key, value) ->
                assertThat(containerConfiguration, hasEntry(key, value.toString())));
    }
}
