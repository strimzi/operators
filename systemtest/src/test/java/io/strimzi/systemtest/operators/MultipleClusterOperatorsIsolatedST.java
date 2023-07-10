/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.systemtest.utils.specific.MetricsUtils.setupCOMetricsCollectorInNamespace;
import static org.hamcrest.CoreMatchers.is;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
public class MultipleClusterOperatorsIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleClusterOperatorsIsolatedST.class);

    public static final String DEFAULT_NAMESPACE = "multiple-co-cluster-test";
    public static final String FIRST_NAMESPACE = "first-co-namespace";
    public static final String SECOND_NAMESPACE = "second-co-namespace";

    public static final String FIRST_CO_NAME = "first-" + Constants.STRIMZI_DEPLOYMENT_NAME;
    public static final String SECOND_CO_NAME = "second-" + Constants.STRIMZI_DEPLOYMENT_NAME;

    public static final EnvVar FIRST_CO_SELECTOR_ENV = new EnvVar("STRIMZI_CUSTOM_RESOURCE_SELECTOR", "app.kubernetes.io/operator=" + FIRST_CO_NAME, null);
    public static final EnvVar SECOND_CO_SELECTOR_ENV = new EnvVar("STRIMZI_CUSTOM_RESOURCE_SELECTOR", "app.kubernetes.io/operator=" + SECOND_CO_NAME, null);

    public static final EnvVar FIRST_CO_LEASE_NAME_ENV = new EnvVar("STRIMZI_LEADER_ELECTION_LEASE_NAME", FIRST_CO_NAME, null);
    public static final EnvVar SECOND_CO_LEASE_NAME_ENV = new EnvVar("STRIMZI_LEADER_ELECTION_LEASE_NAME", SECOND_CO_NAME, null);

    public static final Map<String, String> FIRST_CO_SELECTOR = Collections.singletonMap("app.kubernetes.io/operator", FIRST_CO_NAME);
    public static final Map<String, String> SECOND_CO_SELECTOR = Collections.singletonMap("app.kubernetes.io/operator", SECOND_CO_NAME);

    /**
     * @description This test case checks how two Cluster Operators operates operands deployed in namespace watched by both of these operators, and transition
     * of operand from one Cluster Operator to another.
     *
     * @steps
     *  1. - Deploy two Cluster Operators - first to the first namespace and second to the second namespace - both watching all namespaces.
     *     - Cluster Operators are successfully deployed.
     *  2. - Set up scrapers and metric collectors for both of Cluster Operators.
     *     - Mentioned resources are successfully set up.
     *  3. - Create and Set namespace 'multiple-co-cluster-test' as default, because all resources from now on will be deployed here.
     *     - Namespace is created and set as default.
     *  4. - Deploy Kafka operand in default namespace without label 'app.kubernetes.io/operator'.
     *     - Kafka is not deployed.
     *  5. - Verify state of metric 'strimzi_resource' for Kafka, which holds number of operated Kafka operands.
     *     - Metric 'strimzi_resource' exposed by Cluster Operators is not present indicating no Kafka resource is being operated.
     *  6. - Modify Kafka custom resource, by changing its label 'app.kubernetes.io/operator' to point to the first Cluster Operator
     *     - Kafka is deployed and operated by the first Cluster Operator.
     *  7. - Deploy Kafka Connect (with label 'app.kubernetes.io/operator' pointing the first Cluster Operator) and  Kafka Connector
     *     - Both operands are successfully deployed, and managed by the first Cluster Operator.
     *  8. - Produce messages into the Kafka Topic and consume with Sink Connector.
     *     - Messages are produced and later handled correctly by the Connector.
     *  9. - Management of Kafka is switched to the second Cluster Operator by modifying value of label 'app.kubernetes.io/operator'.
     *     - Management is modified, later confirmed by the expected change in the value of metric 'strimzi_resource' for kind Kafka on both Cluster Operators.
     *  10. - Verify 'strimzi_resource' of each Cluster Operator for all deployed operands.
     *      - Metric 'strimzi_resource' indicates expected number of operated resources for both Cluster Operators.
     *
     * @usecase
     *  - cluster-operator-metrics
     *  - cluster-operator-watcher
     *  - connect
     *  - kafka
     *  - labels
     *  - metrics
     */
    @IsolatedTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testMultipleCOsInDifferentNamespaces(ExtensionContext extensionContext) {
        // Strimzi is deployed with cluster-wide access in this class STRIMZI_RBAC_SCOPE=NAMESPACE won't work
        assumeFalse(Environment.isNamespaceRbacScope());

        TestStorage testStorage = new TestStorage(extensionContext, DEFAULT_NAMESPACE);

        String firstCOScraperName = FIRST_NAMESPACE + "-" + Constants.SCRAPER_NAME;
        String secondCOScraperName = SECOND_NAMESPACE + "-" + Constants.SCRAPER_NAME;

        LOGGER.info("Deploying Cluster Operators: {}, {} in respective namespaces: {}, {}", FIRST_CO_NAME, SECOND_CO_NAME, FIRST_NAMESPACE, SECOND_NAMESPACE);
        deployCOInNamespace(extensionContext, FIRST_CO_NAME, FIRST_NAMESPACE, Collections.singletonList(FIRST_CO_SELECTOR_ENV), true);
        deployCOInNamespace(extensionContext, SECOND_CO_NAME, SECOND_NAMESPACE, Collections.singletonList(SECOND_CO_SELECTOR_ENV), true);

        LOGGER.info("Deploying scraper Pods: {}, {} for later metrics retrieval", firstCOScraperName, secondCOScraperName);
        resourceManager.createResource(extensionContext, true,
            ScraperTemplates.scraperPod(FIRST_NAMESPACE, firstCOScraperName).build(),
            ScraperTemplates.scraperPod(SECOND_NAMESPACE, secondCOScraperName).build()
        );

        LOGGER.info("Setting up metric collectors targeting Cluster Operators: {}, {}", FIRST_CO_NAME, SECOND_CO_NAME);
        String firstCOScraper = FIRST_NAMESPACE + "-" + Constants.SCRAPER_NAME;
        MetricsCollector firstCoMetricsCollector = setupCOMetricsCollectorInNamespace(FIRST_CO_NAME, FIRST_NAMESPACE, firstCOScraper);
        String secondCOScraper = SECOND_NAMESPACE + "-" + Constants.SCRAPER_NAME;
        MetricsCollector secondCoMetricsCollector = setupCOMetricsCollectorInNamespace(SECOND_CO_NAME, SECOND_NAMESPACE, secondCOScraper);

        LOGGER.info("Deploying Namespace: {} to host all additional operands", testStorage.getNamespaceName());
        cluster.createNamespace(testStorage.getNamespaceName());
        StUtils.copyImagePullSecrets(testStorage.getNamespaceName());

        LOGGER.info("Set cluster namespace to {}, as all operands will be from now on deployd here", testStorage.getNamespaceName());
        cluster.setNamespace(testStorage.getNamespaceName());

        LOGGER.info("Deploying Kafka: {}/{} without CR selector", testStorage.getNamespaceName(), testStorage.getClusterName());
        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3).build());

        // checking that no pods with prefix 'clusterName' will be created in some time
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), testStorage.getClusterName(), 0);

        // verify that metric signalizing managing of kafka is not present in either of cluster operators
        MetricsUtils.assertCoMetricResourcesNullOrZero(firstCoMetricsCollector, Kafka.RESOURCE_KIND, testStorage.getNamespaceName());
        MetricsUtils.assertCoMetricResourcesNullOrZero(secondCoMetricsCollector, Kafka.RESOURCE_KIND, testStorage.getNamespaceName());

        LOGGER.info("Adding {} selector of {} into Kafka: {} CR", FIRST_CO_SELECTOR, FIRST_CO_NAME, testStorage.getClusterName());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getMetadata().setLabels(FIRST_CO_SELECTOR), testStorage.getNamespaceName());
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
                .editOrNewMetadata()
                    .addToLabels(FIRST_CO_SELECTOR)
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build());

        String kafkaConnectPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        LOGGER.info("Deploying KafkaConnector with file sink and CR selector - {} - different than selector in Kafka", SECOND_CO_SELECTOR);
        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", testStorage.getTopicName())
            .endSpec()
            .build());

        KafkaClients basicClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .build();

        resourceManager.createResource(extensionContext, basicClients.producerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), testStorage.getNamespaceName(), MESSAGE_COUNT);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "Hello-world - 99");

        LOGGER.info("Verifying that all operands in Namespace: {} are managed by Cluster Operator: {}", testStorage.getNamespaceName(), FIRST_CO_NAME);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(firstCoMetricsCollector, Kafka.RESOURCE_KIND, 1);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(firstCoMetricsCollector, KafkaConnect.RESOURCE_KIND, 1);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(firstCoMetricsCollector, KafkaConnector.RESOURCE_KIND, 1);

        LOGGER.info("Switch management of Kafka Cluster: {}/{} operand from CO: {} to CO: {}", testStorage.getNamespaceName(), testStorage.getClusterName(), FIRST_CO_NAME, SECOND_CO_NAME);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getMetadata().getLabels().replace("app.kubernetes.io/operator", SECOND_CO_NAME);
        }, testStorage.getNamespaceName());

        LOGGER.info("Verifying that number of managed Kafka resources in increased in CO: {} and decreased om CO: {}", SECOND_CO_NAME, FIRST_CO_NAME);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(secondCoMetricsCollector, Kafka.RESOURCE_KIND, 1);
        MetricsUtils.assertMetricResourcesLowerThanOrEqualTo(firstCoMetricsCollector, Kafka.RESOURCE_KIND, 0);
    }

    /**
     * @description This test case checks how two Cluster Operators deployed in the same namespace operates operands including KafkaRebalance and transition
     * of operand from one Cluster Operator to another.
     *
     * @steps
     *  1. - Deploy 2 Cluster Operators in the same namespace, with additional env variable 'STRIMZI_LEADER_ELECTION_LEASE_NAME'.
     *     - Cluster Operators are successfully deployed.
     *  2. - Set up scrapers and metric collectors for first Cluster Operators.
     *     - Mentioned resources are successfully set up.
     *  3. - Deploy Kafka Cluster with 3 Kafka replicas and label 'app.kubernetes.io/operator' pointing to the first Cluster Operator.
     *     - Kafka is deployed.
     *  4. - Modify Kafka custom resource, by removing its 'app.kubernetes.io/operator' and changing number of replicas to 4
     *     - Kafka is not scaled to 4 replicas.
     *  5. - Deploy Kafka Rebalance without 'app.kubernetes.io/operator' label.
     *     - For a stable period of time, Kafka Rebalance is ignored as well.
     *  6. - Modify Kafka Rebalance labels 'second-strimzi-cluster-operator' to point to Cluster Operator and
     *     - Messages are produced and later handled correctly by the Connector.
     *  7. - Management of Kafka is switched to the second Cluster Operator by modifying value of label 'app.kubernetes.io/operator'.
     *     - Management is modified, and reballance finally takes place.
     *  8. - Verify that Operators operate expected operands.
     *     - Operators operate expected operands.
     *
     * @usecase
     *  - cluster-operator-metrics
     *  - cluster-operator-watcher
     *  - connect
     *  - kafka
     *  - labels
     *  - metrics
     *  - rebalance
     */
    @IsolatedTest
    @Tag(CRUISE_CONTROL)
    @KRaftNotSupported("The scaling of the Kafka Pods is not working properly at the moment")
    void testKafkaCCAndRebalanceWithMultipleCOs(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext, DEFAULT_NAMESPACE);
        LabelSelector kafkaSelector = KafkaResource.getLabelSelector(testStorage.getClusterName(), KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()));

        int scaleTo = 4;

        LOGGER.info("Deploying 2 Cluster Operators: {}, {} in the same namespace: {}", FIRST_CO_NAME, SECOND_CO_NAME, testStorage.getNamespaceName());
        deployCOInNamespace(extensionContext, FIRST_CO_NAME, testStorage.getNamespaceName(), List.of(FIRST_CO_SELECTOR_ENV, FIRST_CO_LEASE_NAME_ENV), false);
        deployCOInNamespace(extensionContext, SECOND_CO_NAME, testStorage.getNamespaceName(), List.of(SECOND_CO_SELECTOR_ENV, SECOND_CO_LEASE_NAME_ENV), false);

        String secondCOScraperName = testStorage.getNamespaceName() + "-" + Constants.SCRAPER_NAME;

        LOGGER.info("Deploying scraper Pod: {}, for later metrics retrieval", secondCOScraperName);
        resourceManager.createResource(extensionContext, true, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), secondCOScraperName).build());

        LOGGER.info("Setting up metric collectors targeting Cluster Operator: {}", SECOND_CO_NAME);
        String coScraperName = testStorage.getNamespaceName() + "-" + Constants.SCRAPER_NAME;
        MetricsCollector secondCoMetricsCollector = setupCOMetricsCollectorInNamespace(SECOND_CO_NAME, testStorage.getNamespaceName(), coScraperName);

        LOGGER.info("Deploying Kafka with {} selector of {}", FIRST_CO_NAME, FIRST_CO_SELECTOR);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3)
            .editOrNewMetadata()
                .addToLabels(FIRST_CO_SELECTOR)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build());

        LOGGER.info("Removing CR selector from Kafka and increasing number of replicas to 4, new Pod should not appear");
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp -> {
                Map<String, String> labels = knp.getMetadata().getLabels();
                labels.put("app.kubernetes.io/operator", "random-operator-value");

                knp.getMetadata().setLabels(labels);
                knp.getSpec().setReplicas(scaleTo);
            }, testStorage.getNamespaceName());
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getMetadata().getLabels().clear();
            kafka.getSpec().getKafka().setReplicas(scaleTo);
        }, testStorage.getNamespaceName());

        // because KafkaRebalance is pointing to Kafka with CC cluster, we need to create KR before adding the label back
        // to test if KR will be ignored
        LOGGER.info("Creating KafkaRebalance when CC doesn't have label for CO, the KR should be ignored");
        resourceManager.createResource(extensionContext, false, KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal",
                    "NetworkInboundCapacityGoal", "MinTopicLeadersPerBrokerGoal",
                    "NetworkOutboundCapacityGoal", "ReplicaCapacityGoal")
                .withSkipHardGoalCheck(true)
                // skip sanity check: because of removal 'RackAwareGoal'
            .endSpec()
            .build());

        KafkaUtils.waitForClusterStability(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Checking if KafkaRebalance is still ignored, after the cluster stability wait");

        // because KR is ignored, it shouldn't contain any status
        assertNull(KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus());

        LOGGER.info("Adding {} selector of {} to Kafka", SECOND_CO_SELECTOR, SECOND_CO_NAME);
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(),
                knp -> knp.getMetadata().getLabels().putAll(SECOND_CO_SELECTOR), testStorage.getNamespaceName());
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getMetadata().setLabels(SECOND_CO_SELECTOR), testStorage.getNamespaceName());

        LOGGER.info("Waiting for Kafka to scales Pods to {}", scaleTo);
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), kafkaSelector, scaleTo);

        assertThat(PodUtils.podSnapshot(testStorage.getNamespaceName(), kafkaSelector).size(), is(scaleTo));

        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Verifying that operands are operated by expected Cluster Operator {}", FIRST_CO_NAME);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(secondCoMetricsCollector, KafkaRebalance.RESOURCE_KIND, 1);
        MetricsUtils.assertMetricResourcesHigherThanOrEqualTo(secondCoMetricsCollector, Kafka.RESOURCE_KIND, 1);
    }

    void deployCOInNamespace(ExtensionContext extensionContext, String coName, String coNamespace, List<EnvVar> extraEnvs, boolean multipleNamespaces) {
        String namespace = multipleNamespaces ? Constants.WATCH_ALL_NAMESPACES : coNamespace;

        if (multipleNamespaces) {
            // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
            List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(coNamespace, coName);
            clusterRoleBindingList.forEach(
                clusterRoleBinding -> ResourceManager.getInstance().createResource(extensionContext, clusterRoleBinding));
        }

        LOGGER.info("Creating: {} in Namespace: {}", coName, coNamespace);

        clusterOperator = clusterOperator.defaultInstallation(extensionContext)
            .withNamespace(coNamespace)
            .withClusterOperatorName(coName)
            .withWatchingNamespaces(namespace)
            .withExtraLabels(Collections.singletonMap("app.kubernetes.io/operator", coName))
            .withExtraEnvVars(extraEnvs)
            .createInstallation()
            .runBundleInstallation();
    }

    @BeforeAll
    void setup() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());
    }
}
