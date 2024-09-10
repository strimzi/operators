/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceConfigurationBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceModeBrokers;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.strimzi.api.ResourceAnnotations.ANNO_STRIMZI_IO_REBALANCE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAutoRebalancingMockTest {
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private static final MockCertManager CERT_MANAGER = new MockCertManager();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(10, "a", "a");

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;
    private String namespace;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private KafkaAssemblyOperator operator;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaNodePoolCrd()
                .withKafkaConnectCrd()
                .withKafkaMirrorMaker2Crd()
                .withKafkaRebalanceCrd()
                .withStrimziPodSetCrd()
                .withPodController()
                .withDeploymentController()
                .withServiceController()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");

        Kafka cluster = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withAnnotations(Map.of(
                            Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled",
                            Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"
                    ))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withConfig(new HashMap<>())
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                    .endKafka()
                    .withNewCruiseControl()
                        .withAutoRebalance(
                                new KafkaAutoRebalanceConfigurationBuilder()
                                        .withMode(KafkaRebalanceMode.ADD_BROKERS)
                                        .withNewTemplate("my-add-remove-brokers-rebalancing-template")
                                        .build(),
                                new KafkaAutoRebalanceConfigurationBuilder()
                                        .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                                        .withNewTemplate("my-add-remove-brokers-rebalancing-template")
                                        .build())
                    .endCruiseControl()
                .endSpec()
                .build();

        KafkaNodePool controllers = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("controllers")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("brokers")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(5)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();

        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(controllers).create();
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(brokers).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(cluster).create();

        supplier = new ResourceOperatorSupplier(vertx, client, null, ResourceUtils.adminClientProvider(), null,
                ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), null, PFA, 2_000, mock(BrokersInUseCheck.class));

        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        operator = new KafkaAssemblyOperator(vertx, PFA, CERT_MANAGER, PASSWORD_GENERATOR, supplier, config);
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
        sharedWorkerExecutor.close();
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @Test
    public void testAutoRebalancingScaleDown(VertxTestContext context) {
        // getting the mocked BrokersInUseCheck class to mock broker scale down operation for check failure
        BrokersInUseCheck operations = supplier.brokersInUseCheck;
        when(operations.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(3, 4)));

        KafkaRebalance kafkaRebalanceTemplate = buildKafkaRebalanceTemplate("my-add-remove-brokers-rebalancing-template", List.of("CpuCapacityGoal"));
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalanceTemplate).create();

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling down the brokers
                    scaleKafkaCluster(3);
                })))
                // 2nd reconcile, getting the scaling down
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleDown, KafkaRebalanceMode.REMOVE_BROKERS, List.of(3, 4));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertKafkaRebalanceStatus(kr, KafkaRebalanceMode.REMOVE_BROKERS, List.of(3, 4), List.of("CpuCapacityGoal"));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to New state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.New);
                })))
                // 3rd reconcile, handling auto-rebalancing with KafkaRebalance in New state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Rebalancing state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Rebalancing);
                })))
                // 4th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Ready state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Ready);
                })))
                // 5th reconcile, handling auto-rebalancing with KafkaRebalance in Ready state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    assertThat(k.getStatus().getAutoRebalance().getModes(), is(nullValue()));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertThat(kr, is(nullValue()));

                    reconciliation.flag();
                })));
    }

    @Test
    public void testAutoRebalancingScaleUp(VertxTestContext context) {
        KafkaRebalance kafkaRebalanceTemplate = buildKafkaRebalanceTemplate("my-add-remove-brokers-rebalancing-template", List.of("CpuCapacityGoal"));
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalanceTemplate).create();

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling up the brokers
                    scaleKafkaCluster(7);
                })))
                // 2nd reconcile, getting the scaling up
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleUp, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertKafkaRebalanceStatus(kr, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9), List.of("CpuCapacityGoal"));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to New state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.New);
                })))
                // 3rd reconcile, handling auto-rebalancing with KafkaRebalance in New state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Rebalancing state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Rebalancing);
                })))
                // 4th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Ready state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Ready);
                })))
                // 5th reconcile, handling auto-rebalancing with KafkaRebalance in Ready state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    assertThat(k.getStatus().getAutoRebalance().getModes(), is(nullValue()));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertThat(kr, is(nullValue()));

                    reconciliation.flag();
                })));
    }

    @Test
    public void testAutoRebalancingDoubleScaleUp(VertxTestContext context) {
        KafkaRebalance kafkaRebalanceTemplate = buildKafkaRebalanceTemplate("my-add-remove-brokers-rebalancing-template", List.of("CpuCapacityGoal"));
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalanceTemplate).create();

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling up the brokers
                    scaleKafkaCluster(7);
                })))
                // 2nd reconcile, getting the scaling up
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleUp, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertKafkaRebalanceStatus(kr, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9), List.of("CpuCapacityGoal"));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to New state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.New);
                })))
                // 3rd reconcile, handling auto-rebalancing with KafkaRebalance in New state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Rebalancing state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Rebalancing);
                })))
                // 4th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));

                    // scaling up the brokers again (while there is an auto-rebalancing on stand up already running)
                    scaleKafkaCluster(9);
                })))
                // 5th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state and a new request of scaling up
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleUp, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9, 10, 11));

                    // check KafkaRebalance was updated with newly added brokers and refreshed
                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertKafkaRebalanceStatus(kr, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9, 10, 11), List.of("CpuCapacityGoal"));
                    assertThat(kr.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_REBALANCE), is("refresh"));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Rebalancing state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Rebalancing);
                })))
                // 6th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Ready state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Ready);
                })))
                // 7th reconcile, handling auto-rebalancing with KafkaRebalance in Ready state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    assertThat(k.getStatus().getAutoRebalance().getModes(), is(nullValue()));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertThat(kr, is(nullValue()));

                    reconciliation.flag();
                })));
    }

    @Test
    public void testAutoRebalancingDoubleScaleDown(VertxTestContext context) {
        // getting the mocked BrokersInUseCheck class to mock broker scale down operation for check failure
        BrokersInUseCheck operations = supplier.brokersInUseCheck;
        when(operations.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(4)));

        KafkaRebalance kafkaRebalanceTemplate = buildKafkaRebalanceTemplate("my-add-remove-brokers-rebalancing-template", List.of("CpuCapacityGoal"));
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalanceTemplate).create();

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling down the brokers
                    scaleKafkaCluster(4);
                })))
                // 2nd reconcile, getting the scaling down
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleDown, KafkaRebalanceMode.REMOVE_BROKERS, List.of(4));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertKafkaRebalanceStatus(kr, KafkaRebalanceMode.REMOVE_BROKERS, List.of(4), List.of("CpuCapacityGoal"));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to New state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.New);
                })))
                // 3rd reconcile, handling auto-rebalancing with KafkaRebalance in New state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Rebalancing state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Rebalancing);
                })))
                // 4th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));

                    // scaling down the brokers again (while there is an auto-rebalancing on scale down already running)
                    scaleKafkaCluster(3);
                })))
                // 5th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state and a new request of scaling down
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleDown, KafkaRebalanceMode.REMOVE_BROKERS, List.of(3, 4));

                    // check KafkaRebalance was updated with newly removed brokers and refreshed
                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertKafkaRebalanceStatus(kr, KafkaRebalanceMode.REMOVE_BROKERS, List.of(3, 4), List.of("CpuCapacityGoal"));
                    assertThat(kr.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_REBALANCE), is("refresh"));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Rebalancing state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Rebalancing);
                })))
                // 6th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Ready state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Ready);
                })))
                // 7th reconcile, handling auto-rebalancing with KafkaRebalance in Ready state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    assertThat(k.getStatus().getAutoRebalance().getModes(), is(nullValue()));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertThat(kr, is(nullValue()));

                    reconciliation.flag();
                })));
    }

    @Test
    public void testAutoRebalancingScaleUpScaleDown(VertxTestContext context) {
        // getting the mocked BrokersInUseCheck class to mock broker scale down operation for check failure
        BrokersInUseCheck operations = supplier.brokersInUseCheck;
        when(operations.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(8, 9)));

        KafkaRebalance kafkaRebalanceTemplate = buildKafkaRebalanceTemplate("my-add-remove-brokers-rebalancing-template", List.of("CpuCapacityGoal"));
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalanceTemplate).create();

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling up the brokers
                    scaleKafkaCluster(7);
                })))
                // 2nd reconcile, getting the scaling up
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleUp, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertKafkaRebalanceStatus(kr, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9), List.of("CpuCapacityGoal"));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to New state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.New);
                })))
                // 3rd reconcile, handling auto-rebalancing with KafkaRebalance in New state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Rebalancing state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Rebalancing);
                })))
                // 4th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));

                    // scaling down the brokers (while there is an auto-rebalancing on scale up already running)
                    scaleKafkaCluster(5);
                })))
                // 5th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state and a new request of scaling down
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    // if nodes blocked on scale down are the same of newly added ones, the auto-rebalancing on scale up is not queued, because
                    // the added nodes won't exist anymore after the scale down is complete
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleDown, KafkaRebalanceMode.REMOVE_BROKERS, List.of(8, 9));
                    assertThat(isAutoRebalanceModeBrokers(k, KafkaRebalanceMode.ADD_BROKERS), is(false));

                    // check KafkaRebalance about auto-rebalancing on scale up was deleted (rebalancing was stopped)
                    KafkaRebalance krAddBrokers = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertThat(krAddBrokers, is(nullValue()));

                    // a KafkaRebalance for running prioritize auto-rebalancing on scale down was created
                    KafkaRebalance krRemoveBrokers = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertKafkaRebalanceStatus(krRemoveBrokers, KafkaRebalanceMode.REMOVE_BROKERS, List.of(8, 9), List.of("CpuCapacityGoal"));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to New state
                    patchKafkaRebalanceState(krRemoveBrokers, KafkaRebalanceState.New);
                })))
                // 6th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Ready state
                    patchKafkaRebalanceState(kr, KafkaRebalanceState.Ready);
                })))
                // 7th reconcile, handling auto-rebalancing with KafkaRebalance in Ready state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    assertThat(k.getStatus().getAutoRebalance().getModes(), is(nullValue()));

                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertThat(kr, is(nullValue()));

                    reconciliation.flag();
                })));
    }

    @Test
    public void testNoAutoRebalancingIdleOnClusterCreation(VertxTestContext context) {
        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // just checking that on Kafka cluster creation with no scaling, the auto-rebalancing is just in Idle state
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    reconciliation.flag();
                })));
    }

    @Test
    public void testNoAutoRebalancingIdleNoScaling(VertxTestContext context) {
        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // just checking that on Kafka cluster creation with no scaling, the auto-rebalancing is just in Idle state
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                })))
                // 2nd reconcile, no scaling down/up triggered
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // just checking that without any scaling, the auto-rebalancing just stays in Idle state
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    reconciliation.flag();
                })));
    }

    @Test
    public void testNoAutoRebalancingWithoutCruiseControl(VertxTestContext context) {
        // remove the Cruise Control definition, to test the auto-rebalancing reconciler behaviour
        Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).edit(
                k -> new KafkaBuilder(k)
                        .editSpec()
                            .withCruiseControl(null)
                        .endSpec()
                        .build()
        );

        Checkpoint reconciliation = context.checkpoint();
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // just checking that on Kafka cluster creation with no Cruise Control, the auto-rebalancing doesn't run
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance(), is(nullValue()));
                    reconciliation.flag();
                })));
    }

    @Test
    public void testAutoRebalancingMissingKafkaRebalanceTemplate(VertxTestContext context) {
        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling up the brokers
                    scaleKafkaCluster(7);
                })))
                // 2nd reconcile, getting the scaling up
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    assertThat(k.getStatus().getAutoRebalance().getModes(), is(nullValue()));

                    reconciliation.flag();
                })));
    }

    @Test
    public void testAutoRebalancingWithoutSpecifiedTemplate(VertxTestContext context) {
        // edit the Kafka cluster without specifying the KafkaRebalance template
        // so auto-rebalancing will happen by using default Cruise Control configuration
        Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).edit(
                k -> new KafkaBuilder(k)
                        .editSpec()
                            .editCruiseControl()
                                .withAutoRebalance(
                                        new KafkaAutoRebalanceConfigurationBuilder()
                                                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                                                .build(),
                                        new KafkaAutoRebalanceConfigurationBuilder()
                                                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                                                .build()
                                )
                            .endCruiseControl()
                        .endSpec()
                        .build()
        );

        Checkpoint reconciliation = context.checkpoint();

        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling up the brokers
                    scaleKafkaCluster(7);
                })))
                // 2nd reconcile, getting the scaling up
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertKafkaAutoRebalanceStatus(k, KafkaAutoRebalanceState.RebalanceOnScaleUp, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9));

                    // KafkaRebalance was created with right mode and brokers but leaving goals empty (then rebalancing uses the Cruise Control defaults)
                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertKafkaRebalanceStatus(kr, KafkaRebalanceMode.ADD_BROKERS, List.of(8, 9), null);

                    reconciliation.flag();
                })));
    }

    @Test
    public void testAutoRebalancingWithoutSpecifiedMode(VertxTestContext context) {
        // edit the Kafka cluster without specifying the KafkaRebalance mode for scaling up
        Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).edit(
                k -> new KafkaBuilder(k)
                        .editSpec()
                        .editCruiseControl()
                        .withAutoRebalance(
                                new KafkaAutoRebalanceConfigurationBuilder()
                                        .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                                        .withNewTemplate("my-add-remove-brokers-rebalancing-template")
                                        .build()
                        )
                        .endCruiseControl()
                        .endSpec()
                        .build()
        );

        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling up the brokers
                    scaleKafkaCluster(7);
                })))
                // 2nd reconcile, auto-rebalancing for scaling up can't run, no mode specified in the auto-rebalance configuration
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.failing(e -> context.verify(() -> {
                    assertThat(e.getMessage(), is("No auto-rebalancing configuration specified for mode " + KafkaRebalanceMode.ADD_BROKERS));
                    reconciliation.flag();
                })));
    }

    private void patchKafkaRebalanceState(KafkaRebalance kafkaRebalance, KafkaRebalanceState state) {
        KafkaRebalance kafkaRebalancePatch = new KafkaRebalanceBuilder(kafkaRebalance)
                .withNewStatus()
                .withObservedGeneration(1L)
                .withConditions(new ConditionBuilder()
                        .withType(state.name())
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalancePatch).updateStatus();
    }

    private void scaleKafkaCluster(int replicas) {
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").edit(
                p -> new KafkaNodePoolBuilder(p)
                        .editSpec()
                            .withReplicas(replicas)
                        .endSpec()
                        .build()
        );
    }

    private void assertKafkaAutoRebalanceStatus(Kafka kafka, KafkaAutoRebalanceState state, KafkaRebalanceMode mode, List<Integer> brokers) {
        assertThat(kafka.getStatus().getAutoRebalance().getState(), is(state));
        Optional<KafkaAutoRebalanceModeBrokers> addModeBrokers = kafka.getStatus().getAutoRebalance().getModes().stream().filter(m -> m.getMode().equals(mode)).findFirst();
        assertThat(addModeBrokers.isPresent(), is(true));
        assertThat(addModeBrokers.get().getBrokers().size(), is(brokers.size()));
        assertThat(addModeBrokers.get().getBrokers().containsAll(brokers), is(true));
    }

    private void assertKafkaRebalanceStatus(KafkaRebalance kafkaRebalance, KafkaRebalanceMode mode, List<Integer> brokers, List<String> goals) {
        assertThat(kafkaRebalance, is(notNullValue()));
        assertThat(kafkaRebalance.getSpec().getMode(), is(mode));
        assertThat(kafkaRebalance.getSpec().getBrokers().size(), is(brokers.size()));
        assertThat(kafkaRebalance.getSpec().getBrokers().containsAll(brokers), is(true));
        if (goals != null) {
            assertThat(kafkaRebalance.getSpec().getGoals().containsAll(goals), is(true));
        } else {
            assertThat(kafkaRebalance.getSpec().getGoals(), is(nullValue()));
        }
    }

    private boolean isAutoRebalanceModeBrokers(Kafka kafka, KafkaRebalanceMode mode) {
        return kafka.getStatus().getAutoRebalance().getModes().stream().anyMatch(m -> m.getMode().equals(mode));
    }

    private KafkaRebalance buildKafkaRebalanceTemplate(String name, List<String> goals) {
        return new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(Map.of(ANNO_STRIMZI_IO_REBALANCE, "template"))
                .endMetadata()
                .withNewSpec()
                    .withGoals(goals)
                .endSpec()
                .build();
    }
}
