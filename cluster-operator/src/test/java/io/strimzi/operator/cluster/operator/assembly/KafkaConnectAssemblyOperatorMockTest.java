/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectAssemblyOperatorMockTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectAssemblyOperatorMockTest.class);
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private static final String CLUSTER_NAME = "my-connect-cluster";
    private static final int REPLICAS = 3;

    private static WorkerExecutor sharedWorkerExecutor;
    private static Vertx vertx;
    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private KafkaConnectAssemblyOperator kco;
    private StrimziPodSetController podSetController;
    private ResourceOperatorSupplier supplier;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaConnectCrd()
                .withKafkaMirrorMaker2Crd()
                .withStrimziPodSetCrd()
                .withPodController()
                .build();
        mockKube.start();
        client = mockKube.client();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void afterAll() {
        sharedWorkerExecutor.close();
        vertx.close();
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        supplier = new ResourceOperatorSupplier(vertx, client, ResourceUtils.adminClientProvider(), ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), PFA);
        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
    }

    private Future<Void> createConnectCluster(VertxTestContext context, KafkaConnectApi kafkaConnectApi, boolean reconciliationPaused) {
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        this.kco = new KafkaConnectAssemblyOperator(vertx, PFA, supplier, config, foo -> kafkaConnectApi);

        Promise<Void> created = Promise.promise();

        LOGGER.info("Reconciling initially -> create");
        kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                if (!reconciliationPaused) {
                    assertThat(Crds.strimziPodSetOperation(client).inNamespace(namespace).withName(KafkaConnectResources.componentName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(client.configMaps().inNamespace(namespace).withName(KafkaConnectResources.configMapName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(client.services().inNamespace(namespace).withName(KafkaConnectResources.serviceName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(client.policy().v1().podDisruptionBudget().inNamespace(namespace).withName(KafkaConnectResources.componentName(CLUSTER_NAME)).get(), is(notNullValue()));
                } else {
                    assertThat(Crds.strimziPodSetOperation(client).inNamespace(namespace).withName(KafkaConnectResources.componentName(CLUSTER_NAME)).get(), is(nullValue()));
                }
                created.complete();
            })));
        return created.future();
    }

    @Test
    public void testReconcileCreateAndUpdate(VertxTestContext context) {
        Crds.kafkaConnectOperation(client).resource(new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withLabels(Map.of("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(REPLICAS)
                    .withBootstrapServers("my-kafka:9092")
                .endSpec()
            .build()).create();
        KafkaConnectApi mock = mock(KafkaConnectApi.class);
        when(mock.list(any(), anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(emptyList()));
        when(mock.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(emptyList()));

        Checkpoint async = context.checkpoint();
        createConnectCluster(context, mock, false)
            .onComplete(context.succeeding(i -> { }))
            .compose(v -> {
                LOGGER.info("Reconciling again -> update");
                return kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, namespace, CLUSTER_NAME));
            })
            .onComplete(context.succeeding(v -> async.flag()));

    }

    @Test
    public void testPauseReconcileUnpause(VertxTestContext context) {
        Crds.kafkaConnectOperation(client).resource(new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withLabels(Map.of("foo", "bar"))
                    .withAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(REPLICAS)
                    .withBootstrapServers("my-kafka:9092")
                .endSpec()
                .build()).create();
        KafkaConnectApi mock = mock(KafkaConnectApi.class);
        when(mock.list(any(), anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(emptyList()));
        when(mock.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(emptyList()));

        Checkpoint async = context.checkpoint();
        createConnectCluster(context, mock, true)
                .onComplete(context.succeeding(i -> { }))
                .compose(v -> {
                    LOGGER.info("Reconciling again -> update");
                    return kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, namespace, CLUSTER_NAME));
                })
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Resource<KafkaConnect> resource = Crds.kafkaConnectOperation(client).inNamespace(namespace).withName(CLUSTER_NAME);
                    if (resource.get().getStatus() == null) {
                        context.failNow("Status is null");
                    }
                    List<Condition> conditions = resource.get().getStatus().getConditions();
                    boolean conditionFound = false;
                    if (conditions != null && !conditions.isEmpty()) {
                        for (Condition condition: conditions) {
                            if ("ReconciliationPaused".equals(condition.getType())) {
                                conditionFound = true;
                                break;
                            }
                        }
                    }
                    assertTrue(conditionFound);
                })))
                .compose(v -> {
                    Crds.kafkaConnectOperation(client).inNamespace(namespace).resource(new KafkaConnectBuilder()
                            .withNewMetadata()
                                .withName(CLUSTER_NAME)
                                .withNamespace(namespace)
                                .withLabels(Map.of("foo", "bar"))
                                .withAnnotations(singletonMap("strimzi.io/pause-reconciliation", "false"))
                            .endMetadata()
                            .withNewSpec()
                                .withReplicas(REPLICAS)
                                .withBootstrapServers("my-kafka:9092")
                            .endSpec()
                            .build()).update();
                    LOGGER.info("Reconciling again -> update");
                    return kco.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, namespace, CLUSTER_NAME));
                })
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Resource<KafkaConnect> resource = Crds.kafkaConnectOperation(client).inNamespace(namespace).withName(CLUSTER_NAME);
                    if (resource.get().getStatus() == null) {
                        context.failNow("Status is null");
                    }
                    List<Condition> conditions = resource.get().getStatus().getConditions();
                    boolean conditionFound = false;
                    if (conditions != null && !conditions.isEmpty()) {
                        for (Condition condition: conditions) {
                            if ("ReconciliationPaused".equals(condition.getType())) {
                                conditionFound = true;
                                break;
                            }
                        }
                    }
                    assertFalse(conditionFound);

                    async.flag();
                })));
    }
}
