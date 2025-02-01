/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaConnectorOffsetsAnnotation;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.DefaultKafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
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

import java.util.ArrayList;
import java.util.HashMap;
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
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaMirrorMaker2AssemblyOperatorMockTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMaker2AssemblyOperatorMockTest.class);

    private static final String CLUSTER_NAME = "my-mm2-cluster";
    private static final String CONFIGMAP_NAME = "my-config-map";
    private static final int REPLICAS = 3;
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private static final Map<String, String> EXPECTED_CONNECTOR_CONFIG = new HashMap<>();
    static {
        EXPECTED_CONNECTOR_CONFIG.put("connector.class", "org.apache.kafka.connect.mirror.MirrorSourceConnector");
        EXPECTED_CONNECTOR_CONFIG.put("name", "source->target.MirrorSourceConnector");
        EXPECTED_CONNECTOR_CONFIG.put("replication.factor", "-1");
        EXPECTED_CONNECTOR_CONFIG.put("source.cluster.alias", "source");
        EXPECTED_CONNECTOR_CONFIG.put("source.cluster.bootstrap.servers", "source:9092");
        EXPECTED_CONNECTOR_CONFIG.put("source.cluster.security.protocol", "PLAINTEXT");
        EXPECTED_CONNECTOR_CONFIG.put("target.cluster.alias", "target");
        EXPECTED_CONNECTOR_CONFIG.put("target.cluster.bootstrap.servers", "target:9092");
        EXPECTED_CONNECTOR_CONFIG.put("target.cluster.security.protocol", "PLAINTEXT");
        EXPECTED_CONNECTOR_CONFIG.put("tasks.max", "1");
    }
    private static final String LIST_OFFSETS_JSON = "{\"offsets\": [{" +
            "\"partition\": {\"kafka_topic\": \"my-topic\",\"kafka_partition\": 2}," +
            "\"offset\": {\"kafka_offset\": 4}}]}";

    private static final String ALTER_OFFSETS_JSON = "{\"offsets\": [{" +
            "\"partition\": {\"kafka_topic\": \"my-topic\",\"kafka_partition\": 2}," +
            "\"offset\": {\"kafka_offset\": 2}}]}";

    private static final String RESET_OFFSETS_JSON = "{\"offsets\": [{" +
            "\"partition\": {\"kafka_topic\": \"my-topic\",\"kafka_partition\": 2}," +
            "\"offset\": {\"kafka_offset\": 2}}]}";

    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private String connectorOffsets;

    private static Vertx vertx;
    private KafkaMirrorMaker2AssemblyOperator kco;

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

        supplier = new ResourceOperatorSupplier(vertx, client,
                new DefaultAdminClientProvider(),
                new DefaultKafkaAgentClientProvider(),
                ResourceUtils.metricsProvider(),
                PFA);
        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
    }

    private Future<Void> createMirrorMaker2Cluster(VertxTestContext context, KafkaConnectApi kafkaConnectApi, boolean reconciliationPaused) {
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        kco = new KafkaMirrorMaker2AssemblyOperator(vertx, PFA,
            supplier,
            config,
            foo -> kafkaConnectApi);

        LOGGER.info("Reconciling initially -> create");
        Promise<Void> created = Promise.promise();

        kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, namespace, CLUSTER_NAME))
            .onComplete(context.succeeding(ar -> context.verify(() -> {
                if (!reconciliationPaused) {
                    assertThat(Crds.strimziPodSetOperation(client).inNamespace(namespace).withName(KafkaMirrorMaker2Resources.componentName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(client.configMaps().inNamespace(namespace).withName(KafkaMirrorMaker2Resources.configMapName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(client.services().inNamespace(namespace).withName(KafkaMirrorMaker2Resources.serviceName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(client.policy().v1().podDisruptionBudget().inNamespace(namespace).withName(KafkaMirrorMaker2Resources.componentName(CLUSTER_NAME)).get(), is(notNullValue()));
                } else {
                    assertThat(Crds.strimziPodSetOperation(client).inNamespace(namespace).withName(KafkaMirrorMaker2Resources.componentName(CLUSTER_NAME)).get(), is(nullValue()));
                }

                created.complete();
            })));

        return created.future();
    }

    private KafkaConnectApi mockConnectApi(String state)    {
        connectorOffsets = LIST_OFFSETS_JSON;
        Map<String, Object> status = Map.of("connector", Map.of("state", state));
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        when(mockConnectApi.list(any(), any(), anyInt())).thenReturn(CompletableFuture.completedFuture(new ArrayList<>()));
        when(mockConnectApi.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(mockConnectApi.getConnectorConfig(any(), any(), any(), anyInt(), eq("source->target.MirrorSourceConnector"))).thenReturn(CompletableFuture.completedFuture(EXPECTED_CONNECTOR_CONFIG));
        when(mockConnectApi.status(any(), any(), anyInt(), eq("source->target.MirrorSourceConnector"))).thenReturn(CompletableFuture.completedFuture(status));
        when(mockConnectApi.statusWithBackOff(any(), any(), any(), anyInt(), eq("source->target.MirrorSourceConnector"))).thenReturn(CompletableFuture.completedFuture(status));
        when(mockConnectApi.getConnectorTopics(any(), any(), anyInt(), eq("source->target.MirrorSourceConnector"))).thenReturn(CompletableFuture.completedFuture(List.of()));
        when(mockConnectApi.restart(any(), anyInt(), any(), anyBoolean(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(Map.of("connector", Map.of("state", "RESTARTING"))));
        when(mockConnectApi.getConnectorOffsets(any(), any(), anyInt(), anyString())).thenAnswer(invocation -> CompletableFuture.completedFuture(connectorOffsets));
        when(mockConnectApi.alterConnectorOffsets(any(), any(), anyInt(), anyString(), anyString())).thenAnswer(invocation -> {
            connectorOffsets = invocation.getArgument(4);
            return CompletableFuture.completedFuture(null);
        });
        when(mockConnectApi.resetConnectorOffsets(any(), any(), anyInt(), anyString())).thenAnswer(invocation -> {
            connectorOffsets = RESET_OFFSETS_JSON;
            return CompletableFuture.completedFuture(null);
        });

        return mockConnectApi;
    }

    @Test
    public void testReconcileUpdate(VertxTestContext context) {
        Crds.kafkaMirrorMaker2Operation(client).resource(new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withLabels(Map.of("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(REPLICAS)
                    .withConnectCluster("target")
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("source").withBootstrapServers("source:9092").build(),
                            new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("target").withBootstrapServers("target:9092").build())
                    .withMirrors(List.of())
                .endSpec()
            .build()).create();

        KafkaConnectApi mock = mock(KafkaConnectApi.class);
        when(mock.list(any(), anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(emptyList()));
        when(mock.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(CompletableFuture.completedFuture(null));

        Checkpoint async = context.checkpoint();
        createMirrorMaker2Cluster(context, mock, false)
            .onComplete(context.succeeding(i -> { }))
            .compose(i -> {
                LOGGER.info("Reconciling again -> update");
                return kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, namespace, CLUSTER_NAME));
            })
            .onComplete(context.succeeding(v -> async.flag()));
    }

    @Test
    public void testPauseReconcile(VertxTestContext context) {
        Crds.kafkaMirrorMaker2Operation(client).resource(new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withLabels(Map.of("foo", "bar"))
                    .withAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(REPLICAS)
                    .withConnectCluster("target")
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("source").withBootstrapServers("source:9092").build(),
                            new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("target").withBootstrapServers("target:9092").build())
                    .withMirrors(List.of())
                .endSpec()
                .build()).create();

        KafkaConnectApi mock = mock(KafkaConnectApi.class);
        when(mock.list(any(), anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(emptyList()));
        when(mock.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(CompletableFuture.completedFuture(null));

        Checkpoint async = context.checkpoint();
        createMirrorMaker2Cluster(context, mock, true)
                .onComplete(context.succeeding(v -> {
                    LOGGER.info("Reconciling again -> update");
                    kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, namespace, CLUSTER_NAME));
                }))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Resource<KafkaMirrorMaker2> resource = Crds.kafkaMirrorMaker2Operation(client).inNamespace(namespace).withName(CLUSTER_NAME);

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
                    async.flag();
                })));
    }

    @Test
    public void testListOffsets(VertxTestContext context) {
        String connectorName = "source->target.MirrorSourceConnector";
        Crds.kafkaMirrorMaker2Operation(client).resource(new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withLabels(Map.of("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(REPLICAS)
                    .withConnectCluster("target")
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("source").withBootstrapServers("source:9092").build(),
                            new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("target").withBootstrapServers("target:9092").build())
                    .withMirrors(
                            new KafkaMirrorMaker2MirrorSpecBuilder()
                                    .withSourceCluster("source")
                                    .withTargetCluster("target")
                                    .withNewSourceConnector()
                                        .withTasksMax(1)
                                        .withConfig(Map.of("replication.factor", -1))
                                        .withNewListOffsets()
                                            .withNewToConfigMap(CONFIGMAP_NAME)
                                        .endListOffsets()
                                    .endSourceConnector()
                                    .build()
                    )
                .endSpec()
                .build()).create();

        KafkaConnectApi connectApi = mockConnectApi("RUNNING");

        Checkpoint async = context.checkpoint();
        createMirrorMaker2Cluster(context, connectApi, false)
                .compose(i -> {
                    LOGGER.info("Annotating KafkaMirrorMaker2 to list offsets");
                    Crds.kafkaMirrorMaker2Operation(client).inNamespace(namespace).withName(CLUSTER_NAME).edit(spec -> new KafkaMirrorMaker2Builder(spec)
                            .editMetadata()
                            .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, KafkaConnectorOffsetsAnnotation.list.toString())
                            .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR, connectorName)
                            .endMetadata()
                            .build());
                    return kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, namespace, CLUSTER_NAME));
                })
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    //Assert annotations removed
                    KafkaMirrorMaker2 kafkaMirrorMaker2 = Crds.kafkaMirrorMaker2Operation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(kafkaMirrorMaker2.getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));
                    assertThat(kafkaMirrorMaker2.getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR)));

                    //Assert configmap exists
                    ConfigMap configMap = client.configMaps().inNamespace(namespace).withName(CONFIGMAP_NAME).get();
                    assertNotNull(configMap);

                    //Assert owner reference
                    List<OwnerReference> ownerReferences = configMap.getMetadata().getOwnerReferences();
                    assertThat(ownerReferences, hasSize(1));
                    assertThat(ownerReferences.get(0).getName(), is(CLUSTER_NAME));
                    assertThat(ownerReferences.get(0).getKind(), is(KafkaMirrorMaker2.RESOURCE_KIND));

                    //Assert ConfigMap data
                    Map<String, String> data = configMap.getData();
                    assertThat(data, aMapWithSize(1));
                    assertThat(data, hasEntry("source--target.MirrorSourceConnector.json", LIST_OFFSETS_JSON));
                    async.flag();
                })));
    }

    @Test
    public void testAlterOffsets(VertxTestContext context) {
        String connectorName = "source->target.MirrorSourceConnector";
        Crds.kafkaMirrorMaker2Operation(client).resource(new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withLabels(Map.of("foo", "bar"))
                .endMetadata()
                    .withNewSpec()
                    .withReplicas(REPLICAS)
                    .withConnectCluster("target")
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("source").withBootstrapServers("source:9092").build(),
                            new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("target").withBootstrapServers("target:9092").build())
                    .withMirrors(
                            new KafkaMirrorMaker2MirrorSpecBuilder()
                                    .withSourceCluster("source")
                                    .withTargetCluster("target")
                                    .withNewSourceConnector()
                                        .withTasksMax(1)
                                        .withConfig(Map.of("replication.factor", -1))
                                        .withState(ConnectorState.STOPPED)
                                        .withNewAlterOffsets()
                                            .withNewFromConfigMap(CONFIGMAP_NAME)
                                        .endAlterOffsets()
                                    .endSourceConnector()
                                    .build()
                    )
                .endSpec()
                .build()).create();

        KafkaConnectApi connectApi = mockConnectApi("STOPPED");

        //Create ConfigMap
        ConfigMap configMapResource = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(CONFIGMAP_NAME)
                .endMetadata()
                .withData(Map.of("source--target.MirrorSourceConnector.json", ALTER_OFFSETS_JSON))
                .build();
        client.configMaps()
                .inNamespace(namespace)
                .resource(configMapResource)
                .create();

        Checkpoint async = context.checkpoint();

        createMirrorMaker2Cluster(context, connectApi, false)
                .compose(i -> {
                    LOGGER.info("Annotating KafkaMirrorMaker2 to alter offsets");
                    Crds.kafkaMirrorMaker2Operation(client).inNamespace(namespace).withName(CLUSTER_NAME).edit(spec -> new KafkaMirrorMaker2Builder(spec)
                            .editMetadata()
                            .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, KafkaConnectorOffsetsAnnotation.alter.toString())
                            .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR, connectorName)
                            .endMetadata()
                            .build());
                    return kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, namespace, CLUSTER_NAME));
                })
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    //Assert annotations removed
                    KafkaMirrorMaker2 kafkaMirrorMaker2 = Crds.kafkaMirrorMaker2Operation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(kafkaMirrorMaker2.getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));
                    assertThat(kafkaMirrorMaker2.getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR)));

                    //Assert offsets altered
                    assertThat(connectorOffsets, is(ALTER_OFFSETS_JSON));
                    async.flag();
                })));
    }

    @Test
    public void testResetOffsets(VertxTestContext context) {
        String connectorName = "source->target.MirrorSourceConnector";
        Crds.kafkaMirrorMaker2Operation(client).resource(new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withLabels(Map.of("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(REPLICAS)
                    .withConnectCluster("target")
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("source").withBootstrapServers("source:9092").build(),
                            new KafkaMirrorMaker2ClusterSpecBuilder().withAlias("target").withBootstrapServers("target:9092").build())
                    .withMirrors(
                            new KafkaMirrorMaker2MirrorSpecBuilder()
                                    .withSourceCluster("source")
                                    .withTargetCluster("target")
                                    .withNewSourceConnector()
                                        .withTasksMax(1)
                                        .withConfig(Map.of("replication.factor", -1))
                                        .withState(ConnectorState.STOPPED)
                                    .endSourceConnector()
                                    .build()
                    )
                .endSpec()
                .build()).create();

        KafkaConnectApi connectApi = mockConnectApi("STOPPED");

        Checkpoint async = context.checkpoint();

        createMirrorMaker2Cluster(context, connectApi, false)
                .compose(i -> {
                    LOGGER.info("Annotating KafkaMirrorMaker2 to reset offsets");
                    Crds.kafkaMirrorMaker2Operation(client).inNamespace(namespace).withName(CLUSTER_NAME).edit(spec -> new KafkaMirrorMaker2Builder(spec)
                            .editMetadata()
                            .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, KafkaConnectorOffsetsAnnotation.reset.toString())
                            .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR, connectorName)
                            .endMetadata()
                            .build());
                    return kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, namespace, CLUSTER_NAME));
                })
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    //Assert annotations removed
                    KafkaMirrorMaker2 kafkaMirrorMaker2 = Crds.kafkaMirrorMaker2Operation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(kafkaMirrorMaker2.getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS)));
                    assertThat(kafkaMirrorMaker2.getMetadata().getAnnotations(), not(hasKey(ResourceAnnotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR)));

                    //Assert offsets reset
                    assertThat(connectorOffsets, is(RESET_OFFSETS_JSON));
                    async.flag();
                })));
    }
}
