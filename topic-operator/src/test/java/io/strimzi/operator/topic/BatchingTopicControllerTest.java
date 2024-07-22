/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.Either;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.PartitionedByError;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicOperatorException;
import io.strimzi.operator.topic.model.TopicState;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.strimzi.api.kafka.model.topic.KafkaTopic.RESOURCE_KIND;
import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.PENDING;
import static io.strimzi.operator.topic.TopicOperatorUtil.topicName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * This unit test suite is not intended to provide lots of coverage of the {@link BatchingTopicController}, 
 * rather it aims to cover some parts that a difficult to test via {@link TopicControllerIT}.
 * 
 * Some examples are internal operations and behaviors, or when you need to test a system 
 * behavior without starting a slow external service (e.g. Cruise Control).
 */
@ExtendWith(KafkaClusterExtension.class)
class BatchingTopicControllerTest {
    private static final String NAMESPACE = TopicOperatorTestUtil.namespaceName(BatchingTopicControllerTest.class);

    private static MockKube3 mockKube;
    private static KubernetesClient kubernetesClient;
    private final Admin[] kafkaAdminClient = new Admin[] {null};

    @BeforeAll
    public static void beforeAll() {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaTopicCrd()
            .withDeletionController()
            .withNamespaces(NAMESPACE)
            .build();
        mockKube.start();
        kubernetesClient = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @AfterEach
    public void afterEach() {
        if (kafkaAdminClient[0] != null) {
            kafkaAdminClient[0].close();
        }
        TopicOperatorTestUtil.cleanupNamespace(kubernetesClient, NAMESPACE);
    }

    private static <T> KafkaFuture<T> interruptedFuture() throws ExecutionException, InterruptedException {
        var future = Mockito.mock(KafkaFuture.class);
        doThrow(new InterruptedException()).when(future).get();
        return future;
    }

    private KafkaTopic createKafkaTopic(String name) {
        return createKafkaTopic(name, 1);
    }

    private KafkaTopic createKafkaTopic(String topicName, int replicas) {
        var kt = Crds.topicOperation(kubernetesClient).resource(new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName(topicName)
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                    .withNewSpec()
                    .withPartitions(2)
                    .withReplicas(replicas)
                .endSpec().build()).create();
        return kt;
    }

    private void assertOnUpdateThrowsInterruptedException(Admin kafkaAdminClient, KafkaTopic kafkaTopic) {
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();

        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"), kafkaAdminClient, kubernetesClient, metricsHolder, new ReplicasChangeHandler(config, metricsHolder));

        List<ReconcilableTopic> batch = List.of(new ReconcilableTopic(new Reconciliation("test", "KafkaTopic", NAMESPACE, "my-topic"), kafkaTopic, topicName(kafkaTopic)));
        assertThrows(InterruptedException.class, () -> controller.onUpdate(batch));
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDescribeTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient[0]);
        var result = Mockito.mock(DescribeTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).allTopicNames();
        Mockito.doReturn(Map.of(topicName, interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(kafkaAdminClientSpy).describeTopics(any(Collection.class));

        KafkaTopic kt = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(kafkaAdminClientSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDescribeConfigs(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient[0]);
        var result = Mockito.mock(DescribeConfigsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicConfigResource(), interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(kafkaAdminClientSpy).describeConfigs(Mockito.argThat(a -> a.stream().anyMatch(x -> x.type() == ConfigResource.Type.TOPIC)));

        KafkaTopic kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(kafkaAdminClientSpy, kafkaTopic);
    }

    private static ConfigResource topicConfigResource() {
        return new ConfigResource(ConfigResource.Type.TOPIC, "my-topic");
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreateTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient[0]);
        var result = Mockito.mock(CreateTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicName, interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(kafkaAdminClientSpy).createTopics(any());

        KafkaTopic kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(kafkaAdminClientSpy, kafkaTopic);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromIncrementalAlterConfigs(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        kafkaAdminClient[0].createTopics(List.of(new NewTopic(topicName, 1, (short) 1).configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")))).all().get();
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient[0]);
        var result = Mockito.mock(AlterConfigsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicConfigResource(), interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(kafkaAdminClientSpy).incrementalAlterConfigs(any());
        
        KafkaTopic kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(kafkaAdminClientSpy, kafkaTopic);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreatePartitions(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        kafkaAdminClient[0].createTopics(List.of(new NewTopic("my-topic", 1, (short) 1))).all().get();

        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient[0]);
        var result = Mockito.mock(CreatePartitionsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicName, interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(kafkaAdminClientSpy).createPartitions(any());
        
        KafkaTopic kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(kafkaAdminClientSpy, kafkaTopic);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromListReassignments(
            @BrokerCluster(numBrokers = 2)
            KafkaCluster cluster) throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        kafkaAdminClient[0].createTopics(List.of(new NewTopic(topicName, 2, (short) 2))).all().get();

        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient[0]);
        var result = Mockito.mock(ListPartitionReassignmentsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).reassignments();
        Mockito.doReturn(result).when(kafkaAdminClientSpy).listPartitionReassignments(any(Set.class));
        
        KafkaTopic kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(kafkaAdminClientSpy, kafkaTopic);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDeleteTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        kafkaAdminClient[0].createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();
        
        createKafkaTopic(topicName);
        Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").edit(theKt -> 
            new KafkaTopicBuilder(theKt).editOrNewMetadata().addToFinalizers(BatchingTopicController.FINALIZER).endMetadata().build());
        Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").delete();
        var withDeletionTimestamp = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();

        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient[0]);
        var result = Mockito.mock(DeleteTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicName, interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(kafkaAdminClientSpy).deleteTopics(any(TopicCollection.TopicNameCollection.class));
        
        assertOnUpdateThrowsInterruptedException(kafkaAdminClientSpy, withDeletionTimestamp);
    }

    // TODO kube client interrupted exceptions

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void replicasChangeShouldBeReconciled(boolean cruiseControlEnabled) {
        var topicName = "my-topic";
        var replicationFactor = 1;

        // setup
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        Mockito.doReturn(cruiseControlEnabled).when(config).cruiseControlEnabled();

        var describeClusterResult = Mockito.mock(DescribeClusterResult.class);
        Mockito.doReturn(KafkaFuture.completedFuture(List.of())).when(describeClusterResult).nodes();

        var partitionReassignmentResult = Mockito.mock(ListPartitionReassignmentsResult.class);
        var topicPartition = Mockito.mock(TopicPartition.class);
        var partitionReassignment = Mockito.mock(PartitionReassignment.class);
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment))).when(partitionReassignmentResult).reassignments();

        var kafkaAdminClient = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(kafkaAdminClient).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(kafkaAdminClient).listPartitionReassignments(any(Set.class));
        
        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();
        
        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();
        
        var inputKt = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(++replicationFactor)
            .endSpec()
            .build();
        var inputRt = TopicOperatorTestUtil.reconcilableTopic(inputKt, NAMESPACE);

        var reconcilableTopics = List.of(inputRt);
        var currentStatesOrError = new PartitionedByError<>(List.of(new Pair<>(inputRt, Either.ofRight(currentState))), List.of());
        
        var outputKt = new KafkaTopicBuilder(inputKt)
            .withStatus(new KafkaTopicStatusBuilder()
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                    .withState(PENDING)
                    .withTargetReplicas(replicationFactor)
                    .build())
                .build())
            .build();
        var outputRt = TopicOperatorTestUtil.reconcilableTopic(outputKt, NAMESPACE);

        var replicasChangeHandler = Mockito.mock(ReplicasChangeHandler.class);
        Mockito.doReturn(List.of(outputRt)).when(replicasChangeHandler).requestPendingChanges(anyList());
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestOngoingChanges(anyList());

        // test
        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"), kafkaAdminClient, kubernetesClient, metricsHolder, replicasChangeHandler);
        var results = controller.checkReplicasChanges(reconcilableTopics, currentStatesOrError);
        
        if (cruiseControlEnabled) {
            assertThat(results.ok().count(), is(1L));
            assertThat(results.ok().findFirst().get().getKey().kt(), is(outputKt));
        } else {
            assertThat(results.errors().count(), is(1L));
            assertThat(results.errors().findFirst().get().getValue(), instanceOf(TopicOperatorException.NotSupported.class));
        }
    }
    
    @Test
    public void replicasChangeShouldCompleteWhenSpecIsReverted() {
        var topicName = "my-topic";
        var replicationFactor = 3;

        // setup: pending with error and .spec.replicas == uniqueReplicationFactor
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();

        var describeClusterResult = Mockito.mock(DescribeClusterResult.class);
        Mockito.doReturn(KafkaFuture.completedFuture(List.of())).when(describeClusterResult).nodes();

        var partitionReassignmentResult = Mockito.mock(ListPartitionReassignmentsResult.class);
        var topicPartition = Mockito.mock(TopicPartition.class);
        var partitionReassignment = Mockito.mock(PartitionReassignment.class);
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment))).when(partitionReassignmentResult).reassignments();

        var kafkaAdminClient = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(kafkaAdminClient).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(kafkaAdminClient).listPartitionReassignments(any(Set.class));

        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();

        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();
        
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(replicationFactor)
            .endSpec()
            .withStatus(new KafkaTopicStatusBuilder()
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                        .withMessage("Error message")
                        .withState(PENDING)
                        .withTargetReplicas(replicationFactor)
                    .build())
                .build())
            .build();

        var replicasChangeHandler = Mockito.mock(ReplicasChangeHandler.class);
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestPendingChanges(anyList());
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestOngoingChanges(anyList());

        var reconcilableTopics = List.of(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        PartitionedByError<ReconcilableTopic, TopicState> currentStatesOrError = new PartitionedByError<>(List.of(), List.of());
        
        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"), kafkaAdminClient, kubernetesClient, metricsHolder, new ReplicasChangeHandler(config, metricsHolder));
        var results = controller.checkReplicasChanges(reconcilableTopics, currentStatesOrError);

        assertThat(results.ok().count(), is(1L));
        assertThat(results.ok().findFirst().get().getKey().kt().getStatus().getReplicasChange(), is(nullValue()));
    }

    @Test
    public void replicasChangeShouldCompleteWhenCruiseControlRestarts() {
        var topicName = "my-topic";
        var replicationFactor = 1;

        // setup: pending with .spec.replicas == uniqueReplicationFactor
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();

        var describeClusterResult = Mockito.mock(DescribeClusterResult.class);
        Mockito.doReturn(KafkaFuture.completedFuture(List.of())).when(describeClusterResult).nodes();

        var partitionReassignmentResult = Mockito.mock(ListPartitionReassignmentsResult.class);
        var topicPartition = Mockito.mock(TopicPartition.class);
        var partitionReassignment = Mockito.mock(PartitionReassignment.class);
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment))).when(partitionReassignmentResult).reassignments();

        var kafkaAdminClient = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(kafkaAdminClient).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(kafkaAdminClient).listPartitionReassignments(any(Set.class));

        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();

        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();

        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(replicationFactor)
                .endSpec()
            .withStatus(new KafkaTopicStatusBuilder()
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                        .withState(PENDING)
                        .withTargetReplicas(replicationFactor)
                    .build())
                .build())
            .build();

        var replicasChangeHandler = Mockito.mock(ReplicasChangeHandler.class);
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestPendingChanges(anyList());
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestOngoingChanges(anyList());

        var reconcilableTopics = List.of(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        PartitionedByError<ReconcilableTopic, TopicState> currentStatesOrError = new PartitionedByError<>(List.of(), List.of());

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"), kafkaAdminClient, kubernetesClient, metricsHolder, new ReplicasChangeHandler(config, metricsHolder));
        var results = controller.checkReplicasChanges(reconcilableTopics, currentStatesOrError);
        
        assertThat(results.ok().count(), is(1L));
        assertThat(results.ok().findFirst().get().getKey().kt().getStatus().getReplicasChange(), is(nullValue()));
    }

    @Test
    public void shouldNotCallGetClusterConfigWhenDisabled() {
        var kafkaAdminClient = Mockito.mock(Admin.class);
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
              TopicOperatorConfig.SASL_ENABLED.key(), "true",
              TopicOperatorConfig.SKIP_CLUSTER_CONFIG_REVIEW.key(), "true"
        ));

        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        new BatchingTopicController(config, Map.of("key", "VALUE"), kafkaAdminClient, kubernetesClient, metricsHolder, new ReplicasChangeHandler(config, metricsHolder));
        
        verifyNoInteractions(kafkaAdminClient);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "ALL", "" })
    public void shouldUpdateProperties(String alterableTopicConfig, KafkaCluster cluster) throws InterruptedException, ExecutionException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        kafkaAdminClient[0].createTopics(List.of(new NewTopic(topicName, 1, (short) 1).configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")))).all().get();
        var kafkaAdminClient = Mockito.spy(this.kafkaAdminClient[0]);

        // Setup the KafkaTopic with 1 property change that is not in the alterableTopicConfig list.
        var kafkaTopic = Crds.topicOperation(kubernetesClient).resource(
              new KafkaTopicBuilder()
                  .withNewMetadata()
                      .withName(topicName)
                      .withNamespace(NAMESPACE)
                      .addToLabels("key", "VALUE")
                  .endMetadata()
                  .withNewSpec()
                      .withConfig(Map.of(
                           TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                           TopicConfig.CLEANUP_POLICY_CONFIG, "compact"))
                      .withPartitions(2)
                      .withReplicas(1)
                  .endSpec()
                  .build()).create();

        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).skipClusterConfigReview();
        Mockito.doReturn(alterableTopicConfig).when(config).alterableTopicConfig();

        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"), kafkaAdminClient, kubernetesClient, metricsHolder, new ReplicasChangeHandler(config, metricsHolder));
        
        var reconcilableTopics = List.of(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        controller.onUpdate(reconcilableTopics);

        Mockito.verify(kafkaAdminClient).incrementalAlterConfigs(any());

        var updateTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).get();

        var conditionList = updateTopic.getStatus().getConditions();
        assertEquals(1, conditionList.size());

        var readyCondition = conditionList.stream().filter(condition -> condition.getType().equals("Ready")).findFirst().get();
        assertEquals("True", readyCondition.getStatus());
    }

    @ParameterizedTest
    @ValueSource(strings = { "NONE", "sdasdas", "retention.bytes; retention.ms" })
    public void shouldNotUpdateAnyPropertiesWarnOnAllProperties(String alterableTopicConfig, KafkaCluster cluster) throws InterruptedException, ExecutionException {
        var topicName = "my-topic";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        kafkaAdminClient[0].createTopics(List.of(new NewTopic(topicName, 1, (short) 1).configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")))).all().get();
        var kafkaAdminClient = Mockito.spy(this.kafkaAdminClient[0]);

        // Setup the KafkaTopic with 2 property changes and an empty alterableTopicConfig list.
        var kafkaTopic = Crds.topicOperation(kubernetesClient).resource(
              new KafkaTopicBuilder()
                    .withNewMetadata()
                        .withName(topicName)
                        .withNamespace(NAMESPACE)
                        .addToLabels("key", "VALUE")
                    .endMetadata()
                    .withNewSpec()
                        .withConfig(Map.of(
                            TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                            TopicConfig.CLEANUP_POLICY_CONFIG, "compact"))
                        .withPartitions(2)
                        .withReplicas(1)
                    .endSpec()
                    .build()).create();

        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).skipClusterConfigReview();
        Mockito.doReturn(alterableTopicConfig).when(config).alterableTopicConfig();

        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"), kafkaAdminClient, kubernetesClient, metricsHolder, new ReplicasChangeHandler(config, metricsHolder));

        var reconcilableTopics = List.of(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        controller.onUpdate(reconcilableTopics);

        Mockito.verify(kafkaAdminClient, Mockito.never()).incrementalAlterConfigs(any());

        var updateTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).get();

        var conditionList = updateTopic.getStatus().getConditions();
        assertEquals(2, conditionList.size());

        var readyCondition = conditionList.stream().filter(condition -> condition.getType().equals("Ready")).findFirst().get();
        assertEquals("True", readyCondition.getStatus());

        var notConfiguredCondition = conditionList.stream().filter(condition -> condition.getType().equals("Warning")).findFirst().get();
        assertEquals("These .spec.config properties are not configurable: [cleanup.policy, compression.type]", notConfiguredCondition.getMessage());
        assertEquals("NotConfigurable", notConfiguredCondition.getReason());
        assertEquals("True", notConfiguredCondition.getStatus());
    }

    @Test
    public void shouldNotUpdatePropertiesNotInTheAlterableProperties(KafkaCluster cluster) throws InterruptedException, ExecutionException {
        var topicName = "my-topic";
        var alterableTopicConfig = "compression.type, max.message.bytes, message.timestamp.difference.max.ms, message.timestamp.type, retention.bytes, retention.ms";
        kafkaAdminClient[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        kafkaAdminClient[0].createTopics(List.of(new NewTopic(topicName, 1, (short) 1).configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")))).all().get();
        var kafkaAdminClient = Mockito.spy(this.kafkaAdminClient[0]);

        // Setup the KafkaTopic with 1 property change that is not in the alterableTopicConfig list.
        var kafkaTopic = Crds.topicOperation(kubernetesClient).resource(
              new KafkaTopicBuilder()
                    .withNewMetadata()
                        .withName(topicName)
                        .withNamespace(NAMESPACE)
                        .addToLabels("key", "VALUE")
                    .endMetadata()
                        .withNewSpec()
                        .withConfig(Map.of(
                            TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                            TopicConfig.CLEANUP_POLICY_CONFIG, "compact"))
                        .withPartitions(2)
                        .withReplicas(1)
                    .endSpec()
                    .build()).create();

        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).skipClusterConfigReview();
        Mockito.doReturn(alterableTopicConfig).when(config).alterableTopicConfig();

        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"), kafkaAdminClient, kubernetesClient, metricsHolder, new ReplicasChangeHandler(config, metricsHolder));

        var reconcilableTopics = List.of(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        controller.onUpdate(reconcilableTopics);

        Mockito.verify(kafkaAdminClient, Mockito.never()).incrementalAlterConfigs(any());

        var updateTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).get();

        var conditionList = updateTopic.getStatus().getConditions();
        assertEquals(2, conditionList.size());

        var readyCondition = conditionList.stream().filter(condition -> condition.getType().equals("Ready")).findFirst().get();
        assertEquals("True", readyCondition.getStatus());

        var notConfiguredCondition = conditionList.stream().filter(condition -> condition.getType().equals("Warning")).findFirst().get();
        assertEquals("These .spec.config properties are not configurable: [cleanup.policy]", notConfiguredCondition.getMessage());
        assertEquals("NotConfigurable", notConfiguredCondition.getReason());
        assertEquals("True", notConfiguredCondition.getStatus());
    }
}
