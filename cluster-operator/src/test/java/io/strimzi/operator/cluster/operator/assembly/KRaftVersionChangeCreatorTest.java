/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaUpgradeException;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KRaftVersionChangeCreatorTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    //////////
    // Test how metadata version is determined
    //////////

    @Test
    public void testMetadataVersionAtNoChange() {
        assertThat(KRaftVersionChangeCreator.metadataVersionWithoutKafkaVersionChange(Reconciliation.DUMMY_RECONCILIATION, "3.4-IV1", VERSIONS.defaultVersion().metadataVersion(), VERSIONS.defaultVersion()),
                is(VERSIONS.defaultVersion().messageVersion()));

        assertThat(KRaftVersionChangeCreator.metadataVersionWithoutKafkaVersionChange(Reconciliation.DUMMY_RECONCILIATION, "3.4-IV1", null, VERSIONS.defaultVersion()),
                is(VERSIONS.defaultVersion().messageVersion()));

        assertThat(KRaftVersionChangeCreator.metadataVersionWithoutKafkaVersionChange(Reconciliation.DUMMY_RECONCILIATION, null, null, VERSIONS.defaultVersion()),
                is(VERSIONS.defaultVersion().messageVersion()));

        assertThat(KRaftVersionChangeCreator.metadataVersionWithoutKafkaVersionChange(Reconciliation.DUMMY_RECONCILIATION, "3.4-IV1", "3.5", VERSIONS.defaultVersion()),
                is("3.5"));

        assertThat(KRaftVersionChangeCreator.metadataVersionWithoutKafkaVersionChange(Reconciliation.DUMMY_RECONCILIATION, "3.4-IV1", "3.5-IV2", VERSIONS.defaultVersion()),
                is("3.5-IV2"));

        assertThat(KRaftVersionChangeCreator.metadataVersionWithoutKafkaVersionChange(Reconciliation.DUMMY_RECONCILIATION, "3.4-IV1", "5.11", VERSIONS.defaultVersion()),
                is("3.4-IV1"));

        assertThat(KRaftVersionChangeCreator.metadataVersionWithoutKafkaVersionChange(Reconciliation.DUMMY_RECONCILIATION, null, "5.11", VERSIONS.defaultVersion()),
                is(VERSIONS.defaultVersion().messageVersion()));
    }

    @Test
    public void testMetadataVersionAtUpgrade() {
        assertThat(KRaftVersionChangeCreator.metadataVersionAtUpgrade(Reconciliation.DUMMY_RECONCILIATION, "3.6-IV2", VERSIONS.defaultVersion()),
                is("3.6-IV2"));

        assertThat(KRaftVersionChangeCreator.metadataVersionAtUpgrade(Reconciliation.DUMMY_RECONCILIATION, "3.4-IV2", VERSIONS.defaultVersion()),
                is("3.4-IV2"));

        assertThat(KRaftVersionChangeCreator.metadataVersionAtUpgrade(Reconciliation.DUMMY_RECONCILIATION, null, VERSIONS.defaultVersion()),
                is(VERSIONS.defaultVersion().metadataVersion()));

        KafkaUpgradeException ex = assertThrows(KafkaUpgradeException.class, () -> KRaftVersionChangeCreator.metadataVersionAtUpgrade(Reconciliation.DUMMY_RECONCILIATION, "5.11-IV2", VERSIONS.defaultVersion()));
        assertThat(ex.getMessage(), is("The current metadata version (5.11-IV2) has to be lower or equal to the Kafka broker version we upgrade from (" + VERSIONS.defaultVersion().version() + ")"));
    }

    @Test
    public void testMetadataVersionAtDowngrade() {
        assertThat(KRaftVersionChangeCreator.metadataVersionAtDowngrade(Reconciliation.DUMMY_RECONCILIATION, "3.6-IV2", VERSIONS.defaultVersion()),
                is("3.6-IV2"));

        assertThat(KRaftVersionChangeCreator.metadataVersionAtDowngrade(Reconciliation.DUMMY_RECONCILIATION, "3.6", VERSIONS.defaultVersion()),
                is("3.6"));

        assertThat(KRaftVersionChangeCreator.metadataVersionAtDowngrade(Reconciliation.DUMMY_RECONCILIATION, "3.4-IV2", VERSIONS.defaultVersion()),
                is("3.4-IV2"));

        assertThat(KRaftVersionChangeCreator.metadataVersionAtDowngrade(Reconciliation.DUMMY_RECONCILIATION, null, VERSIONS.defaultVersion()),
                is(VERSIONS.defaultVersion().metadataVersion()));

        KafkaUpgradeException ex = assertThrows(KafkaUpgradeException.class, () -> KRaftVersionChangeCreator.metadataVersionAtDowngrade(Reconciliation.DUMMY_RECONCILIATION, "5.11-IV2", VERSIONS.defaultVersion()));
        assertThat(ex.getMessage(), is("The current metadata version (5.11-IV2) has to be lower or equal to the Kafka broker version we are downgrading to (" + VERSIONS.defaultVersion().version() + ")"));
    }

    //////////
    // Test for a new cluster
    //////////

    @Test
    public void testNewClusterWithAllVersions(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), VERSIONS.defaultVersion().metadataVersion(), VERSIONS.defaultVersion().metadataVersion()),
                mockRos(List.of())
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.defaultVersion().metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testNewClusterWithoutVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, null, null),
                mockRos(List.of())
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.defaultVersion().metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testNewClusterWithKafkaVersionOnly(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), null, null),
                mockRos(List.of())
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.defaultVersion().metadataVersion()));

            async.flag();
        })));
    }

    //////////
    // Tests for an existing cluster without upgrade
    //////////

    @Test
    public void testExistingClusterWithAllVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), VERSIONS.defaultVersion().metadataVersion(), VERSIONS.defaultVersion().metadataVersion()),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.defaultVersion().metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testExistingClusterWithoutVersions(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, VERSIONS.defaultVersion().metadataVersion(), null),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.defaultVersion().metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testExistingClusterWithoutVersionsWithOldMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, "3.4-IV2", null),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.defaultVersion().metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testExistingClusterWithNewMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), "3.4-IV2", "3.6"),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is("3.6"));

            async.flag();
        })));
    }

    @Test
    public void testExistingClusterWithRemovedMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), "3.4-IV2", null),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.defaultVersion().metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testExistingClusterWithWrongMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), "3.4-IV2", "5.11-IV2"),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is("3.4-IV2"));

            async.flag();
        })));
    }

    @Test
    public void testExistingClusterWithPodWithoutAnnotations(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), VERSIONS.defaultVersion().metadataVersion(), VERSIONS.defaultVersion().metadataVersion()),
                mockRos(mockUniformPods(null))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.failing(c -> context.verify(() -> {
            assertThat(c, is(instanceOf(KafkaUpgradeException.class)));
            assertThat(c.getMessage(), is("Kafka Pods exist, but do not contain the strimzi.io/kafka-version annotation to detect their version. Kafka upgrade cannot be detected."));

            async.flag();
        })));
    }

    //////////
    // Upgrade tests
    //////////

    @Test
    public void testUpgradeWithAllVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion(), VERSIONS.defaultVersion().metadataVersion()),
                mockRos(mockUniformPods(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithoutVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion(), null),
                mockRos(mockUniformPods(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithAllVersionAndMixedPods(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion(), VERSIONS.defaultVersion().metadataVersion()),
                mockRos(mockMixedPods(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithoutVersionAndMixedPods(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(null, VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion(), null),
                mockRos(mockMixedPods(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)));
            assertThat(c.to(), is(VERSIONS.defaultVersion()));
            assertThat(c.metadataVersion(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testUpgradeWithWrongCurrentMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.defaultVersion().version(), "5.11-IV1", VERSIONS.defaultVersion().metadataVersion()),
                mockRos(mockUniformPods(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.failing(c -> context.verify(() -> {
            assertThat(c, is(instanceOf(KafkaUpgradeException.class)));
            assertThat(c.getMessage(), is("The current metadata version (5.11-IV1) has to be lower or equal to the Kafka broker version we upgrade from (" + VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version() + ")"));

            async.flag();
        })));
    }

    //////////
    // Downgrade tests
    //////////

    @Test
    public void testDowngradeWithAllVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion(), VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)));
            assertThat(c.metadataVersion(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testDowngradeWithUnsetDesiredMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion(), null),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)));
            assertThat(c.metadataVersion(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()));

            async.flag();
        })));
    }

    @Test
    public void testDowngradeWithOldMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), "3.4-IV2", "3.4-IV2"),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)));
            assertThat(c.metadataVersion(), is("3.4-IV2"));

            async.flag();
        })));
    }

    @Test
    public void testDowngradeWithAllVersionAndMixedPods(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion(), VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()),
                mockRos(mockMixedPods(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from(), is(VERSIONS.defaultVersion()));
            assertThat(c.to(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)));
            assertThat(c.metadataVersion(), is(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()));

            async.flag();
        })));
    }
    
    @Test
    public void testDowngradeFromUnknownVersion(VertxTestContext context) {
        String unknownVersion = getHigherVersionThanLatest();
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.version(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).version(), VERSIONS.version(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).metadataVersion(), VERSIONS.version(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).metadataVersion()),
                mockRos(mockUniformPods(unknownVersion))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(c.from().version(), is(unknownVersion));
            assertThat(c.to(), is(VERSIONS.version(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)));
            assertThat(c.metadataVersion(), is(VERSIONS.version(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).metadataVersion()));

            async.flag();
        })));
    }
    
    private String getHigherVersionThanLatest() {
        String[] parsedVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION.split("\\.");
        int incrementedMinorVersion = Integer.parseInt(parsedVersion[1]) + 1;
        parsedVersion[1] = Integer.toString(incrementedMinorVersion);
        return String.join(".", parsedVersion);  
    }

    @Test
    public void testDowngradeWithWrongCurrentMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), "5.11-IV1", VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.failing(c -> context.verify(() -> {
            assertThat(c, is(instanceOf(KafkaUpgradeException.class)));
            assertThat(c.getMessage(), is("The current metadata version (5.11-IV1) has to be lower or equal to the Kafka broker version we are downgrading to (" + VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version() + ")"));

            async.flag();
        })));
    }

    @Test
    public void testDowngradeWithNonDowngradedCurrentMetadataVersion(VertxTestContext context) {
        KRaftVersionChangeCreator vcc = mockVersionChangeCreator(
                mockKafka(VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), VERSIONS.defaultVersion().metadataVersion(), VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).metadataVersion()),
                mockRos(mockUniformPods(VERSIONS.defaultVersion().version()))
        );

        Checkpoint async = context.checkpoint();
        vcc.reconcile().onComplete(context.failing(c -> context.verify(() -> {
            assertThat(c, is(instanceOf(KafkaUpgradeException.class)));
            assertThat(c.getMessage(), is("The current metadata version (" + VERSIONS.defaultVersion().metadataVersion() + ") has to be lower or equal to the Kafka broker version we are downgrading to (" + VERSIONS.version(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version() + ")"));

            async.flag();
        })));
    }

    //////////
    // Utility methods
    //////////

    // Creates the VersionChangeCreator with the mocks
    private KRaftVersionChangeCreator mockVersionChangeCreator(Kafka kafka, ResourceOperatorSupplier ros)  {
        return new KRaftVersionChangeCreator(new Reconciliation("test", "Kafka", NAMESPACE, CLUSTER_NAME), kafka, ResourceUtils.dummyClusterOperatorConfig(), ros);
    }

    // Creates ResourceOperatorSupplier with mocks
    private ResourceOperatorSupplier mockRos(List<Pod> pods)   {
        ResourceOperatorSupplier ros = ResourceUtils.supplierWithMocks(false);

        PodOperator podOps = ros.podOperations;
        when(podOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        return ros;
    }

    // Prepares the Kafka CR with the specified versions
    private Kafka mockKafka(String kafkaVersion, String currentMetadataVersion, String desiredMetadataVersion)  {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                    .endKafka()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        if (kafkaVersion != null)   {
            kafka.getSpec().getKafka().setVersion(kafkaVersion);
        }

        if (desiredMetadataVersion != null)   {
            kafka.getSpec().getKafka().setMetadataVersion(desiredMetadataVersion);
        }

        if (currentMetadataVersion != null)   {
            kafka.setStatus(new KafkaStatusBuilder().withKafkaMetadataVersion(currentMetadataVersion).build());
        }

        return kafka;
    }

    // Prepares the Pods all with the same versions
    private List<Pod> mockUniformPods(String kafkaVersion)  {
        List<Pod> pods = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            Pod pod = new PodBuilder()
                    .withNewMetadata()
                        .withNamespace(NAMESPACE)
                        .withName(CLUSTER_NAME  + "-kafka-" + i)
                        .withAnnotations(new HashMap<>())
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build();

            if (kafkaVersion != null)   {
                pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion);
            }

            pods.add(pod);
        }

        return pods;
    }

    // Prepares the Pods all with mixed versions
    private List<Pod> mockMixedPods(String kafkaVersion, String kafkaVersion2)  {
        List<Pod> pods = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            Pod pod = new PodBuilder()
                    .withNewMetadata()
                        .withNamespace(NAMESPACE)
                        .withName(CLUSTER_NAME  + "-kafka-" + i)
                        .withAnnotations(new HashMap<>())
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build();

            if (i % 2 == 0) {
                if (kafkaVersion != null) {
                    pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion);
                }
            } else {
                if (kafkaVersion2 != null) {
                    pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, kafkaVersion2);
                }
            }

            pods.add(pod);
        }

        return pods;
    }
}
