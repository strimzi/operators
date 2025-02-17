/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import javax.security.auth.login.AppConfigurationEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.operator.cluster.model.KafkaMirrorMaker2Connectors.PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("deprecation") // Uses deprecated getPause() field in tests
public class KafkaMirrorMaker2ConnectorsTest {
    private static final String PREFIX = "prefix.";

    private final static KafkaMirrorMaker2 KMM2 = new KafkaMirrorMaker2Builder()
            .withNewMetadata()
                .withName("my-mm2")
                .withNamespace("my-namespace")
            .endMetadata()
            .withNewSpec()
            .withReplicas(3)
            .withConnectCluster("target")
            .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                    .withAlias("source")
                    .withBootstrapServers("source:9092")
                    .build(),
                    new KafkaMirrorMaker2ClusterSpecBuilder()
                            .withAlias("target")
                            .withBootstrapServers("target:9092")
                            .build())
            .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                    .withSourceCluster("source")
                    .withTargetCluster("target")
                    .withNewSourceConnector()
                        .withTasksMax(5)
                        .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                    .endSourceConnector()
                    .withNewCheckpointConnector()
                        .withTasksMax(3)
                        .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                    .endCheckpointConnector()
                    .withNewHeartbeatConnector()
                        .withTasksMax(1)
                    .endHeartbeatConnector()
                    .withTopicsPattern("my-topic-.*")
                    .withTopicsExcludePattern("exclude-topic-.*")
                    .withGroupsPattern("my-group-.*")
                    .withGroupsExcludePattern("exclude-group-.*")
                    .build())
            .endSpec()
            .build();

    @Test
    public void testValidation()    {
        assertDoesNotThrow(() -> KafkaMirrorMaker2Connectors.validateConnectors(KMM2));
    }

    @Test
    public void testFailingValidation()    {
        // Missing spec
        KafkaMirrorMaker2 kmm2WithoutSpec = new KafkaMirrorMaker2Builder(KMM2)
                .withSpec(null)
                .build();
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2WithoutSpec));
        assertThat(ex.getMessage(), is(".spec section is required for KafkaMirrorMaker2 resource"));

        // Missing clusters
        KafkaMirrorMaker2 kmm2WithoutClusters = new KafkaMirrorMaker2Builder(KMM2)
                .withNewSpec()
                    .withMirrors(List.of())
                .endSpec()
                .build();
        ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2WithoutClusters));
        assertThat(ex.getMessage(), is(".spec.clusters and .spec.mirrors sections are required in KafkaMirrorMaker2 resource"));

        // Missing mirrors
        KafkaMirrorMaker2 kmm2WithoutMirrors = new KafkaMirrorMaker2Builder(KMM2)
                .withNewSpec()
                    .withClusters(List.of())
                .endSpec()
                .build();
        ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2WithoutMirrors));
        assertThat(ex.getMessage(), is(".spec.clusters and .spec.mirrors sections are required in KafkaMirrorMaker2 resource"));

        // Missing alias
        KafkaMirrorMaker2 kmm2WrongAlias = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .editMirror(0)
                        .withSourceCluster(null)
                        .withTargetCluster("wrong-target")
                    .endMirror()
                .endSpec()
                .build();
        ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2WrongAlias));
        assertThat(ex.getMessage(), is("KafkaMirrorMaker2 resource validation failed: " +
                "[Each MirrorMaker 2 mirror definition has to specify the source cluster alias, " +
                "Target cluster alias wrong-target is used in a mirror definition, but cluster with this alias does not exist in cluster definitions, " +
                "Connect cluster alias (currently set to target) must match the target cluster alias wrong-target or both clusters must have the same bootstrap servers.]"));
    }

    @Test
    public void testMirrorTargetClusterNotSameAsConnectCluster() {
        // The most obvious error case, where connect cluster is set to the source cluster instead of target
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withConnectCluster("source")
                .endSpec()
                .build();
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2));
        assertThat(ex.getMessage(), is("KafkaMirrorMaker2 resource validation failed: " +
                "[Connect cluster alias (currently set to source) must match the target cluster alias target or both clusters must have the same bootstrap servers.]"));

        // A case where one mirror has the correct target cluster, but the other does not
        KafkaMirrorMaker2 kmm2CorrectAndIncorrectMirror = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                .addToClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                                .withAlias("third")
                                .withBootstrapServers("third:9092")
                                .build())
                .addToMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withSourceCluster("source")
                        .withTargetCluster("third").build())
                .endSpec()
                .build();
        ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2CorrectAndIncorrectMirror));
        assertThat(ex.getMessage(), is("KafkaMirrorMaker2 resource validation failed: " +
                "[Connect cluster alias (currently set to target) must match the target cluster alias third or both clusters must have the same bootstrap servers.]"));
    }

    @Test
    public void testClusterNotSameButBootstrapUrlSame() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withConnectCluster("source")
                        .addToClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                            .withAlias("third")
                            .withBootstrapServers("source:9092")
                            .build())
                    .editMirror(0)
                        .withTargetCluster("third")
                    .endMirror()
                .endSpec()
                .build();

        assertDoesNotThrow(() -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2));
    }

    @Test
    public void testSourceClusterNotConnectCluster() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withConnectCluster("target")
                        .addToClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                            .withAlias("third")
                            .withBootstrapServers("source:9092")
                            .build())
                    .editMirror(0)
                        .withTargetCluster("third")
                    .endMirror()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2));
        assertThat(ex.getMessage(), is("KafkaMirrorMaker2 resource validation failed: " +
                "[Connect cluster alias (currently set to target) must match the target cluster alias third or both clusters must have the same bootstrap servers.]"));
    }

    @Test
    public void testMultipleMirrors() {
        KafkaMirrorMaker2 kmm2CorrectAndIncorrectMirror = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .addToClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                        .withAlias("fourth")
                        .withBootstrapServers("target:9092")
                        .build())
                    .addToMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withSourceCluster("source")
                        .withTargetCluster("fourth").build())
                .endSpec()
                .build();

        assertDoesNotThrow(() -> KafkaMirrorMaker2Connectors.validateConnectors(kmm2CorrectAndIncorrectMirror));
    }

    @Test
    public void testConnectors() {
        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KMM2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        Map<String, Object> expectedAll = new TreeMap<>();
        expectedAll.put("source.cluster.alias", "source");
        expectedAll.put("source.cluster.bootstrap.servers", "source:9092");
        expectedAll.put("source.cluster.security.protocol", "PLAINTEXT");
        expectedAll.put("target.cluster.alias", "target");
        expectedAll.put("target.cluster.bootstrap.servers", "target:9092");
        expectedAll.put("target.cluster.security.protocol", "PLAINTEXT");
        expectedAll.put("topics", "my-topic-.*");
        expectedAll.put("topics.exclude", "exclude-topic-.*");
        expectedAll.put("groups", "my-group-.*");
        expectedAll.put("groups.exclude", "exclude-group-.*");

        Map<String, Object> expectedSource = new TreeMap<>(expectedAll);
        expectedSource.put("sync.topic.acls.enabled", "false");

        Map<String, Object> expectedCheckpoint = new TreeMap<>(expectedAll);
        expectedCheckpoint.put("sync.group.offsets.enabled", "true");

        assertThat(kcs.size(), is(3));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(5));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedSource));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(3));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedCheckpoint));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorHeartbeatConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(1));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedAll));
    }

    @Test
    public void testConnectorsWithMultipleSources() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .addToClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                            .withAlias("other-source")
                            .withBootstrapServers("other-source:9092")
                            .build())
                    .addToMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withSourceCluster("other-source")
                            .withTargetCluster("target")
                            .withNewSourceConnector()
                                .withTasksMax(15)
                                .withConfig(Map.of("sync.topic.acls.enabled", "true"))
                            .endSourceConnector()
                            .withNewCheckpointConnector()
                                .withTasksMax(13)
                                .withConfig(Map.of("sync.group.offsets.enabled", "false"))
                            .endCheckpointConnector()
                            .withNewHeartbeatConnector()
                                .withTasksMax(11)
                            .endHeartbeatConnector()
                            .build())
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        Map<String, Object> expectedAll = new TreeMap<>();
        expectedAll.put("source.cluster.alias", "source");
        expectedAll.put("source.cluster.bootstrap.servers", "source:9092");
        expectedAll.put("source.cluster.security.protocol", "PLAINTEXT");
        expectedAll.put("target.cluster.alias", "target");
        expectedAll.put("target.cluster.bootstrap.servers", "target:9092");
        expectedAll.put("target.cluster.security.protocol", "PLAINTEXT");
        expectedAll.put("topics", "my-topic-.*");
        expectedAll.put("topics.exclude", "exclude-topic-.*");
        expectedAll.put("groups", "my-group-.*");
        expectedAll.put("groups.exclude", "exclude-group-.*");

        Map<String, Object> expectedSource = new TreeMap<>(expectedAll);
        expectedSource.put("sync.topic.acls.enabled", "false");

        Map<String, Object> expectedCheckpoint = new TreeMap<>(expectedAll);
        expectedCheckpoint.put("sync.group.offsets.enabled", "true");

        Map<String, Object> expectedAll2 = new TreeMap<>();
        expectedAll2.put("source.cluster.alias", "other-source");
        expectedAll2.put("source.cluster.bootstrap.servers", "other-source:9092");
        expectedAll2.put("source.cluster.security.protocol", "PLAINTEXT");
        expectedAll2.put("target.cluster.alias", "target");
        expectedAll2.put("target.cluster.bootstrap.servers", "target:9092");
        expectedAll2.put("target.cluster.security.protocol", "PLAINTEXT");

        Map<String, Object> expectedSource2 = new TreeMap<>(expectedAll2);
        expectedSource2.put("sync.topic.acls.enabled", "true");

        Map<String, Object> expectedCheckpoint2 = new TreeMap<>(expectedAll2);
        expectedCheckpoint2.put("sync.group.offsets.enabled", "false");

        assertThat(kcs.size(), is(6));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(5));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedSource));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(3));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedCheckpoint));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorHeartbeatConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(1));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedAll));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("other-source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("other-source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(15));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedSource2));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("other-source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("other-source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(13));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedCheckpoint2));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("other-source->target.MirrorHeartbeatConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("other-source->target.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getTasksMax(), is(11));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(nullValue()));
        assertThat(kc.getSpec().getConfig(), is(expectedAll2));
    }

    @Test
    public void testConnectorsOnlySome() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .editMirror(0)
                        .withCheckpointConnector(null)
                    .endMirror()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        assertThat(kcs.size(), is(2));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorHeartbeatConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"));
    }

    @Test
    public void testConnectorsPauseState() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withState(ConnectorState.PAUSED)
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .withPause(true)
                        .endCheckpointConnector()
                        .editHeartbeatConnector()
                            .withState(ConnectorState.STOPPED)
                            .withPause(true)
                        .endHeartbeatConnector()
                    .endMirror()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        assertThat(kcs.size(), is(3));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getPause(), is(nullValue()));
        assertThat(kc.getSpec().getState(), is(ConnectorState.PAUSED));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getPause(), is(true));
        assertThat(kc.getSpec().getState(), is(nullValue()));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorHeartbeatConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getPause(), is(true));
        assertThat(kc.getSpec().getState(), is(ConnectorState.STOPPED));
    }

    @Test
    public void testConnectorsWithAutoRestart() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .withNewAutoRestart()
                                .withEnabled()
                                .withMaxRestarts(1874)
                            .endAutoRestart()
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .withNewAutoRestart()
                                .withEnabled(false)
                            .endAutoRestart()
                        .endCheckpointConnector()
                    .endMirror()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        List<KafkaConnector> kcs = connectors.generateConnectorDefinitions();

        assertThat(kcs.size(), is(3));

        KafkaConnector kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorSourceConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorSourceConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorSourceConnector"));
        assertThat(kc.getSpec().getAutoRestart(), is(notNullValue()));
        assertThat(kc.getSpec().getAutoRestart().isEnabled(), is(true));
        assertThat(kc.getSpec().getAutoRestart().getMaxRestarts(), is(1874));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorCheckpointConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorCheckpointConnector"));
        assertThat(kc.getSpec().getAutoRestart(), is(notNullValue()));
        assertThat(kc.getSpec().getAutoRestart().isEnabled(), is(false));

        kc = kcs.stream().filter(k -> k.getMetadata().getName().contains("source->target.MirrorHeartbeatConnector")).findFirst().orElseThrow();
        assertThat(kc.getMetadata().getName(), is("source->target.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getClassName(), is("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"));
        assertThat(kc.getSpec().getAutoRestart(), is(nullValue()));
    }

    @Test
    public void testConnectorConfiguration() {
        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KMM2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(KMM2.getSpec().getMirrors().get(0),
                KMM2.getSpec().getMirrors().get(0).getSourceConnector(),
                KMM2.getSpec().getClusters().get(0),
                KMM2.getSpec().getClusters().get(1));

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("sync.topic.acls.enabled", "false");
        expected.put("topics", "my-topic-.*");
        expected.put("topics.exclude", "exclude-topic-.*");
        expected.put("groups", "my-group-.*");
        expected.put("groups.exclude", "exclude-group-.*");

        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testConnectorConfigurationAlsoWithDeprecatedFields() {
        KafkaMirrorMaker2MirrorSpec mirror = new KafkaMirrorMaker2MirrorSpecBuilder(KMM2.getSpec().getMirrors().get(0))
                .withGroupsBlacklistPattern("other-group-.*")
                .withTopicsBlacklistPattern("other-topic-.*")

                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KMM2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(mirror,
                KMM2.getSpec().getMirrors().get(0).getSourceConnector(),
                KMM2.getSpec().getClusters().get(0),
                KMM2.getSpec().getClusters().get(1));

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("sync.topic.acls.enabled", "false");
        expected.put("topics", "my-topic-.*");
        expected.put("topics.exclude", "exclude-topic-.*");
        expected.put("groups", "my-group-.*");
        expected.put("groups.exclude", "exclude-group-.*");

        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testConnectorConfigurationOnlyWithDeprecatedFields() {
        KafkaMirrorMaker2MirrorSpec mirror = new KafkaMirrorMaker2MirrorSpecBuilder(KMM2.getSpec().getMirrors().get(0))
                .withGroupsBlacklistPattern("other-group-.*")
                .withTopicsBlacklistPattern("other-topic-.*")
                .withTopicsExcludePattern(null)
                .withGroupsExcludePattern(null)
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KMM2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(mirror,
                KMM2.getSpec().getMirrors().get(0).getSourceConnector(),
                KMM2.getSpec().getClusters().get(0),
                KMM2.getSpec().getClusters().get(1));

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("sync.topic.acls.enabled", "false");
        expected.put("topics", "my-topic-.*");
        expected.put("topics.exclude", "other-topic-.*");
        expected.put("groups", "my-group-.*");
        expected.put("groups.exclude", "other-group-.*");

        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testConnectorConfigurationOnlyWithRackAndTracing() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                .withNewRack()
                    .withTopologyKey("my-topology-key")
                .endRack()
                .withNewOpenTelemetryTracing()
                .endOpenTelemetryTracing()
                .endSpec()
                .build();

        KafkaMirrorMaker2Connectors connectors = KafkaMirrorMaker2Connectors.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2);
        Map<String, Object> config = connectors.prepareMirrorMaker2ConnectorConfig(KMM2.getSpec().getMirrors().get(0),
                KMM2.getSpec().getMirrors().get(0).getSourceConnector(),
                KMM2.getSpec().getClusters().get(0),
                KMM2.getSpec().getClusters().get(1));

        Map<String, Object> expected = new TreeMap<>();
        expected.put("source.cluster.alias", "source");
        expected.put("source.cluster.bootstrap.servers", "source:9092");
        expected.put("source.cluster.security.protocol", "PLAINTEXT");
        expected.put("target.cluster.alias", "target");
        expected.put("target.cluster.bootstrap.servers", "target:9092");
        expected.put("target.cluster.security.protocol", "PLAINTEXT");
        expected.put("sync.topic.acls.enabled", "false");
        expected.put("topics", "my-topic-.*");
        expected.put("topics.exclude", "exclude-topic-.*");
        expected.put("groups", "my-group-.*");
        expected.put("groups.exclude", "exclude-group-.*");
        expected.put("consumer.client.rack", "${strimzifile:/tmp/strimzi-connect.properties:consumer.client.rack}");
        expected.put("consumer.interceptor.classes", "io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor");
        expected.put("producer.interceptor.classes", "io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor");

        assertThat(new TreeMap<>(config), is(expected));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithoutAuth() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(config, cluster, PREFIX);

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                                "prefix.security.protocol", "PLAINTEXT",
                                "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithoutAuthWithClusterConfig() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withConfig(Map.of("config.storage.replication.factor", "-1"))
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(config, cluster, PREFIX);

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                                "prefix.security.protocol", "PLAINTEXT",
                                "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092",
                                "prefix.config.storage.replication.factor", "-1"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithTlsAuth() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName("my-secret")
                        .withCertificate("my.crt")
                        .withKey("my.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withNewTls()
                .endTls()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(config, cluster, PREFIX);

        assertThat(config.containsKey("prefix.sasl.jaas.config"), is(false));
        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                                "prefix.security.protocol", "SSL",
                                "prefix.ssl.keystore.location", "/tmp/kafka/clusters/sourceClusterAlias.keystore.p12",
                                "prefix.ssl.keystore.password", PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR,
                                "prefix.ssl.keystore.type", "PKCS12",
                                "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithPlain() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("shaza")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("pa55word")
                    .endPasswordSecret()
                    .endKafkaClientAuthenticationPlain()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(config, cluster, PREFIX);

        String jaasConfig = (String) config.remove("prefix.sasl.jaas.config");
        AppConfigurationEntry configEntry = AuthenticationUtilsTest.parseJaasConfig(jaasConfig);
        assertThat(configEntry.getLoginModuleName(), is("org.apache.kafka.common.security.plain.PlainLoginModule"));
        assertThat(configEntry.getOptions(),
                is(Map.of("username", "shaza",
                        "password", "${strimzidir:/opt/kafka/mm2-password/sourceClusterAlias/my-secret:pa55word}")));

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                        "prefix.security.protocol", "SASL_PLAINTEXT",
                        "prefix.sasl.mechanism", "PLAIN",
                        "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithScram() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("shaza")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("pa55word")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(config, cluster, PREFIX);

        String jaasConfig = (String) config.remove("prefix.sasl.jaas.config");
        AppConfigurationEntry configEntry = AuthenticationUtilsTest.parseJaasConfig(jaasConfig);
        assertThat(configEntry.getLoginModuleName(), is("org.apache.kafka.common.security.scram.ScramLoginModule"));
        assertThat(configEntry.getOptions(),
                is(Map.of("username", "shaza",
                        "password", "${strimzidir:/opt/kafka/mm2-password/sourceClusterAlias/my-secret:pa55word}")));

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                        "prefix.security.protocol", "SASL_PLAINTEXT",
                        "prefix.sasl.mechanism", "SCRAM-SHA-512",
                        "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithOauth() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationOAuth()
                    .withNewAccessToken()
                        .withKey("accessTokenKey")
                        .withSecretName("accessTokenSecretName")
                    .endAccessToken()
                    .withNewClientSecret()
                        .withKey("clientSecretKey")
                        .withSecretName("clientSecretSecretName")
                    .endClientSecret()
                    .withNewPasswordSecret()
                        .withPassword("passwordSecretPassword")
                        .withSecretName("passwordSecretSecretName")
                    .endPasswordSecret()
                    .withNewRefreshToken()
                        .withKey("refreshTokenKey")
                        .withSecretName("refreshTokenSecretName")
                    .endRefreshToken()
                    .withNewClientAssertion()
                        .withKey("clientAssertionKey")
                        .withSecretName("clientAssertionSecretName")
                    .endClientAssertion()
                    .withTlsTrustedCertificates(new CertSecretSourceBuilder().withCertificate("ca.crt").withSecretName("my-oauth-secret").build())
                .endKafkaClientAuthenticationOAuth()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(config, cluster, PREFIX);

        String jaasConfig = (String) config.remove("prefix.sasl.jaas.config");
        AppConfigurationEntry configEntry = AuthenticationUtilsTest.parseJaasConfig(jaasConfig);
        assertThat(configEntry.getLoginModuleName(), is("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"));
        assertThat(configEntry.getOptions(),
                is(Map.of("oauth.client.secret", "${strimzidir:/opt/kafka/mm2-oauth/sourceClusterAlias/clientSecretSecretName:clientSecretKey}",
                        "oauth.access.token", "${strimzidir:/opt/kafka/mm2-oauth/sourceClusterAlias/accessTokenSecretName:accessTokenKey}",
                        "oauth.refresh.token", "${strimzidir:/opt/kafka/mm2-oauth/sourceClusterAlias/refreshTokenSecretName:refreshTokenKey}",
                        "oauth.password.grant.password", "${strimzidir:/opt/kafka/mm2-oauth/sourceClusterAlias/passwordSecretSecretName:passwordSecretPassword}",
                        "oauth.client.assertion", "${strimzidir:/opt/kafka/mm2-oauth/sourceClusterAlias/clientAssertionSecretName:clientAssertionKey}",
                        "oauth.ssl.truststore.location", "/tmp/kafka/clusters/sourceClusterAlias-oauth.truststore.p12",
                        "oauth.ssl.truststore.type", "PKCS12",
                        "oauth.ssl.truststore.password", PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR)));

        assertThat(config,
                is(Map.of("prefix.alias", "sourceClusterAlias",
                        "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092",
                        "prefix.sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                        "prefix.sasl.mechanism", "OAUTHBEARER",
                        "prefix.security.protocol", "SASL_PLAINTEXT")));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithAccessTokenLocationOauth() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationOAuth()
                .withAccessTokenLocation("/var/run/secrets/kubernetes.io/serviceaccount/token")
                .endKafkaClientAuthenticationOAuth()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(config, cluster, PREFIX);

        String jaasConfig = (String) config.remove("prefix.sasl.jaas.config");
        AppConfigurationEntry configEntry = AuthenticationUtilsTest.parseJaasConfig(jaasConfig);
        assertThat(configEntry.getLoginModuleName(), is("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"));
        assertThat(configEntry.getOptions(),
                is(Map.of("oauth.access.token.location", "/var/run/secrets/kubernetes.io/serviceaccount/token")));

        assertThat(config,
                is(Map.of("prefix.alias", "sourceClusterAlias",
                        "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092",
                        "prefix.sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                        "prefix.sasl.mechanism", "OAUTHBEARER",
                        "prefix.security.protocol", "SASL_PLAINTEXT")));
    }

    @Test
    public void testAddClusterToMirrorMaker2ConnectorConfigWithScramAndTlsEncryption() {
        Map<String, Object> config = new HashMap<>();
        KafkaMirrorMaker2ClusterSpec cluster = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("sourceClusterAlias")
                .withBootstrapServers("sourceClusterAlias.sourceNamespace.svc:9092")
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("shaza")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("pa55word")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-tls").withCertificate("ca.crt").build())
                .endTls()
                .build();

        KafkaMirrorMaker2Connectors.addClusterToMirrorMaker2ConnectorConfig(config, cluster, PREFIX);

        String jaasConfig = (String) config.remove("prefix.sasl.jaas.config");
        AppConfigurationEntry configEntry = AuthenticationUtilsTest.parseJaasConfig(jaasConfig);
        assertThat("org.apache.kafka.common.security.scram.ScramLoginModule", is(configEntry.getLoginModuleName()));
        assertThat(configEntry.getOptions(),
                is(Map.of("username", "shaza",
                        "password", "${strimzidir:/opt/kafka/mm2-password/sourceClusterAlias/my-secret:pa55word}")));

        assertThat(new TreeMap<>(config),
                is(new TreeMap<>(Map.of("prefix.alias", "sourceClusterAlias",
                        "prefix.security.protocol", "SASL_SSL",
                       "prefix.ssl.truststore.location", "/tmp/kafka/clusters/sourceClusterAlias.truststore.p12",
                       "prefix.ssl.truststore.password", PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR,
                        "prefix.ssl.truststore.type", "PKCS12",
                        "prefix.sasl.mechanism", "SCRAM-SHA-512",
                        "prefix.bootstrap.servers", "sourceClusterAlias.sourceNamespace.svc:9092"))));
    }
}
