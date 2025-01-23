/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.cruisecontrol.BrokerCapacity;
import io.strimzi.operator.cluster.model.cruisecontrol.Capacity;
import io.strimzi.operator.cluster.model.cruisecontrol.CpuCapacity;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static io.strimzi.operator.cluster.model.CruiseControl.API_HEALTHCHECK_PATH;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters.ANOMALY_DETECTION_CONFIG_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters.DEFAULT_GOALS_CONFIG_KEY;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({
    "checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity"
})
@ParallelSuite
public class CruiseControlTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String REPLICATION_FACTOR = "3";
    private static final String MIN_INSYNC_REPLICAS = "2";
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(CLUSTER_NAME)
                .withLabels(Map.of("my-user-label", "cromulent"))
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("tls")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .build())
                    .withConfig(Map.of(CruiseControl.MIN_INSYNC_REPLICAS, MIN_INSYNC_REPLICAS, KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, REPLICATION_FACTOR))
                .endKafka()
                .withNewCruiseControl()
                .endCruiseControl()
            .endSpec()
            .build();
    private static final Set<NodeRef> NODES = Set.of(
            new NodeRef("my-cluster-brokers-0", 0, "brokers", false, true),
            new NodeRef("my-cluster-brokers-1", 1, "brokers", false, true),
            new NodeRef("my-cluster-brokers-2", 2, "brokers", false, true)
    );
    private static final Map<String, Storage> STORAGE = Map.of("brokers", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build());


    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @ParallelTest
    public void testBrokerCapacity() {
        // Test user defined capacities
        String userDefinedCpuCapacity = "2575m";
        JsonObject expectedCpuCapacity = new CpuCapacity(userDefinedCpuCapacity).getJson();

        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity userDefinedBrokerCapacity = new io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity();
        userDefinedBrokerCapacity.setCpu(userDefinedCpuCapacity);
        userDefinedBrokerCapacity.setInboundNetwork("50000KB/s");
        userDefinedBrokerCapacity.setOutboundNetwork("50000KB/s");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withBrokerCapacity(userDefinedBrokerCapacity)
                    .endCruiseControl()
                .endSpec()
                .build();
        Map<String, Storage> storage = Map.of("brokers", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("50Gi").build(), new PersistentClaimStorageBuilder().withId(1).withSize("60Gi").build()).build());
        Map<String, ResourceRequirements> resources = Map.of("brokers", new ResourceRequirementsBuilder().withRequests(Map.of(Capacity.RESOURCE_TYPE, new Quantity("400m"))).withLimits(Map.of(Capacity.RESOURCE_TYPE, new Quantity("0.5"))).build());
        CruiseControl cc = createCruiseControl(kafka, NODES, storage, resources);

        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        JsonObject capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        JsonArray brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);

        for (Object brokerEntry : brokerEntries) {
            JsonObject brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY);
            Object diskCapacity = brokerCapacity.getValue(Capacity.DISK_KEY);
            JsonObject cpuCapacity  = brokerCapacity.getJsonObject(Capacity.CPU_KEY);

            assertThat(isJBOD(diskCapacity), is(true));
            assertThat(cpuCapacity, is(expectedCpuCapacity));
        }
    }

    @ParallelTest
    public void testBrokerCapacityOverrides() {
        // Test capacity overrides
        String userDefinedCpuCapacity = "2575m";
        String userDefinedCpuCapacityOverride0 = "1.222";
        String inboundNetwork = "50000KB/s";
        String inboundNetworkOverride0 = "25000KB/s";
        String inboundNetworkOverride1 = "10000KiB/s";
        String outboundNetworkOverride1 = "15000KB/s";

        int broker0 = 0;
        int broker1 = 1;
        int broker2 = 2;

        List<Integer> overrideList0 = List.of(broker0, broker1, broker2, broker0);
        List<Integer> overrideList1 = List.of(broker1);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewBrokerCapacity()
                            .withCpu(userDefinedCpuCapacity)
                            .withInboundNetwork(inboundNetwork)
                            .addNewOverride()
                                .withBrokers(overrideList0)
                                .withCpu(userDefinedCpuCapacityOverride0)
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
                .build();
        Map<String, Storage> storage = Map.of("brokers", new PersistentClaimStorageBuilder().withId(0).withSize("50Gi").build());
        Map<String, ResourceRequirements> resources = Map.of("brokers", new ResourceRequirementsBuilder().withRequests(Map.of(Capacity.RESOURCE_TYPE, new Quantity("400m"))).withLimits(Map.of(Capacity.RESOURCE_TYPE, new Quantity("0.5"))).build());
        CruiseControl cc = createCruiseControl(kafka, NODES, storage, resources);

        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        JsonObject capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        JsonArray brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);

        for (Object brokerEntry : brokerEntries) {
            JsonObject brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY);
            Object diskCapacity = brokerCapacity.getValue(Capacity.DISK_KEY);

            assertThat(isJBOD(diskCapacity), is(false));
        }

        JsonObject brokerEntry0 = brokerEntries.getJsonObject(broker0).getJsonObject(Capacity.CAPACITY_KEY);
        assertThat(brokerEntry0.getJsonObject(Capacity.CPU_KEY), is(new CpuCapacity(userDefinedCpuCapacityOverride0).getJson()));
        assertThat(brokerEntry0.getString(Capacity.INBOUND_NETWORK_KEY), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));
        assertThat(brokerEntry0.getString(Capacity.OUTBOUND_NETWORK_KEY), is(BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND));

        // When the same broker id is specified in brokers list of multiple overrides, use the value specified in the first override.
        JsonObject brokerEntry1 = brokerEntries.getJsonObject(broker1).getJsonObject(Capacity.CAPACITY_KEY);
        assertThat(brokerEntry1.getJsonObject(Capacity.CPU_KEY), is(new CpuCapacity(userDefinedCpuCapacityOverride0).getJson()));
        assertThat(brokerEntry1.getString(Capacity.INBOUND_NETWORK_KEY), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));
        assertThat(brokerEntry1.getString(Capacity.OUTBOUND_NETWORK_KEY), is(BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND));

        JsonObject brokerEntry2 = brokerEntries.getJsonObject(broker2).getJsonObject(Capacity.CAPACITY_KEY);
        assertThat(brokerEntry2.getJsonObject(Capacity.CPU_KEY), is(new CpuCapacity(userDefinedCpuCapacityOverride0).getJson()));
        assertThat(brokerEntry2.getString(Capacity.INBOUND_NETWORK_KEY), is(Capacity.getThroughputInKiB(inboundNetworkOverride0)));
    }

    @ParallelTest
    public void testBrokerCapacityGeneratedCpu() {
        String userDefinedCpuCapacity = "2575m";
        String userDefinedCpuCapacityOverride0 = "1.222";
        String inboundNetwork = "50000KB/s";
        String inboundNetworkOverride0 = "25000KB/s";
        String inboundNetworkOverride1 = "10000KiB/s";
        String outboundNetworkOverride1 = "15000KB/s";

        int broker0 = 0;
        int broker1 = 1;
        int broker2 = 2;

        List<Integer> overrideList0 = List.of(broker0, broker1, broker2, broker0);
        List<Integer> overrideList1 = List.of(broker1);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewBrokerCapacity()
                            .withCpu(userDefinedCpuCapacity)
                            .withInboundNetwork(inboundNetwork)
                            .addNewOverride()
                                .withBrokers(overrideList0)
                                .withCpu(userDefinedCpuCapacityOverride0)
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
                .build();
        Map<String, Storage> storage = Map.of("brokers", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("50Gi").build(), new PersistentClaimStorageBuilder().withId(1).withSize("60Gi").build()).build());
        Map<String, ResourceRequirements> resources = Map.of("brokers", new ResourceRequirementsBuilder().withRequests(Map.of(Capacity.RESOURCE_TYPE, new Quantity("500m"))).withLimits(Map.of(Capacity.RESOURCE_TYPE, new Quantity("0.5"))).build());
        CruiseControl cc = createCruiseControl(kafka, NODES, storage, resources);

        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        JsonObject capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        JsonObject expectedCpuCapacity = new CpuCapacity(userDefinedCpuCapacityOverride0).getJson();
        JsonArray brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);
        for (Object brokerEntry : brokerEntries) {
            JsonObject brokerCapacity = ((JsonObject) brokerEntry).getJsonObject(Capacity.CAPACITY_KEY);
            JsonObject cpuCapacity  = brokerCapacity.getJsonObject(Capacity.CPU_KEY);
            assertThat(cpuCapacity, is(expectedCpuCapacity));
        }
    }

    @ParallelTest
    public void testBrokerCapacitiesWithPools() {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-pool1-0", 0, "pool1", false, true),
                new NodeRef("my-cluster-pool1-1", 1, "pool1", false, true),
                new NodeRef("my-cluster-pool1-2", 2, "pool1", false, true),
                new NodeRef("my-cluster-pool2-10", 10, "pool2", false, true),
                new NodeRef("my-cluster-pool2-11", 11, "pool2", false, true),
                new NodeRef("my-cluster-pool2-12", 12, "pool2", false, true)
        );

        Map<String, Storage> storage = new HashMap<>();
        storage.put("pool1", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build());
        storage.put("pool2", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(1).withSize("1Ti").build()).build());

        Map<String, ResourceRequirements> resources = new HashMap<>();
        resources.put("pool1", new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build());
        resources.put("pool2", new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("5"), "memory", new Quantity("20Gi"))).build());

        // Test the capacity
        CruiseControl cc = createCruiseControl(KAFKA, nodes, storage, resources);
        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        JsonObject capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        JsonArray brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);

        assertThat(brokerEntries.size(), is(6));

        // Broker 0
        JsonObject brokerEntry = brokerEntries.getJsonObject(0);
        assertThat(brokerEntry.getInteger("brokerId"), is(0));
        JsonObject brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("4.0"));
        JsonObject brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-0/kafka-log0"), is("102400.0"));

        // Broker 1
        brokerEntry = brokerEntries.getJsonObject(1);
        assertThat(brokerEntry.getInteger("brokerId"), is(1));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("4.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-0/kafka-log1"), is("102400.0"));

        // Broker 2
        brokerEntry = brokerEntries.getJsonObject(2);
        assertThat(brokerEntry.getInteger("brokerId"), is(2));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("4.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-0/kafka-log2"), is("102400.0"));

        // Broker 10
        brokerEntry = brokerEntries.getJsonObject(3);
        assertThat(brokerEntry.getInteger("brokerId"), is(10));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("5.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-1/kafka-log10"), is("1048576.0"));

        // Broker 11
        brokerEntry = brokerEntries.getJsonObject(4);
        assertThat(brokerEntry.getInteger("brokerId"), is(11));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("5.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-1/kafka-log11"), is("1048576.0"));

        // Broker 12
        brokerEntry = brokerEntries.getJsonObject(5);
        assertThat(brokerEntry.getInteger("brokerId"), is(12));
        brokerCpuCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY);
        assertThat(brokerCpuCapacity.getString("num.cores"), is("5.0"));
        brokerDiskCapacity = brokerEntry.getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.DISK_KEY);
        assertThat(brokerDiskCapacity.getString("/var/lib/kafka/data-1/kafka-log12"), is("1048576.0"));
    }

    @ParallelTest
    public void testFromConfigMap() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withImage("my-image:latest")
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        assertThat(cc.namespace, is(NAMESPACE));
        assertThat(cc.cluster, is(CLUSTER_NAME));
        assertThat(cc.getImage(), is("my-image:latest"));
    }

    @ParallelTest
    public void testGenerateDeployment() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewTemplate()
                            .withNewPod()
                                .withTmpDirSizeLimit("100Mi")
                            .endPod()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getMetadata().getName(), is(CruiseControlResources.componentName(CLUSTER_NAME)));
        assertThat(dep.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
        TestUtils.checkOwnerReference(dep, kafka);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        assertThat(containers.size(), is(1));

        // checks on the main Cruise Control container
        Container ccContainer = containers.get(0);
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(15));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(5));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(15));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(5));

        assertThat(ccContainer.getEnv(), is(getExpectedEnvVars()));
        assertThat(ccContainer.getPorts().size(), is(1));
        assertThat(ccContainer.getPorts().get(0).getName(), is(CruiseControl.REST_API_PORT_NAME));
        assertThat(ccContainer.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));

        // Test volumes
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes.size(), is(5));

        Volume volume = volumes.stream().filter(vol -> CruiseControl.TLS_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(CruiseControlResources.secretName(CLUSTER_NAME)));

        volume = volumes.stream().filter(vol -> CruiseControl.TLS_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(AbstractModel.clusterCaCertSecretName(CLUSTER_NAME)));

        volume = volumes.stream().filter(vol -> CruiseControl.API_AUTH_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(CruiseControlResources.apiSecretName(CLUSTER_NAME)));

        volume = volumes.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));
        assertThat(volume.getEmptyDir().getSizeLimit(), is(new Quantity("100Mi")));

        volume = volumes.stream().filter(vol -> CruiseControl.CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getConfigMap().getName(), is(CruiseControlResources.configMapName(CLUSTER_NAME)));

        // Test volume mounts
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(volumesMounts.size(), is(5));

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_CC_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_CA_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.API_AUTH_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.API_AUTH_CONFIG_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.CONFIG_VOLUME_MOUNT));
    }

    @ParallelTest
    public void testEnvVars() {
        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());
        assertThat(cc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());

        Deployment dep = cc.generateDeployment(Map.of(), true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = cc.generateDeployment(Map.of(), true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testContainerTemplateEnvVarsWithKeyConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
        String testEnvTwoValue = "my-special-value";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewTemplate()
                            .withNewCruiseControlContainer()
                                .withEnv(envVar1, envVar2)
                            .endCruiseControlContainer()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        List<EnvVar> envVarList = cc.getEnvVars();
        assertThat(envVarList, hasItems(new EnvVar(testEnvOneKey, testEnvOneValue, null)));
        assertThat(envVarList, hasItems(new EnvVar(testEnvTwoKey, "my-cluster-kafka-bootstrap:9091", null)));
    }

    @ParallelTest
    public void testCruiseControlNotDeployed() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withCruiseControl(null)
                .endSpec()
                .build();
        assertThat(createCruiseControl(kafka, NODES, STORAGE, Map.of()), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateService() {
        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());
        Service svc = cc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(CruiseControlResources.serviceName(CLUSTER_NAME))));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(CruiseControl.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(CruiseControl.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));

        TestUtils.checkOwnerReference(svc, KAFKA);
    }

    @SuppressWarnings("MethodLength")
    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.modifiableMap("l1", "v1", "l2", "v2");
        Map<String, String> depAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.modifiableMap("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = TestUtils.modifiableMap("l5", "v5", "l6", "v6");
        Map<String, String> svcAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> saLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> saAnnotations = Map.of("a7", "v7", "a8", "v8");

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key1")
                                    .withOperator("In")
                                    .withValues("value1", "value2")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> tolerations = singletonList(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();
        
        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();
        
        List<AdditionalVolume> additionalVolumes  = singletonList(new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build());
        
        List<VolumeMount> additionalVolumeMounts = singletonList(new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume-path")
                .withSubPath("def")
                .build());

        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "my-special-value";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewTemplate()
                            .withNewDeployment()
                                .withNewMetadata()
                                    .withLabels(depLabels)
                                    .withAnnotations(depAnnotations)
                                .endMetadata()
                            .endDeployment()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnnotations)
                                .endMetadata()
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withHostAliases(hostAlias1, hostAlias2)
                                .withAffinity(affinity)
                                .withTolerations(tolerations)
                                .withVolumes(additionalVolumes)
                            .endPod()
                            .withNewCruiseControlContainer()
                                .withVolumeMounts(additionalVolumeMounts)
                                .withEnv(envVar1, envVar2)
                            .endCruiseControlContainer()
                            .withNewApiService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                            .endApiService()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnnotations)
                                .endMetadata()
                            .endServiceAccount()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        // Check Deployment
        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        depLabels.putAll(expectedLabels());
        assertThat(dep.getMetadata().getLabels(), is(depLabels));
        assertThat(dep.getMetadata().getAnnotations(), is(depAnnotations));

        // Check Pods
        podLabels.putAll(expectedLabels());
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), is(podLabels));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations(), is(podAnnotations));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));
        assertThat(dep.getSpec().getTemplate().getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(volume -> "secret-volume-name".equals(volume.getName())).iterator().next().getSecret(), is(secret));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().stream().filter(volumeMount -> "secret-volume-name".equals(volumeMount.getName())).iterator().next(), is(additionalVolumeMounts.get(0)));

        // Check Env Vars
        List<EnvVar> envVarList = cc.getEnvVars();
        assertThat(envVarList, hasItems(new EnvVar(testEnvOneKey, testEnvOneValue, null)));
        assertThat(envVarList, hasItems(new EnvVar(testEnvTwoKey, testEnvTwoValue, null)));

        // Check Service
        svcLabels.putAll(expectedLabels());
        Service svc = cc.generateService();
        assertThat(svc.getMetadata().getLabels(), is(svcLabels));
        assertThat(svc.getMetadata().getAnnotations(), is(svcAnnotations));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check Service Account
        ServiceAccount sa = cc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnotations.entrySet()), is(true));
    }

    @ParallelTest
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        Container ccContainer = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(ccContainer.getResources().getLimits(), is(limits));
        assertThat(ccContainer.getResources().getRequests(), is(requests));
    }

    @ParallelTest
    public void testCpuCapacityGeneration() {
        String brokerCpuCapacity = "6.0";
        String brokerCpuCapacityOverride = "2.0";
        String resourceLimitCpu = "3.0";
        String resourceRequestCpu = "4.0";

        Map<String, Storage> storage = Map.of("brokers", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build());

        /* In this test case, when override is set for a broker, it takes precedence over the general broker capacity setting for that
           specific broker, but the general broker capacity takes precedence over the Kafka resource request */
        Map<String, ResourceRequirements> resources = Map.of("brokers", new ResourceRequirementsBuilder().addToLimits("cpu", new Quantity(resourceLimitCpu)).addToRequests("cpu", new Quantity(resourceRequestCpu)).build());

        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacityOne = new BrokerCapacityBuilder()
                .withCpu(brokerCpuCapacity)
                .withOverrides()
                    .addNewOverride()
                        .addToBrokers(0)
                        .withCpu(brokerCpuCapacityOverride)
                    .endOverride()
                .build();

        verifyBrokerCapacity(storage, resources, brokerCapacityOne, brokerCpuCapacityOverride, brokerCpuCapacity, brokerCpuCapacity);

        // In this test case, when override is set for a broker, it takes precedence over the Kafka resource request
        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacityTwo = new BrokerCapacityBuilder()
                .withOverrides()
                    .addNewOverride()
                        .addToBrokers(0)
                        .withCpu(brokerCpuCapacityOverride)
                    .endOverride()
                .build();

        verifyBrokerCapacity(storage, resources, brokerCapacityTwo, brokerCpuCapacityOverride, resourceRequestCpu, resourceRequestCpu);

        /* In this test case, when neither the override nor the CPU resource request are configured but the CPU
           resource limit for CPU is set for the Kafka brokers; therefore, resource limit will be used as the CPU capacity */
        resources = Map.of("brokers", new ResourceRequirementsBuilder().addToLimits("cpu", new Quantity(resourceLimitCpu)).build());

        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacityThree = new BrokerCapacityBuilder().build();

        verifyBrokerCapacity(storage, resources, brokerCapacityThree, resourceLimitCpu, resourceLimitCpu, resourceLimitCpu);

        /* In this test case, when neither the override nor the Kafka resource requests or limits for CPU are configured,
           the CPU capacity will be set to DEFAULT_CPU_CORE_CAPACITY */
        resources = Map.of("brokers", new ResourceRequirementsBuilder().build());

        verifyBrokerCapacity(storage, resources, brokerCapacityThree, BrokerCapacity.DEFAULT_CPU_CORE_CAPACITY, BrokerCapacity.DEFAULT_CPU_CORE_CAPACITY, BrokerCapacity.DEFAULT_CPU_CORE_CAPACITY);
    }

    private void verifyBrokerCapacity(Map<String, Storage> storage,
                                      Map<String, ResourceRequirements> resources,
                                      io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacity,
                                      String brokerOneCpuValue,
                                      String brokerTwoCpuValue,
                                      String brokerThreeCpuValue) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withBrokerCapacity(brokerCapacity)
                    .endCruiseControl()
                .endSpec()
                .build();

        CruiseControl cc = createCruiseControl(kafka, NODES, storage, resources);

        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        JsonObject capacity = new JsonObject(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));
        JsonArray brokerEntries = capacity.getJsonArray(Capacity.CAPACITIES_KEY);

        assertThat(brokerEntries.getJsonObject(0).getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY).getString("num.cores"), is(Matchers.equalTo(brokerOneCpuValue)));
        assertThat(brokerEntries.getJsonObject(1).getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY).getString("num.cores"), is(Matchers.equalTo(brokerTwoCpuValue)));
        assertThat(brokerEntries.getJsonObject(2).getJsonObject(Capacity.CAPACITY_KEY).getJsonObject(Capacity.CPU_KEY).getString("num.cores"), is(Matchers.equalTo(brokerThreeCpuValue)));
    }

    @ParallelTest
    public void testApiSecurity() {
        // Test with security enabled
        testApiSecurity(true, true);

        // Test with security disabled
        testApiSecurity(false, false);
    }

    public void testApiSecurity(Boolean apiAuthEnabled, Boolean apiSslEnabled) {
        String e1Key = CruiseControl.ENV_VAR_API_AUTH_ENABLED;
        String e1Value = apiAuthEnabled.toString();
        EnvVar e1 = new EnvVar(e1Key, e1Value, null);

        String e2Key = CruiseControl.ENV_VAR_API_SSL_ENABLED;
        String e2Value = apiSslEnabled.toString();
        EnvVar e2 = new EnvVar(e2Key, e2Value, null);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withConfig(Map.of(CruiseControlConfigurationParameters.WEBSERVER_SECURITY_ENABLE.getValue(), apiAuthEnabled,
                                CruiseControlConfigurationParameters.WEBSERVER_SSL_ENABLE.getValue(), apiSslEnabled))
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        List<EnvVar> envVarList = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertThat(envVarList.contains(e1),  is(true));
        assertThat(envVarList.contains(e2),  is(true));
    }

    @ParallelTest
    public void testProbeConfiguration()   {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(123)
                            .withTimeoutSeconds(456)
                        .endLivenessProbe()
                        .withNewReadinessProbe()
                           .withInitialDelaySeconds(789)
                           .withTimeoutSeconds(987)
                        .endReadinessProbe()
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        // checks on the main Cruise Control container
        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        Container ccContainer = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(123));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(456));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(789));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(987));
    }

    @ParallelTest
    public void testSecurityContext() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(123L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(456L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(789L));
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());
        cc.securityProvider = new RestrictedPodSecurityProvider();
        cc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
    }

    @ParallelTest
    public void testJvmOptions() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewJvmOptions()
                            .withXms("128m")
                            .withXmx("256m")
                            .withXx(Map.of("InitiatingHeapOccupancyPercent", "36"))
                            .withJavaSystemProperties(new SystemPropertyBuilder().withName("myProperty").withValue("myValue").build(),
                                    new SystemPropertyBuilder().withName("myProperty2").withValue("myValue2").build())
                        .endJvmOptions()
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        EnvVar systemProps = cc.getEnvVars().stream().filter(var -> AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES.equals(var.getName())).findFirst().orElse(null);
        assertThat(systemProps, is(notNullValue()));
        assertThat(systemProps.getValue(), containsString("-DmyProperty=myValue"));
        assertThat(systemProps.getValue(), containsString("-DmyProperty2=myValue2"));

        EnvVar heapOpts = cc.getEnvVars().stream().filter(var -> AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS.equals(var.getName())).findFirst().orElse(null);
        assertThat(heapOpts, is(notNullValue()));
        assertThat(heapOpts.getValue(), containsString("-Xms128m"));
        assertThat(heapOpts.getValue(), containsString("-Xmx256m"));

        EnvVar perfOptions = cc.getEnvVars().stream().filter(var -> AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS.equals(var.getName())).findFirst().orElse(null);
        assertThat(perfOptions, is(notNullValue()));
        assertThat(perfOptions.getValue(), containsString("-XX:InitiatingHeapOccupancyPercent=36"));
    }

    @ParallelTest
    public void testCruiseControlContainerSecurityContext() {
        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withNewTemplate()
                            .withNewCruiseControlContainer()
                                .withSecurityContext(securityContext)
                            .endCruiseControlContainer()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        Deployment dep = cc.generateDeployment(Map.of(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(CruiseControl.CRUISE_CONTROL_CONTAINER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicy() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());

        NetworkPolicy np = cc.generateNetworkPolicy("operator-namespace", null, false);
        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);
        assertThat(rules.size(), is(1));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicyInTheSameNamespace() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .build();
        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeerBuilder()
            .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, format("%s-entity-operator", CLUSTER_NAME)))
            .endPodSelector()
            .build();

        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());

        NetworkPolicy np = cc.generateNetworkPolicy(NAMESPACE, null, true);
        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);
        assertThat(rules.size(), is(2));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
        assertThat(rules.contains(entityOperatorPeer), is(true));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicyWithNamespaceLabels() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector()
                    .withMatchLabels(Collections.singletonMap("nsLabelKey", "nsLabelValue"))
                .endNamespaceSelector()
                .build();
        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeerBuilder()
            .withNewPodSelector()
                .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, format("%s-entity-operator", CLUSTER_NAME)))
            .endPodSelector()
            .withNewNamespaceSelector()
                .withMatchLabels(Collections.singletonMap("nsLabelKey", "nsLabelValue"))
            .endNamespaceSelector()
            .build();

        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());

        NetworkPolicy np = cc.generateNetworkPolicy(null, Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")), true);
        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();
        assertThat(rules.size(), is(2));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
        assertThat(rules.contains(entityOperatorPeer), is(true));
    }

    @ParallelTest
    public void testGoalsCheck() {
        String customGoals = "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
                "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal";

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withConfig(Map.of(DEFAULT_GOALS_CONFIG_KEY.getValue(), customGoals))
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        String anomalyDetectionGoals =  cc.configuration.asOrderedProperties().asMap().get(ANOMALY_DETECTION_CONFIG_KEY.getValue());
        assertThat(anomalyDetectionGoals, is(customGoals));
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withMetricsConfig(new JmxPrometheusExporterMetricsBuilder()
                                .withNewValueFrom()
                                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                                .endValueFrom()
                                .build())
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        assertThat(cc.metrics().isEnabled(), is(true));
        assertThat(cc.metrics().getConfigMapName(), is("my-metrics-configuration"));
        assertThat(cc.metrics().getConfigMapKey(), is("config.yaml"));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());
        assertThat(cc.metrics(), is(nullValue()));
    }

    @ParallelTest
    public void testStrimziReporterMetricsConfig() {
        Kafka kafka = new KafkaBuilder(KAFKA)
            .editSpec()
                .withNewCruiseControl()
                    .withNewStrimziMetricsReporterConfig()
                        .withNewValues()
                            .withAllowList(List.of("kafka_log.*", "kafka_network.*"))
                        .endValues()
                    .endStrimziMetricsReporterConfig()
                .endCruiseControl()
            .endSpec()
            .build();
        
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, 
            () -> createCruiseControl(kafka, NODES, STORAGE, Map.of()));

        assertThat(ex.getMessage(), is("The Strimzi Metrics Reporter is not supported for Cruise Control"));
    }

    @ParallelTest
    public void testDefaultTopicNames() {
        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());

        Map<String, String> expectedTopicConfigs = new HashMap<>();
        expectedTopicConfigs.put(CruiseControlConfigurationParameters.PARTITION_METRIC_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_PARTITION_METRIC_TOPIC_NAME);
        expectedTopicConfigs.put(CruiseControlConfigurationParameters.BROKER_METRIC_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_BROKER_METRIC_TOPIC_NAME);
        expectedTopicConfigs.put(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue(), CruiseControlConfigurationParameters.DEFAULT_METRIC_REPORTER_TOPIC_NAME);

        expectedTopicConfigs.forEach((configParam, name) -> assertThat(cc.configuration.getConfiguration(), containsString(String.format("%s=%s", configParam, name))));
    }

    @ParallelTest
    public void testCustomTopicNames() {
        Map<String, Object> customConfig = new HashMap<>();
        customConfig.put(CruiseControlConfigurationParameters.PARTITION_METRIC_TOPIC_NAME.getValue(), "partition-topic");
        customConfig.put(CruiseControlConfigurationParameters.BROKER_METRIC_TOPIC_NAME.getValue(), "broker-topic");
        customConfig.put(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue(), "metric-reporter-topic");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withConfig(customConfig)
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());
        customConfig.forEach((configParam, name) -> assertThat(cc.configuration.getConfiguration(), containsString(String.format("%s=%s", configParam, name))));
    }

    @ParallelTest
    public void testDefaultSampleStoreTopicReplicationFactorConfig() {
        // Test that the replication factor of Cruise Control's sample store topic is set to Kafka cluster's `default.replication.factor`
        // when not explicitly set in Cruise Control config
        CruiseControl cc = createCruiseControl(KAFKA, NODES, STORAGE, Map.of());

        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        assertThat(parsePropertiesString(configMap.getData().get(CruiseControl.SERVER_CONFIG_FILENAME)).getProperty(CruiseControlConfigurationParameters.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR.getValue()), is("3"));
    }

    @ParallelTest
    public void testCustomSampleStoreTopicReplicationFactorConfig() {
        // Test that the replication factor of Cruise Control's sample store topic is set to value set in Cruise Control config
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                        .withConfig(Map.of(CruiseControlConfigurationParameters.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR.getValue(), 7))
                    .endCruiseControl()
                .endSpec()
                .build();
        CruiseControl cc = createCruiseControl(kafka, NODES, STORAGE, Map.of());

        ConfigMap configMap = cc.generateConfigMap(new MetricsAndLogging(null, null));
        assertThat(parsePropertiesString(configMap.getData().get(CruiseControl.SERVER_CONFIG_FILENAME)).getProperty(CruiseControlConfigurationParameters.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR.getValue()), is("7"));
    }

    ////////////////////
    // Utility methods
    ////////////////////

    private static Properties parsePropertiesString(String kafkaPropertiesString) {
        Properties properties = new Properties();
        try (StringReader reader = new StringReader(kafkaPropertiesString)) {
            properties.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    private CruiseControl createCruiseControl(Kafka kafka, Set<NodeRef> nodes, Map<String, Storage> storageMap, Map<String, ResourceRequirements> resourceRequirementsMap) {
        return CruiseControl
                .fromCrd(
                        Reconciliation.DUMMY_RECONCILIATION,
                        kafka,
                        VERSIONS,
                        nodes,
                        storageMap,
                        resourceRequirementsMap,
                        SHARED_ENV_PROVIDER
                );
    }

    private Map<String, String> expectedLabels(String name) {
        return TestUtils.modifiableMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, CruiseControl.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, CruiseControl.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, CLUSTER_NAME,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + CLUSTER_NAME,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels() {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels() {
        return expectedLabels(CruiseControlResources.componentName(CLUSTER_NAME));
    }

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_CRUISE_CONTROL_JMX_EXPORTER_ENABLED).withValue(Boolean.toString(CruiseControl.DEFAULT_CRUISE_CONTROL_METRICS_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS).withValue(KafkaResources.bootstrapServiceName(CLUSTER_NAME) + ":" + KafkaCluster.REPLICATION_PORT).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_SSL_ENABLED).withValue(Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SSL_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_AUTH_ENABLED).withValue(Boolean.toString(CruiseControlConfigurationParameters.DEFAULT_WEBSERVER_SECURITY_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_HEALTHCHECK_USERNAME).withValue(CruiseControlApiProperties.HEALTHCHECK_USERNAME).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_PORT).withValue(Integer.toString(CruiseControl.REST_API_PORT)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_API_HEALTHCHECK_PATH).withValue(API_HEALTHCHECK_PATH).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_KAFKA_HEAP_OPTS).withValue("-Xms" + JvmOptionUtils.DEFAULT_JVM_XMS).build());
        return expected;
    }

    private static boolean isJBOD(Object diskCapacity) {
        return diskCapacity instanceof JsonObject;
    }
}
