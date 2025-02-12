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
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTermBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.ProbeBuilder;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationOpaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.jmx.JmxModel;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.jmx.JmxModel.JMX_PORT;
import static io.strimzi.operator.cluster.model.jmx.JmxModel.JMX_PORT_NAME;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
@ParallelSuite
public class KafkaClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static String NAMESPACE = "test";
    private final static String CLUSTER = "foo";
    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                .endKafka()
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_MIXED = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("mixed")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static Map<Integer, Map<String, String>> ADVERTISED_HOSTNAMES = Map.of(
            3, Map.of("PLAIN_9092", "mixed-3", "TLS_9093", "mixed-3"),
            4, Map.of("PLAIN_9092", "mixed-4", "TLS_9093", "mixed-4"),
            5, Map.of("PLAIN_9092", "broker-5", "TLS_9093", "broker-5"),
            6, Map.of("PLAIN_9092", "broker-6", "TLS_9093", "broker-6"),
            7, Map.of("PLAIN_9092", "broker-7", "TLS_9093", "broker-7")
    );
    private final static Map<Integer, Map<String, String>> ADVERTISED_PORTS = Map.of(
            3, Map.of("PLAIN_9092", "9092", "TLS_9093", "10003"),
            4, Map.of("PLAIN_9092", "9092", "TLS_9093", "10004"),
            5, Map.of("PLAIN_9092", "9092", "TLS_9093", "10005"),
            6, Map.of("PLAIN_9092", "9092", "TLS_9093", "10006"),
            7, Map.of("PLAIN_9092", "9092", "TLS_9093", "10007")
    );

    private static final List<KafkaPool> POOLS = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
    private final static KafkaCluster KC = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

    //////////
    // Utility methods
    //////////
    private Map<String, String> expectedSelectorLabels()    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
                Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER),
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
    }

    private Map<String, String> expectedBrokerSelectorLabels()    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
                Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER),
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_BROKER_ROLE_LABEL, "true");
    }

    private void checkHeadlessService(Service headless) {
        assertThat(headless.getMetadata().getName(), is(KafkaResources.brokersServiceName(CLUSTER)));
        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getClusterIP(), is("None"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(5));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(KafkaCluster.CONTROLPLANE_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.CONTROLPLANE_PORT));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(headless.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(KafkaCluster.KAFKA_AGENT_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(KafkaCluster.KAFKA_AGENT_PORT));
        assertThat(headless.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(3).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(3).getPort(), is(9092));
        assertThat(headless.getSpec().getPorts().get(3).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(4).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(4).getPort(), is(9093));
        assertThat(headless.getSpec().getPorts().get(4).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(headless.getSpec().getIpFamilies(), is(nullValue()));

        assertThat(headless.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(false));
    }

    private List<Secret> generateBrokerSecrets(Set<String> externalBootstrapAddress, Map<Integer, Set<String>> externalAddresses) {
        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), CLUSTER, null, null);
        clusterCa.createRenewOrReplace(NAMESPACE, Map.of(), Map.of(), Map.of(), null, true);
        ClientsCa clientsCa = new ClientsCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), null, null, null, null, 365, 30, true, CertificateExpirationPolicy.RENEW_CERTIFICATE);
        clientsCa.createRenewOrReplace(NAMESPACE, Map.of(), Map.of(), Map.of(), null, true);

        return KC.generateCertificatesSecrets(clusterCa, clientsCa, List.of(), externalBootstrapAddress, externalAddresses, true);
    }

    //////////
    // Tests
    //////////

    @ParallelTest
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm("{\"animal\":\"wombat\"}", "kafka-metrics-config", "kafka-metrics-config.yml");
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("kafka-metrics-config.yml", "kafka-metrics-config", false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(metricsCm, null), ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(cms.size(), is(8));

        for (ConfigMap cm : cms)    {
            if (cm.getMetadata().getName().contains("controllers")) {
                TestUtils.checkOwnerReference(cm, POOL_CONTROLLERS);
            } else if (cm.getMetadata().getName().contains("mixed")) {
                TestUtils.checkOwnerReference(cm, POOL_MIXED);
            } else {
                TestUtils.checkOwnerReference(cm, POOL_BROKERS);
            }

            assertThat(cm.getData().get(JmxPrometheusExporterModel.CONFIG_MAP_KEY), is("{\"animal\":\"wombat\"}"));
        }
    }

    @ParallelTest
    public void testStrimziMetricsReporterConfigs() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewStrimziMetricsReporterConfig()
                            .withNewValues()
                                .withAllowList("kafka_log.*", "kafka_network.*")
                            .endValues()
                        .endStrimziMetricsReporterConfig()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(new MetricsAndLogging(null, null), ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(cms.size(), is(8));

        for (ConfigMap cm : cms) {
            assertThat(cm.getData().toString(), containsString("kafka.metrics.reporters=io.strimzi.kafka.metrics.YammerPrometheusMetricsReporter"));
            assertThat(cm.getData().toString(), containsString("prometheus.metrics.reporter.listener.enable=true"));
            assertThat(cm.getData().toString(), containsString("prometheus.metrics.reporter.listener=http://0.0.0.0:9404"));
            assertThat(cm.getData().toString(), containsString("allowlist=kafka_log.*,kafka_network.*"));
        }
    }

    @ParallelTest
    public void testStrimziMetricsReporterNetworkPolicy() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewStrimziMetricsReporterConfig()
                            .withNewValues()
                                .withAllowList("kafka_log.*", "kafka_network.*")
                            .endValues()
                        .endStrimziMetricsReporterConfig()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(null, null);
        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(StrimziMetricsReporterModel.METRICS_PORT))).collect(Collectors.toList());

        assertThat(rules.size(), is(1));
    }

    @ParallelTest
    public void  testJavaSystemProperties() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewJvmOptions()
                            .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                    new SystemPropertyBuilder().withName("something.else").withValue("42").build()))
                        .endJvmOptions()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            final Optional<EnvVar> envVarValue = pod.getSpec().getContainers().stream().findAny().orElseThrow().getEnv().stream().filter(env -> env.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findAny();
            assertThat(envVarValue.isPresent(), is(true));
        }));
    }

    @ParallelTest
    public void  testJavaSystemPropertiesInNodePools() {
        KafkaNodePool controllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("42").build()))
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("1874").build()))
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("1919").build()))
                    .endJvmOptions()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(controllers, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            final Optional<EnvVar> envVarValue = pod.getSpec().getContainers().stream().findAny().orElseThrow().getEnv().stream().filter(env -> env.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findAny();
            assertThat(envVarValue.isPresent(), is(true));
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=42"));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=1874"));
            } else {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=1919"));
            }
        }));
    }

    @ParallelTest
    public void  testJavaSystemPropertiesInNodePoolsAndKafka() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewJvmOptions()
                            .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                    new SystemPropertyBuilder().withName("something.else").withValue("42").build()))
                        .endJvmOptions()
                    .endKafka()
                .endSpec()
                .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("1874").build()))
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewJvmOptions()
                        .withJavaSystemProperties(List.of(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build(),
                                new SystemPropertyBuilder().withName("something.else").withValue("1919").build()))
                    .endJvmOptions()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            final Optional<EnvVar> envVarValue = pod.getSpec().getContainers().stream().findAny().orElseThrow().getEnv().stream().filter(env -> env.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findAny();
            assertThat(envVarValue.isPresent(), is(true));
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=42"));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=1874"));
            } else {
                assertThat(envVarValue.get().getValue(), is("-Djavax.net.debug=verbose -Dsomething.else=1919"));
            }
        }));
    }

    @ParallelTest
    public void testCustomImage() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withImage("my-image:my-tag")
                        .withBrokerRackInitImage("my-init-image:my-init-tag")
                        .withNewRack()
                            .withTopologyKey("rack-key")
                        .endRack()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Check container
            assertThat(pod.getSpec().getContainers().stream().findAny().orElseThrow().getImage(), is("my-image:my-tag"));

            // Check Init container
            if (!pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(pod.getSpec().getInitContainers().stream().findAny().orElseThrow().getImage(), is("my-init-image:my-init-tag"));
            }
        }));
    }

    @ParallelTest
    public void testHealthChecks() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withLivenessProbe(new ProbeBuilder()
                                .withInitialDelaySeconds(1)
                                .withPeriodSeconds(2)
                                .withTimeoutSeconds(3)
                                .withSuccessThreshold(4)
                                .withFailureThreshold(5)
                                .build())
                        .withReadinessProbe(new ProbeBuilder()
                                .withInitialDelaySeconds(6)
                                .withPeriodSeconds(7)
                                .withTimeoutSeconds(8)
                                .withSuccessThreshold(9)
                                .withFailureThreshold(10)
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();

            assertThat(cont.getLivenessProbe().getInitialDelaySeconds(), is(1));
            assertThat(cont.getLivenessProbe().getPeriodSeconds(), is(2));
            assertThat(cont.getLivenessProbe().getTimeoutSeconds(), is(3));
            assertThat(cont.getLivenessProbe().getSuccessThreshold(), is(4));
            assertThat(cont.getLivenessProbe().getFailureThreshold(), is(5));

            assertThat(cont.getReadinessProbe().getInitialDelaySeconds(), is(6));
            assertThat(cont.getReadinessProbe().getPeriodSeconds(), is(7));
            assertThat(cont.getReadinessProbe().getTimeoutSeconds(), is(8));
            assertThat(cont.getReadinessProbe().getSuccessThreshold(), is(9));
            assertThat(cont.getReadinessProbe().getFailureThreshold(), is(10));
        }));
    }

    @ParallelTest
    public void testInitContainerTemplate() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        // Test env var conflict
        ContainerEnvVar envVar3 = new ContainerEnvVar();
        String testEnvThreeKey = KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME;
        String testEnvThreeValue = "test.env.three";
        envVar3.setName(testEnvThreeKey);
        envVar3.setValue(testEnvThreeValue);

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        // Set a node-port listener to force init-container to be templated
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                        .withNewTemplate()
                            .withNewInitContainer()
                                .withEnv(envVar1, envVar2, envVar3)
                                .withSecurityContext(securityContext)
                                .withVolumeMounts(additionalVolumeMount)
                            .endInitContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));
            } else {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(securityContext));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(2));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
                assertThat(initCont.getVolumeMounts().get(1).getName(), is("secret-volume-name"));
                assertThat(initCont.getVolumeMounts().get(1).getMountPath(), is("/mnt/secret-volume"));
            }
        }));
    }

    @ParallelTest
    public void testInitContainerTemplateInKafkaAndNodePool() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        // Test env var conflict
        ContainerEnvVar envVar3 = new ContainerEnvVar();
        String testEnvThreeKey = KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME;
        String testEnvThreeValue = "test.env.three";
        envVar3.setName(testEnvThreeKey);
        envVar3.setValue(testEnvThreeValue);

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        // Set a node-port listener to force init-container to be templated
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                        .withNewTemplate()
                            .withNewInitContainer()
                                .withEnv(envVar1, envVar2, envVar3)
                                .withSecurityContext(securityContext)
                                .withVolumeMounts(additionalVolumeMount)
                            .endInitContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar2, envVar3)
                            .withSecurityContext(securityContext)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar1, envVar3)
                            .withVolumeMounts(additionalVolumeMount)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(securityContext));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(1));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
            } else {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(Matchers.nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(2));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
                assertThat(initCont.getVolumeMounts().get(1).getName(), is("secret-volume-name"));
                assertThat(initCont.getVolumeMounts().get(1).getMountPath(), is("/mnt/secret-volume"));
            }
        }));
    }

    @ParallelTest
    public void testInitContainerTemplateInNodePool() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        // Test env var conflict
        ContainerEnvVar envVar3 = new ContainerEnvVar();
        String testEnvThreeKey = KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME;
        String testEnvThreeValue = "test.env.three";
        envVar3.setName(testEnvThreeKey);
        envVar3.setValue(testEnvThreeValue);

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        // Set a node-port listener to force init-container to be templated
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool controllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar1, envVar2, envVar3)
                            .withSecurityContext(securityContext)
                            .withVolumeMounts(additionalVolumeMount)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar2, envVar3)
                            .withSecurityContext(securityContext)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewInitContainer()
                            .withEnv(envVar1, envVar3)
                            .withVolumeMounts(additionalVolumeMount)
                        .endInitContainer()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(controllers, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(securityContext));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(1));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
            } else {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getName(), is(KafkaCluster.INIT_NAME));
                assertThat(initCont.getSecurityContext(), is(Matchers.nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                assertThat(initCont.getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(initCont.getEnv().stream().filter(e -> KafkaCluster.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));

                assertThat(initCont.getVolumeMounts().size(), is(2));
                assertThat(initCont.getVolumeMounts().get(0).getName(), is("rack-volume"));
                assertThat(initCont.getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
                assertThat(initCont.getVolumeMounts().get(1).getName(), is("secret-volume-name"));
                assertThat(initCont.getVolumeMounts().get(1).getMountPath(), is("/mnt/secret-volume"));
            }
        }));
    }

    @ParallelTest
    public void testGenerateService() {
        Service clusterIp = KC.generateService();

        assertThat(clusterIp.getSpec().getType(), is("ClusterIP"));
        assertThat(clusterIp.getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(clusterIp.getSpec().getPorts().size(), is(3));
        assertThat(clusterIp.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(clusterIp.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(1).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(1).getPort(), is(9092));
        assertThat(clusterIp.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(2).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(2).getPort(), is(9093));
        assertThat(clusterIp.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(clusterIp.getSpec().getIpFamilies(), is(nullValue()));
        assertThat(clusterIp.getSpec().getPublishNotReadyAddresses(), is(nullValue()));
        
        assertThat(clusterIp.getMetadata().getAnnotations(), hasKey("strimzi.io/discovery"));        
        JsonArray annotation = new JsonArray(clusterIp.getMetadata().getAnnotations().get("strimzi.io/discovery"));
        JsonObject listener1 = annotation.getJsonObject(0);
        assertThat(listener1.getString("port"), is("9092"));
        assertThat(listener1.getString("tls"), is("false"));
        assertThat(listener1.getString("protocol"), is("kafka"));
        assertThat(listener1.getString("auth"), is("none"));
        JsonObject listener2 = annotation.getJsonObject(1);
        assertThat(listener2.getString("port"), is("9093"));
        assertThat(listener2.getString("tls"), is("true"));
        assertThat(listener2.getString("protocol"), is("kafka"));
        assertThat(listener2.getString("auth"), is("none"));

        assertThat(clusterIp.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(true));
        assertThat(clusterIp.getMetadata().getLabels().get(Labels.STRIMZI_DISCOVERY_LABEL), is("true"));

        TestUtils.checkOwnerReference(clusterIp, KAFKA);
    }

    @ParallelTest
    public void testGenerateServiceWithoutMetrics() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetricsConfig(null)
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        Service clusterIp = kc.generateService();

        assertThat(clusterIp.getSpec().getType(), is("ClusterIP"));
        assertThat(clusterIp.getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(clusterIp.getSpec().getPorts().size(), is(3));
        assertThat(clusterIp.getSpec().getPorts().get(0).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(clusterIp.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(1).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(1).getPort(), is(9092));
        assertThat(clusterIp.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(clusterIp.getSpec().getPorts().get(2).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(clusterIp.getSpec().getPorts().get(2).getPort(), is(9093));
        assertThat(clusterIp.getSpec().getPorts().get(2).getProtocol(), is("TCP"));

        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/port"), is(false));
        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/scrape"), is(false));
        assertThat(clusterIp.getMetadata().getAnnotations().containsKey("prometheus.io/path"), is(false));

        assertThat(clusterIp.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(true));
        assertThat(clusterIp.getMetadata().getLabels().get(Labels.STRIMZI_DISCOVERY_LABEL), is("true"));

        TestUtils.checkOwnerReference(clusterIp, KAFKA);
    }

    @ParallelTest
    public void testGenerateHeadlessServiceWithJmxMetrics() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        Service headless = kc.generateHeadlessService();

        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(6));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(KafkaCluster.CONTROLPLANE_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(KafkaCluster.CONTROLPLANE_PORT));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(KafkaCluster.REPLICATION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(KafkaCluster.REPLICATION_PORT));
        assertThat(headless.getSpec().getPorts().get(1).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(KafkaCluster.KAFKA_AGENT_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(KafkaCluster.KAFKA_AGENT_PORT));
        assertThat(headless.getSpec().getPorts().get(2).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(3).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(3).getPort(), is(9092));
        assertThat(headless.getSpec().getPorts().get(3).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(4).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(4).getPort(), is(9093));
        assertThat(headless.getSpec().getPorts().get(4).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getPorts().get(5).getName(), is(JmxModel.JMX_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(5).getPort(), is(JmxModel.JMX_PORT));
        assertThat(headless.getSpec().getPorts().get(5).getProtocol(), is("TCP"));

        assertThat(headless.getMetadata().getLabels().containsKey(Labels.STRIMZI_DISCOVERY_LABEL), is(false));

        TestUtils.checkOwnerReference(headless, KAFKA);
    }

    @ParallelTest
    public void testExposesJmxContainerPortWhenJmxEnabled() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder().build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            ContainerPort jmxPort = cont.getPorts().stream().filter(port -> JMX_PORT_NAME.equals(port.getName())).findFirst().orElseThrow();
            assertThat(jmxPort.getContainerPort(), is(JMX_PORT));
        }));
    }

    @ParallelTest
    public void testWithJmxMetricsExporterContainerPorts() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.INTERNAL).withTls().build(),
                                new GenericKafkaListenerBuilder().withName("external").withPort(9094).withType(KafkaListenerType.NODEPORT).withTls().build())
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef("metrics-cm", "metrics.json", false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endKafka()
                .endSpec()
                .build();

        assertExpectedContainerPortsAreSet(kafka);
    }

    @ParallelTest
    public void testWithStrimziMetricsReporterContainerPorts() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.INTERNAL).withTls().build(),
                                new GenericKafkaListenerBuilder().withName("external").withPort(9094).withType(KafkaListenerType.NODEPORT).withTls().build())
                        .withNewStrimziMetricsReporterConfig()
                            .withNewValues()
                                .withAllowList("kafka_log.*", "kafka_network.*")
                            .endValues()
                        .endStrimziMetricsReporterConfig()
                    .endKafka()
                .endSpec()
                .build();

        assertExpectedContainerPortsAreSet(kafka);
    }

    private void assertExpectedContainerPortsAreSet(Kafka kafka) {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(ports.size(), is(3));
                assertThat(ports.get(0).getContainerPort(), is(8443));
                assertThat(ports.get(1).getContainerPort(), is(9090));
                assertThat(ports.get(2).getContainerPort(), is(9404));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(ports.size(), is(6));
                assertThat(ports.get(0).getContainerPort(), is(8443));
                assertThat(ports.get(1).getContainerPort(), is(9090));
                assertThat(ports.get(2).getContainerPort(), is(9091));
                assertThat(ports.get(3).getContainerPort(), is(9093));
                assertThat(ports.get(4).getContainerPort(), is(9094));
                assertThat(ports.get(5).getContainerPort(), is(9404));
            } else {
                assertThat(ports.size(), is(5));
                assertThat(ports.get(0).getContainerPort(), is(8443));
                assertThat(ports.get(1).getContainerPort(), is(9091));
                assertThat(ports.get(2).getContainerPort(), is(9093));
                assertThat(ports.get(3).getContainerPort(), is(9094));
                assertThat(ports.get(4).getContainerPort(), is(9404));
            }
        }));
    }

    @ParallelTest
    public void testAuxiliaryResourcesTemplate() {
        Map<String, String> pdbLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> pdbAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> crbLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> crbAnnotations = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> saAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> jmxLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> jmxAnnotations = Map.of("a7", "v7", "a8", "v8");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("external2")
                                    .withPort(9095)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .build())
                        .withNewJmxOptions()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                        .endJmxOptions()
                        .withNewTemplate()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withLabels(pdbLabels)
                                    .withAnnotations(pdbAnnotations)
                                .endMetadata()
                            .endPodDisruptionBudget()
                            .withNewClusterRoleBinding()
                                .withNewMetadata()
                                    .withLabels(crbLabels)
                                    .withAnnotations(crbAnnotations)
                                .endMetadata()
                            .endClusterRoleBinding()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnnotations)
                                .endMetadata()
                            .endServiceAccount()
                            .withNewJmxSecret()
                                .withNewMetadata()
                                    .withLabels(jmxLabels)
                                    .withAnnotations(jmxAnnotations)
                                .endMetadata()
                            .endJmxSecret()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));

        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kc.generateClusterRoleBinding("namespace");
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnnotations.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnnotations.entrySet()), is(true));

        // Check JMX Secret
        Secret jmxSecret = kc.jmx().jmxSecret(null);
        assertThat(jmxSecret.getMetadata().getLabels().entrySet().containsAll(jmxLabels.entrySet()), is(true));
        assertThat(jmxSecret.getMetadata().getAnnotations().entrySet().containsAll(jmxAnnotations.entrySet()), is(true));
    }

    @ParallelTest
    public void testJmxSecret() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withJmxOptions(new KafkaJmxOptionsBuilder()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder()
                                  .build())
                            .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        Secret jmxSecret = kc.jmx().jmxSecret(null);

        assertThat(jmxSecret.getData(), hasKey("jmx-username"));
        assertThat(jmxSecret.getData(), hasKey("jmx-password"));

        Secret newJmxSecret = kc.jmx().jmxSecret(jmxSecret);

        assertThat(newJmxSecret.getData(), hasKey("jmx-username"));
        assertThat(newJmxSecret.getData(), hasKey("jmx-password"));
        assertThat(newJmxSecret.getData().get("jmx-username"), is(jmxSecret.getData().get("jmx-username")));
        assertThat(newJmxSecret.getData().get("jmx-password"), is(jmxSecret.getData().get("jmx-password")));
    }

    @ParallelTest
    public void testGenerateHeadlessService() {
        Service headless = KC.generateHeadlessService();
        checkHeadlessService(headless);
        TestUtils.checkOwnerReference(headless, KAFKA);
    }

    @ParallelTest
    public void testPerBrokerConfiguration() {
        String config = KC.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(config, CoreMatchers.containsString("node.id=1"));
        assertThat(config, CoreMatchers.containsString("log.dirs=/var/lib/kafka/data-0/kafka-log1"));
        assertThat(config, CoreMatchers.containsString("\nlisteners=CONTROLPLANE-9090://0.0.0.0:9090\n"));
        assertThat(config, CoreMatchers.containsString("advertised.listeners=CONTROLPLANE-9090://foo-controllers-1.foo-kafka-brokers.test.svc:9090\n"));
        assertThat(config, CoreMatchers.containsString("process.roles=controller\n"));
        assertThat(config, CoreMatchers.containsString("controller.quorum.voters=0@foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local:9090,1@foo-controllers-1.foo-kafka-brokers.test.svc.cluster.local:9090,2@foo-controllers-2.foo-kafka-brokers.test.svc.cluster.local:9090,3@foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local:9090,4@foo-mixed-4.foo-kafka-brokers.test.svc.cluster.local:9090\n"));

        config = KC.generatePerBrokerConfiguration(4, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(config, CoreMatchers.containsString("node.id=4"));
        assertThat(config, CoreMatchers.containsString("log.dirs=/var/lib/kafka/data-0/kafka-log4"));
        assertThat(config, CoreMatchers.containsString("\nlisteners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092,TLS-9093://0.0.0.0:9093\n"));
        assertThat(config, CoreMatchers.containsString("advertised.listeners=CONTROLPLANE-9090://foo-mixed-4.foo-kafka-brokers.test.svc:9090,REPLICATION-9091://foo-mixed-4.foo-kafka-brokers.test.svc:9091,PLAIN-9092://mixed-4:9092,TLS-9093://mixed-4:10004\n"));
        assertThat(config, CoreMatchers.containsString("process.roles=broker,controller\n"));
        assertThat(config, CoreMatchers.containsString("controller.quorum.voters=0@foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local:9090,1@foo-controllers-1.foo-kafka-brokers.test.svc.cluster.local:9090,2@foo-controllers-2.foo-kafka-brokers.test.svc.cluster.local:9090,3@foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local:9090,4@foo-mixed-4.foo-kafka-brokers.test.svc.cluster.local:9090\n"));

        config = KC.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(config, CoreMatchers.containsString("node.id=6"));
        assertThat(config, CoreMatchers.containsString("log.dirs=/var/lib/kafka/data-0/kafka-log6"));
        assertThat(config, CoreMatchers.containsString("\nlisteners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092,TLS-9093://0.0.0.0:9093\n"));
        assertThat(config, CoreMatchers.containsString("advertised.listeners=REPLICATION-9091://foo-brokers-6.foo-kafka-brokers.test.svc:9091,PLAIN-9092://broker-6:9092,TLS-9093://broker-6:10006\n"));
        assertThat(config, CoreMatchers.containsString("process.roles=broker\n"));
        assertThat(config, CoreMatchers.containsString("controller.quorum.voters=0@foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local:9090,1@foo-controllers-1.foo-kafka-brokers.test.svc.cluster.local:9090,2@foo-controllers-2.foo-kafka-brokers.test.svc.cluster.local:9090,3@foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local:9090,4@foo-mixed-4.foo-kafka-brokers.test.svc.cluster.local:9090\n"));
    }

    @ParallelTest
    public void testPerBrokerConfigMaps() {
        MetricsAndLogging metricsAndLogging = new MetricsAndLogging(null, null);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, "dummy-cluster-id", SHARED_ENV_PROVIDER);
        List<ConfigMap> cms = kc.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);

        assertThat(cms.size(), is(8));

        for (ConfigMap cm : cms)    {
            assertThat(cm.getMetadata().getName(), startsWith("foo-"));

            if (cm.getMetadata().getName().contains("controllers")) {
                assertThat(cm.getData().size(), is(5));
                assertThat(cm.getData().get(LoggingModel.LOG4J1_CONFIG_MAP_KEY), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), CoreMatchers.containsString("process.roles=controller\n"));
                assertThat(cm.getData().get(KafkaCluster.BROKER_LISTENERS_FILENAME), is(nullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is("dummy-cluster-id"));
            } else if (cm.getMetadata().getName().contains("brokers")) {
                assertThat(cm.getData().size(), is(5));
                assertThat(cm.getData().get(LoggingModel.LOG4J1_CONFIG_MAP_KEY), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), CoreMatchers.containsString("process.roles=broker\n"));
                assertThat(cm.getData().get(KafkaCluster.BROKER_LISTENERS_FILENAME), is("PLAIN_9092 TLS_9093"));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is("dummy-cluster-id"));
            } else {
                assertThat(cm.getData().size(), is(5));
                assertThat(cm.getData().get(LoggingModel.LOG4J1_CONFIG_MAP_KEY), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), is(notNullValue()));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CONFIGURATION_FILENAME), CoreMatchers.containsString("process.roles=broker,controller\n"));
                assertThat(cm.getData().get(KafkaCluster.BROKER_LISTENERS_FILENAME), is("PLAIN_9092 TLS_9093"));
                assertThat(cm.getData().get(KafkaCluster.BROKER_CLUSTER_ID_FILENAME), is("dummy-cluster-id"));
            }
        }
    }

    @ParallelTest
    public void testGenerateBrokerSecret() throws CertificateParsingException {
        List<Secret> secrets = generateBrokerSecrets(null, Map.of());
        Set<String> secretDataKeys = new HashSet<>();
        Map<String, Secret> secretMap = new HashMap<>();
        secrets.forEach(secret -> {
            secretDataKeys.addAll(secret.getData().keySet());
            secretMap.put(secret.getMetadata().getName(), secret);
        });
        assertThat(secretDataKeys, is(Set.of(
                "foo-controllers-0.crt",  "foo-controllers-0.key",
                "foo-controllers-1.crt", "foo-controllers-1.key",
                "foo-controllers-2.crt", "foo-controllers-2.key",
                "foo-mixed-3.crt",  "foo-mixed-3.key",
                "foo-mixed-4.crt", "foo-mixed-4.key",
                "foo-brokers-5.crt",  "foo-brokers-5.key",
                "foo-brokers-6.crt", "foo-brokers-6.key",
                "foo-brokers-7.crt", "foo-brokers-7.key")));

        X509Certificate cert = Ca.cert(secretMap.get("foo-controllers-0"), "foo-controllers-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));

        cert = Ca.cert(secretMap.get("foo-mixed-3"), "foo-mixed-3.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));

        cert = Ca.cert(secretMap.get("foo-brokers-6"), "foo-brokers-6.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));
    }

    @ParallelTest
    public void testGenerateBrokerSecretExternal() throws CertificateParsingException {
        Map<Integer, Set<String>> externalAddresses = Map.of(
                0, Set.of("123.10.125.130"),
                3, Set.of("123.10.125.133"),
                6, Set.of("123.10.125.136"));

        List<Secret> secrets = generateBrokerSecrets(Set.of("123.10.125.140"), externalAddresses);
        Set<String> secretDataKeys = new HashSet<>();
        Map<String, Secret> secretMap = new HashMap<>();
        secrets.forEach(secret -> {
            secretDataKeys.addAll(secret.getData().keySet());
            secretMap.put(secret.getMetadata().getName(), secret);
        });
        assertThat(secretDataKeys, is(Set.of(
                "foo-controllers-0.crt",  "foo-controllers-0.key",
                "foo-controllers-1.crt", "foo-controllers-1.key",
                "foo-controllers-2.crt", "foo-controllers-2.key",
                "foo-mixed-3.crt",  "foo-mixed-3.key",
                "foo-mixed-4.crt", "foo-mixed-4.key",
                "foo-brokers-5.crt",  "foo-brokers-5.key",
                "foo-brokers-6.crt", "foo-brokers-6.key",
                "foo-brokers-7.crt", "foo-brokers-7.key")));

        X509Certificate cert = Ca.cert(secretMap.get("foo-controllers-0"), "foo-controllers-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));

        cert = Ca.cert(secretMap.get("foo-mixed-3"), "foo-mixed-3.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(12));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"),
                List.of(7, "123.10.125.140"),
                List.of(7, "123.10.125.133"))));

        cert = Ca.cert(secretMap.get("foo-brokers-6"), "foo-brokers-6.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(12));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"),
                List.of(7, "123.10.125.140"),
                List.of(7, "123.10.125.136"))));
    }

    @ParallelTest
    public void testGenerateBrokerSecretExternalWithManyDNS() throws CertificateParsingException {
        Map<Integer, Set<String>> externalAddresses = Map.of(
                0, Set.of("123.10.125.130", "my-broker-0"),
                3, Set.of("123.10.125.133", "my-broker-3"),
                6, Set.of("123.10.125.136", "my-broker-6"));

        List<Secret> secrets = generateBrokerSecrets(Set.of("123.10.125.140", "my-bootstrap"), externalAddresses);
        Set<String> secretDataKeys = new HashSet<>();
        Map<String, Secret> secretMap = new HashMap<>();
        secrets.forEach(secret -> {
            secretDataKeys.addAll(secret.getData().keySet());
            secretMap.put(secret.getMetadata().getName(), secret);
        });

        assertThat(secretDataKeys, is(Set.of(
                "foo-controllers-0.crt",  "foo-controllers-0.key",
                "foo-controllers-1.crt", "foo-controllers-1.key",
                "foo-controllers-2.crt", "foo-controllers-2.key",
                "foo-mixed-3.crt",  "foo-mixed-3.key",
                "foo-mixed-4.crt", "foo-mixed-4.key",
                "foo-brokers-5.crt",  "foo-brokers-5.key",
                "foo-brokers-6.crt", "foo-brokers-6.key",
                "foo-brokers-7.crt", "foo-brokers-7.key")));

        X509Certificate cert = Ca.cert(secretMap.get("foo-controllers-0"), "foo-controllers-0.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(10));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-controllers-0.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"))));

        cert = Ca.cert(secretMap.get("foo-mixed-3"), "foo-mixed-3.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(14));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-mixed-3.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "my-broker-3"),
                List.of(2, "my-bootstrap"),
                List.of(7, "123.10.125.140"),
                List.of(7, "123.10.125.133"))));

        cert = Ca.cert(secretMap.get("foo-brokers-6"), "foo-brokers-6.crt");
        assertThat(cert.getSubjectX500Principal().getName(), is("CN=foo-kafka,O=io.strimzi"));
        assertThat(cert.getSubjectAlternativeNames().size(), is(14));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(Set.of(
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "foo-brokers-6.foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-bootstrap"),
                List.of(2, "foo-kafka-bootstrap.test"),
                List.of(2, "foo-kafka-bootstrap.test.svc"),
                List.of(2, "foo-kafka-bootstrap.test.svc.cluster.local"),
                List.of(2, "foo-kafka-brokers"),
                List.of(2, "foo-kafka-brokers.test"),
                List.of(2, "foo-kafka-brokers.test.svc"),
                List.of(2, "foo-kafka-brokers.test.svc.cluster.local"),
                List.of(2, "my-broker-6"),
                List.of(2, "my-bootstrap"),
                List.of(7, "123.10.125.140"),
                List.of(7, "123.10.125.136"))));
    }

    @ParallelTest
    public void testControlPlanePortNetworkPolicy() {
        NetworkPolicyPeer kafkaBrokersPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER)))
                .endPodSelector()
                .build();
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        // Check Network Policies => Different namespace
        NetworkPolicy np = KC.generateNetworkPolicy("operator-namespace", null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CONTROLPLANE_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.CONTROLPLANE_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(2));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }

    @ParallelTest
    public void testReplicationPortNetworkPolicy() {
        NetworkPolicyPeer kafkaBrokersPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, CLUSTER, Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer eoPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer kafkaExporterPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaExporterResources.componentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer cruiseControlPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_NAME_LABEL, CruiseControlResources.componentName(CLUSTER)))
                .endPodSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeerSameNamespace = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .build();

        NetworkPolicyPeer clusterOperatorPeerNamespaceWithLabels = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                .withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector()
                    .withMatchLabels(Map.of("nsLabelKey", "nsLabelValue"))
                .endNamespaceSelector()
                .build();

        // Check Network Policies => Different namespace
        NetworkPolicy np = KC.generateNetworkPolicy("operator-namespace", null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeer), is(true));

        // Check Network Policies => Same namespace
        np = KC.generateNetworkPolicy(NAMESPACE, null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeerSameNamespace), is(true));

        // Check Network Policies => Namespace with Labels
        np = KC.generateNetworkPolicy("operator-namespace", Labels.fromMap(Map.of("nsLabelKey", "nsLabelValue")));

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).findFirst().orElse(null), is(notNullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(KafkaCluster.REPLICATION_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElseThrow();

        assertThat(rules.size(), is(5));
        assertThat(rules.contains(kafkaBrokersPeer), is(true));
        assertThat(rules.contains(eoPeer), is(true));
        assertThat(rules.contains(kafkaExporterPeer), is(true));
        assertThat(rules.contains(cruiseControlPeer), is(true));
        assertThat(rules.contains(clusterOperatorPeerNamespaceWithLabels), is(true));
    }

    @ParallelTest
    public void testNetworkPolicyPeers() {
        NetworkPolicyPeer peer1 = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchExpressions(new LabelSelectorRequirementBuilder().withKey("my-key1").withValues("my-value1").build())
                .endPodSelector()
                .build();

        NetworkPolicyPeer peer2 = new NetworkPolicyPeerBuilder()
                .withNewNamespaceSelector()
                    .withMatchExpressions(new LabelSelectorRequirementBuilder().withKey("my-key2").withValues("my-value2").build())
                .endNamespaceSelector()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withNetworkPolicyPeers(peer1)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .withNetworkPolicyPeers(peer2)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .withNetworkPolicyPeers(peer1, peer2)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check Network Policies
        NetworkPolicy np = kc.generateNetworkPolicy(null, null);

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9092))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().get(0), is(peer1));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9093))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().get(0), is(peer2));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9094))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom().size(), is(2));
        assertThat(rules.get(0).getFrom().contains(peer1), is(true));
        assertThat(rules.get(0).getFrom().contains(peer2), is(true));
    }

    @ParallelTest
    public void testNoNetworkPolicyPeers() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check Network Policies
        NetworkPolicy np = kc.generateNetworkPolicy(null, null);

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9092))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9093))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));

        rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(9094))).collect(Collectors.toList());
        assertThat(rules.size(), is(1));
        assertThat(rules.get(0).getFrom(), is(nullValue()));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget()   {
        PodDisruptionBudget pdb = KC.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(7));
        assertThat(pdb.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().toMap()));
    }

    @ParallelTest
    public void testCustomizedPodDisruptionBudget()   {
        Map<String, String> pdbLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> pdbAnnotations = Map.of("a1", "v1", "a2", "v2");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPodDisruptionBudget()
                                .withNewMetadata()
                                    .withAnnotations(pdbAnnotations)
                                    .withLabels(pdbLabels)
                                .endMetadata()
                                .withMaxUnavailable(2)
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();

        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnnotations.entrySet()), is(true));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(6));
        assertThat(pdb.getSpec().getSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
    }

    @ParallelTest
    public void testClusterRoleBindingNodePort() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb.getMetadata().getName(), is(KafkaResources.initContainerClusterRoleBindingName(CLUSTER, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(kc.componentName));
    }

    @ParallelTest
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withNewRack("my-topology-label")
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        ClusterRoleBinding crb = kc.generateClusterRoleBinding(testNamespace);

        assertThat(crb.getMetadata().getName(), is(KafkaResources.initContainerClusterRoleBindingName(CLUSTER, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(kc.componentName));
    }

    @ParallelTest
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        ClusterRoleBinding crb = KC.generateClusterRoleBinding(testNamespace);

        assertThat(crb, is(nullValue()));
    }

    @ParallelTest
    public void testReplicasAndRelatedOptionsValidationNok() {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                        .editKafka()
                            .withConfig(Map.of("offsets.topic.replication.factor", 6))
                        .endKafka()
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });
        assertThat(ex.getMessage(), is("Kafka configuration option 'offsets.topic.replication.factor' should be set to " + 5 + " or less because this cluster has only " + 5 + " Kafka broker(s)."));
    }

    @ParallelTest
    public void testReplicasAndRelatedOptionsValidationOk() {
        assertDoesNotThrow(() -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                        .editKafka()
                            .withConfig(Map.of("offsets.topic.replication.factor", 5))
                        .endKafka()
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testCruiseControl() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        KafkaNodePool controllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi"))).build())
                .endSpec()
                .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("1500m"), "memory", new Quantity("18Gi"))).build())
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("2500m"), "memory", new Quantity("28Gi"))).build())
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(controllers, mixed, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        String brokerConfig = kafkaCluster.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        // Not set for controller only nodes
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS.toString())));
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.toString())));
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.toString())));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(3, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + 1));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + 1));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + 1));

        // Test values generated for Cruise Control capacity configuration
        Map<String, Storage> storage = kafkaCluster.getStorageByPoolName();
        assertThat(storage.size(), is(3));
        assertThat(storage.get("controllers"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(storage.get("mixed"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));
        assertThat(storage.get("brokers"), is(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build()));

        Map<String, ResourceRequirements> resources = kafkaCluster.getBrokerResourceRequirementsByPoolName();
        assertThat(resources.size(), is(2));
        assertThat(resources.get("mixed").getLimits(), is(Map.of("cpu", new Quantity("1500m"), "memory", new Quantity("18Gi"))));
        assertThat(resources.get("brokers").getLimits(), is(Map.of("cpu", new Quantity("2500m"), "memory", new Quantity("28Gi"))));
    }

    @ParallelTest
    public void testCruiseControlCustomMetricsReporterTopic() {
        int replicationFactor = 3;
        int minInSync = 2;
        int partitions = 5;

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS.getValue(), partitions,
                                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue(), replicationFactor,
                                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), minInSync
                        ))
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        String brokerConfig = kafkaCluster.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        // The metrics reporter is not configured in a controller only node
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + partitions)));
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + replicationFactor)));
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync)));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(3, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + partitions));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + replicationFactor));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + partitions));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + replicationFactor));
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));
    }

    @ParallelTest
    public void testCruiseControlCustomMetricsReporterTopicMinInSync() {
        int minInSync = 1;

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), minInSync
                        ))
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        String brokerConfig = kafkaCluster.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        // The metrics reporter is not configured in a controller only node
        assertThat(brokerConfig, not(CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync)));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(3, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));

        brokerConfig = kafkaCluster.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(brokerConfig, CoreMatchers.containsString(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + minInSync));
    }

    @ParallelTest
    public void testCruiseControlWithSingleNodeKafka() {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                        .withNewCruiseControl()
                        .endCruiseControl()
                    .endSpec()
                    .build();
            KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                    .editSpec()
                        .withReplicas(1)
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });

        assertThat(ex.getMessage(), is("Kafka " + NAMESPACE + "/" + CLUSTER + " has invalid configuration. " +
                "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers."));
    }

    @ParallelTest
    public void testCruiseControlWithMinISRGreaterThanReplicas() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), 3,
                                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue(), 2))
                    .endKafka()
                .withNewCruiseControl()
                .endCruiseControl()
                .endSpec()
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testCruiseControlWithMinISRGreaterThanDefaultReplicas() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue(), 2))
                    .endKafka()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetricsConfig(metrics)
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        assertThat(kc.metrics().isEnabled(), is(true));
        assertThat(((JmxPrometheusExporterModel) kc.metrics()).getConfigMapName(), is("my-metrics-configuration"));
        assertThat(((JmxPrometheusExporterModel) kc.metrics()).getConfigMapKey(), is("config.yaml"));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        assertThat(KC.metrics(), is(nullValue()));
    }

    @ParallelTest
    public void testExternalAddressEnvVarNotSetInControllers() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("external").withPort(9094).withType(KafkaListenerType.NODEPORT).withTls().build())
                        .withNewRack()
                            .withTopologyKey("my-topology-key")
                        .endRack()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(pod.getSpec().getInitContainers(), is(empty()));
            } else {
                List<EnvVar> envVars = pod.getSpec().getInitContainers().stream().findAny().orElseThrow().getEnv();
                assertThat(envVars.size(), is(3));
                assertThat(envVars.get(0).getName(), is("NODE_NAME"));
                assertThat(envVars.get(0).getValueFrom(), is(notNullValue()));
                assertThat(envVars.get(1).getName(), is("RACK_TOPOLOGY_KEY"));
                assertThat(envVars.get(1).getValue(), is("my-topology-key"));
                assertThat(envVars.get(2).getName(), is("EXTERNAL_ADDRESS"));
                assertThat(envVars.get(2).getValue(), is("TRUE"));
            }
        }));
    }

    @ParallelTest
    public void testKafkaInitContainerSectionIsConfigurable() {
        Map<String, Quantity> limits = new HashMap<>();
        limits.put("cpu", Quantity.parse("1"));
        limits.put("memory", Quantity.parse("256Mi"));

        Map<String, Quantity> requirements = new HashMap<>();
        requirements.put("cpu", Quantity.parse("100m"));
        requirements.put("memory", Quantity.parse("128Mi"));

        ResourceRequirements resourceReq = new ResourceRequirementsBuilder()
            .withLimits(limits)
            .withRequests(requirements)
            .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withResources(resourceReq)
                    .withNewRack()
                        .withTopologyKey("rack-key")
                    .endRack()
                .endKafka()
            .endSpec()
            .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            if (!pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                ResourceRequirements initContainersResources = pod.getSpec().getInitContainers().stream().findAny().orElseThrow().getResources();
                assertThat(initContainersResources.getRequests(), is(requirements));
                assertThat(initContainersResources.getLimits(), is(limits));
            }
        }));
    }

    @ParallelTest
    public void testKafkaInitContainerSectionIsConfigurableInKafkaAndNodePool() {
        Map<String, Quantity> limits = new HashMap<>();
        limits.put("cpu", Quantity.parse("1"));
        limits.put("memory", Quantity.parse("256Mi"));

        Map<String, Quantity> requirements = new HashMap<>();
        requirements.put("cpu", Quantity.parse("100m"));
        requirements.put("memory", Quantity.parse("128Mi"));

        ResourceRequirements resourceReq = new ResourceRequirementsBuilder()
            .withLimits(limits)
            .withRequests(requirements)
            .build();

        Map<String, Quantity> poolLimits = new HashMap<>();
        poolLimits.put("cpu", Quantity.parse("10"));
        poolLimits.put("memory", Quantity.parse("2560Mi"));

        Map<String, Quantity> poolRequirements = new HashMap<>();
        poolRequirements.put("cpu", Quantity.parse("1000m"));
        poolRequirements.put("memory", Quantity.parse("1280Mi"));

        ResourceRequirements poolResourceReq = new ResourceRequirementsBuilder()
            .withLimits(poolLimits)
            .withRequests(poolRequirements)
            .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withResources(resourceReq)
                    .withNewRack()
                        .withTopologyKey("rack-key")
                    .endRack()
                .endKafka()
            .endSpec()
            .build();
        KafkaNodePool controllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .editSpec()
                    .withResources(poolResourceReq)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(controllers, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(pod.getSpec().getInitContainers(), is(empty()));
            } else {
                ResourceRequirements initContainersResources = pod.getSpec().getInitContainers().get(0).getResources();
                assertThat(initContainersResources.getRequests(), is(requirements));
                assertThat(initContainersResources.getLimits(), is(limits));
            }
        }));
    }

    @ParallelTest
    public void testKafkaInitContainerSectionIsConfigurableOnlyInNodePool() {
        Map<String, Quantity> poolLimits = new HashMap<>();
        poolLimits.put("cpu", Quantity.parse("1"));
        poolLimits.put("memory", Quantity.parse("256Mi"));

        Map<String, Quantity> poolRequirements = new HashMap<>();
        poolRequirements.put("cpu", Quantity.parse("100m"));
        poolRequirements.put("memory", Quantity.parse("128Mi"));

        ResourceRequirements poolResourceReq = new ResourceRequirementsBuilder()
            .withLimits(poolLimits)
            .withRequests(poolRequirements)
            .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withNewRack()
                        .withTopologyKey("rack-key")
                    .endRack()
                .endKafka()
            .endSpec()
            .build();
        KafkaNodePool mixed = new KafkaNodePoolBuilder(POOL_MIXED)
                .editSpec()
                    .withResources(poolResourceReq)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, mixed, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));
            } else if (pod.getMetadata().getName().startsWith(CLUSTER + "-mixed")) {
                assertThat(initCont.getResources().getRequests(), is(poolRequirements));
                assertThat(initCont.getResources().getLimits(), is(poolLimits));
            } else {
                assertThat(initCont.getResources(), is(nullValue()));
            }
        }));
    }

    @ParallelTest
    public void testInvalidVersion() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("6.6.6")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 6.6.6. Supported versions are:"));
    }

    @ParallelTest
    public void testUnsupportedVersion() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("2.6.0")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 2.6.0. Supported versions are:"));
    }

    @ParallelTest
    public void testInvalidVersionWithCustomImage() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion("2.6.0")
                        .withImage("my-custom/image:latest")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException exc = assertThrows(KafkaVersion.UnsupportedKafkaVersionException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });

        assertThat(exc.getMessage(), containsString("Unsupported Kafka.spec.kafka.version: 2.6.0. Supported versions are:"));
    }

    @ParallelTest
    public void testKRaftMetadataVersionValidation()    {
        // Valid values
        assertDoesNotThrow(() -> KafkaCluster.validateMetadataVersion("3.6"));
        assertDoesNotThrow(() -> KafkaCluster.validateMetadataVersion("3.6-IV2"));

        // Minimum supported versions
        assertDoesNotThrow(() -> KafkaCluster.validateMetadataVersion("3.3"));
        assertDoesNotThrow(() -> KafkaCluster.validateMetadataVersion("3.3-IV0"));

        // Invalid Values
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaCluster.validateMetadataVersion("3.6-IV9"));
        assertThat(e.getMessage(), containsString("Metadata version 3.6-IV9 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KafkaCluster.validateMetadataVersion("3"));
        assertThat(e.getMessage(), containsString("Metadata version 3 is invalid"));

        e = assertThrows(InvalidResourceException.class, () -> KafkaCluster.validateMetadataVersion("3.2"));
        assertThat(e.getMessage(), containsString("The oldest supported metadata version is 3.3-IV0"));

        e = assertThrows(InvalidResourceException.class, () -> KafkaCluster.validateMetadataVersion("3.2-IV0"));
        assertThat(e.getMessage(), containsString("The oldest supported metadata version is 3.3-IV0"));
    }

    @ParallelTest
    public void testKRaftMetadataVersionValidationInFromCrd()   {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetadataVersion("3.6-IV9")
                    .endKafka()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class,
                () -> KafkaCluster.fromCrd(
                        Reconciliation.DUMMY_RECONCILIATION,
                        kafka,
                        POOLS,
                        VERSIONS,
                        new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, "3.6-IV9"),
                        null,
                        SHARED_ENV_PROVIDER));
        assertThat(ex.getMessage(), containsString("Metadata version 3.6-IV9 is invalid"));
    }

    @ParallelTest
    public void testCustomKRaftMetadataVersion()   {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetadataVersion("3.5-IV1")
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(
                Reconciliation.DUMMY_RECONCILIATION,
                kafka,
                POOLS,
                VERSIONS,
                new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, "3.5-IV1"),
                null,
                SHARED_ENV_PROVIDER);

        assertThat(kc.getMetadataVersion(), is("3.5-IV1"));
    }

    @ParallelTest
    public void testRackAffinity() {
        Affinity rackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("failure-domain.beta.kubernetes.io/zone")
                                    .withOperator("Exists")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(100)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("strimzi.io/cluster", "foo", "strimzi.io/name", "foo-kafka"))
                                        .endLabelSelector()
                                        .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewRack()
                            .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                        .endRack()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                assertThat(pod.getSpec().getAffinity(), is(rackAffinity));
            }
        }
    }

    @ParallelTest
    public void testAffinityAndTolerations() {
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

        List<Toleration> toleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withAffinity(affinity)
                                .withTolerations(toleration)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                assertThat(pod.getSpec().getAffinity(), is(affinity));
                assertThat(pod.getSpec().getTolerations(), is(toleration));
            }
        }
    }

    @ParallelTest
    public void testAffinityAndTolerationsInKafkaAndKafkaPool() {
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
        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key2")
                                    .withOperator("In")
                                    .withValues("value3", "value4")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> toleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());
        List<Toleration> poolToleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key2")
                .withOperator("Equal")
                .withValue("value2")
                .build());

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withAffinity(affinity)
                                .withTolerations(toleration)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                            .withTolerations(poolToleration)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getAffinity(), is(poolAffinity));
                    assertThat(pod.getSpec().getTolerations(), is(poolToleration));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(affinity));
                    assertThat(pod.getSpec().getTolerations(), is(toleration));
                }
            }
        }
    }

    @ParallelTest
    public void testAffinityAndTolerationsInKafkaPool() {
        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key2")
                                    .withOperator("In")
                                    .withValues("value3", "value4")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> poolToleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key2")
                .withOperator("Equal")
                .withValue("value2")
                .build());

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                            .withTolerations(poolToleration)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getAffinity(), is(poolAffinity));
                    assertThat(pod.getSpec().getTolerations(), is(poolToleration));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(new Affinity()));
                    assertThat(pod.getSpec().getTolerations(), is(List.of()));
                }
            }
        }
    }

    @ParallelTest
    public void testAffinityAndRack() {
        Affinity mergedRackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(
                                new NodeSelectorTermBuilder()
                                        .addNewMatchExpression()
                                            .withKey("key1")
                                            .withOperator("In")
                                            .withValues("value1", "value2")
                                        .endMatchExpression()
                                        .addNewMatchExpression()
                                            .withKey("failure-domain.beta.kubernetes.io/zone")
                                            .withOperator("Exists")
                                        .endMatchExpression()
                                        .build()
                        )
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("storage", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build(),
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(100)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("strimzi.io/cluster", "foo", "strimzi.io/name", "foo-kafka"))
                                        .endLabelSelector()
                                        .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

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
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("storage", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewRack()
                            .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                        .endRack()
                        .withNewTemplate()
                            .withNewPod()
                                .withAffinity(affinity)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                assertThat(pod.getSpec().getAffinity(), is(mergedRackAffinity));
            }
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testAffinityAndRackInKafkaAndKafkaPool() {
        Affinity mergedRackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(
                                new NodeSelectorTermBuilder()
                                        .addNewMatchExpression()
                                            .withKey("key1")
                                            .withOperator("In")
                                            .withValues("value1", "value2")
                                        .endMatchExpression()
                                        .addNewMatchExpression()
                                            .withKey("failure-domain.beta.kubernetes.io/zone")
                                            .withOperator("Exists")
                                        .endMatchExpression()
                                        .build()
                        )
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("storage", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build(),
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(100)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("strimzi.io/cluster", "foo", "strimzi.io/name", "foo-kafka"))
                                        .endLabelSelector()
                                        .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Affinity poolMergedRackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(
                                new NodeSelectorTermBuilder()
                                        .addNewMatchExpression()
                                            .withKey("key2")
                                            .withOperator("In")
                                            .withValues("value3", "value4")
                                        .endMatchExpression()
                                        .addNewMatchExpression()
                                            .withKey("failure-domain.beta.kubernetes.io/zone")
                                            .withOperator("Exists")
                                        .endMatchExpression()
                                        .build()
                        )
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("database", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build(),
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(100)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("strimzi.io/cluster", "foo", "strimzi.io/name", "foo-kafka"))
                                        .endLabelSelector()
                                        .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

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
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("storage", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();
        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key2")
                                    .withOperator("In")
                                    .withValues("value3", "value4")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("database", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewRack()
                            .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                        .endRack()
                        .withNewTemplate()
                            .withNewPod()
                                .withAffinity(affinity)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getAffinity(), is(poolMergedRackAffinity));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(mergedRackAffinity));
                }
            }
        }
    }

    @ParallelTest
    public void testAffinityAndRackInKafkaPool() {
        Affinity rackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("failure-domain.beta.kubernetes.io/zone")
                                    .withOperator("Exists")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(100)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("strimzi.io/cluster", "foo", "strimzi.io/name", "foo-kafka"))
                                        .endLabelSelector()
                                        .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Affinity mergedRackAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(
                                new NodeSelectorTermBuilder()
                                        .addNewMatchExpression()
                                            .withKey("key2")
                                            .withOperator("In")
                                            .withValues("value3", "value4")
                                        .endMatchExpression()
                                        .addNewMatchExpression()
                                            .withKey("failure-domain.beta.kubernetes.io/zone")
                                            .withOperator("Exists")
                                        .endMatchExpression()
                                        .build()
                        )
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("database", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build(),
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(100)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("strimzi.io/cluster", "foo", "strimzi.io/name", "foo-kafka"))
                                        .endLabelSelector()
                                        .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key2")
                                    .withOperator("In")
                                    .withValues("value3", "value4")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .withNewPodAntiAffinity()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(
                            new WeightedPodAffinityTermBuilder()
                                    .withWeight(50)
                                    .withNewPodAffinityTerm()
                                        .withNewLabelSelector()
                                            .withMatchLabels(Map.of("database", "true"))
                                        .endLabelSelector()
                                        .withTopologyKey("kubernetes.io/hostname")
                                    .endPodAffinityTerm()
                                    .build()
                    )
                .endPodAntiAffinity()
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewRack()
                            .withTopologyKey("failure-domain.beta.kubernetes.io/zone")
                        .endRack()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withAffinity(poolAffinity)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getAffinity(), is(mergedRackAffinity));
                } else {
                    assertThat(pod.getSpec().getAffinity(), is(rackAffinity));
                }
            }
        }
    }

    @ParallelTest
    public void testGenerateDeploymentWithOpa() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withAuthorization(
                            new KafkaAuthorizationOpaBuilder()
                                    .withUrl("http://opa:8080")
                                    .withTlsTrustedCertificates(cert1, cert2)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Volume mounts
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "authz-opa-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-opa-certs/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "authz-opa-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-opa-certs/second-certificate"));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_OPA_AUTHZ_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt"));

            // Volumes
            List<Volume> volumes = pod.getSpec().getVolumes();
            assertThat(volumes.stream().filter(vol -> "authz-opa-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(volumes.stream().filter(vol -> "authz-opa-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
        }));

        String brokerConfig = kc.generatePerBrokerConfiguration(1, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        // Due to a bug, this is set when custom values are configured even in controllers
        assertThat(brokerConfig, CoreMatchers.containsString("authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer\n"));
        assertThat(brokerConfig, CoreMatchers.containsString("opa.authorizer.url=http://opa:8080\n"));

        brokerConfig = kc.generatePerBrokerConfiguration(3, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(brokerConfig, CoreMatchers.containsString("authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer\n"));
        assertThat(brokerConfig, CoreMatchers.containsString("opa.authorizer.url=http://opa:8080\n"));

        brokerConfig = kc.generatePerBrokerConfiguration(6, ADVERTISED_HOSTNAMES, ADVERTISED_PORTS);
        assertThat(brokerConfig, CoreMatchers.containsString("authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer\n"));
        assertThat(brokerConfig, CoreMatchers.containsString("opa.authorizer.url=http://opa:8080\n"));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        // Test ALWAYS policy
        List<StrimziPodSet> podSets = KC.generatePodSets(true, ImagePullPolicy.ALWAYS, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
            }
        }

        // Test IFNOTPRESENT policy
        podSets = KC.generatePodSets(true, ImagePullPolicy.IFNOTPRESENT, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
            }
        }

        // Test NEVER policy
        podSets = KC.generatePodSets(true, ImagePullPolicy.NEVER, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.NEVER.toString()));
            }
        }
    }

    @ParallelTest
    public void testImagePullSecrets() {
        // CR configuration has priority -> CO configuration is ignored if both are set
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret1, secret2)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
            }
        }
    }

    @ParallelTest
    public void testImagePullSecretsFromCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        List<StrimziPodSet> podSets = KC.generatePodSets(true, null, secrets, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
            }
        }
    }

    @ParallelTest
    public void testImagePullSecretsFromBoth() {
        // CR configuration has priority -> CO configuration is ignored if both are set
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret2)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, List.of(secret1), node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
            }
        }
    }

    @ParallelTest
    public void testImagePullSecretsFromKafkaAndNodePool() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret1)
                            .endPod()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));

                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
                } else {
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(false));
                }
            }
        }
    }

    @ParallelTest
    public void testImagePullSecretsFromCoAndNodePool() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, List.of(secret1), node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));

                if (pod.getMetadata().getName().contains("brokers"))    {
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
                } else {
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(false));
                }
            }
        }
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        List<StrimziPodSet> podSets = KC.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(0));
            }
        }
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        kc.securityProvider = new RestrictedPodSecurityProvider();
        kc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(0L));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
            }
        }
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        List<StrimziPodSet> podSets = KC.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(0L));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testCustomLabelsFromCR() {
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editMetadata()
                    .withLabels(Map.of("foo", "bar"))
                .endMetadata()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            if (podSet.getMetadata().getName().contains("brokers")) {
                assertThat(podSet.getMetadata().getLabels().get("foo"), is("bar"));
            } else {
                assertThat(podSet.getMetadata().getLabels().get("foo"), is(nullValue()));
            }

            List<Pod> pods = PodSetUtils.podSetToPods(podSet);
            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers")) {
                    assertThat(pod.getMetadata().getLabels().get("foo"), is("bar"));
                } else {
                    assertThat(pod.getMetadata().getLabels().get("foo"), is(nullValue()));
                }
            }
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testPodSet()   {
        List<StrimziPodSet> podSets = KC.generatePodSets(true, null, null, node -> Map.of("test-anno", "test-value"));
        assertThat(podSets.size(), is(3));

        // Controllers
        StrimziPodSet podSet = podSets.stream().filter(sps -> (CLUSTER + "-controllers").equals(sps.getMetadata().getName())).findFirst().orElse(null);
        assertThat(podSet, is(notNullValue()));

        TestUtils.checkOwnerReference(podSet, POOL_CONTROLLERS);
        assertThat(podSet.getMetadata().getName(), is(CLUSTER + "-controllers"));
        assertThat(podSet.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().withStrimziPoolName("controllers").toMap()));
        assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(KC.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build()).build())));

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(podSet);
        assertThat(pods.size(), is(3));

        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(KC.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(CLUSTER + "-controllers").toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(2));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("test-anno"), is("test-value"));

            assertThat(pod.getSpec().getHostname(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getSubdomain(), is(KafkaResources.brokersServiceName(CLUSTER)));
            assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
            assertThat(pod.getSpec().getVolumes().stream()
                    .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                    .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(15));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(15));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));

            assertThat(pod.getSpec().getVolumes().size(), is(7));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
        }

        // Mixed nodes
        podSet = podSets.stream().filter(sps -> (CLUSTER + "-mixed").equals(sps.getMetadata().getName())).findFirst().orElse(null);
        assertThat(podSet, is(notNullValue()));

        TestUtils.checkOwnerReference(podSet, POOL_MIXED);
        assertThat(podSet.getMetadata().getName(), is(CLUSTER + "-mixed"));
        assertThat(podSet.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().withStrimziPoolName("mixed").toMap()));
        assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(KC.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build()).build())));

        // We need to loop through the pods to make sure they have the right values
        pods = PodSetUtils.podSetToPods(podSet);
        assertThat(pods.size(), is(2));

        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(KC.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(CLUSTER + "-mixed").toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(2));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("test-anno"), is("test-value"));

            assertThat(pod.getSpec().getHostname(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getSubdomain(), is(KafkaResources.brokersServiceName(CLUSTER)));
            assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
            assertThat(pod.getSpec().getVolumes().stream()
                    .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                    .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(15));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(15));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));

            assertThat(pod.getSpec().getVolumes().size(), is(7));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
        }

        // Brokers
        podSet = podSets.stream().filter(sps -> (CLUSTER + "-brokers").equals(sps.getMetadata().getName())).findFirst().orElse(null);
        assertThat(podSet, is(notNullValue()));

        TestUtils.checkOwnerReference(podSet, POOL_BROKERS);
        assertThat(podSet.getMetadata().getName(), is(CLUSTER + "-brokers"));
        assertThat(podSet.getSpec().getSelector().getMatchLabels(), is(KC.getSelectorLabels().withStrimziPoolName("brokers").toMap()));
        assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(KC.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(podSet.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_STORAGE), is(ModelUtils.encodeStorageToJson(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build()).build())));

        // We need to loop through the pods to make sure they have the right values
        pods = PodSetUtils.podSetToPods(podSet);
        assertThat(pods.size(), is(3));

        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(KC.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(CLUSTER + "-brokers").toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(2));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("test-anno"), is("test-value"));

            assertThat(pod.getSpec().getHostname(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getSubdomain(), is(KafkaResources.brokersServiceName(CLUSTER)));
            assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
            assertThat(pod.getSpec().getVolumes().stream()
                    .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                    .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(15));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(15));
            assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));

            assertThat(pod.getSpec().getVolumes().size(), is(7));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
            assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
            assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
            assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
            assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testCustomizedPodSet()   {
        // Prepare various template values
        Map<String, String> spsLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> spsAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = Map.of("a3", "v3", "a4", "v4");

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        TopologySpreadConstraint tsc1 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/zone")
                .withMaxSkew(1)
                .withWhenUnsatisfiable("DoNotSchedule")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

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

        List<Toleration> toleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        // Used to test env var conflict
        ContainerEnvVar envVar3 = new ContainerEnvVar();
        String testEnvThreeKey = KafkaCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED;
        String testEnvThreeValue = "test.env.three";
        envVar3.setName(testEnvThreeKey);
        envVar3.setValue(testEnvThreeValue);

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(true)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        String image = "my-custom-image:latest";

        Probe livenessProbe = new Probe();
        livenessProbe.setInitialDelaySeconds(1);
        livenessProbe.setTimeoutSeconds(2);
        livenessProbe.setSuccessThreshold(3);
        livenessProbe.setFailureThreshold(4);
        livenessProbe.setPeriodSeconds(5);

        Probe readinessProbe = new Probe();
        readinessProbe.setInitialDelaySeconds(6);
        readinessProbe.setTimeoutSeconds(7);
        readinessProbe.setSuccessThreshold(8);
        readinessProbe.setFailureThreshold(9);
        readinessProbe.setPeriodSeconds(10);

        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();

        AdditionalVolume additionalVolume  = new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        // Use the template values in Kafka CR
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withImage(image)
                        .withResources(new ResourceRequirementsBuilder()
                                .withRequests(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi")))
                                .withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi")))
                                .build())
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                        .withReadinessProbe(readinessProbe)
                        .withLivenessProbe(livenessProbe)
                        .withConfig(Map.of("foo", "bar"))
                        .withNewTemplate()
                            .withNewPodSet()
                                .withNewMetadata()
                                    .withLabels(spsLabels)
                                    .withAnnotations(spsAnnotations)
                                .endMetadata()
                            .endPodSet()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnnotations)
                                .endMetadata()
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withHostAliases(hostAlias1, hostAlias2)
                                .withTopologySpreadConstraints(tsc1, tsc2)
                                .withAffinity(affinity)
                                .withTolerations(toleration)
                                .withEnableServiceLinks(false)
                                .withTmpDirSizeLimit("10Mi")
                                .withTerminationGracePeriodSeconds(123)
                                .withImagePullSecrets(secret1, secret2)
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                                .withVolumes(additionalVolume)
                            .endPod()
                            .withNewKafkaContainer()
                                .withEnv(envVar1, envVar2, envVar3)
                                .withSecurityContext(securityContext)
                                .withVolumeMounts(additionalVolumeMount)
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        // Test the resources
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of("special", "annotation"));
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(true));
            assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(true));

            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                // Metadata
                assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
                assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(true));
                assertThat(pod.getMetadata().getAnnotations().get("special"), is("annotation"));

                // Pod
                assertThat(pod.getSpec().getPriorityClassName(), is("top-priority"));
                assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
                assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
                assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
                assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
                assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
                assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
                assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
                assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(123L));
                assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(456L));
                assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(789L));
                assertThat(pod.getSpec().getAffinity(), is(affinity));
                assertThat(pod.getSpec().getTolerations(), is(toleration));

                assertThat(pod.getSpec().getVolumes().size(), is(8));
                assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
                assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
                assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is(pod.getMetadata().getName()));
                assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
                assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
                assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
                assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
                assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
                assertThat(pod.getSpec().getVolumes().get(7).getName(), is("secret-volume-name"));
                assertThat(pod.getSpec().getVolumes().get(7).getSecret(), is(notNullValue()));

                // Containers
                assertThat(pod.getSpec().getContainers().size(), is(1));
                assertThat(pod.getSpec().getContainers().get(0).getImage(), is(image));
                assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(securityContext));
                assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(livenessProbe.getTimeoutSeconds()));
                assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(livenessProbe.getInitialDelaySeconds()));
                assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getFailureThreshold(), is(livenessProbe.getFailureThreshold()));
                assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getSuccessThreshold(), is(livenessProbe.getSuccessThreshold()));
                assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getPeriodSeconds(), is(livenessProbe.getPeriodSeconds()));
                assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(readinessProbe.getTimeoutSeconds()));
                assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(readinessProbe.getInitialDelaySeconds()));
                assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getFailureThreshold(), is(readinessProbe.getFailureThreshold()));
                assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getSuccessThreshold(), is(readinessProbe.getSuccessThreshold()));
                assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getPeriodSeconds(), is(readinessProbe.getPeriodSeconds()));
                assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("true"));
                assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));
                assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi"))));
                assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi"))));

                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(8));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getName(), is("secret-volume-name"));
                assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getMountPath(), is("/mnt/secret-volume"));
            }
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testCustomizedPodSetInKafkaAndNodePool()   {
        // Prepare various template values
        Map<String, String> spsLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> spsAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> poolSpsLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> poolSpsAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> poolPodLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> poolPodAnnotations = Map.of("a7", "v7", "a8", "v8");

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        TopologySpreadConstraint tsc1 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/zone")
                .withMaxSkew(1)
                .withWhenUnsatisfiable("DoNotSchedule")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

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

        Affinity poolAffinity = new AffinityBuilder()
                .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                        .addNewMatchExpression()
                        .withKey("key2")
                        .withOperator("In")
                        .withValues("value1", "value2")
                        .endMatchExpression()
                        .build())
                .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> toleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        List<Toleration> poolToleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key2")
                .withOperator("Equal")
                .withValue("value2")
                .build());

        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        // Used to test env var conflict
        ContainerEnvVar envVar3 = new ContainerEnvVar();
        String testEnvThreeKey = KafkaCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED;
        String testEnvThreeValue = "test.env.three";
        envVar3.setName(testEnvThreeKey);
        envVar3.setValue(testEnvThreeValue);

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(true)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        SecurityContext poolSecurityContext = new SecurityContextBuilder()
                .withPrivileged(true)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(true)
                .withRunAsNonRoot(false)
                .withNewCapabilities()
                    .addToDrop("NONE")
                .endCapabilities()
                .build();

        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();

        AdditionalVolume additionalVolume  = new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build();

        AdditionalVolume poolAdditionalVolume  = new AdditionalVolumeBuilder()
                .withName("secret-volume-name2")
                .withSecret(secret)
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        VolumeMount poolAdditionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name2")
                .withMountPath("/mnt/secret-volume2")
                .withSubPath("def")
                .build();

        // Use the template values in Kafka CR
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withResources(new ResourceRequirementsBuilder()
                                .withRequests(Map.of("cpu", new Quantity("1000m"), "memory", new Quantity("40Gi")))
                                .withLimits(Map.of("cpu", new Quantity("5000m"), "memory", new Quantity("80Gi")))
                                .build())
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                        .withConfig(Map.of("foo", "bar"))
                        .withNewTemplate()
                            .withNewPodSet()
                                .withNewMetadata()
                                    .withLabels(spsLabels)
                                    .withAnnotations(spsAnnotations)
                                .endMetadata()
                            .endPodSet()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnnotations)
                                .endMetadata()
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withHostAliases(hostAlias2)
                                .withTopologySpreadConstraints(tsc2)
                                .withAffinity(affinity)
                                .withTolerations(toleration)
                                .withEnableServiceLinks(false)
                                .withTmpDirSizeLimit("13Mi")
                                .withTerminationGracePeriodSeconds(321)
                                .withImagePullSecrets(secret2)
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(1230L).withRunAsGroup(4560L).withRunAsUser(7890L).build())
                                .withVolumes(additionalVolume)
                            .endPod()
                            .withNewKafkaContainer()
                                .withEnv(envVar2, envVar3)
                                .withSecurityContext(securityContext)
                                .withVolumeMounts(additionalVolumeMount)
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder()
                            .withRequests(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi")))
                            .withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi")))
                            .build())
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(false)
                    .endJvmOptions()
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(poolSpsLabels)
                                .withAnnotations(poolSpsAnnotations)
                            .endMetadata()
                        .endPodSet()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(poolPodLabels)
                                .withAnnotations(poolPodAnnotations)
                            .endMetadata()
                            .withPriorityClassName("top-priority2")
                            .withSchedulerName("my-scheduler2")
                            .withHostAliases(hostAlias1)
                            .withTopologySpreadConstraints(tsc1)
                            .withAffinity(poolAffinity)
                            .withTolerations(poolToleration)
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                            .withTerminationGracePeriodSeconds(123)
                            .withImagePullSecrets(secret1)
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .withVolumes(poolAdditionalVolume)
                        .endPod()
                        .withNewKafkaContainer()
                            .withEnv(envVar1, envVar3)
                            .withSecurityContext(poolSecurityContext)
                            .withVolumeMounts(poolAdditionalVolumeMount)
                        .endKafkaContainer()
                    .endTemplate()
                .endSpec()
                .build();

        // Test the resources
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of("special", "annotation"));
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            if (podSet.getMetadata().getName().contains("brokers")) {
                assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(poolSpsLabels.entrySet()), is(true));
                assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(poolSpsAnnotations.entrySet()), is(true));
            } else {
                assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(true));
                assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(true));
            }

            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers")) {
                    // Metadata
                    assertThat(pod.getMetadata().getLabels().entrySet().containsAll(poolPodLabels.entrySet()), is(true));
                    assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(poolPodAnnotations.entrySet()), is(true));
                    assertThat(pod.getMetadata().getAnnotations().get("special"), is("annotation"));

                    // Pod
                    assertThat(pod.getSpec().getPriorityClassName(), is("top-priority2"));
                    assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler2"));
                    assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1));
                    assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1));
                    assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
                    assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
                    assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                    assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
                    assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(123L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(456L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(789L));
                    assertThat(pod.getSpec().getAffinity(), is(poolAffinity));
                    assertThat(pod.getSpec().getTolerations(), is(poolToleration));

                    assertThat(pod.getSpec().getVolumes().size(), is(8));
                    assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
                    assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
                    assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
                    assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(7).getName(), is("secret-volume-name2"));
                    assertThat(pod.getSpec().getVolumes().get(7).getSecret(), is(notNullValue()));

                    // Containers
                    assertThat(pod.getSpec().getContainers().size(), is(1));
                    assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(poolSecurityContext));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("false"));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi"))));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi"))));

                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(8));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getName(), is("secret-volume-name2"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getMountPath(), is("/mnt/secret-volume2"));
                } else {
                    assertThat(pod.getSpec().getPriorityClassName(), is("top-priority"));
                    assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
                    assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias2));
                    assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc2));
                    assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
                    assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(321L));
                    assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
                    assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
                    assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(1230L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(4560L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(7890L));
                    assertThat(pod.getSpec().getAffinity(), is(affinity));
                    assertThat(pod.getSpec().getTolerations(), is(toleration));

                    assertThat(pod.getSpec().getVolumes().size(), is(8));
                    assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir().getSizeLimit(), is(new Quantity("13Mi")));
                    assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
                    assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
                    assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(7).getName(), is("secret-volume-name"));
                    assertThat(pod.getSpec().getVolumes().get(7).getSecret(), is(notNullValue()));

                    // Containers
                    assertThat(pod.getSpec().getContainers().size(), is(1));
                    assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(securityContext));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("true"));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(Map.of("cpu", new Quantity("1000m"), "memory", new Quantity("40Gi"))));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(Map.of("cpu", new Quantity("5000m"), "memory", new Quantity("80Gi"))));

                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(8));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getName(), is("secret-volume-name"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getMountPath(), is("/mnt/secret-volume"));
                }
            }
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testCustomizedPodSetInNodePool()   {
        // Prepare various template values
        Map<String, String> spsLabels = Map.of("l1", "v1", "l2", "v2");
        Map<String, String> spsAnnotations = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnnotations = Map.of("a3", "v3", "a4", "v4");

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        TopologySpreadConstraint tsc1 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/zone")
                .withMaxSkew(1)
                .withWhenUnsatisfiable("DoNotSchedule")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .build();

        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

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

        List<Toleration> toleration = List.of(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        // Used to test env var conflict
        ContainerEnvVar envVar3 = new ContainerEnvVar();
        String testEnvThreeKey = KafkaCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED;
        String testEnvThreeValue = "test.env.three";
        envVar3.setName(testEnvThreeKey);
        envVar3.setValue(testEnvThreeValue);

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(true)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();

        AdditionalVolume additionalVolume  = new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build();

        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret-volume")
                .withSubPath("def")
                .build();

        // Use the template values in Kafka CR
        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder()
                            .withRequests(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi")))
                            .withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi")))
                            .build())
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(spsLabels)
                                .withAnnotations(spsAnnotations)
                            .endMetadata()
                        .endPodSet()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(podLabels)
                                .withAnnotations(podAnnotations)
                            .endMetadata()
                            .withPriorityClassName("top-priority")
                            .withSchedulerName("my-scheduler")
                            .withHostAliases(hostAlias1, hostAlias2)
                            .withTopologySpreadConstraints(tsc1, tsc2)
                            .withAffinity(affinity)
                            .withTolerations(toleration)
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                            .withTerminationGracePeriodSeconds(123)
                            .withImagePullSecrets(secret1, secret2)
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .withVolumes(additionalVolume)
                        .endPod()
                        .withNewKafkaContainer()
                            .withEnv(envVar1, envVar2, envVar3)
                            .withSecurityContext(securityContext)
                            .withVolumeMounts(additionalVolumeMount)
                        .endKafkaContainer()
                    .endTemplate()
                .endSpec()
                .build();

        // Test the resources
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Test generated SPS
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of("special", "annotation"));
        assertThat(podSets.size(), is(3));

        for (StrimziPodSet podSet : podSets)    {
            if (podSet.getMetadata().getName().contains("brokers")) {
                assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(true));
                assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(true));
            } else {
                assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(spsLabels.entrySet()), is(false));
                assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnotations.entrySet()), is(false));
            }

            // We need to loop through the pods to make sure they have the right values
            List<Pod> pods = PodSetUtils.podSetToPods(podSet);

            for (Pod pod : pods) {
                if (pod.getMetadata().getName().contains("brokers")) {
                    // Metadata
                    assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
                    assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(true));
                    assertThat(pod.getMetadata().getAnnotations().get("special"), is("annotation"));

                    // Pod
                    assertThat(pod.getSpec().getPriorityClassName(), is("top-priority"));
                    assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
                    assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
                    assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
                    assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
                    assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
                    assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
                    assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
                    assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
                    assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(123L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(456L));
                    assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(789L));
                    assertThat(pod.getSpec().getAffinity(), is(affinity));
                    assertThat(pod.getSpec().getTolerations(), is(toleration));

                    assertThat(pod.getSpec().getVolumes().size(), is(8));
                    assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
                    assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
                    assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
                    assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(7).getName(), is("secret-volume-name"));
                    assertThat(pod.getSpec().getVolumes().get(7).getSecret(), is(notNullValue()));

                    // Containers
                    assertThat(pod.getSpec().getContainers().size(), is(1));
                    assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(securityContext));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("true"));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar1.getValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(envVar2.getValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is(not(envVar3.getValue())));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(Map.of("cpu", new Quantity("100m"), "memory", new Quantity("4Gi"))));
                    assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("8Gi"))));

                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(8));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getName(), is("secret-volume-name"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(7).getMountPath(), is("/mnt/secret-volume"));
                } else {
                    // Metadata
                    assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(false));
                    assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnnotations.entrySet()), is(false));
                    assertThat(pod.getMetadata().getAnnotations().get("special"), is("annotation"));

                    // Pod
                    assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
                    assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
                    assertThat(pod.getSpec().getHostAliases(), is(List.of()));
                    assertThat(pod.getSpec().getTopologySpreadConstraints(), is(List.of()));
                    assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
                    assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
                    assertThat(pod.getSpec().getImagePullSecrets().size(), is(0));
                    assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(0L));
                    assertThat(pod.getSpec().getAffinity(), is(new Affinity()));
                    assertThat(pod.getSpec().getTolerations(), is(List.of()));

                    assertThat(pod.getSpec().getVolumes().size(), is(7));
                    assertThat(pod.getSpec().getVolumes().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getVolumes().get(0).getPersistentVolumeClaim(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir(), is(notNullValue()));
                    assertThat(pod.getSpec().getVolumes().get(1).getEmptyDir().getSizeLimit(), is(new Quantity("5Mi")));
                    assertThat(pod.getSpec().getVolumes().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("foo-cluster-ca-cert"));
                    assertThat(pod.getSpec().getVolumes().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getVolumes().get(4).getSecret().getSecretName(), is("foo-clients-ca-cert"));
                    assertThat(pod.getSpec().getVolumes().get(5).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is(pod.getMetadata().getName()));
                    assertThat(pod.getSpec().getVolumes().get(6).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getVolumes().get(6).getEmptyDir(), is(notNullValue()));

                    // Containers
                    assertThat(pod.getSpec().getContainers().size(), is(1));
                    assertThat(pod.getSpec().getContainers().get(0).getSecurityContext(), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> AbstractModel.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED.equals(e.getName())).findFirst().orElseThrow().getValue(), is("false"));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar1.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar2.getName().equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                    assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(e -> envVar3.getName().equals(e.getName())).findFirst().orElseThrow().getValue(), is("false"));
                    assertThat(pod.getSpec().getContainers().get(0).getResources(), is(nullValue()));

                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(7));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/var/lib/kafka/data-0"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaCluster.CLUSTER_CA_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is(KafkaCluster.BROKER_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaCluster.BROKER_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaCluster.CLIENT_CA_CERTS_VOLUME_MOUNT));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getName(), is("kafka-metrics-and-logging"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(5).getMountPath(), is("/opt/kafka/custom-config/"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getName(), is("ready-files"));
                    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(6).getMountPath(), is("/var/opt/kafka"));
                }
            }
        }
    }

    @ParallelTest
    public void testNodePoolForNodeId()   {
        // Existing node
        assertThat(KC.nodePoolForNodeId(1).poolName, is("controllers"));
        assertThat(KC.nodePoolForNodeId(3).poolName, is("mixed"));
        assertThat(KC.nodePoolForNodeId(6).poolName, is("brokers"));

        // Non-existing node
        KafkaCluster.NodePoolNotFoundException e = assertThrows(KafkaCluster.NodePoolNotFoundException.class, () -> KC.nodePoolForNodeId(1874));
        assertThat(e.getMessage(), is("Node ID 1874 does not belong to any known node pool!"));
    }

    @ParallelTest
    public void testNodesAndStatuses()  {
        Set<NodeRef> nodes = KC.nodes();
        assertThat(nodes.size(), is(8));
        assertThat(nodes, hasItems(new NodeRef(CLUSTER + "-controllers-0", 0, "controllers", true, false),
                new NodeRef(CLUSTER + "-controllers-1", 1, "controllers", true, false),
                new NodeRef(CLUSTER + "-controllers-2", 2, "controllers", true, false),
                new NodeRef(CLUSTER + "-mixed-3", 3, "mixed", true, true),
                new NodeRef(CLUSTER + "-mixed-4", 4, "mixed", true, true),
                new NodeRef(CLUSTER + "-brokers-5", 5, "brokers", false, true),
                new NodeRef(CLUSTER + "-brokers-6", 6, "brokers", false, true),
                new NodeRef(CLUSTER + "-brokers-7", 7, "brokers", false, true)));

        Set<NodeRef> brokerNodes = KC.brokerNodes();
        assertThat(brokerNodes.size(), is(5));
        assertThat(brokerNodes, hasItems(new NodeRef(CLUSTER + "-mixed-3", 3, "mixed", true, true),
                new NodeRef(CLUSTER + "-mixed-4", 4, "mixed", true, true),
                new NodeRef(CLUSTER + "-brokers-5", 5, "brokers", false, true),
                new NodeRef(CLUSTER + "-brokers-6", 6, "brokers", false, true),
                new NodeRef(CLUSTER + "-brokers-7", 7, "brokers", false, true)));

        Set<NodeRef> controllerNodes = KC.controllerNodes();
        assertThat(controllerNodes.size(), is(5));
        assertThat(controllerNodes, hasItems(new NodeRef(CLUSTER + "-controllers-0", 0, "controllers", true, false),
                new NodeRef(CLUSTER + "-controllers-1", 1, "controllers", true, false),
                new NodeRef(CLUSTER + "-controllers-2", 2, "controllers", true, false),
                new NodeRef(CLUSTER + "-mixed-3", 3, "mixed", true, true),
                new NodeRef(CLUSTER + "-mixed-4", 4, "mixed", true, true)));

        Map<String, KafkaNodePoolStatus> statuses = KC.nodePoolStatuses();
        assertThat(statuses.size(), is(3));
        assertThat(statuses.get("controllers").getReplicas(), is(3));
        assertThat(statuses.get("controllers").getLabelSelector(), is("strimzi.io/cluster=foo,strimzi.io/name=foo-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=controllers"));
        assertThat(statuses.get("controllers").getNodeIds().size(), is(3));
        assertThat(statuses.get("controllers").getNodeIds(), hasItems(0, 1, 2));
        assertThat(statuses.get("controllers").getRoles().size(), is(1));
        assertThat(statuses.get("controllers").getRoles(), hasItems(ProcessRoles.CONTROLLER));
        assertThat(statuses.get("mixed").getReplicas(), is(2));
        assertThat(statuses.get("mixed").getLabelSelector(), is("strimzi.io/cluster=foo,strimzi.io/name=foo-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=mixed"));
        assertThat(statuses.get("mixed").getNodeIds().size(), is(2));
        assertThat(statuses.get("mixed").getNodeIds(), hasItems(3, 4));
        assertThat(statuses.get("mixed").getRoles().size(), is(2));
        assertThat(statuses.get("mixed").getRoles(), hasItems(ProcessRoles.CONTROLLER, ProcessRoles.BROKER));
        assertThat(statuses.get("brokers").getReplicas(), is(3));
        assertThat(statuses.get("brokers").getLabelSelector(), is("strimzi.io/cluster=foo,strimzi.io/name=foo-kafka,strimzi.io/kind=Kafka,strimzi.io/pool-name=brokers"));
        assertThat(statuses.get("brokers").getNodeIds().size(), is(3));
        assertThat(statuses.get("brokers").getNodeIds(), hasItems(5, 6, 7));
        assertThat(statuses.get("brokers").getRoles().size(), is(1));
        assertThat(statuses.get("brokers").getRoles(), hasItems(ProcessRoles.BROKER));
    }
}
