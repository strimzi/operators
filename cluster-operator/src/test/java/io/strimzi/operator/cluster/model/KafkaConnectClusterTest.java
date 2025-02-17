/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.PodDNSConfigBuilder;
import io.fabric8.kubernetes.api.model.PodDNSConfigOptionBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.DnsPolicy;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
@ParallelSuite
public class KafkaConnectClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider(Map.of(
        "NO_PROXY", new EnvVarBuilder().withName("NO_PROXY").withValue("127.0.0.1").build(),
        "NON_SHARED_VAR", new EnvVarBuilder().withName("NON_SHARED_VAR").withValue("foo").build()
    ));

    private final String namespace = "test";
    private final String clusterName = "foo";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final int healthDelay = 100;
    private final int healthTimeout = 10;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String metricsCMName = "metrics-cm";
    private final ConfigMap metricsCM = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm(metricsCmJson, metricsCMName, "metrics-config.yml");
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", metricsCMName);
    private final String configurationJson = "foo: bar";
    private final String bootstrapServers = "foo-kafka:9092";
    private final String kafkaHeapOpts = "-Xms" + JvmOptionUtils.DEFAULT_JVM_XMS;

    private final OrderedProperties defaultConfiguration = new OrderedProperties()
            .addPair("offset.storage.topic", "connect-cluster-offsets")
            .addPair("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .addPair("config.storage.topic", "connect-cluster-configs")
            .addPair("key.converter", "org.apache.kafka.connect.json.JsonConverter")
            .addPair("group.id", "connect-cluster")
            .addPair("status.storage.topic", "connect-cluster-status");

    private final OrderedProperties expectedConfiguration = new OrderedProperties()
            .addMapPairs(defaultConfiguration.asMap())
            .addPair("foo", "bar");

    private final KafkaConnect resource = new KafkaConnectBuilder(ResourceUtils.createEmptyKafkaConnect(namespace, clusterName))
            .withNewSpec()
                .withConfig((Map<String, Object>) ReadWriteUtils.readObjectFromYamlString(configurationJson, Map.class))
                .withImage(image)
                .withReplicas(replicas)
                .withReadinessProbe(new Probe(healthDelay, healthTimeout))
                .withLivenessProbe(new Probe(healthDelay, healthTimeout))
                .withBootstrapServers(bootstrapServers)
            .endSpec()
            .build();

    private final KafkaConnect resourceWithMetrics = new KafkaConnectBuilder(resource)
            .editSpec()
                .withMetricsConfig(jmxMetricsConfig)
            .endSpec()
            .build();

    private final KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resourceWithMetrics, VERSIONS, SHARED_ENV_PROVIDER);

    @ParallelTest
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCM, null));
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertThat(metricsCm.getData().get(MetricsModel.CONFIG_MAP_KEY), is(metricsCmJson));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.modifiableMap(Labels.STRIMZI_CLUSTER_LABEL, this.clusterName,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaConnectCluster.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectCluster.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, this.clusterName,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.clusterName,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaConnectResources.componentName(clusterName));
    }

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION).withValue(expectedConfiguration.asPairs()).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        expected.add(new EnvVarBuilder().withName("NO_PROXY").withValue("127.0.0.1").build());
        return expected;
    }

    @ParallelTest
    public void testDefaultValues() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ResourceUtils.createEmptyKafkaConnect(namespace, clusterName), VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.image, is(KafkaVersionTestUtils.DEFAULT_KAFKA_CONNECT_IMAGE));
        assertThat(kc.getReplicas(), is(3));
        assertThat(kc.readinessProbeOptions.getInitialDelaySeconds(), is(60));
        assertThat(kc.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(kc.livenessProbeOptions.getInitialDelaySeconds(), is(60));
        assertThat(kc.livenessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(kc.configuration.asOrderedProperties(), is(defaultConfiguration));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(kc.getReplicas(), is(replicas));
        assertThat(kc.image, is(image));
        assertThat(kc.readinessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kc.readinessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kc.livenessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kc.livenessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kc.configuration.asOrderedProperties(), is(expectedConfiguration));
        assertThat(kc.bootstrapServers, is(bootstrapServers));
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(kc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testGenerateService()   {
        Service svc = kc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(kc.getComponentName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getMetadata().getAnnotations().size(), is(0));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));

        TestUtils.checkOwnerReference(svc, resource);
    }

    @ParallelTest
    public void testGenerateHeadlessService()   {
        KafkaConnect resource = new KafkaConnectBuilder(resourceWithMetrics)
                .editSpec()
                    .withNewTemplate()
                        .withNewHeadlessService()
                            .withNewMetadata()
                                .withLabels(Map.of("label1", "label-value1"))
                                .withAnnotations(Map.of("anno1", "anno-value1"))
                            .endMetadata()
                        .endHeadlessService()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        Service svc = kc.generateHeadlessService();

        Map<String, String> expectedLabels = expectedLabels(kc.getComponentName());
        expectedLabels.put("label1", "label-value1");

        assertThat(svc.getMetadata().getLabels(), is(expectedLabels));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of("anno1", "anno-value1")));
        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getClusterIP(), is("None"));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        TestUtils.checkOwnerReference(svc, resource);
    }

    @ParallelTest
    public void testPodSetWithRack() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resourceWithMetrics)
                .editOrNewSpec()
                    .withNewRack()
                        .withTopologyKey("topology-key")
                    .endRack()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            // check that pod spec contains the init Kafka container
            List<Container> initContainers = pod.getSpec().getInitContainers();
            assertThat(initContainers, is(notNullValue()));
            assertThat(initContainers.size() > 0, is(true));

            boolean isInitKafkaConnect =
                    initContainers.stream().anyMatch(container -> container.getName().equals(KafkaConnectCluster.INIT_NAME));
            assertThat(isInitKafkaConnect, is(true));
        });
    }

    @ParallelTest
    public void withAffinity() throws IOException {
        ResourceTester<KafkaConnect, KafkaConnectCluster> resourceTester = new ResourceTester<>(KafkaConnect.class, VERSIONS, (connect, lookup) -> KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, connect, lookup, SHARED_ENV_PROVIDER), this.getClass().getSimpleName() + ".withAffinity");
        resourceTester.assertDesiredModel("-StrimziPodSet.yaml", kcc -> {
            StrimziPodSet podSet = kcc.generatePodSet(replicas, Map.of(), Map.of(), true, null, null, null);
            return PodSetUtils.mapToPod(podSet.getSpec().getPods().get(0)).getSpec().getAffinity();
        });
    }

    @ParallelTest
    public void withTolerations() throws IOException {
        ResourceTester<KafkaConnect, KafkaConnectCluster> resourceTester = new ResourceTester<>(KafkaConnect.class, VERSIONS, (connect, lookup) -> KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, connect, lookup, SHARED_ENV_PROVIDER), this.getClass().getSimpleName() + ".withTolerations");
        resourceTester.assertDesiredModel("-StrimziPodSet.yaml", kcc -> {
            StrimziPodSet podSet = kcc.generatePodSet(replicas, Map.of(), Map.of(), true, null, null, null);
            return PodSetUtils.mapToPod(podSet.getSpec().getPods().get(0)).getSpec().getTolerations();
        });
    }

    @ParallelTest
    public void testPodSetWithTls() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                    .endTls()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("my-another-secret"));

            List<Container> containers = pod.getSpec().getContainers();
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-another-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS),
                    is("my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS), is("true"));
        });
    }

    @ParallelTest
    public void testPodSetWithTlsAuth() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("user-secret")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("user-secret"));

            List<Container> containers = pod.getSpec().getContainers();

            assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "user-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT), is("user-secret/user.crt"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY), is("user-secret/user.key"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS), is("true"));
        });
    }

    @ParallelTest
    public void testPodSetWithTlsSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("my-secret")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            // 3 = 1 temp volume + 1 volume from logging/metrics + just 1 from above certs Secret
            assertThat(pod.getSpec().getVolumes().size(), is(3));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
        });
    }

    @ParallelTest
    public void testPodSetWithScramSha512Auth() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("user1-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            List<Container> containers = pod.getSpec().getContainers();
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("scram-sha-512"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testPodSetWithScramSha512AuthAndTLSSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
            .editSpec()
                .editOrNewTls()
                    .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("user1.password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
            .endSpec()
            .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(3));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));

            List<Container> containers = pod.getSpec().getContainers();

            assertThat(containers.get(0).getVolumeMounts().size(), is(4));
            assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
            assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-512"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
        });
    }

    @ParallelTest
    public void testPodSetWithScramSha256Auth() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewKafkaClientAuthenticationScramSha256()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("user1-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha256()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            List<Container> containers = pod.getSpec().getContainers();
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("scram-sha-256"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testPodSetWithScramSha256AuthAndTLSSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                    .endTls()
                    .withNewKafkaClientAuthenticationScramSha256()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("my-secret")
                            .withPassword("user1.password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha256()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(3));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));

            List<Container> containers = pod.getSpec().getContainers();

            assertThat(containers.get(0).getVolumeMounts().size(), is(4));
            assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
            assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-256"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
        });
    }

    @ParallelTest
    public void testPodSetWithPlainAuth() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("user1-secret")
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
            .endSpec()
            .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            List<Container> containers = pod.getSpec().getContainers();
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("plain"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for plain client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testPodSetWithPlainAuthAndTLSSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
            .editSpec()
                .editOrNewTls()
                    .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("user1.password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
            .endSpec()
            .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(3));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));

            List<Container> containers = pod.getSpec().getContainers();

            assertThat(containers.get(0).getVolumeMounts().size(), is(4));
            assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
            assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is("my-secret"));
            assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "plain"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
        });
    }

    @ParallelTest
    public void testPodSet()   {
        KafkaConnect resource = new KafkaConnectBuilder(resourceWithMetrics)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(Map.of("label1", "label-value1"))
                                .withAnnotations(Map.of("anno1", "anno-value1"))
                            .endMetadata()
                        .endPodSet()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet ps = kc.generatePodSet(3, Map.of("anno2", "anno-value2"), Map.of("anno3", "anno-value3"), false, null, null, null);

        assertThat(ps.getMetadata().getName(), is(KafkaConnectResources.componentName(clusterName)));
        assertThat(ps.getMetadata().getLabels().entrySet().containsAll(kc.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(ps.getMetadata().getAnnotations(), is(Map.of("anno1", "anno-value1", "anno2", "anno-value2")));
        TestUtils.checkOwnerReference(ps, resource);
        assertThat(ps.getSpec().getSelector().getMatchLabels(), is(kc.getSelectorLabels().withStrimziPodSetController(KafkaConnectResources.componentName(clusterName)).toMap()));
        assertThat(ps.getSpec().getPods().size(), is(3));

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(kc.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(kc.getComponentName()).toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(2));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("anno3"), is("anno-value3"));

            assertThat(pod.getSpec().getHostname(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getSubdomain(), is(KafkaConnectResources.componentName(clusterName)));
            assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
            assertThat(pod.getSpec().getVolumes().stream()
                    .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                    .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getName(), is(KafkaConnectResources.componentName(this.clusterName)));
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is(kc.image));
            assertThat(pod.getSpec().getContainers().get(0).getEnv(), is(getExpectedEnvVars()));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(healthTimeout));
            assertThat(pod.getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(healthDelay));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(healthTimeout));
            assertThat(pod.getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(healthDelay));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().size(), is(2));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getContainerPort(), is(KafkaConnectCluster.REST_API_PORT));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getPorts().get(0).getProtocol(), is("TCP"));
        }
    }

    @ParallelTest
    @SuppressWarnings({"checkstyle:methodlength"})
    public void testTemplate() {
        Map<String, String> spsLabels = Map.of("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(spsLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> spsAnnos = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> pdbLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> pdbAnots = Map.of("a7", "v7", "a8", "v8");

        Map<String, String> crbLabels = Map.of("l9", "v9", "l10", "v10");
        Map<String, String> crbAnots = Map.of("a9", "v9", "a10", "v10");

        Map<String, String> saLabels = Map.of("l11", "v11", "l12", "v12");
        Map<String, String> saAnots = Map.of("a11", "v11", "a12", "v12");

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
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
                .build();

        DnsPolicy dnsPolicy = DnsPolicy.NONE;
        PodDNSConfig dnsConfig = new PodDNSConfigBuilder()
            .withNameservers("192.0.2.1")
            .withSearches("ns1.svc.cluster-domain.example", "my.dns.search.suffix")
            .withOptions(
                new PodDNSConfigOptionBuilder()
                        .withName("ndots")
                        .withValue("2")
                        .build(), 
                new PodDNSConfigOptionBuilder()
                        .withName("edns0")
                        .build()
            )
            .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
                .build();
        
        ConfigMapVolumeSource configMap = new ConfigMapVolumeSourceBuilder()
                .withName("configMap1")
                .build();
        
        SecretVolumeSource secret = new SecretVolumeSourceBuilder()
                .withSecretName("secret1")
                .build();
        
        EmptyDirVolumeSource emptyDir = new EmptyDirVolumeSourceBuilder()
                .withMedium("Memory")
                .build();
        
        AdditionalVolume additionalVolumeConfigMap = new AdditionalVolumeBuilder()
                .withName("config-map-volume-name")
                .withConfigMap(configMap)
                .build();
        
        AdditionalVolume additionalVolumeSecret = new AdditionalVolumeBuilder()
                .withName("secret-volume-name")
                .withSecret(secret)
                .build();
        
        AdditionalVolume additionalVolumeEmptyDir = new AdditionalVolumeBuilder()
                .withName("empty-dir-volume-name")
                .withEmptyDir(emptyDir)
                .build();
        
        VolumeMount additionalVolumeMountConfigMap = new VolumeMountBuilder()
                .withName("config-map-volume-name")
                .withMountPath("/mnt/config")
                .withSubPath("def")
                .build();
        
        VolumeMount additionalVolumeMountSecret = new VolumeMountBuilder()
                .withName("secret-volume-name")
                .withMountPath("/mnt/secret")
                .withSubPath("abc")
                .build();
        
        VolumeMount additionalVolumeMountEmptyDir = new VolumeMountBuilder()
                .withName("empty-dir-volume-name")
                .withMountPath("/mnt/empty-dir")
                .withSubPath("def")
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewRack("my-topology-key")
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(spsLabels)
                                .withAnnotations(spsAnnos)
                            .endMetadata()
                        .endPodSet()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(podLabels)
                                .withAnnotations(podAnots)
                            .endMetadata()
                            .withPriorityClassName("top-priority")
                            .withSchedulerName("my-scheduler")
                            .withHostAliases(hostAlias1, hostAlias2)
                            .withDnsPolicy(dnsPolicy)
                            .withDnsConfig(dnsConfig)
                            .withTopologySpreadConstraints(tsc1, tsc2)
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                            .withVolumes(additionalVolumeSecret, additionalVolumeEmptyDir, additionalVolumeConfigMap)
                        .endPod()
                        .withNewConnectContainer()
                            .withVolumeMounts(additionalVolumeMountSecret, additionalVolumeMountEmptyDir)
                        .endConnectContainer()
                        .withNewInitContainer()
                            .withVolumeMounts(additionalVolumeMountConfigMap)
                        .endInitContainer()
                        .withNewApiService()
                            .withNewMetadata()
                                .withLabels(svcLabels)
                                .withAnnotations(svcAnots)
                            .endMetadata()
                            .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                            .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                        .endApiService()
                        .withNewPodDisruptionBudget()
                            .withNewMetadata()
                                .withLabels(pdbLabels)
                                .withAnnotations(pdbAnots)
                            .endMetadata()
                        .endPodDisruptionBudget()
                        .withNewClusterRoleBinding()
                            .withNewMetadata()
                                .withLabels(crbLabels)
                                .withAnnotations(crbAnots)
                            .endMetadata()
                        .endClusterRoleBinding()
                        .withNewServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnots)
                            .endMetadata()
                        .endServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
            assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(spsAnnos.entrySet()), is(true));

            // Check Pods
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
            assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
            assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
            assertThat(pod.getSpec().getDnsPolicy(), is(DnsPolicy.NONE.toValue()));
            assertThat(pod.getSpec().getDnsConfig(), is(dnsConfig));
            assertThat(pod.getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
            assertThat(pod.getSpec().getEnableServiceLinks(), is(false));


            assertThat(pod.getSpec().getVolumes().size(), is(6));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getVolumes().get(1).getConfigMap().getName(), is("foo-connect-config"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("1Mi")));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("secret-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(3).getSecret().getSecretName(), is("secret1"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("empty-dir-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(4).getEmptyDir().getMedium(), is("Memory"));
            assertThat(pod.getSpec().getVolumes().get(5).getName(), is("config-map-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(5).getConfigMap().getName(), is("configMap1"));

            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().size(), is(2));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(0).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(1).getName(), is("config-map-volume-name"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/mnt/config"));

            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(5));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is("/opt/kafka/init"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("secret-volume-name"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/mnt/secret"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getName(), is("empty-dir-volume-name"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(4).getMountPath(), is("/mnt/empty-dir"));
        });

        // Check Service
        Service svc = kc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kc.generateClusterRoleBinding();
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testExternalConfigurationSecretEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withEnv(env)
                .endExternalConfiguration()

                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);

        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
            assertThat(selected.get(0).getValueFrom().getSecretKeyRef(), is(env.getValueFrom().getSecretKeyRef()));
        });
    }

    @ParallelTest
    public void testExternalConfigurationConfigEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);

        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
            assertThat(selected.get(0).getValueFrom().getConfigMapKeyRef(), is(env.getValueFrom().getConfigMapKeyRef()));
        });
    }

    @ParallelTest
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationSecretVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
        });
    }

    @ParallelTest
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationSecretVolumesWithDots() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my.volume")
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my.secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my.volume"));
        });
    }

    @ParallelTest
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationConfigVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
        });
    }

    @ParallelTest
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationConfigVolumesWithDots() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my.volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my.map").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my.volume"));
        });
    }

    @ParallelTest
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationInvalidVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(0));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(0));
        });
    }

    @ParallelTest
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testNoExternalConfigurationVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withVolumes(volume)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(0));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(0));
        });
    }

    @ParallelTest
    public void testInvalidExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(0));
        });
    }

    @ParallelTest
    public void testNoExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(0));
        });
    }

    @ParallelTest
    public void testGracePeriod() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
        });
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        });
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        });
    }

    @ParallelTest
    public void testImagePullSecretsCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, List.of(secret1, secret2), null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(2));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(true));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        });
    }

    @ParallelTest
    public void testImagePullSecretsBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, List.of(secret1), null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        });
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets(), is(List.of()));
        });
    }

    @ParallelTest
    public void testSecurityContext() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
            assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(123L));
            assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(456L));
            assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(789L));
        });
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
        });
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        kc.securityProvider = new RestrictedPodSecurityProvider();
        kc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
            assertThat(pod.getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
        });
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(0)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(1)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, ImagePullPolicy.ALWAYS, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        });

        podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, ImagePullPolicy.IFNOTPRESENT, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        });
    }

    @ParallelTest
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getResources().getLimits(), is(limits));
            assertThat(cont.getResources().getRequests(), is(requests));
        });
    }

    @ParallelTest
    public void testJvmOptions() {
        Map<String, String> xx = new HashMap<>(2);
        xx.put("UseG1GC", "true");
        xx.put("MaxGCPauseMillis", "20");

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewJvmOptions()
                        .withXms("512m")
                        .withXmx("1024m")
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"), is(true));
        });
    }

    @ParallelTest
    public void testKafkaConnectContainerEnvVars() {

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

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaConnectContainer = new ContainerTemplate();
        kafkaConnectContainer.setEnv(testEnvs);

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaConnectContainer)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testKafkaContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaConnectContainer = new ContainerTemplate();
        kafkaConnectContainer.setEnv(testEnvs);

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaConnectContainer)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER).getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));
    }

    @ParallelTest
    public void testKafkaConnectContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .editOrNewTemplate()
                        .withNewConnectContainer()
                            .withSecurityContext(securityContext)
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaConnectCluster kcc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kcc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getContainers(),
                    hasItem(allOf(
                            hasProperty("name", equalTo(clusterName + "-connect")),
                            hasProperty("securityContext", equalTo(securityContext))
                    )));
        });
    }

    @ParallelTest
    public void testOpenTelemetryTracing() {
        KafkaConnectBuilder builder = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewOpenTelemetryTracing()
                    .endOpenTelemetryTracing()
                .endSpec();
        KafkaConnect resource = builder.build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(env -> KafkaConnectCluster.ENV_VAR_STRIMZI_TRACING.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals(OpenTelemetryTracing.TYPE_OPENTELEMETRY), is(true));
            assertThat(cont.getEnv().stream().filter(env -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("consumer.interceptor.classes=" + OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME), is(true));
            assertThat(cont.getEnv().stream().filter(env -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("producer.interceptor.classes=" + OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME), is(true));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithAccessToken() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withNewAccessToken()
                                        .withSecretName("my-token-secret")
                                        .withKey("my-token-key")
                                    .endAccessToken()
                                    .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().isEmpty(), is(true));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithRefreshToken() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withConnectTimeoutSeconds(15)
                                .withReadTimeoutSeconds(15)
                                .withHttpRetries(2)
                                .withHttpRetryPauseMs(500)
                                .withEnableMetrics(true)
                                .withIncludeAcceptHeader(false)
                                .withNewRefreshToken()
                                    .withSecretName("my-token-secret")
                                    .withKey("my-token-key")
                                .endRefreshToken()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server",
                            ClientConfig.OAUTH_CONNECT_TIMEOUT_SECONDS, "15", ClientConfig.OAUTH_READ_TIMEOUT_SECONDS, "15", ClientConfig.OAUTH_HTTP_RETRIES, "2", ClientConfig.OAUTH_HTTP_RETRY_PAUSE_MILLIS, "500", ClientConfig.OAUTH_ENABLE_METRICS, "true", ClientConfig.OAUTH_INCLUDE_ACCEPT_HEADER, "false")));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithClientSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withAudience("kafka")
                                .withScope("all")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_SCOPE, "all", ClientConfig.OAUTH_AUDIENCE, "kafka")));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithClientSecretAndSaslExtensions() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withAudience("kafka")
                                .withScope("all")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .withSaslExtensions(new TreeMap(Map.of("key1", "value1", "key2", "value2")))
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server",
                            ClientConfig.OAUTH_SCOPE, "all", ClientConfig.OAUTH_AUDIENCE, "kafka", ClientConfig.OAUTH_SASL_EXTENSION_PREFIX + "key1", "value1", ClientConfig.OAUTH_SASL_EXTENSION_PREFIX + "key2", "value2")));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithClientAssertion() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withAudience("kafka")
                                .withScope("all")
                                .withNewClientAssertion()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientAssertion()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_ASSERTION.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_ASSERTION.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_SCOPE, "all", ClientConfig.OAUTH_AUDIENCE, "kafka")));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_CUSTOM_OAUTH_CALLBACK_CLASS.equals(var.getName())).findFirst().orElseThrow().getValue(), is("my-callback-class"));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithUsernameAndPassword() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withClientId("my-client-id")
                                .withUsername("user1")
                                .withNewPasswordSecret()
                                .withSecretName("my-password-secret")
                                .withPassword("user1.password")
                                .endPasswordSecret()
                                .withNewClientSecret()
                                .withSecretName("my-secret-secret")
                                .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-password-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("user1.password"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"",
                            ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_PASSWORD_GRANT_USERNAME, "user1", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                    .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withTokenEndpointUri("http://my-oauth-server")
                                    .build())
                    .endSpec()
                    .build();

            KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                    .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                    .endClientSecret()
                                    .build())
                    .endSpec()
                    .build();

            KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .withDisableTlsHostnameVerification(true)
                                .withTlsTrustedCertificates(cert1, cert2, cert3)
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kc.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

            // Volume mounts
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "second-certificate"));

            // Volumes
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> "oauth-certs-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> "oauth-certs-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "KAFKA_CONNECT_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
        });
    }

    @ParallelTest
    public void testNetworkPolicyWithConnectorOperator() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resourceWithMetrics)
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(true, "operator-namespace", null);

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Map.of()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @ParallelTest
    public void testNetworkPolicyWithConnectorOperatorSameNamespace() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resourceWithMetrics)
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(true, namespace, null);

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @ParallelTest
    public void testNetworkPolicyWithConnectorOperatorWithNamespaceLabels() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resourceWithMetrics)
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        NetworkPolicy np = kc.generateNetworkPolicy(true, "operator-namespace", Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));

        assertThat(np.getMetadata().getName(), is(kc.getComponentName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(MetricsModel.METRICS_PORT));
    }

    @ParallelTest
    public void testNetworkPolicyWithoutConnectorOperator() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.generateNetworkPolicy(false, null, null), is(nullValue()));
    }

    @ParallelTest
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";
        String topologyKey = "topology-key";

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                    .editOrNewMetadata()
                        .withNamespace(testNamespace)
                    .endMetadata()
                    .editOrNewSpec()
                        .withNewRack(topologyKey)
                    .endSpec()
                .build();

        KafkaConnectCluster cluster = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb.getMetadata().getName(), is(KafkaConnectResources.initContainerClusterRoleBindingName(clusterName, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(cluster.componentName));
    }

    @ParallelTest
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editOrNewMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .build();

        KafkaConnectCluster cluster = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb, is(nullValue()));
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.metrics().isEnabled(), is(true));
        assertThat(kc.metrics().getConfigMapName(), is("my-metrics-configuration"));
        assertThat(kc.metrics().getConfigMapKey(), is("config.yaml"));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kc.metrics().isEnabled(), is(false));
        assertThat(kc.metrics().getConfigMapName(), is(nullValue()));
        assertThat(kc.metrics().getConfigMapKey(), is(nullValue()));
    }

    @ParallelTest
    public void testJmxSecretCustomLabelsAndAnnotations() {
        Map<String, String> customLabels = new HashMap<>(2);
        customLabels.put("label1", "value1");
        customLabels.put("label2", "value2");

        Map<String, String> customAnnotations = new HashMap<>(2);
        customAnnotations.put("anno1", "value3");
        customAnnotations.put("anno2", "value4");

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withJmxOptions(new KafkaJmxOptionsBuilder()
                            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder()
                                    .build())
                            .build())
                    .withNewTemplate()
                        .withNewJmxSecret()
                            .withNewMetadata()
                                .withAnnotations(customAnnotations)
                                .withLabels(customLabels)
                            .endMetadata()
                        .endJmxSecret()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaConnectCluster kafkaConnectCluster = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS, SHARED_ENV_PROVIDER);

        Secret jmxSecret = kafkaConnectCluster.jmx().jmxSecret(null);

        for (Map.Entry<String, String> entry : customAnnotations.entrySet()) {
            assertThat(jmxSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry : customLabels.entrySet()) {
            assertThat(jmxSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    @ParallelTest
    public void testKafkaConnectInitContainerSectionIsConfigurable() {
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

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(this.resource)
            .editSpec()
                .withResources(resourceReq)
                .withNewRack()
                    .withTopologyKey("rack-key")
                .endRack()
            .endSpec()
            .build();

        KafkaConnectCluster kcc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS, SHARED_ENV_PROVIDER);

        ResourceRequirements initContainersResources = kcc.createInitContainer(ImagePullPolicy.IFNOTPRESENT).getResources();
        assertThat(initContainersResources.getRequests(), is(requirements));
        assertThat(initContainersResources.getLimits(), is(limits));
    }
}
