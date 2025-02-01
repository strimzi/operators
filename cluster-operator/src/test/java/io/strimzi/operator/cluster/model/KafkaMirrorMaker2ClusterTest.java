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
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.PodDNSConfigBuilder;
import io.fabric8.kubernetes.api.model.PodDNSConfigOptionBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
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
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
@ParallelSuite
public class KafkaMirrorMaker2ClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
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
    private final String targetClusterAlias = "target";
    private final String kafkaHeapOpts = "-Xms" + JvmOptionUtils.DEFAULT_JVM_XMS;

    private final OrderedProperties defaultConfiguration = new OrderedProperties()
            .addPair("config.storage.topic", "mirrormaker2-cluster-configs")
            .addPair("group.id", "mirrormaker2-cluster")
            .addPair("status.storage.topic", "mirrormaker2-cluster-status")
            .addPair("offset.storage.topic", "mirrormaker2-cluster-offsets")
            .addPair("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .addPair("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .addPair("header.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");

    private final OrderedProperties expectedConfiguration = new OrderedProperties()
            .addMapPairs(defaultConfiguration.asMap())
            .addPair("foo", "bar");

    private final KafkaMirrorMaker2ClusterSpec targetCluster = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(targetClusterAlias)
            .withBootstrapServers(bootstrapServers)
            .withConfig((Map<String, Object>) ReadWriteUtils.readObjectFromYamlString(configurationJson, Map.class))
            .build();

    private final KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(ResourceUtils.createEmptyKafkaMirrorMaker2(namespace, clusterName))
            .withNewSpec()
                .withImage(image)
                .withReplicas(replicas)
                .withReadinessProbe(new Probe(healthDelay, healthTimeout))
                .withLivenessProbe(new Probe(healthDelay, healthTimeout))
                .withConnectCluster(targetClusterAlias)
                .withClusters(targetCluster)
                .withMirrors(List.of())
            .endSpec()
            .build();

    private final KafkaMirrorMaker2 resourceWithMetrics = new KafkaMirrorMaker2Builder(resource)
            .editSpec()
                .withMetricsConfig(jmxMetricsConfig)
            .endSpec()
            .build();

    private ConfigMap metricsCm;
    private final KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resourceWithMetrics, VERSIONS, SHARED_ENV_PROVIDER);
    {
        // we were setting metricsEnabled in fromCrd, which was just checking it for non-null. With metrics in CM, we have to check
        // its content, what is done in generateMetricsAndLogConfigMap
        metricsCm = kmm2.generateConnectConfigMap(new MetricsAndLogging(metricsCM, null));
    }

    @ParallelTest
    public void testMetricsConfigMap() {
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap configMap) {
        assertThat(configMap.getData().get(MetricsModel.CONFIG_MAP_KEY), is(metricsCmJson));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("bootstrap.servers=" + bootstrapServers));
        assertThat(connectConfigurations, containsString(expectedConfiguration.asPairs()));
        // MirrorMaker relies on env and file config providers to be configured by default
        assertThat(connectConfigurations, containsString("config.providers=strimzienv,strimzifile"));
        assertThat(connectConfigurations, containsString("config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider"));
        assertThat(connectConfigurations, containsString("config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider"));
    }

    private Map<String, String> expectedLabels(String name)    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, this.clusterName,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaMirrorMaker2Cluster.COMPONENT_TYPE,
                Labels.KUBERNETES_NAME_LABEL, KafkaMirrorMaker2Cluster.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, this.clusterName,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.clusterName,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaMirrorMaker2Resources.componentName(clusterName));
    }

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS).withValue(targetClusterAlias).build());
        return expected;
    }

    @ParallelTest
    public void testDefaultValues() {
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ResourceUtils.createEmptyKafkaMirrorMaker2(namespace, clusterName), VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kmm2.image, is(KafkaVersionTestUtils.DEFAULT_KAFKA_CONNECT_IMAGE));
        assertThat(kmm2.getReplicas(), is(3));
        assertThat(kmm2.readinessProbeOptions.getInitialDelaySeconds(), is(60));
        assertThat(kmm2.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(kmm2.livenessProbeOptions.getInitialDelaySeconds(), is(60));
        assertThat(kmm2.livenessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(kmm2.configuration.asOrderedProperties(), is(defaultConfiguration));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(kmm2.getReplicas(), is(replicas));
        assertThat(kmm2.image, is(image));
        assertThat(kmm2.readinessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kmm2.readinessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kmm2.livenessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kmm2.livenessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kmm2.configuration.asOrderedProperties(), is(expectedConfiguration));
        assertThat(kmm2.bootstrapServers, is(bootstrapServers));
    }

    @ParallelTest
    public void testEnvVars() {
        assertThat(kmm2.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testGenerateService()   {
        Service svc = kmm2.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(kmm2.getComponentName())));
        assertThat(svc.getMetadata().getAnnotations().size(), is(0));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaMirrorMaker2Cluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaMirrorMaker2Cluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));

        TestUtils.checkOwnerReference(svc, resource);
    }

    @ParallelTest
    public void testGenerateServiceWithoutMetrics()   {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withMetricsConfig(null)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        Service svc = kmm2.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(kmm2.getComponentName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaMirrorMaker2Cluster.REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaMirrorMaker2Cluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/port"), is(false));
        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/scrape"), is(false));
        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/path"), is(false));

        TestUtils.checkOwnerReference(svc, resource);
    }

    @ParallelTest
    public void testPodSet()   {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(resourceWithMetrics)
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
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet ps = kmm2.generatePodSet(3, Map.of("anno2", "anno-value2"), Map.of("anno3", "anno-value3"), false, null, null, null);

        assertThat(ps.getMetadata().getName(), is(KafkaMirrorMaker2Resources.componentName(clusterName)));
        assertThat(ps.getMetadata().getLabels().entrySet().containsAll(kmm2.labels.withAdditionalLabels(null).toMap().entrySet()), is(true));
        assertThat(ps.getMetadata().getAnnotations(), is(Map.of("anno1", "anno-value1", "anno2", "anno-value2")));
        TestUtils.checkOwnerReference(ps, resource);
        assertThat(ps.getSpec().getSelector().getMatchLabels(), is(kmm2.getSelectorLabels().withStrimziPodSetController(KafkaMirrorMaker2Resources.componentName(clusterName)).toMap()));
        assertThat(ps.getSpec().getPods().size(), is(3));

        // We need to loop through the pods to make sure they have the right values
        List<Pod> pods = PodSetUtils.podSetToPods(ps);
        for (Pod pod : pods)  {
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(kmm2.labels.withStrimziPodName(pod.getMetadata().getName()).withStrimziPodSetController(kmm2.getComponentName()).toMap().entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().size(), is(2));
            assertThat(pod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(notNullValue()));
            assertThat(pod.getMetadata().getAnnotations().get("anno3"), is("anno-value3"));

            assertThat(pod.getSpec().getHostname(), is(pod.getMetadata().getName()));
            assertThat(pod.getSpec().getSubdomain(), is(KafkaMirrorMaker2Resources.componentName(clusterName)));
            assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
            assertThat(pod.getSpec().getVolumes().stream()
                    .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
                    .findFirst().orElseThrow().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

            assertThat(pod.getSpec().getContainers().size(), is(1));
            assertThat(pod.getSpec().getContainers().get(0).getName(), is(KafkaMirrorMaker2Resources.componentName(this.clusterName)));
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is(kmm2.image));
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
    public void withAffinity() throws IOException {
        ResourceTester<KafkaMirrorMaker2, KafkaMirrorMaker2Cluster> resourceTester = new ResourceTester<>(KafkaMirrorMaker2.class, VERSIONS, (kafkaMirrorMaker2, versions) ->
            KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, versions, SHARED_ENV_PROVIDER), this.getClass().getSimpleName() + ".withAffinity");

        resourceTester.assertDesiredModel("-StrimziPodSet.yaml", kmm2 -> {
            StrimziPodSet podSet = kmm2.generatePodSet(replicas, Map.of(), Map.of(), true, null, null, null);
            return PodSetUtils.mapToPod(podSet.getSpec().getPods().get(0)).getSpec().getAffinity();
        });
    }

    @ParallelTest
    public void withTolerations() throws IOException {
        ResourceTester<KafkaMirrorMaker2, KafkaMirrorMaker2Cluster> resourceTester = new ResourceTester<>(KafkaMirrorMaker2.class, VERSIONS, (kafkaMirrorMaker2, versions) ->
            KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, versions, SHARED_ENV_PROVIDER), this.getClass().getSimpleName() + ".withTolerations");

        resourceTester.assertDesiredModel("-StrimziPodSet.yaml", kmm2 -> {
            StrimziPodSet podSet = kmm2.generatePodSet(replicas, Map.of(), Map.of(), true, null, null, null);
            return PodSetUtils.mapToPod(podSet.getSpec().getPods().get(0)).getSpec().getTolerations();
        });
    }

    @ParallelTest
    public void testPodSetWithTls() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithTls = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                .endTls()
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithTls)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("my-another-secret"));

            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-another-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS),
                    is("my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_CLUSTERS), is("true"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS),
                    is("target=my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
        });
    }

    @ParallelTest
    public void testPodSetWithTlsWithoutCerts() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithTls = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewTls()
                .endTls()
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithTls)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS),
                    is(nullValue()));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_CLUSTERS), is("true"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS),
                    is(nullValue()));
        });
    }

    @ParallelTest
    public void testPodSetWithTlsAuth() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithTlsAuth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("user-secret"));

            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "user-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT), is("user-secret/user.crt"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY), is("user-secret/user.key"));
        });
    }

    @ParallelTest
    public void testPodSetWithTlsSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();

        KafkaMirrorMaker2ClusterSpec sourceClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder(targetClusterWithTlsAuth)
                .withAlias("source")
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithTlsAuth, sourceClusterWithTlsAuth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("source-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(5));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
        });
    }

    @ParallelTest
    public void testPodSetWithScramSha512Auth() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("user1-secret")
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithScramSha512Auth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("scram-sha-512"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testPodSetWithScramSha512AuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
            .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
            .editSpec()
                .withClusters(targetClusterWithScramSha512Auth)
            .endSpec()
            .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(4));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(6));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-512"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication for multiple clusters.
     * It checks that the volumes and volume mounts that reference the secret are correctly created and that each volume name and volume mount path is only
     * created once - duplicate volume names and duplicate volume mount paths will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testPodSetWithMultipleClustersScramSha512AuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
            .build();
        KafkaMirrorMaker2ClusterSpec sourceClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder(targetClusterWithScramSha512Auth)
            .withAlias("source")
            .withBootstrapServers("source-bootstrap-kafka:9092")
            .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
            .editSpec()
                .withClusters(targetClusterWithScramSha512Auth, sourceClusterWithScramSha512Auth)
            .endSpec()
            .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("source-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(8));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
            assertThat(cont.getVolumeMounts().get(7).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(7).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "source/my-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-512"));
        });
    }

    @ParallelTest
    public void testPodSetWithScramSha256Auth() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha256Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewKafkaClientAuthenticationScramSha256()
                .withUsername("user1")
                .withNewPasswordSecret()
                .withSecretName("user1-secret")
                .withPassword("password")
                .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha256()
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithScramSha256Auth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("scram-sha-256"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 256 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testPodSetWithScramSha256AuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha256Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithScramSha256Auth)
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(4));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(6));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-256"));
        });
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 256 client authentication for multiple clusters.
     * It checks that the volumes and volume mounts that reference the secret are correctly created and that each volume name and volume mount path is only
     * created once - duplicate volume names and duplicate volume mount paths will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testPodSetWithMultipleClustersScramSha256AuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha256Auth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();
        KafkaMirrorMaker2ClusterSpec sourceClusterWithScramSha256Auth = new KafkaMirrorMaker2ClusterSpecBuilder(targetClusterWithScramSha256Auth)
                .withAlias("source")
                .withBootstrapServers("source-bootstrap-kafka:9092")
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithScramSha256Auth, sourceClusterWithScramSha256Auth)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("source-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(8));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(6).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + "source/my-secret"));
            assertThat(cont.getVolumeMounts().get(7).getName(), is("source-my-secret"));
            assertThat(cont.getVolumeMounts().get(7).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + "source/my-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-256"));
        });
    }

    @ParallelTest
    public void testPodSetWithPlainAuth() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithPlainAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("user1-secret")
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()  
                    .withClusters(targetClusterWithPlainAuth)                
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("user1-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont).get(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("plain"));
        });
    }


    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for plain client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testPodSetWithPlainAuthAndTLSSameSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithPlainAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
            .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
            .editSpec()
                .withClusters(targetClusterWithPlainAuth)
            .endSpec()
            .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getVolumes().size(), is(4));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("my-secret"));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("target-my-secret"));

            Container cont = pod.getSpec().getContainers().get(0);

            assertThat(cont.getVolumeMounts().size(), is(6));
            assertThat(cont.getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(cont.getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(cont.getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(cont.getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(cont.getVolumeMounts().get(2).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMaker2Cluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getName(), is("my-secret"));
            assertThat(cont.getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMaker2Cluster.PASSWORD_VOLUME_MOUNT + "my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getName(), is("target-my-secret"));
            assertThat(cont.getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT + targetClusterAlias + "/my-secret"));

            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
            assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(cont), hasEntry(KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "plain"));
        });
    }

    @ParallelTest
    @SuppressWarnings({"checkstyle:methodlength"})
    public void testTemplate() {
        Map<String, String> podSetLabels = Map.of("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedPodSetLabels = new HashMap<>(podSetLabels);
        expectedPodSetLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> podSetAnnos = Map.of("a1", "v1", "a2", "v2");

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

        ConfigMapVolumeSource configMap = new ConfigMapVolumeSourceBuilder()
                .withName("configMap1")
                .build();

        PersistentVolumeClaimVolumeSource pvc = new PersistentVolumeClaimVolumeSourceBuilder()
                .withClaimName("pvc-name")
                .withReadOnly(true)
                .build();

        AdditionalVolume additionalVolumeConfigMap = new AdditionalVolumeBuilder()
                .withName("config-map-volume-name")
                .withConfigMap(configMap)
                .build();

        AdditionalVolume additionalVolumePvc = new AdditionalVolumeBuilder()
                .withName("pvc-volume-name")
                .withPersistentVolumeClaim(pvc)
                .build();

        VolumeMount additionalVolumeMountConfigMap = new VolumeMountBuilder()
                .withName("config-map-volume-name")
                .withMountPath("/mnt/myconfigmap")
                .withSubPath("def")
                .build();

        VolumeMount additionalVolumeMountPvc = new VolumeMountBuilder()
                .withName("pvc-volume-name")
                .withMountPath("/mnt/mypvc")
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewRack("my-topology-key")
                    .withNewTemplate()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(podSetLabels)
                                .withAnnotations(podSetAnnos)
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
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                            .withVolumes(additionalVolumeConfigMap, additionalVolumePvc)
                        .endPod()
                        .withNewInitContainer()
                            .withVolumeMounts(additionalVolumeMountPvc)
                        .endInitContainer()
                        .withNewConnectContainer()
                            .withVolumeMounts(additionalVolumeMountConfigMap)
                        .endConnectContainer()
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
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        assertThat(podSet.getMetadata().getLabels().entrySet().containsAll(expectedPodSetLabels.entrySet()), is(true));
        assertThat(podSet.getMetadata().getAnnotations().entrySet().containsAll(podSetAnnos.entrySet()), is(true));

        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            // Check Pods
            assertThat(pod.getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
            assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
            assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
            assertThat(pod.getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
            assertThat(pod.getSpec().getDnsPolicy(), is(DnsPolicy.NONE.toValue()));
            assertThat(pod.getSpec().getDnsConfig(), is(dnsConfig));
            assertThat(pod.getSpec().getEnableServiceLinks(), is(false));

            assertThat(pod.getSpec().getVolumes().size(), is(5));
            assertThat(pod.getSpec().getVolumes().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
            assertThat(pod.getSpec().getVolumes().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getVolumes().get(1).getConfigMap().getName(), is("foo-mirrormaker2-config"));
            assertThat(pod.getSpec().getVolumes().get(2).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir(), is(notNullValue()));
            assertThat(pod.getSpec().getVolumes().get(2).getEmptyDir().getSizeLimit(), is(new Quantity("1Mi")));
            assertThat(pod.getSpec().getVolumes().get(3).getName(), is("config-map-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(3).getConfigMap().getName(), is("configMap1"));
            assertThat(pod.getSpec().getVolumes().get(4).getName(), is("pvc-volume-name"));
            assertThat(pod.getSpec().getVolumes().get(4).getPersistentVolumeClaim().getClaimName(), is("pvc-name"));

            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().size(), is(2));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(0).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/opt/kafka/init"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(1).getName(), is("pvc-volume-name"));
            assertThat(pod.getSpec().getInitContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/mnt/mypvc"));

            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(4));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("kafka-connect-configurations"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is("rack-volume"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is("/opt/kafka/init"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getName(), is("config-map-volume-name"));
            assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(3).getMountPath(), is("/mnt/myconfigmap"));
        });

        // Check Service
        Service svc = kmm2.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kmm2.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kmm2.generateClusterRoleBinding();
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kmm2.generateServiceAccount();
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withEnv(env)
                .endExternalConfiguration()

                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
        });
    }

    @ParallelTest
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testExternalConfigurationConfigVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withVolumes(volume)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(1));
            assertThat(selected.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(1));
            assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
            assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(0));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selectedVolumeMounts.size(), is(0));
        });
    }

    @ParallelTest
    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    public void testNoExternalConfigurationVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withVolumes(volume)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<Volume> volumes = pod.getSpec().getVolumes();
            List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
            assertThat(selected.size(), is(0));

            List<VolumeMount> volumeMounts = pod.getSpec().getContainers().get(0).getVolumeMounts();
            List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaMirrorMaker2Cluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).toList();
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            List<EnvVar> envs = pod.getSpec().getContainers().get(0).getEnv();
            List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).toList();
            assertThat(selected.size(), is(0));
        });
    }

    @ParallelTest
    public void testGracePeriod() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(123L));
        });
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource).build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        });
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, List.of(secret1, secret2), null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, List.of(secret1), null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets().size(), is(1));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret1), is(false));
            assertThat(pod.getSpec().getImagePullSecrets().contains(secret2), is(true));
        });
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource).build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getImagePullSecrets(), is(List.of()));
        });
    }

    @ParallelTest
    public void testSecurityContext() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(notNullValue()));
            assertThat(pod.getSpec().getSecurityContext().getFsGroup(), is(123L));
            assertThat(pod.getSpec().getSecurityContext().getRunAsGroup(), is(456L));
            assertThat(pod.getSpec().getSecurityContext().getRunAsUser(), is(789L));
        });
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource).build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
        });
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        kmm2.securityProvider = new RestrictedPodSecurityProvider();
        kmm2.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kmm2.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(0)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource).build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kmm2.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(1)));
        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, ImagePullPolicy.ALWAYS, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            assertThat(pod.getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        });

        // Check PodSet
        podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, ImagePullPolicy.IFNOTPRESENT, null, null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewJvmOptions()
                        .withXms("512m")
                        .withXmx("1024m")
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"), is(true));
            assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"), is(true));
        });
    }

    @ParallelTest
    public void testKafkaMirrorMaker2ContainerEnvVars() {

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
        ContainerTemplate kafkaMirrorMaker2Container = new ContainerTemplate();
        kafkaMirrorMaker2Container.setEnv(testEnvs);

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaMirrorMaker2Container)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testOpenTelemetryTracing() {
        KafkaMirrorMaker2Builder builder = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withNewOpenTelemetryTracing()
                    .endOpenTelemetryTracing()
                .endSpec();
        KafkaMirrorMaker2 resource = builder.build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check config map
        ConfigMap configMap = kmm2.generateConnectConfigMap(new MetricsAndLogging(metricsCM, null));
        String connectConfigurations = configMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME);
        assertThat(connectConfigurations, containsString("consumer.interceptor.classes=" + OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME));
        assertThat(connectConfigurations, containsString("producer.interceptor.classes=" + OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME));

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(env -> KafkaMirrorMaker2Cluster.ENV_VAR_STRIMZI_TRACING.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals(OpenTelemetryTracing.TYPE_OPENTELEMETRY), is(true));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithAccessToken() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithAccessToken = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                    new KafkaClientAuthenticationOAuthBuilder()
                            .withNewAccessToken()
                                .withSecretName("my-token-secret")
                                .withKey("my-token-key")
                            .endAccessToken()
                            .build())
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithAccessToken)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().isEmpty(), is(true));
            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-token-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @ParallelTest
    public void testPodSetWithAccessTokenLocation() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithServiceAccountOAuth = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withAccessTokenLocation("/var/run/secrets/kubernetes.io/serviceaccount/token")
                                .build())
                .build();

        KafkaMirrorMaker2ClusterSpec sourceClusterWithServiceAccountOAuth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("source-cluster-1")
                .withBootstrapServers("source-bootstrap-kafka:9092")
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withAccessTokenLocation("/var/run/secrets/kubernetes.io/serviceaccount/token")
                                .build())
                .build();

        KafkaMirrorMaker2ClusterSpec sourceCluster2WithServiceAccountOAuth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias("source-cluster-2")
                .withBootstrapServers("source2-bootstrap-kafka:9092")
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withAccessTokenLocation("/var/run/secrets/kubernetes.io/serviceaccount/token")
                                .build())
                .build();

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithServiceAccountOAuth, sourceClusterWithServiceAccountOAuth, sourceCluster2WithServiceAccountOAuth)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\"", ClientConfig.OAUTH_ACCESS_TOKEN_LOCATION, "/var/run/secrets/kubernetes.io/serviceaccount/token")));

            assertThat(cont.getVolumeMounts().stream().filter(var -> var.getName().endsWith("token-secret")).count(), is(0L));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithRefreshToken() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithRefreshToken = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithRefreshToken)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server",
                            ClientConfig.OAUTH_CONNECT_TIMEOUT_SECONDS, "15", ClientConfig.OAUTH_READ_TIMEOUT_SECONDS, "15", ClientConfig.OAUTH_HTTP_RETRIES, "2", ClientConfig.OAUTH_HTTP_RETRY_PAUSE_MILLIS, "500", ClientConfig.OAUTH_ENABLE_METRICS, "true", ClientConfig.OAUTH_INCLUDE_ACCEPT_HEADER, "false")));

            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-token-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithClientSecret() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithClientSecret = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                    new KafkaClientAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withTokenEndpointUri("http://my-oauth-server")
                            .withNewClientSecret()
                                .withSecretName("my-secret-secret")
                                .withKey("my-secret-key")
                            .endClientSecret()
                            .build())
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithClientSecret)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));

            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-secret-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithClientSecretAndSaslExtensions() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithClientSecret = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .withSaslExtensions(new TreeMap(Map.of("key1", "value1", "key2", "value2")))
                                .build())
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithClientSecret)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server",
                            ClientConfig.OAUTH_SASL_EXTENSION_PREFIX + "key1", "value1", ClientConfig.OAUTH_SASL_EXTENSION_PREFIX + "key2", "value2")));

            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-secret-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithClientAssertion() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithClientSecret = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewClientAssertion()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientAssertion()
                                .build())
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithClientSecret)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_ASSERTION.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_ASSERTION.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithUsernameAndPassword() {
        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithUsernameAndPassword = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
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
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                .withClusters(targetClusterWithOAuthWithUsernameAndPassword)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().get(0);
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-password-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("user1.password"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMaker2Cluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                    is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_PASSWORD_GRANT_USERNAME, "user1", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));

            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-password-secret".equals(var.getName())).count(), is(1L));
            assertThat(cont.getVolumeMounts().stream().filter(var -> "target-my-secret-secret".equals(var.getName())).count(), is(1L));
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithMissingClientSecret = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withTokenEndpointUri("http://my-oauth-server")
                                    .build())
                    .build();
            KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                    .editSpec()
                        .withClusters(targetClusterWithOAuthWithMissingClientSecret)
                    .endSpec()
                    .build();

            KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testPodSetWithOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithMissingUri = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
                    .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                    .build();
            KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                    .editSpec()
                        .withClusters(targetClusterWithOAuthWithMissingUri)
                    .endSpec()
                    .build();

            KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
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

        KafkaMirrorMaker2ClusterSpec targetClusterWithOAuthWithTls = new KafkaMirrorMaker2ClusterSpecBuilder(this.targetCluster)
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
                .build();
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withClusters(targetClusterWithOAuthWithTls)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = kmm2.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
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
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "target-oauth-certs-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "target/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "target-oauth-certs-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMaker2Cluster.MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "target/second-certificate"));

            // Volumes
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> "oauth-certs-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> "oauth-certs-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> "target-oauth-certs-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(pod.getSpec().getVolumes().stream().filter(vol -> "target-oauth-certs-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "KAFKA_CONNECT_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
            assertThat(cont.getEnv().stream().filter(e -> "KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS_CLUSTERS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("target=first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
        });
    }

    @ParallelTest
    public void testNetworkPolicy() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resourceWithMetrics)
                .build();
        KafkaMirrorMaker2Cluster kc = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        kc.generateConnectConfigMap(new MetricsAndLogging(metricsCM, null));

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
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resourceWithMetrics)
                .build();
        KafkaMirrorMaker2Cluster kc = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        kc.generateConnectConfigMap(new MetricsAndLogging(metricsCM, null));

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
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resourceWithMetrics)
                .build();
        KafkaMirrorMaker2Cluster kc = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        kc.generateConnectConfigMap(new MetricsAndLogging(metricsCM, null));

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
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker2 kafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(this.resource)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster kmm = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kmm.metrics().isEnabled(), is(true));
        assertThat(kmm.metrics().getConfigMapName(), is("my-metrics-configuration"));
        assertThat(kmm.metrics().getConfigMapKey(), is("config.yaml"));
    }

    @ParallelTest
    public void testJmxSecretCustomLabelsAndAnnotations() {
        Map<String, String> customLabels = new HashMap<>(2);
        customLabels.put("label1", "value1");
        customLabels.put("label2", "value2");

        Map<String, String> customAnnotations = new HashMap<>(2);
        customAnnotations.put("anno1", "value3");
        customAnnotations.put("anno2", "value4");

        KafkaMirrorMaker2 kafkaMirrorMaker2 = new KafkaMirrorMaker2Builder(this.resource)
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

        KafkaMirrorMaker2Cluster kmm2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaMirrorMaker2, VERSIONS, SHARED_ENV_PROVIDER);

        Secret jmxSecret = kmm2.jmx().jmxSecret(null);

        for (Map.Entry<String, String> entry : customAnnotations.entrySet()) {
            assertThat(jmxSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry : customLabels.entrySet()) {
            assertThat(jmxSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        KafkaMirrorMaker2Cluster kmm = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS, SHARED_ENV_PROVIDER);

        assertThat(kmm.metrics().isEnabled(), is(false));
        assertThat(kmm.metrics().getConfigMapName(), is(nullValue()));
        assertThat(kmm.metrics().getConfigMapKey(), is(nullValue()));
    }

    @ParallelTest
    public void testPodSetWithRack() {
        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editOrNewSpec()
                    .withNewRack()
                        .withTopologyKey("topology-key")
                    .endRack()
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);

        // Check PodSet
        StrimziPodSet podSet = cluster.generatePodSet(3, Map.of(), Map.of(), false, null, null, null);
        PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            // check that pod spec contains the init Kafka container
            List<Container> initContainers = pod.getSpec().getInitContainers();
            assertThat(initContainers, is(notNullValue()));
            assertThat(initContainers.size() > 0, is(true));

            boolean isKafkaInitContainer = initContainers.stream().anyMatch(container -> container.getName().equals(KafkaConnectCluster.INIT_NAME));
            assertThat(isKafkaInitContainer, is(true));
        });
    }

    @ParallelTest
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";
        String topologyKey = "topology-key";

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editOrNewMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .editOrNewSpec()
                    .withNewRack(topologyKey)
                .endSpec()
                .build();

        KafkaMirrorMaker2Cluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb.getMetadata().getName(), is(KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(clusterName, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(cluster.componentName));
    }

    @ParallelTest
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        KafkaMirrorMaker2 resource = new KafkaMirrorMaker2Builder(this.resource)
                .editOrNewMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .build();

        KafkaConnectCluster cluster = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb, is(nullValue()));
    }
}
