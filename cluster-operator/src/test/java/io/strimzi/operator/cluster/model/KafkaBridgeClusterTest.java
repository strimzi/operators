/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeConsumerSpecBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeProducerSpecBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.JvmOptionsBuilder;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static io.strimzi.operator.cluster.model.KafkaBridgeCluster.ENV_VAR_KAFKA_INIT_INIT_FOLDER_KEY;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class KafkaBridgeClusterTest {
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    static final String ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM = "KAFKA_BRIDGE_SASL_MECHANISM";
    static final String ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME = "KAFKA_BRIDGE_SASL_USERNAME";
    static final String ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE = "KAFKA_BRIDGE_SASL_PASSWORD_FILE";
    static final String ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_KEY = "KAFKA_BRIDGE_TLS_AUTH_KEY";
    static final String ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_CERT = "KAFKA_BRIDGE_TLS_AUTH_CERT";
    static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_ASSERTION = "KAFKA_BRIDGE_OAUTH_CLIENT_ASSERTION";
    static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_PASSWORD_GRANT_PASSWORD = "KAFKA_BRIDGE_OAUTH_PASSWORD_GRANT_PASSWORD";
    static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG = "KAFKA_BRIDGE_OAUTH_CONFIG";
    static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET = "KAFKA_BRIDGE_OAUTH_CLIENT_SECRET";
    static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN = "KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN";
    static final String ENV_VAR_KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN = "KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN";

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 1;
    private final String image = "my-image:latest";
    private final int healthDelay = 15;
    private final int healthTimeout = 5;
    private final String bootstrapServers = "foo-kafka:9092";

    private final KafkaBridge resource = new KafkaBridgeBuilder(ResourceUtils.createEmptyKafkaBridge(namespace, cluster))
            .withNewSpec()
                .withEnableMetrics(true)
                .withImage(image)
                .withReplicas(replicas)
                .withBootstrapServers(bootstrapServers)
                .withNewHttp(8080)
            .endSpec()
            .build();
    private final KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

    private final MetricsAndLogging metricsAndLogging = new MetricsAndLogging(null, null);

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.modifiableMap(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_COMPONENT_TYPE_LABEL, KafkaBridgeCluster.COMPONENT_TYPE,
                Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND,
                Labels.KUBERNETES_NAME_LABEL, KafkaBridgeCluster.COMPONENT_TYPE,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedServiceLabels(String name)    {
        Map<String, String> serviceLabels = expectedLabels(name);
        serviceLabels.put(Labels.STRIMZI_DISCOVERY_LABEL, "true");

        return serviceLabels;
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels(KafkaBridgeResources.componentName(cluster))).strimziSelectorLabels().toMap();
    }

    protected List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(String.valueOf(JvmOptions.DEFAULT_GC_LOGGING_ENABLED)).build());
        return expected;
    }

    private static Volume getVolume(Deployment dep, String volumeName) {
        return dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(volume -> volumeName.equals(volume.getName())).iterator().next();
    }

    private static VolumeMount getVolumeMount(Container container, String volumeName) {
        return container.getVolumeMounts().stream().filter(volumeMount -> volumeName.equals(volumeMount.getName())).iterator().next();
    }

    @ParallelTest
    public void testDefaultValues() {
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ResourceUtils.createEmptyKafkaBridge(namespace, cluster), SHARED_ENV_PROVIDER);

        assertThat(kbc.image, is("quay.io/strimzi/kafka-bridge:latest"));
        assertThat(kbc.getReplicas(), is(1));
        assertThat(kbc.readinessProbeOptions.getInitialDelaySeconds(), is(15));
        assertThat(kbc.readinessProbeOptions.getTimeoutSeconds(), is(5));
        assertThat(kbc.livenessProbeOptions.getInitialDelaySeconds(), is(15));
        assertThat(kbc.livenessProbeOptions.getTimeoutSeconds(), is(5));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(kbc.getReplicas(), is(replicas));
        assertThat(kbc.image, is(image));
        assertThat(kbc.readinessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kbc.readinessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kbc.livenessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kbc.livenessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(kbc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testGenerateService()   {
        Service svc = kbc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedServiceLabels(kbc.getComponentName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(KafkaBridgeCluster.DEFAULT_REST_API_PORT));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaBridgeCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));

        assertThat(svc.getMetadata().getAnnotations(), is(kbc.getDiscoveryAnnotation(KafkaBridgeCluster.DEFAULT_REST_API_PORT)));

        TestUtils.checkOwnerReference(svc, resource);
    }

    @ParallelTest
    public void testGenerateDeployment()   {
        Deployment dep = kbc.generateDeployment(new HashMap<>(), true, null, null);

        assertThat(dep.getMetadata().getName(), is(KafkaBridgeResources.componentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        Map<String, String> expectedDeploymentLabels = expectedLabels(KafkaBridgeResources.componentName(cluster));
        assertThat(dep.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(dep.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));
        assertThat(dep.getSpec().getReplicas(), is(replicas));
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName(), is(KafkaBridgeResources.componentName(cluster)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is(kbc.image));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv(), is(getExpectedEnvVars()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(healthDelay));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(healthTimeout));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(healthDelay));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(healthTimeout));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort(), is(KafkaBridgeCluster.DEFAULT_REST_API_PORT));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName(), is(KafkaBridgeCluster.REST_API_PORT_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge().getIntVal(), is(1));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable().getIntVal(), is(0));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream()
            .filter(volume -> volume.getName().equalsIgnoreCase("strimzi-tmp"))
            .findFirst().get().getEmptyDir().getSizeLimit(), is(new Quantity(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE)));

        TestUtils.checkOwnerReference(dep, resource);
    }

    @ParallelTest
    public void testGenerateDeploymentWithTls() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                    .endTls()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("my-another-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-another-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS), is("my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsAuth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("user-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "user-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_CERT), is("user-secret/user.crt"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_KEY), is("user-secret/user.key"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsSameSecret() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        // 2 = 1 volume from logging/metrics + just 1 from above certs Secret
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(3));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaBridgeCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME), is("user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM), is("scram-sha-512"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithScramSha256Auth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaBridgeCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME), is("user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM), is("scram-sha-256"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithPlainAuth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaBridgeCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME), is("user1"));
        assertThat(io.strimzi.operator.cluster.TestUtils.containerEnvVars(containers.get(0)).get(ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM), is("plain"));
    }

    @ParallelTest
    @SuppressWarnings({"checkstyle:methodlength"})
    public void testTemplate() {
        Map<String, String> depLabels = Map.of("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> depAnots = Map.of("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = Map.of("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = Map.of("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> pdbLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> pdbAnots = Map.of("a7", "v7", "a8", "v8");

        Map<String, String> saLabels = Map.of("l9", "v9", "l10", "v10");
        Map<String, String> saAnots = Map.of("a9", "v9", "a10", "v10");

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

        TopologySpreadConstraint tsc1 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/zone")
                .withMaxSkew(1)
                .withWhenUnsatisfiable("DoNotSchedule")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
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
                .withMountPath("/mnt/secret")
                .withSubPath("def")
                .build());

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewDeployment()
                            .withNewMetadata()
                                .withLabels(depLabels)
                                .withAnnotations(depAnots)
                            .endMetadata()
                            .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                        .endDeployment()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(podLabels)
                                .withAnnotations(podAnots)
                            .endMetadata()
                            .withPriorityClassName("top-priority")
                            .withSchedulerName("my-scheduler")
                            .withAffinity(affinity)
                            .withTolerations(tolerations)
                            .withTopologySpreadConstraints(tsc1, tsc2)
                            .withEnableServiceLinks(false)
                            .withTmpDirSizeLimit("10Mi")
                            .withVolumes(additionalVolumes)
                        .endPod()
                        .withNewBridgeContainer()
                            .withVolumeMounts(additionalVolumeMounts)
                        .endBridgeContainer()
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
                        .withNewServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnots)
                            .endMetadata()
                        .endServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        // Check Deployment
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getStrategy().getType(), is("Recreate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate(), is(nullValue()));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));
        assertThat(getVolume(dep, VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME).getEmptyDir().getSizeLimit(), is(new Quantity("10Mi")));
        assertThat(getVolume(dep, "secret-volume-name").getSecret(), is(secret));
        assertThat(getVolumeMount(dep.getSpec().getTemplate().getSpec().getContainers().get(0), "secret-volume-name"), is(additionalVolumeMounts.get(0)));

        // Check Service
        Service svc = kbc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kbc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testGracePeriod() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(123L));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(30L));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, secrets);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, singletonList(secret1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(123L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(456L));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(789L));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        kbc.securityProvider = new RestrictedPodSecurityProvider();
        kbc.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(Collections.emptyMap(), true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = kbc.generateDeployment(Collections.emptyMap(), true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        Deployment dep = kbc.generateDeployment(Collections.emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(cont.getResources().getLimits(), is(limits));
        assertThat(cont.getResources().getRequests(), is(requests));
    }

    @ParallelTest
    public void testKafkaBridgeContainerEnvVars() {

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
        ContainerTemplate kafkaBridgeContainer = new ContainerTemplate();
        kafkaBridgeContainer.setEnv(testEnvs);

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withNewTemplate()
                .withBridgeContainer(kafkaBridgeContainer)
                .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithRack() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editOrNewSpec()
                .withNewRack()
                .withTopologyKey("topology-key")
                .endRack()
                .endSpec()
                .build();

        assertRackAwareDeploymentConfigured(resource, "quay.io/strimzi/operator:latest");
    }

    @ParallelTest
    public void testGenerateDeploymentWithRackAndCustomInitImage() {
        final String customImage = "quay.io/strimzi/operator:custom";
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editOrNewSpec()
                .withClientRackInitImage(customImage)
                .withNewRack()
                .withTopologyKey("topology-key")
                .endRack()
                .endSpec()
                .build();

        assertRackAwareDeploymentConfigured(resource, customImage);
    }
    
    @ParallelTest
    public void testGenerateDeploymentWithRackAndInitVolumeMounts() {
        
        ConfigMapVolumeSource configMap = new ConfigMapVolumeSourceBuilder()
                .withName("config-map-name")
                .build();
        
        AdditionalVolume additionalVolume = new AdditionalVolumeBuilder()
                .withName("config-map-volume-name")
                .withConfigMap(configMap)
                .build();
        
        VolumeMount additionalVolumeMount = new VolumeMountBuilder()
                .withName("config-map-volume-name-2")
                .withMountPath("/mnt/config-map")
                .withSubPath("def")
                .build();
        
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editOrNewSpec()
                .withNewRack()
                .withTopologyKey("topology-key")
                .endRack()
                .withNewTemplate()
                .withNewPod()
                    .withVolumes(singletonList(additionalVolume))
                .endPod()
                    .withNewInitContainer()
                        .withVolumeMounts(singletonList(additionalVolumeMount))
                    .endInitContainer()
                .endTemplate()
                .endSpec()
                .build();

        Deployment deployment = assertRackAwareDeploymentConfigured(resource, "quay.io/strimzi/operator:latest");
        assertThat(getVolume(deployment, "config-map-volume-name").getConfigMap(), is(configMap));
        assertThat(getVolumeMount(deployment.getSpec().getTemplate().getSpec().getInitContainers().get(0), "config-map-volume-name-2"), is(additionalVolumeMount));

    }
    
    private static Deployment assertRackAwareDeploymentConfigured(final KafkaBridge resource, final String expectedInitImage) {
        KafkaBridgeCluster bridgeCluster = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment deployment = bridgeCluster.generateDeployment(new HashMap<>(), false, null, null);

        assertThat(resource.getSpec().getRack(), is(notNullValue()));
        PodSpec podSpec = deployment.getSpec().getTemplate().getSpec();
        
        List<Container> containers = podSpec.getContainers();
        assertThat(containers, is(notNullValue()));
        assertThat(containers, hasSize(1));
        final Optional<Container> maybeContainer = containers.stream().filter(container -> container.getName().equals(bridgeCluster.getComponentName())).findFirst();
        assertThat(maybeContainer.isPresent(), is(true));
        final Container bridgeContainer = maybeContainer.get();

        assertThat(bridgeContainer.getVolumeMounts(), hasSize(3));
        final Optional<VolumeMount> volumeMountOptional = bridgeContainer.getVolumeMounts().stream().filter(volumeMount -> volumeMount.getName().equals("rack-volume")).findFirst();
        assertThat(volumeMountOptional.isPresent(), is(true));
        final VolumeMount bridgeVolumeMount = volumeMountOptional.get();
        assertThat(bridgeVolumeMount.getName(), is(KafkaBridgeCluster.INIT_VOLUME_NAME));
        assertThat(bridgeVolumeMount.getMountPath(), is(KafkaBridgeCluster.INIT_VOLUME_MOUNT));

        List<Container> initContainers = podSpec.getInitContainers();
        assertThat(initContainers, is(notNullValue()));
        assertThat(initContainers.size() > 0, is(true));

        final Optional<Container> first = initContainers.stream().filter(container -> container.getName().equals(KafkaBridgeCluster.INIT_NAME)).findFirst();
        assertThat(first.isPresent(), is(true));
        final Container container = first.get();
        assertThat(container.getImage(), is(expectedInitImage));
        assertThat(container.getEnv().size(), greaterThanOrEqualTo(3));
        final List<EnvVar> initEnv = container.getEnv();
        assertThat(initEnv.stream().filter(var -> AbstractModel.ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY.equals(var.getName())).findFirst().orElseThrow().getValue(), is(
                "topology-key"));
        assertThat(initEnv.stream().filter(var -> ENV_VAR_KAFKA_INIT_INIT_FOLDER_KEY.equals(var.getName())).findFirst().orElseThrow().getValue(), is(KafkaBridgeCluster.INIT_VOLUME_MOUNT));
        assertThat(initEnv.stream().filter(var -> AbstractModel.ENV_VAR_KAFKA_INIT_NODE_NAME.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getFieldRef().getFieldPath(), is("spec.nodeName"));

        assertThat(getVolumeMount(container, KafkaBridgeCluster.INIT_VOLUME_NAME).getMountPath(), is(KafkaBridgeCluster.INIT_VOLUME_MOUNT));
        
        return deployment;
    }

    @ParallelTest
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";
        String topologyKey = "topology-key";

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editOrNewMetadata()
                .withNamespace(testNamespace)
                .endMetadata()
                .editOrNewSpec()
                .withNewRack(topologyKey)
                .endSpec()
                .build();

        KafkaBridgeCluster bridgeCluster = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = bridgeCluster.generateClusterRoleBinding();

        assertThat(crb.getMetadata().getName(), is(KafkaBridgeResources.initContainerClusterRoleBindingName(cluster, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(bridgeCluster.componentName));
    }

    @ParallelTest
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editOrNewMetadata()
                .withNamespace(testNamespace)
                .endMetadata()
                .build();

        KafkaBridgeCluster cluster = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        ClusterRoleBinding crb = cluster.generateClusterRoleBinding();

        assertThat(crb, is(nullValue()));
    }

    @ParallelTest
    public void testKafkaBridgeContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_METRICS_ENABLED;
        String testEnvOneValue = "false";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS;
        String testEnvTwoValue = "PEM certs";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaBridgeContainer = new ContainerTemplate();
        kafkaBridgeContainer.setEnv(testEnvs);

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName("tls-trusted-certificate")
                        .withCertificate("pem-content")
                    .endTrustedCertificate()
                .endTls()
                .withNewTemplate()
                    .withBridgeContainer(kafkaBridgeContainer)
                .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER).getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithAccessToken() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().isEmpty(), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithAccessTokenLocation() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withAccessTokenLocation("/var/run/secrets/kubernetes.io/serviceaccount/token")
                                .build())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\"", ClientConfig.OAUTH_ACCESS_TOKEN_LOCATION, "/var/run/secrets/kubernetes.io/serviceaccount/token")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithRefreshToken() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewRefreshToken()
                                    .withSecretName("my-token-secret")
                                    .withKey("my-token-key")
                                .endRefreshToken()
                                .build())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithClientSecret() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_SCOPE, "all", ClientConfig.OAUTH_AUDIENCE, "kafka")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithClientSecretAndSaslExtensions() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server",
                        ClientConfig.OAUTH_SCOPE, "all", ClientConfig.OAUTH_AUDIENCE, "kafka", ClientConfig.OAUTH_SASL_EXTENSION_PREFIX + "key1", "value1", ClientConfig.OAUTH_SASL_EXTENSION_PREFIX + "key2", "value2")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithClientAssertion() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_ASSERTION.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_ASSERTION.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_SCOPE, "all", ClientConfig.OAUTH_AUDIENCE, "kafka")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithUsernameAndPassword() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_PASSWORD_GRANT_PASSWORD.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-password-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_PASSWORD_GRANT_PASSWORD.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("user1.password"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"",
                        ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_PASSWORD_GRANT_USERNAME, "user1", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                    .editSpec()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .build())
                    .endSpec()
                    .build();

            KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

            KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        });
    }


    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithTls() {
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

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaBridgeCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "first-certificate"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaBridgeCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "second-certificate"));

        // Volumes
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

        // Environment variable
        assertThat(cont.getEnv().stream().filter(e -> "KAFKA_BRIDGE_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthUsingOpaqueTokensAndTimeouts() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withConnectTimeoutSeconds(15)
                                .withReadTimeoutSeconds(15)
                                .withHttpRetries(2)
                                .withHttpRetryPauseMs(500)
                                .withNewClientSecret()
                                .withSecretName("my-secret-secret")
                                .withKey("my-secret-key")
                                .endClientSecret()
                                .withAccessTokenIsJwt(false)
                                .withMaxTokenExpirySeconds(600)
                                .withEnableMetrics(true)
                                .withIncludeAcceptHeader(false)
                                .build())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server",
                        ClientConfig.OAUTH_ACCESS_TOKEN_IS_JWT, "false", ClientConfig.OAUTH_MAX_TOKEN_EXPIRY_SECONDS, "600",
                        ClientConfig.OAUTH_CONNECT_TIMEOUT_SECONDS, "15", ClientConfig.OAUTH_READ_TIMEOUT_SECONDS, "15", ClientConfig.OAUTH_HTTP_RETRIES, "2", ClientConfig.OAUTH_HTTP_RETRY_PAUSE_MILLIS, "500", ClientConfig.OAUTH_ENABLE_METRICS, "true", ClientConfig.OAUTH_INCLUDE_ACCEPT_HEADER, "false")));
    }

    @ParallelTest
    public void testDifferentHttpPort()   {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewHttp(1874)
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        // Check ports in container
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getLivenessProbe().getHttpGet().getPort(), is(new IntOrString(KafkaBridgeCluster.REST_API_PORT_NAME)));
        assertThat(cont.getReadinessProbe().getHttpGet().getPort(), is(new IntOrString(KafkaBridgeCluster.REST_API_PORT_NAME)));
        assertThat(cont.getPorts().get(0).getContainerPort(), is(1874));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName(), is(KafkaBridgeCluster.REST_API_PORT_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol(), is("TCP"));

        // Check ports on Service
        Service svc = kb.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedServiceLabels(kb.getComponentName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(1874));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaBridgeCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getMetadata().getAnnotations(), is(kbc.getDiscoveryAnnotation(1874)));
        TestUtils.checkOwnerReference(svc, resource);
    }

    @ParallelTest
    public void testProbeConfiguration()   {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(20)
                        .withPeriodSeconds(21)
                        .withTimeoutSeconds(22)
                    .endLivenessProbe()
                    .withNewReadinessProbe()
                        .withInitialDelaySeconds(30)
                        .withPeriodSeconds(31)
                        .withTimeoutSeconds(32)
                    .endReadinessProbe()
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment dep = kb.generateDeployment(new HashMap<>(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getLivenessProbe().getInitialDelaySeconds(), is(20));
        assertThat(cont.getLivenessProbe().getPeriodSeconds(), is(21));
        assertThat(cont.getLivenessProbe().getTimeoutSeconds(), is(22));
        assertThat(cont.getReadinessProbe().getInitialDelaySeconds(), is(30));
        assertThat(cont.getReadinessProbe().getPeriodSeconds(), is(31));
        assertThat(cont.getReadinessProbe().getTimeoutSeconds(), is(32));
    }

    @ParallelTest
    public void testOpenTelemetryTracingConfiguration() {
        KafkaBridgeBuilder builder = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withTracing(new OpenTelemetryTracing())
                .endSpec();
        KafkaBridge resource = builder.build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment deployment = kb.generateDeployment(new HashMap<>(), true, null, null);

        ConfigMap configMap = kb.generateBridgeConfigMap(metricsAndLogging);
        assertThat(configMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME), containsString("bridge.tracing=" + OpenTelemetryTracing.TYPE_OPENTELEMETRY));
    }

    @ParallelTest
    public void testCorsConfiguration() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .editHttp()
                        .withNewCors()
                            .withAllowedOrigins("https://strimzi.io", "https://cncf.io")
                            .withAllowedMethods("GET", "POST", "PUT", "DELETE", "PATCH")
                        .endCors()
                    .endHttp()
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment deployment = kb.generateDeployment(new HashMap<>(), true, null, null);

        ConfigMap configMap = kb.generateBridgeConfigMap(metricsAndLogging);
        assertThat(configMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME), containsString("http.cors.enabled=true"));
        assertThat(configMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME), containsString("http.cors.allowedOrigins=https://strimzi.io,https://cncf.io"));
        assertThat(configMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME), containsString("http.cors.allowedMethods=GET,POST,PUT,DELETE,PATCH"));
    }

    @ParallelTest
    public void testJvmOptions() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withJvmOptions(
                            new JvmOptionsBuilder()
                                    .withXms("128m")
                                    .withXmx("256m")
                                    .withXx(Collections.singletonMap("InitiatingHeapOccupancyPercent", "36"))
                                    .withJavaSystemProperties(
                                            new SystemPropertyBuilder().withName("myProperty1").withValue("myValue1").build(),
                                            new SystemPropertyBuilder().withName("myProperty2").withValue("myValue2").build()
                                    )
                            .build())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);

        EnvVar systemProps = kb.getEnvVars().stream().filter(envVar -> AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES.equals(envVar.getName())).findFirst().orElse(null);
        assertThat(systemProps, is(notNullValue()));
        assertThat(systemProps.getValue(), containsString("-DmyProperty1=myValue1"));
        assertThat(systemProps.getValue(), containsString("-DmyProperty2=myValue2"));

        EnvVar javaOpts = kb.getEnvVars().stream().filter(envVar -> AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS.equals(envVar.getName())).findFirst().orElse(null);
        assertThat(javaOpts, is(notNullValue()));
        assertThat(javaOpts.getValue(), containsString("-Xms128m"));
        assertThat(javaOpts.getValue(), containsString("-Xmx256m"));
        assertThat(javaOpts.getValue(), containsString("-XX:InitiatingHeapOccupancyPercent=36"));
    }

    @ParallelTest
    public void testConsumerProducerOptions() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withConsumer(new KafkaBridgeConsumerSpecBuilder().withTimeoutSeconds(60).build())
                    .withProducer(new KafkaBridgeProducerSpecBuilder().build())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, SHARED_ENV_PROVIDER);
        Deployment deployment = kb.generateDeployment(new HashMap<>(), true, null, null);

        ConfigMap configMap = kb.generateBridgeConfigMap(metricsAndLogging);
        assertThat(configMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME), containsString("http.timeoutSeconds=60"));
        assertThat(configMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME), containsString("http.consumer.enabled=true"));
        assertThat(configMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME), containsString("http.producer.enabled=true"));
    }

    @ParallelTest
    public void testConfigurationConfigMap() {
        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, SHARED_ENV_PROVIDER);
        Deployment deployment = kb.generateDeployment(new HashMap<>(), true, null, null);
        ConfigMap configMap = kb.generateBridgeConfigMap(metricsAndLogging);

        assertThat(configMap, is(notNullValue()));
        assertThat(configMap.getData().get(LoggingModel.LOG4J2_CONFIG_MAP_KEY), is(notNullValue()));
        assertThat(configMap.getData().get(JmxPrometheusExporterModel.CONFIG_MAP_KEY), is(nullValue()));
        assertThat(configMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME), is(notNullValue()));
    }
}
