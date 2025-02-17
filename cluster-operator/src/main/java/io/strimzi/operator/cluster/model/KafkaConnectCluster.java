/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSource;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.ProbeBuilder;
import io.strimzi.api.kafka.model.common.Rack;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.InternalServiceTemplate;
import io.strimzi.api.kafka.model.common.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.tracing.JaegerTracing;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.api.kafka.model.connect.ExternalConfiguration;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvVarSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectTemplate;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.jmx.JmxModel;
import io.strimzi.operator.cluster.model.jmx.SupportsJmx;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.LoggingUtils;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.common.template.DeploymentStrategy.ROLLING_UPDATE;

/**
 * Kafka Connect model class
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class KafkaConnectCluster extends AbstractModel implements SupportsMetrics, SupportsLogging, SupportsJmx {
    /**
     * Port of the Kafka Connect REST API
     */
    public static final int REST_API_PORT = 8083;

    protected static final String COMPONENT_TYPE = "kafka-connect";
    protected static final String REST_API_PORT_NAME = "rest-api";
    protected static final String TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/connect-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT = "/opt/kafka/connect-password/";
    protected static final String EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH = "/opt/kafka/external-configuration/";
    protected static final String EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX = "ext-conf-";
    protected static final String OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/oauth-certs/";
    protected static final String LOG_AND_METRICS_CONFIG_VOLUME_NAME = "kafka-metrics-and-logging";
    protected static final String LOG_AND_METRICS_CONFIG_VOLUME_MOUNT = "/opt/kafka/custom-config/";

    // Configuration defaults
    private static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder().withTimeoutSeconds(5).withInitialDelaySeconds(60).build();

    // Kafka Connect configuration keys (EnvVariables)
    protected static final String ENV_VAR_PREFIX = "KAFKA_CONNECT_";
    protected static final String ENV_VAR_KAFKA_CONNECT_CONFIGURATION = "KAFKA_CONNECT_CONFIGURATION";
    protected static final String ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED = "KAFKA_CONNECT_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS = "KAFKA_CONNECT_BOOTSTRAP_SERVERS";
    protected static final String ENV_VAR_KAFKA_CONNECT_TLS = "KAFKA_CONNECT_TLS";
    protected static final String ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS = "KAFKA_CONNECT_TRUSTED_CERTS";
    protected static final String ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT = "KAFKA_CONNECT_TLS_AUTH_CERT";
    protected static final String ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY = "KAFKA_CONNECT_TLS_AUTH_KEY";
    protected static final String ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE = "KAFKA_CONNECT_SASL_PASSWORD_FILE";
    protected static final String ENV_VAR_KAFKA_CONNECT_SASL_USERNAME = "KAFKA_CONNECT_SASL_USERNAME";
    protected static final String ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM = "KAFKA_CONNECT_SASL_MECHANISM";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG = "KAFKA_CONNECT_OAUTH_CONFIG";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET = "KAFKA_CONNECT_OAUTH_CLIENT_SECRET";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN = "KAFKA_CONNECT_OAUTH_ACCESS_TOKEN";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN = "KAFKA_CONNECT_OAUTH_REFRESH_TOKEN";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD = "KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_ASSERTION = "KAFKA_CONNECT_OAUTH_CLIENT_ASSERTION";
    protected static final String ENV_VAR_CUSTOM_OAUTH_CALLBACK_CLASS = "CUSTOM_OAUTH_CALLBACK_CLASS";
    protected static final String ENV_VAR_STRIMZI_TRACING = "STRIMZI_TRACING";

    protected static final String CO_ENV_VAR_CUSTOM_CONNECT_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_CONNECT_LABELS";

    protected int replicas;
    private Rack rack;
    private String initImage;
    protected String serviceName;
    protected String loggingAndMetricsConfigMapName;

    protected String bootstrapServers;
    @SuppressWarnings("deprecation") // External Configuration environment variables are deprecated
    protected List<ExternalConfigurationEnv> externalEnvs = Collections.emptyList();

    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    protected List<ExternalConfigurationVolumeSource> externalVolumes = Collections.emptyList();
    protected Tracing tracing;
    protected JmxModel jmx;
    protected MetricsModel metrics;
    protected LoggingModel logging;
    protected AbstractConfiguration configuration;

    private ClientTls tls;
    private KafkaClientAuthentication authentication;

    // Templates
    protected PodDisruptionBudgetTemplate templatePodDisruptionBudget;
    protected ResourceTemplate templateInitClusterRoleBinding;
    protected DeploymentTemplate templateDeployment;
    protected ResourceTemplate templatePodSet;
    protected PodTemplate templatePod;
    protected InternalServiceTemplate templateService;
    protected InternalServiceTemplate templateHeadlessService;
    protected ContainerTemplate templateInitContainer;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_CONNECT_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected KafkaConnectCluster(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        this(reconciliation, resource, KafkaConnectResources.componentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param name              Name of the Strimzi component usually consisting from the cluster name and component type
     * @param componentType configurable allow other classes to extend this class
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected KafkaConnectCluster(Reconciliation reconciliation, HasMetadata resource, String name, String componentType, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, name, componentType, sharedEnvironmentProvider);

        this.serviceName = KafkaConnectResources.serviceName(cluster);
        this.loggingAndMetricsConfigMapName = KafkaConnectResources.metricsAndLogConfigMapName(cluster);
    }

    /**
     * Creates the Kafka Connect model instance from the Kafka Connect CRD
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaConnect      Kafka connect custom resource
     * @param versions          Supported Kafka versions
     * @param sharedEnvironmentProvider Shared environment provider
     *
     * @return  Instance of the Kafka Connect model class
     */
    public static KafkaConnectCluster fromCrd(Reconciliation reconciliation,
                                              KafkaConnect kafkaConnect,
                                              KafkaVersion.Lookup versions,
                                              SharedEnvironmentProvider sharedEnvironmentProvider) {
        return fromSpec(reconciliation, kafkaConnect.getSpec(), versions, new KafkaConnectCluster(reconciliation, kafkaConnect, sharedEnvironmentProvider));
    }

    /**
     * Abstracts the calling of setters on a (subclass of) KafkaConnectCluster
     * from the instantiation of the (subclass of) KafkaConnectCluster,
     * thus permitting reuse of the setter-calling code for subclasses.
     *
     * @param reconciliation    Reconciliation marker
     * @param spec              Spec section of the Kafka Connect resource
     * @param versions          Supported Kafka versions
     * @param result            Kafka Connect resource which will be returned as the result
     *
     * @param <C>   Type of the Kafka Connect cluster
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity", "deprecation"})
    protected static <C extends KafkaConnectCluster> C fromSpec(Reconciliation reconciliation,
                                                                KafkaConnectSpec spec,
                                                                KafkaVersion.Lookup versions,
                                                                C result) {
        result.replicas = spec.getReplicas();
        result.tracing = spec.getTracing();

        // Might already contain configuration from Mirror Maker 2 which extends Connect
        // We have to check it and either use the Mirror Maker 2 configs or get the Connect configs
        AbstractConfiguration config = result.configuration;
        if (config == null) {
            config = new KafkaConnectConfiguration(reconciliation, spec.getConfig().entrySet());
            result.configuration = config;
        }
        if (result.tracing != null)   {
            if (JaegerTracing.TYPE_JAEGER.equals(result.tracing.getType())) {
                LOGGER.warnCr(reconciliation, "Tracing type \"{}\" is not supported anymore and will be ignored", JaegerTracing.TYPE_JAEGER);
            } else if (OpenTelemetryTracing.TYPE_OPENTELEMETRY.equals(result.tracing.getType())) {
                config.setConfigOption("consumer.interceptor.classes", OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME);
                config.setConfigOption("producer.interceptor.classes", OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME);
            }
        }

        if (result.getImage() == null) {
            result.image = versions.kafkaConnectVersion(spec.getImage(), spec.getVersion());
        }

        result.resources = spec.getResources();
        result.gcLoggingEnabled = spec.getJvmOptions() == null ? JvmOptions.DEFAULT_GC_LOGGING_ENABLED : spec.getJvmOptions().isGcLoggingEnabled();

        result.jvmOptions = spec.getJvmOptions();
        result.metrics = new MetricsModel(spec);
        result.logging = new LoggingModel(spec, result.getClass().getSimpleName(), false, true);
        result.jmx = new JmxModel(
                reconciliation.namespace(),
                KafkaConnectResources.jmxSecretName(result.cluster),
                result.labels,
                result.ownerReference,
                spec
        );
        result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(spec, DEFAULT_HEALTHCHECK_OPTIONS);
        result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(spec, DEFAULT_HEALTHCHECK_OPTIONS);

        result.setRack(spec.getRack());

        String initImage = spec.getClientRackInitImage();
        if (initImage == null) {
            initImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_INIT_IMAGE, "quay.io/strimzi/operator:latest");
        }
        result.setInitImage(initImage);

        result.setBootstrapServers(spec.getBootstrapServers());

        result.setTls(spec.getTls());
        String warnMsg = AuthenticationUtils.validateClientAuthentication(spec.getAuthentication(), spec.getTls() != null);
        if (!warnMsg.isEmpty()) {
            LOGGER.warnCr(reconciliation, warnMsg);
        }
        result.setAuthentication(spec.getAuthentication());

        if (spec.getTemplate() != null) {
            KafkaConnectTemplate template = spec.getTemplate();

            result.templatePodDisruptionBudget = template.getPodDisruptionBudget();
            result.templateInitClusterRoleBinding = template.getClusterRoleBinding();
            result.templateDeployment = template.getDeployment();
            result.templatePodSet = template.getPodSet();
            result.templatePod = template.getPod();
            result.templateService = template.getApiService();
            result.templateHeadlessService = template.getHeadlessService();
            result.templateServiceAccount = template.getServiceAccount();
            result.templateContainer = template.getConnectContainer();
            result.templateInitContainer = template.getInitContainer();
        }

        if (spec.getExternalConfiguration() != null)    {
            ExternalConfiguration externalConfiguration = spec.getExternalConfiguration();

            if (externalConfiguration.getEnv() != null && !externalConfiguration.getEnv().isEmpty())    {
                result.externalEnvs = externalConfiguration.getEnv();
            }

            if (externalConfiguration.getVolumes() != null && !externalConfiguration.getVolumes().isEmpty())    {
                result.externalVolumes = externalConfiguration.getVolumes();
            }
        }

        return result;
    }

    /**
     * @return The Kubernetes service name.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * @return  Generates the Kafka Connect service
     */
    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(1);
        ports.add(ServiceUtils.createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP"));

        ports.addAll(jmx.servicePorts());

        return ServiceUtils.createClusterIpService(
                serviceName,
                namespace,
                labels,
                ownerReference,
                templateService,
                ports
        );
    }

    /**
     * Generates a headless Service according to configured defaults
     *
     * @return The generated Service
     */
    public Service generateHeadlessService() {
        List<ServicePort> ports = List.of(ServiceUtils.createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP"));

        return ServiceUtils.createHeadlessService(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateHeadlessService,
                ports
        );
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(2);
        portList.add(ContainerUtils.createContainerPort(REST_API_PORT_NAME, REST_API_PORT));
        if (metrics.isEnabled()) {
            portList.add(ContainerUtils.createContainerPort(MetricsModel.METRICS_PORT_NAME, MetricsModel.METRICS_PORT));
        }

        portList.addAll(jmx.containerPorts());

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(2);
        volumeList.add(VolumeUtils.createTempDirVolume(templatePod));
        volumeList.add(VolumeUtils.createConfigMapVolume(LOG_AND_METRICS_CONFIG_VOLUME_NAME, loggingAndMetricsConfigMapName));

        if (rack != null) {
            volumeList.add(VolumeUtils.createEmptyDirVolume(INIT_VOLUME_NAME, "1Mi", "Memory"));
        }

        if (tls != null) {
            CertUtils.createTrustedCertificatesVolumes(volumeList, tls.getTrustedCertificates(), isOpenShift);
        }
        AuthenticationUtils.configureClientAuthenticationVolumes(authentication, volumeList, "oauth-certs", isOpenShift);
        volumeList.addAll(getExternalConfigurationVolumes(isOpenShift));
        
        TemplateUtils.addAdditionalVolumes(templatePod, volumeList);

        return volumeList;
    }

    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    private List<Volume> getExternalConfigurationVolumes(boolean isOpenShift)  {
        int mode = 0444;
        if (isOpenShift) {
            mode = 0440;
        }

        List<Volume> volumeList = new ArrayList<>(0);

        for (ExternalConfigurationVolumeSource volume : externalVolumes)    {
            String name = volume.getName();

            if (name != null) {
                if (volume.getConfigMap() != null && volume.getSecret() != null) {
                    LOGGER.warnCr(reconciliation, "Volume {} with external Kafka Connect configuration has to contain exactly one volume source reference to either ConfigMap or Secret", name);
                } else  {
                    if (volume.getConfigMap() != null) {
                        ConfigMapVolumeSource source = volume.getConfigMap();
                        source.setDefaultMode(mode);

                        Volume newVol = new VolumeBuilder()
                                .withName(VolumeUtils.getValidVolumeName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name))
                                .withConfigMap(source)
                                .build();

                        volumeList.add(newVol);
                    } else if (volume.getSecret() != null)    {
                        SecretVolumeSource source = volume.getSecret();
                        source.setDefaultMode(mode);

                        Volume newVol = new VolumeBuilder()
                                .withName(VolumeUtils.getValidVolumeName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name))
                                .withSecret(source)
                                .build();

                        volumeList.add(newVol);
                    }
                }
            }
        }

        return volumeList;
    }

    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(2);
        volumeMountList.add(VolumeUtils.createTempDirVolumeMount());
        volumeMountList.add(VolumeUtils.createVolumeMount(LOG_AND_METRICS_CONFIG_VOLUME_NAME, LOG_AND_METRICS_CONFIG_VOLUME_MOUNT));

        if (rack != null) {
            volumeMountList.add(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT));
        }

        if (tls != null) {
            CertUtils.createTrustedCertificatesVolumeMounts(volumeMountList, tls.getTrustedCertificates(), TLS_CERTS_BASE_VOLUME_MOUNT);
        }
        AuthenticationUtils.configureClientAuthenticationVolumeMounts(authentication, volumeMountList, TLS_CERTS_BASE_VOLUME_MOUNT, PASSWORD_VOLUME_MOUNT, OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, "oauth-certs");
        volumeMountList.addAll(getExternalConfigurationVolumeMounts());

        TemplateUtils.addAdditionalVolumeMounts(volumeMountList, templateContainer);

        return volumeMountList;
    }
    
    private List<VolumeMount> getInitContainerVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT));
        TemplateUtils.addAdditionalVolumeMounts(volumeMountList, templateInitContainer);
        return volumeMountList;
    }

    @SuppressWarnings("deprecation") // External Configuration volumes are deprecated
    private List<VolumeMount> getExternalConfigurationVolumeMounts()    {
        List<VolumeMount> volumeMountList = new ArrayList<>(0);

        for (ExternalConfigurationVolumeSource volume : externalVolumes)    {
            String name = volume.getName();

            if (name != null)   {
                if (volume.getConfigMap() != null && volume.getSecret() != null) {
                    LOGGER.warnCr(reconciliation, "Volume {} with external Kafka Connect configuration has to contain exactly one volume source reference to either ConfigMap or Secret", name);
                } else  if (volume.getConfigMap() != null || volume.getSecret() != null) {
                    VolumeMount volumeMount = new VolumeMountBuilder()
                            .withName(VolumeUtils.getValidVolumeName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name))
                            .withMountPath(EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + name)
                            .build();

                    volumeMountList.add(volumeMount);
                }
            }
        }

        return volumeMountList;
    }

    /**
     * Returns a combined affinity: Adding the affinity needed for the "kafka-rack" to the user-provided affinity.
     */
    protected Affinity getMergedAffinity() {
        Affinity userAffinity = templatePod != null && templatePod.getAffinity() != null ? templatePod.getAffinity() : new Affinity();
        AffinityBuilder builder = new AffinityBuilder(userAffinity);
        if (rack != null) {
            builder = ModelUtils.populateAffinityBuilderWithRackLabelSelector(builder, userAffinity, rack.getTopologyKey());
        }
        return builder.build();
    }

    /**
     * Generates the StrimziPodSet for the Kafka cluster.
     * enabled.
     *
     * @param replicas                  Number of replicas the StrimziPodSet should have. During scale-ups or scale-downs, node
     *                                  sets with different numbers of pods are generated.
     * @param podSetAnnotations         Map with StrimziPodSet annotations
     * @param podAnnotations            Map with Pod annotations
     * @param isOpenShift               Flags whether we are on OpenShift or not
     * @param imagePullPolicy           Image pull policy which will be used by the pods
     * @param imagePullSecrets          List of image pull secrets
     * @param customContainerImage      Custom container image produced by Kafka Connect Build. If null, the default
     *                                  image will be used.
     *
     * @return                          Generated StrimziPodSet with Kafka Connect pods
     */
    public StrimziPodSet generatePodSet(int replicas,
                                        Map<String, String> podSetAnnotations,
                                        Map<String, String> podAnnotations,
                                        boolean isOpenShift,
                                        ImagePullPolicy imagePullPolicy,
                                        List<LocalObjectReference> imagePullSecrets,
                                        String customContainerImage) {
        return WorkloadUtils.createPodSet(
                componentName,
                namespace,
                labels,
                ownerReference,
                templatePodSet,
                replicas,
                podSetAnnotations,
                labels.strimziSelectorLabels().withStrimziPodSetController(componentName),
                podId -> WorkloadUtils.createStatefulPod(
                        reconciliation,
                        componentName + "-" + podId,
                        namespace,
                        labels,
                        componentName,
                        componentName,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        podAnnotations,
                        componentName,
                        getMergedAffinity(),
                        ContainerUtils.listOrNull(createInitContainer(imagePullPolicy)),
                        List.of(createContainer(imagePullPolicy, customContainerImage)),
                        getVolumes(isOpenShift),
                        imagePullSecrets,
                        securityProvider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(templatePod))
                )
        );
    }

    /* test */ Container createInitContainer(ImagePullPolicy imagePullPolicy) {
        if (rack != null) {
            return ContainerUtils.createContainer(
                    INIT_NAME,
                    initImage,
                    List.of("/opt/strimzi/bin/kafka_init_run.sh"),
                    securityProvider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateInitContainer)),
                    resources,
                    getInitContainerEnvVars(),
                    null,
                    getInitContainerVolumeMounts(),
                    null,
                    null,
                    imagePullPolicy
            );
        } else {
            return null;
        }
    }

    private Container createContainer(ImagePullPolicy imagePullPolicy, String customContainerImage) {
        return ContainerUtils.createContainer(
                componentName,
                customContainerImage != null ? customContainerImage : image,
                List.of(getCommand()),
                securityProvider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                getContainerPortList(),
                getVolumeMounts(),
                ProbeUtils.httpProbe(livenessProbeOptions, "/", REST_API_PORT_NAME),
                ProbeUtils.httpProbe(readinessProbeOptions, "/", REST_API_PORT_NAME),
                imagePullPolicy
        );
    }

    protected List<EnvVar> getInitContainerEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVarFromFieldRef(ENV_VAR_KAFKA_INIT_NODE_NAME, "spec.nodeName"));

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY, rack.getTopologyKey()));

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateInitContainer);

        return varList;
    }

    /**
     * The command for running Connect has to be passed through a method so that we can handle different run commands
     * for Connect and Mirror Maker 2 (which inherits from this class) without duplicating the whole container creation.
     * This method is overridden in KafkaMirrorMaker2Model.
     *
     * @return  Command for starting Kafka Connect container
     */
    protected String getCommand() {
        return "/opt/kafka/kafka_connect_run.sh";
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_CONNECT_CONFIGURATION, configuration.getConfiguration()));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED, String.valueOf(metrics.isEnabled())));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS, bootstrapServers));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));

        JvmOptionUtils.heapOptions(varList, 75, 0L, jvmOptions, resources);
        JvmOptionUtils.jvmPerformanceOptions(varList, jvmOptions);
        JvmOptionUtils.jvmSystemProperties(varList, jvmOptions);

        if (tls != null) {
            populateTLSEnvVars(varList);
        }

        AuthenticationUtils.configureClientAuthenticationEnvVars(authentication, varList, name -> ENV_VAR_PREFIX + name);

        if (tracing != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_TRACING, tracing.getType()));
        }

        varList.addAll(jmx.envVars());

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        varList.addAll(getExternalConfigurationEnvVars());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    private void populateTLSEnvVars(final List<EnvVar> varList) {
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_CONNECT_TLS, "true"));

        if (tls.getTrustedCertificates() != null && !tls.getTrustedCertificates().isEmpty()) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS, CertUtils.trustedCertsEnvVar(tls.getTrustedCertificates())));
        }
    }

    @SuppressWarnings("deprecation") // External Configuration environment variables are deprecated
    private List<EnvVar> getExternalConfigurationEnvVars()   {
        List<EnvVar> varList = new ArrayList<>();

        for (ExternalConfigurationEnv var : externalEnvs)    {
            String name = var.getName();

            if (name != null && !name.startsWith("KAFKA_") && !name.startsWith("STRIMZI_")) {
                ExternalConfigurationEnvVarSource valueFrom = var.getValueFrom();

                if (valueFrom != null)  {
                    if (valueFrom.getConfigMapKeyRef() != null && valueFrom.getSecretKeyRef() != null) {
                        LOGGER.warnCr(reconciliation, "Environment variable {} with external Kafka Connect configuration has to contain exactly one reference to either ConfigMap or Secret", name);
                    } else {
                        if (valueFrom.getConfigMapKeyRef() != null) {
                            EnvVarSource envVarSource = new EnvVarSourceBuilder()
                                    .withConfigMapKeyRef(var.getValueFrom().getConfigMapKeyRef())
                                    .build();

                            varList.add(new EnvVarBuilder().withName(name).withValueFrom(envVarSource).build());
                        } else if (valueFrom.getSecretKeyRef() != null)    {
                            EnvVarSource envVarSource = new EnvVarSourceBuilder()
                                    .withSecretKeyRef(var.getValueFrom().getSecretKeyRef())
                                    .build();

                            varList.add(new EnvVarBuilder().withName(name).withValueFrom(envVarSource).build());
                        }
                    }
                }
            } else {
                LOGGER.warnCr(reconciliation, "Name of an environment variable with external Kafka Connect configuration cannot start with `KAFKA_` or `STRIMZI`.");
            }
        }

        return varList;
    }

    protected void setRack(Rack rack) {
        this.rack = rack;
    }

    protected void setInitImage(String initImage) {
        this.initImage = initImage;
    }

    /**
     * Set the bootstrap servers to connect to
     *
     * @param bootstrapServers bootstrap servers comma separated list
     */
    protected void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Set the tls configuration with the certificate to trust
     *
     * @param tls trusted certificates list
     */
    protected void setTls(ClientTls tls) {
        this.tls = tls;
    }

    /**
     * Sets the configured authentication
     *
     * @param authentication Authentication configuration
     */
    protected void setAuthentication(KafkaClientAuthentication authentication) {
        this.authentication = authentication;
    }

    /**
     * Generates the PodDisruptionBudget
     *
     * @return The pod disruption budget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return PodDisruptionBudgetUtils.createCustomControllerPodDisruptionBudget(componentName, namespace, labels, ownerReference, templatePodDisruptionBudget, replicas);
    }

    /**
     * @return  Name of the ClusterRoleBinding for the Connect init container
     */
    public String getInitContainerClusterRoleBindingName() {
        return KafkaConnectResources.initContainerClusterRoleBindingName(cluster, namespace);
    }

    /**
     * Generates the NetworkPolicies relevant for Kafka Connect nodes
     *
     * @param connectorOperatorEnabled Whether the ConnectorOperator is enabled or not
     * @param operatorNamespace                             Namespace where the Strimzi Cluster Operator runs. Null if not configured.
     * @param operatorNamespaceLabels                       Labels of the namespace where the Strimzi Cluster Operator runs. Null if not configured.
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy(boolean connectorOperatorEnabled,
                                               String operatorNamespace, Labels operatorNamespaceLabels) {
        if (connectorOperatorEnabled) {
            NetworkPolicyPeer clusterOperatorPeer = NetworkPolicyUtils.createPeer(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"), NetworkPolicyUtils.clusterOperatorNamespaceSelector(namespace, operatorNamespace, operatorNamespaceLabels));
            NetworkPolicyPeer connectPeer = NetworkPolicyUtils.createPeer(labels.strimziSelectorLabels().toMap());

            // List of network policy rules for all ports
            List<NetworkPolicyIngressRule> rules = new ArrayList<>();

            // Give CO and Connect itself access to the REST API
            rules.add(NetworkPolicyUtils.createIngressRule(REST_API_PORT, List.of(connectPeer, clusterOperatorPeer)));

            // The Metrics port (if enabled) is opened to all by default
            if (metrics.isEnabled()) {
                rules.add(NetworkPolicyUtils.createIngressRule(MetricsModel.METRICS_PORT, List.of()));
            }

            // The JMX port (if enabled) is opened to all by default
            rules.addAll(jmx.networkPolicyIngresRules());

            // Build the final network policy with all rules covering all the ports
            return NetworkPolicyUtils.createNetworkPolicy(
                    componentName,
                    namespace,
                    labels,
                    ownerReference,
                    rules
            );
        } else {
            return null;
        }
    }

    /**
     * Returns the Tracing object with tracing configuration or null if tracing was not enabled.
     *
     * @return  Tracing object with tracing configuration
     */
    public Tracing getTracing() {
        return tracing;
    }

    /**
     * Creates the ClusterRoleBinding which is used to bind the Kafka Connect SA to the ClusterRole
     * which permissions the Kafka init container to access K8S nodes (necessary for rack-awareness).
     *
     * @return The cluster role binding.
     */
    public ClusterRoleBinding generateClusterRoleBinding() {
        if (rack != null) {
            Subject subject = new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName(componentName)
                    .withNamespace(namespace)
                    .build();

            RoleRef roleRef = new RoleRefBuilder()
                    .withName("strimzi-kafka-client")
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .build();

            return RbacUtils
                    .createClusterRoleBinding(getInitContainerClusterRoleBindingName(), roleRef, List.of(subject), labels, templateInitClusterRoleBinding);
        } else {
            return null;
        }
    }

    /**
     * @return  Default logging configuration needed to update loggers in Kafka Connect (and Kafka Mirror Maker 2 which
     *          is based on Kafka Connect)
     */
    public OrderedProperties defaultLogConfig()   {
        return LoggingUtils.defaultLogConfig(reconciliation, this.getClass().getSimpleName());
    }

    /**
     * The default labels Connect pod has to be passed through a method so that we can handle different labels for
     * Connect and Mirror Maker 2 (which inherits from this class) without duplicating the whole pod creation.
     * This method is overridden in KafkaMirrorMaker2Model.
     *
     * @return Default Pod Labels for Kafka Connect
     */
    protected Map<String, String> defaultPodLabels() {
        return DEFAULT_POD_LABELS;
    }

    /**
     * Generates a metrics and logging ConfigMap according to the configuration. If this operand doesn't support logging
     * or metrics, they will nto be set.
     *
     * @param metricsAndLogging     The external CMs with logging and metrics configuration
     *
     * @return The generated ConfigMap
     */
    public ConfigMap generateMetricsAndLogConfigMap(MetricsAndLogging metricsAndLogging) {
        return ConfigMapUtils
                .createConfigMap(
                        loggingAndMetricsConfigMapName,
                        namespace,
                        labels,
                        ownerReference,
                        ConfigMapUtils.generateMetricsAndLogConfigMapData(reconciliation, this, metricsAndLogging)
                );
    }

    /**
     * @return The number of replicas
     */
    public int getReplicas() {
        return replicas;
    }

    /**
     * @return  JMX Model instance for configuring JMX access
     */
    public JmxModel jmx() {
        return jmx;
    }

    /**
     * @return  Metrics Model instance for configuring Prometheus metrics
     */
    public MetricsModel metrics()   {
        return metrics;
    }

    /**
     * @return  Logging Model instance for configuring logging
     */
    public LoggingModel logging()   {
        return logging;
    }

    /**
     * @return  Returns the preferred Deployment Strategy. This is used for the migration form Deployment to
     * StrimziPodSet or the other way around
     */
    public DeploymentStrategy deploymentStrategy()  {
        return TemplateUtils.deploymentStrategy(templateDeployment, ROLLING_UPDATE);
    }
}
