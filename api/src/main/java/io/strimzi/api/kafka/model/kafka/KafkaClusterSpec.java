/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.HasConfigurableLogging;
import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Logging;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.Rack;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.common.jmx.HasJmxOptions;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptions;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPlugin;
import io.strimzi.api.kafka.model.kafka.tieredstorage.TieredStorage;
import io.strimzi.crdgenerator.annotations.AddedIn;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.MinimumItems;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a Kafka "cluster" managed by Strimzi.  
 * As of Strimzi 0.46.x, only KRaft-based Kafka deployments are supported.  
 * ZooKeeper-related configuration options are no longer valid.  
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "version", "metadataVersion", "replicas", "image", "listeners", "config", "storage", "authorization", "rack",
    "brokerRackInitImage", "livenessProbe", "readinessProbe", "jvmOptions", "jmxOptions", "resources", "metricsConfig",
    "logging", "template", "tieredStorage", "quotas"})
@EqualsAndHashCode
@ToString
public class KafkaClusterSpec implements HasConfigurableMetrics, HasConfigurableLogging, HasJmxOptions, HasReadinessProbe, HasLivenessProbe, UnknownPropertyPreserving {
    public static final String FORBIDDEN_PREFIXES = "listeners, advertised., broker., listener., host.name, port, "
            + "inter.broker.listener.name, sasl., ssl., security., password., log.dir, "
            + "zookeeper.connect, zookeeper.set.acl, zookeeper.ssl, zookeeper.clientCnxnSocket, authorizer., super.user, "
            + "cruise.control.metrics.topic, cruise.control.metrics.reporter.bootstrap.servers, "
            + "node.id, process.roles, controller., metadata.log.dir, zookeeper.metadata.migration.enable, " // KRaft options
            + "client.quota.callback.static.kafka.admin., client.quota.callback.static.produce, client.quota.callback.static.fetch, "
            + "client.quota.callback.static.storage.per.volume.limit.min.available., client.quota.callback.static.excluded.principal.name.list, "
            + "kafka.metric.reporters, prometheus.metrics.reporter.";

    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "zookeeper.connection.timeout.ms, sasl.server.max.receive.size, "
            + "ssl.cipher.suites, ssl.protocol, ssl.enabled.protocols, ssl.secure.random.implementation, "
            + "cruise.control.metrics.topic.num.partitions, cruise.control.metrics.topic.replication.factor, cruise.control.metrics.topic.retention.ms, "
            + "cruise.control.metrics.topic.auto.create.retries, cruise.control.metrics.topic.auto.create.timeout.ms, "
            + "cruise.control.metrics.topic.min.insync.replicas, "
            + "controller.quorum.election.backoff.max.ms, controller.quorum.election.timeout.ms, controller.quorum.fetch.timeout.ms"; // KRaft options

    protected Storage storage;
    private String version;
    private String metadataVersion;
    private Map<String, Object> config = new HashMap<>(0);
    private String brokerRackInitImage;
    private Rack rack;
    private Logging logging;
    private Integer replicas;
    private String image;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private JvmOptions jvmOptions;
    private KafkaJmxOptions jmxOptions;
    private MetricsConfig metricsConfig;
    private List<GenericKafkaListener> listeners;
    private KafkaAuthorization authorization;
    private KafkaClusterTemplate template;
    private TieredStorage tieredStorage;
    private QuotasPlugin quotas;
    private Map<String, Object> additionalProperties;

    @Description("The Kafka broker version. Defaults to the latest version. " +
            "Consult the user documentation to understand the process required to upgrade or downgrade the version.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @AddedIn("0.39.0")
    @Description("The KRaft metadata version used by the Kafka cluster. " +
            "This property is ignored when running in ZooKeeper mode. " +
            "If the property is not set, it defaults to the metadata version that corresponds to the `version` property.")
    public String getMetadataVersion() {
        return metadataVersion;
    }

    public void setMetadataVersion(String metadataVersion) {
        this.metadataVersion = metadataVersion;
    }

    @Description("Kafka broker config properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES + " (with the exception of: " + FORBIDDEN_PREFIX_EXCEPTIONS + ").")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("The image of the init container used for initializing the `broker.rack`.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getBrokerRackInitImage() {
        return brokerRackInitImage;
    }

    public void setBrokerRackInitImage(String brokerRackInitImage) {
        this.brokerRackInitImage = brokerRackInitImage;
    }

    @Description("Configuration of the `broker.rack` broker config.")
    @JsonProperty("rack")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Rack getRack() {
        return rack;
    }

    public void setRack(Rack rack) {
        this.rack = rack;
    }

    @Deprecated
    @DeprecatedProperty(description = "Use `KafkaNodePool` resources.")
    @PresentInVersions("v1alpha1-v1beta2")
    @Description("Storage is now configured in the `KafkaNodePool` resources and this option is ignored.")
    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    @Description("Logging configuration for Kafka")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public Logging getLogging() {
        return logging;
    }

    @Override
    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @Deprecated
    @DeprecatedProperty(description = "Use `KafkaNodePool` resources.")
    @PresentInVersions("v1alpha1-v1beta2")
    @Description("Replicas are now configured in `KafkaNodePool` resources and this option is ignored.")
    @Minimum(1)
    public Integer getReplicas() {
        return replicas;
    }

    @Deprecated
    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    @Description("The container image used for Kafka pods. "
        + "If the property is not set, the default Kafka image version is determined based on the `version` configuration. "
        + "The image names are specifically mapped to corresponding versions in the Cluster Operator configuration. "
        + "Changing the Kafka image version does not automatically update the image versions for other components, such as Kafka Exporter. ")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @KubeLink(group = "core", version = "v1", kind = "resourcerequirements")
    @Description("CPU and memory resources to reserve.")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod liveness checking.")
    public Probe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(Probe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Pod readiness checking.")
    public Probe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(Probe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("JVM Options for pods")
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("JMX Options for Kafka brokers")
    @JsonProperty("jmxOptions")
    public KafkaJmxOptions getJmxOptions() {
        return jmxOptions;
    }

    public void setJmxOptions(KafkaJmxOptions jmxOptions) {
        this.jmxOptions = jmxOptions;
    }

    @Description("Metrics configuration.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    @Override
    public void setMetricsConfig(MetricsConfig metricsConfig) {
        this.metricsConfig = metricsConfig;
    }

    @Description("Configures listeners to provide access to Kafka brokers.")
    @MinimumItems(1)
    @JsonProperty(required = true)
    public List<GenericKafkaListener> getListeners() {
        return listeners;
    }

    public void setListeners(List<GenericKafkaListener> listeners) {
        this.listeners = listeners;
    }

    @Description("Authorization configuration for Kafka brokers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaAuthorization getAuthorization() {
        return authorization;
    }

    public void setAuthorization(KafkaAuthorization authorization) {
        this.authorization = authorization;
    }

    @Description("Template for Kafka cluster resources. " +
            "The template allows users to specify how the Kubernetes resources are generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaClusterTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaClusterTemplate template) {
        this.template = template;
    }

    @Description("Configure the tiered storage feature for Kafka brokers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TieredStorage getTieredStorage() {
        return tieredStorage;
    }

    public void setTieredStorage(TieredStorage tieredStorage) {
        this.tieredStorage = tieredStorage;
    }

    @Description("Quotas plugin configuration for Kafka brokers allows setting quotas for disk usage, produce/fetch rates, and more. " +
        "Supported plugin types include `kafka` (default) and `strimzi`. " +
        "If not specified, the default `kafka` quotas plugin is used.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public QuotasPlugin getQuotas() {
        return quotas;
    }

    public void setQuotas(QuotasPlugin quotas) {
        this.quotas = quotas;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
