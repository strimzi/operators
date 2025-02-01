/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.build.DockerOutput;
import io.strimzi.api.kafka.model.connect.build.DockerOutputBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;

public class KafkaConnectTemplates {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectTemplates.class);

    private static final String METRICS_CONNECT_CONFIG_MAP_SUFFIX = "-connect-metrics";
    private static final String CONFIG_MAP_KEY = "metrics-config.yml";

    private KafkaConnectTemplates() {}

    public static KafkaConnectBuilder kafkaConnect(
        final String namespaceName,
        final String kafkaConnectClusterName,
        final String kafkaClusterName,
        final int kafkaConnectReplicas
    ) {
        return defaultKafkaConnect(namespaceName, kafkaConnectClusterName, kafkaClusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnect(
        final String namespaceName,
        final String kafkaClusterName,
        final int kafkaConnectReplicas
    ) {
        return kafkaConnect(namespaceName, kafkaClusterName, kafkaClusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetricsAndFileSinkPlugin(
        final String namespaceName,
        final String kafkaConnectClusterName,
        final String kafkaClusterName,
        final int replicas
    ) {
        return kafkaConnectWithFilePlugin(namespaceName, kafkaConnectClusterName, kafkaClusterName, replicas)
            .editOrNewSpec()
            .withNewJmxPrometheusExporterMetricsConfig()
                .withNewValueFrom()
                    .withNewConfigMapKeyRef(CONFIG_MAP_KEY, getConfigMapName(kafkaConnectClusterName), false)
                .endValueFrom()
            .endJmxPrometheusExporterMetricsConfig()
            .endSpec();
    }

    public static ConfigMap connectMetricsConfigMap(String namespaceName, String kafkaConnectClusterName) {
        return new ConfigMapBuilder(FileUtils.extractConfigMapFromYAMLWithResources(TestConstants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG, "connect-metrics"))
            .editOrNewMetadata()
                .withNamespace(namespaceName)
                .withName(getConfigMapName(kafkaConnectClusterName))
            .endMetadata()
            .build();
    }

    private static String getConfigMapName(String kafkaConnectClusterName) {
        return kafkaConnectClusterName + METRICS_CONNECT_CONFIG_MAP_SUFFIX;
    }

    private static KafkaConnectBuilder defaultKafkaConnect(
        final String namespaceName,
        String kafkaConnectClusterName,
        String kafkaClusterName,
        int kafkaConnectReplicas
    ) {
        return new KafkaConnectBuilder()
            .withNewMetadata()
                .withName(kafkaConnectClusterName)
                .withNamespace(namespaceName)
            .endMetadata()
            .editOrNewSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterName))
                .withReplicas(kafkaConnectReplicas)
                .withNewTls()
                    .withTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterName))
                            .withCertificate("ca.crt")
                            .build()
                    )
                .endTls()
                .addToConfig("group.id", KafkaConnectResources.componentName(kafkaConnectClusterName))
                .addToConfig("offset.storage.topic", KafkaConnectResources.configStorageTopicOffsets(kafkaConnectClusterName))
                .addToConfig("config.storage.topic", KafkaConnectResources.configMapName(kafkaConnectClusterName))
                .addToConfig("status.storage.topic", KafkaConnectResources.configStorageTopicStatus(kafkaConnectClusterName))
                .addToConfig("config.storage.replication.factor", "-1")
                .addToConfig("offset.storage.replication.factor", "-1")
                .addToConfig("status.storage.replication.factor", "-1")
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String namespaceName, String kafkaClusterName, int replicas) {
        return kafkaConnectWithFilePlugin(namespaceName, kafkaClusterName, kafkaClusterName, replicas);
    }

    /**
     * Method for creating the KafkaConnect builder with File plugin - using the KafkaConnect build feature.
     * @param namespaceName namespace, where the KafkaConnect resource will be deployed
     * @param kafkaConnectClusterName Name for the KafkaConnect resource
     * @param kafkaClusterName name of the Kafka cluster
     * @param replicas number of KafkaConnect replicas
     * @return KafkaConnect builder with File plugin
     */
    public static KafkaConnectBuilder kafkaConnectWithFilePlugin(String namespaceName, String kafkaConnectClusterName, String kafkaClusterName, int replicas) {
        return addFileSinkPluginOrImage(namespaceName, kafkaConnect(namespaceName, kafkaConnectClusterName, kafkaClusterName, replicas));
    }

    /**
     * Method for adding Connect Build with file-sink plugin to the Connect spec or set Connect's image in case that
     * the image is set in `CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN` env. variable
     * @param namespaceName namespace for output registry
     * @param kafkaConnectBuilder builder of the Connect resource
     * @return updated Connect resource in builder
     */
    @SuppressFBWarnings("DMI_RANDOM_USED_ONLY_ONCE")
    public static KafkaConnectBuilder addFileSinkPluginOrImage(String namespaceName, KafkaConnectBuilder kafkaConnectBuilder) {
        if (!KubeClusterResource.getInstance().isMicroShift() && Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN.isEmpty()) {
            final Plugin fileSinkPlugin = new PluginBuilder()
                .withName("file-plugin")
                .withArtifacts(
                    new JarArtifactBuilder()
                        .withUrl(Environment.ST_FILE_PLUGIN_URL)
                        .build()
                )
                .build();

            final String imageFullPath = Environment.getImageOutputRegistry(namespaceName, TestConstants.ST_CONNECT_BUILD_IMAGE_NAME, String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));

            return kafkaConnectBuilder
                .editOrNewSpec()
                    .editOrNewBuild()
                        .withPlugins(fileSinkPlugin)
                        .withOutput(dockerOutput(imageFullPath))
                    .endBuild()
                .endSpec();
        } else {
            if (KubeClusterResource.getInstance().isMicroShift()) {
                LOGGER.warn("Using MicroShift cluster - you should have created your own Connect image with file-sink plugin and pass the image into {} env variable", Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN_ENV);
            }

            LOGGER.info("Using {} image from {} env variable", Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN, Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN_ENV);

            return kafkaConnectBuilder
                .editOrNewSpec()
                    .withImage(Environment.CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN)
                .endSpec();
        }
    }

    public static DockerOutput dockerOutput(String imageName) {
        DockerOutputBuilder dockerOutputBuilder = new DockerOutputBuilder().withImage(imageName);
        if (Environment.CONNECT_BUILD_REGISTRY_SECRET != null && !Environment.CONNECT_BUILD_REGISTRY_SECRET.isEmpty()) {
            dockerOutputBuilder.withPushSecret(Environment.CONNECT_BUILD_REGISTRY_SECRET);
        }

        // if we use Kind we add insecure option
        if (KubeClusterResource.getInstance().isKind()) {
            dockerOutputBuilder.withAdditionalKanikoOptions(
                // --insecure for PUSH via HTTP instead of HTTPS
                "--insecure");
        }

        return dockerOutputBuilder.build();
    }
}
