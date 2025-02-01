/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import lombok.ToString;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaConnect} cluster.
 */
@ToString
public class KafkaConnectResources {
    protected KafkaConnectResources() { }

    /**
     * Returns the name of the Kafka Connect {@code Deployment} for a {@code KafkaConnect} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding Kafka Connect {@code Deployment}.
     */
    public static String componentName(String clusterName) {
        return clusterName + "-connect";
    }

    /**
     * Returns the name of the Kafka Connect {@code ServiceAccount} for a {@code KafkaConnect} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding Kafka Connect {@code ServiceAccount}.
     */
    public static String serviceAccountName(String clusterName) {
        return componentName(clusterName);
    }

    /**
     * Returns the name of the HTTP REST {@code Service} for a {@code KafkaConnect} cluster of the given name. This
     * returns only the name of the service without any namespace. Therefore it cannot be used to connect to the Connect
     * REST API. USe the {@code qualifiedServiceName} or {@code url} methods instead.
     *
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding {@code Service}.
     */
    public static String serviceName(String clusterName) {
        return clusterName + "-connect-api";
    }

    /**
     * Returns the name of the Kafka Secret with JMX credentials.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     *
     * @return The name of the corresponding Kafka JMX Secret.
     */
    public static String jmxSecretName(String clusterName) {
        return clusterName + "-kafka-connect-jmx";
    }

    /**
     * Returns the name of the Kafka Connect configurations including metrics and log {@code ConfigMap}
     * for a {@code KafkaConnect} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding KafkaConnect configurations {@code ConfigMap}.
     */
    public static String configMapName(String clusterName) {
        return componentName(clusterName) + "-config";
    }

    /**
     * Returns the name of the Kafka Connect config offsets for a {@code KafkaConnect} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding KafkaConnect config offsets value.
     */
    public static String configStorageTopicOffsets(String clusterName) {
        return componentName(clusterName) + "-offsets";
    }

    /**
     * Returns the name of the Kafka Connect config status for a {@code KafkaConnect} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding KafkaConnect config status value.
     */
    public static String configStorageTopicStatus(String clusterName) {
        return componentName(clusterName) + "-status";
    }

    /**
     * Returns qualified name of the service which works across different namespaces.
     *
     * @param clusterName   Name of the Connect CR
     * @param namespace     Namespace of the Connect deployment
     * @return              qualified namespace in the format "&lt;service-name&gt;.&lt;namespace&gt;.svc"
     */
    public static String qualifiedServiceName(String clusterName, String namespace) {
        return serviceName(clusterName) + "." + namespace + ".svc";
    }

    /**
     * Returns the URL of the Kafka Connect REST API for a {@code KafkaConnect} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @param namespace The namespace where {@code KafkaConnect} cluster is running.
     * @param port The port on which the {@code KafkaConnect} API is available.
     * @return The base URL of the {@code KafkaConnect} REST API.
     */
    public static String url(String clusterName, String namespace, int port) {
        return "http://" + qualifiedServiceName(clusterName, namespace) + ":" + port;
    }

    /**
     * Get the name of the resource init container role binding given the name of the {@code namespace} and {@code cluster}.
     *
     * @param clusterName   The cluster name.
     * @param namespace     The namespace.
     *
     * @return The name of the init container's cluster role binding.
     */
    public static String initContainerClusterRoleBindingName(String clusterName, String namespace) {
        return "strimzi-" + namespace + "-" + componentName(clusterName) + "-init";
    }

    /**
     * Returns the name of the Kafka Connect {@code ConfigMap} for a {@code KafkaConnect} build which contains the Dockerfile.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding Kafka Connect {@code ConfigMap}.
     */
    public static String dockerFileConfigMapName(String clusterName) {
        return componentName(clusterName) + "-dockerfile";
    }

    /**
     * Returns the name of the Kafka Connect {@code Pod} for a {@code KafkaConnect} build that builds the new image.
     *
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding Kafka Connect build {@code Pod}.
     */
    public static String buildPodName(String clusterName) {
        return componentName(clusterName) + "-build";
    }

    /**
     * Returns the name of the Kafka Connect Build {@code ServiceAccount} for a {@code KafkaConnect} cluster of the given name.
     *
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     *
     * @return The name of the corresponding Kafka Connect Build {@code ServiceAccount}.
     */
    public static String buildServiceAccountName(String clusterName) {
        return componentName(clusterName) + "-build";
    }

    /**
     * Returns the name of the Kafka Connect {@code BuildConfig} for a {@code KafkaConnect} build that builds the new image.
     *
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding Kafka Connect {@code BuildConfig}.
     */
    public static String buildConfigName(String clusterName) {
        return componentName(clusterName) + "-build";
    }

    /**
     * Returns the name of the Kafka Connect {@code Build} for a {@code KafkaConnect} build that builds the new image.
     *
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @param buildVersion The version of the build for which the name should be generated
     *
     * @return The name of the corresponding Kafka Connect {@code BuildConfig} {@code Build}.
     */
    public static String buildName(String clusterName, Long buildVersion) {
        return buildConfigName(clusterName) + "-" + buildVersion;
    }
}
