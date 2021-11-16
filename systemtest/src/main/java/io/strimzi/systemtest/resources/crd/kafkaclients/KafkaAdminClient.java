/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Initial implementation of test-client, which supports Topic management operations only.
 * Create, delete, update topics and partitions increase (in bulk, with offsets).
 */
public class KafkaAdminClient {

    private static final Logger LOGGER = LogManager.getLogger(KafkaAdminClient.class);

    private final String adminName;
    private final String bootstrapAddress;
    private final String topicName;
    private final int topicCount;
    private final int topicOffset;
    private final int partitions;
    private final int replicationFactor;
    private final String topicOperation;
    private final String additionalConfig;
    private String namespaceName;

    public KafkaAdminClient(KafkaAdminClient.Builder builder) {
        if (builder.topicOperation == null || builder.topicOperation.isEmpty()) {
            throw new InvalidParameterException("TopicOperation must be set.");
        }
        if (builder.bootstrapAddress == null || builder.bootstrapAddress.isEmpty()) {
            throw new InvalidParameterException("Bootstrap server is not set.");
        }
        if ((builder.topicName == null || builder.topicName.isEmpty())
                && !(builder.topicOperation.equals("help") || builder.topicOperation.equals("list"))) {
            throw new InvalidParameterException("Topic name (or 'prefix' if topic count > 1) is not set.");
        }

        replicationFactor = (builder.replicationFactor <= 0) ? 1 : builder.replicationFactor;
        partitions = (builder.partitions <= 0) ? 1 : builder.partitions;
        topicCount = (builder.topicCount <= 0) ? 1 : builder.topicCount;
        topicOffset = builder.topicOffset;
        adminName = builder.adminName;
        bootstrapAddress = builder.bootstrapAddress;
        topicName = builder.topicName;
        additionalConfig = builder.additionalConfig;
        namespaceName = builder.namespaceName;
        topicOperation = builder.topicOperation;
    }

    public String getAdminName() {
        return adminName;
    }

    public String getBootstrapAddress() {
        return bootstrapAddress;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getTopicCount() {
        return topicCount;
    }

    public int getTopicOffset() {
        return topicOffset;
    }

    public String getAdditionalConfig() {
        return additionalConfig;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public int getPartitions() {
        return partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getTopicOperation() {
        return topicOperation;
    }

    public KafkaAdminClient.Builder newBuilder() {
        return new KafkaAdminClient.Builder();
    }

    public KafkaAdminClient.Builder updateBuilder(KafkaAdminClient.Builder builder) {
        return builder
            .withAdditionalConfig(getAdditionalConfig())
            .withBootstrapAddress(getBootstrapAddress())
            .withTopicOperation(getTopicOperation())
            .withTopicCount(getTopicCount())
            .withTopicOffset(getTopicOffset())
            .withPartitions(getPartitions())
            .withReplicationFactor(getReplicationFactor())
            .withKafkaAdminClientName(getAdminName())
            .withTopicName(getTopicName())
            .withNamespaceName(getNamespaceName());
    }

    public static String getAdminClientScramConfig(String namespace, String kafkaUsername, int timeoutMs) {
        final String saslJaasConfigEncrypted = kubeClient().getSecret(namespace, kafkaUsername).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = new String(Base64.getDecoder().decode(saslJaasConfigEncrypted), StandardCharsets.US_ASCII);
        return "sasl.mechanism=SCRAM-SHA-512\n" +
                "security.protocol=" + SecurityProtocol.SASL_PLAINTEXT + "\n" +
                "sasl.jaas.config=" + saslJaasConfigDecrypted + "\n" +
                "request.timeout.ms=" + timeoutMs;
    }

    public KafkaAdminClient.Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    public JobBuilder defaultAdmin() {
        if (namespaceName == null || namespaceName.isEmpty()) {
            LOGGER.info("Deploying {} to namespace: {}", adminName, ResourceManager.kubeClient().getNamespace());
            namespaceName = ResourceManager.kubeClient().getNamespace();
        }

        Map<String, String> adminLabels = new HashMap<>();
        adminLabels.put("app", adminName);
        adminLabels.put(Constants.KAFKA_ADMIN_CLIENT_LABEL_KEY, Constants.KAFKA_ADMIN_CLIENT_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(namespaceName)
                .withLabels(adminLabels)
                .withName(adminName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(adminName)
                        .withNamespace(namespaceName)
                        .withLabels(adminLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                            .withContainers()
                                .addNewContainer()
                                .withName(adminName)
                                .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                .withImage(Environment.TEST_ADMIN_IMAGE)
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(bootstrapAddress)
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC_OPERATION")
                                    .withValue(topicOperation)
                                .endEnv()
                                .addNewEnv()
                                    .withName("REPLICATION_FACTOR")
                                    .withValue(String.valueOf(replicationFactor))
                                .endEnv()
                                .addNewEnv()
                                    .withName("PARTITIONS")
                                    .withValue(String.valueOf(partitions))
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPICS_COUNT")
                                    .withValue(String.valueOf(topicCount))
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC_OFFSET")
                                    .withValue(String.valueOf(topicOffset))
                                .endEnv()
                                .addNewEnv()
                                    .withName("LOG_LEVEL")
                                    .withValue("DEBUG")
                                .endEnv()
                                .addNewEnv()
                                    .withName("ADDITIONAL_CONFIG")
                                    .withValue(additionalConfig)
                                .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public static class Builder {
        private int partitions;
        private int replicationFactor;
        private String topicOperation;
        private String adminName;
        private String bootstrapAddress;
        private String topicName;
        private int topicCount;
        private int topicOffset;
        private String additionalConfig;
        private String namespaceName;

        public KafkaAdminClient.Builder withKafkaAdminClientName(String adminName) {
            this.adminName = adminName;
            return this;
        }

        public KafkaAdminClient.Builder withBootstrapAddress(String bootstrapAddress) {
            this.bootstrapAddress = bootstrapAddress;
            return this;
        }

        public KafkaAdminClient.Builder withTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public KafkaAdminClient.Builder withTopicCount(int topicCount) {
            this.topicCount = topicCount;
            return this;
        }

        public KafkaAdminClient.Builder withTopicOffset(int topicOffset) {
            this.topicOffset = topicOffset;
            return this;
        }

        public KafkaAdminClient.Builder withAdditionalConfig(String additionalConfig) {
            this.additionalConfig = additionalConfig;
            return this;
        }

        public KafkaAdminClient.Builder withNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return this;
        }

        public KafkaAdminClient.Builder withPartitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        public KafkaAdminClient.Builder withTopicOperation(String topicOperation) {
            this.topicOperation = topicOperation;
            return this;
        }

        public KafkaAdminClient.Builder withReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public KafkaAdminClient build() {
            return new KafkaAdminClient(this);
        }
    }
}
