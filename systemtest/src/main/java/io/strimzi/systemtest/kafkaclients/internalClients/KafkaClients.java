/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.PodSecurityProfile;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.ClientUtils;
import io.sundr.builder.annotations.Buildable;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Buildable(editableEnabled = false)
public class KafkaClients extends BaseClients {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClients.class);

    private String producerName;
    private String consumerName;
    private String message;
    private int messageCount;
    private String consumerGroup;
    private long delayMs;
    private String username;
    private String caCertSecretName;
    private String headers;
    private PodSecurityProfile podSecurityPolicy;
    private String messagesPerTransaction;

    public String getProducerName() {
        return producerName;
    }

    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        if (message == null || message.isEmpty()) {
            message = "Hello-world";
        }
        this.message = message;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        if (messageCount <= 0) {
            throw new InvalidParameterException("Message count is less than 1");
        }
        this.messageCount = messageCount;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void generateNewConsumerGroup() {
        final String newConsumerGroup = ClientUtils.generateRandomConsumerGroup();
        LOGGER.info("Regenerating new consumer group {} for clients {} {}", newConsumerGroup, this.getProducerName(), this.getConsumerName());
        this.setConsumerGroup(newConsumerGroup);
    }

    public void setConsumerGroup(String consumerGroup) {
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            LOGGER.info("Consumer group were not specified going to create the random one");
            consumerGroup = ClientUtils.generateRandomConsumerGroup();
        }
        this.consumerGroup = consumerGroup;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getCaCertSecretName() {
        return caCertSecretName;
    }

    public void setCaCertSecretName(String caCertSecretName) {
        this.caCertSecretName = caCertSecretName;
    }

    public String getHeaders() {
        return headers;
    }

    public void setHeaders(String headers) {
        this.headers = headers;
    }

    public PodSecurityProfile getPodSecurityPolicy() {
        return this.podSecurityPolicy;
    }
    public void setPodSecurityPolicy(final PodSecurityProfile podSecurityPolicy) {
        this.podSecurityPolicy = podSecurityPolicy;
    }

    public void setMessagesPerTransaction(String messagesPerTransaction) {
        this.messagesPerTransaction = messagesPerTransaction;
    }

    public String getMessagesPerTransaction() {
        return messagesPerTransaction;
    }

    public Job producerStrimzi() {
        return defaultProducerStrimzi().build();
    }

    public Job producerScramShaPlainStrimzi() {
        this.configureScramSha(SecurityProtocol.SASL_PLAINTEXT);
        return defaultProducerStrimzi().build();
    }

    public Job producerScramShaTlsStrimzi(final String clusterName) {
        this.configureScramSha(SecurityProtocol.SASL_SSL);

        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addToEnv(this.getClusterCaCertEnv(clusterName))
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    public Job producerTlsStrimzi(final String clusterName) {
        this.configureTls();

        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addToEnv(this.getClusterCaCertEnv(clusterName))
                            .addAllToEnv(this.getTlsEnvVars())
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    public JobBuilder defaultProducerStrimzi() {
        if (producerName == null || producerName.isEmpty()) {
            throw new InvalidParameterException("Producer name is not set.");
        }

        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);
        producerLabels.put(TestConstants.KAFKA_CLIENTS_LABEL_KEY, TestConstants.KAFKA_CLIENTS_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        final JobBuilder builder = new JobBuilder()
            .withNewMetadata()
                .withNamespace(this.getNamespaceName())
                .withLabels(producerLabels)
                .withName(producerName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(producerName)
                        .withNamespace(this.getNamespaceName())
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                        .withContainers()
                            .addNewContainer()
                                .withName(producerName)
                                .withImagePullPolicy(TestConstants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                .withImage(Environment.TEST_CLIENTS_IMAGE)
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(this.getBootstrapAddress())
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(this.getTopicName())
                                .endEnv()
                                .addNewEnv()
                                    .withName("DELAY_MS")
                                    .withValue(String.valueOf(delayMs))
                                .endEnv()
                                .addNewEnv()
                                    .withName("LOG_LEVEL")
                                    .withValue("DEBUG")
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue(String.valueOf(messageCount))
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE")
                                    .withValue(message)
                                .endEnv()
                                .addNewEnv()
                                    .withName("PRODUCER_ACKS")
                                    .withValue("all")
                                .endEnv()
                                .addNewEnv()
                                    .withName("ADDITIONAL_CONFIG")
                                    .withValue(this.getAdditionalConfig())
                                .endEnv()
                                .addNewEnv()
                                    .withName("BLOCKING_PRODUCER")
                                    .withValue("true")
                                .endEnv()
                                .addNewEnv()
                                    .withName("CLIENT_TYPE")
                                    .withValue("KafkaProducer")
                                .endEnv()
                            .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();

        if (this.getHeaders() != null) {
            builder
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .addNewEnv()
                                    .withName("HEADERS")
                                    .withValue(this.getHeaders())
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec();
        }

        if (this.getMessagesPerTransaction() != null) {
            builder
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .addNewEnv()
                                    .withName("MESSAGES_PER_TRANSACTION")
                                    .withValue(this.getMessagesPerTransaction())
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec();
        }

        if (PodSecurityProfile.RESTRICTED == this.podSecurityPolicy) {
            this.enableRestrictedProfile(builder);
        }

        return builder;
    }

    public Job consumerScramShaPlainStrimzi() {
        this.configureScramSha(SecurityProtocol.SASL_PLAINTEXT);
        return defaultConsumerStrimzi().build();
    }

    public Job consumerScramShaTlsStrimzi(final String clusterName) {
        this.configureScramSha(SecurityProtocol.SASL_SSL);

        return defaultConsumerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addToEnv(this.getClusterCaCertEnv(clusterName))
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    public Job consumerTlsStrimzi(final String clusterName) {
        this.configureTls();

        return defaultConsumerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addToEnv(this.getClusterCaCertEnv(clusterName))
                            .addAllToEnv(this.getTlsEnvVars())
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    public Job consumerStrimzi() {
        return defaultConsumerStrimzi().build();
    }

    public JobBuilder defaultConsumerStrimzi() {
        if (consumerName == null || consumerName.isEmpty()) {
            throw new InvalidParameterException("Consumer name is not set.");
        }

        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);
        consumerLabels.put(TestConstants.KAFKA_CLIENTS_LABEL_KEY, TestConstants.KAFKA_CLIENTS_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        final JobBuilder builder = new JobBuilder()
            .withNewMetadata()
                .withNamespace(this.getNamespaceName())
                .withLabels(consumerLabels)
                .withName(consumerName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(consumerLabels)
                        .withNamespace(this.getNamespaceName())
                        .withName(consumerName)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                            .withContainers()
                                .addNewContainer()
                                    .withName(consumerName)
                                    .withImagePullPolicy(TestConstants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                                    .withImage(Environment.TEST_CLIENTS_IMAGE)
                                    .addNewEnv()
                                        .withName("BOOTSTRAP_SERVERS")
                                        .withValue(this.getBootstrapAddress())
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC")
                                        .withValue(this.getTopicName())
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("DELAY_MS")
                                        .withValue(String.valueOf(delayMs))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("LOG_LEVEL")
                                        .withValue("DEBUG")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MESSAGE_COUNT")
                                        .withValue(String.valueOf(messageCount))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("GROUP_ID")
                                        .withValue(consumerGroup)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("ADDITIONAL_CONFIG")
                                        .withValue(this.getAdditionalConfig())
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("CLIENT_TYPE")
                                        .withValue("KafkaConsumer")
                                    .endEnv()
                                .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();

        if (PodSecurityProfile.RESTRICTED == this.podSecurityPolicy) {
            this.enableRestrictedProfile(builder);
        }
        return builder;
    }

    protected EnvVar getClusterCaCertEnv(String clusterName) {
        final String caSecretName = this.getCaCertSecretName() == null || this.getCaCertSecretName().isEmpty() ?
            KafkaResources.clusterCaCertificateSecretName(clusterName) : this.getCaCertSecretName();

        return new EnvVarBuilder()
            .withName("CA_CRT")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(caSecretName)
                    .withKey("ca.crt")
                .endSecretKeyRef()
            .endValueFrom()
            .build();
    }

    final protected void configureScramSha(SecurityProtocol securityProtocol) {
        if (this.getUsername() == null || this.getUsername().isEmpty()) {
            throw new InvalidParameterException("User name for SCRAM-SHA is not set");
        }

        final String saslJaasConfigEncrypted = ResourceManager.kubeClient().getSecret(this.getNamespaceName(), this.getUsername()).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = Util.decodeFromBase64(saslJaasConfigEncrypted);

        this.setAdditionalConfig(this.getAdditionalConfig() +
            // scram-sha
            "sasl.mechanism=SCRAM-SHA-512\n" +
            "security.protocol=" + securityProtocol + "\n" +
            "sasl.jaas.config=" + saslJaasConfigDecrypted);
    }

    final protected void configureTls() {
        this.setAdditionalConfig(this.getAdditionalConfig() +
            "sasl.mechanism=GSSAPI\n" +
            "security.protocol=" + SecurityProtocol.SSL + "\n");
    }

    protected List<EnvVar> getTlsEnvVars() {
        if (this.getUsername() == null || this.getUsername().isEmpty()) {
            throw new InvalidParameterException("User name for TLS is not set");
        }

        EnvVar userCrt = new EnvVarBuilder()
            .withName("USER_CRT")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(this.getUsername())
                    .withKey("user.crt")
                .endSecretKeyRef()
            .endValueFrom()
            .build();

        EnvVar userKey = new EnvVarBuilder()
            .withName("USER_KEY")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(this.getUsername())
                    .withKey("user.key")
                .endSecretKeyRef()
            .endValueFrom()
            .build();

        return List.of(userCrt, userKey);
    }

    private void enableRestrictedProfile(final JobBuilder jobBuilder) {
        jobBuilder
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .withNewSecurityContext()
                            .withRunAsNonRoot(true)
                            .withNewSeccompProfile()
                                .withType("RuntimeDefault")
                            .endSeccompProfile()
                        .endSecurityContext()
                        .editFirstContainer()
                            .withNewSecurityContext()
                                .withAllowPrivilegeEscalation(false)
                                .withNewCapabilities()
                                    .withDrop("ALL")
                                .endCapabilities()
                                .withRunAsNonRoot(true)
                                .withNewSeccompProfile()
                                    .withType("RuntimeDefault")
                                .endSeccompProfile()
                            .endSecurityContext()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }
}
