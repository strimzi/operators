/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(INTERNAL_CLIENTS_USED)
@Tag(BRIDGE)
@Tag(REGRESSION)
@ParallelSuite
class HttpBridgeScramShaST extends HttpBridgeAbstractST {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeScramShaST.class);
    private static final String NAMESPACE = "bridge-scram-sha-namespace";
    private final String httpBridgeScramShaClusterName = "http-bridge-scram-sha-cluster-name";

    private String kafkaClientsPodName;
    private KafkaBridgeExampleClients kafkaBridgeClientJob;

    @ParallelTest
    void testSendSimpleMessageTlsScramSha(ExtensionContext extensionContext) {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaBridgeExampleClients kafkaBridgeClientJb = kafkaBridgeClientJob.toBuilder().withTopicName(topicName).build();

        // Create topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeScramShaClusterName, topicName)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());
        resourceManager.createResource(extensionContext, kafkaBridgeClientJb.producerStrimziBridge()
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(httpBridgeScramShaClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @ParallelTest
    void testReceiveSimpleMessageTlsScramSha(ExtensionContext extensionContext) {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaBridgeExampleClients kafkaBridgeClientJb = kafkaBridgeClientJob.toBuilder().withTopicName(topicName).build();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(httpBridgeScramShaClusterName, TOPIC_NAME)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata().build());
        resourceManager.createResource(extensionContext, kafkaBridgeClientJb.consumerStrimziBridge()
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        // Send messages to Kafka
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(httpBridgeScramShaClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withUsingPodName(kafkaClientsPodName)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(internalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        cluster.createNamespace(extensionContext, NAMESPACE);

        LOGGER.info("Deploy Kafka and KafkaBridge before tests");

        // Deploy kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(httpBridgeScramShaClusterName, 1, 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .build())
                .endKafka()
            .endSpec().build());

        String kafkaClientsName = NAMESPACE + "-shared-" + Constants.KAFKA_CLIENTS;

        // Create Kafka user
        KafkaUser scramShaUser = KafkaUserTemplates.scramShaUser(httpBridgeScramShaClusterName, USER_NAME)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();

        resourceManager.createResource(extensionContext, scramShaUser);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(NAMESPACE, true, kafkaClientsName, scramShaUser).build());

        kafkaClientsPodName = kubeClient(NAMESPACE).listPodsByPrefixInName(NAMESPACE, kafkaClientsName).get(0).getMetadata().getName();

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(USER_NAME);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(httpBridgeScramShaClusterName));

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(httpBridgeScramShaClusterName,
            KafkaResources.tlsBootstrapAddress(httpBridgeScramShaClusterName), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                    .withNewConsumer()
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .endConsumer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(USER_NAME)
                        .withPasswordSecret(passwordSecret)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecret)
                    .endTls()
                .endSpec().build());

        producerName = producerName + new Random().nextInt(Integer.MAX_VALUE);
        consumerName = consumerName + new Random().nextInt(Integer.MAX_VALUE);

        kafkaBridgeClientJob = (KafkaBridgeExampleClients) new KafkaBridgeExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeScramShaClusterName))
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(NAMESPACE)
            .build();
    }
}
