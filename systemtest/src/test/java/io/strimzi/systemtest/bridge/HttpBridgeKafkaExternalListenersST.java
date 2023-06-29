/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.KafkaBridgeSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.ServiceResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Random;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
public class HttpBridgeKafkaExternalListenersST extends AbstractST {
    private static final String BRIDGE_EXTERNAL_SERVICE =  "shared-http-bridge-external-service";
    private final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
    private final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

    @ParallelNamespaceTest("Creating a node port service and thus avoiding 409 error (service already exists)")
    void testScramShaAuthWithWeirdUsername(ExtensionContext extensionContext) {
        final TestStorage ts = new TestStorage(extensionContext);
        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in MirrorMaker spec
        final PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(weirdUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationScramSha512()
                .withUsername(weirdUserName)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(extensionContext, weirdUserName, new KafkaListenerAuthenticationScramSha512(), bridgeSpec, ts);
    }

    @ParallelNamespaceTest("Creating a node port service and thus avoiding 409 error")
    void testTlsAuthWithWeirdUsername(ExtensionContext extensionContext) {
        final TestStorage ts = new TestStorage(extensionContext);
        // Create weird named user with . and maximum of 64 chars -> TLS
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(ts.getClusterName()));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationTls()
                .withNewCertificateAndKey()
                    .withSecretName(weirdUserName)
                    .withCertificate("user.crt")
                    .withKey("user.key")
                .endCertificateAndKey()
            .endKafkaClientAuthenticationTls()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(extensionContext, weirdUserName, new KafkaListenerAuthenticationTls(), bridgeSpec, ts);
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    private void testWeirdUsername(ExtensionContext extensionContext, String weirdUserName, KafkaListenerAuthentication auth,
                                   KafkaBridgeSpec spec, TestStorage ts) {
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(ts.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(ts.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withAuth(auth)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                        .withPort(9094)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(true)
                        .withAuth(auth)
                        .build())
                .endKafka()
            .endSpec()
            .build());

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(ts.getClusterName() + "-" + producerName)
            .withConsumerName(ts.getClusterName() + "-" + consumerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(ts.getClusterName()))
            .withTopicName(ts.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withPort(Constants.HTTP_BRIDGE_DEFAULT_PORT)
            .withNamespaceName(ts.getNamespaceName())
            .build();

        // Create topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(ts).build());

        // Create user
        if (auth.getType().equals(Constants.TLS_LISTENER_DEFAULT_NAME)) {
            resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(ts.getNamespaceName(), ts.getClusterName(), weirdUserName)
                .editMetadata()
                    .withNamespace(ts.getNamespaceName())
                .endMetadata()
                .build());
        } else {
            resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(ts.getNamespaceName(), ts.getClusterName(), weirdUserName)
                .editMetadata()
                    .withNamespace(ts.getNamespaceName())
                .endMetadata()
                .build());
        }

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(ts.getClusterName(), KafkaResources.tlsBootstrapAddress(ts.getClusterName()), 1)
                .editMetadata()
                    .withNamespace(ts.getNamespaceName())
                .endMetadata()
                .withNewSpecLike(spec)
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(ts.getClusterName()))
                    .withNewHttp(Constants.HTTP_BRIDGE_DEFAULT_PORT)
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());

        final Service service = KafkaBridgeUtils.createBridgeNodePortService(ts.getClusterName(), ts.getNamespaceName(), BRIDGE_EXTERNAL_SERVICE);
        ServiceResource.createServiceResource(extensionContext, service, ts.getNamespaceName());

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.consumerStrimziBridge());

        final String kafkaProducerExternalName = "kafka-producer-external" + new Random().nextInt(Integer.MAX_VALUE);

        final List<ListenerStatus> listenerStatusList = KafkaResource.kafkaClient().inNamespace(ts.getNamespaceName()).withName(ts.getClusterName()).get().getStatus().getListeners();
        final String externalBootstrapServers = listenerStatusList.stream().filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers();

        final KafkaClients externalKafkaProducer = new KafkaClientsBuilder()
            .withProducerName(kafkaProducerExternalName)
            .withBootstrapAddress(externalBootstrapServers)
            .withNamespaceName(ts.getNamespaceName())
            .withTopicName(ts.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .withUsername(weirdUserName)
            // we disable ssl.endpoint.identification.algorithm for external listener (i.e., NodePort),
            // because TLS hostname verification is not supported on such listener type.
            .withAdditionalConfig("ssl.endpoint.identification.algorithm=\n")
            .build();

        if (auth.getType().equals(Constants.TLS_LISTENER_DEFAULT_NAME)) {
            // tls producer
            resourceManager.createResource(extensionContext, externalKafkaProducer.producerTlsStrimzi(ts.getClusterName()));
        } else {
            // scram-sha producer
            resourceManager.createResource(extensionContext, externalKafkaProducer.producerScramShaTlsStrimzi(ts.getClusterName()));
        }

        ClientUtils.waitForClientSuccess(kafkaProducerExternalName, ts.getNamespaceName(), MESSAGE_COUNT);

        ClientUtils.waitForClientSuccess(ts.getClusterName() + "-" + consumerName, ts.getNamespaceName(), MESSAGE_COUNT);
    }

    @BeforeAll
    void setUp(final ExtensionContext extensionContext) {
        clusterOperator = clusterOperator.defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }

}
