/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.FIPSNotSupported;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClients;
import io.strimzi.systemtest.kafkaclients.internalClients.BridgeClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClientsBuilder;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.ARM64_UNSUPPORTED;
import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER;
import static io.strimzi.systemtest.TestConstants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestConstants.OAUTH;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(ACCEPTANCE)
@Tag(ARM64_UNSUPPORTED)
@FIPSNotSupported("Keycloak is not customized to run on FIPS env - https://github.com/strimzi/strimzi-kafka-operator/issues/8331")
public class OauthTlsST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthTlsST.class);

    private final String oauthClusterName = "oauth-cluster-tls-name";

    @Description(
            "As an OAuth producer, I am able to produce messages to the Kafka Broker\n" +
            "As an OAuth consumer, I am able to consumer messages from the Kafka Broker using encrypted communication")
    @ParallelTest
    void testProducerConsumer(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(extensionContext, oauthExampleClients.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, oauthExampleClients.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
    }

    @Description("As an OAuth KafkaConnect, I am able to sink messages from Kafka Broker topic using encrypted communication.")
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerConnect(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(extensionContext, oauthExampleClients.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, oauthExampleClients.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, oauthClusterName, 1)
            .editSpec()
                .withConfig(connectorConfig)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-connect")
                    .withNewClientSecret()
                    .withSecretName("my-connect-oauth")
                    .withKey(OAUTH_KEY)
                    .endClientSecret()
                    .withTlsTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                            .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                            .build())
                    .withDisableTlsHostnameVerification(true)
                .endKafkaClientAuthenticationOAuth()
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(oauthClusterName + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(oauthClusterName + "-kafka-bootstrap:9093")
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, connect, ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, testStorage.getScraperName()).build());

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        String kafkaConnectPodName = kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String scraperPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, testStorage.getScraperName()).get(0).getMetadata().getName();
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(Environment.TEST_SUITE_NAMESPACE, scraperPodName, testStorage.getTopicName(), TestConstants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 8083));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, "\"Hello-world - 99\"");
    }

    @Description("As a OAuth bridge, i am able to send messages to bridge endpoint using encrypted communication")
    @ParallelTest
    @Tag(BRIDGE)
    void testProducerConsumerBridge(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(extensionContext, oauthExampleClients.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, oauthExampleClients.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(oauthClusterName, KafkaResources.tlsBootstrapAddress(oauthClusterName), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewTls()
                    .withTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withCertificate("ca.crt")
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(oauthClusterName)).build())
                .endTls()
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-bridge")
                    .withNewClientSecret()
                        .withSecretName(BRIDGE_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                    .addNewTlsTrustedCertificate()
                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                    .endTlsTrustedCertificate()
                    .withDisableTlsHostnameVerification(true)
                .endKafkaClientAuthenticationOAuth()
            .endSpec()
            .build());

        producerName = "bridge-producer-" + testStorage.getClusterName();

        BridgeClients kafkaBridgeClientJob = new BridgeClientsBuilder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(10)
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaBridgeClientJob.producerStrimziBridge());
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
    }

    @Description("As a OAuth MirrorMaker, I am able to replicate Topic data using using encrypted communication")
    @IsolatedTest("Using more tha one Kafka cluster in one Namespace")
    @Tag(MIRROR_MAKER)
    @Tag(NODEPORT_SUPPORTED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMirrorMaker(ExtensionContext extensionContext) {
        // Nodeport needs cluster wide rights to work properly which is not possible with STRIMZI_RBAC_SCOPE=NAMESPACE
        assumeFalse(Environment.isNamespaceRbacScope());
        final TestStorage testStorage = new TestStorage(extensionContext);

        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());

        KafkaOauthClients oauthExampleClients = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(extensionContext, oauthExampleClients.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, oauthExampleClients.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        String targetKafkaCluster = oauthClusterName + "-target";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(targetKafkaCluster, 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(OauthAbstractST.BUILD_OAUTH_TLS_LISTENER.apply(keycloakInstance),
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                        .build())
                                    .withDisableTlsHostnameVerification(true)
                                .endKafkaListenerAuthenticationOAuth()
                                .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(oauthClusterName, oauthClusterName, targetKafkaCluster,
            ClientUtils.generateRandomConsumerGroup(), 1, true)
                .editMetadata()
                    .withNamespace(Environment.TEST_SUITE_NAMESPACE)
                .endMetadata()
                .editSpec()
                    .withNewConsumer()
                        // this is for kafka tls connection
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(oauthClusterName))
                                .build())
                        .endTls()
                        .withBootstrapServers(KafkaResources.tlsBootstrapAddress(oauthClusterName))
                        .withGroupId(ClientUtils.generateRandomConsumerGroup())
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withNewKafkaClientAuthenticationOAuth()
                            .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            // this is for authorization server tls connection
                            .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                .build())
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaClientAuthenticationOAuth()
                    .endConsumer()
                    .withNewProducer()
                        .withBootstrapServers(KafkaResources.tlsBootstrapAddress(targetKafkaCluster))
                        // this is for kafka tls connection
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(targetKafkaCluster))
                                .build())
                        .endTls()
                        .withNewKafkaClientAuthenticationOAuth()
                            .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            // this is for authorization server tls connection
                            .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                .build())
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaClientAuthenticationOAuth()
                        .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                    .endProducer()
                .endSpec()
                .build());

        String mirrorMakerPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, KafkaMirrorMakerResources.componentName(oauthClusterName)).get(0).getMetadata().getName();
        String kafkaMirrorMakerLogs = kubeClient().logsInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, mirrorMakerPodName);

        assertThat(kafkaMirrorMakerLogs,
            not(containsString("keytool error: java.io.FileNotFoundException: /opt/kafka/consumer-oauth-certs/**/* (No such file or directory)")));

        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, testStorage.getUsername()).build());
        KafkaUserUtils.waitForKafkaUserCreation(Environment.TEST_SUITE_NAMESPACE, testStorage.getUsername());

        LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", targetKafkaCluster);

        KafkaOauthClients kafkaOauthClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withClientUserName(testStorage.getUsername())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(targetKafkaCluster))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withOauthClientSecret(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaOauthClientJob.consumerStrimziOauthTls(targetKafkaCluster));

        ClientUtils.waitForClientSuccess(consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
    }

    @ParallelTest
    void testIntrospectionEndpoint(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());

        keycloakInstance.setIntrospectionEndpointUri("https://" + keycloakInstance.getHttpsUri() + "/realms/internal/protocol/openid-connect/token/introspect");
        String introspectionKafka = oauthClusterName + "-intro";

        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                .build();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(introspectionKafka, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName("tls")
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationOAuth()
                            .withClientId(OAUTH_KAFKA_BROKER_NAME)
                            .withNewClientSecret()
                                .withSecretName(OAUTH_KAFKA_BROKER_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            .withAccessTokenIsJwt(false)
                            .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                            .withIntrospectionEndpointUri(keycloakInstance.getIntrospectionEndpointUri())
                            .withTlsTrustedCertificates(cert)
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaListenerAuthenticationOAuth()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaOauthClients oauthInternalClientIntrospectionJob = new KafkaOauthClientsBuilder()
                .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(introspectionKafka))
                .withTopicName(testStorage.getTopicName())
                .withMessageCount(testStorage.getMessageCount())
                .withOauthClientId(OAUTH_CLIENT_NAME)
                .withOauthClientSecret(OAUTH_CLIENT_SECRET)
                .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .build();

        resourceManager.createResourceWithWait(extensionContext, oauthInternalClientIntrospectionJob.producerStrimziOauthTls(introspectionKafka));
        ClientUtils.waitForClientSuccess(producerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, oauthInternalClientIntrospectionJob.consumerStrimziOauthTls(introspectionKafka));
        ClientUtils.waitForClientSuccess(consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext) {
        super.setupCoAndKeycloak(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        keycloakInstance.setRealm("internal", true);

        LOGGER.info("Keycloak settings {}", keycloakInstance.toString());

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(oauthClusterName, 3)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(OauthAbstractST.BUILD_OAUTH_TLS_LISTENER.apply(keycloakInstance))
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, OAUTH_CLIENT_NAME).build());
    }
}



