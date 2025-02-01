/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.KafkaTopicDescription;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.VerificationUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.COMPONENT_SCALING;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER2)
@Tag(CONNECT_COMPONENTS)
class MirrorMaker2ST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMaker2ST.class);

    @ParallelNamespaceTest
    void testMirrorMaker2() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int mirrorMakerReplicasCount = 2;

        Map<String, Object> expectedConfig = StUtils.loadProperties("bootstrap.servers=" + KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()) + "\n" +
                "group.id=mirrormaker2-cluster\n" +
                "key.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "value.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "header.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "config.storage.topic=mirrormaker2-cluster-configs\n" +
                "status.storage.topic=mirrormaker2-cluster-status\n" +
                "offset.storage.topic=mirrormaker2-cluster-offsets\n" +
                "config.storage.replication.factor=-1\n" +
                "status.storage.replication.factor=-1\n" +
                "offset.storage.replication.factor=-1\n");

        // Deploy source and target kafka
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build());

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, mirrorMakerReplicasCount, false)
            .editSpec()
                .editFirstMirror()
                    .editSourceConnector()
                        .addToConfig("refresh.topics.interval.seconds", "1")
                    .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        // Deploy source topic
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), 3).build());

        // Check brokers availability
        LOGGER.info("Messages exchange - Topic: {}, cluster: {} and message count of {}",
            testStorage.getTopicName(), testStorage.getSourceClusterName(), testStorage.getMessageCount());

        final KafkaClients sourceClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()));
        resourceManager.createResourceWithWait(sourceClients.producerStrimzi(), sourceClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Verifying configurations in config map");
        ConfigMap configMap = kubeClient().namespace(testStorage.getNamespaceName()).getConfigMap(KafkaMirrorMaker2Resources.configMapName(testStorage.getClusterName()));
        String connectConfigurations = configMap.getData().get("kafka-connect.properties");
        Map<String, Object> config = StUtils.loadProperties(connectConfigurations);
        assertThat(config.entrySet().containsAll(expectedConfig.entrySet()), is(true));
        VerificationUtils.verifyClusterOperatorMM2DockerImage(clusterOperator.getDeploymentNamespace(), testStorage.getNamespaceName(), testStorage.getClusterName());

        VerificationUtils.verifyPodsLabels(testStorage.getNamespaceName(), KafkaMirrorMaker2Resources.componentName(testStorage.getClusterName()), testStorage.getMM2Selector());
        VerificationUtils.verifyServiceLabels(testStorage.getNamespaceName(), KafkaMirrorMaker2Resources.serviceName(testStorage.getClusterName()), testStorage.getMM2Selector());
        VerificationUtils.verifyConfigMapsLabels(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName());
        VerificationUtils.verifyServiceAccountsLabels(testStorage.getNamespaceName(), testStorage.getSourceClusterName());

        LOGGER.info("Now setting Topic to {} and cluster to {} - the messages should be mirrored",
            testStorage.getMirroredSourceTopicName(), testStorage.getTargetClusterName());

        final KafkaClients targetClients = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .build();

        LOGGER.info("Consumer in target cluster and Topic should receive {} messages", testStorage.getMessageCount());
        resourceManager.createResourceWithWait(targetClients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LOGGER.info("Mirrored successful");

        // Test Manual Rolling Update
        LOGGER.info("MirrorMaker2 manual rolling update");
        final Map<String, String> mm2PodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector());
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), KafkaMirrorMaker2Resources.componentName(testStorage.getClusterName()), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getMM2Selector(), mirrorMakerReplicasCount, mm2PodsSnapshot);

        resourceManager.createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName())
            ).build()
        );
        final AdminClient targetClusterAdminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());

        LOGGER.info("Verifying topic {} has expected partitions: {}", testStorage.getMirroredSourceTopicName(), 3);
        final KafkaTopicDescription mirroredTopic = targetClusterAdminClient.describeTopic(testStorage.getMirroredSourceTopicName());
        assertThat(mirroredTopic.partitionCount(), is(3));

        // Replace source topic resource with new data and check that mm2 update target topic as well
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getTopicName(), kt -> kt.getSpec().setPartitions(8));
        AdminClientUtils.waitForTopicPartitionInKafka(targetClusterAdminClient, testStorage.getMirroredSourceTopicName(), 8);
    }

    /**
     * Test mirroring messages by MirrorMaker 2 over tls transport using mutual tls auth
     */
    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    void testMirrorMaker2TlsAndTlsClientAuth() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );

        // Deploy source kafka with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withAuth(new KafkaListenerAuthenticationTls())
                        .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );

        // Deploy target kafka with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withAuth(new KafkaListenerAuthenticationTls())
                        .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy topic
        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), 3).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getSourceUsername(), testStorage.getSourceClusterName()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getTargetUsername(), testStorage.getTargetClusterName()).build()
        );

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getSourceClusterName()));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getTargetClusterName()));

        // Check brokers availability
        LOGGER.info("Messages exchange - Topic: {}, cluster: {}", testStorage.getSourceClusterName(), testStorage.getMessageCount());
        final KafkaClients sourceClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getSourceClusterName()))
            .withUsername(testStorage.getSourceUsername())
            .build();
        resourceManager.createResourceWithWait(sourceClients.producerTlsStrimzi(testStorage.getSourceClusterName()), sourceClients.consumerTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, 1, true)
            .editSpec()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getSourceClusterName()))
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(testStorage.getSourceUsername())
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endCluster()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getTargetClusterName()))
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(testStorage.getTargetUsername())
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endCluster()
            .endSpec()
            .build());

        LOGGER.info("Consuming from mirrored Topic: {}, cluster: {}, user: {}", testStorage.getMirroredSourceTopicName(), testStorage.getTargetClusterName(), testStorage.getTargetClusterName());
        final KafkaClients targetClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .withUsername(testStorage.getTargetUsername())
            .build();
        resourceManager.createResourceWithWait(targetClients.consumerTlsStrimzi(testStorage.getTargetClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LOGGER.info("Checking topic is mirrored correctly in target cluster");

        // Deploy kafka admin on communicating with target kafka cluster.

        resourceManager.createResourceWithWait(
            AdminClientTemplates.tlsAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getTargetUsername(),
                testStorage.getAdminName(),
                testStorage.getTargetClusterName(),
                KafkaResources.tlsBootstrapAddress(testStorage.getTargetClusterName())
            ));

        final AdminClient targetClusterAdminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());

        AdminClientUtils.waitForTopicPresence(targetClusterAdminClient, testStorage.getMirroredSourceTopicName());
        assertThat(targetClusterAdminClient.describeTopic(testStorage.getMirroredSourceTopicName()).partitionCount(), is(3));
    }

    /**
     * Test mirroring messages by MirrorMaker 2 over tls transport using scram-sha-512 auth
     */
    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelNamespaceTest
    void testMirrorMaker2TlsAndScramSha512Auth() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int partitionCount = 3;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                    .endKafka()
                .endSpec()
                .build()
        );

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), partitionCount).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getSourceUsername(), testStorage.getSourceClusterName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getTargetUsername(), testStorage.getTargetClusterName()).build()
        );

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker2 spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(testStorage.getSourceUsername());
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker2 spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(testStorage.getTargetUsername());
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getSourceClusterName()));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getTargetClusterName()));


        LOGGER.info("Messages exchange - Topic: {}, cluster: {}", testStorage.getTopicName(), testStorage.getSourceClusterName());
        final KafkaClients sourceClients = ClientUtils.getInstantScramShaClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getSourceClusterName()))
            .withUsername(testStorage.getSourceUsername())
            .build();
        resourceManager.createResourceWithWait(sourceClients.producerScramShaTlsStrimzi(testStorage.getSourceClusterName()), sourceClients.consumerScramShaTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, 1, true)
            .editSpec()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getSourceClusterName()))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(testStorage.getSourceUsername())
                        .withPasswordSecret(passwordSecretSource)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endCluster()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getTargetClusterName()))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(testStorage.getTargetUsername())
                        .withPasswordSecret(passwordSecretTarget)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endCluster()
            .endSpec()
            .build());

        LOGGER.info("Consuming from mirrored Topic: {}, cluster: {}, user: {}", testStorage.getMirroredSourceTopicName(), testStorage.getTargetClusterName(), testStorage.getTargetClusterName());
        final KafkaClients targetClients = ClientUtils.getInstantScramShaClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .withUsername(testStorage.getTargetUsername())
            .build();
        resourceManager.createResourceWithWait(targetClients.consumerScramShaTlsStrimzi(testStorage.getTargetClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LOGGER.info("Checking topic is mirrored correctly in target cluster");

        // deploy admin client
        resourceManager.createResourceWithWait(
            AdminClientTemplates.scramShaOverTlsAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getTargetUsername(),
                testStorage.getAdminName(),
                testStorage.getTargetClusterName(),
                KafkaResources.tlsBootstrapAddress(testStorage.getTargetClusterName())
        ));
        final AdminClient targetClusterAdminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());

        AdminClientUtils.waitForTopicPresence(targetClusterAdminClient, testStorage.getMirroredSourceTopicName());
        assertThat(targetClusterAdminClient.describeTopic(testStorage.getMirroredSourceTopicName()).partitionCount(), is(partitionCount));
    }

    @ParallelNamespaceTest
    @Tag(COMPONENT_SCALING)
    void testScaleMirrorMaker2UpAndDownToZero() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build());

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, 1, false).build());

        int scaleTo = 2;
        long mm2ObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();
        String mm2GenName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaMirrorMaker2 up <-------");

        LOGGER.info("Scaling subresource replicas to {}", scaleTo);

        cmdKubeClient(testStorage.getNamespaceName()).scaleByName(KafkaMirrorMaker2.RESOURCE_KIND, testStorage.getClusterName(), scaleTo);
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> mm2Pods = kubeClient(testStorage.getNamespaceName()).listPodNames(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);

        StUtils.waitUntilSupplierIsSatisfied("KafkaMirrorMaker2 expected size (status, replicas, pod count)",
            () -> kubeClient(testStorage.getNamespaceName()).listPodNames(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).size() == scaleTo &&
                KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getReplicas() == scaleTo &&
                KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getReplicas() == scaleTo);

        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        long actualObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getMetadata().getGeneration();

        assertTrue(mm2ObsGen < actualObsGen);

        mm2ObsGen = actualObsGen;

        LOGGER.info("-------> Scaling KafkaMirrorMaker2 down <-------");

        LOGGER.info("Scaling MirrorMaker2 to zero");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), mm2 -> mm2.getSpec().setReplicas(0));

        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), 0, true, () -> { });

        mm2Pods = kubeClient().listPodNames(testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        KafkaMirrorMaker2Status mm2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        actualObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getMetadata().getGeneration();

        assertThat(mm2Pods.size(), is(0));
        assertThat(mm2Status.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(actualObsGen, is(not(mm2ObsGen)));

        TestUtils.waitFor("Until MirrorMaker2 status url is null", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT, () -> {
            KafkaMirrorMaker2Status mm2StatusUrl = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
            return mm2StatusUrl.getUrl() == null;
        });
    }

    @ParallelNamespaceTest
    void testMirrorMaker2CorrectlyMirrorsHeaders() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build());
        // Deploy Topic for example clients
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName()).build());

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, 1, false)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .addToConfig("refresh.topics.interval.seconds", "1")
                        .endSourceConnector()
                    .endMirror()
                .endSpec().build());

        // Deploying example clients for checking if mm2 will mirror messages with headers

        final KafkaClients targetKafkaClientsJob = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .build();
        resourceManager.createResourceWithWait(targetKafkaClientsJob.consumerStrimzi());

        final KafkaClients sourceKafkaClientsJob = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()))
            .withHeaders("header_key_one=header_value_one, header_key_two=header_value_two")
            .build();
        resourceManager.createResourceWithWait(sourceKafkaClientsJob.producerStrimzi());

        ClientUtils.waitForInstantClientSuccess(testStorage, false);

        LOGGER.info("Checking log of {} Job if the headers are correct", testStorage.getConsumerName());
        String header1 = "key: header_key_one, value: header_value_one";
        String header2 = "key: header_key_two, value: header_value_two";
        String log = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getConsumerName()).get(0).getMetadata().getName(), "", testStorage.getMessageCount() + "s");
        assertThat(log, containsString(header1));
        assertThat(log, containsString(header2));
    }

    /*
     * This test is using the Kafka Identity Replication policy. This is what should be used by all new users.
     */
    @ParallelNamespaceTest
    void testIdentityReplicationPolicy() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );

        resourceManager.createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        // Create topic
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), 3).build());

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, 1, false)
            .editSpec()
                .editMirror(0)
                    .editSourceConnector()
                        .addToConfig("replication.policy.class", "org.apache.kafka.connect.mirror.IdentityReplicationPolicy")
                        .addToConfig("refresh.topics.interval.seconds", "1")
                    .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Sending and receiving messages via {}", testStorage.getSourceClusterName());
        final KafkaClients sourceClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()));
        resourceManager.createResourceWithWait(sourceClients.producerStrimzi(), sourceClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Consuming mirrored messages via {}", testStorage.getTargetClusterName());
        final KafkaClients targetClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()));
        resourceManager.createResourceWithWait(targetClients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    void testRestoreOffsetsInConsumerGroup() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String consumerGroup = ClientUtils.generateRandomConsumerGroup();

        final String sourceProducerName = testStorage.getProducerName() + "-source";
        final String sourceConsumerName = testStorage.getConsumerName() + "-source";

        final String targetProducerName = testStorage.getProducerName() + "-target";
        final String targetConsumerName = testStorage.getConsumerName() + "-target";

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build()
        );

        Map<String, Object> sourceConnectorConfig = new HashMap<>();
        sourceConnectorConfig.put("refresh.topics.interval.seconds", "1");
        sourceConnectorConfig.put("replication.factor", "1");
        sourceConnectorConfig.put("offset-syncs.topic.replication.factor", "1");

        Map<String, Object> checkpointConnectorConfig = new HashMap<>();
        checkpointConnectorConfig.put("refresh.groups.interval.seconds", "1");
        checkpointConnectorConfig.put("sync.group.offsets.enabled", "true");
        checkpointConnectorConfig.put("sync.group.offsets.interval.seconds", "1");
        checkpointConnectorConfig.put("emit.checkpoints.enabled", "true");
        checkpointConnectorConfig.put("emit.checkpoints.interval.seconds", "1");
        checkpointConnectorConfig.put("checkpoints.topic.replication.factor", "1");

        Map<String, Object> heartbeatConnectorConfig = new HashMap<>();
        heartbeatConnectorConfig.put("heartbeats.topic.replication.factor", "1");

        resourceManager.createResourceWithWait(
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), 1, false)
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig(sourceConnectorConfig)
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .addToConfig(checkpointConnectorConfig)
                        .endCheckpointConnector()
                        .editOrNewHeartbeatConnector()
                            .addToConfig(heartbeatConnectorConfig)
                        .endHeartbeatConnector()
                        .withTopicsPattern(".*")
                        .withGroupsPattern(".*")
                    .endMirror()
                .endSpec()
                .build(),
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), testStorage.getTargetClusterName(), testStorage.getSourceClusterName(), 1, false)
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig(sourceConnectorConfig)
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .addToConfig(checkpointConnectorConfig)
                        .endCheckpointConnector()
                        .editOrNewHeartbeatConnector()
                            .addToConfig(heartbeatConnectorConfig)
                        .endHeartbeatConnector()
                        .withTopicsPattern(".*")
                        .withGroupsPattern(".*")
                    .endMirror()
                .endSpec()
                .build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), 3).build()
        );

        KafkaClients initialInternalClientSourceJob = new KafkaClientsBuilder()
                .withProducerName(sourceProducerName)
                .withConsumerName(sourceConsumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()))
                .withTopicName(testStorage.getTopicName())
                .withMessageCount(testStorage.getMessageCount())
                .withMessage("Producer A")
                .withConsumerGroup(consumerGroup)
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        KafkaClients initialInternalClientTargetJob = new KafkaClientsBuilder()
                .withProducerName(targetProducerName)
                .withConsumerName(targetConsumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
                .withTopicName(testStorage.getMirroredSourceTopicName())
                .withMessageCount(testStorage.getMessageCount())
                .withConsumerGroup(consumerGroup)
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        LOGGER.info("Send & receive {} messages to/from Source cluster", testStorage.getMessageCount());
        resourceManager.createResourceWithWait(
            initialInternalClientSourceJob.producerStrimzi(),
            initialInternalClientSourceJob.consumerStrimzi());

        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), sourceConsumerName, sourceProducerName, testStorage.getMessageCount());

        LOGGER.info("Send {} messages to Source cluster", testStorage.getMessageCount());
        KafkaClients internalClientSourceJob = new KafkaClientsBuilder(initialInternalClientSourceJob).withMessage("Producer B").build();

        resourceManager.createResourceWithWait(
            internalClientSourceJob.producerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), sourceProducerName, testStorage.getMessageCount());

        LOGGER.info("Receive {} messages from mirrored Topic on target cluster", testStorage.getMessageCount());
        resourceManager.createResourceWithWait(
            initialInternalClientTargetJob.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), targetConsumerName, testStorage.getMessageCount());

        LOGGER.info("Send 50 messages to Source cluster");
        internalClientSourceJob = new KafkaClientsBuilder(internalClientSourceJob).withMessageCount(50).withMessage("Producer C").build();
        resourceManager.createResourceWithWait(
            internalClientSourceJob.producerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), sourceProducerName, 50);

        LOGGER.info("Receive 10 messages from source cluster");
        internalClientSourceJob = new KafkaClientsBuilder(internalClientSourceJob).withMessageCount(10).withAdditionalConfig("max.poll.records=10").build();
        resourceManager.createResourceWithWait(
            internalClientSourceJob.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), sourceConsumerName, 10);

        LOGGER.info("Receive 40 messages from mirrored Topic on target cluster");
        KafkaClients internalClientTargetJob = new KafkaClientsBuilder(initialInternalClientTargetJob).withMessageCount(40).build();
        resourceManager.createResourceWithWait(
            internalClientTargetJob.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), targetConsumerName, 40);

        LOGGER.info("There should be no more messages to read. Try to consume at least 1 message. " +
                "This client Job should fail on timeout.");
        resourceManager.createResourceWithWait(
            initialInternalClientTargetJob.consumerStrimzi());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(testStorage.getNamespaceName(), targetConsumerName, 1));

        LOGGER.info("As it's Active-Active MirrorMaker2 mode, there should be no more messages to read from Source cluster" +
                " topic. This client Job should fail on timeout.");
        resourceManager.createResourceWithWait(
            initialInternalClientSourceJob.consumerStrimzi());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(testStorage.getNamespaceName(), sourceConsumerName, 1));
    }

    @ParallelNamespaceTest
    void testKafkaMirrorMaker2ConnectorsStateAndOffsetManagement() throws JsonProcessingException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String listOffsetsConfigMap = testStorage.getClusterName() + "-offsets-list";
        final ObjectMapper mapper = new ObjectMapper();
        final String sourceConnectorName = String.format("%s->%s.MirrorSourceConnector", testStorage.getSourceClusterName(), testStorage.getTargetClusterName());

        final String errorMessage = "One or more connectors are in FAILED state";

        final KafkaClients sourceKafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()));
        final KafkaClients targetKafkaCLients = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .build();

        LOGGER.info("Deploy Kafka clusters and KafkaMirrorMaker2: {}/{} with wrong bootstrap service name configuration", testStorage.getNamespaceName(), testStorage.getClusterName());
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 3).build(),

            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );
        KafkaMirrorMaker2 kmm2 = KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, 1, false)
            .editSpec()
            .editFirstMirror()
                .editOrNewSourceConnector()
                    .withNewListOffsets()
                        .withNewToConfigMap(listOffsetsConfigMap)
                    .endListOffsets()
                .endSourceConnector()
            .endMirror()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getSourceClusterName()))
                    // typo in bootstrap name, connectors should not connect and MM2 should be in NotReady state with error
                    .withBootstrapServers(KafkaResources.bootstrapServiceName(testStorage.getSourceClusterName()) + ".:9092")
                .endCluster()
            .endSpec()
            .build();

        resourceManager.createResourceWithoutWait(kmm2);

        final String scraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        LOGGER.info("Deploying network policies for KafkaMirrorMaker2");
        NetworkPolicyResource.deployNetworkPolicyForResource(kmm2, KafkaMirrorMaker2Resources.componentName(testStorage.getClusterName()));

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2StatusMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), errorMessage);

        LOGGER.info("Modify originally wrong bootstrap service name configuration in KafkaMirrorMaker2: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), mm2 ->
            mm2.getSpec().getClusters().stream().filter(mm2ClusterSpec -> mm2ClusterSpec.getAlias().equals(testStorage.getSourceClusterName()))
                .findFirst().get().setBootstrapServers(KafkaUtils.namespacedPlainBootstrapAddress(testStorage.getNamespaceName(), testStorage.getSourceClusterName())));

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(testStorage.getNamespaceName(), testStorage.getClusterName());

        KafkaMirrorMaker2Status kmm2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertFalse(kmm2Status.getConditions().stream().anyMatch(condition -> condition.getMessage() != null && condition.getMessage().contains(errorMessage)));

        LOGGER.info("Pausing KafkaMirrorMaker2: {}/{} source connector", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            mm2 -> mm2.getSpec().getMirrors().get(0).getSourceConnector().setState(ConnectorState.PAUSED)
        );

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), 3).build());

        LOGGER.info("Success to produce and consume messages on source Kafka Cluster: {}/{} while connector is stopped", testStorage.getNamespaceName(), testStorage.getSourceClusterName());
        resourceManager.createResourceWithWait(sourceKafkaClients.producerStrimzi(), sourceKafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
        LOGGER.info("Fail to consume messages on target Kafka Cluster: {}/{} while connector is stopped", testStorage.getNamespaceName(), testStorage.getSourceClusterName());
        resourceManager.createResourceWithWait(targetKafkaCLients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientTimeout(testStorage);

        LOGGER.info("Re-running KafkaMirrorMaker2: {}/{} source connector", testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            mm2 -> mm2.getSpec().getMirrors().get(0).getSourceConnector().setState(ConnectorState.RUNNING)
        );

        LOGGER.info("Consumer in target cluster and Topic should receive {} messages", testStorage.getMessageCount());
        resourceManager.createResourceWithWait(targetKafkaCLients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        KafkaConnectorUtils.waitForOffsetInConnector(
            testStorage.getNamespaceName(),
            scraperPodName,
            KafkaMirrorMaker2Resources.serviceName(testStorage.getClusterName()),
            sourceConnectorName,
            "/offsets/0/offset/offset",
            99
        );

        LOGGER.info("Checking Source Connector's offset using the offset management");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            mm2 -> mm2.getMetadata().getAnnotations().putAll(Map.of(
                    Annotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, "list",
                    Annotations.ANNO_STRIMZI_IO_MIRRORMAKER_CONNECTOR, sourceConnectorName
                ))
        );

        // wait for creation of the CM
        ConfigMapUtils.waitForCreationOfConfigMap(testStorage.getNamespaceName(), listOffsetsConfigMap);

        // checking the config map
        ConfigMap listConfigMap = kubeClient().getConfigMap(testStorage.getNamespaceName(), listOffsetsConfigMap);
        JsonNode offsets = mapper.readTree(listConfigMap.getData().get(sourceConnectorName.replace("->", "--") + ".json"));

        assertThat("Offset config map contains correct offset value", offsets.get("offsets").get(0).get("offset").get("offset").asInt(), is(99));
    }

    /**
     * Test mirroring messages by MirrorMaker 2 over tls transport using scram-sha-512 auth
     * while user Scram passwords, CA cluster and clients certificates are changed.
     */
    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testKMM2RollAfterSecretsCertsUpdateScramSha() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String customSecretSource = "custom-secret-source";
        final String customSecretTarget = "custom-secret-target";

        // Deploy source and target kafkas with tls listener and SCRAM-SHA authentication
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1)
                .editSpec()
                    .editKafka()
                        .withListeners(
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                                .build()
                        )
                    .endKafka()
                .endSpec()
                .build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1)
                .editSpec()
                    .editKafka()
                        .withListeners(
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                                .build()
                        )
                    .endKafka()
                .endSpec()
                .build()
        );

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getSourceUsername(), testStorage.getSourceClusterName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getTargetUsername(), testStorage.getTargetClusterName()).build()
        );

        // Initialize PasswordSecretSource to set this as PasswordSecret in Source/Target MirrorMaker2 spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(testStorage.getSourceUsername());
        passwordSecretSource.setPassword("password");

        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(testStorage.getTargetUsername());
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getSourceClusterName()));

        // Initialize CertSecretSource with certificate and secret names for target (using pattern)
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setPattern("*.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getTargetClusterName()));

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, 1, true)
            .editSpec()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getSourceClusterName()))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(testStorage.getSourceUsername())
                        .withPasswordSecret(passwordSecretSource)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endCluster()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getTargetClusterName()))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(testStorage.getTargetUsername())
                        .withPasswordSecret(passwordSecretTarget)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endCluster()
            .endSpec()
            .build());

        LOGGER.info("Sending and receiving messages using Topic: {}", testStorage.getSourceClusterName());
        final KafkaClients sourceClients = ClientUtils.getInstantScramShaClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getSourceClusterName()))
            .withUsername(testStorage.getSourceUsername())
            .build();
        resourceManager.createResourceWithWait(sourceClients.producerScramShaTlsStrimzi(testStorage.getSourceClusterName()), sourceClients.consumerScramShaTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Consuming mirrored messages using Topic: {}", testStorage.getTargetClusterName());
        final KafkaClients targetClients = ClientUtils.getInstantScramShaClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .withUsername(testStorage.getTargetUsername())
            .build();
        resourceManager.createResourceWithWait(targetClients.consumerScramShaTlsStrimzi(testStorage.getTargetClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LOGGER.info("Messages successfully mirrored");

        Map<String, String> mmSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector());

        LOGGER.info("Changing KafkaUser sha-password on MirrorMaker2 Source and make sure it rolled");

        KafkaUserUtils.modifyKafkaUserPasswordWithNewSecret(testStorage.getNamespaceName(), testStorage.getSourceUsername(), customSecretSource, "UjhlTjJhSHhQN1lzVDZmQ2pNMWRRb1d6VnBYNWJHa1U=");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), 1, mmSnapshot);
        mmSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector());

        KafkaUserUtils.modifyKafkaUserPasswordWithNewSecret(testStorage.getNamespaceName(), testStorage.getTargetUsername(), customSecretTarget, "VDZmQ2pNMWRRb1d6VnBYNWJHa1VSOGVOMmFIeFA3WXM=");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), 1, mmSnapshot);

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), 1, mmSnapshot);

        //producing to source and consuming from target cluster after rolling update.
        resourceManager.createResourceWithWait(sourceClients.producerScramShaTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        resourceManager.createResourceWithWait(targetClients.consumerScramShaTlsStrimzi(testStorage.getTargetClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testKMM2RollAfterSecretsCertsUpdateTLS() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );

        // Deploy source kafka with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1)
            .editSpec()
                .editKafka()
                    .addToConfig("min.insync.replicas", 1)
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withAuth(new KafkaListenerAuthenticationTls())
                        .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );

        // Deploy target kafka with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1)
            .editSpec()
                .editKafka()
                    .addToConfig("min.insync.replicas", 1)
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withAuth(new KafkaListenerAuthenticationTls())
                        .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getSourceClusterName(), 3).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getSourceUsername(), testStorage.getSourceClusterName()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getTargetUsername(), testStorage.getTargetClusterName()).build()
        );

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getSourceClusterName()));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getTargetClusterName()));

        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage, 1, true)
            .editSpec()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getSourceClusterName()))
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(testStorage.getSourceUsername())
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endCluster()
                .editMatchingCluster(spec -> spec.getAlias().equals(testStorage.getTargetClusterName()))
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(testStorage.getTargetUsername())
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endCluster()
                .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig("refresh.topics.interval.seconds", 1)
                        .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        Map<String, String> mmSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector());

        final KafkaClients sourceClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getSourceClusterName()))
            .withUsername(testStorage.getSourceUsername())
            .build();

        final KafkaClients targetClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(testStorage.getMirroredSourceTopicName())
            .withUsername(testStorage.getTargetUsername())
            .build();

        LOGGER.info("Producing messages in source cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getSourceClusterName());
        resourceManager.createResourceWithWait(sourceClients.producerTlsStrimzi(testStorage.getSourceClusterName()), sourceClients.consumerTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Consuming messages in target cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getTargetClusterName());
        resourceManager.createResourceWithWait(targetClients.consumerTlsStrimzi(testStorage.getTargetClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LabelSelector controlSourceSelector = KafkaResource.getLabelSelector(testStorage.getSourceClusterName(), StrimziPodSetResource.getControllerComponentName(testStorage.getSourceClusterName()));
        LabelSelector brokerSourceSelector = KafkaResource.getLabelSelector(testStorage.getSourceClusterName(), StrimziPodSetResource.getBrokerComponentName(testStorage.getSourceClusterName()));

        LabelSelector controlTargetSelector = KafkaResource.getLabelSelector(testStorage.getTargetClusterName(), StrimziPodSetResource.getControllerComponentName(testStorage.getTargetClusterName()));
        LabelSelector brokerTargetSelector = KafkaResource.getLabelSelector(testStorage.getTargetClusterName(), StrimziPodSetResource.getBrokerComponentName(testStorage.getTargetClusterName()));

        Map<String, String> brokerSourcePods = PodUtils.podSnapshot(testStorage.getNamespaceName(), brokerSourceSelector);
        Map<String, String> controlSourcePods = PodUtils.podSnapshot(testStorage.getNamespaceName(), controlSourceSelector);
        Map<String, String> eoSourcePods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getSourceClusterName()));
        Map<String, String> brokerTargetPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), brokerTargetSelector);
        Map<String, String> controlTargetPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), controlTargetSelector);
        Map<String, String> eoTargetPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getTargetClusterName()));

        LOGGER.info("Renew Clients CA secret for Source cluster via annotation");
        String sourceClientsCaSecretName = KafkaResources.clientsCaCertificateSecretName(testStorage.getSourceClusterName());
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), sourceClientsCaSecretName, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true");
        brokerSourcePods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSourceSelector, 1, brokerSourcePods);
        mmSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), 1, mmSnapshot);

        LOGGER.info("Renew Clients CA secret for target cluster via annotation");
        String targetClientsCaSecretName = KafkaResources.clientsCaCertificateSecretName(testStorage.getTargetClusterName());
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), targetClientsCaSecretName, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true");
        brokerTargetPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controlTargetSelector, 1, brokerTargetPods);
        mmSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), 1, mmSnapshot);

        LOGGER.info("Producing messages in source cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getSourceClusterName());
        resourceManager.createResourceWithWait(sourceClients.producerTlsStrimzi(testStorage.getSourceClusterName()), sourceClients.consumerTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Consuming messages in target cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getTargetClusterName());
        resourceManager.createResourceWithWait(targetClients.consumerTlsStrimzi(testStorage.getTargetClusterName()));
        // Extend the timeout for clients to be sure that all messages are synced by MM2
        JobUtils.waitForJobSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), TestConstants.GLOBAL_TIMEOUT_LONG);
        JobUtils.deleteJobsWithWait(testStorage.getNamespaceName(), testStorage.getConsumerName());

        LOGGER.info("Renew Cluster CA secret for Source clusters via annotation");
        String sourceClusterCaSecretName = KafkaResources.clusterCaCertificateSecretName(testStorage.getSourceClusterName());
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), sourceClusterCaSecretName, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true");

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controlSourceSelector, 1, controlSourcePods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSourceSelector, 1, brokerSourcePods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getSourceClusterName()), 1, eoSourcePods);
        mmSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), 1, mmSnapshot);

        LOGGER.info("Renew Cluster CA secret for target clusters via annotation");
        String targetClusterCaSecretName = KafkaResources.clusterCaCertificateSecretName(testStorage.getTargetClusterName());
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), targetClusterCaSecretName, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true");

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controlTargetSelector, 1, controlTargetPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerTargetSelector, 1, brokerTargetPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getTargetClusterName()), 1, eoTargetPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getMM2Selector(), 1, mmSnapshot);

        LOGGER.info("Producing messages in source cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getSourceClusterName());
        resourceManager.createResourceWithWait(sourceClients.producerTlsStrimzi(testStorage.getSourceClusterName()), sourceClients.consumerTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Consuming messages in target cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getTargetClusterName());
        resourceManager.createResourceWithWait(targetClients.consumerTlsStrimzi(testStorage.getTargetClusterName()));
        // Extend the timeout for clients to be sure that all messages are synced by MM2
        JobUtils.waitForJobSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), TestConstants.GLOBAL_TIMEOUT_LONG);
        JobUtils.deleteJobsWithWait(testStorage.getNamespaceName(), testStorage.getConsumerName());
    }

    @BeforeAll
    void setup() {

        clusterOperator = clusterOperator.defaultInstallation()
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
