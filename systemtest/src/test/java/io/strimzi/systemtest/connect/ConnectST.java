/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.common.PasswordSecretSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connector.AutoRestartBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.KindIPv6NotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.COMPONENT_SCALING;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.SANITY;
import static io.strimzi.systemtest.TestConstants.SMOKE;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
class ConnectST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectST.class);

    @ParallelNamespaceTest
    void testDeployRollUndeploy(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        final int connectReplicasCount = 2;

        final Map<String, Object> exceptedConfig = StUtils.loadProperties("group.id=" + KafkaConnectResources.componentName(testStorage.getClusterName()) + "\n" +
                "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "config.storage.replication.factor=-1\n" +
                "offset.storage.replication.factor=-1\n" +
                "status.storage.replication.factor=-1\n" +
                "config.storage.topic=" + KafkaConnectResources.metricsAndLogConfigMapName(testStorage.getClusterName()) + "\n" +
                "status.storage.topic=" + KafkaConnectResources.configStorageTopicStatus(testStorage.getClusterName()) + "\n" +
                "offset.storage.topic=" + KafkaConnectResources.configStorageTopicOffsets(testStorage.getClusterName()) + "\n");

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), connectReplicasCount).build());

        // Test ManualRolling Update
        LOGGER.info("KafkaConnect manual rolling update");
        final Map<String, String> connectPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), connectReplicasCount, connectPodsSnapshot);

        final String podName = PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));
        final String kafkaPodJson = TestUtils.toJsonString(kubeClient(testStorage.getNamespaceName()).getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(exceptedConfig));
        testDockerImagesForKafkaConnect(clusterOperator.getDeploymentNamespace(), testStorage.getNamespaceName(), testStorage.getClusterName());

        verifyLabelsOnPods(testStorage.getNamespaceName(), testStorage.getClusterName(), "connect", "KafkaConnect");
        verifyLabelsForService(testStorage.getNamespaceName(), testStorage.getClusterName(), "connect", "connect-api", "KafkaConnect");
        verifyLabelsForConfigMaps(testStorage.getNamespaceName(), testStorage.getClusterName(), null, "");
        verifyLabelsForServiceAccounts(testStorage.getNamespaceName(), testStorage.getClusterName(), null);
    }

    private void testDockerImagesForKafkaConnect(String clusterOperatorNamespace, String connectNamespaceName, String clusterName) {
        LOGGER.info("Verifying docker image names");
        Map<String, String> imgFromDeplConf = getImagesFromConfig(clusterOperatorNamespace);
        //Verifying docker image for kafka connect
        String connectImageName = PodUtils.getFirstContainerImageNameFromPod(connectNamespaceName, kubeClient(connectNamespaceName).listPodsByPrefixInName(KafkaConnectResources.componentName(clusterName)).
                get(0).getMetadata().getName());

        String connectVersion = Crds.kafkaConnectOperation(kubeClient(connectNamespaceName).getClient()).inNamespace(connectNamespaceName).withName(clusterName).get().getSpec().getVersion();
        if (connectVersion == null) {
            connectVersion = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_CONNECT_IMAGE_MAP)).get(connectVersion), is(connectImageName));
        LOGGER.info("Docker images verified");
    }

    /**
     * @description This test case verifies pausing, stopping and running of connector via 'spec.pause' or 'spec.state' specification.
     *
     * @steps
     *  1. - Deploy prerequisites for running FileSink KafkaConnector, that is KafkaTopic, Kafka cluster, KafkaConnect, and FileSink KafkaConnector.
     *     - All resources are deployed and ready.
     *  2. - Pause and run connector by modifying 'spec.pause' property of Connector, while also producing messages when connector pauses.
     *     - Connector is paused and resumed as expected, after connector is resumed, produced messages are present in destination file, indicating connector resumed correctly.
     *  3. - Stop and run connector by modifying 'spec.state' property of Connector (with priority over now set 'spec.pause=false'), while also producing messages when connector stops.
     *     - Connector stops and resumes as expected, after resuming, produced messages are present in destination file, indicating connector resumed correctly.
     *  4. - Pause and run connector by modifying 'spec.state' property of Connector (with priority over now set 'spec.pause=false'), while also producing messages when connector pauses.
     *     - Connector pauses and resumes as expected, after resuming, produced messages are present in destination file, indicating connector resumed correctly.
     *
     * @usecase
     *  - kafka-connect
     *  - kafka-connector
     *  - connector-state
     */
    @ParallelNamespaceTest
    @Tag(SANITY)
    @Tag(SMOKE)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectAndConnectorStateWithFileSinkPlugin(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        final String connectPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(testStorage.getNamespaceName(), connectPodName);

        LOGGER.info("Creating KafkaConnector: {}/{} without 'spec.pause' or 'spec.state' specified", testStorage.getNamespaceName(), testStorage.getClusterName());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", testStorage.getTopicName())
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        // TODO in future releases can be removed completely as 'spec.pause' property is becoming deprecated.
        LOGGER.info("Verify pausing and running KafkaConnector, by setting 'spec.pause' property to 'true' and 'false'");
        verifySinkConnectorByBlockAndUnblock(testStorage, connectPodName,
            connector -> connector.getSpec().setPause(true),
            connector -> connector.getSpec().setPause(false));

        LOGGER.info("Verify stopping and running KafkaConnector, by setting 'spec.state' property to '' and false");
        verifySinkConnectorByBlockAndUnblock(testStorage, connectPodName,
            connector -> connector.getSpec().setState(ConnectorState.STOPPED),
            connector -> connector.getSpec().setState(ConnectorState.RUNNING));

        LOGGER.info("Verify pausing and running KafkaConnector, by setting 'spec.state' property to 'paused' and 'running'");
        verifySinkConnectorByBlockAndUnblock(testStorage, connectPodName,
            connector -> connector.getSpec().setState(ConnectorState.PAUSED),
            connector -> connector.getSpec().setState(ConnectorState.RUNNING));
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectWithPlainAndScramShaAuthentication(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        // Use a Kafka with plain listener disabled
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser kafkaUser =  KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getUsername()).build();

        resourceManager.createResourceWithWait(extensionContext, kafkaUser);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage).build());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(testStorage.getUsername())
                    .withPasswordSecret(new PasswordSecretSourceBuilder()
                        .withSecretName(testStorage.getUsername())
                        .withPassword("password")
                        .build())
                .endKafkaClientAuthenticationScramSha512()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withReplicas(1)
            .endSpec()
            .build();
        // This is required to be able to remove the TLS setting, the builder cannot remove it
        connect.getSpec().setTls(null);
        
        resourceManager.createResourceWithWait(extensionContext, connect, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        final String kafkaConnectPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        final String kafkaConnectLogs = kubeClient(testStorage.getNamespaceName()).logs(kafkaConnectPodName);
        final String scraperPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getScraperName()).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(testStorage.getNamespaceName(), kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect Pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink KafkaConnector via Pod: {}/{} with Topic: {}", testStorage.getNamespaceName(), scraperPodName, testStorage.getTopicName());
        KafkaConnectorUtils.createFileSinkConnector(testStorage.getNamespaceName(), scraperPodName, testStorage.getTopicName(),
            TestConstants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(testStorage.getClusterName(), testStorage.getNamespaceName(), 8083));

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerScramShaPlainStrimzi(),
            kafkaClients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @ParallelNamespaceTest
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectAndConnectorFileSinkPlugin(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, connect, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        String connectorName = "license-source";

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(connectorName, testStorage.getClusterName(), 2)
            .editSpec()
                .addToConfig("topic", testStorage.getTopicName())
            .endSpec()
            .build());

        final String scraperPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getScraperName()).get(0).getMetadata().getName();

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        String service = KafkaConnectResources.url(testStorage.getClusterName(), testStorage.getNamespaceName(), 8083);
        String output = cmdKubeClient(testStorage.getNamespaceName()).execInPod(scraperPodName, "/bin/bash", "-c", "curl " + service + "/connectors/" + connectorName).out();
        assertThat(output, containsString("\"name\":\"license-source\""));
        assertThat(output, containsString("\"connector.class\":\"org.apache.kafka.connect.file.FileStreamSourceConnector\""));
        assertThat(output, containsString("\"tasks.max\":\"2\""));
        assertThat(output, containsString("\"topic\":\"" + testStorage.getTopicName() + "\""));
    }


    @ParallelNamespaceTest
    void testJvmAndResources(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .withResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("400M"))
                    .addToLimits("cpu", new Quantity("2"))
                    .addToRequests("memory", new Quantity("300M"))
                    .addToRequests("cpu", new Quantity("1"))
                    .build())
                .withNewJvmOptions()
                    .withXmx("200m")
                    .withXms("200m")
                    .withXx(jvmOptionsXX)
                .endJvmOptions()
            .endSpec()
            .build());

        String podName = PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));
        assertResources(testStorage.getNamespaceName(), podName, KafkaConnectResources.componentName(testStorage.getClusterName()),
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(testStorage.getNamespaceName(), podName, KafkaConnectResources.componentName(testStorage.getClusterName()),
                "-Xmx200m", "-Xms200m", "-XX:+UseG1GC");
    }

    @ParallelNamespaceTest
    @Tag(COMPONENT_SCALING)
    void testKafkaConnectScaleUpScaleDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1).build());

        // kafka cluster Connect already deployed
        List<Pod> connectPods = kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName()));
        int initialReplicas = connectPods.size();
        assertThat(initialReplicas, is(1));
        final int scaleTo = initialReplicas + 3;

        LOGGER.info("Scaling up to {}", scaleTo);
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), c -> c.getSpec().setReplicas(scaleTo), testStorage.getNamespaceName());

        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), scaleTo, true);
        connectPods = kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName()));
        assertThat(connectPods.size(), is(scaleTo));

        LOGGER.info("Scaling down to {}", initialReplicas);
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), c -> c.getSpec().setReplicas(initialReplicas), testStorage.getNamespaceName());

        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), initialReplicas, true);
        connectPods = kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName()));
        assertThat(connectPods.size(), is(initialReplicas));
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectWithTlsAndTlsClientAuthentication(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        KafkaUser kafkaUser = KafkaUserTemplates.tlsUser(testStorage).build();

        resourceManager.createResourceWithWait(extensionContext, kafkaUser);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage).build());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(testStorage.getClusterName() + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(testStorage.getClusterName() + "-kafka-bootstrap:9093")
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(testStorage.getUsername())
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, connect, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        final String kafkaConnectPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        final String kafkaConnectLogs = kubeClient(testStorage.getNamespaceName()).logs(kafkaConnectPodName);
        final String scraperPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getScraperName()).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(testStorage.getNamespaceName(), kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect Pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink KafkaConnector via Pod: {}/{} with Topic: {}", testStorage.getNamespaceName(), scraperPodName, testStorage.getTopicName());
        KafkaConnectorUtils.createFileSinkConnector(testStorage.getNamespaceName(), scraperPodName, testStorage.getTopicName(),
            TestConstants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(testStorage.getClusterName(), testStorage.getNamespaceName(), 8083));

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectWithTlsAndScramShaAuthentication(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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
            .build());

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(testStorage).build();

        resourceManager.createResourceWithWait(extensionContext, kafkaUser);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage).build());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(testStorage.getClusterName() + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(testStorage.getClusterName() + "-kafka-bootstrap:9093")
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(testStorage.getUsername())
                    .withNewPasswordSecret()
                        .withSecretName(testStorage.getUsername())
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, connect, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        final String kafkaConnectPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        final String kafkaConnectLogs = kubeClient(testStorage.getNamespaceName()).logs(kafkaConnectPodName);
        final String scraperPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Verifying that KafkaConnect Pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink KafkaConnector via Pod: {}/{} with Topic: {}", testStorage.getNamespaceName(), scraperPodName, testStorage.getTopicName());
        KafkaConnectorUtils.createFileSinkConnector(testStorage.getNamespaceName(), scraperPodName, testStorage.getTopicName(),
            TestConstants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(testStorage.getClusterName(), testStorage.getNamespaceName(), 8083));

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @ParallelNamespaceTest
    @MicroShiftNotSupported("The test is using Connect Build feature that is not available on MicroShift")
    void testConnectorTaskAutoRestart(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());

        final String imageFullPath = Environment.getImageOutputRegistry(testStorage.getNamespaceName(), TestConstants.ST_CONNECT_BUILD_IMAGE_NAME, String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage).build());

        final Plugin echoSinkPlugin = new PluginBuilder()
            .withName(TestConstants.ECHO_SINK_CONNECTOR_NAME)
            .withArtifacts(
                new JarArtifactBuilder()
                    .withUrl(TestConstants.ECHO_SINK_JAR_URL)
                    .withSha512sum(TestConstants.ECHO_SINK_JAR_CHECKSUM)
                    .build())
            .build();

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewBuild()
                    .withPlugins(echoSinkPlugin)
                    .withOutput(KafkaConnectTemplates.dockerOutput(imageFullPath))
                .endBuild()
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, connect, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());
        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        // How many messages should be sent and at what count should the test connector fail
        final int failMessageCount = 5;

        Map<String, Object> echoSinkConfig = new HashMap<>();
        echoSinkConfig.put("topics", testStorage.getTopicName());
        echoSinkConfig.put("level", "INFO");
        echoSinkConfig.put("fail.task.after.records", failMessageCount);

        LOGGER.info("Creating EchoSink KafkaConnector");
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(TestConstants.ECHO_SINK_CONNECTOR_NAME, testStorage.getClusterName())
            .editOrNewSpec()
                .withTasksMax(1)
                .withClassName(TestConstants.ECHO_SINK_CLASS_NAME)
                .withConfig(echoSinkConfig)
                .withAutoRestart(new AutoRestartBuilder().withEnabled().build())
            .endSpec()
            .build());

        // Send messages to topic
        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(failMessageCount)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerStrimzi());
        ClientUtils.waitForProducerClientSuccess(testStorage);

        // After connector picks up messages from topic it fails task
        // If it's the first time echo-sink task failed - it's immediately restarted and connector adds count to autoRestartCount.
        KafkaConnectorUtils.waitForConnectorAutoRestartCount(testStorage.getNamespaceName(), TestConstants.ECHO_SINK_CONNECTOR_NAME, 1);
        // Give some time to connector task to get back to running state after recovery
        KafkaConnectorUtils.waitForConnectorTaskState(testStorage.getNamespaceName(), TestConstants.ECHO_SINK_CONNECTOR_NAME, 0, "RUNNING");
        // If task is in running state for about 2 minutes, it resets the auto-restart count back to 0
        KafkaConnectorUtils.waitForConnectorAutoRestartCount(testStorage.getNamespaceName(), TestConstants.ECHO_SINK_CONNECTOR_NAME, 0);
    }

    @ParallelNamespaceTest
    void testCustomAndUpdatedValues(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String usedVariable = "KAFKA_CONNECT_CONFIGURATION";

        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");
        envVarGeneral.put(usedVariable, "test.value");

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, Object> connectConfig = new HashMap<>();
        connectConfig.put("config.storage.replication.factor", "-1");
        connectConfig.put("offset.storage.replication.factor", "-1");
        connectConfig.put("status.storage.replication.factor", "-1");

        final int initialDelaySeconds = 30;
        final int timeoutSeconds = 10;
        final int updatedInitialDelaySeconds = 31;
        final int updatedTimeoutSeconds = 11;
        final int periodSeconds = 10;
        final int successThreshold = 1;
        final int failureThreshold = 3;
        final int updatedPeriodSeconds = 5;
        final int updatedFailureThreshold = 1;

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endConnectContainer()
                .endTemplate()
                .withNewReadinessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endLivenessProbe()
            .endSpec()
            .build());

        Map<String, String> connectSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verifying values before update");
        checkReadinessLivenessProbe(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()), KafkaConnectResources.componentName(testStorage.getClusterName()), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()), KafkaConnectResources.componentName(testStorage.getClusterName()), envVarGeneral);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(
                StUtils.checkEnvVarInPod(testStorage.getNamespaceName(), kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName(), usedVariable),
                is(not("test.value"))
        );

        LOGGER.info("Updating values in MirrorMaker container");

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kc -> {
            kc.getSpec().getTemplate().getConnectContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            kc.getSpec().setConfig(connectConfig);
            kc.getSpec().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kc.getSpec().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kc.getSpec().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kc.getSpec().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kc.getSpec().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kc.getSpec().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kc.getSpec().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            kc.getSpec().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1, connectSnapshot);

        LOGGER.info("Verifying values after update");
        checkReadinessLivenessProbe(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()), KafkaConnectResources.componentName(testStorage.getClusterName()), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()), KafkaConnectResources.componentName(testStorage.getClusterName()), envVarUpdated);
        checkComponentConfiguration(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()), KafkaConnectResources.componentName(testStorage.getClusterName()), "KAFKA_CONNECT_CONFIGURATION", connectConfig);
    }

    @ParallelNamespaceTest
    @KindIPv6NotSupported("error checking push permissions -- make sure you entered the correct tag name, and that you are " +
        "authenticated correctly, and try again: checking push permission for \"/namespace-0/strimzi-sts-connect-build:2020576698\": " +
        "creating push check transport for index.docker.io failed: Get \"https://index.docker.io/v2/\": dial tcp: lookup index.docker.io on " +
        "[fd00:10:96::a]:53: server misbehaving; Get \"http://index.docker.io/v2/\": dial tcp: lookup index.docker.io on [fd00:10:96::a]:53: " +
        "server misbehaving")
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ACCEPTANCE)
    void testMultiNodeKafkaConnectWithConnectorCreation(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        final String connectClusterName = testStorage.getClusterName() + "-2";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        // Crate connect cluster with default connect image
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 3)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("group.id", connectClusterName)
                .addToConfig("offset.storage.topic", connectClusterName + "-offsets")
                .addToConfig("config.storage.topic", connectClusterName + "-config")
                .addToConfig("status.storage.topic", connectClusterName + "-status")
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", testStorage.getTopicName())
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        String execConnectPod =  kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(
            KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        JsonObject connectStatus = new JsonObject(cmdKubeClient(testStorage.getNamespaceName()).execInPod(
                execConnectPod,
                "curl", "-X", "GET", "http://localhost:8083/connectors/" + testStorage.getClusterName() + "/status").out()
        );

        String workerNode = connectStatus.getJsonObject("connector").getString("worker_id").split(":")[0];
        String connectorPodName = workerNode.substring(0, workerNode.indexOf("."));

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, TestConstants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(CONNECTOR_OPERATOR)
    @ParallelNamespaceTest
    void testConnectTlsAuthWithWeirdUserName(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        // Create weird named user with . and maximum of 64 chars -> TLS
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withAuth(new KafkaListenerAuthenticationTls())
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withAuth(new KafkaListenerAuthenticationTls())
                                .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), weirdUserName).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder()
                        .withCertificate("ca.crt")
                        .withSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()))
                        .build())
                .endTls()
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(weirdUserName)
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .endSpec()
            .build());

        testConnectAuthorizationWithWeirdUserName(extensionContext, testStorage.getClusterName(), weirdUserName, SecurityProtocol.SSL, testStorage.getTopicName());
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(CONNECTOR_OPERATOR)
    @ParallelNamespaceTest
    void testConnectScramShaAuthWithWeirdUserName(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasdsadasdasdasdasdgasgadfasdad";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withAuth(new KafkaListenerAuthenticationScramSha512())
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withAuth(new KafkaListenerAuthenticationScramSha512())
                                .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), weirdUserName).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editOrNewSpec()
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(weirdUserName)
                        .withPasswordSecret(new PasswordSecretSourceBuilder()
                            .withSecretName(weirdUserName)
                            .withPassword("password")
                            .build())
                    .endKafkaClientAuthenticationScramSha512()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewTls()
                        .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName()))
                                .build())
                    .endTls()
                .endSpec()
                .build());

        testConnectAuthorizationWithWeirdUserName(extensionContext, testStorage.getClusterName(), weirdUserName, SecurityProtocol.SASL_SSL, testStorage.getTopicName());
    }

    void testConnectAuthorizationWithWeirdUserName(ExtensionContext extensionContext, String clusterName, String userName, SecurityProtocol securityProtocol, String topicName) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String connectorPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getNamespaceName(), clusterName + "-connect").get(0).getMetadata().getName();

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
            .endSpec()
            .build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(securityProtocol)
            .withTopicName(topicName)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(externalKafkaClient.sendMessagesTls(), is(testStorage.getMessageCount()));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, TestConstants.DEFAULT_SINK_FILE_PATH, "\"Hello-world - 99\"");
    }

    @ParallelNamespaceTest
    @Tag(COMPONENT_SCALING)
    void testScaleConnectWithoutConnectorToZero(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 2).build());

        List<Pod> connectPods = kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName()));

        assertThat(connectPods.size(), is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kafkaConnect -> kafkaConnect.getSpec().setReplicas(0), testStorage.getNamespaceName());

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 0, true);

        connectPods = kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName()));
        KafkaConnectStatus connectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();

        assertThat(connectPods.size(), is(0));
        assertThat(connectStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @ParallelNamespaceTest
    @Tag(CONNECTOR_OPERATOR)
    @Tag(COMPONENT_SCALING)
    void testScaleConnectWithConnectorToZero(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 2)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", testStorage.getTopicName())
            .endSpec()
            .build());

        List<Pod> connectPods = kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName()));

        assertThat(connectPods.size(), is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kafkaConnect -> kafkaConnect.getSpec().setReplicas(0), testStorage.getNamespaceName());

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 0, true);
        KafkaConnectorUtils.waitForConnectorNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        connectPods = kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName()));
        KafkaConnectStatus connectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        KafkaConnectorStatus connectorStatus = KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();

        assertThat(connectPods.size(), is(0));
        assertThat(connectStatus.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getMessage().contains("has 0 replicas")), is(true));
    }

    @ParallelNamespaceTest
    @Tag(CONNECTOR_OPERATOR)
    @Tag(COMPONENT_SCALING)
    void testScaleConnectAndConnectorSubresource(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", testStorage.getTopicName())
            .endSpec()
            .build());

        final int scaleTo = 4;
        final long connectObsGen = KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();
        final String connectGenName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaConnect subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient(testStorage.getNamespaceName()).scaleByName(KafkaConnect.RESOURCE_KIND, testStorage.getClusterName(), scaleTo);

        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), scaleTo, true);

        LOGGER.info("Check if replicas is set to {}, observed generation is higher - for spec and status - naming prefix should be same", scaleTo);

        StUtils.waitUntilSupplierIsSatisfied(() -> kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName())).size() == scaleTo &&
                KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getReplicas() == scaleTo &&
                KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getReplicas() == scaleTo);

        List<Pod> connectPods = kubeClient(testStorage.getNamespaceName()).listPods(Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.componentName(testStorage.getClusterName()));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(connectObsGen < KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration(), is(true));

        LOGGER.info("-------> Scaling KafkaConnector subresource <-------");
        LOGGER.info("Scaling subresource task max to {}", scaleTo);
        cmdKubeClient(testStorage.getNamespaceName()).scaleByName(KafkaConnector.RESOURCE_KIND, testStorage.getClusterName(), scaleTo);
        KafkaConnectorUtils.waitForConnectorsTaskMaxChange(testStorage.getNamespaceName(), testStorage.getClusterName(), scaleTo);

        LOGGER.info("Check if taskMax is set to {}", scaleTo);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getTasksMax(), is(scaleTo));
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getTasksMax(), is(scaleTo));

        LOGGER.info("Check taskMax on Connect Pods API");
        for (Pod pod : connectPods) {
            LOGGER.info("Checking taskMax over API for Pod: {}/{}", pod.getMetadata().getNamespace(), pod.getMetadata().getName());
            TestUtils.waitFor(String.format("pod's %s API return tasksMax=%s", pod.getMetadata().getName(), scaleTo),
                TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT_SHORT, () -> {
                    JsonObject json = new JsonObject(KafkaConnectorUtils.getConnectorSpecFromConnectAPI(testStorage.getNamespaceName(), pod.getMetadata().getName(), testStorage.getClusterName()));
                    return Integer.parseInt(json.getJsonObject("config").getString("tasks.max")) == scaleTo;
                });
        }
    }

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMountingSecretAndConfigMapAsVolumesAndEnvVars(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        final String secretPassword = "password";
        final String encodedPassword = Base64.getEncoder().encodeToString(secretPassword.getBytes());

        final String secretEnv = "MY_CONNECT_SECRET";
        final String configMapEnv = "MY_CONNECT_CONFIG_MAP";

        final String dotedSecretEnv = "MY_DOTED_CONNECT_SECRET";
        final String dotedConfigMapEnv = "MY_DOTED_CONNECT_CONFIG_MAP";

        final String configMapName = "connect-config-map";
        final String secretName = "connect-secret";

        final String dotedConfigMapName = "connect.config.map";
        final String dotedSecretName = "connect.secret";

        final String configMapKey = "my-key";
        final String secretKey = "my-secret-key";

        final String configMapValue = "my-value";

        Secret connectSecret = new SecretBuilder()
            .withNewMetadata()
                .withName(secretName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withType("Opaque")
            .addToData(secretKey, encodedPassword)
            .build();

        ConfigMap configMap = new ConfigMapBuilder()
            .editOrNewMetadata()
                .withName(configMapName)
            .endMetadata()
            .addToData(configMapKey, configMapValue)
            .build();

        Secret dotedConnectSecret = new SecretBuilder()
            .withNewMetadata()
                .withName(dotedSecretName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withType("Opaque")
            .addToData(secretKey, encodedPassword)
            .build();

        ConfigMap dotedConfigMap = new ConfigMapBuilder()
            .editOrNewMetadata()
                .withName(dotedConfigMapName)
            .endMetadata()
            .addToData(configMapKey, configMapValue)
            .build();

        kubeClient(testStorage.getNamespaceName()).createSecret(connectSecret);
        kubeClient(testStorage.getNamespaceName()).createSecret(dotedConnectSecret);

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMap);
        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), dotedConfigMap);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .withNewExternalConfiguration()
                    .addNewVolume()
                        .withName(secretName)
                        .withSecret(new SecretVolumeSourceBuilder().withSecretName(secretName).build())
                    .endVolume()
                    .addNewVolume()
                        .withName(configMapName)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder().withName(configMapName).build())
                    .endVolume()
                    .addNewVolume()
                        .withName(dotedSecretName)
                        .withSecret(new SecretVolumeSourceBuilder().withSecretName(dotedSecretName).build())
                    .endVolume()
                    .addNewVolume()
                        .withName(dotedConfigMapName)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder().withName(dotedConfigMapName).build())
                    .endVolume()
                    .addNewEnv()
                        .withName(secretEnv)
                        .withNewValueFrom()
                            .withSecretKeyRef(
                                new SecretKeySelectorBuilder()
                                    .withKey(secretKey)
                                    .withName(connectSecret.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withName(configMapEnv)
                        .withNewValueFrom()
                            .withConfigMapKeyRef(
                                new ConfigMapKeySelectorBuilder()
                                    .withKey(configMapKey)
                                    .withName(configMap.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withName(dotedSecretEnv)
                        .withNewValueFrom()
                            .withSecretKeyRef(
                                new SecretKeySelectorBuilder()
                                    .withKey(secretKey)
                                    .withName(dotedConnectSecret.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withName(dotedConfigMapEnv)
                        .withNewValueFrom()
                            .withConfigMapKeyRef(
                                new ConfigMapKeySelectorBuilder()
                                    .withKey(configMapKey)
                                    .withName(dotedConfigMap.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                .endExternalConfiguration()
            .endSpec()
            .build());

        String connectPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();

        LOGGER.info("Check if the ENVs contains desired values");
        assertThat(cmdKubeClient(testStorage.getNamespaceName()).execInPod(connectPodName, "/bin/bash", "-c", "printenv " + secretEnv).out().trim(), equalTo(secretPassword));
        assertThat(cmdKubeClient(testStorage.getNamespaceName()).execInPod(connectPodName, "/bin/bash", "-c", "printenv " + configMapEnv).out().trim(), equalTo(configMapValue));
        assertThat(cmdKubeClient(testStorage.getNamespaceName()).execInPod(connectPodName, "/bin/bash", "-c", "printenv " + dotedSecretEnv).out().trim(), equalTo(secretPassword));
        assertThat(cmdKubeClient(testStorage.getNamespaceName()).execInPod(connectPodName, "/bin/bash", "-c", "printenv " + dotedConfigMapEnv).out().trim(), equalTo(configMapValue));

        LOGGER.info("Check if volumes contains desired values");
        assertThat(
            cmdKubeClient(testStorage.getNamespaceName()).execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + configMapName + "/" + configMapKey).out().trim(),
            equalTo(configMapValue)
        );
        assertThat(
            cmdKubeClient(testStorage.getNamespaceName()).execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + secretName + "/" + secretKey).out().trim(),
            equalTo(secretPassword)
        );
        assertThat(
            cmdKubeClient(testStorage.getNamespaceName()).execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + dotedConfigMapName + "/" + configMapKey).out().trim(),
            equalTo(configMapValue)
        );
        assertThat(
            cmdKubeClient(testStorage.getNamespaceName()).execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + dotedSecretName + "/" + secretKey).out().trim(),
            equalTo(secretPassword)
        );
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    // changing the password in Secret should cause the RU of connect pod
    void testKafkaConnectWithScramShaAuthenticationRolledAfterPasswordChanged(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .build())
                .endKafka()
                .endSpec()
                .build());

        Secret passwordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("custom-pwd-secret")
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .addToStringData("pwd", "completely_secret_password_long_enough_for_fips")
                .build();

        kubeClient(testStorage.getNamespaceName()).createSecret(passwordSecret);

        KafkaUser kafkaUser =  KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getKafkaUsername())
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("pwd", "custom-pwd-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaUser);

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
                .withNewSpec()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(testStorage.getKafkaUsername())
                        .withPasswordSecret(new PasswordSecretSourceBuilder()
                            .withSecretName(testStorage.getKafkaUsername())
                            .withPassword("password")
                            .build())
                    .endKafkaClientAuthenticationScramSha512()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withVersion(Environment.ST_KAFKA_VERSION)
                    .withReplicas(1)
                .endSpec()
                .build());

        final String kafkaConnectPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(testStorage.getNamespaceName(), kafkaConnectPodName);

        Map<String, String> connectSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());
        Secret newPasswordSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("new-custom-pwd-secret")
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .addToStringData("pwd", "completely_different_secret_password_long_enough_for_fips")
                .build();

        kubeClient(testStorage.getNamespaceName()).createSecret(newPasswordSecret);

        kafkaUser = KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getKafkaUsername())
                .editSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                        .withNewPassword()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("pwd", "new-custom-pwd-secret", false)
                            .endValueFrom()
                        .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();

        KafkaUserResource.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).resource(kafkaUser).update();

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1, connectSnapshot);

        final String kafkaConnectPodNameAfterRU = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(testStorage.getNamespaceName(), kafkaConnectPodNameAfterRU);
    }

    /**
     * This method tests the (FileSink) connector's ability to pause/stop and resume correctly.
     *
     * Firstly the connector is blocked (stopped or paused) using the provided 'blockEditor' consumer.
     * Then, the existing messages in the FileSink file are cleared, so in next step any new message present would indicate connector running again.
     * New messages are produced to the topic, but due to the connector being blocked, these messages should not appear in the FileSink file.
     * Finally, unblocking the connector using the 'unblockEditor' consumer, and the test checks if new messages appear in the FileSink file.
     *
     * @param testStorage         contains configuration.
     * @param kafkaConnectPodName The name of the Kafka Connect Pod.
     * @param connectorBlockEditor         A consumer function to apply the block specification on the Kafka Connector.
     * @param connectorUnblockEditor       A consumer function to apply the unblock specification on the Kafka Connector.
     *
     * @throws Exception if messages produced during FileSink KafkaConnector being stopped/paused do not appear in destination file in reasonable time.
     */
    void verifySinkConnectorByBlockAndUnblock(TestStorage testStorage, String kafkaConnectPodName, Consumer<KafkaConnector> connectorBlockEditor, Consumer<KafkaConnector> connectorUnblockEditor) {
        LOGGER.info("Blocking KafkaConnector: {}", testStorage.getClusterName());
        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(testStorage.getClusterName(), connectorBlockEditor, testStorage.getNamespaceName());

        LOGGER.info("Clearing FileSink file to check if KafkaConnector will be really paused/stopped");
        KafkaConnectUtils.clearFileSinkFile(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH);

        // messages need to be produced for each run (although there are more messages in topic eventually, sink will not copy messages it copied previously (before clearing them)
        LOGGER.info("Producing new messages which are to be watched KafkaConnector once it is resumed");
        final KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();
        resourceManager.createResourceWithWait(testStorage.getExtensionContext(), kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Because KafkaConnector is blocked, no messages should appear to FileSink file");
        assertThrows(Exception.class, () -> KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, "99"));

        LOGGER.info("Unblocking KafkaConnector, newly produced messages should again appear to FileSink file");
        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(testStorage.getClusterName(), connectorUnblockEditor, testStorage.getNamespaceName());
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @BeforeAll
    void setUp(final ExtensionContext extensionContext) {
        clusterOperator = clusterOperator
            .defaultInstallation(extensionContext)
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
