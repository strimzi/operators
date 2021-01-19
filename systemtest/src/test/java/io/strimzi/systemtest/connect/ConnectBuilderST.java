/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connect.build.TgzArtifactBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(CONNECT_COMPONENTS)
@Tag(CONNECT)
class ConnectBuilderST extends AbstractST {

    public static final String NAMESPACE = "connect-builder-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(ConnectBuilderST.class);

    private static final String ECHO_SINK_CLASS_NAME = "cz.scholz.kafka.connect.echosink.EchoSinkConnector";
    private static final String CAMEL_TELEGRAM_CLASS_NAME = "org.apache.camel.kafkaconnector.telegram.CamelTelegramSinkConnector";

    private String imageName = "";

    private static final Plugin PLUGIN_WITH_TAR_AND_JAR = new PluginBuilder()
        .withName("connector-with-tar-and-jar")
        .withArtifacts(
            new JarArtifactBuilder()
                .withNewUrl("https://github.com/scholzj/echo-sink/releases/download/1.1.0/echo-sink-1.1.0.jar")
                .withNewSha512sum("b7da48d5ecd1e4199886d169ced1bf702ffbdfd704d69e0da97e78ff63c1bcece2f59c2c6c751f9c20be73472b8cb6a31b6fd4f75558c1cb9d96daa9e9e603d2")
                .build(),
            new TgzArtifactBuilder()
                .withNewUrl("https://github.com/scholzj/echo-sink/archive/1.1.0.tar.gz")
                .withNewSha512sum("5318b1f031d4e5eeab6f8b774c76de297237574fc51d1e81b03a10e0b5d5435a46a108b85fdb604c644529f38830ae83239c17b6ec91c90a60ac790119bb2950")
                .build())
        .build();

    @Test
    void testBuildFailsWithWrongChecksumOfArtifact() {
        Plugin pluginWithWrongChecksum = new PluginBuilder()
            .withName("connector-with-empty-checksum")
            .withArtifacts(new JarArtifactBuilder()
                .withNewUrl("https://github.com/scholzj/echo-sink/releases/download/1.1.0/echo-sink-1.1.0.jar")
                .withNewSha512sum("ads")
                .build())
            .build();

        KafkaConnectResource.kafkaConnectWithoutWait(KafkaConnectResource.kafkaConnect(clusterName, 1)
            .editOrNewSpec()
                .withNewBuild()
                    .withPlugins(pluginWithWrongChecksum)
                    .withNewDockerOutput()
                        .withNewImage(imageName)
                    .endDockerOutput()
                .endBuild()
            .endSpec()
            .build());

        KafkaConnectUtils.waitForConnectNotReady(clusterName);
        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(clusterName, NAMESPACE, "The Kafka Connect build .* failed");

        LOGGER.info("Checking if KafkaConnect status condition contains message about build failure");
        KafkaConnect connect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get();
        Condition connectCondition = connect.getStatus().getConditions().stream().findFirst().get();

        assertTrue(connectCondition.getMessage().matches("The Kafka Connect build .* failed"));
        assertThat(connectCondition.getType(), is(NotReady.toString()));

        LOGGER.info("Replacing plugin's checksum with right one");
        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kafkaConnect -> {
            Plugin pluginWithRightChecksum = new PluginBuilder()
                .withName("connector-with-empty-checksum")
                .withArtifacts(new JarArtifactBuilder()
                    .withNewUrl("https://github.com/scholzj/echo-sink/releases/download/1.1.0/echo-sink-1.1.0.jar")
                    .withNewSha512sum("b7da48d5ecd1e4199886d169ced1bf702ffbdfd704d69e0da97e78ff63c1bcece2f59c2c6c751f9c20be73472b8cb6a31b6fd4f75558c1cb9d96daa9e9e603d2")
                    .build())
                .build();

            kafkaConnect.getSpec().getBuild().getPlugins().remove(0);
            kafkaConnect.getSpec().getBuild().getPlugins().add(pluginWithRightChecksum);
        });

        KafkaConnectUtils.waitForConnectReady(clusterName);
    }

    @Test
    void testBuildWithJarAndTgz() {
        // this test also testing push into Docker output
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaTopicResource.create(KafkaTopicResource.topic(clusterName, topicName).build());

        KafkaConnectResource.create(KafkaConnectResource.kafkaConnect(clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR)
                    .withNewDockerOutput()
                        .withNewImage(imageName)
                    .endDockerOutput()
                .endBuild()
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "INFO")
                .endInlineLogging()
            .endSpec()
            .build(), false);

        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("level", "INFO");

        KafkaClientsResource.create(KafkaClientsResource.deployKafkaClients(false, kafkaClientsName).build());
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        KafkaConnectorResource.create(KafkaConnectorResource.kafkaConnector(clusterName)
            .editOrNewSpec()
                .withClassName(ECHO_SINK_CLASS_NAME)
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        KafkaConnector kafkaConnector = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(clusterName).get();

        assertThat(kafkaConnector.getSpec().getClassName(), is(ECHO_SINK_CLASS_NAME));

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.sendMessagesPlain();

        String connectPodName = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(connectPodName, "Received message with key 'null' and value '99'");
    }

    @OpenShiftOnly
    @Test
    void testPushIntoImageStream() {
        String imageStreamName = "custom-image-stream";
        ImageStream imageStream = new ImageStreamBuilder()
            .editOrNewMetadata()
                .withName(imageStreamName)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();

        kubeClient().getClient().adapt(OpenShiftClient.class).imageStreams().create(imageStream);

        KafkaConnectResource.create(KafkaConnectResource.kafkaConnect(clusterName, 1)
            .editOrNewSpec()
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR)
                    .withNewImageStreamOutput()
                        .withNewImage(imageStreamName + ":latest")
                    .endImageStreamOutput()
                .endBuild()
            .endSpec()
            .build(), false);

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get();

        LOGGER.info("Checking, if KafkaConnect has all artifacts and if is successfully created");
        assertThat(kafkaConnect.getSpec().getBuild().getPlugins().get(0).getArtifacts().size(), is(2));
        assertThat(kafkaConnect.getSpec().getBuild().getOutput().getType(), is("imagestream"));
        assertThat(kafkaConnect.getSpec().getBuild().getOutput().getImage(), is(imageStreamName + ":latest"));
        assertThat(kafkaConnect.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
    }

    @Test
    void testUpdateConnectWithAnotherPlugin() {
        String echoConnector = "echo-sink-connector";
        String camelConnector = "camel-telegram-connector";

        Plugin secondPlugin =  new PluginBuilder()
            .withName("camel-telegram")
            .withArtifacts(
                new TgzArtifactBuilder()
                    .withNewUrl("https://repo.maven.apache.org/maven2/org/apache/camel/kafkaconnector/camel-telegram-kafka-connector/0.7.0/camel-telegram-kafka-connector-0.7.0-package.tar.gz")
                    .withNewSha512sum("a9b1ac63e3284bea7836d7d24d84208c49cdf5600070e6bd1535de654f6920b74ad950d51733e8020bf4187870699819f54ef5859c7846ee4081507f48873479")
                    .build())
            .build();

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaTopicResource.create(KafkaTopicResource.topic(clusterName, topicName).build());

        KafkaClientsResource.create(KafkaClientsResource.deployKafkaClients(false, kafkaClientsName).build());
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        KafkaConnectResource.create(KafkaConnectResource.kafkaConnect(clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR)
                    .withNewDockerOutput()
                        .withNewImage(imageName)
                    .endDockerOutput()
                .endBuild()
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "INFO")
                .endInlineLogging()
            .endSpec()
            .build(), true);

        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("level", "INFO");

        LOGGER.info("Creating EchoSink connector");
        KafkaConnectorResource.create(KafkaConnectorResource.kafkaConnector(echoConnector, clusterName)
            .editOrNewSpec()
                .withClassName(ECHO_SINK_CLASS_NAME)
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        String deploymentName = KafkaConnectResources.deploymentName(clusterName);
        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(deploymentName);

        LOGGER.info("Checking that KafkaConnect API contains EchoSink connector and not Camel-Telegram Connector class name");
        String plugins = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + KafkaConnectResources.serviceName(clusterName) + ":8083/connector-plugins").out();

        assertFalse(plugins.contains(CAMEL_TELEGRAM_CLASS_NAME));
        assertTrue(plugins.contains(ECHO_SINK_CLASS_NAME));

        LOGGER.info("Adding one more connector to the KafkaConnect");
        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kafkaConnect -> {
            kafkaConnect.getSpec().getBuild().getPlugins().add(secondPlugin);
        });

        DeploymentUtils.waitTillDepHasRolled(deploymentName, 1, connectSnapshot);

        LOGGER.info("Creating Camel-Telegram connector");
        KafkaConnectorResource.create(KafkaConnectorResource.kafkaConnector(camelConnector, clusterName)
            .editOrNewSpec()
                .withClassName(CAMEL_TELEGRAM_CLASS_NAME)
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get();

        LOGGER.info("Checking if both Connectors were created and Connect contains both plugins");
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).list().getItems().size(), is(2));
        assertThat(kafkaConnect.getSpec().getBuild().getPlugins().size(), is(2));

        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(ECHO_SINK_CLASS_NAME)));
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(CAMEL_TELEGRAM_CLASS_NAME)));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT);

        KafkaResource.create(KafkaResource.kafkaEphemeral(clusterName, 3).build());

        String outputRegistry = "";

        if (cluster.isNotKubernetes()) {
            outputRegistry = "image-registry.openshift-image-registry.svc:5000/";
        }
        else {
            LOGGER.warn("For running these tests on K8s you have to have internal registry deployed on localhost:5000");
            outputRegistry = "localhost:5000/";
        }
        imageName = outputRegistry + NAMESPACE + "/connect-build:latest";
    }
}
