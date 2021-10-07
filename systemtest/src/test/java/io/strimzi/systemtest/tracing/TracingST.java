/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaTracingExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.KAFKA_TRACING_CLIENT_KEY;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NAMESPACE_KEY;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRACING;
import static io.strimzi.systemtest.bridge.HttpBridgeAbstractST.bridgePort;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@Tag(TRACING)
@Tag(INTERNAL_CLIENTS_USED)
public class TracingST extends AbstractST {

    private static final String NAMESPACE = "tracing-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(TracingST.class);

    private static final String JAEGER_PRODUCER_SERVICE = "hello-world-producer";
    private static final String JAEGER_CONSUMER_SERVICE = "hello-world-consumer";
    private static final String JAEGER_KAFKA_STREAMS_SERVICE = "hello-world-streams";
    private static final String JAEGER_MIRROR_MAKER_SERVICE = "my-mirror-maker";
    private static final String JAEGER_MIRROR_MAKER2_SERVICE = "my-mirror-maker2";
    private static final String JAEGER_KAFKA_CONNECT_SERVICE = "my-connect";
    private static final String JAEGER_KAFKA_BRIDGE_SERVICE = "my-kafka-bridge";

    protected static final String PRODUCER_JOB_NAME = "hello-world-producer";
    protected static final String CONSUMER_JOB_NAME = "hello-world-consumer";

    private static final String JAEGER_INSTANCE_NAME = "my-jaeger";
    private static final String JAEGER_SAMPLER_TYPE = "const";
    private static final String JAEGER_SAMPLER_PARAM = "1";
    private static final String JAEGER_OPERATOR_DEPLOYMENT_NAME = "jaeger-operator";
    private static final String JAEGER_AGENT_NAME = JAEGER_INSTANCE_NAME + "-agent";
    private static final String JAEGER_QUERY_SERVICE = JAEGER_INSTANCE_NAME + "-query";

    private static final String JAEGER_VERSION = "1.22.1";

    private Stack<String> jaegerConfigs = new Stack<>();

    private final String jaegerInstancePath = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/" + TracingUtils.getValidTracingVersion() + "/jaeger-instance.yaml";
    private final String jaegerOperatorFilesPath = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/" + TracingUtils.getValidTracingVersion() + "/operator-files/";

    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    void testProducerConsumerStreamsService(ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String clusterName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
        final String topicName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString();
        final String streamsTopicName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString();
        final String clientsPodName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString();
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString();

        final KafkaTracingExampleClients tracingClients = (KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, streamsTopicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, tracingClients.producerWithTracing().build());

        TracingUtils.verify(namespaceName, JAEGER_PRODUCER_SERVICE, clientsPodName, JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, tracingClients.consumerWithTracing().build());

        TracingUtils.verify(namespaceName, JAEGER_CONSUMER_SERVICE, clientsPodName, JAEGER_QUERY_SERVICE);
//        TODO: Disabled because of issue with Streams API and tracing. Uncomment this after fix.
//        resourceManager.createResource(extensionContext, tracingClients.kafkaStreamsWithTracing().build());
//
//        TracingUtils.verify(namespaceName, JAEGER_KAFKA_STREAMS_SERVICE, clientsPodName, JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service(ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
        final String kafkaClusterTargetName = kafkaClusterSourceName + "-target";

        final String topicName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString();
        final String sourceTopicName = kafkaClusterSourceName + "." + topicName;
        final String clientsPodName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString();
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString();

        final KafkaTracingExampleClients tracingClients = (KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, sourceTopicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = tracingClients.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient = tracingClients.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(kafkaClusterSourceName + "." +topicName)
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(kafkaClusterSourceName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .withNewJaegerTracing()
                .endJaegerTracing()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .addNewEnv()
                            .withName("JAEGER_SERVICE_NAME")
                            .withValue(JAEGER_MIRROR_MAKER2_SERVICE)
                        .endEnv()
                        .addNewEnv()
                            .withName("JAEGER_AGENT_HOST")
                            .withValue(JAEGER_AGENT_NAME)
                        .endEnv()
                        .addNewEnv()
                            .withName("JAEGER_SAMPLER_TYPE")
                            .withValue(JAEGER_SAMPLER_TYPE)
                        .endEnv()
                        .addNewEnv()
                            .withName("JAEGER_SAMPLER_PARAM")
                            .withValue(JAEGER_SAMPLER_PARAM)
                        .endEnv()
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .build());

        TracingUtils.verify(namespaceName, JAEGER_PRODUCER_SERVICE, clientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_CONSUMER_SERVICE, clientsPodName, "From_" + sourceTopicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_MIRROR_MAKER2_SERVICE, clientsPodName,"From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_MIRROR_MAKER2_SERVICE, clientsPodName, "To_" + sourceTopicName, JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMakerService(ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
        final String kafkaClusterTargetName = kafkaClusterSourceName + "-target";

        final String topicName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString();
        final String clientsPodName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString();
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString();

        final KafkaTracingExampleClients tracingClients = (KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, topicName + "-target")
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
                .withTopicName(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .endSpec()
            .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = tracingClients.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient =  tracingClients.toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(kafkaClusterSourceName, kafkaClusterSourceName, kafkaClusterTargetName,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_MIRROR_MAKER_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .build());

        TracingUtils.verify(namespaceName, JAEGER_PRODUCER_SERVICE, clientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_CONSUMER_SERVICE, clientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_MIRROR_MAKER_SERVICE, clientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_MIRROR_MAKER_SERVICE, clientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerStreamsConnectService(ExtensionContext extensionContext) {
        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
        assumeFalse(Environment.isNamespaceRbacScope());

        final String clusterName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
        final String topicName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString();
        final String streamsTopicName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString();
        final String producerName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PRODUCER_KEY).toString();
        final String consumerName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CONSUMER_KEY).toString();
        final String clientsPodName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString();
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString();

        final KafkaTracingExampleClients tracingClients = (KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, streamsTopicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .withNewSpec()
                .withConfig(configOfKafkaConnect)
                .withNewJaegerTracing()
                .endJaegerTracing()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(clusterName))
                .withReplicas(1)
                .withNewTemplate()
                    .withNewConnectContainer()
                        .addNewEnv()
                            .withName("JAEGER_SERVICE_NAME")
                            .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                        .endEnv()
                        .addNewEnv()
                            .withName("JAEGER_AGENT_HOST")
                            .withValue(JAEGER_AGENT_NAME)
                        .endEnv()
                        .addNewEnv()
                            .withName("JAEGER_SAMPLER_TYPE")
                            .withValue(JAEGER_SAMPLER_TYPE)
                        .endEnv()
                        .addNewEnv()
                            .withName("JAEGER_SAMPLER_PARAM")
                            .withValue(JAEGER_SAMPLER_PARAM)
                        .endEnv()
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, tracingClients.producerWithTracing().build());
        resourceManager.createResource(extensionContext, tracingClients.consumerWithTracing().build());
        ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, namespaceName, MESSAGE_COUNT);

//        TODO: Disabled because of issue with Streams API and tracing. Uncomment this after fix.
//        resourceManager.createResource(extensionContext, tracingClients.kafkaStreamsWithTracing().build());
//
//        TracingUtils.verify(namespaceName, JAEGER_KAFKA_STREAMS_SERVICE, clientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
//        TracingUtils.verify(namespaceName, JAEGER_KAFKA_STREAMS_SERVICE, clientsPodName, "To_" + streamsTopicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_KAFKA_CONNECT_SERVICE, clientsPodName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_PRODUCER_SERVICE, clientsPodName, "To_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_CONSUMER_SERVICE, clientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
        TracingUtils.verify(namespaceName, JAEGER_KAFKA_CONNECT_SERVICE, clientsPodName, "From_" + topicName, JAEGER_QUERY_SERVICE);
    }

    @Tag(BRIDGE)
    @ParallelNamespaceTest
    void testKafkaBridgeService(ExtensionContext extensionContext) {

        final String clusterName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
        final String topicName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString();
        final String clientsPodName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString();
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString();

        final KafkaTracingExampleClients tracingClients = (KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(clusterName, KafkaResources.plainBootstrapAddress(clusterName), 1)
            .editSpec()
                .withNewJaegerTracing()
                .endJaegerTracing()
                    .withNewTemplate()
                        .withNewBridgeContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_BRIDGE_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endBridgeContainer()
                    .endTemplate()
            .endSpec()
            .build());

        String bridgeProducer = "bridge-producer";
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(bridgeProducer)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(clusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());
        resourceManager.createResource(extensionContext, tracingClients.consumerWithTracing().build());
        ClientUtils.waitForClientSuccess(bridgeProducer, namespaceName, MESSAGE_COUNT);

        TracingUtils.verify(namespaceName, JAEGER_KAFKA_BRIDGE_SERVICE, clientsPodName, JAEGER_QUERY_SERVICE);
    }

    /**
     * Delete Jaeger instance
     */
    void deleteJaeger() {
        while (!jaegerConfigs.empty()) {
            cmdKubeClient().namespace(cluster.getNamespace()).deleteContent(jaegerConfigs.pop());
        }
    }

    private void deployJaegerContent() throws FileNotFoundException {
        File folder = new File(jaegerOperatorFilesPath);
        File[] files = folder.listFiles();

        if (files != null && files.length > 0) {
            for (File file : files) {
                String yamlContent = TestUtils.setMetadataNamespace(file, NAMESPACE)
                    .replace("namespace: \"observability\"", "namespace: \"" + NAMESPACE + "\"");
                jaegerConfigs.push(yamlContent);
                LOGGER.info("Creating {} from {}", file.getName(), file.getAbsolutePath());
                cmdKubeClient(NAMESPACE).applyContent(yamlContent);
            }
        } else {
            throw new FileNotFoundException("Folder with Jaeger files is empty or doesn't exist");
        }
    }

    private void deployJaegerOperator(ExtensionContext extensionContext) throws IOException, FileNotFoundException {
        LOGGER.info("=== Applying jaeger operator install files ===");

        deployJaegerContent();

        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(() -> this.deleteJaeger()));
        DeploymentUtils.waitForDeploymentAndPodsReady(NAMESPACE, JAEGER_OPERATOR_DEPLOYMENT_NAME, 1);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(Constants.NETWORK_POLICY)
            .withNewMetadata()
                .withName("jaeger-allow")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .addNewIngress()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("app", "jaeger")
                .endPodSelector()
                .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());
        resourceManager.createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for jaeger successfully applied");
    }

    /**
     * Install of Jaeger instance
     */
    void deployJaegerInstance(ExtensionContext extensionContext, String namespaceName) {
        LOGGER.info("=== Applying jaeger instance install file ===");

        String instanceYamlContent = TestUtils.getContent(new File(jaegerInstancePath), TestUtils::toYamlString);
        cmdKubeClient(namespaceName).applyContent(instanceYamlContent.replaceAll("image: 'jaegertracing/all-in-one:*'", "image: 'jaegertracing/all-in-one:" + JAEGER_VERSION.substring(0, 4) + "'"));
        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(() -> cmdKubeClient(namespaceName).deleteContent(instanceYamlContent)));
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, JAEGER_INSTANCE_NAME, 1);
    }

    @BeforeEach
    void createTestResources(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String streamsTopicTargetName = KafkaTopicUtils.generateRandomNameOfTopic();
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String producerName = clusterName + PRODUCER_JOB_NAME;
        final String consumerName = clusterName + CONSUMER_JOB_NAME;

        deployJaegerInstance(extensionContext, namespaceName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, false, kafkaClientsName).build());

        final String kafkaClientsPodName = kubeClient(namespaceName).listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        final KafkaTracingExampleClients kafkaTracingClient = new KafkaTracingExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicName)
            .withStreamsTopicTargetName(streamsTopicTargetName)
            .withMessageCount(MESSAGE_COUNT)
            .withJaegerServiceProducerName(JAEGER_PRODUCER_SERVICE)
            .withJaegerServiceConsumerName(JAEGER_CONSUMER_SERVICE)
            .withJaegerServiceStreamsName(JAEGER_KAFKA_STREAMS_SERVICE)
            .withJaegerServiceAgentName(JAEGER_AGENT_NAME)
            .build();

        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.NAMESPACE_KEY, namespaceName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CLUSTER_KEY, clusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TOPIC_KEY, topicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.STREAM_TOPIC_KEY, streamsTopicTargetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_CLIENTS_KEY, kafkaClientsName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_CLIENTS_POD_KEY, kafkaClientsPodName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PRODUCER_KEY, producerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CONSUMER_KEY, consumerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_TRACING_CLIENT_KEY, kafkaTracingClient);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) throws IOException {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .createInstallation()
            .runInstallation();
        // deployment of the jaeger
        deployJaegerOperator(extensionContext);
    }
}
