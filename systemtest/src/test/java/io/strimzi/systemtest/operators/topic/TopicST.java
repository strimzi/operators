/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.specific.ScraperUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourceNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourceState;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourcesHigherThanOrEqualTo;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(REGRESSION)
@KRaftNotSupported("Topic Operator is not supported by KRaft mode and is used in this test class")
public class TopicST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);
    private static final String KAFKA_CLUSTER_NAME = "topic-cluster-name";
    private static final String SCRAPER_NAME = KAFKA_CLUSTER_NAME + "-" + Constants.SCRAPER_NAME;

    private String scraperPodName;

    private static int topicOperatorReconciliationInterval;

    @ParallelTest
    void testMoreReplicasThanAvailableBrokers(ExtensionContext extensionContext) {
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        int topicReplicationFactor = 5;
        int topicPartitions = 5;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, topicName, topicPartitions, topicReplicationFactor, 1, clusterOperator.getDeploymentNamespace()).build();
        resourceManager.createResource(extensionContext, false, kafkaTopic);

        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic doesn't exists in Kafka itself", !hasTopicInKafka(topicName, KAFKA_CLUSTER_NAME));

        String errorMessage = "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 5 larger than available brokers: 3";

        KafkaTopicUtils.waitForKafkaTopicNotReady(clusterOperator.getDeploymentNamespace(), topicName);
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString("CompletionException"));

        LOGGER.info("Delete Topic: {}", topicName);
        cmdKubeClient(clusterOperator.getDeploymentNamespace()).deleteByName("kafkatopic", topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(clusterOperator.getDeploymentNamespace(), topicName);

        topicReplicationFactor = 3;
        final String newTopicName = "topic-example-new";

        kafkaTopic = KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, newTopicName, topicPartitions, topicReplicationFactor, clusterOperator.getDeploymentNamespace()).build();
        resourceManager.createResource(extensionContext, kafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, KAFKA_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, newTopicName));
    }

    @ParallelTest
    void testCreateTopicViaKafka(ExtensionContext extensionContext) {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        int topicPartitions = 3;

        LOGGER.debug("Creating Topic: {} with {} replicas and {} partitions", topicName, 3, topicPartitions);
        KafkaCmdClient.createTopicUsingPodCli(clusterOperator.getDeploymentNamespace(), scraperPodName, KafkaResources.plainBootstrapAddress(KAFKA_CLUSTER_NAME), topicName, 3, topicPartitions);

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(topicName).get();

        verifyTopicViaKafkaTopicCRK8s(kafkaTopic, topicName, topicPartitions, KAFKA_CLUSTER_NAME);

        topicPartitions = 5;
        LOGGER.info("Editing Topic via Kafka, settings to partitions {}", topicPartitions);

        KafkaCmdClient.updateTopicPartitionsCountUsingPodCli(clusterOperator.getDeploymentNamespace(), scraperPodName, KafkaResources.plainBootstrapAddress(KAFKA_CLUSTER_NAME), topicName, topicPartitions);
        LOGGER.debug("Topic: {} updated from {} to {} partitions", topicName, 3, topicPartitions);

        KafkaTopicUtils.waitForKafkaTopicPartitionChange(clusterOperator.getDeploymentNamespace(), topicName, topicPartitions);
        verifyTopicViaKafka(clusterOperator.getDeploymentNamespace(), topicName, topicPartitions, KAFKA_CLUSTER_NAME);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    void testCreateTopicViaAdminClient(ExtensionContext extensionContext) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                            .build())
                .endKafka()
            .endSpec()
            .build());

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaResource.kafkaClient().inNamespace(clusterOperator.getDeploymentNamespace())
            .withName(clusterName).get().getStatus().getListeners().stream()
            .filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(properties)) {

            Set<String> topics = adminClient.listTopics().names().get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
            int topicsSize = topics.size(); // new KafkaStreamsTopicStore has topology input topics

            LOGGER.info("Creating async Topic: {} via Admin client", topicName);
            CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(topicName, 1, (short) 1)));
            crt.all().get();

            TestUtils.waitFor("Kafka cluster has " + (topicsSize + 1) + " KafkaTopic", Constants.GLOBAL_POLL_INTERVAL,
                Constants.GLOBAL_TIMEOUT, () -> {
                    Set<String> updatedKafkaTopics = new HashSet<>();
                    try {
                        updatedKafkaTopics = adminClient.listTopics().names().get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
                        LOGGER.info("Verifying that in Kafka cluster contains {} Topics", topicsSize + 1);

                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        e.printStackTrace();
                    }
                    return updatedKafkaTopics.size() == topicsSize + 1 && updatedKafkaTopics.contains(topicName);

                });

            KafkaTopicUtils.waitForKafkaTopicCreation(clusterOperator.getDeploymentNamespace(), topicName);
            KafkaTopicUtils.waitForKafkaTopicReady(clusterOperator.getDeploymentNamespace(), topicName);
        }

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(topicName).get();

        LOGGER.info("Verifying that corresponding {} KafkaTopic custom resources were created and Topic is in Ready state", 1);
        assertThat(kafkaTopic.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(1));
        assertThat(kafkaTopic.getSpec().getReplicas(), is(1));
    }

    @Tag(NODEPORT_SUPPORTED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCreateDeleteCreate(ExtensionContext extensionContext) throws InterruptedException {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(false)
                                .build())
                    .endKafka()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalSeconds(120)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build());

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaResource.kafkaClient().inNamespace(clusterOperator.getDeploymentNamespace())
                .withName(clusterName).get().getStatus().getListeners().stream()
                .filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
                .findFirst()
                .orElseThrow(RuntimeException::new)
                .getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(properties)) {

            String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

            resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, clusterOperator.getDeploymentNamespace())
                .editSpec()
                    .withReplicas(3)
                .endSpec()
                .build());
            KafkaTopicUtils.waitForKafkaTopicReady(clusterOperator.getDeploymentNamespace(), topicName);

            adminClient.describeTopics(singletonList(topicName)).topicNameValues().get(topicName);

            for (int i = 0; i < 10; i++) {
                Thread.sleep(2_000);
                LOGGER.info("Iteration {}: Deleting {}", i, topicName);
                cmdKubeClient(clusterOperator.getDeploymentNamespace()).deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
                KafkaTopicUtils.waitForKafkaTopicDeletion(clusterOperator.getDeploymentNamespace(), topicName);
                TestUtils.waitFor("Deletion of Topic: " + topicName, 1000, 15_000, () -> {
                    try {
                        return !adminClient.listTopics().names().get().contains(topicName);
                    } catch (ExecutionException | InterruptedException e) {
                        return false;
                    }
                });
                Thread.sleep(2_000);
                long t0 = System.currentTimeMillis();
                LOGGER.info("Iteration {}: Recreating {}", i, topicName);
                resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, clusterOperator.getDeploymentNamespace())
                    .editSpec()
                        .withReplicas(3)
                    .endSpec()
                    .build());
                ResourceManager.waitForResourceStatus(KafkaTopicResource.kafkaTopicClient(), "KafkaTopic", clusterOperator.getDeploymentNamespace(), topicName, Ready, 15_000);
                TestUtils.waitFor("Recreation of Topic: " + topicName, 1000, 2_000, () -> {
                    try {
                        return adminClient.listTopics().names().get().contains(topicName);
                    } catch (ExecutionException | InterruptedException e) {
                        return false;
                    }
                });
                if (System.currentTimeMillis() - t0 > 10_000) {
                    fail("Took too long to recreate");
                }
            }
        }
    }

    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendingMessagesToNonExistingTopic(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(KAFKA_CLUSTER_NAME))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        LOGGER.info("Checking if {} is on Topic list", testStorage.getTopicName());
        assertFalse(hasTopicInKafka(testStorage.getTopicName(), KAFKA_CLUSTER_NAME));
        LOGGER.info("Topic with name {} is not created yet", testStorage.getTopicName());

        LOGGER.info("Trying to send messages to non-existing Topic: {}", testStorage.getTopicName());

        resourceManager.createResource(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Checking if {} is on Topic list", testStorage.getTopicName());
        assertTrue(hasTopicInKafka(testStorage.getTopicName(), KAFKA_CLUSTER_NAME));

        KafkaTopicUtils.waitForKafkaTopicCreation(clusterOperator.getDeploymentNamespace(), testStorage.getTopicName());
        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(testStorage.getTopicName()).get();
        assertThat(kafkaTopic, notNullValue());

        assertThat(kafkaTopic.getStatus(), notNullValue());
        assertThat(kafkaTopic.getStatus().getConditions(), notNullValue());
        assertThat(kafkaTopic.getStatus().getConditions().isEmpty(), is(false));
        assertThat(kafkaTopic.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
        LOGGER.info("Topic successfully created");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testDeleteTopicEnableFalse(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 1, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .addToConfig("delete.topic.enable", false)
                .endKafka()
            .endSpec()
            .build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, clients.producerStrimzi());
        ClientUtils.waitForProducerClientSuccess(testStorage);

        String topicUid = KafkaTopicUtils.topicSnapshot(clusterOperator.getDeploymentNamespace(), testStorage.getTopicName());
        LOGGER.info("Deleting KafkaTopic: {}/{}", clusterOperator.getDeploymentNamespace(), testStorage.getTopicName());
        KafkaTopicResource.kafkaTopicClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(testStorage.getTopicName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        LOGGER.info("KafkaTopic: {}/{} deleted", clusterOperator.getDeploymentNamespace(), testStorage.getTopicName());

        KafkaTopicUtils.waitTopicHasRolled(clusterOperator.getDeploymentNamespace(), testStorage.getTopicName(), topicUid);

        LOGGER.info("Waiting for KafkaTopic: {}/{} recreation", clusterOperator.getDeploymentNamespace(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicCreation(clusterOperator.getDeploymentNamespace(), testStorage.getTopicName());
        LOGGER.info("KafkaTopic: {}/{} recreated", clusterOperator.getDeploymentNamespace(), testStorage.getTopicName());

        resourceManager.createResource(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelTest
    void testCreateTopicAfterUnsupportedOperation(ExtensionContext extensionContext) {
        String topicName = "topic-with-replication-to-change";
        String newTopicName = "another-topic";

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, topicName, clusterOperator.getDeploymentNamespace())
            .editSpec()
                .withReplicas(3)
                .withPartitions(3)
            .endSpec()
            .build();

        resourceManager.createResource(extensionContext, kafkaTopic);
        KafkaTopicUtils.waitForKafkaTopicReady(clusterOperator.getDeploymentNamespace(), topicName);

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, t -> {
            t.getSpec().setReplicas(1);
            t.getSpec().setPartitions(1);
        }, clusterOperator.getDeploymentNamespace());
        KafkaTopicUtils.waitForKafkaTopicNotReady(clusterOperator.getDeploymentNamespace(), topicName);

        String exceptedMessage = "Number of partitions cannot be decreased";
        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(topicName).get().getStatus().getConditions().get(0).getMessage(), is(exceptedMessage));

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        KafkaTopic newKafkaTopic = KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, newTopicName, 1, 1, clusterOperator.getDeploymentNamespace()).build();

        resourceManager.createResource(extensionContext, newKafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(topicName, KAFKA_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, KAFKA_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(newKafkaTopic, newTopicName));

        cmdKubeClient(clusterOperator.getDeploymentNamespace()).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(clusterOperator.getDeploymentNamespace(), topicName);
        cmdKubeClient(clusterOperator.getDeploymentNamespace()).deleteByName(KafkaTopic.RESOURCE_SINGULAR, newTopicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(clusterOperator.getDeploymentNamespace(), newTopicName);
    }

    /**
     * @description This test case checks Topic Operator metrics regarding different states of KafkaTopic.
     *
     * @steps
     *  1. - Create KafkaTopic
     *     - KafkaTopic is ready
     *  2. - Create metrics collector for Topic Operator and collect the metrics
     *     - Metrics collected
     *  3. - Check that TOpic Operator metrics contains data about reconciliations
     *     - Metrics contains proper data
     *  4. - Check that metrics contain info about KafkaTopic with name stored in 'topicName' is Ready
     *     - Metrics contains proper data
     *  5. - Change spec.topicName for topic 'topicName' and wait for NotReady status
     *     - KafkaTopic is in NotReady state
     *  6. - Check that metrics contain info about KafkaTopic 'topicName' cannot be renamed and that KT status has proper values
     *     - Metrics contains proper data and status contains proper values
     *  7. - Revert changes in KafkaTopic and change number of Replicas
     *     - KafkaTopic CR replica count is changed
     *  8. - Check that metrics contain info about KafkaTopic 'topicName' replicas count cannot be changed and KT status has proper values
     *     - Metrics contains proper data and KT status has proper values
     *  9. - Decrease KT number of partitions
     *     - Partitions count changed
     *  10. - Check that metrics contains info about KafkaTopic NotReady status and KT status has proper values (cannot change partition count)
     *      - Metrics contains proper data and KT status has proper values
     *  11. - Set KafkaTopic configuration to default one
     *      - KafkaTopic is in Ready state
     *  12. - Check that metrics contain info about KafkaTopic 'topicName' is Ready
     *      - Metrics contains proper data
     *
     * @testcase
     *  - topic-operator-metrics
     *  - kafkatopic-ready
     *  - kafkatopic-not-ready
     */
    @IsolatedTest
    @KRaftNotSupported("Topic Operator is not supported by KRaft mode and is used in this test class")
    void testKafkaTopicDifferentStates(ExtensionContext extensionContext) throws InterruptedException {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        int initialReplicas = 1;
        int initialPartitions = 5;
        int decreasePartitions = 1;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, topicName, initialPartitions, initialReplicas, clusterOperator.getDeploymentNamespace()).build());
        KafkaTopicUtils.waitForKafkaTopicReady(clusterOperator.getDeploymentNamespace(), topicName);

        LOGGER.info("Found the following Topics:");
        cmdKubeClient().list(KafkaTopic.RESOURCE_KIND).forEach(item -> {
            LOGGER.info("{}: {}", KafkaTopic.RESOURCE_KIND, item);
        });

        MetricsCollector toMetricsCollector = new MetricsCollector.Builder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withScraperPodName(scraperPodName)
            .withComponentName(KAFKA_CLUSTER_NAME)
            .withComponentType(ComponentType.TopicOperator)
            .build();

        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_successful_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_count", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_sum", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_max", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_periodical_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourcesHigherThanOrEqualTo(toMetricsCollector, KafkaTopic.RESOURCE_KIND, 3);

        String reasonMessage = "none";
        String reason = "";

        LOGGER.info("Checking if resource state metric reason message is \"none\" and KafkaTopic is ready");
        assertMetricResourceState(toMetricsCollector, KafkaTopic.RESOURCE_KIND, topicName, clusterOperator.getDeploymentNamespace(), 1, reasonMessage);
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), Ready, 1);

        LOGGER.info("Changing Topic name in spec.topicName");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setTopicName("some-other-name"), clusterOperator.getDeploymentNamespace());
        KafkaTopicUtils.waitForKafkaTopicNotReady(clusterOperator.getDeploymentNamespace(), topicName);

        reason = "IllegalArgumentException";
        reasonMessage = "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.";
        assertMetricResourceState(toMetricsCollector, KafkaTopic.RESOURCE_KIND, topicName, clusterOperator.getDeploymentNamespace(), 0, reasonMessage);
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), NotReady, reason, reasonMessage, 2);

        LOGGER.info("Changing back to it's original name and scaling replicas to be higher number");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> {
            kafkaTopic.getSpec().setTopicName(topicName);
            kafkaTopic.getSpec().setReplicas(12);
        }, clusterOperator.getDeploymentNamespace());

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(clusterOperator.getDeploymentNamespace(), topicName, 12);

        reason = "ReplicationFactorChangeException";
        reasonMessage = "Changing 'spec.replicas' is not supported.";
        KafkaTopicUtils.waitForKafkaTopicNotReady(clusterOperator.getDeploymentNamespace(), topicName);
        assertMetricResourceState(toMetricsCollector, KafkaTopic.RESOURCE_KIND, topicName, clusterOperator.getDeploymentNamespace(), 0, reasonMessage);
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), NotReady, reason, reasonMessage, 3);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setReplicas(initialReplicas), clusterOperator.getDeploymentNamespace());
        KafkaTopicUtils.waitForKafkaTopicReady(clusterOperator.getDeploymentNamespace(), topicName);
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), Ready, 4);

        reasonMessage = "none";
        assertMetricResourceState(toMetricsCollector, KafkaTopic.RESOURCE_KIND, topicName, clusterOperator.getDeploymentNamespace(), 1, reasonMessage);
        KafkaTopicUtils.waitForKafkaTopicReady(clusterOperator.getDeploymentNamespace(), topicName);

        LOGGER.info("Decreasing number of partitions to {}", decreasePartitions);
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setPartitions(decreasePartitions), clusterOperator.getDeploymentNamespace());
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(clusterOperator.getDeploymentNamespace(), topicName, decreasePartitions);
        KafkaTopicUtils.waitForKafkaTopicNotReady(clusterOperator.getDeploymentNamespace(), topicName);

        reason = "PartitionDecreaseException";
        reasonMessage = "Number of partitions cannot be decreased";
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), NotReady, reason, reasonMessage, 5);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Waiting {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), NotReady, reason, reasonMessage, 5);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> {
            kafkaTopic.getSpec().setReplicas(initialReplicas);
            kafkaTopic.getSpec().setPartitions(initialPartitions);
        }, clusterOperator.getDeploymentNamespace());
        KafkaTopicUtils.waitForKafkaTopicReady(clusterOperator.getDeploymentNamespace(), topicName);
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), Ready, 6);

        // Check failed reconciliations
        // This is currently doesn't work properly and will be changed in UTO implementation - https://github.com/strimzi/proposals/pull/76
        // assertMetricValueHigherThan(toMetricsCollector, "strimzi_reconciliations_failed_total{kind=\"" + KafkaTopic.RESOURCE_KIND + ",}", 3);
    }

    @ParallelTest
    @KRaftNotSupported("Topic Operator is not supported by KRaft mode and is used in this test class")
    void testKafkaTopicChangingMinInSyncReplicas(ExtensionContext extensionContext) throws InterruptedException {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, topicName, 5, clusterOperator.getDeploymentNamespace()).build());
        KafkaTopicUtils.waitForKafkaTopicReady(clusterOperator.getDeploymentNamespace(), topicName);
        String invalidValue = "x";
        String reason = "InvalidConfigurationException";
        String reasonMessage = String.format("Invalid value %s for configuration min.insync.replicas", invalidValue);

        LOGGER.info("Changing min.insync.replicas to random char");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName,
            kafkaTopic -> kafkaTopic.getSpec().getConfig().put("min.insync.replicas", invalidValue),
            clusterOperator.getDeploymentNamespace());
        KafkaTopicUtils.waitForKafkaTopicNotReady(clusterOperator.getDeploymentNamespace(), topicName);
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), NotReady, reason, reasonMessage, 2);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Waiting {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicStatus(topicName, clusterOperator.getDeploymentNamespace(), NotReady, reason, reasonMessage, 2);
    }

    /**
     * @description This test case checks that Kafka cluster will not act upon KafkaTopic Custom Resources
     * which are not of its concern, i.e., KafkaTopic Custom Resources are not labeled accordingly.
     *
     * @steps
     *  1. - Deploy Kafka with short reconciliation time configured on Topic Operator
     *     - Kafka is deployed
     *  2. - Create KafkaTopic Custom Resource without any labels provided
     *     - KafkaTopic Custom resource is created
     *  3. - Verify that KafkaTopic specified by created KafkaTopic is not created
     *     - Given KafkaTopic is not present inside Kafka cluster
     *  4. - Delete given KafkaTopic Custom Resource
     *     - KafkaTopic Custom Resource is deleted
     *
     * @testcase
     *  - topic-operator
     *  - kafka-topic
     *  - labels
     */
    @ParallelNamespaceTest
    @KRaftNotSupported("Topic Operator is not supported by KRaft mode and is used in this test class")
    void testTopicWithoutLabels(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String namespaceName = testStorage.getNamespaceName();
        final String clusterName = testStorage.getClusterName();
        final String scraperName = testStorage.getScraperName();
        final String kafkaTopicName = testStorage.getTargetTopicName();
        final int topicOperatorReconciliationSeconds = 10;

        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        resourceManager.createResource(extensionContext,
            ScraperTemplates.scraperPod(namespaceName, scraperName).build(),
            KafkaTemplates.kafkaEphemeral(clusterName, 3)
                .editSpec()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalSeconds(topicOperatorReconciliationSeconds)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec().build()
        );

        final String scraperPodName =  kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();

        LOGGER.info("Creating KafkaTopic: {}/{} in without any label", namespaceName, kafkaTopicName);
        resourceManager.createResource(extensionContext, false, KafkaTopicTemplates.topic(clusterName, kafkaTopicName, 1, 1, 1, namespaceName)
            .editMetadata()
                .withLabels(null)
            .endMetadata().build()
        );

        // Checking that resource was created
        LOGGER.info("Verifying presence of KafkaTopic: {}/{}", namespaceName, kafkaTopicName);
        assertThat(cmdKubeClient(namespaceName).list("kafkatopic"), hasItems(kafkaTopicName));

        // Checking that TO didn't handle new topic and zk pods don't contain new topic
        KafkaTopicUtils.verifyUnchangedTopicAbsence(namespaceName, scraperPodName, clusterName, kafkaTopicName, topicOperatorReconciliationSeconds);

        // Checking TO logs
        String tOPodName = cmdKubeClient(namespaceName).listResourcesByLabel("pod", Labels.STRIMZI_NAME_LABEL + "=" + clusterName + "-entity-operator").get(0);
        String tOlogs = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString(String.format("Created topic '%s'", kafkaTopicName))));

        //Deleting topic
        cmdKubeClient(namespaceName).deleteByName("kafkatopic", kafkaTopicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespaceName,  kafkaTopicName);

        //Checking KafkaTopic is not present inside Kafka cluster
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName));
        assertThat(topics, not(hasItems(kafkaTopicName)));
    }

    void assertKafkaTopicStatus(String topicName, String namespace, CustomResourceStatus status, int expectedObservedGeneration) {
        assertKafkaTopicStatus(topicName, namespace,  status, null, null, expectedObservedGeneration);
    }

    void assertKafkaTopicStatus(String topicName, String namespace, CustomResourceStatus status, String reason, String message, int expectedObservedGeneration) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().stream()
                .anyMatch(condition -> condition.getType().equals(status.toString())), CoreMatchers.is(true));
        assertThat("KafkaTopic status has incorrect Observed Generation", kafkaTopicStatus.getObservedGeneration(), CoreMatchers.is((long) expectedObservedGeneration));
        if (reason != null) {
            assertThat(kafkaTopicStatus.getConditions().stream()
                .anyMatch(condition -> condition.getReason().equals(reason)), CoreMatchers.is(true));
        }
        if (message != null) {
            assertThat(kafkaTopicStatus.getConditions().stream()
                    .anyMatch(condition -> condition.getMessage().contains(message)), CoreMatchers.is(true));
        }
    }

    boolean hasTopicInKafka(String topicName, String clusterName) {
        LOGGER.info("Checking Topic: {} in Kafka", topicName);
        return KafkaCmdClient.listTopicsUsingPodCli(clusterOperator.getDeploymentNamespace(), scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)).contains(topicName);
    }

    boolean hasTopicInCRK8s(KafkaTopic kafkaTopic, String topicName) {
        LOGGER.info("Checking in KafkaTopic CR that Topic: {} exists", topicName);
        return kafkaTopic.getMetadata().getName().equals(topicName);
    }

    void verifyTopicViaKafka(final String namespaceName, String topicName, int topicPartitions, String clusterName) {
        TestUtils.waitFor("Describing Topic: " + topicName + " using pod CLI", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> {
                try {
                    String topicInfo =  KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), topicName);
                    LOGGER.info("Checking Topic: {} in Kafka: {}", topicName, clusterName);
                    LOGGER.debug("Topic: {} info: {}", topicName, topicInfo);
                    assertThat(topicInfo, containsString("Topic: " + topicName));
                    assertThat(topicInfo, containsString("PartitionCount: " + topicPartitions));
                    return true;
                } catch (KubeClusterException e) {
                    LOGGER.info("Describing Topic using Pod cli occurred following error: {}", e.getMessage());
                    return false;
                }
            });
    }

    void verifyTopicViaKafkaTopicCRK8s(KafkaTopic kafkaTopic, String topicName, int topicPartitions, String clusterName) {
        LOGGER.info("Checking in KafkaTopic CR that Topic: {} was created with expected settings", topicName);
        assertThat(kafkaTopic, is(notNullValue()));
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(clusterOperator.getDeploymentNamespace(), scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)), hasItem(topicName));
        assertThat(kafkaTopic.getMetadata().getName(), is(topicName));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(topicPartitions));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying shared Kafka: {}/{} across all test cases", clusterOperator.getDeploymentNamespace(), KAFKA_CLUSTER_NAME);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(KAFKA_CLUSTER_NAME, 3, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withReconciliationIntervalSeconds((int) Constants.RECONCILIATION_INTERVAL / 1000)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(clusterOperator.getDeploymentNamespace(), SCRAPER_NAME).build()
        );

        scraperPodName = ScraperUtils.getScraperPod(clusterOperator.getDeploymentNamespace()).getMetadata().getName();
        topicOperatorReconciliationInterval = KafkaResource.kafkaClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(KAFKA_CLUSTER_NAME).get()
                .getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalSeconds() * 1000 + 5_000;
    }
}
