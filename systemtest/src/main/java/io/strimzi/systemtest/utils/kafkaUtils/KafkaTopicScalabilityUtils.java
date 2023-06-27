/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaTopicSpec;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.extension.ExtensionContext;
import io.strimzi.systemtest.enums.CustomResourceStatus;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This class contains crucial methods to create, modify and check large amount of KafkaTopics
 * */
public class KafkaTopicScalabilityUtils {


    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    private KafkaTopicScalabilityUtils() {}

    public static void createTopicsViaK8s(ExtensionContext extensionContext, String namespaceName, String clusterName, String topicPrefix,
                                          int numberOfTopics, int numberOfPartitions, int numberOfReplicas, int minInSyncReplicas) {
        LOGGER.info("Creating {} Topics via Kubernetes", numberOfTopics);

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopicName = topicPrefix + i;
            ResourceManager.getInstance().createResource(extensionContext, false, KafkaTopicTemplates.topic(
                            clusterName, currentTopicName, numberOfPartitions, numberOfReplicas, minInSyncReplicas, namespaceName).build());
        }
    }

    public static void waitForTopicStatus(String namespaceName, String topicPrefix, int numberOfTopics, Enum<?> state) {
        LOGGER.info("Verifying that {} Topics are in {} state", numberOfTopics, state.toString());
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() ->
                    KafkaTopicUtils.waitForKafkaTopicStatus(namespaceName, currentTopic, state)
            ));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All Topics are in correct state"));

        allTopics.join();
    }

    public static void waitForTopicsNotReady(String namespaceName, String topicPrefix, int numberOfTopics) {
        KafkaTopicScalabilityUtils.waitForTopicStatus(namespaceName, topicPrefix, numberOfTopics, CustomResourceStatus.NotReady);
    }

    public static void waitForTopicsReady(String namespaceName, String topicPrefix, int numberOfTopics) {
        KafkaTopicScalabilityUtils.waitForTopicStatus(namespaceName, topicPrefix, numberOfTopics, CustomResourceStatus.Ready);
    }

    public static void waitForTopicsContainConfig(String namespaceName, String topicPrefix, int numberOfTopics, Map<String, Object> config) {
        LOGGER.info("Verifying that {} Topics contain right config", numberOfTopics);
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() -> {
                KafkaTopicUtils.waitForTopicConfigContains(namespaceName, currentTopic, config);
            }));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All Topics contain right config"));

        allTopics.join();
    }

    public static void modifyBigAmountOfTopics(String namespaceName, String topicPrefix, int numberOfTopics, KafkaTopicSpec topicSpec) {
        LOGGER.info("Modify {} Topics via Kubernetes", numberOfTopics);

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopicName = topicPrefix + i;
            KafkaTopicResource.replaceTopicResourceInSpecificNamespace(currentTopicName, kafkaTopic -> kafkaTopic.setSpec(topicSpec), namespaceName);
        }
    }

}
