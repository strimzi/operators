/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

public class KafkaTopicUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    private static final String TOPIC_NAME_PREFIX = "my-topic-";
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();
    private static final Random RANDOM = new Random();

    private KafkaTopicUtils() {}

    /**
     * Generated random name for the KafkaTopic resource
     * @return random name with additional salt
     */
    public static String generateRandomNameOfTopic() {
        String salt = RANDOM.nextInt(Integer.MAX_VALUE) + "-" + RANDOM.nextInt(Integer.MAX_VALUE);

        return  TOPIC_NAME_PREFIX + salt;
    }

    /**
     * Method which return UID for specific topic
     * @param namespaceName Namespace name
     * @param topicName Topic name
     * @return topic UID
     */
    public static String topicSnapshot(final String namespaceName, String topicName) {
        return KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getMetadata().getUid();
    }

    /**
     * Method which wait until topic has rolled form one generation to another.
     * @param namespaceName name of the namespace
     * @param topicName topic name
     * @param topicUid topic UID
     * @return topic new UID
     */
    public static String waitTopicHasRolled(final String namespaceName, String topicName, String topicUid) {
        TestUtils.waitFor("Topic: " + namespaceName + "/" + topicName + " has rolled", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> !topicUid.equals(topicSnapshot(namespaceName, topicName)));
        return topicSnapshot(namespaceName, topicName);
    }

    public static void waitForKafkaTopicCreation(String namespaceName, String topicName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} creation ", namespaceName, topicName);
        TestUtils.waitFor("creation of KafkaTopic: " + namespaceName + "/" + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName)
                    .withName(topicName).get().getStatus().getConditions().get(0).getType().equals(Ready.toString()),
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicCreationByNamePrefix(String namespaceName, String topicNamePrefix) {
        LOGGER.info("Waiting for Topic {}/{} creation", namespaceName, topicNamePrefix);
        TestUtils.waitFor("creation of KafkaTopic: " + namespaceName + "/" + topicNamePrefix, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).list().getItems().stream()
                    .filter(topic -> topic.getMetadata().getName().contains(topicNamePrefix))
                    .findFirst().orElseThrow().getStatus().getConditions().get(0).getType().equals(Ready.toString())
        );
    }

    public static void waitForKafkaTopicDeletion(String namespaceName, String topicName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} deletion", namespaceName, topicName);
        TestUtils.waitFor("deletion of KafkaTopic: " + namespaceName + "/" + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get() == null) {
                    return true;
                } else {
                    LOGGER.warn("KafkaTopic: {}/{} is not deleted yet! Triggering force delete by cmd client!", namespaceName, topicName);
                    cmdKubeClient(namespaceName).deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
                    return false;
                }
            },
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicPartitionChange(String namespaceName, String topicName, int partitions) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to change", namespaceName, topicName);
        TestUtils.waitFor("change of KafkaTopic: " + namespaceName + "/" + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getSpec().getPartitions() == partitions,
            () -> LOGGER.error("KafkaTopic: {}/{} did not change partition", namespaceName, KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicReplicasChange(String namespaceName, String topicName, int replicas) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to change", namespaceName, topicName);
        TestUtils.waitFor("change of KafkaTopic: " + namespaceName + "/" + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getSpec().getReplicas() == replicas,
            () -> LOGGER.error("KafkaTopic: {}/{} did not change replicas", namespaceName, KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    /**
     * Wait until KafkaTopic is in desired status
     * @param namespaceName Namespace name
     * @param topicName name of KafkaTopic
     * @param state desired state
     */
    public static boolean waitForKafkaTopicStatus(String namespaceName, String topicName, Enum<?> state) {
        return ResourceManager.waitForResourceStatus(KafkaTopicResource.kafkaTopicClient(), KafkaTopic.RESOURCE_KIND,
            namespaceName, topicName, state, ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND));
    }

    public static boolean waitForKafkaTopicReady(String namespaceName, String topicName) {
        return waitForKafkaTopicStatus(namespaceName, topicName, Ready);
    }

    public static boolean waitForKafkaTopicNotReady(final String namespaceName, String topicName) {
        return waitForKafkaTopicStatus(namespaceName, topicName, NotReady);
    }

    public static void waitForTopicConfigContains(String namespaceName, String topicName, Map<String, Object> config) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to contain correct config", namespaceName, topicName);
        TestUtils.waitFor("KafkaTopic: " + namespaceName + "/" + topicName + " to contain correct config",
                Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
                () -> KafkaTopicUtils.configsAreEqual(KafkaTopicResource.kafkaTopicClient()
                        .inNamespace(namespaceName).withName(topicName).get().getSpec().getConfig(), config)
        );
        LOGGER.info("KafkaTopic: {}/{} contains correct config", namespaceName, topicName);
    }

    public static boolean configsAreEqual(Map<String, Object> actualConf, Map<String, Object> expectedConf) {
        if ((actualConf != null && expectedConf != null) && (expectedConf.size() == actualConf.size())) {
            return expectedConf.entrySet().stream()
                    .allMatch(expected -> expected.getValue().toString().equals(actualConf.get(expected.getKey()).toString()));
        }
        return false;
    }

    public static void waitForKafkaTopicSpecStability(final String namespaceName, String topicName, String scraperPodName, String bootstrapServer) {
        int[] stableCounter = {0};

        String oldSpec = KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, bootstrapServer, topicName);

        TestUtils.waitFor("KafkaTopic's spec to be stable", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            if (oldSpec.equals(KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, bootstrapServer, topicName))) {
                stableCounter[0]++;
                if (stableCounter[0] == Constants.GLOBAL_STABILITY_OFFSET_COUNT) {
                    LOGGER.info("KafkaTopic's spec is stable for: {} poll intervals", stableCounter[0]);
                    return true;
                }
            } else {
                LOGGER.info("KafkaTopic's spec is not stable. Going to set the counter to zero");
                stableCounter[0] = 0;
                return false;
            }
            LOGGER.info("KafkaTopic's spec gonna be stable in {} polls", Constants.GLOBAL_STABILITY_OFFSET_COUNT - stableCounter[0]);
            return false;
        });
    }

    public static List<KafkaTopic> getAllKafkaTopicsWithPrefix(String namespace, String prefix) {
        return KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).list().getItems()
            .stream().filter(p -> p.getMetadata().getName().startsWith(prefix))
            .collect(Collectors.toList());
    }

    public static void deleteAllKafkaTopicsByPrefixWithWait(String namespace, String prefix) {
        KafkaTopicUtils.getAllKafkaTopicsWithPrefix(namespace, prefix).forEach(topic ->
            cmdKubeClient().namespace(namespace).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topic.getMetadata().getName())
        );
    }

    public static void waitForTopicsByPrefixDeletionUsingPodCli(String namespace, String prefix, String bootstrapName, String scraperPodName, String properties) {
        LOGGER.info("Waiting for all Topics with prefix: {} to be deleted from Kafka", prefix);
        TestUtils.waitFor("deletion of all Topics with prefix: " + prefix, Constants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> !KafkaCmdClient.listTopicsUsingPodCliWithConfigProperties(namespace, scraperPodName, bootstrapName, properties).contains(prefix));
    }

    public static void waitForTopicWillBePresentInKafka(String namespaceName, String topicName, String bootstrapName, String scraperPodName) {
        LOGGER.info("Waiting for KafkaTopic: {}/{} to be present in Kafka", namespaceName, topicName);
        TestUtils.waitFor("KafkaTopic: " + namespaceName + "/" + topicName + " to be present in Kafka", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, bootstrapName).contains(topicName));
    }

    public static List<String> getKafkaTopicReplicasForEachPartition(String namespaceName, String topicName, String podName, String bootstrapServer) {
        return Arrays.stream(KafkaCmdClient.describeTopicUsingPodCli(namespaceName, podName, bootstrapServer, topicName)
            .replaceFirst("Topic.*\n", "")
            .replaceAll(".*Replicas: ", "")
            .replaceAll("\tIsr.*", "")
            .split("\n"))
            .collect(Collectors.toList());
    }

    public static void waitForTopicWithPrefixDeletion(String namespaceName, String topicPrefix) {
        TestUtils.waitFor("deletion of all topics with prefix: " + topicPrefix, Constants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> {
                try {
                    return getAllKafkaTopicsWithPrefix(namespaceName, topicPrefix).size() == 0;
                } catch (Exception e) {
                    return e.getMessage().contains("Not Found") || e.getMessage().contains("the server doesn't have a resource type");
                }
            });
    }

    /**
     * Verifies that {@code absentTopicName} topic remains absent in {@code clusterName} Kafka cluster residing in {@code namespaceName},
     * for two times {@code topicOperatorReconciliationSeconds} duration (in seconds) of Topic Operator reconciliation time,
     * by querying the cluster using kafka scripts from {@code queryingPodName} Pod.
     *
     * @param namespaceName Namespace name
     * @param queryingPodName  the name of the pod to query KafkaTopic from
     * @param clusterName name of Kafka cluster
     * @param absentTopicName name of Kafka topic which should not be created
     * @param topicOperatorReconciliationSeconds interval in seconds for Topic Operator to reconcile
     * @throws AssertionError in case topic is created
     */
    public static void verifyUnchangedTopicAbsence(String namespaceName, String queryingPodName, String clusterName, String absentTopicName, int topicOperatorReconciliationSeconds) {

        long reconciliationDuration = Duration.ofSeconds(topicOperatorReconciliationSeconds).toMillis();
        long endTime = System.currentTimeMillis() + 2 * reconciliationDuration;

        LOGGER.info("Verifying absence of Topic: {}/{} in listed KafkaTopic(s) for next {} second(s)", namespaceName, absentTopicName, reconciliationDuration / 1000, namespaceName);

        while (System.currentTimeMillis() < endTime) {
            assertThat(KafkaCmdClient.listTopicsUsingPodCli(namespaceName, queryingPodName, KafkaResources.plainBootstrapAddress(clusterName)), not(hasItems(absentTopicName)));
            try {
                Thread.sleep(Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
