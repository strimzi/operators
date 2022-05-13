/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Random;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

/**
 * ClientUtils class, which provides static methods for the all type clients
 * @see io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient
 */
public class ClientUtils {

    private static final Logger LOGGER = LogManager.getLogger(ClientUtils.class);
    private static final String CONSUMER_GROUP_NAME = "my-consumer-group-";
    private static Random rng = new Random();

    // ensuring that object can not be created outside of class
    private ClientUtils() {}

    public static void waitForClientsSuccess(String producerName, String consumerName, String namespace, int messageCount) {
        waitForClientsSuccess(producerName, consumerName, namespace, messageCount, true);
    }

    public static void waitForClientsSuccess(String producerName, String consumerName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting till producer {} and consumer {} finish", producerName, consumerName);
        TestUtils.waitFor("clients finished", Constants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> kubeClient().checkSucceededJobStatus(namespace, producerName, 1)
                && kubeClient().checkSucceededJobStatus(namespace, consumerName, 1),
            () -> {
                JobUtils.logCurrentJobStatus(producerName, namespace);
                JobUtils.logCurrentJobStatus(consumerName, namespace);
            });

        if (deleteAfterSuccess) {
            JobUtils.deleteJobsWithWait(namespace, producerName, consumerName);
        }
    }

    public static void waitForClientSuccess(String jobName, String namespace, int messageCount) {
        waitForClientSuccess(jobName, namespace, messageCount, true);
    }

    public static void waitForClientSuccess(String jobName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer/consumer:{} to finished", jobName);
        TestUtils.waitFor("job finished", Constants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
            () -> {
                LOGGER.debug("Job {} in namespace {}, has status {}", jobName, namespace, kubeClient().namespace(namespace).getJobStatus(jobName));
                return kubeClient().checkSucceededJobStatus(namespace, jobName, 1);
            },
            () -> JobUtils.logCurrentJobStatus(jobName, namespace));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespace, jobName);
        }
    }

    public static void waitForClientTimeout(String jobName, String namespace, int messageCount) {
        waitForClientTimeout(jobName, namespace, messageCount, true);
    }

    public static void waitForClientTimeout(String jobName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer/consumer:{} to finish with failure.", jobName);
        try {
            TestUtils.waitFor("Job did not finish within time limit (as expected).", Constants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> kubeClient().checkFailedJobStatus(namespace, jobName, 1),
                () -> JobUtils.logCurrentJobStatus(jobName, namespace));

            if (deleteAfterSuccess) {
                JobUtils.deleteJobWithWait(namespace, jobName);
            }
        } catch (WaitException e) {
            if (e.getMessage().contains("Timeout after ")) {
                LOGGER.info("Client job '{}' finished with expected timeout.", jobName);
                if (deleteAfterSuccess) {
                    JobUtils.deleteJobWithWait(namespace, jobName);
                }
            } else {
                JobUtils.logCurrentJobStatus(jobName, namespace);
                throw e;
            }
        }
    }

    public static void waitForClientsTimeout(String producerName, String consumerName, String namespace, int messageCount) {
        waitForClientsTimeout(producerName, consumerName, namespace, messageCount, true);
    }

    public static void waitForClientsTimeout(String producerName, String consumerName, String namespace, int messageCount, boolean deleteAfterSuccess) {
        LOGGER.info("Waiting for producer: {} and consumer: {} to finish with failure.", producerName, consumerName);

        try {
            TestUtils.waitFor("clients timeout", Constants.GLOBAL_POLL_INTERVAL, timeoutForClientFinishJob(messageCount),
                () -> kubeClient().checkFailedJobStatus(namespace, producerName, 1)
                    && kubeClient().checkFailedJobStatus(namespace, consumerName, 1),
                () -> {
                    JobUtils.logCurrentJobStatus(producerName, namespace);
                    JobUtils.logCurrentJobStatus(consumerName, namespace);
                });

            if (deleteAfterSuccess) {
                JobUtils.deleteJobsWithWait(namespace, producerName, consumerName);
            }
        } catch (WaitException e) {
            if (e.getMessage().contains("Timeout after ")) {
                LOGGER.info("Both jobs - {} and {} - hit the wait timeout - as expected", producerName, consumerName);
                if (deleteAfterSuccess) {
                    JobUtils.deleteJobsWithWait(namespace, producerName, consumerName);
                }
            } else {
                throw e;
            }
        }
    }

    public static void waitForClientContainsMessage(String jobName, String namespace, String message) {
        waitForClientContainsMessage(jobName, namespace, message, true);
    }

    public static void waitForClientContainsMessage(String jobName, String namespace, String message, boolean deleteAfterSuccess) {
        String jobPodName = PodUtils.getPodNameByPrefix(namespace, jobName);
        LOGGER.info("Waiting for client:{} will contain message: {}", jobName, message);

        TestUtils.waitFor("job contains message " + message, Constants.GLOBAL_POLL_INTERVAL, Constants.THROTTLING_EXCEPTION_TIMEOUT,
            () -> kubeClient().logsInSpecificNamespace(namespace, jobPodName).contains(message),
            () -> JobUtils.logCurrentJobStatus(jobName, namespace));

        if (deleteAfterSuccess) {
            JobUtils.deleteJobWithWait(namespace, jobName);
        }
    }

    private static long timeoutForClientFinishJob(int messagesCount) {
        // need to add at least 2minutes for finishing the job
        return (long) messagesCount * 1000 + Duration.ofMinutes(2).toMillis();
    }

    /**
     * Method which generates random consumer group name
     * @return consumer group name with pattern: my-consumer-group-*-*
     */
    public static String generateRandomConsumerGroup() {
        int salt = rng.nextInt(Integer.MAX_VALUE);

        return CONSUMER_GROUP_NAME + salt;
    }
}

