/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.jayway.jsonpath.JsonPath;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.utils.TestExecutionWatcher;
import io.strimzi.systemtest.clients.lib.KafkaClient;
import io.strimzi.systemtest.interfaces.TestSeparator;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import io.strimzi.test.BaseITST;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.HelmClient;
import io.strimzi.test.k8s.KubeClusterException;
import io.strimzi.test.k8s.ExecResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.test.TestUtils.entry;
import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.TestUtils.toYamlString;
import static io.strimzi.test.TestUtils.waitFor;
import static io.strimzi.systemtest.matchers.Matchers.logHasNoUnexpectedErrors;
import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
@ExtendWith(TestExecutionWatcher.class)
public abstract class AbstractST extends BaseITST implements TestSeparator {

    static {
        Crds.registerCustomKinds();
    }

    protected static final Environment ENVIRONMENT = Environment.getInstance();

    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);
    protected static final String CLUSTER_NAME = "my-cluster";
    protected static final String ZK_IMAGE = "STRIMZI_DEFAULT_ZOOKEEPER_IMAGE";
    protected static final String KAFKA_IMAGE_MAP = "STRIMZI_KAFKA_IMAGES";
    protected static final String KAFKA_CONNECT_IMAGE_MAP = "STRIMZI_KAFKA_CONNECT_IMAGES";
    protected static final String TO_IMAGE = "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE";
    protected static final String UO_IMAGE = "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE";
    protected static final String KAFKA_INIT_IMAGE = "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE";
    protected static final String TLS_SIDECAR_ZOOKEEPER_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ZOOKEEPER_IMAGE";
    protected static final String TLS_SIDECAR_KAFKA_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE";
    protected static final String TLS_SIDECAR_EO_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE";
    protected static final String TEST_TOPIC_NAME = "test-topic";
    private static final String CLUSTER_OPERATOR_PREFIX = "strimzi";

    public static final String TOPIC_CM = "../examples/topic/kafka-topic.yaml";
    public static final String HELM_CHART = "../helm-charts/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";
    public static final String REQUESTS_MEMORY = "512Mi";
    public static final String REQUESTS_CPU = "200m";
    public static final String LIMITS_MEMORY = "512Mi";
    public static final String LIMITS_CPU = "1000m";

    public static final String TEST_LOG_DIR = ENVIRONMENT.getTestLogDir();

    Resources testMethodResources;
    static Resources testClassResources;
    static String operationID;
    Random rng = new Random();

    protected HelmClient helmClient() {
        return CLUSTER.helmClient();
    }

    static String kafkaClusterName(String clusterName) {
        return KafkaResources.kafkaStatefulSetName(clusterName);
    }

    static String kafkaConnectName(String clusterName) {
        return clusterName + "-connect";
    }

    static String kafkaMirrorMakerName(String clusterName) {
        return clusterName + "-mirror-maker";
    }

    static String kafkaPodName(String clusterName, int podId) {
        return KafkaResources.kafkaPodName(clusterName, podId);
    }

    static String kafkaServiceName(String clusterName) {
        return KafkaResources.bootstrapServiceName(clusterName);
    }

    static String kafkaHeadlessServiceName(String clusterName) {
        return KafkaResources.brokersServiceName(clusterName);
    }

    static String kafkaMetricsConfigName(String clusterName) {
        return KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName);
    }

    static String zookeeperClusterName(String clusterName) {
        return KafkaResources.zookeeperStatefulSetName(clusterName);
    }

    static String zookeeperPodName(String clusterName, int podId) {
        return KafkaResources.zookeeperPodName(clusterName, podId);
    }

    static String zookeeperServiceName(String clusterName) {
        return zookeeperClusterName(clusterName) + "-client";
    }

    static String zookeeperHeadlessServiceName(String clusterName) {
        return zookeeperClusterName(clusterName) + "-nodes";
    }

    static String zookeeperMetricsConfigName(String clusterName) {
        return KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName);
    }

    static String zookeeperPVCName(String clusterName, int podId) {
        return "data-" + zookeeperClusterName(clusterName) + "-" + podId;
    }

    static String entityOperatorDeploymentName(String clusterName) {
        return KafkaResources.entityOperatorDeploymentName(clusterName);
    }

    private <T extends CustomResource, L extends CustomResourceList<T>, D extends Doneable<T>>
        void replaceCrdResource(Class<T> crdClass, Class<L> listClass, Class<D> doneableClass, String resourceName, Consumer<T> editor) {
        Resource<T, D> namedResource = Crds.operation(CLIENT, crdClass, listClass, doneableClass).inNamespace(KUBE_CLIENT.namespace()).withName(resourceName);
        T resource = namedResource.get();
        editor.accept(resource);
        namedResource.replace(resource);
    }

    void replaceKafkaResource(String resourceName, Consumer<Kafka> editor) {
        replaceCrdResource(Kafka.class, KafkaList.class, DoneableKafka.class, resourceName, editor);
    }

    void replaceKafkaConnectResource(String resourceName, Consumer<KafkaConnect> editor) {
        replaceCrdResource(KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class, resourceName, editor);
    }

    void replaceTopicResource(String resourceName, Consumer<KafkaTopic> editor) {
        replaceCrdResource(KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class, resourceName, editor);
    }

    void replaceMirrorMakerResource(String resourceName, Consumer<KafkaMirrorMaker> editor) {
        replaceCrdResource(KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class, resourceName, editor);
    }

    String getBrokerApiVersions(String podName) {
        AtomicReference<String> versions = new AtomicReference<>();
        waitFor("kafka-broker-api-versions.sh success", Constants.GET_BROKER_API_INTERVAL, Constants.GET_BROKER_API_TIMEOUT, () -> {
            try {
                String output = KUBE_CLIENT.execInPod(podName,
                        "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092").out();
                versions.set(output);
                return true;
            } catch (KubeClusterException e) {
                LOGGER.trace("/opt/kafka/bin/kafka-broker-api-versions.sh: {}", e.getMessage());
                return false;
            }
        });
        return versions.get();
    }

    void waitForZkMntr(Pattern pattern, int... podIndexes) {
        long timeoutMs = 120_000L;
        long pollMs = 1_000L;

        for (int podIndex : podIndexes) {
            String zookeeperPod = zookeeperPodName(CLUSTER_NAME, podIndex);
            String zookeeperPort = String.valueOf(2181 * 10 + podIndex);
            waitFor("mntr", pollMs, timeoutMs, () -> {
                try {
                    String output = KUBE_CLIENT.execInPod(zookeeperPod,
                        "/bin/bash", "-c", "echo mntr | nc localhost " + zookeeperPort).out();

                    if (pattern.matcher(output).find()) {
                        return true;
                    }
                } catch (KubeClusterException e) {
                    LOGGER.trace("Exception while waiting for ZK to become leader/follower, ignoring", e);
                }
                return false;
                },
                () -> LOGGER.info("zookeeper `mntr` output at the point of timeout does not match {}:{}{}",
                    pattern.pattern(),
                    System.lineSeparator(),
                    indent(KUBE_CLIENT.execInPod(zookeeperPod, "/bin/bash", "-c", "echo mntr | nc localhost " + zookeeperPort).out()))
            );
        }
    }

    static String getValueFromJson(String json, String jsonPath) {
        return JsonPath.parse(json).read(jsonPath).toString();
    }

    /**
     * Translate key/value pairs fromatted like properties into a Map
     * @param keyValuePairs Pairs in key=value format; pairs are separated by newlines
     * @return THe map of key/values
     */
    static Map<String, String> loadProperties(String keyValuePairs) {
        try {
            Properties actual = new Properties();
            actual.load(new StringReader(keyValuePairs));
            return (Map) actual;
        } catch (IOException e) {
            throw new AssertionError("Invalid Properties definiton", e);
        }
    }

    /**
     * Get a Map of properties from an environment variable in json.
     * @param json The json from which to extract properties
     * @param envVar The environment variable name
     * @return The properties which the variable contains
     */
    static Map<String, String> getPropertiesFromJson(String json, String envVar) {
        List<String> array = JsonPath.parse(json).read(globalVariableJsonPathBuilder(envVar));
        return loadProperties(array.get(0));
    }

    /**
     * Get a jsonPath which can be used to extract envariable variables from a spec
     * @param envVar The environment variable name
     * @return The json path
     */
    static String globalVariableJsonPathBuilder(String envVar) {
        return "$.spec.containers[*].env[?(@.name=='" + envVar + "')].value";
    }

    List<Event> getEvents(String resourceUid) {
        return CLIENT.events().inNamespace(KUBE_CLIENT.namespace()).list().getItems().stream()
                .filter(event -> event.getInvolvedObject().getUid().equals(resourceUid))
                .collect(Collectors.toList());
    }

    public void sendMessages(String podName, String clusterName, String topic, int messagesCount) {
        LOGGER.info("Sending messages");
        String command = "sh bin/kafka-verifiable-producer.sh --broker-list " +
                KafkaResources.plainBootstrapAddress(clusterName) + " --topic " + topic + " --max-messages " + messagesCount + "";

        LOGGER.info("Command for kafka-verifiable-producer.sh {}", command);

        KUBERNETES.execInPod(podName, "/bin/bash", "-c", command);
    }

    public String consumeMessages(String clusterName, String topic, int groupID, int timeout, int kafkaPodID) {
        LOGGER.info("Consuming messages");
        String output = KUBE_CLIENT.execInPod(kafkaPodName(clusterName, kafkaPodID), "/bin/bash", "-c",
                "bin/kafka-verifiable-consumer.sh --broker-list " +
                        KafkaResources.plainBootstrapAddress(clusterName) + " --topic " + topic + " --group-id " + groupID + " & sleep "
                        + timeout + "; kill %1").out();
        output = "[" + output.replaceAll("\n", ",") + "]";
        LOGGER.info("Output for kafka-verifiable-consumer.sh {}", output);
        return output;

    }

    protected void assertResources(String namespace, String podName, String containerName, String memoryLimit, String cpuLimit, String memoryRequest, String cpuRequest) {
        Pod po = KUBERNETES.getPod(podName);
        assertNotNull(po, "Not found an expected pod  " + podName + " in namespace " + namespace + " but found " +
                KUBERNETES.listPods().stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()));

        Optional optional = po.getSpec().getContainers().stream().filter(c -> c.getName().equals(containerName)).findFirst();
        assertTrue(optional.isPresent(), "Not found an expected container " + containerName);

        Container container = (Container) optional.get();
        Map<String, Quantity> limits = container.getResources().getLimits();
        assertEquals(memoryLimit, limits.get("memory").getAmount());
        assertEquals(cpuLimit, limits.get("cpu").getAmount());
        Map<String, Quantity> requests = container.getResources().getRequests();
        assertEquals(memoryRequest, requests.get("memory").getAmount());
        assertEquals(cpuRequest, requests.get("cpu").getAmount());
    }

    protected void assertExpectedJavaOpts(String podName, String expectedXmx, String expectedXms, String expectedServer, String expectedXx) {
        List<List<String>> cmdLines = commandLines(podName, "java");
        assertEquals(1, cmdLines.size(), "Expected exactly 1 java process to be running");
        List<String> cmd = cmdLines.get(0);
        int toIndex = cmd.indexOf("-jar");
        if (toIndex != -1) {
            // Just consider arguments to the JVM, not the application running in it
            cmd = cmd.subList(0, toIndex);
            // We should do something similar if the class not -jar was given, but that's
            // hard to do properly.
        }
        assertCmdOption(cmd, expectedXmx);
        assertCmdOption(cmd, expectedXms);
        assertCmdOption(cmd, expectedServer);
        assertCmdOption(cmd, expectedXx);
    }

    private void assertCmdOption(List<String> cmd, String expectedXmx) {
        if (!cmd.contains(expectedXmx)) {
            fail("Failed to find argument matching " + expectedXmx + " in java command line " +
                    cmd.stream().collect(Collectors.joining("\n")));
        }
    }

    private List<List<String>> commandLines(String podName, String cmd) {
        List<List<String>> result = new ArrayList<>();
        ExecResult pr = KUBE_CLIENT.execInPod(podName, "/bin/bash", "-c",
                "for pid in $(ps -C java -o pid h); do cat /proc/$pid/cmdline; done"
        );
        for (String cmdLine : pr.out().split("\n")) {
            result.add(asList(cmdLine.split("\0")));
        }
        return result;
    }

    void assertNoCoErrorsLogged(long sinceSeconds) {
        LOGGER.info("Search in strimzi-cluster-operator log for errors in last {} seconds", sinceSeconds);
        String clusterOperatorLog = KUBE_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", sinceSeconds, "Exception", "Error", "Throwable");
        assertThat(clusterOperatorLog, logHasNoUnexpectedErrors());
    }

    public List<String> listTopicsUsingPodCLI(String clusterName, int zkPodId) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return Arrays.asList(KUBE_CLIENT.execInPod(podName, "/bin/bash", "-c",
                "bin/kafka-topics.sh --list --zookeeper localhost:" + port).out().split("\\s+"));
    }

    public String createTopicUsingPodCLI(String clusterName, int zkPodId, String topic, int replicationFactor, int partitions) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return KUBE_CLIENT.execInPod(podName, "/bin/bash", "-c",
                "bin/kafka-topics.sh --zookeeper localhost:" + port + " --create " + " --topic " + topic +
                        " --replication-factor " + replicationFactor + " --partitions " + partitions).out();
    }

    public String deleteTopicUsingPodCLI(String clusterName, int zkPodId, String topic) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return KUBE_CLIENT.execInPod(podName, "/bin/bash", "-c",
                "bin/kafka-topics.sh --zookeeper localhost:" + port + " --delete --topic " + topic).out();
    }

    public List<String>  describeTopicUsingPodCLI(String clusterName, int zkPodId, String topic) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return Arrays.asList(KUBE_CLIENT.execInPod(podName, "/bin/bash", "-c",
                "bin/kafka-topics.sh --zookeeper localhost:" + port + " --describe --topic " + topic).out().split("\\s+"));
    }

    public String updateTopicPartitionsCountUsingPodCLI(String clusterName, int zkPodId, String topic, int partitions) {
        String podName = zookeeperPodName(clusterName, zkPodId);
        int port = 2181 * 10 + zkPodId;
        return KUBE_CLIENT.execInPod(podName, "/bin/bash", "-c",
                "bin/kafka-topics.sh --zookeeper localhost:" + port + " --alter --topic " + topic + " --partitions " + partitions).out();
    }

    public Map<String, String> getImagesFromConfig() {
        Map<String, String> images = new HashMap<>();
        for (Container c : KUBERNETES.getDeployment("strimzi-cluster-operator").getSpec().getTemplate().getSpec().getContainers()) {
            for (EnvVar envVar : c.getEnv()) {
                images.put(envVar.getName(), envVar.getValue());
            }
        }
        return images;
    }

    public String getContainerImageNameFromPod(String podName) {
        String clusterOperatorJson = KUBE_CLIENT.getResourceAsJson("pod", podName);
        return JsonPath.parse(clusterOperatorJson).read("$.spec.containers[*].image").toString().replaceAll("[\"\\[\\]\\\\]", "");
    }

    public String getContainerImageNameFromPod(String podName, String containerName) {
        String clusterOperatorJson = KUBE_CLIENT.getResourceAsJson("pod", podName);
        return JsonPath.parse(clusterOperatorJson).read("$.spec.containers[?(@.name =='" + containerName + "')].image").toString().replaceAll("[\"\\[\\]\\\\]", "");
    }

    public String  getInitContainerImageName(String podName) {
        String clusterOperatorJson = KUBE_CLIENT.getResourceAsJson("pod", podName);
        return JsonPath.parse(clusterOperatorJson).read("$.spec.initContainers[-1].image");
    }

    protected void createTestMethodResources() {
        LOGGER.info("Creating resources before the test");
        testMethodResources = new Resources(namespacedClient());
    }

    protected static void createTestClassResources() {
        LOGGER.info("Creating test class resources");
        testClassResources = new Resources(namespacedClient());
    }

    protected void deleteTestMethodResources() throws Exception {
        if (testMethodResources != null) {
            testMethodResources.deleteResources();
            testMethodResources = null;
        }
    }

    Resources testMethodResources() {
        return testMethodResources;
    }

    String startTimeMeasuring(Operation operation) {
        TimeMeasuringSystem.setTestName(testClass, testName);
        return TimeMeasuringSystem.startOperation(operation);
    }

    /** Get the name of the pod for a job */
    String jobPodName(Job job) {
        return podNameWithLabels(job.getSpec().getTemplate().getMetadata().getLabels());
    }

    String userOperatorPodName() {
        return podNameWithLabels(Collections.singletonMap("strimzi.io/name", CLUSTER_NAME + "-entity-operator"));
    }

    String podNameWithLabels(Map<String, String> labels) {
        List<Pod> pods = KUBERNETES.listPods();
        if (pods.size() != 1) {
            fail("There are " + pods.size() +  " pods with labels " + labels);
        }
        return pods.get(0).getMetadata().getName();
    }

    /**
     * Greps logs from a pod which ran kafka-verifiable-producer.sh and
     * kafka-verifiable-consumer.sh
     */
    void checkPings(int messagesCount, Job job) {
        String podName = jobPodName(job);
        String log = KUBERNETES.logs(podName);
        Matcher m = BRACE_PATTERN.matcher(log);
        boolean producerSuccess = false;
        boolean consumerSuccess = false;
        while (m.find()) {
            String json = m.group();
            String name2 = getValueFromJson(json, "$.name");
            if ("tool_data".equals(name2)) {
                assertEquals(String.valueOf(messagesCount), getValueFromJson(json, "$.sent"));
                assertEquals(String.valueOf(messagesCount), getValueFromJson(json, "$.acked"));
                producerSuccess = true;
            } else if ("records_consumed".equals(name2)) {
                assertEquals(String.valueOf(messagesCount), getValueFromJson(json, "$.count"));
                consumerSuccess = true;
            }
        }
        if (!producerSuccess || !consumerSuccess) {
            LOGGER.info("log from pod {}:\n----\n{}\n----", podName, indent(log));
        }
        assertTrue(producerSuccess, "The producer didn't send any messages (no tool_data message)");
        assertTrue(consumerSuccess, "The consumer didn't consume any messages (no records_consumed message)");
    }

    /**
     * Waits for a job to complete successfully, {@link org.junit.jupiter.api.Assertions#fail()}ing
     * if it completes with any failed pods.
     * @throws TimeoutException if the job doesn't complete quickly enough.
     */
    Job waitForJobSuccess(Job job) {
        // Wait for the job to succeed
        try {
            LOGGER.debug("Waiting for Job completion: {}", job);
            waitFor("Job completion", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT, () -> {
                Job jobs = KUBERNETES.getJob(job.getMetadata().getName());
                JobStatus status;
                if (jobs == null || (status = jobs.getStatus()) == null) {
                    LOGGER.debug("Poll job is null");
                    return false;
                } else {
                    if (status.getFailed() != null && status.getFailed() > 0) {
                        LOGGER.debug("Poll job failed");
                        fail();
                    } else if (status.getSucceeded() != null && status.getSucceeded() == 1) {
                        LOGGER.debug("Poll job succeeded");
                        return true;
                    } else if (status.getActive() != null && status.getActive() > 0) {
                        LOGGER.debug("Poll job has active");
                        return false;
                    }
                }
                LOGGER.debug("Poll job in indeterminate state");
                return false;
            });
            return job;
        } catch (TimeoutException e) {
            LOGGER.info("Original Job: {}", job);
            try {
                LOGGER.info("Job: {}", indent(toYamlString(KUBERNETES.getJob(job.getMetadata().getName()))));
            } catch (Exception | AssertionError t) {
                LOGGER.info("Job not available: {}", t.getMessage());
            }
            try {
                LOGGER.info("Pod: {}", indent(toYamlString(KUBERNETES.getJob(jobPodName(job)))));
            } catch (Exception | AssertionError t) {
                LOGGER.info("Pod not available: {}", t.getMessage());
            }
            try {
                LOGGER.info("Job timeout: Job Pod logs\n----\n{}\n----", indent(KUBERNETES.logs(jobPodName(job))));
            } catch (Exception | AssertionError t) {
                LOGGER.info("Pod logs not available: {}", t.getMessage());
            }
            try {
                LOGGER.info("Job timeout: User Operator Pod logs\n----\n{}\n----", indent(KUBERNETES.logs(userOperatorPodName(), "user-operator")));
            } catch (Exception | AssertionError t) {
                LOGGER.info("Pod logs not available: {}", t.getMessage());
            }
            throw e;
        }
    }

    String saslConfigs(KafkaUser kafkaUser) {
        Secret secret = KUBERNETES.getSecret(kafkaUser.getMetadata().getName());

        String password = new String(Base64.getDecoder().decode(secret.getData().get("password")));
        if (password == null) {
            LOGGER.info("Secret {}:\n{}", kafkaUser.getMetadata().getName(), toYamlString(secret));
            throw new RuntimeException("The Secret " + kafkaUser.getMetadata().getName() + " lacks the 'password' key");
        }
        return "sasl.mechanism=SCRAM-SHA-512\n" +
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\\n" +
                "username=\"" + kafkaUser.getMetadata().getName() + "\" \\\n" +
                "password=\"" + password + "\";\n";
    }

    private PodSpecBuilder createPodSpecForProducer(ContainerBuilder cb, KafkaUser kafkaUser, boolean tlsListener, String bootstrapServer) {
        PodSpecBuilder podSpecBuilder = new PodSpecBuilder()
                .withRestartPolicy("OnFailure");

        String kafkaUserName = kafkaUser != null ? kafkaUser.getMetadata().getName() : null;
        boolean scramShaUser = kafkaUser != null && kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication;
        boolean tlsUser = kafkaUser != null && kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication;

        String producerConfiguration = "acks=all\n";
        if (tlsListener) {
            if (scramShaUser) {
                producerConfiguration += "security.protocol=SASL_SSL\n";
                producerConfiguration += saslConfigs(kafkaUser);
            } else {
                producerConfiguration += "security.protocol=SSL\n";
            }
            producerConfiguration +=
                    "ssl.truststore.location=/tmp/truststore.p12\n" +
                            "ssl.truststore.type=pkcs12\n";
        } else {
            if (scramShaUser) {
                producerConfiguration += "security.protocol=SASL_PLAINTEXT\n";
                producerConfiguration += saslConfigs(kafkaUser);
            } else {
                producerConfiguration += "security.protocol=PLAINTEXT\n";
            }
        }

        if (tlsUser) {
            producerConfiguration +=
                    "ssl.keystore.location=/tmp/keystore.p12\n" +
                            "ssl.keystore.type=pkcs12\n";
            cb.addNewEnv().withName("PRODUCER_TLS").withValue("TRUE").endEnv();

            String userSecretVolumeName = "tls-cert";
            String userSecretMountPoint = "/opt/kafka/user-secret";
            cb.addNewVolumeMount()
                    .withName(userSecretVolumeName)
                    .withMountPath(userSecretMountPoint)
                    .endVolumeMount()
                    .addNewEnv().withName("USER_LOCATION").withValue(userSecretMountPoint).endEnv();
            podSpecBuilder
                    .addNewVolume()
                    .withName(userSecretVolumeName)
                    .withNewSecret()
                    .withSecretName(kafkaUserName)
                    .endSecret()
                    .endVolume();
        }

        cb.addNewEnv().withName("PRODUCER_CONFIGURATION").withValue(producerConfiguration).endEnv();

        if (kafkaUserName != null) {
            cb.addNewEnv().withName("KAFKA_USER").withValue(kafkaUserName).endEnv();
        }

        if (tlsListener) {
            String clusterCaSecretName = clusterCaCertSecretName(bootstrapServer);
            String clusterCaSecretVolumeName = "ca-cert";
            String caSecretMountPoint = "/opt/kafka/cluster-ca";
            cb.addNewVolumeMount()
                .withName(clusterCaSecretVolumeName)
                .withMountPath(caSecretMountPoint)
                .endVolumeMount()
                .addNewEnv().withName("PRODUCER_TLS").withValue("TRUE").endEnv()
                .addNewEnv().withName("CA_LOCATION").withValue(caSecretMountPoint).endEnv()
                .addNewEnv().withName("TRUSTSTORE_LOCATION").withValue("/tmp/truststore.p12").endEnv();
            if (tlsUser) {
                cb.addNewEnv().withName("KEYSTORE_LOCATION").withValue("/tmp/keystore.p12").endEnv();
            }
            podSpecBuilder
                .addNewVolume()
                    .withName(clusterCaSecretVolumeName)
                        .withNewSecret()
                            .withSecretName(clusterCaSecretName)
                        .endSecret()
                .endVolume();
        }

        return podSpecBuilder.withContainers(cb.build());
    }

    Job sendRecordsToClusterJob(String bootstrapServer, String name, String topic, int messagesCount, KafkaUser kafkaUser, boolean tlsListener) {

        String connect = tlsListener ? bootstrapServer + "-kafka-bootstrap:9093" : bootstrapServer + "-kafka-bootstrap:9092";

        ContainerBuilder cb = new ContainerBuilder()
                .withName("send-records")
                .withImage(changeOrgAndTag("strimzi/test-client:latest"))
                .addNewEnv().withName("PRODUCER_OPTS").withValue(
                        "--broker-list " + connect + " " +
                                "--topic " + topic + " " +
                                "--max-messages " + messagesCount).endEnv()
                .withCommand("/opt/kafka/producer.sh");

        PodSpec producerPodSpec = createPodSpecForProducer(cb, kafkaUser, tlsListener, bootstrapServer).build();

        Job job = resources().deleteLater(KUBERNETES.createJob(new JobBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                    .withNewSpec()
                        .withNewTemplate()
                        .withNewMetadata()
                        .withName(name)
                        .addToLabels("job", name)
                .endMetadata()
                    .withSpec(producerPodSpec)
                        .endTemplate()
                    .endSpec()
                .build()));
        LOGGER.info("Created Job {}", job);
        return job;
    }

    private PodSpecBuilder createPodSpecForConsumer(ContainerBuilder cb, KafkaUser kafkaUser, boolean tlsListener, String bootstrapServer) {

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder()
                .withRestartPolicy("OnFailure");

        String kafkaUserName = kafkaUser != null ? kafkaUser.getMetadata().getName() : null;
        boolean scramShaUser = kafkaUser != null && kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication;
        boolean tlsUser = kafkaUser != null && kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication;

        String consumerConfiguration = "auto.offset.reset=earliest\n";
        if (tlsListener) {
            if (scramShaUser) {
                consumerConfiguration += "security.protocol=SASL_SSL\n";
                consumerConfiguration += saslConfigs(kafkaUser);
            } else {
                consumerConfiguration += "security.protocol=SSL\n";
            }
            consumerConfiguration += "auto.offset.reset=earliest\n" +
                    "ssl.truststore.location=/tmp/truststore.p12\n" +
                    "ssl.truststore.type=pkcs12\n";
        } else {
            if (scramShaUser) {
                consumerConfiguration += "security.protocol=SASL_PLAINTEXT\n";
                consumerConfiguration += saslConfigs(kafkaUser);
            } else {
                consumerConfiguration += "security.protocol=PLAINTEXT\n";
            }
        }

        if (tlsUser) {
            consumerConfiguration += "auto.offset.reset=earliest\n" +
                    "ssl.keystore.location=/tmp/keystore.p12\n" +
                    "ssl.keystore.type=pkcs12\n";
            cb.addNewEnv().withName("CONSUMER_TLS").withValue("TRUE").endEnv();

            String userSecretVolumeName = "tls-cert";
            String userSecretMountPoint = "/opt/kafka/user-secret";
            cb.addNewVolumeMount()
                    .withName(userSecretVolumeName)
                    .withMountPath(userSecretMountPoint)
                    .endVolumeMount()
                    .addNewEnv().withName("USER_LOCATION").withValue(userSecretMountPoint).endEnv();
            podSpecBuilder
                    .addNewVolume()
                    .withName(userSecretVolumeName)
                    .withNewSecret()
                    .withSecretName(kafkaUserName)
                    .endSecret()
                    .endVolume();
        }

        cb.addNewEnv().withName("CONSUMER_CONFIGURATION").withValue(consumerConfiguration).endEnv();

        if (kafkaUserName != null) {
            cb.addNewEnv().withName("KAFKA_USER").withValue(kafkaUserName).endEnv();
        }

        if (tlsListener) {
            String clusterCaSecretName = clusterCaCertSecretName(bootstrapServer);
            String clusterCaSecretVolumeName = "ca-cert";
            String caSecretMountPoint = "/opt/kafka/cluster-ca";
            cb.addNewVolumeMount()
                    .withName(clusterCaSecretVolumeName)
                    .withMountPath(caSecretMountPoint)
                    .endVolumeMount()
                    .addNewEnv().withName("CONSUMER_TLS").withValue("TRUE").endEnv()
                    .addNewEnv().withName("CA_LOCATION").withValue(caSecretMountPoint).endEnv()
                    .addNewEnv().withName("TRUSTSTORE_LOCATION").withValue("/tmp/truststore.p12").endEnv();
            if (tlsUser) {
                cb.addNewEnv().withName("KEYSTORE_LOCATION").withValue("/tmp/keystore.p12").endEnv();
            }
            podSpecBuilder
                    .addNewVolume()
                    .withName(clusterCaSecretVolumeName)
                    .withNewSecret()
                    .withSecretName(clusterCaSecretName)
                    .endSecret()
                    .endVolume();
        }
        return podSpecBuilder.withContainers(cb.build());
    }

    Job readMessagesFromClusterJob(String bootstrapServer, String name, String topic, int messagesCount, KafkaUser kafkaUser, boolean tlsListener) {

        String connect = tlsListener ? bootstrapServer + "-kafka-bootstrap:9093" : bootstrapServer + "-kafka-bootstrap:9092";
        ContainerBuilder cb = new ContainerBuilder()
                .withName("read-messages")
                .withImage(changeOrgAndTag("strimzi/test-client:latest"))
                .addNewEnv().withName("CONSUMER_OPTS").withValue(
                        "--broker-list " + connect + " " +
                                "--group-id " + name + "-" + "my-group" + " " +
                                "--verbose " +
                                "--topic " + topic + " " +
                                "--max-messages " + messagesCount).endEnv()
                .withCommand("/opt/kafka/consumer.sh");


        PodSpec consumerPodSpec = createPodSpecForConsumer(cb, kafkaUser, tlsListener, bootstrapServer).build();

        Job job = resources().deleteLater(KUBERNETES.createJob(new JobBuilder()
            .withNewMetadata()
                .withName(name)
            .endMetadata()
            .withNewSpec()
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(name)
                            .addToLabels("job", name)
                    .endMetadata()
                    .withSpec(consumerPodSpec)
                .endTemplate()
            .endSpec()
            .build()));
        LOGGER.info("Created Job {}", job);
        return job;
    }

    /**
     * Greps logs from a pod which ran kafka-verifiable-consumer.sh
     */
    void checkRecordsForConsumer(int messagesCount, Job job) {
        String podName = jobPodName(job);
        String log = KUBERNETES.logs(podName);
        Matcher m = BRACE_PATTERN.matcher(log);
        boolean consumerSuccess = false;
        while (m.find()) {
            String json = m.group();
            String name = getValueFromJson(json, "$.name");
            if ("records_consumed".equals(name)) {
                assertEquals(String.valueOf(messagesCount), getValueFromJson(json, "$.count"));
                consumerSuccess = true;
            }
        }
        if (!consumerSuccess) {
            LOGGER.info("log from pod {}:\n----\n{}\n----", podName, indent(log));
        }
        assertTrue(consumerSuccess, "The consumer didn't consume any messages (no records_consumed message)");
    }

    String clusterCaCertSecretName(String cluster) {
        return cluster + "-cluster-ca-cert";
    }

    void waitTillSecretExists(String secretName) {
        waitFor("secret " + secretName + " exists", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KUBERNETES.getSecret(secretName) != null);
        try {
            Thread.sleep(60000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void waitForPodDeletion(String namespace, String podName) {
        LOGGER.info("Waiting when Pod {} will be deleted", podName);

        TestUtils.waitFor("statefulset " + podName, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> CLIENT.pods().inNamespace(namespace).withName(podName).get() == null);
    }

    /**
     * Wait till all pods in specific namespace being deleted and recreate testing environment in case of some pods cannot be deleted.
     * @param time timeout in miliseconds
     * @param namespace namespace where we expect no pods or only CO pod
     * @throws Exception exception
     */
    void waitForDeletion(long time, String namespace) throws Exception {
        List<Pod> pods = CLIENT.pods().inNamespace(namespace).list().getItems().stream().filter(
            p -> !p.getMetadata().getName().startsWith(CLUSTER_OPERATOR_PREFIX)).collect(Collectors.toList());
        // Delete pods in case of kubernetes keep them up
        pods.forEach(p -> CLIENT.pods().inNamespace(namespace).delete(p));

        LOGGER.info("Wait for {} ms after cleanup to make sure everything is deleted", time);
        Thread.sleep(time);

        // Collect pods again after proper removal
        pods = KUBERNETES.listPods().stream().filter(
            p -> !p.getMetadata().getName().startsWith(CLUSTER_OPERATOR_PREFIX)).collect(Collectors.toList());
        long podCount = pods.size();

        StringBuilder nonTerminated = new StringBuilder();
        if (podCount > 0) {
            pods.forEach(
                p -> nonTerminated.append("\n").append(p.getMetadata().getName()).append(" - ").append(p.getStatus().getPhase())
            );
            throw new Exception("There are some unexpected pods! Cleanup is not finished properly!" + nonTerminated);
        }
    }

    /**
     * Recreate namespace and CO after test failure
     * @param coNamespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to.
     */
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        testClassResources.deleteResources();

        deleteClusterOperatorInstallFiles();
        deleteNamespaces();

        createNamespaces(coNamespace, bindingsNamespaces);
        applyClusterOperatorInstallFiles();

        testClassResources = new Resources(KUBERNETES.getClient());

        applyRoleBindings(coNamespace, bindingsNamespaces);
        // 050-Deployment
        testClassResources.clusterOperator(coNamespace).done();
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces list of namespaces where Bindings should be deployed to
     */
    private static void applyRoleBindings(String namespace, List<String> bindingsNamespaces) {
        for (String bindingsNamespace : bindingsNamespaces) {
            // 020-RoleBinding
            testClassResources.kubernetesRoleBinding("../install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 021-ClusterRoleBinding
            testClassResources.kubernetesClusterRoleBinding("../install/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 030-ClusterRoleBinding
            testClassResources.kubernetesClusterRoleBinding("../install/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace, bindingsNamespace);
            // 031-RoleBinding
            testClassResources.kubernetesRoleBinding("../install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", namespace, bindingsNamespace);
            // 032-RoleBinding
            testClassResources.kubernetesRoleBinding("../install/cluster-operator/032-RoleBinding-strimzi-cluster-operator-topic-operator-delegation.yaml", namespace, bindingsNamespace);
        }
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     */
    static void applyRoleBindings(String namespace) {
        applyRoleBindings(namespace, Collections.singletonList(namespace));
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to
     */
    static void applyRoleBindings(String namespace, String... bindingsNamespaces) {
        applyRoleBindings(namespace, Arrays.asList(bindingsNamespaces));
    }

    /**
     * Deploy CO via helm chart. Using config file stored in test resources.
     */
    void deployClusterOperatorViaHelmChart() {
        String dockerOrg = ENVIRONMENT.getStrimziOrg();
        String dockerTag = ENVIRONMENT.getStrimziTag();

        Map<String, String> values = Collections.unmodifiableMap(Stream.of(
                entry("imageRepositoryOverride", dockerOrg),
                entry("imageTagOverride", dockerTag),
                entry("image.pullPolicy", Constants.IMAGE_PULL_POLICY),
                entry("resources.requests.memory", REQUESTS_MEMORY),
                entry("resources.requests.cpu", REQUESTS_CPU),
                entry("resources.limits.memory", LIMITS_MEMORY),
                entry("resources.limits.cpu", LIMITS_CPU),
                entry("logLevel", ENVIRONMENT.getStrimziLogLevel()))
                .collect(TestUtils.entriesToMap()));

        LOGGER.info("Creating cluster operator with Helm Chart before test class {}", testClass);
        Path pathToChart = new File(HELM_CHART).toPath();
        String oldNamespace = KUBE_CLIENT.namespace("kube-system");
        InputStream helmAccountAsStream = getClass().getClassLoader().getResourceAsStream("helm/helm-service-account.yaml");
        String helmServiceAccount = TestUtils.readResource(helmAccountAsStream);
        KUBE_CLIENT.applyContent(helmServiceAccount);
        helmClient().init();
        KUBE_CLIENT.namespace(oldNamespace);
        helmClient().install(pathToChart, HELM_RELEASE_NAME, values);
    }

    /**
     * Delete CO deployed via helm chart.
     */
    void deleteClusterOperatorViaHelmChart() {
        LOGGER.info("Deleting cluster operator with Helm Chart after test class {}", testClass);
        helmClient().delete(HELM_RELEASE_NAME);
    }

    /**
     * Wait for cluster availability, check availability of external routes with TLS
     * @param userName user name
     * @param namespace cluster namespace
     * @throws Exception
     */
    void waitForClusterAvailabilityTls(String userName, String namespace) throws Exception {
        int messageCount = 50;
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);

        KafkaClient testClient = new KafkaClient();
        try {
            Future producer = testClient.sendMessagesTls(topicName, namespace, CLUSTER_NAME, userName, messageCount);
            Future consumer = testClient.receiveMessagesTls(topicName, namespace, CLUSTER_NAME, userName, messageCount);

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
            e.printStackTrace();
            throw e;
        } finally {
            testClient.close();
        }
    }

    /**
     * Wait for cluster availability, check availability of external routes without TLS
     * @param namespace cluster namespace
     * @throws Exception
     */
    void waitForClusterAvailability(String namespace) throws Exception {
        int messageCount = 50;
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);

        KafkaClient testClient = new KafkaClient();
        try {
            Future producer = testClient.sendMessages(topicName, namespace, CLUSTER_NAME, messageCount);
            Future consumer = testClient.receiveMessages(topicName, namespace, CLUSTER_NAME, messageCount);

            assertThat("Producer produced all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));
        } catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
            e.printStackTrace();
            throw e;
        } finally {
            testClient.close();
        }
    }

    void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
    }

    void tearDownEnvironmentAfterAll() {
        testClassResources.deleteResources();
    }

    @AfterEach
    void teardownEnvironmentMethod(ExtensionContext context) throws Exception {
        if (ENVIRONMENT.getNoteardown() == null) {
            if (context.getExecutionException().isPresent()) {
                LOGGER.info("Test execution contains exception, going to recreate test environment");
                recreateTestEnv(clusterOperatorNamespace, bindingsNamespaces);
                LOGGER.info("Env recreated.");
            }
            tearDownEnvironmentAfterEach();
        }
    }

    @AfterAll
    void teardownEnvironmentClass() {
        if (ENVIRONMENT.getNoteardown() == null) {
            tearDownEnvironmentAfterAll();
            teardownEnvForOperator();
        }
    }
}