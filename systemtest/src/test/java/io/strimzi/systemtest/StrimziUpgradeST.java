/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(REGRESSION)
public class StrimziUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";
    private String zkSsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
    private String kafkaSsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
    private String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);

    private Map<String, String> zkPods;
    private Map<String, String> kafkaPods;
    private Map<String, String> eoPods;

    @ParameterizedTest
    @CsvFileSource(resources = "/StrimziUpgradeST.csv")
    void upgradeStrimziVersion(String fromVersion, String toVersion, String urlFrom, String urlTo, String images, String procedures) throws IOException {
        KUBE_CMD_CLIENT.namespace(NAMESPACE);
        File coDir = null;
        File kafkaEphemeralYaml = null;
        File kafkaTopicYaml = null;
        File kafkaUserYaml = null;

        try {
            String url = urlFrom;
            File dir = StUtils.downloadAndUnzip(url);

            coDir = new File(dir, "strimzi-" + fromVersion + "/install/cluster-operator/");
            // Modify + apply installation files
            copyModifyApply(coDir);

            LOGGER.info("Waiting for CO deployment");
            StUtils.waitForDeploymentReady("strimzi-cluster-operator");

            // Deploy a Kafka cluster
            kafkaEphemeralYaml = new File(dir, "strimzi-" + fromVersion + "/examples/kafka/kafka-ephemeral.yaml");
            KUBE_CMD_CLIENT.create(kafkaEphemeralYaml);
            // Wait for readiness
            waitForClusterReadiness();

            // And a topic and a user
            kafkaTopicYaml = new File(dir, "strimzi-" + fromVersion + "/examples/topic/kafka-topic.yaml");
            KUBE_CMD_CLIENT.create(kafkaTopicYaml);
            kafkaUserYaml = new File(dir, "strimzi-" + fromVersion + "/examples/user/kafka-user.yaml");
            KUBE_CMD_CLIENT.create(kafkaUserYaml);

            makeSnapshots();

            List<Pod> pods = KUBE_CLIENT.listPods(KUBE_CLIENT.getStatefulSetSelectors(zkSsName));
            for (Pod pod : pods) {
                LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            }

            // Execution of required procedures before upgrading CO
            if (!procedures.isEmpty()) {
                String[] proceduresArray = procedures.split("\\s*,\\s*");
                for (String procedure : proceduresArray) {
                    switch (procedure) {
                        case "set log message format version to 2.0": {
                            replaceKafka(CLUSTER_NAME, k -> k.getSpec().getKafka().getConfig().put("log.message.format.version", "2.0"));
                            StUtils.waitTillSsHasRolled(NAMESPACE, kafkaSsName, kafkaPods);
                            makeSnapshots();
                            break;
                        }
                        case "set log message format version to 2.1": {
                            replaceKafka(CLUSTER_NAME, k -> k.getSpec().getKafka().getConfig().put("log.message.format.version", "2.1"));
                            StUtils.waitTillSsHasRolled(NAMESPACE, kafkaSsName, kafkaPods);
                            makeSnapshots();
                            break;
                        }
                        case "set Kafka version to 2.0.0": {
                            replaceKafka(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion("2.0.0"));
                            makeSnapshots();
                            break;
                        }
                        case "set Kafka version to 2.1.0": {
                            replaceKafka(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion("2.1.0"));
                            StUtils.waitTillSsHasRolled(NAMESPACE, kafkaSsName, kafkaPods);
                            makeSnapshots();
                            break;
                        }
                        case "set Kafka version to 2.1.1": {
                            replaceKafka(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion("2.1.1"));
                            StUtils.waitTillSsHasRolled(NAMESPACE, kafkaSsName, kafkaPods);
                            makeSnapshots();
                            break;
                        }
                    }
                }
            }

            // Upgrade the CO
            // Modify + apply installation files
            if ("HEAD" .equals(toVersion)) {
                LOGGER.info("Updating");
                copyModifyApply(new File("../install/cluster-operator"));
                LOGGER.info("Waiting for CO redeployment");
                StUtils.waitForDeploymentReady("strimzi-cluster-operator");
                waitForRollingUpdate(images);
            } else {
                url = urlTo;
                dir = StUtils.downloadAndUnzip(url);
                coDir = new File(dir, "strimzi-" + toVersion + "/install/cluster-operator/");
                copyModifyApply(coDir);
                LOGGER.info("Waiting for CO deployment");
                StUtils.waitForDeploymentReady("strimzi-cluster-operator");
                waitForRollingUpdate(images);
            }

            // Tidy up
        } catch (KubeClusterException e) {
            if (kafkaEphemeralYaml != null) {
                KUBE_CMD_CLIENT.delete(kafkaEphemeralYaml);
            }
            if (coDir != null) {
                KUBE_CMD_CLIENT.delete(coDir);
            }
            throw e;
        } finally {
            deleteInstalledYamls(new File("../install/cluster-operator"));
        }

    }

    private void copyModifyApply(File root) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                KUBE_CMD_CLIENT.applyContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else if (f.getName().matches("050-Deployment.*")) {
                KUBE_CMD_CLIENT.applyContent(TestUtils.changeDeploymentNamespaceUpgrade(f, NAMESPACE));
            } else {
                KUBE_CMD_CLIENT.apply(f);
            }
        });
    }

    private void deleteInstalledYamls(File root) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                KUBE_CMD_CLIENT.deleteContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else {
                KUBE_CMD_CLIENT.delete(f);
            }
        });
    }

    private void waitForClusterReadiness() {
        // Wait for readiness
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        StUtils.waitForAllStatefulSetPodsReady("my-cluster-zookeeper");
        LOGGER.info("Waiting for Kafka StatefulSet");
        StUtils.waitForAllStatefulSetPodsReady("my-cluster-kafka");
        LOGGER.info("Waiting for EO Deployment");
        StUtils.waitForDeploymentReady("my-cluster-entity-operator");
    }

    private void waitForRollingUpdate(String images) {
        if (images.isEmpty()) {
            fail("There are no expected images");
        }
        String[] imagesArray = images.split("\\s*,\\s*");
        String zkImage = imagesArray[0];
        String kafkaImage = imagesArray[1];
        String tOImage = imagesArray[2];
        String uOImage = imagesArray[3];

        LOGGER.info("Waiting for ZK SS roll");
        StUtils.waitTillSsHasRolled(NAMESPACE, zkSsName, zkPods);
        LOGGER.info("Checking ZK pods using new image");
        waitTillAllPodsUseImage(KUBE_CLIENT.getStatefulSet(zkSsName).getSpec().getSelector().getMatchLabels(),
                zkImage);

        LOGGER.info("Waiting for Kafka SS roll");
        StUtils.waitTillSsHasRolled(NAMESPACE, kafkaSsName, kafkaPods);
        LOGGER.info("Checking Kafka pods using new image");
        waitTillAllPodsUseImage(KUBE_CLIENT.getStatefulSet(kafkaSsName).getSpec().getSelector().getMatchLabels(),
                kafkaImage);
        LOGGER.info("Waiting for EO Dep roll");
        // Check the TO and UO also got upgraded
        StUtils.waitTillDepHasRolled(NAMESPACE, eoDepName, eoPods);
        LOGGER.info("Checking EO pod using new image");
        waitTillAllContainersUseImage(
                KUBE_CLIENT.getDeployment(eoDepName).getSpec().getSelector().getMatchLabels(),
                0,
                tOImage);
        waitTillAllContainersUseImage(
                KUBE_CLIENT.getDeployment(eoDepName).getSpec().getSelector().getMatchLabels(),
                1,
                uOImage);
    }

    private void makeSnapshots() {
        zkPods = StUtils.ssSnapshot(NAMESPACE, zkSsName);
        kafkaPods = StUtils.ssSnapshot(NAMESPACE, kafkaSsName);
        eoPods = StUtils.depSnapshot(NAMESPACE, eoDepName);
    }

    private Kafka getKafka(String resourceName) {
        Resource<Kafka, DoneableKafka> namedResource = Crds.kafkaV1Alpha1Operation(KUBE_CLIENT.getClient()).inNamespace(KUBE_CLIENT.getNamespace()).withName(resourceName);
        return namedResource.get();
    }

    private void replaceKafka(String resourceName, Consumer<Kafka> editor) {
        Resource<Kafka, DoneableKafka> namedResource = Crds.kafkaV1Alpha1Operation(KUBE_CLIENT.getClient()).inNamespace(KUBE_CLIENT.getNamespace()).withName(resourceName);
        Kafka kafka = getKafka(resourceName);
        editor.accept(kafka);
        namedResource.replace(kafka);
    }

    private void waitTillAllPodsUseImage(Map<String, String> matchLabels, String image) {
        waitTillAllContainersUseImage(matchLabels, 0, image);
    }

    private void waitTillAllContainersUseImage(Map<String, String> matchLabels, int container, String image) {
        TestUtils.waitFor("All pods matching " + matchLabels + " to have image " + image, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            List<Pod> pods1 = KUBE_CLIENT.listPods(matchLabels);
            for (Pod pod : pods1) {
                if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                    LOGGER.info("Expected image: {} \nCurrent image: {}", image, pod.getSpec().getContainers().get(container).getImage());
                    return false;
                }
            }
            return true;
        });
    }

    @BeforeEach
    void setupEnvironment() {
        LOGGER.info("Creating namespace: {}", NAMESPACE);
        createNamespace(NAMESPACE);
    }

    @Override
    void tearDownEnvironmentAfterEach() {
        deleteNamespaces();
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        deleteNamespaces();
        createNamespace(NAMESPACE);
    }
}
