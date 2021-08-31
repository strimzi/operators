/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.logs.CollectorElement;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.strimzi.test.TestUtils.writeFile;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

/**
 * LogCollector collects logs from all resources **if test case or preparation phase fails**. All can be found
 * inside ./systemtest/target/logs directory where the structure of the logs are as follows:
 *
 * ./logs
 *      /time/
 *          /test-suite_time/
 *              /test-case/
 *                  deployment.log
 *                  configmap.log
 *                  ...
 *
 *              cluster-operator.log    // shared cluster operator logs for all tests inside one test suite
 *          /another-test-suite_time/
 *      ...
 */
public class LogCollector {
    private static final Logger LOGGER = LogManager.getLogger(LogCollector.class);

    private static final String CURRENT_DATE;

    static {
        // Get current date to create a unique folder
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.of("GMT"));
        CURRENT_DATE = dateTimeFormatter.format(LocalDateTime.now());
    }

    private final KubeClient kubeClient;
    private final File testSuite;
    private final File testCase;
    private final File logDir;
    private final String clusterOperatorNamespace;
    private File namespaceFile;
    private CollectorElement collectorElement;

    public LogCollector(CollectorElement collectorElement, KubeClient kubeClient, String logDir) throws IOException {
        this.collectorElement = collectorElement;
        this.kubeClient = kubeClient;

        this.logDir = new File(logDir + "/" + CURRENT_DATE);
        final String logSuiteDir = this.logDir + "/" + collectorElement.getTestClassName();

        this.testSuite = new File(logSuiteDir);

        // contract only one Cluster Operator deployment inside all namespaces
        Pod clusterOperatorPod = kubeClient.getClient().pods().inAnyNamespace().list().getItems().stream()
                .filter(pod -> pod.getMetadata().getName().contains(Constants.STRIMZI_DEPLOYMENT_NAME))
                // contract only one Cluster Operator deployment inside all namespaces
                .findFirst()
                .orElseGet(Pod::new);

        this.clusterOperatorNamespace = clusterOperatorPod.getMetadata() != null ?
                clusterOperatorPod.getMetadata().getNamespace() :
                kubeClient.getNamespace();

        this.testCase = new File(logSuiteDir + "/" + collectorElement.getTestMethodName());

        boolean logDirExist = this.logDir.exists() || this.logDir.mkdirs();
        boolean logTestSuiteDirExist = this.testSuite.exists() || this.testSuite.mkdirs();
        boolean logTestCaseDirExist = this.testCase.exists() || this.testCase.mkdirs();

        if (!logDirExist) {
            throw new IOException("Unable to create path");
        }
        if (!logTestSuiteDirExist) {
            throw new IOException("Unable to create path");
        }
        if (!logTestCaseDirExist) {
            throw new IOException("Unable to create path");
        }
    }

    /**
     * Core method which collects all logs from events, configs-maps, pods, deployment, statefulset, replicaset...in case test fails.
     */
    public synchronized void collect() {
        Set<String> namespaces = KubeClusterResource.getMapWithSuiteNamespaces().get(this.collectorElement);
        AtomicReference<CollectorElement> collectorElement = new AtomicReference<>();
        collectorElement.set(CollectorElement.emptyElement());

        if (namespaces == null) {
            collectorElement.set(CollectorElement.createCollectorElement(this.collectorElement.getTestClassName()));
            namespaces = KubeClusterResource.getMapWithSuiteNamespaces().get(collectorElement.get());
        }

        if (namespaces != null) {
            namespaces.add(clusterOperatorNamespace);
            // collect logs for all namespace related to test suite
            namespaces.forEach(namespace -> {
                if (collectorElement.get().isEmpty()) {
                    namespaceFile = new File(this.testCase + "/" + namespace);
                } else {
                    namespaceFile = new File(this.testSuite +  "/" + namespace);
                }

                boolean namespaceLogDirExist = this.namespaceFile.exists() || this.namespaceFile.mkdirs();
                if (!namespaceLogDirExist) throw new RuntimeException("Unable to create path");

                this.collectEvents(namespace);
                this.collectConfigMaps(namespace);
                this.collectLogsFromPods(namespace);
                this.collectDeployments(namespace);
                this.collectStatefulSets(namespace);
                this.collectReplicaSets(namespace);
                this.collectStrimzi(namespace);
                this.collectClusterInfo(namespace);
            });
        }
    }

    private void collectLogsFromPods(String namespace) {
        try {
            LOGGER.info("Collecting logs for Pod(s) in Namespace {}", namespace);

            // in case we are in the cluster operator namespace we wants shared logs for whole test suite
            if (namespace.equals(this.clusterOperatorNamespace)) {
                kubeClient.listPods(namespace).forEach(pod -> {
                    String podName = pod.getMetadata().getName();
                    pod.getStatus().getContainerStatuses().forEach(
                        containerStatus -> scrapeAndCreateLogs(testSuite, podName, containerStatus, namespace));
                });
            } else {
                kubeClient.listPods(namespace).forEach(pod -> {
                    String podName = pod.getMetadata().getName();
                    pod.getStatus().getContainerStatuses().forEach(
                        containerStatus -> scrapeAndCreateLogs(namespaceFile, podName, containerStatus, namespace));
                });
            }
        } catch (Exception allExceptions) {
            LOGGER.warn("Searching for logs in all pods failed! Some of the logs will not be stored. Exception message" + allExceptions.getMessage());
        }
    }

    private void collectEvents(String namespace) {
        LOGGER.info("Collecting events in Namespace {}", namespace);
        String events = cmdKubeClient(namespace).getEvents();
        // Write events to file
        writeFile(namespaceFile + "/events.log", events);
    }

    private void collectConfigMaps(String namespace) {
        LOGGER.info("Collecting ConfigMaps in Namespace {}", namespace);
        kubeClient.listConfigMaps(namespace).forEach(configMap -> {
            writeFile(namespaceFile + "/" + configMap.getMetadata().getName() + ".log", configMap.toString());
        });
    }

    private void collectDeployments(String namespace) {
        LOGGER.info("Collecting Deployments in Namespace {}", namespace);
        writeFile(namespaceFile + "/deployments.log", cmdKubeClient(namespace).getResourcesAsYaml(Constants.DEPLOYMENT));
    }

    private void collectStatefulSets(String namespace) {
        LOGGER.info("Collecting StatefulSets in Namespace {}", namespace);
        writeFile(namespaceFile + "/statefulsets.log", cmdKubeClient(namespace).getResourcesAsYaml(Constants.STATEFUL_SET));
    }

    private void collectReplicaSets(String namespace) {
        LOGGER.info("Collecting ReplicaSets in Namespace {}", namespace);
        writeFile(namespaceFile + "/replicasets.log", cmdKubeClient(namespace).getResourcesAsYaml("replicaset"));
    }

    private void collectStrimzi(String namespace) {
        LOGGER.info("Collecting Strimzi in Namespace {}", namespace);
        String crData = cmdKubeClient(namespace).exec(false, false, "get", "strimzi", "-o", "yaml", "-n", namespaceFile.getName()).out();
        writeFile(namespaceFile + "/strimzi-custom-resources.log", crData);
    }

    private void collectClusterInfo(String namespace) {
        LOGGER.info("Collecting cluster status");
        String nodes = cmdKubeClient(namespace).exec(false, false, "describe", "nodes").out();
        writeFile(this.testSuite + "/cluster-status.log", nodes);
    }

    private void scrapeAndCreateLogs(File path, String podName, ContainerStatus containerStatus, String namespace) {
        String log = kubeClient.getPodResource(namespace, podName).inContainer(containerStatus.getName()).getLog();
        // Write logs from containers to files
        writeFile(path + "/logs-pod-" + podName + "-container-" + containerStatus.getName() + ".log", log);
        // Describe all pods
        String describe = cmdKubeClient(namespace).describe("pod", podName);
        writeFile(path + "/describe-pod-" + podName + "-container-" + containerStatus.getName() + ".log", describe);
    }
}
