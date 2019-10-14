/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bases;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.test.BaseITST.cmdKubeClient;
import static io.strimzi.test.BaseITST.kubeClient;

public interface IMetrics {

    Logger LOGGER = LogManager.getLogger(IMetrics.class);
    Object LOCK = new Object();

    /**
     * Collect metrics from specific pod
     * @param podName pod name
     * @return collected metrics
     */
    default String collectMetrics(String podName, String metricsPath) throws InterruptedException, ExecutionException, IOException {
        ArrayList<String> command = new ArrayList<>();
        command.add("curl");
        command.add(kubeClient().getPod(podName).getStatus().getPodIP() + ":9404" + metricsPath);
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", kubeClient().getNamespace(), "--"));
        executableCommand.addAll(command);

        Exec exec = new Exec();
        // 20 seconds should be enough for collect data from the pod
        int ret = exec.execute(null, executableCommand, 20_000);

        synchronized (LOCK) {
            LOGGER.info("Metrics collection for pod {} return code - {}", podName, ret);
        }

        return exec.out();
    }

    default HashMap<String, String> collectKafkaPodsMetrics(String clusterName) {
        LabelSelector kafkaSelector = kubeClient().getStatefulSetSelectors(KafkaResources.kafkaStatefulSetName(clusterName));
        return collectMetricsFromPods(kafkaSelector);
    }

    default HashMap<String, String> collectZookeeperPodsMetrics(String clusterName) {
        LabelSelector zookeeperSelector = kubeClient().getStatefulSetSelectors(KafkaResources.zookeeperStatefulSetName(clusterName));
        return collectMetricsFromPods(zookeeperSelector);
    }

    default HashMap<String, String> collectKafkaConnectPodsMetrics(String clusterName) {
        LabelSelector connectSelector = kubeClient().getDeploymentSelectors(KafkaConnectResources.deploymentName(clusterName));
        return collectMetricsFromPods(connectSelector);
    }

    default HashMap<String, String> collectKafkaExporterPodsMetrics(String clusterName) {
        LabelSelector connectSelector = kubeClient().getDeploymentSelectors(KafkaExporterResources.deploymentName(clusterName));
        return collectMetricsFromPods(connectSelector, "/metrics");
    }


    /**
     * Parse out specific metric from whole metrics file
     * @param pattern regex patern for specific metric
     * @param data all metrics data
     * @return list of parsed values
     */
    default ArrayList<Double> collectSpecificMetric(Pattern pattern, HashMap<String, String> data) {
        ArrayList<Double> values = new ArrayList<>();

        data.forEach((k, v) -> {
            Matcher t = pattern.matcher(v);
            if (t.find()) {
                values.add(Double.parseDouble(t.group(1)));
            }
        });
        return values;
    }

    /**
     * Collect metrics from all pods with specific selector
     * @param labelSelector pod selector
     * @return map with metrics {podName, metrics}
     */
    default HashMap<String, String> collectMetricsFromPods(LabelSelector labelSelector) {
        return collectMetricsFromPods(labelSelector, "");
    }

    /**
     * Collect metrics from all pods with specific selector
     * @param labelSelector pod selector
     * @param metricsPath additional path where metrics are available
     * @return map with metrics {podName, metrics}
     */
    default HashMap<String, String> collectMetricsFromPods(LabelSelector labelSelector, String metricsPath) {
        HashMap<String, String> map = new HashMap<>();
        kubeClient().listPods(labelSelector).forEach(p -> {
            try {
                map.put(p.getMetadata().getName(), collectMetrics(p.getMetadata().getName(), metricsPath));
            } catch (InterruptedException | ExecutionException | IOException e) {
                e.printStackTrace();
            }
        });

        return  map;
    }
}
