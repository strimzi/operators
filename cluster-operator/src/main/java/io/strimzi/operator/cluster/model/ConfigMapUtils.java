/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared methods for working with Config Maps
 */
public class ConfigMapUtils {
    /**
     * Creates a Config Map
     *
     * @param name              Name of the Config Map
     * @param namespace         Namespace of the Config Map
     * @param labels            Labels of the Config Map
     * @param ownerReference    OwnerReference of the Config Map
     * @param data              Data which will be stored in the Config Map
     *
     * @return  New Config Map
     */
    public static ConfigMap createConfigMap(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            Map<String, String> data
    ) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withData(data)
                .build();
    }

    /**
     * Generates a metrics and logging ConfigMap according to configured defaults. This is used with most operands, but
     * not all of them. Kafka brokers have own methods in the KafkaCluster class. So does the Bridge. And Kafka Exporter
     * has no metrics or logging ConfigMap at all.
     *
     * @param reconciliation        Reconciliation marker
     * @param model                 AbstractModel instance possibly implementing SupportsLogging and SupportsMetrics interfaces
     * @param metricsAndLogging     ConfigMaps with external logging and metrics configuration
     *
     * @return The generated ConfigMap.
     */
    public static Map<String, String> generateMetricsAndLogConfigMapData(Reconciliation reconciliation, AbstractModel model, MetricsAndLogging metricsAndLogging) {
        Map<String, String> data = new HashMap<>(2);

        if (model instanceof SupportsLogging supportsLogging) {
            data.put(supportsLogging.logging().configMapKey(), supportsLogging.logging().loggingConfiguration(reconciliation, metricsAndLogging.loggingCm()));
        }

        // this is only for JMX Prometheus Exporter, because Strimzi Metrics Reporter configuration is in the Kafka configuration file
        if (model instanceof SupportsMetrics supportMetrics && supportMetrics.metrics() instanceof JmxPrometheusExporterModel) {
            String parseResult = ((JmxPrometheusExporterModel) supportMetrics.metrics()).metricsJson(reconciliation, metricsAndLogging.metricsCm());
            if (parseResult != null) {
                data.put(JmxPrometheusExporterModel.CONFIG_MAP_KEY, parseResult);
            }
        }

        return data;
    }
}
