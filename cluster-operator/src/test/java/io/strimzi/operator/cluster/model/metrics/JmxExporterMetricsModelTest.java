/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JmxExporterMetricsModelTest {
    @Test
    public void testDisabled()   {
        JmxPrometheusExporterModel metrics = new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().build());

        assertThat(metrics.isEnabled(), is(false));
        assertThat(metrics.getConfigMapName(), is(nullValue()));
        assertThat(metrics.getConfigMapKey(), is(nullValue()));
    }

    @Test
    public void testMetrics()   {
        MetricsConfig metricsConfig = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelector("my-key", "my-name", false))
                .endValueFrom()
                .build();

        JmxPrometheusExporterModel metrics = new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().withMetricsConfig(metricsConfig).build());

        assertThat(metrics.isEnabled(), is(true));
        assertThat(metrics.getConfigMapName(), is("my-name"));
        assertThat(metrics.getConfigMapKey(), is("my-key"));
        assertThat(metrics.metricsJson(Reconciliation.DUMMY_RECONCILIATION, new ConfigMapBuilder().withData(Map.of("my-key", "")).build()), is("{}"));
        assertThat(metrics.metricsJson(Reconciliation.DUMMY_RECONCILIATION, new ConfigMapBuilder().withData(Map.of("my-key", "foo: bar")).build()), is("{\"foo\":\"bar\"}"));
    }

    @Test
    public void testProblemWithConfigMap()   {
        MetricsConfig metricsConfig = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                .withConfigMapKeyRef(new ConfigMapKeySelector("my-key", "my-name", false))
                .endValueFrom()
                .build();

        JmxPrometheusExporterModel metrics = new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().withMetricsConfig(metricsConfig).build());

        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> metrics.metricsJson(Reconciliation.DUMMY_RECONCILIATION, null));
        assertThat(ex.getMessage(), is("ConfigMap my-name does not exist"));

        ex = assertThrows(InvalidConfigurationException.class, () -> metrics.metricsJson(Reconciliation.DUMMY_RECONCILIATION, new ConfigMapBuilder().withData(Map.of("other-key", "foo: bar")).build()));
        assertThat(ex.getMessage(), is("ConfigMap my-name does not contain specified key my-key"));

        ex = assertThrows(InvalidConfigurationException.class, () -> metrics.metricsJson(Reconciliation.DUMMY_RECONCILIATION, new ConfigMapBuilder().withData(Map.of("my-key", "foo: -")).build()));
        assertThat(ex.getMessage(), startsWith("Failed to parse metrics configuration"));
    }

    @Test
    public void testPrometheusJmxMetricsValidation() {
        assertDoesNotThrow(() -> JmxPrometheusExporterModel.validateJmxExporterMetricsConfig(new JmxPrometheusExporterMetricsBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("my-key", "my-name", false)).endValueFrom().build()));

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> JmxPrometheusExporterModel.validateJmxExporterMetricsConfig(new JmxPrometheusExporterMetricsBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector()).endValueFrom().build()));
        assertThat(ex.getMessage(), is("Metrics configuration is invalid: [Name of the Config Map with metrics configuration is missing, The key under which the metrics configuration is stored in the ConfigMap is missing]"));

        ex = assertThrows(InvalidResourceException.class, () -> JmxPrometheusExporterModel.validateJmxExporterMetricsConfig(new JmxPrometheusExporterMetricsBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector(null, "my-name", false)).endValueFrom().build()));
        assertThat(ex.getMessage(), is("Metrics configuration is invalid: [The key under which the metrics configuration is stored in the ConfigMap is missing]"));

        ex = assertThrows(InvalidResourceException.class, () -> JmxPrometheusExporterModel.validateJmxExporterMetricsConfig(new JmxPrometheusExporterMetricsBuilder().withNewValueFrom().endValueFrom().build()));
        assertThat(ex.getMessage(), is("Metrics configuration is invalid: [Config Map reference is missing]"));

        ex = assertThrows(InvalidResourceException.class, () -> JmxPrometheusExporterModel.validateJmxExporterMetricsConfig(new JmxPrometheusExporterMetricsBuilder().build()));
        assertThat(ex.getMessage(), is("Metrics configuration is invalid: [Config Map reference is missing]"));
    }
}
