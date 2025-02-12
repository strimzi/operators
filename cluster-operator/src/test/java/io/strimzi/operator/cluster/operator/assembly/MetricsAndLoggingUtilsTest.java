/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.strimzi.api.kafka.model.common.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class MetricsAndLoggingUtilsTest {
    @Test
    public void testNoMetricsAndNoExternalLogging(VertxTestContext context)   {
        LoggingModel logging = new LoggingModel(new KafkaConnectSpec(), "KafkaConnectCluster", false, true);
        JmxPrometheusExporterModel metrics = new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().build());

        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);

        Checkpoint async = context.checkpoint();
        MetricsAndLoggingUtils.metricsAndLogging(Reconciliation.DUMMY_RECONCILIATION, mockCmOps, logging, metrics)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(v.loggingCm(), is(nullValue()));
                    assertThat(v.metricsCm(), is(nullValue()));

                    verify(mockCmOps, never()).getAsync(any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testMetricsAndExternalLogging(VertxTestContext context)   {
        LoggingModel logging = new LoggingModel(new KafkaConnectSpecBuilder().withLogging(new ExternalLoggingBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("log4j.properties", "logging-cm", false)).endValueFrom().build()).build(), "KafkaConnectCluster", false, true);
        JmxPrometheusExporterModel metrics = new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().withMetricsConfig(new JmxPrometheusExporterMetricsBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("metrics.yaml", "metrics-cm", false)).endValueFrom().build()).build());

        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        when(mockCmOps.getAsync(any(), eq("logging-cm"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withNewMetadata().withName("logging-cm").endMetadata().withData(Map.of()).build()));
        when(mockCmOps.getAsync(any(), eq("metrics-cm"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withNewMetadata().withName("metrics-cm").endMetadata().withData(Map.of()).build()));

        Checkpoint async = context.checkpoint();
        MetricsAndLoggingUtils.metricsAndLogging(Reconciliation.DUMMY_RECONCILIATION, mockCmOps, logging, metrics)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(v.loggingCm(), is(notNullValue()));
                    assertThat(v.loggingCm().getMetadata().getName(), is("logging-cm"));

                    assertThat(v.metricsCm(), is(notNullValue()));
                    assertThat(v.metricsCm().getMetadata().getName(), is("metrics-cm"));

                    verify(mockCmOps, times(2)).getAsync(any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testNoMetricsAndExternalLogging(VertxTestContext context)   {
        LoggingModel logging = new LoggingModel(new KafkaConnectSpecBuilder().withLogging(new ExternalLoggingBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("log4j.properties", "logging-cm", false)).endValueFrom().build()).build(), "KafkaConnectCluster", false, true);
        JmxPrometheusExporterModel metrics = new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().build());

        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        when(mockCmOps.getAsync(any(), eq("logging-cm"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withNewMetadata().withName("logging-cm").endMetadata().withData(Map.of()).build()));

        Checkpoint async = context.checkpoint();
        MetricsAndLoggingUtils.metricsAndLogging(Reconciliation.DUMMY_RECONCILIATION, mockCmOps, logging, metrics)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(v.loggingCm(), is(notNullValue()));
                    assertThat(v.loggingCm().getMetadata().getName(), is("logging-cm"));

                    assertThat(v.metricsCm(), is(nullValue()));

                    verify(mockCmOps, times(1)).getAsync(any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testMetricsAndNoExternalLogging(VertxTestContext context)   {
        LoggingModel logging = new LoggingModel(new KafkaConnectSpec(), "KafkaConnectCluster", false, true);
        JmxPrometheusExporterModel metrics = new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().withMetricsConfig(new JmxPrometheusExporterMetricsBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("metrics.yaml", "metrics-cm", false)).endValueFrom().build()).build());

        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        when(mockCmOps.getAsync(any(), eq("metrics-cm"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withNewMetadata().withName("metrics-cm").endMetadata().withData(Map.of()).build()));

        Checkpoint async = context.checkpoint();
        MetricsAndLoggingUtils.metricsAndLogging(Reconciliation.DUMMY_RECONCILIATION, mockCmOps, logging, metrics)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(v.loggingCm(), is(nullValue()));

                    assertThat(v.metricsCm(), is(notNullValue()));
                    assertThat(v.metricsCm().getMetadata().getName(), is("metrics-cm"));

                    verify(mockCmOps, times(1)).getAsync(any(), any());

                    async.flag();
                })));
    }
}
