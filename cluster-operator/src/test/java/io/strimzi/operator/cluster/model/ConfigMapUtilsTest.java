/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.strimzi.api.kafka.model.common.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigMapUtilsTest {
    private final static String NAME = "my-cm";
    private final static String NAMESPACE = "my-namespace";
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("my-kind")
            .withName("my-name")
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-name")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type")
            .withAdditionalLabels(Map.of("label-1", "value-1", "label-2", "value-2"));
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    @Test
    public void testConfigMapCreation() {
        ConfigMap cm = ConfigMapUtils.createConfigMap(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, Map.of("key1", "value1", "key2", "value2"));

        assertThat(cm.getMetadata().getName(), is(NAME));
        assertThat(cm.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(cm.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(cm.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(cm.getMetadata().getAnnotations(), is(Map.of()));

        assertThat(cm.getData(), is(Map.of("key1", "value1", "key2", "value2")));
    }


    @Test
    public void testConfigMapDataNoMetricsNoLogging()   {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        Map<String, String> data = ConfigMapUtils.generateMetricsAndLogConfigMapData(Reconciliation.DUMMY_RECONCILIATION, new ModelWithoutMetricsAndLogging(kafka), new MetricsAndLogging(null, null));

        assertThat(data.size(), is(0));
    }

    @Test
    public void testConfigMapDataWithMetricsAndLogging()   {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        Map<String, String> data = ConfigMapUtils.generateMetricsAndLogConfigMapData(Reconciliation.DUMMY_RECONCILIATION, new ModelWithMetricsAndLogging(kafka), new MetricsAndLogging(new ConfigMapBuilder().withNewMetadata().withName("metrics-cm").endMetadata().withData(Map.of("metrics.yaml", "")).build(), new ConfigMapBuilder().withNewMetadata().withName("logging-cm").endMetadata().withData(Map.of("log4j.properties", "")).build()));

        assertThat(data.size(), is(2));
        assertThat(data.get(JmxPrometheusExporterModel.CONFIG_MAP_KEY), is(notNullValue()));
        assertThat(data.get(LoggingModel.LOG4J1_CONFIG_MAP_KEY), is(notNullValue()));
    }

    private static class ModelWithoutMetricsAndLogging extends AbstractModel   {
        public ModelWithoutMetricsAndLogging(HasMetadata resource) {
            super(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()),
                resource, resource.getMetadata().getName() + "-model-app", "model-app", SHARED_ENV_PROVIDER);
        }
    }

    private static class ModelWithMetricsAndLogging extends AbstractModel implements SupportsMetrics, SupportsLogging {
        public ModelWithMetricsAndLogging(HasMetadata resource) {
            super(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()),
                resource, resource.getMetadata().getName() + "-model-app", "model-app", SHARED_ENV_PROVIDER);
        }

        @Override
        public LoggingModel logging() {
            return new LoggingModel(new KafkaConnectSpecBuilder().withLogging(new ExternalLoggingBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("log4j.properties", "logging-cm", false)).endValueFrom().build()).build(), "KafkaConnectCluster", false, true);
        }

        @Override
        public JmxPrometheusExporterModel metrics() {
            return new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().withMetricsConfig(new JmxPrometheusExporterMetricsBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("metrics.yaml", "metrics-cm", false)).endValueFrom().build()).build());
        }
    }
}
