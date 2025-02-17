/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpecBuilder;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziMetricsReporterModelTest {
    @Test
    public void testDisabled() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder().build());

        assertThat(metrics.isEnabled(), is(false));
        assertThat(metrics.getAllowList(), is(Optional.empty()));
    }

    @Test
    public void testEnabled() {
        StrimziMetricsReporter metricsConfig = new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList(List.of("kafka_log.*", "kafka_network.*"))
                .endValues()
                .build();
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(metricsConfig).build());

        assertThat(metrics.isEnabled(), is(true));
        assertThat(metrics.getAllowList().get(), is("kafka_log.*,kafka_network.*"));
    }

    @Test
    public void testValidation() {
        assertDoesNotThrow(() -> StrimziMetricsReporterModel.validate(new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList(List.of("kafka_log.*", "kafka_network.*"))
                .endValues()
                .build())
        );

        InvalidResourceException ise0 = assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                        .withAllowList(List.of())
                        .endValues()
                        .build())
        );
        assertThat(ise0.getMessage(), is("Metrics configuration is invalid: [Allowlist should contain at least one element]"));


        InvalidResourceException ise1 = Assertions.assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                        .withAllowList(List.of("kafka_network.*", "kafka_log.***", "[a+"))
                        .endValues()
                        .build())
        );
        assertThat(ise1.getMessage(), is("Metrics configuration is invalid: [Invalid regex: kafka_log.***, Dangling meta character '*', Invalid regex: [a+, Unclosed character class]"));
    }
}
