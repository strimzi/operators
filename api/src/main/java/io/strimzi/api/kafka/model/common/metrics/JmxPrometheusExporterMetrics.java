/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.ExternalConfigurationReference;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * JMX Prometheus Exporter metrics config
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"type", "valueFrom"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class JmxPrometheusExporterMetrics extends MetricsConfig {
    public static final String TYPE_JMX_EXPORTER = "jmxPrometheusExporter";

    private ExternalConfigurationReference valueFrom;

    @Description("ConfigMap entry where the Prometheus JMX Exporter configuration is stored. ")
    public ExternalConfigurationReference getValueFrom() {
        return valueFrom;
    }

    public void setValueFrom(ExternalConfigurationReference valueFrom) {
        this.valueFrom = valueFrom;
    }

    @Description("Must be `" + TYPE_JMX_EXPORTER + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_JMX_EXPORTER;
    }
}
