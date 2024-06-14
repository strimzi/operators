/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.tracing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures the tracing
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@SuppressWarnings("deprecation") // Jaeger Tracing is deprecated
@JsonSubTypes({
    @JsonSubTypes.Type(name = JaegerTracing.TYPE_JAEGER, value = JaegerTracing.class),
    @JsonSubTypes.Type(name = OpenTelemetryTracing.TYPE_OPENTELEMETRY, value = OpenTelemetryTracing.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class Tracing implements UnknownPropertyPreserving {
    private Map<String, Object> additionalProperties;

    @Description("Type of the tracing used. " +
            "Currently the only supported type is `opentelemetry` for OpenTelemetry tracing. " +
            "As of Strimzi 0.37.0, `jaeger` type is not supported anymore and this option is ignored.")
    public abstract String getType();

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
