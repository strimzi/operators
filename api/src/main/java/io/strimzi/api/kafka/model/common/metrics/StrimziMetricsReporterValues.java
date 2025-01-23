/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Strimzi Metrics Reporter configuration.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"allowList"})
@EqualsAndHashCode()
@ToString
public class StrimziMetricsReporterValues implements UnknownPropertyPreserving {
    private static final String DEFAULT_REGEX = ".*";

    private List<String> allowList = List.of(DEFAULT_REGEX);
    private Map<String, Object> additionalProperties;

    @Description("A comma separated list of regex patterns to specify the metrics to collect. Default: `" + DEFAULT_REGEX + "`.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<String> getAllowList() {
        return allowList;
    }

    public void setAllowList(List<String> allowList) {
        this.allowList = allowList;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}