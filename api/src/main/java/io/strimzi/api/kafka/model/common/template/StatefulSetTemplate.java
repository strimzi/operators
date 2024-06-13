/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Representation of a template for Strimzi resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"metadata", "podManagementPolicy"})
@EqualsAndHashCode
@ToString
public class StatefulSetTemplate implements HasMetadataTemplate, UnknownPropertyPreserving {
    private MetadataTemplate metadata;
    private PodManagementPolicy podManagementPolicy;
    private Map<String, Object> additionalProperties;

    @Description("Metadata applied to the resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public MetadataTemplate getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataTemplate metadata) {
        this.metadata = metadata;
    }

    @Description("PodManagementPolicy which will be used for this StatefulSet. " +
            "Valid values are `Parallel` and `OrderedReady`. " +
            "Defaults to `Parallel`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodManagementPolicy getPodManagementPolicy() {
        return podManagementPolicy;
    }

    public void setPodManagementPolicy(PodManagementPolicy podManagementPolicy) {
        this.podManagementPolicy = podManagementPolicy;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
