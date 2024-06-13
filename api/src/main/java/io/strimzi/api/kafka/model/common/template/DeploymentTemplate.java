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
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Representation of a template for Strimzi resources.
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"metadata", "deploymentStrategy"})
@EqualsAndHashCode
@ToString
public class DeploymentTemplate implements HasMetadataTemplate, UnknownPropertyPreserving {
    private MetadataTemplate metadata;
    private DeploymentStrategy deploymentStrategy;
    private Map<String, Object> additionalProperties;

    @Description("Metadata applied to the resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public MetadataTemplate getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataTemplate metadata) {
        this.metadata = metadata;
    }

    @Description("Pod replacement strategy for deployment configuration changes. " +
            "Valid values are `RollingUpdate` and `Recreate`. " +
            "Defaults to `RollingUpdate`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public DeploymentStrategy getDeploymentStrategy() {
        return deploymentStrategy;
    }

    public void setDeploymentStrategy(DeploymentStrategy deploymentStrategy) {
        this.deploymentStrategy = deploymentStrategy;
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
