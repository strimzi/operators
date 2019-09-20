/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import io.strimzi.crdgenerator.annotations.Description;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Represent the Quotas configuration for Kafka User
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaUserQuotas implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Integer producerByteRate;
    private Integer consumerByteRate;
    private Integer requestPercentage;

    private Map<String, Object> additionalProperties;

    @Description("Producer byte rate")
    public Integer getProducerByteRate() {
        return producerByteRate;
    }

    public void setProducerByteRate(Integer producerByteRate) {
        this.producerByteRate = producerByteRate;
    }

    @Description("Consumer byte rate")
    public Integer getConsumerByteRate() {
        return consumerByteRate;
    }

    public void setConsumerByteRate(Integer consumerByteRate) {
        this.consumerByteRate = consumerByteRate;
    }

    @Description("Request percentage")
    public Integer getRequestPercentage() {
        return requestPercentage;
    }

    public void setRequestPercentage(Integer requestPercentage) {
        this.requestPercentage = requestPercentage;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
