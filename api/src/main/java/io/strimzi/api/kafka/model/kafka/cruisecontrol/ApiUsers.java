/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.api.kafka.model.kafka.cruisecontrol.HashLoginServiceApiUsers.TYPE_HASH_LOGIN_SERVICE;

/**
 * Abstract baseclass for different representations of Cruise Control's API users config, discriminated by {@link #getType() type}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = HashLoginServiceApiUsers.class, name = TYPE_HASH_LOGIN_SERVICE)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class ApiUsers implements UnknownPropertyPreserving {
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Type of the Cruise Control API users configuration. "
            + "Supported format is: " + "`" + TYPE_HASH_LOGIN_SERVICE + "`")
    public abstract String getType();

    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
