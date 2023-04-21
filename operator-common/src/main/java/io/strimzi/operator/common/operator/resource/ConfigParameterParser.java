/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;


import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.Properties;

import static java.util.Arrays.asList;

/**
 * Abstraction for things which convert a single configuration parameter value from a String to some specific type.
 */
public interface ConfigParameterParser<T> {

    /**
     * Parses the string based on its type
     *
     * @param configValue config value in String format
     * @throws InvalidConfigurationException if the given configuration value is not supported
     * @return the value based on its type
     */
    T parse(String configValue) throws InvalidConfigurationException;

    /**
     * A java string
     */
    ConfigParameterParser<String> STRING = configValue -> configValue;

    /**
     * A non empty java string
     */
    ConfigParameterParser<String> NON_EMPTY_STRING = configValue -> {
        if (configValue == null || configValue.isEmpty()) {
            throw new InvalidConfigurationException("Failed to parse. Value cannot be empty or null");
        } else {
            return configValue;
        }
    };

    /**
     * A semicolon-delimited list of strings.
     */
    ConfigParameterParser<List<String>> SEMICOLON_SEPARATED_LIST = configValue -> {
        List<String> windows = null;
        if (configValue != null && !configValue.isEmpty()) {
            windows = asList(configValue.split(";"));
        }
        return windows;
    };

    /**
     * Returns Properties based on its String format
     */
    ConfigParameterParser<Properties> PROPERTIES = configValue -> {

        Properties kafkaAdminClientConfiguration = new Properties();

        if (configValue != null) {
            try {
                kafkaAdminClientConfiguration.load(new StringReader(configValue));
            } catch (IOException | IllegalArgumentException e) {
                throw new InvalidConfigurationException("Failed to parse the configuration string " + configValue);
            }
        }

        return kafkaAdminClientConfiguration;
    };

    /**
     * A Java Long
     */
    ConfigParameterParser<Long> LONG = configValue -> {
        try {
            return Long.parseLong(configValue);
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Failed to parse. Value " + configValue + " is not valid");
        }
    };


    /**
     * A Java Duration
     */
    ConfigParameterParser<Duration> DURATION = configValue -> {
        try {
            return Duration.ofMillis(Long.parseLong(configValue));
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Failed to parse. Value " + configValue + " is not valid");
        }
    };

    /**
     * A Java Integer
     */
    ConfigParameterParser<Integer> INTEGER = configValue -> {
        try {
            return Integer.parseInt(configValue);
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Failed to parse. Value " + configValue + " is not valid", e);
        }
    };

    /**
     * Strictly Positive Number
     * @param parser ConfigParameterParser object
     * @param <T>    Type of parameter
     * @return Positive number
     */
    static <T extends Number> ConfigParameterParser<T> strictlyPositive(ConfigParameterParser<T> parser) {
        return configValue -> {
            var value = parser.parse(configValue);
            if (value.longValue() <= 0) {
                throw new InvalidConfigurationException("Failed to parse. Negative value is not supported for this configuration");
            }
            return value;
        };
    }

    /**
     * A Java Boolean
     */
    ConfigParameterParser<Boolean> BOOLEAN = configValue -> {
        if (configValue.equals("true") || configValue.equals("false")) {
            return Boolean.parseBoolean(configValue);
        } else {
            throw new InvalidConfigurationException("Failed to parse. Value " + configValue + " is not valid");
        }
    };

    /**
     * A kubernetes selector.
     */
    ConfigParameterParser<Labels> LABEL_PREDICATE = stringLabels -> {
        try {
            return Labels.fromString(stringLabels);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Failed to parse. Value " + stringLabels + " is not valid", e);
        }
    };

    /**
     * A kubernetes LocalObjectReference list
     */
    ConfigParameterParser<List<LocalObjectReference>> LOCAL_OBJECT_REFERENCE_LIST = imagePullSecretList -> {
        List<LocalObjectReference> imagePullSecrets = null;

        if (imagePullSecretList != null && !imagePullSecretList.isEmpty()) {
            if (imagePullSecretList.matches("(\\s*[a-z0-9.-]+\\s*,)*\\s*[a-z0-9.-]+\\s*")) {
                imagePullSecrets = Arrays.stream(imagePullSecretList.trim().split("\\s*,+\\s*")).map(secret -> new LocalObjectReferenceBuilder().withName(secret).build()).collect(Collectors.toList());
            } else {
                throw new InvalidConfigurationException("Not a valid list of secret names");
            }
        }
        return imagePullSecrets;
    };


    /**
     * Set of namespaces
     */
    ConfigParameterParser<Set<String>> NAMESPACE_SET = namespacesList -> {
        Set<String> namespaces;
        if (namespacesList.equals("*")) {
            namespaces = Collections.singleton(AbstractNamespacedResourceOperator.ANY_NAMESPACE);
        } else {
            if (namespacesList.trim().equals(AbstractNamespacedResourceOperator.ANY_NAMESPACE)) {
                namespaces = Collections.singleton(AbstractNamespacedResourceOperator.ANY_NAMESPACE);
            } else if (namespacesList.matches("(\\s*[a-z0-9.-]+\\s*,)*\\s*[a-z0-9.-]+\\s*")) {
                namespaces = new HashSet<>(asList(namespacesList.trim().split("\\s*,+\\s*")));
            } else {
                throw new InvalidConfigurationException("Not a valid list of namespaces nor the 'any namespace' wildcard "
                        + AbstractNamespacedResourceOperator.ANY_NAMESPACE);
            }
        }

        return namespaces;
    };
}

