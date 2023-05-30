/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides the default shared environment provider.
 */
public class DefaultSharedEnvironmentProvider implements SharedEnvironmentProvider {
    private Map<String, EnvVar> envVarMap;

    /**
     * Creates the shared environment provider.
     */
    public DefaultSharedEnvironmentProvider() {
        Map<String, EnvVar> envVarMap = new HashMap<>();
        for (String name : EnvVarName.names()) {
            if (System.getenv(name) != null) {
                envVarMap.put(name, new EnvVarBuilder()
                    .withName(name)
                    .withValue(System.getenv(name))
                    .build());
            }
        }
        this.envVarMap = Collections.unmodifiableMap(envVarMap);
    }

    @Override
    public Collection<EnvVar> variables() {
        return Collections.unmodifiableCollection(envVarMap.values());
    }

    @Override
    public String value(String name) {
        return name != null && envVarMap.get(name) != null
            ? envVarMap.get(name).getValue() : null;
    }
}
