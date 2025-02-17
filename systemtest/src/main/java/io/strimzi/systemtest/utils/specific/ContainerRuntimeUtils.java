/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.TestConstants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ContainerRuntimeUtils {
    private static final String RUNTIME = detectRuntime();

    private static String detectRuntime() {
        if (isCommandAvailable(TestConstants.PODMAN)) {
            return TestConstants.PODMAN;
        } else if (isCommandAvailable(TestConstants.DOCKER)) {
            return TestConstants.DOCKER;
        } else {
            throw new IllegalStateException("Neither Podman nor Docker is available on the system.");
        }
    }

    private static boolean isCommandAvailable(String command) {
        try {
            Process process = new ProcessBuilder("which", command).start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                final String output = reader.readLine();
                return output != null && !output.isEmpty();
            }
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Returns the container runtime detected on the system.
     *
     * @return A string indicating the container runtime, either "podman" or "docker".
     * @throws IllegalStateException if neither Podman nor Docker is available.
     */
    public static String getRuntime() {
        return RUNTIME;
    }
}
