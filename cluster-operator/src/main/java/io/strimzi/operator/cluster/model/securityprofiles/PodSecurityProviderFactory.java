/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.platform.PlatformFeatures;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;
import io.strimzi.plugin.security.profiles.impl.BaselinePodSecurityProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ServiceLoader;
import java.util.stream.Collectors;

public class PodSecurityProviderFactory {
    private static final Logger LOGGER = LogManager.getLogger(PodSecurityProviderFactory.class);

    // The default value is set for Unit Tests which might be run without the factory being initialized
    private static PodSecurityProvider provider;
    static {
        provider = new BaselinePodSecurityProvider();
        provider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));
    }

    /* test */ static PodSecurityProvider findProviderOrThrow(String providerClass)   {
        ServiceLoader<PodSecurityProvider> loader = ServiceLoader.load(PodSecurityProvider.class);

        for (PodSecurityProvider provider : loader)  {
            if (providerClass.equals(provider.getClass().getCanonicalName()))   {
                LOGGER.info("Found PodSecurityProvider {}", providerClass);
                return provider;
            }
        }

        // The provider was not found
        LOGGER.warn("PodSecurityProvider {} was not found. Available providers are {}", providerClass, loader.stream().map(p -> p.getClass().getCanonicalName()).collect(Collectors.toSet()));
        throw new InvalidConfigurationException("PodSecurityProvider " + providerClass + " was not found.");
    }

    public static void initialize(String providerClass, PlatformFeatures platformFeatures) {
        PodSecurityProviderFactory.provider = findProviderOrThrow(providerClass);
        LOGGER.info("Initializing PodSecurityProvider {}", providerClass);
        PodSecurityProviderFactory.provider.configure(platformFeatures);
    }

    public static PodSecurityProvider getProvider()  {
        return provider;
    }
}
