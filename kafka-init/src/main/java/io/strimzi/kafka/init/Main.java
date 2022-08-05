/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Version;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {

        final String strimziVersion = Main.class.getPackage().getImplementationVersion();
        LOGGER.info("Init-kafka {} is starting", strimziVersion);
        InitWriterConfig config = InitWriterConfig.fromMap(System.getenv());

        final String userAgent = "fabric8-kubernetes-client/" + Version.clientVersion() 
                                + " strimzi-init-kafka/" + strimziVersion;
        final Config kubernetesClientConfig = new ConfigBuilder().withUserAgent(userAgent).build();
        KubernetesClient client = new KubernetesClientBuilder().withConfig(kubernetesClientConfig).build();

        LOGGER.info("Init-kafka started with config: {}", config);

        InitWriter writer = new InitWriter(client, config);

        if (config.getRackTopologyKey() != null) {
            if (!writer.writeRack()) {
                System.exit(1);
            }
        }

        if (config.isExternalAddress()) {
            if (!writer.writeExternalAddress()) {
                System.exit(1);
            }
        }

        client.close();
    }
}
