/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Version;
import io.strimzi.api.kafka.Crds;
import io.vertx.core.Vertx;

import java.util.HashMap;
import java.util.Map;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The entry-point to the topic operator.
 * Main responsibility is to deploy a {@link Session} with an appropriate Config and KubeClient,
 * redeploying if the config changes.
 */
public class Main {

    private final static Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("TopicOperator {} is starting", Main.class.getPackage().getImplementationVersion());
        Main main = new Main();
        main.run();
    }

    public void run() {
        Map<String, String> m = new HashMap<>(System.getenv());
        m.keySet().retainAll(Config.keyNames());
        Config config = new Config(m);
        deploy(config);
    }

    private void deploy(Config config) {
        final String strimziVersion = Main.class.getPackage().getImplementationVersion();
        final String userAgent = "fabric8-kubernetes-client/" + Version.clientVersion() 
        + " strimzi-operator-topic/" + strimziVersion;
        final io.fabric8.kubernetes.client.Config kubernetesClientConfig = new ConfigBuilder().withUserAgent(userAgent).build();
        KubernetesClient kubeClient = new KubernetesClientBuilder().withConfig(kubernetesClientConfig).build();
        Crds.registerCustomKinds();
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setJvmMetricsEnabled(true)
                        .setEnabled(true));
        Vertx vertx = Vertx.vertx(options);

        Session session = new Session(kubeClient, config);
        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Session deployed");
            } else {
                LOGGER.error("Error deploying Session", ar.cause());
            }
        });
    }
}
