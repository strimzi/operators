/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.BasicItemStore;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.http.HealthCheckAndMetricsServer;
import io.strimzi.operator.common.http.Liveness;
import io.strimzi.operator.common.http.Readiness;
import org.apache.kafka.clients.admin.Admin;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Entrypoint for unidirectional TO.
 */
public class TopicOperatorMain implements Liveness, Readiness {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicOperatorMain.class);

    private final ResourceEventHandler<KafkaTopic> handler;
    private final String namespace;
    private final KubernetesClient client;
    /* test */ final BatchingLoop queue;
    private final long resyncIntervalMs;
    private final BasicItemStore<KafkaTopic> itemStore;
    /* test */ final BatchingTopicController controller;
    private final Admin admin;
    private SharedIndexInformer<KafkaTopic> informer; // guarded by this
    Thread shutdownHook; // guarded by this

    private final HealthCheckAndMetricsServer healthAndMetricsServer;

    TopicOperatorMain(
                      String namespace,
                      Map<String, String> selector,
                      Admin admin,
                      KubernetesClient client,
                      TopicOperatorConfig config) throws ExecutionException, InterruptedException {
        this.namespace = namespace;
        this.client = client;
        this.resyncIntervalMs = config.fullReconciliationIntervalMs();
        this.admin = admin;
        this.controller = new BatchingTopicController(selector, admin, client, config.useFinalizer());
        this.itemStore = new BasicItemStore<KafkaTopic>(Cache::metaNamespaceKeyFunc);
        this.queue = new BatchingLoop(config.maxQueueSize(),  controller, 1, config.maxBatchSize(), config.maxBatchLingerMs(), itemStore, this::stop);
        this.handler = new TopicOperatorEventHandler(queue, config.useFinalizer());
        this.healthAndMetricsServer = new HealthCheckAndMetricsServer(8080, this, this, null);
    }

    synchronized void start() {
        LOGGER.infoOp("Starting operator");
        if (shutdownHook != null) {
            throw new IllegalStateException();
        }

        shutdownHook = new Thread(this::shutdown, "TopicOperator-shutdown-hook");
        LOGGER.infoOp("Installing shutdown hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        LOGGER.infoOp("Starting health and metrics");
        healthAndMetricsServer.start();
        LOGGER.infoOp("Starting queue");
        queue.start();
        informer = Crds.topicOperation(client)
                .inNamespace(namespace)
                // Do NOT use withLabels to filter the informer, since the controller is stateful
                // (topics need to be added to removed from TopicController.topics if KafkaTopics transition between
                // selected and unselected).
                .runnableInformer(resyncIntervalMs)
                .addEventHandlerWithResyncPeriod(handler, resyncIntervalMs)
                .itemStore(itemStore);
        LOGGER.infoOp("Starting informer");
        informer.run();
    }

    synchronized void stop() {
        if (shutdownHook == null) {
            throw new IllegalStateException();
        }
        // shutdown(), will be be invoked indirectly by calling
        // h.run() has the side effect of nullifying this.shutdownHook
        // so retain a reference now so we have something to call
        // removeShutdownHook() with.
        var h = shutdownHook;
        // Call run (not start()) on the thread so that shutdown() is executed
        // on this thread.
        shutdown();
        // stop() is _not_ called from the shutdown hook, so calling
        // removeShutdownHook() should not cause IAE.
        Runtime.getRuntime().removeShutdownHook(h);
    }

    private synchronized void shutdown() {
        // Note: This method can be invoked on either via the shutdown hook thread or
        // on the thread(s) on which stop()/start() are called
        LOGGER.infoOp("Shutdown initiated");
        try {
            shutdownHook = null;
            if (informer != null) {
                informer.stop();
                informer = null;
            }
            this.queue.stop();
            this.admin.close();
            this.healthAndMetricsServer.stop();
            LOGGER.infoOp("Shutdown completed normally");
        } catch (InterruptedException e) {
            LOGGER.infoOp("Interrupted during shutdown");
            throw new RuntimeException(e);
        }
    }

    /**
     * Entrypoint for unidirectional TO.
     * @param args Command line args
     * @throws Exception If bad things happen
     */
    public static void main(String[] args) throws Exception {
        TopicOperatorConfig topicOperatorConfig = TopicOperatorConfig.buildFromMap(System.getenv());
        TopicOperatorMain operator = operator(topicOperatorConfig, kubeClient(), Admin.create(topicOperatorConfig.adminClientConfig()));
        operator.start();
        LOGGER.infoOp("Returning from main()");
    }

    static TopicOperatorMain operator(TopicOperatorConfig topicOperatorConfig, KubernetesClient client, Admin admin) throws ExecutionException, InterruptedException {
        return new TopicOperatorMain(topicOperatorConfig.namespace(), topicOperatorConfig.labelSelector().toMap(), admin, client, topicOperatorConfig);
    }

    static KubernetesClient kubeClient() {
        return new OperatorKubernetesClientBuilder(
                    "strimzi-topic-operator",
                    TopicOperatorMain.class.getPackage().getImplementationVersion())
                .build();
    }


    @Override
    public boolean isAlive() {
        boolean running;
        synchronized (this) {
            running = informer.isRunning();
        }
        if (!running) {
            LOGGER.infoOp("isAlive returning false because informer is not running");
            return false;
        } else {
            return queue.isAlive();
        }
    }

    @Override
    public boolean isReady() {
        boolean running;
        synchronized (this) {
            running = informer.isRunning();
        }
        if (!running) {
            LOGGER.infoOp("isReady returning false because informer is not running");
            return false;
        } else {
            return queue.isReady();
        }
    }
}
