/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SslAuthenticationException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * <p>Manages the rolling restart of a Kafka cluster.</p>
 *
 * <p>The following algorithm is used:</p>
 *
 * <pre>
 *   0. Start with a list of all the pods
 *   1. While the list is non-empty:
 *     2. Take the next pod from the list.
 *     3. Test whether the pod needs to be restarted.
 *         If not then:
 *           i.  Wait for it to be ready.
 *           ii. Continue from 1.
 *     4. Otherwise, check whether the pod is the controller
 *         If so, and there are still pods to be maybe-restarted then:
 *           i.  Reschedule the restart of this pod by appending it the list
 *           ii. Continue from 1.
 *     5. Otherwise, check whether the pod can be restarted without "impacting availability"
 *         If not then:
 *           i.  Reschedule the restart of this pod by appending it the list
 *           ii. Continue from 1.
 *     6. Otherwise:
 *         i.   Restart the pod
 *         ii.  Wait for it to become ready (in the kube sense)
 *         iii. Continue from 1.
 * </pre>
 *
 * <p>Where "impacting availability" is defined by {@link KafkaAvailability}.</p>
 *
 * <p>Note the following important properties of this algorithm:</p>
 * <ul>
 *     <li>if there is a spontaneous change in controller while the rolling restart is happening, any new
 *     controller is still the last pod to be rolled, thus avoid unnecessary controller elections.</li>
 *     <li>rolling should happen without impacting any topic's min.isr.</li>
 *     <li>even pods which aren't candidates for rolling are checked for readiness which partly avoids
 *     successive reconciliations each restarting a pod which never becomes ready</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ParameterNumber"})
public class KafkaRoller {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaRoller.class);
    private static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME = "controller.quorum.fetch.timeout.ms";
    private static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT = "2000";
    private final PodOperator podOperations;
    private final long pollingIntervalMs;
    protected final long operationTimeoutMs;
    protected final Vertx vertx;
    private final String cluster;
    private final TlsPemIdentity coTlsPemIdentity;
    private final Set<NodeRef> nodes;
    private final KubernetesRestartEventPublisher eventsPublisher;
    private final Supplier<BackOff> backoffSupplier;
    protected String namespace;
    private final AdminClientProvider adminClientProvider;
    private final KafkaAgentClientProvider kafkaAgentClientProvider;
    private final Function<Integer, String> kafkaConfigProvider;
    private final String kafkaLogging;
    private final KafkaVersion kafkaVersion;
    private final Reconciliation reconciliation;
    private final boolean allowReconfiguration;
    /**
     * Admin client used to send requests that are only relevant for the brokers. It is bootstrapped with broker nodes that might be rolled.
     */
    private Admin brokerAdminClient;
    /**
     * Admin client used to send requests that are only relevant for pure controllers. It is bootstrapped with controller nodes that might be rolled.
     */
    private Admin controllerAdminClient;
    private KafkaAgentClient kafkaAgentClient;

    /**
     * Constructor
     *
     * @param reconciliation            Reconciliation marker
     * @param vertx                     Vert.x instance
     * @param podOperations             Pod operator for managing pods
     * @param pollingIntervalMs         Polling interval in milliseconds
     * @param operationTimeoutMs        Operation timeout in milliseconds
     * @param backOffSupplier           Backoff supplier
     * @param nodes                     List of Kafka node references to consider rolling
     * @param coTlsPemIdentity          Trust set and identity for TLS client authentication for connecting to the Kafka cluster
     * @param adminClientProvider       Kafka Admin client provider
     * @param kafkaAgentClientProvider  Kafka Agent client provider
     * @param kafkaConfigProvider       Kafka configuration provider
     * @param kafkaLogging              Kafka logging configuration
     * @param kafkaVersion              Kafka version
     * @param allowReconfiguration      Flag indicting whether reconfiguration is allowed or not
     * @param eventsPublisher           Kubernetes Events publisher for publishing events about pod restarts
     */
    public KafkaRoller(Reconciliation reconciliation, Vertx vertx, PodOperator podOperations,
                       long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier, Set<NodeRef> nodes,
                       TlsPemIdentity coTlsPemIdentity, AdminClientProvider adminClientProvider, KafkaAgentClientProvider kafkaAgentClientProvider,
                       Function<Integer, String> kafkaConfigProvider, String kafkaLogging, KafkaVersion kafkaVersion, boolean allowReconfiguration, KubernetesRestartEventPublisher eventsPublisher) {
        this.namespace = reconciliation.namespace();
        this.cluster = reconciliation.name();
        this.nodes = nodes;
        this.eventsPublisher = eventsPublisher;
        if (nodes.size() != nodes.stream().distinct().count()) {
            throw new IllegalArgumentException();
        }
        this.backoffSupplier = backOffSupplier;
        this.coTlsPemIdentity = coTlsPemIdentity;
        this.vertx = vertx;
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
        this.pollingIntervalMs = pollingIntervalMs;
        this.adminClientProvider = adminClientProvider;
        this.kafkaAgentClientProvider = kafkaAgentClientProvider;
        this.kafkaConfigProvider = kafkaConfigProvider;
        this.kafkaLogging = kafkaLogging;
        this.kafkaVersion = kafkaVersion;
        this.reconciliation = reconciliation;
        this.allowReconfiguration = allowReconfiguration;
    }

    private final ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor(
        runnable -> new Thread(runnable, "kafka-roller"));

    private final ConcurrentHashMap<String, RestartContext> podToContext = new ConcurrentHashMap<>();
    private Function<Pod, RestartReasons> podNeedsRestart;

    /**
     * Initializes brokerAdminClient, if it has not been initialized yet
     * @return true if the creation of AC succeeded, false otherwise
     */
    private boolean maybeInitBrokerAdminClient() {
        if (this.brokerAdminClient == null) {
            try {
                this.brokerAdminClient = brokerAdminClient(nodes);
            } catch (ForceableProblem e) {
                LOGGER.warnCr(reconciliation, "Failed to create brokerAdminClient.", e);
                return false;
            }
        }
        return true;
    }

    /**
     * Initializes controllerAdminClient if it has not been initialized yet
     * @return true if the creation of AC succeeded, false otherwise
     */
    private boolean maybeInitControllerAdminClient() {
        if (this.controllerAdminClient == null) {
            try {
                this.controllerAdminClient = controllerAdminClient(nodes);
            } catch (ForceableProblem e) {
                LOGGER.warnCr(reconciliation, "Failed to create controllerAdminClient.", e);
                return false;
            }
        }
        return true;
    }

    /**
     * Asynchronously perform a rolling restart of some subset of the pods,
     * completing the returned Future when rolling is complete.
     * Which pods get rolled is determined by {@code podNeedsRestart}.
     * The pods may not be rolled in id order, due to the {@linkplain KafkaRoller rolling algorithm}.
     * @param podNeedsRestart Predicate for determining whether a pod should be rolled.
     * @return A Future completed when rolling is complete.
     */
    public Future<Void> rollingRestart(Function<Pod, RestartReasons> podNeedsRestart) {
        this.podNeedsRestart = podNeedsRestart;
        Promise<Void> result = Promise.promise();
        singleExecutor.submit(() -> {
            try {
                LOGGER.debugCr(reconciliation, "Verifying cluster pods are up-to-date.");
                List<NodeRef> controllerPods = new ArrayList<>();
                List<NodeRef> brokerPods = new ArrayList<>();

                for (NodeRef node : nodes) {
                    // Order the nodes unready first otherwise repeated reconciliations might each restart a pod
                    // only for it not to become ready and thus drive the cluster to a worse state.
                    // in KRaft mode roll unready controllers, then ready controllers, then unready brokers, then ready brokers
                    boolean isReady = podOperations.isReady(namespace, node.podName());

                    if (node.controller()) {
                        controllerPods.add(isReady ? controllerPods.size() : 0, node);
                    } else {
                        brokerPods.add(isReady ? brokerPods.size() : 0, node);
                    }
                }

                LOGGER.debugCr(reconciliation, "Initial order for updating pods (rolling restart or dynamic update) is controller pods={}, broker pods={}", controllerPods, brokerPods);

                List<Future<Void>> controllerFutures = new ArrayList<>(controllerPods.size());
                for (NodeRef node : controllerPods) {
                    controllerFutures.add(schedule(node, 0, TimeUnit.MILLISECONDS));
                }

                Future.join(controllerFutures).compose(v -> {
                    List<Future<Void>> brokerFutures = new ArrayList<>(nodes.size());
                    for (NodeRef broker : brokerPods) {
                        brokerFutures.add(schedule(broker, 0, TimeUnit.MILLISECONDS));
                    }
                    return Future.join(brokerFutures);
                }).onComplete(ar -> {
                    singleExecutor.shutdown();

                    try {
                        if (brokerAdminClient != null) {
                            brokerAdminClient.close(Duration.ofSeconds(30));
                        }
                    } catch (RuntimeException e) {
                        LOGGER.debugCr(reconciliation, "Exception closing broker admin client", e);
                    }

                    try {
                        if (controllerAdminClient != null) {
                            controllerAdminClient.close(Duration.ofSeconds(30));
                        }
                    } catch (RuntimeException e) {
                        LOGGER.debugCr(reconciliation, "Exception closing controller admin client", e);
                    }

                    vertx.runOnContext(ignored -> result.handle(ar.map((Void) null)));
                });
            } catch (Exception e)   {
                // If anything happens, we have to raise the error otherwise the reconciliation would get stuck
                // Its logged at upper level, so we just log it at debug here
                LOGGER.debugCr(reconciliation, "Something went wrong when trying to do a rolling restart", e);
                singleExecutor.shutdown();
                result.fail(e);
            }
        });
        return result.future();
    }

    protected static class RestartContext {
        final Promise<Void> promise;
        final BackOff backOff;
        RestartReasons restartReasons;
        private long connectionErrorStart = 0L;

        boolean needsRestart;
        boolean needsReconfig;
        boolean forceRestart;
        boolean podStuck;
        KafkaBrokerConfigurationDiff brokerConfigDiff;
        KafkaBrokerLoggingConfigurationDiff brokerLoggingDiff;

        RestartContext(Supplier<BackOff> backOffSupplier) {
            promise = Promise.promise();
            backOff = backOffSupplier.get();
            backOff.delayMs();
        }

        public void clearConnectionError() {
            connectionErrorStart = 0L;
        }

        long connectionError() {
            return connectionErrorStart;
        }

        void noteConnectionError() {
            if (connectionErrorStart == 0L) {
                connectionErrorStart = System.currentTimeMillis();
            }
        }

        @Override
        public String toString() {
            return "RestartContext{" +
                    "promise=" + promise +
                    ", backOff=" + backOff +
                    '}';
        }
    }

    /**
     * Schedule the rolling of the given pod at or after the given delay,
     * completed the returned Future when the pod is rolled.
     * When called multiple times with the same podId this method will return the same Future instance.
     * Pods will be rolled one-at-a-time so the delay may be overrun.
     *
     * @param nodeRef   The reference to pod to roll.
     * @param delay     The delay.
     * @param unit      The unit of the delay.
     *
     * @return A future which completes when the pod has been rolled.
     */
    private Future<Void> schedule(NodeRef nodeRef, long delay, TimeUnit unit) {
        RestartContext ctx = podToContext.computeIfAbsent(nodeRef.podName(),
            k -> new RestartContext(backoffSupplier));
        singleExecutor.schedule(() -> {
            LOGGER.debugCr(reconciliation, "Considering updating pod {} after a delay of {} {}", nodeRef, delay, unit);
            try {
                restartIfNecessary(nodeRef, ctx);
                ctx.promise.complete();
            } catch (InterruptedException e) {
                // Let the executor deal with interruption.
                Thread.currentThread().interrupt();
            } catch (FatalProblem e) {
                LOGGER.infoCr(reconciliation, "Could not reconcile {}, giving up without retrying because we encountered a fatal error",
                        nodeRef, e);
                ctx.promise.fail(e);
                singleExecutor.shutdownNow();
                podToContext.forEachValue(Integer.MAX_VALUE, f -> f.promise.tryFail(e));
            } catch (Exception e) {
                if (ctx.backOff.done()) {
                    LOGGER.infoCr(reconciliation, "Could not verify pod {} is up-to-date, giving up after {} attempts. Total delay between attempts {}ms",
                            nodeRef, ctx.backOff.maxAttempts(), ctx.backOff.totalDelayMs(), e);
                    ctx.promise.fail(e instanceof TimeoutException ?
                            new io.strimzi.operator.common.TimeoutException() :
                            e);
                } else {
                    long delay1 = ctx.backOff.delayMs();
                    LOGGER.infoCr(reconciliation, "Will temporarily skip verifying pod {} is up-to-date due to {}, retrying after at least {}ms",
                            nodeRef, e, delay1);
                    schedule(nodeRef, delay1, TimeUnit.MILLISECONDS);
                }
            }
        }, delay, unit);
        return ctx.promise.future();
    }

    /**
     * Restart the given pod now if necessary according to {@link #podNeedsRestart}.
     * This method blocks.
     *
     * @param nodeRef           Reference of pod to roll.
     * @param restartContext    Restart context
     *
     * @throws InterruptedException     Interrupted while waiting.
     * @throws ForceableProblem         Some error. Not thrown when one of restartContext.podStuck, restartContext.backOff.done()
     *                                  or exception.forceNow is true AND canRoll is true. Otherwise, is thrown.
     * @throws UnforceableProblem       Some error, always thrown.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    private void restartIfNecessary(NodeRef nodeRef, RestartContext restartContext)
            throws Exception {
        final Pod pod;
        try {
            pod = podOperations.get(namespace, nodeRef.podName());
            if (pod == null) {
                LOGGER.debugCr(reconciliation, "Pod {} doesn't exist. There seems to be some problem with the creation of pod by StrimziPodSets controller", nodeRef.podName());
                return;
            }
        } catch (KubernetesClientException e) {
            throw new UnforceableProblem("Error getting pod " + nodeRef.podName(), e);
        }

        restartContext.podStuck = isPodStuck(pod);
        if (!restartContext.podStuck) {
            // We want to give pods chance to get ready before we try to connect to the or consider them for rolling.
            // This is important especially for pods which were just started. But only in case when they are not stuck.
            // If the pod is stuck, it suggests that it is running already for some time and it will not become ready.
            // Waiting for it would likely just waste time.
            LOGGER.debugCr(reconciliation, "Waiting for pod {} to become ready before checking its state", nodeRef.podName());
            try {
                await(isReady(pod), operationTimeoutMs, TimeUnit.MILLISECONDS, RuntimeException::new);
            } catch (Exception e) {
                //Initialise the client for KafkaAgent if pod is not ready
                if (kafkaAgentClient == null) {
                    this.kafkaAgentClient = initKafkaAgentClient();
                }

                BrokerState brokerState = kafkaAgentClient.getBrokerState(pod.getMetadata().getName());
                if (brokerState.isBrokerInRecovery()) {
                    throw new UnforceableProblem("Pod " + nodeRef.podName() + " is not ready because the Kafka node is performing log recovery. There are " + brokerState.remainingLogsToRecover() + " logs and " + brokerState.remainingSegmentsToRecover() + " segments left to recover.", e.getCause());
                }

                if (e.getCause() instanceof TimeoutException) {
                    LOGGER.warnCr(reconciliation, "Pod {} is not ready. We will check if KafkaRoller can do anything about it.", nodeRef.podName());
                } else {
                    LOGGER.warnCr(reconciliation, "Failed to wait for the readiness of the pod {}. We will proceed and check if it needs to be rolled.", nodeRef.podName(), e.getCause());
                }
            }
        }

        restartContext.restartReasons = podNeedsRestart.apply(pod);

        // We try to detect the current roles. If we fail to do so, we optimistically assume the roles did not
        // change and the desired roles still apply.
        boolean isBroker = Labels.booleanLabel(pod, Labels.STRIMZI_BROKER_ROLE_LABEL, nodeRef.broker());
        boolean isController = Labels.booleanLabel(pod, Labels.STRIMZI_CONTROLLER_ROLE_LABEL, nodeRef.controller());

        try {
            checkIfRestartOrReconfigureRequired(nodeRef, isController, isBroker, restartContext);
            if (restartContext.forceRestart) {
                LOGGER.debugCr(reconciliation, "Pod {} can be rolled now", nodeRef);
                restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS, restartContext);
            } else if (restartContext.needsRestart || restartContext.needsReconfig) {
                if (deferController(nodeRef, isController, isBroker)) {
                    LOGGER.debugCr(reconciliation, "Pod {} is the active controller and there are other pods to verify first.", nodeRef);
                    throw new ForceableProblem("Pod " + nodeRef.podName() + " is the active controller and there are other pods to verify first");
                } else if (!canRoll(nodeRef, isController, isBroker, 60, TimeUnit.SECONDS, false, restartContext)) {
                    LOGGER.debugCr(reconciliation, "Pod {} cannot be updated right now", nodeRef);
                    throw new UnforceableProblem("Pod " + nodeRef.podName() + " cannot be updated right now.");
                } else {
                    // Check for rollability before trying a dynamic update so that if the dynamic update fails we can go to a full restart
                    if (!maybeDynamicUpdateBrokerConfig(nodeRef, restartContext)) {
                        LOGGER.infoCr(reconciliation, "Rolling Pod {} due to {}", nodeRef, restartContext.restartReasons.getAllReasonNotes());
                        restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS, restartContext);
                    } else {
                        awaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS);
                    }
                }
            } else {
                // By testing even pods which don't need needsRestart for readiness we prevent successive reconciliations
                // from taking out a pod each time (due, e.g. to a configuration error).
                // We rely on Kube to try restarting such pods.
                LOGGER.debugCr(reconciliation, "Pod {} does not need to be restarted", nodeRef);
                LOGGER.debugCr(reconciliation, "Waiting for non-restarted pod {} to become ready", nodeRef);
                await(isReady(namespace, nodeRef.podName()), operationTimeoutMs, TimeUnit.MILLISECONDS, e -> new FatalProblem("Error while waiting for non-restarted pod " + nodeRef.podName() + " to become ready", e));
                LOGGER.debugCr(reconciliation, "Pod {} is now ready", nodeRef);
            }
        } catch (ForceableProblem e) {
            if (restartContext.podStuck || restartContext.backOff.done() || e.forceNow) {

                if (canRoll(nodeRef, isController, isBroker, 60_000, TimeUnit.MILLISECONDS, true, restartContext)) {
                    String errorMsg = e.getMessage();

                    if (e.getCause() != null) {
                        errorMsg += ", caused by:" + (e.getCause().getMessage() != null ? e.getCause().getMessage() : e.getCause());
                    }

                    LOGGER.warnCr(reconciliation, "Pod {} will be force-rolled, due to error: {}", nodeRef, errorMsg);
                    restartContext.restartReasons.add(RestartReason.POD_FORCE_RESTART_ON_ERROR);
                    restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS, restartContext);
                } else {
                    LOGGER.warnCr(reconciliation, "Pod {} can't be safely force-rolled; original error: ", nodeRef, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    KafkaAgentClient initKafkaAgentClient() throws FatalProblem {
        try {
            return kafkaAgentClientProvider.createKafkaAgentClient(reconciliation, coTlsPemIdentity);
        } catch (Exception e) {
            throw new FatalProblem("Failed to initialise KafkaAgentClient", e);
        }
    }

    private boolean podWaitingBecauseOfAnyReasons(Pod pod, Set<String> reasons) {
        if (pod != null && pod.getStatus() != null) {
            Optional<ContainerStatus> kafkaContainerStatus = pod.getStatus().getContainerStatuses().stream()
                    .filter(containerStatus -> containerStatus.getName().equals("kafka")).findFirst();

            if (kafkaContainerStatus.isPresent()) {
                ContainerStateWaiting waiting = kafkaContainerStatus.get().getState().getWaiting();
                if (waiting != null) {
                    return reasons.contains(waiting.getReason());
                }
            }
        }
        return false;
    }

    private boolean isPendingAndUnschedulable(Pod pod) {
        return pod != null
                && pod.getStatus() != null
                && "Pending".equals(pod.getStatus().getPhase())
                && pod.getStatus().getConditions().stream().anyMatch(ps -> "PodScheduled".equals(ps.getType()) && "Unschedulable".equals(ps.getReason()) && "False".equals(ps.getStatus()));
    }

    private boolean isPodStuck(Pod pod) {
        Set<String> set = new HashSet<>();
        set.add("CrashLoopBackOff");
        set.add("ImagePullBackOff");
        set.add("ContainerCreating");
        return isPendingAndUnschedulable(pod) || podWaitingBecauseOfAnyReasons(pod, set);
    }

    /**
     * Dynamically update the broker config if the plan says we can.
     * Return true if the broker was successfully updated dynamically.
     */
    private boolean maybeDynamicUpdateBrokerConfig(NodeRef nodeRef, RestartContext restartContext) throws InterruptedException {
        boolean updatedDynamically;

        if (restartContext.needsReconfig) {
            try {
                dynamicUpdateBrokerConfig(nodeRef, brokerAdminClient, restartContext.brokerConfigDiff, restartContext.brokerLoggingDiff);
                updatedDynamically = true;
            } catch (ForceableProblem e) {
                LOGGER.debugCr(reconciliation, "Pod {} could not be updated dynamically ({}), will restart", nodeRef, e);
                updatedDynamically = false;
            }
        } else {
            updatedDynamically = false;
        }
        return updatedDynamically;
    }

    /**
     * Sets the specified {@code RestartContext} to indicate a forced restart is required.
     * Resets all flags and differences in the context, ensuring only a forced restart
     * is flagged as necessary.
     *
     * @param restartContext The {@code RestartContext} to be updated.
     */
    private void markRestartContextWithForceRestart(RestartContext restartContext) {
        restartContext.needsRestart = false;
        restartContext.needsReconfig = false;
        restartContext.forceRestart = true;
        restartContext.brokerConfigDiff = null;
        restartContext.brokerLoggingDiff = null;
    }

    /**
     * Determine whether the pod should be restarted, or the broker reconfigured.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void checkIfRestartOrReconfigureRequired(NodeRef nodeRef, boolean isController, boolean isBroker, RestartContext restartContext) throws ForceableProblem, InterruptedException, FatalProblem {
        RestartReasons reasonToRestartPod = restartContext.restartReasons;
        if (restartContext.podStuck && !reasonToRestartPod.contains(RestartReason.POD_HAS_OLD_REVISION)) {
            // If the pod is unschedulable then deleting it, or trying to open an Admin client to it will make no difference
            // Treat this as fatal because if it's not possible to schedule one pod then it's likely that proceeding
            // and deleting a different pod in the meantime will likely result in another unschedulable pod.
            throw new FatalProblem("Pod is unschedulable or is not starting");
        }

        if (restartContext.podStuck) {
            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it seems to be stuck and restart might help", nodeRef);
            restartContext.restartReasons.add(RestartReason.POD_STUCK);
            markRestartContextWithForceRestart(restartContext);
            return;
        }

        boolean needsRestart = reasonToRestartPod.shouldRestart();
        KafkaBrokerConfigurationDiff brokerConfigDiff = null;
        KafkaBrokerLoggingConfigurationDiff brokerLoggingDiff = null;
        boolean needsReconfig = false;

        // if it is a pure controller, initialise the admin client specifically for controllers
        boolean adminClientCreated = isController && !isBroker ? maybeInitControllerAdminClient() : maybeInitBrokerAdminClient();
        if (!adminClientCreated) {
            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it does not seem to responding to connection attempts", nodeRef);
            reasonToRestartPod.add(RestartReason.POD_UNRESPONSIVE);
            markRestartContextWithForceRestart(restartContext);
            return;
        }

        if (isBroker) {
            // Always get the broker config. This request gets sent to that specific broker, so it's a proof that we can
            // connect to the broker and that it's capable of responding.
            Config brokerConfig;
            try {
                brokerConfig = brokerConfig(nodeRef);
            } catch (ForceableProblem e) {
                if (restartContext.backOff.done()) {
                    needsRestart = true;
                    brokerConfig = null;
                } else {
                    throw e;
                }
            }

            if (!needsRestart && allowReconfiguration) {
                LOGGER.traceCr(reconciliation, "Pod {}: description {}", nodeRef, brokerConfig);
                brokerConfigDiff = new KafkaBrokerConfigurationDiff(reconciliation, brokerConfig, kafkaConfigProvider.apply(nodeRef.nodeId()), kafkaVersion, nodeRef);
                brokerLoggingDiff = logging(nodeRef);

                if (brokerConfigDiff.getDiffSize() > 0) {
                    if (brokerConfigDiff.canBeUpdatedDynamically()) {
                        LOGGER.debugCr(reconciliation, "Pod {} needs to be reconfigured.", nodeRef);
                        needsReconfig = true;
                    } else {
                        LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, dynamic update cannot be done.", nodeRef);
                        restartContext.restartReasons.add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                        needsRestart = true;
                    }
                }

                // needsRestart value might have changed from the check in the parent if. So we need to check it again.
                if (!needsRestart && brokerLoggingDiff.getDiffSize() > 0) {
                    LOGGER.debugCr(reconciliation, "Pod {} logging needs to be reconfigured.", nodeRef);
                    needsReconfig = true;
                }
            }
        }

        restartContext.needsRestart = needsRestart;
        restartContext.needsReconfig = needsReconfig;
        restartContext.forceRestart = false;
        restartContext.brokerConfigDiff = brokerConfigDiff;
        restartContext.brokerLoggingDiff = brokerLoggingDiff;
    }

    /**
     * Returns a config of the given broker.
     * @param nodeRef The reference of the broker.
     * @return a Future which completes with the config of the given broker.
     */
    /* test */ Config brokerConfig(NodeRef nodeRef) throws ForceableProblem, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(nodeRef.nodeId()));
        return await(VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, brokerAdminClient.describeConfigs(singletonList(resource)).values().get(resource)),
            30, TimeUnit.SECONDS,
            error -> new ForceableProblem("Error getting broker config", error)
        );
    }

    /**
     * Returns logging of the given broker.
     * @param brokerId The id of the broker.
     * @return a Future which completes with the logging of the given broker.
     */
    /* test */ Config brokerLogging(int brokerId) throws ForceableProblem, InterruptedException {
        ConfigResource resource = Util.getBrokersLogging(brokerId);
        return await(VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, brokerAdminClient.describeConfigs(singletonList(resource)).values().get(resource)),
                30, TimeUnit.SECONDS,
            error -> new ForceableProblem("Error getting broker logging", error)
        );
    }

    /* test */ void dynamicUpdateBrokerConfig(NodeRef nodeRef, Admin ac, KafkaBrokerConfigurationDiff configurationDiff, KafkaBrokerLoggingConfigurationDiff logDiff)
            throws ForceableProblem, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> updatedConfig = new HashMap<>(2);
        var podId = nodeRef.nodeId();
        updatedConfig.put(Util.getBrokersConfig(podId), configurationDiff.getConfigDiff());
        updatedConfig.put(Util.getBrokersLogging(podId), logDiff.getLoggingDiff());

        LOGGER.debugCr(reconciliation, "Updating broker configuration {}", nodeRef);
        LOGGER.traceCr(reconciliation, "Updating broker configuration {} with {}", nodeRef, updatedConfig);

        AlterConfigsResult alterConfigResult = ac.incrementalAlterConfigs(updatedConfig);
        KafkaFuture<Void> brokerConfigFuture = alterConfigResult.values().get(Util.getBrokersConfig(podId));
        KafkaFuture<Void> brokerLoggingConfigFuture = alterConfigResult.values().get(Util.getBrokersLogging(podId));
        await(VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, brokerConfigFuture), 30, TimeUnit.SECONDS,
            error -> {
                LOGGER.errorCr(reconciliation, "Error updating broker configuration for pod {}", nodeRef, error);
                return new ForceableProblem("Error updating broker configuration for pod " + nodeRef, error);
            });
        await(VertxUtil.kafkaFutureToVertxFuture(reconciliation, vertx, brokerLoggingConfigFuture), 30, TimeUnit.SECONDS,
            error -> {
                LOGGER.errorCr(reconciliation, "Error updating broker logging configuration pod {}", nodeRef, error);
                return new ForceableProblem("Error updating broker logging configuration pod " + nodeRef, error);
            });

        LOGGER.infoCr(reconciliation, "Dynamic update of pod {} was successful.", nodeRef);
    }

    private KafkaBrokerLoggingConfigurationDiff logging(NodeRef nodeRef)
            throws ForceableProblem, InterruptedException {
        Config brokerLogging = brokerLogging(nodeRef.nodeId());
        LOGGER.traceCr(reconciliation, "Pod {}: logging description {}", nodeRef, brokerLogging);
        return new KafkaBrokerLoggingConfigurationDiff(reconciliation, brokerLogging, kafkaLogging);
    }

    /** Exceptions which we're prepared to ignore (thus forcing a restart) in some circumstances. */
    static final class ForceableProblem extends Exception {
        final boolean forceNow;
        ForceableProblem(String msg) {
            this(msg, null);
        }

        ForceableProblem(String msg, Throwable cause) {
            this(msg, cause, false);
        }

        ForceableProblem(String msg, Throwable cause, boolean forceNow) {
            super(msg, cause);
            this.forceNow = forceNow;
        }

        @Override
        public String toString() {
            /*
             * This is a static nested class, so we want to prevent the Outer$Nested
             * name returned by getSimpleName()
             */
            var name = "ForceableProblem";
            var message = getMessage();
            return (message != null) ? (name + ": " + message) : name;
        }
    }

    /** Exceptions which we're prepared to ignore in the final attempt */
    static final class UnforceableProblem extends Exception {
        UnforceableProblem(String msg) {
            this(msg, null);
        }
        UnforceableProblem(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /** Immediately aborts rolling */
    static final class FatalProblem extends Exception {
        public FatalProblem(String message) {
            super(message);
        }

        FatalProblem(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    private boolean canRoll(NodeRef nodeRef, boolean isController, boolean isBroker, long timeout, TimeUnit unit, boolean ignoreSslError, RestartContext restartContext)
            throws Exception {
        try {
            if (isBroker && isController) {
                //No need to discover the active controller first when it is a combined node, because brokers forward describeQuorum requests to the active controller
                boolean canRollController = await(quorumCheck(brokerAdminClient, nodeRef).canRollController(nodeRef.nodeId()), timeout, unit,
                        t -> new UnforceableProblem("An error while trying to determine the possibility of updating Kafka controller pods", t));
                boolean canRollBroker = await(availability(brokerAdminClient).canRoll(nodeRef.nodeId()), timeout, unit,
                        t -> new ForceableProblem("An error while trying to determine the possibility of updating Kafka broker pods", t));

                return canRollController && canRollBroker;
            } else if (isController) {
                return canRollController(nodeRef, timeout, unit);
            } else {
                return await(availability(brokerAdminClient).canRoll(nodeRef.nodeId()), timeout, unit,
                        t -> new ForceableProblem("An error while trying to determine the possibility of updating Kafka broker pods", t));
            }
        } catch (ForceableProblem | UnforceableProblem e) {
            // If we're not able to connect then roll
            if (ignoreSslError && e.getCause() instanceof SslAuthenticationException) {
                restartContext.restartReasons.add(RestartReason.POD_UNRESPONSIVE);
                return true;
            } else {
                throw e;
            }
        }
    }

    private boolean canRollController(NodeRef nodeRef, long timeout, TimeUnit unit) throws UnforceableProblem, InterruptedException, ForceableProblem {
        // Discover the active controller first so that the admin client can be bootstrapped only with the active controller.
        // This is because quorum info can be requested only from the active controller.
        // TODO: Once KAFKA-18230 is completed and supported in Strimzi, we can remove this method,
        //       and simply use the regular controllerAdminClient for the quorum check
        int activeController = getActiveController(nodeRef, controllerAdminClient, operationTimeoutMs, TimeUnit.MILLISECONDS);
        if (activeController < 0) {
            // TODO: if the active controller is returned as -1 (maybe election was still in progress),
            //      use the admin client bootstrapped with all controller nodes to see if it can hit the active controller by luck.
            //      If it does not hit the active controller, it will return an error and this node will be retried later anyway.
            //      Otherwise we can just return false here, and the node will be then retried later.
            return await(quorumCheck(controllerAdminClient, nodeRef).canRollController(nodeRef.nodeId()), timeout, unit,
                    t -> new UnforceableProblem("An error while trying to determine the possibility of updating Kafka controller pods", t));
        }

        Admin activeControllerAdmin = null;
        try {
            activeControllerAdmin = controllerAdminClient(nodes.stream().filter(node -> (node.nodeId() == activeController)).collect(Collectors.toSet()));
            return await(quorumCheck(activeControllerAdmin, nodeRef).canRollController(nodeRef.nodeId()), timeout, unit,
                    t -> new UnforceableProblem("An error while trying to determine the possibility of updating Kafka controller pods", t));
        } finally {
            try {
                if (activeControllerAdmin != null) {
                    activeControllerAdmin.close(Duration.ofSeconds(30));
                }
            } catch (RuntimeException e) {
                LOGGER.debugCr(reconciliation, "Exception closing controller admin client", e);

            }
        }
    }

    /**
     * Synchronously restart the given pod
     * by deleting it and letting it be recreated by K8s, then synchronously wait for it to be ready.
     *
     * @param pod               The Pod to restart.
     * @param timeout           The timeout.
     * @param unit              The timeout unit.
     * @param restartContext    Restart context
     */
    private void restartAndAwaitReadiness(Pod pod, long timeout, TimeUnit unit, RestartContext restartContext)
            throws InterruptedException, UnforceableProblem, FatalProblem {
        String podName = pod.getMetadata().getName();
        LOGGER.debugCr(reconciliation, "Rolling pod {}", podName);
        await(restart(pod, restartContext), timeout, unit, e -> new UnforceableProblem("Error while trying to restart pod " + podName + " to become ready", e));
        awaitReadiness(pod, timeout, unit);
    }

    private void awaitReadiness(Pod pod, long timeout, TimeUnit unit) throws FatalProblem, InterruptedException {
        String podName = pod.getMetadata().getName();
        LOGGER.debugCr(reconciliation, "Waiting for restarted pod {} to become ready", podName);
        await(isReady(pod), timeout, unit, e -> new FatalProblem("Error while waiting for restarted pod " + podName + " to become ready", e));
        LOGGER.debugCr(reconciliation, "Pod {} is now ready", podName);
    }

    /**
     * Block waiting for up to the given timeout for the given Future to complete, returning its result.
     * @param future The future to wait for.
     * @param timeout The timeout
     * @param unit The timeout unit
     * @param exceptionMapper A function for rethrowing exceptions.
     * @param <T> The result type
     * @param <E> The exception type
     * @return The result of the future
     * @throws E The exception type returned from {@code exceptionMapper}.
     * @throws TimeoutException If the given future is not completed before the timeout.
     * @throws InterruptedException If the waiting was interrupted.
     */
    private static <T, E extends Exception> T await(Future<T> future, long timeout, TimeUnit unit,
                                            Function<Throwable, E> exceptionMapper)
            throws E, InterruptedException {
        CompletableFuture<T> cf = new CompletableFuture<>();
        future.onComplete(ar -> {
            if (ar.succeeded()) {
                cf.complete(ar.result());
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        try {
            return cf.get(timeout, unit);
        } catch (ExecutionException e) {
            throw exceptionMapper.apply(e.getCause());
        } catch (TimeoutException e) {
            throw exceptionMapper.apply(e);
        }
    }

    /**
     * Asynchronously delete the given pod, return a Future which completes when the Pod has been recreated.
     * Note: The pod might not be "ready" when the returned Future completes.
     *
     * @param pod               The pod to be restarted
     * @param restartContext    Restart context
     *
     * @return a Future which completes when the Pod has been recreated
     */
    protected Future<Void> restart(Pod pod, RestartContext restartContext) {
        return podOperations.restart(reconciliation, pod, operationTimeoutMs)
                             .onComplete(i -> vertx.executeBlocking(() -> {
                                 eventsPublisher.publishRestartEvents(pod, restartContext.restartReasons);
                                 return null;
                             }));
    }

    /**
     * Returns an AdminClient instance bootstrapped from the given broker nodes.
     */
    /* test */ Admin brokerAdminClient(Set<NodeRef> nodes) throws ForceableProblem {
        String bootstrapHostnames = nodes.stream().filter(NodeRef::broker).map(node -> DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), node.podName()) + ":" + KafkaCluster.REPLICATION_PORT).collect(Collectors.joining(","));

        try {
            LOGGER.debugCr(reconciliation, "Creating AdminClient for {}", bootstrapHostnames);
            return adminClientProvider.createAdminClient(bootstrapHostnames, coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity());
        } catch (RuntimeException e) {
            throw new ForceableProblem("An error while try to create an admin client with bootstrap brokers " + bootstrapHostnames, e);
        }
    }

    /**
     * Returns an AdminClient instance bootstrapped from the given controller nodes.
     */
    /* test */ Admin controllerAdminClient(Set<NodeRef> nodes) throws ForceableProblem {
        String bootstrapHostnames = nodes.stream().filter(NodeRef::controller).map(node -> DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), node.podName()) + ":" + KafkaCluster.CONTROLPLANE_PORT).collect(Collectors.joining(","));

        try {
            LOGGER.debugCr(reconciliation, "Creating AdminClient for {}", bootstrapHostnames);
            return adminClientProvider.createControllerAdminClient(bootstrapHostnames, coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity());
        } catch (RuntimeException e) {
            throw new ForceableProblem("An error while try to create an admin client with bootstrap controllers " + bootstrapHostnames, e);
        }
    }

    /* test */ KafkaQuorumCheck quorumCheck(Admin ac, NodeRef nodeRef) {
        String controllerQuorumFetchTimeout = CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT;
        String desiredConfig = kafkaConfigProvider.apply(nodeRef.nodeId());

        if (desiredConfig != null) {
            OrderedProperties orderedProperties = new OrderedProperties();
            controllerQuorumFetchTimeout = orderedProperties.addStringPairs(desiredConfig).asMap().getOrDefault(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME, CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT);
        }
        return new KafkaQuorumCheck(reconciliation, ac, vertx, Long.parseLong(controllerQuorumFetchTimeout));
    }

    /* test */ KafkaAvailability availability(Admin ac) {
        return new KafkaAvailability(reconciliation, ac);
    }
    
    /**
     * Return true if the given {@code nodeId} is the active controller and there are other brokers we might yet have to consider.
     * This ensures that the active controller is restarted/reconfigured last.
     * If node role is not controller, then return false as this check does not apply to brokers.
     */
    private boolean deferController(NodeRef nodeRef, boolean isController, boolean isBroker) throws Exception {
        if (!isController) {
            return false;
        }

        int controller = getActiveController(nodeRef, isBroker ? brokerAdminClient : controllerAdminClient, operationTimeoutMs, TimeUnit.MILLISECONDS);
        if (controller == nodeRef.nodeId()) {
            int stillRunning = podToContext.reduceValuesToInt(100, v -> v.promise.future().isComplete() ? 0 : 1,
                    0, Integer::sum);
            return stillRunning > 1;
        } else {
            return false;
        }
    }

    /**
     * Completes the returned future <strong>on the context thread</strong> with the id of the active controller.
     * This will be -1 if there is not currently a controller.
     *
     * @return A future which completes the node id of the controller of the cluster, or -1 if there is not currently a controller.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE") // seems to be completely spurious
    int getActiveController(NodeRef nodeRef, Admin ac, long timeout, TimeUnit unit) throws ForceableProblem, InterruptedException {
        int id;
        Node activeControllerNode;
        // TODO: Previously when discovering the controller, we created an admin client against the single given node,
        //       the reason seems to be so that we can check the connection to it via tcp probe.
        //       I think the purpose of this method should be just to get the active controller, rather than
        //       having an additional purpose of checking connection. Plus, this method is now only called for a controller node
        //       as we don't need to check the active controller when rolling brokers. That means tcp probe is now done only
        //       for a controller node. The tcpProbe for an individual node perhaps should be done earlier in checkIfRestartOrReconfigureRequired()
        //       as this method's purpose is to determine the reason for restart and it already does similar type of checks.

        try {
            DescribeClusterResult describeClusterResult = ac.describeCluster();
            KafkaFuture<Node> controller = describeClusterResult.controller();
            activeControllerNode = controller.get(timeout, unit);
        } catch (ExecutionException | TimeoutException e) {
            throw new ForceableProblem("Error while trying to determine the active controller to roll pod " + nodeRef.podName(), e.getCause());
        }
        
        id = activeControllerNode == null || Node.noNode().equals(activeControllerNode) ? -1 : activeControllerNode.id();
        LOGGER.debugCr(reconciliation, "Active controller is {} ", id);
        return id;
    }

    /**
     * If we've already had trouble connecting to this node try to probe whether the connection is
     * open on the node; if it's not then maybe throw a ForceableProblem to immediately force a restart.
     * This is an optimization for nodes which don't seem to be running.
     */
    private void maybeTcpProbe(NodeRef nodeRef, Exception executionException, boolean isController, RestartContext restartContext) throws Exception {
        int port = isController ? KafkaCluster.CONTROLPLANE_PORT : KafkaCluster.REPLICATION_PORT;
        if (restartContext.connectionError() + nodes.size() * 120_000L >= System.currentTimeMillis()) {
            try {
                LOGGER.debugCr(reconciliation, "Probing TCP port due to previous problems connecting to pod {}", nodeRef);
                // do a tcp connect and close (with a short connect timeout)
                tcpProbe(DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), nodeRef.podName()), port);
            } catch (IOException connectionException) {
                throw new ForceableProblem("Unable to connect to " + nodeRef.podName() + ":" + port, executionException.getCause(), true);
            }
        } else {
            restartContext.noteConnectionError();
            throw new ForceableProblem("Error while trying to determine the active controller from pod " + nodeRef.podName(), executionException.getCause());
        }
    }

    /**
     * Tries to open and close a TCP connection to the given host and port.
     * @param hostname The host
     * @param port The port
     * @throws IOException if anything went wrong.
     */
    /*test*/ void tcpProbe(String hostname, int port) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hostname, port), 5_000);
        }
    }

    @Override
    public String toString() {
        return podToContext.toString();
    }

    protected Future<Void> isReady(Pod pod) {
        return isReady(pod.getMetadata().getNamespace(), pod.getMetadata().getName());
    }

    protected Future<Void> isReady(String namespace, String podName) {
        return podOperations.readiness(reconciliation, namespace, podName, pollingIntervalMs, operationTimeoutMs)
            .recover(error -> {
                LOGGER.warnCr(reconciliation, "Error waiting for pod {}/{} to become ready: {}", namespace, podName, error);
                return Future.failedFuture(error);
            });
    }
}

