/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.UncheckedExecutionException;
import io.strimzi.operator.common.UncheckedInterruptedException;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RackRolling
 */
@SuppressWarnings({"ParameterNumber" })
public class RackRolling {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(RackRolling.class);
    private static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME = "controller.quorum.fetch.timeout.ms";
    private static final long CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT = 2000L;
    private final List<Context> contexts;

    enum Action {
        // Used for brokers that are initially healthy and require neither restart no reconfigure
        NOP,
        // Used in for nodes that are not healthy and require neither restart
        WAIT_FOR_READINESS,
        WAIT_FOR_LOG_RECOVERY,
        // Used in {@link #initialPlan(List, RollClient)} for nodes that require reconfigure
        // before we know whether the actual config changes are reconfigurable
        MAYBE_RECONFIGURE,
        // Used in {@link #refinePlanForReconfigurability(Reconciliation, KafkaVersion, Function, String, RollClient, Map)}
        // once we know a MAYBE_RECONFIGURE node can actually be reconfigured
        RECONFIGURE,
        RESTART_UNHEALTHY,
        RESTART,
    }

    /**
     * Constructs RackRolling instance and initializes contexts for given {@code nodes}
     * to do a rolling restart (or reconfigure) of them.
     *
     * @param podOperator               Pod operator for managing pods
     * @param nodes                     The nodes (not all of which may need restarting).
     * @param reconciliation            Reconciliation marker
     * @param predicate                 The predicate used to determine whether to restart a particular node
     * @param coTlsPemIdentity          Cluster operator PEM identity
     * @param adminClientProvider       Kafka Admin client provider
     * @param kafkaAgentClientProvider  Kafka Agent client provider
     * @param kafkaVersion              Kafka version
     * @param allowReconfiguration      Flag indicting whether reconfiguration is allowed or not
     * @param kafkaConfigProvider       Kafka configuration provider
     * @param kafkaLogging              Kafka logging configuration
     * @param postOperationTimeoutMs    The maximum time in milliseconds to wait after a restart or reconfigure
     * @param maxRestartBatchSize       The maximum number of nodes that might be restarted at once
     * @param maxRestarts               The maximum number of restart that can be done for a node
     * @param maxReconfigs              The maximum number of reconfiguration that can be done for a node
     * @param maxAttempts               The maximum number to operational attempt that can be done for a node
     * @param eventPublisher            Kubernetes Events publisher for publishing events about node restarts
     * @return RackRolling instance
     */
    public static RackRolling rollingRestart(PodOperator podOperator,
                                             Collection<NodeRef> nodes,
                                             Reconciliation reconciliation,
                                             Function<Integer, RestartReasons> predicate,
                                             TlsPemIdentity coTlsPemIdentity,
                                             AdminClientProvider adminClientProvider,
                                             KafkaAgentClientProvider kafkaAgentClientProvider,
                                             Function<Integer, String> kafkaConfigProvider,
                                             boolean allowReconfiguration,
                                             KafkaVersion kafkaVersion,
                                             String kafkaLogging,
                                             long postOperationTimeoutMs,
                                             int maxRestartBatchSize,
                                             int maxRestarts,
                                             int maxReconfigs,
                                             int maxAttempts,
                                             KubernetesRestartEventPublisher eventPublisher) {
        PlatformClient platformClient = new PlatformClientImpl(podOperator, reconciliation.namespace(), reconciliation, postOperationTimeoutMs, eventPublisher);
        Time time = Time.SYSTEM_TIME;
        final var contexts = nodes.stream().map(node -> Context.start(node, platformClient.nodeRoles(node), predicate, time)).collect(Collectors.toList());

        RollClient rollClient = new RollClientImpl(reconciliation, coTlsPemIdentity, adminClientProvider);
        AgentClient agentClient = new AgentClientImpl(kafkaAgentClientProvider.createKafkaAgentClient(reconciliation, coTlsPemIdentity));

        return new RackRolling(time,
                platformClient,
                rollClient,
                agentClient,
                reconciliation,
                kafkaVersion,
                allowReconfiguration,
                kafkaConfigProvider,
                kafkaLogging,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                maxRestarts,
                maxReconfigs,
                maxAttempts,
                contexts);
    }

    // visible for testing
    protected static RackRolling rollingRestart(Time time,
                                                PlatformClient platformClient,
                                                RollClient rollClient,
                                                AgentClient agentClient,
                                                Collection<NodeRef> nodes,
                                                Function<Integer, RestartReasons> predicate,
                                                Reconciliation reconciliation,
                                                KafkaVersion kafkaVersion,
                                                boolean allowReconfiguration,
                                                Function<Integer, String> kafkaConfigProvider,
                                                String desiredLogging,
                                                long postOperationTimeoutMs,
                                                int maxRestartBatchSize,
                                                int maxRestarts,
                                                int maxReconfigs,
                                                int maxAttempts) {
        final var contexts = nodes.stream().map(node -> Context.start(node, platformClient.nodeRoles(node), predicate, time)).collect(Collectors.toList());

        return new RackRolling(time,
                platformClient,
                rollClient,
                agentClient,
                reconciliation,
                kafkaVersion,
                allowReconfiguration,
                kafkaConfigProvider,
                desiredLogging,
                postOperationTimeoutMs,
                maxRestartBatchSize,
                maxRestarts,
                maxReconfigs,
                maxAttempts,
                contexts);
    }

    private final Time time;
    private final PlatformClient platformClient;
    private final RollClient rollClient;
    private final AgentClient agentClient;
    private final Reconciliation reconciliation;
    private final KafkaVersion kafkaVersion;
    private final boolean allowReconfiguration;
    private final Function<Integer, String> kafkaConfigProvider;
    private final String desiredLogging;
    private final long postOperationTimeoutMs;
    private final int maxRestartBatchSize;
    private final int maxRestarts;
    private final int maxReconfigs;
    private final int maxAttempts;

    /**
     * Constructor for RackRolling instance
     * @param time                      initial time to set for context
     * @param platformClient            client for platform calls
     * @param rollClient               client for kafka cluster admin calls
     * @param agentClient               client for kafka agent calls
     * @param reconciliation            Reconciliation marker
     * @param kafkaVersion              Kafka version
     * @param allowReconfiguration      Flag indicting whether reconfiguration is allowed or not
     * @param kafkaConfigProvider       Kafka configuration provider
     * @param desiredLogging            Kafka logging configuration
     * @param postOperationTimeoutMs    The maximum time in milliseconds to wait after a restart or reconfigure
     * @param maxRestartBatchSize       The maximum number of nodes that might be restarted at once
     * @param maxRestarts               The maximum number of restart that can be done for a node
     * @param maxReconfigs              The maximum number of reconfiguration that can be done for a node
     * @param maxAttempts               The maximum number to operational attempt that can be done for a node
     * @param contexts                  List of context for each node
     */
    public RackRolling(Time time,
                       PlatformClient platformClient,
                       RollClient rollClient,
                       AgentClient agentClient,
                       Reconciliation reconciliation,
                       KafkaVersion kafkaVersion,
                       boolean allowReconfiguration,
                       Function<java.lang.Integer, String> kafkaConfigProvider,
                       String desiredLogging,
                       long postOperationTimeoutMs,
                       int maxRestartBatchSize,
                       int maxRestarts,
                       int maxReconfigs,
                       int maxAttempts,
                       List<Context> contexts) {
        this.time = time;
        this.platformClient = platformClient;
        this.rollClient = rollClient;
        this.agentClient = agentClient;
        this.reconciliation = reconciliation;
        this.kafkaVersion = kafkaVersion;
        this.kafkaConfigProvider = kafkaConfigProvider;
        this.desiredLogging = desiredLogging;
        this.postOperationTimeoutMs = postOperationTimeoutMs;
        this.maxRestartBatchSize = maxRestartBatchSize;
        this.maxRestarts = maxRestarts;
        this.maxReconfigs = maxReconfigs;
        this.contexts = contexts;
        this.maxAttempts = maxAttempts;
        this.allowReconfiguration = allowReconfiguration;
    }

    /**
     * Runs the roller via single thread Executor
     *
     * @param vertx Vertx instance
     * @return a future based on the rolling outcome.
     */
    public Future<Void> executeRollingAsync(
            Vertx vertx) {

        Promise<Void> result = Promise.promise();
        var singleExecutor = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "kafka-roller"));
        try {
            singleExecutor.submit(() -> {
                try {
                    executeRolling();
                    vertx.runOnContext(ig -> result.complete());
                } catch (Exception e) {
                    LOGGER.debugCr(reconciliation, "Something went wrong when trying to do a rolling restart", e);
                    vertx.runOnContext(ig -> result.fail(e));
                }
            });
        } finally {
            try {
                System.out.println("Closing controller admin client");
                rollClient.closeControllerAdminClient();
            } catch (RuntimeException e) {
                LOGGER.debugCr(reconciliation, "Exception closing controller admin client", e);
            }

            try {
                System.out.println("Closing broker admin client");
                rollClient.closeBrokerAdminClient();
            } catch (RuntimeException e) {
                LOGGER.debugCr(reconciliation, "Exception closing broker admin client", e);
            }

            singleExecutor.shutdown();
        }
        return result.future();
    }

    private void executeRolling() throws InterruptedException, ExecutionException {
        List<java.lang.Integer> nodesToRestart;
        do {
            nodesToRestart = loop();
        } while (!nodesToRestart.isEmpty());
    }

    /**
     * Process each context to determine which nodes need restarting.
     * Nodes that are not ready (in the Kubernetes sense) will always be considered for restart before any others.
     * The given {@code predicate} will be called for each of the remaining nodes and those for which the function returns a non-empty
     * list of reasons will be restarted.
     *
     * The expected worst case execution time of this function is approximately
     * {@code (timeoutMs * maxRestarts + postOperationTimeoutMs) * size(nodes)}.
     * This is reached when:
     * <ol>
     *     <li>We initially attempt to reconfigure the nodes that have configuration changes</li>
     *     <li>If reconfigurations fail after {@code maxReconfigs}, so we resort to restarts</li>
     *     <li>We require {@code maxRestarts} restarts for each node, and each restart uses the
     *         maximum {@code timeoutMs}.</li>
     * </ol>
     *
     * If a broker node is restarted by this method (because the {@code predicate} function returned empty), then
     * it will be elected as a leader of all its preferred replicas if it's not leading them yet.
     * However, if failed to lead them within a certain time, this will result in a warning log and moving onto the next node.
     *
     * This method is executed repeatedly until there is no nodes left to restart or reconfigure or max attempt is reached for any node.
     * If this method completes normally then all initially unready nodes and the nodes for which the {@code predicate} function returned
     * a non-empty list of reasons (which may be no nodes) will have been successfully restarted and
     * nodes that have configurations changed will have been reconfigured.
     * In other words, successful return from this method indicates that all nodes seem to be up and
     * "functioning normally".
     * If a node fails to become ready after a restart (e.g. recovering its logs) within a certain time, it will be retried for a restart or wait
     * until the maximum restart or maximum attempt has reached.
     *
     * If the maximum restart reached for any node, this method will throw MaxRestartsExceededException.
     * If the maximum attempt reached for any node, this method will throw MaxAttemptsExceededException.
     * If any node is not running but has an up-to-date revision, this method will throw UnrestartableNodesException.
     *
     * @return list of nodes to retry
     * @throws InterruptedException UncheckedInterruptionException  The thread was interrupted
     * @throws ExecutionException UncheckedExecutionException Execution exception from clients
     **/
    public List<Integer> loop() throws InterruptedException, ExecutionException {
        try {
            // Observe current state and update the contexts
            for (var context : contexts) {
                context.transitionTo(observe(reconciliation, platformClient, agentClient, context.nodeRef()), time);
            }

            // We want to give nodes chance to get ready before we try to connect to the or consider them for rolling.
            // This is important especially for nodes which were just started.
            waitForNodeReadiness(contexts.stream().filter(context -> context.state().equals(State.NOT_READY)).collect(Collectors.toList()),
                    (c, e) -> { });

            var byPlan = initialPlan(contexts, rollClient);
            LOGGER.debugCr(reconciliation, "Initial plan: {}", byPlan.entrySet().stream().map(plan -> String.format("\n %s=%s", plan.getKey(), plan.getValue())).collect(Collectors.toSet()));

            if (!byPlan.getOrDefault(Action.WAIT_FOR_LOG_RECOVERY, List.of()).isEmpty()) {
                return waitForLogRecovery(byPlan.get(Action.WAIT_FOR_LOG_RECOVERY));
            }

            // Restart any initially unready nodes
            if (!byPlan.getOrDefault(Action.RESTART_UNHEALTHY, List.of()).isEmpty()) {
                return restartUnhealthyNodes(byPlan.get(Action.RESTART_UNHEALTHY));
            }

            if (!byPlan.getOrDefault(Action.WAIT_FOR_READINESS, List.of()).isEmpty()) {
                return waitForNodeReadiness(byPlan.get(Action.WAIT_FOR_READINESS));
            }

            rollClient.initialiseControllerAdmin(contexts.stream().filter(c -> c.currentRoles().controller()).map(Context::nodeRef).collect(Collectors.toSet()));
            rollClient.initialiseBrokerAdmin(contexts.stream().filter(c -> c.currentRoles().broker()).map(Context::nodeRef).collect(Collectors.toSet()));

            // Refine the plan, reassigning nodes under MAYBE_RECONFIGURE to either RECONFIGURE or RESTART
            // based on whether they have only reconfiguration config changes
            List<Context> maybeConfigureNodes = byPlan.getOrDefault(Action.MAYBE_RECONFIGURE, List.of());
            if (allowReconfiguration && !maybeConfigureNodes.isEmpty()) {
                var nodeConfigs = getNodeConfigs(rollClient, maxAttempts, maybeConfigureNodes);
                if (nodeConfigs == null) {
                    return maybeConfigureNodes.stream().map(Context::nodeId).toList();
                }

                byPlan = refinePlanForReconfigurability(reconciliation,
                        kafkaVersion,
                        kafkaConfigProvider,
                        desiredLogging,
                        maybeConfigureNodes,
                        nodeConfigs,
                        byPlan);
                LOGGER.debugCr(reconciliation, "Refined plan: {}", byPlan.entrySet().stream().map(plan -> String.format("\n %s=%s", plan.getKey(), plan.getValue())).collect(Collectors.toSet()));
            }

            // Reconfigure any reconfigurable nodes
            if (!byPlan.getOrDefault(Action.RECONFIGURE, List.of()).isEmpty()) {
                return reconfigureNodes(byPlan.get(Action.RECONFIGURE));
            }

            // If we get this far then all remaining nodes require a restart
            if (!byPlan.getOrDefault(Action.RESTART, List.of()).isEmpty()) {
                return restartNodes(byPlan.get(Action.RESTART));
            }

            if (byPlan.getOrDefault(Action.NOP, List.of()).size() == contexts.size()) {
                LOGGER.debugCr(reconciliation, "Reconciliation completed successfully: All nodes are ready after restart");
                return List.of();
            }

            return contexts.stream()
                    .filter(c -> !c.state().equals(State.READY))
                    .map(c -> c.nodeRef().nodeId())
                    .collect(Collectors.toList());
        } catch (UncheckedInterruptedException e) {
            throw e.getCause();
        } catch (UncheckedExecutionException e) {
            throw e.getCause();
        }
    }

    /**
     * Makes observations of server of the given context, and return the corresponding state.
     * @param nodeRef The node
     * @return The state
     */
    private static State observe(Reconciliation reconciliation, PlatformClient platformClient, AgentClient agentClient, NodeRef nodeRef) {
        State state;
        var nodeState = platformClient.nodeState(nodeRef);
        LOGGER.debugCr(reconciliation, "Node {}: nodeState is {}", nodeRef, nodeState);
        switch (nodeState) {
            case NOT_RUNNING:
                state = State.NOT_RUNNING;
                break;
            case READY:
                state = State.READY;
                break;
            case NOT_READY:
            default:
                try {
                    var bs = agentClient.getBrokerState(nodeRef);
                    LOGGER.debugCr(reconciliation, "Node {}: brokerState is {}", nodeRef, bs);
                    if (bs.value() >= BrokerState.RUNNING.value() && bs.value() != BrokerState.UNKNOWN.value()) {
                        state = State.READY;
                    } else if (bs.value() == BrokerState.RECOVERY.value()) {
                        LOGGER.warnCr(reconciliation, "Node {} is in log recovery. There are {} logs and {} segments left to recover", nodeRef.nodeId(), bs.remainingLogsToRecover(), bs.remainingSegmentsToRecover());
                        state = State.RECOVERING;
                    } else {
                        state = State.NOT_READY;
                    }
                } catch (Exception e) {
                    LOGGER.warnCr(reconciliation, "Could not get broker state for node {}. This might be temporary if a node was just restarted", nodeRef, e.getCause());
                    state = State.NOT_READY;
                }
        }
        LOGGER.debugCr(reconciliation, "Node {}: observation outcome is {}", nodeRef, state);
        return state;
    }

    private List<Integer> waitForNodeReadiness(List<Context> contexts, BiConsumer<Context, TimeoutException> timeoutHandler) {
        long remainingTimeoutMs = postOperationTimeoutMs;
        for (Context context : contexts) {
            try {
                remainingTimeoutMs = awaitState(reconciliation, time, platformClient, agentClient, context, State.READY, remainingTimeoutMs);
            } catch (TimeoutException e) {
                timeoutHandler.accept(context, e);
            }
        }
        return contexts.stream().map(Context::nodeId).collect(Collectors.toList());
    }

    private static long awaitState(Reconciliation reconciliation,
                                   Time time,
                                   PlatformClient platformClient,
                                   AgentClient agentClient,
                                   Context context,
                                   State targetState,
                                   long timeoutMs) throws TimeoutException {
        LOGGER.debugCr(reconciliation, "Node {}: Waiting for node to enter state {}", context, targetState);
        return Alarm.timer(
                time,
                timeoutMs,
                () -> "Failed to reach " + targetState + " within " + timeoutMs + " ms: " + context
        ).poll(1_000, () -> {
            var state = context.transitionTo(observe(reconciliation, platformClient, agentClient, context.nodeRef()), time);
            return state == targetState;
        });
    }

    private Map<Action, List<Context>> initialPlan(List<Context> contexts, RollClient rollClient) {
        return contexts.stream().collect(Collectors.groupingBy(context -> {
            if (context.state() == State.NOT_RUNNING) {
                LOGGER.debugCr(reconciliation, "{} is in {} state therefore may get restarted first", context.nodeRef(), context.state());
                return Action.RESTART_UNHEALTHY;

            } else if (context.state() == State.RECOVERING) {
                LOGGER.debugCr(reconciliation, "{} is in log recovery therefore will not be restarted", context.nodeRef());
                return Action.WAIT_FOR_LOG_RECOVERY;

            } else if (!rollClient.canConnectToNode(context.nodeRef(), context.currentRoles().controller())) {
                LOGGER.debugCr(reconciliation, "{} will be restarted because it does not seem to responding to connection attempt", context.nodeRef());
                context.reason().add(RestartReason.POD_UNRESPONSIVE);
                return Action.RESTART_UNHEALTHY;

            } else {
                var reasons = context.reason();
                if (reasons.getReasons().isEmpty()) {
                    //TODO: When controller node can be reconfigure, always return Action.MAYBE_RECONFIGURE when node state is ready.
                    return context.state().equals(State.READY) ?
                            context.currentRoles().controller() && !context.currentRoles().broker() ? Action.NOP : Action.MAYBE_RECONFIGURE
                            : Action.WAIT_FOR_READINESS;
                }

                if (context.numRestarts() > 0) {
                    return context.state().equals(State.READY) ? Action.NOP : Action.WAIT_FOR_READINESS;
                }

                return Action.RESTART;
            }
        }));
    }

    private List<Integer> waitForLogRecovery(List<Context> contexts) {
        return waitForNodeReadiness(contexts, (c, e) -> {
            var brokerState = agentClient.getBrokerState(c.nodeRef());
            LOGGER.debugCr(reconciliation, "Node {} is still in log recovery. There are {} logs and {} segments left to recover.", c.nodeRef(), brokerState.remainingLogsToRecover(), brokerState.remainingSegmentsToRecover());
            if (c.numAttempts() >= maxAttempts) {
                throw new MaxAttemptsExceededException("The max attempts (" + maxAttempts + ") to wait for this node "  +  c.nodeRef() + " to finish performing log recovery has been reached. " +
                        "There are " + brokerState.remainingLogsToRecover() + " logs and " + brokerState.remainingSegmentsToRecover() + " segments left to recover.");
            }
            c.incrementNumAttempts();
        });
    }

    private List<Integer> restartUnhealthyNodes(List<Context> contexts) {
        Set<Context> notRunningControllers = new HashSet<>();
        Set<Context> pureControllerNodesToRestart = new HashSet<>();
        Set<Context> combinedNodesToRestart = new HashSet<>();

        for (var c : contexts) {
            if (c.state() == State.NOT_RUNNING) {
                if (!c.reason().contains(RestartReason.POD_HAS_OLD_REVISION)) {
                    // If the node is not running (e.g. unschedulable) then restarting it, likely won't make any difference.
                    // Proceeding and deleting another node may result in it not running too. Avoid restarting it unless it has an old revision.
                    throw new UnrestartableNodesException("Pod is unschedulable or is not starting");
                }

                // Collect all not running controllers to restart them in parallel
                if (c.currentRoles().controller()) {
                    notRunningControllers.add(c);
                    continue;
                }
            }

            if (c.currentRoles().controller()) {
                if (c.currentRoles().broker()) {
                    combinedNodesToRestart.add(c);
                } else {
                    // we always restart a pure controller first, so if we have any, we exit the loop here
                    pureControllerNodesToRestart.add(c);
                    break;
                }
            }
        }

        if (!notRunningControllers.isEmpty()) {
            LOGGER.debugCr(reconciliation, "There are multiple controllers {} that are not running, which runs a risk of losing the quorum. Restarting them in parallel", notRunningControllers);
            restartInParallel(reconciliation, time, platformClient, rollClient, agentClient, notRunningControllers, postOperationTimeoutMs, maxRestarts);
            return notRunningControllers.stream().map(Context::nodeId).collect(Collectors.toList());
        }

        // restart in the following order: pure controllers, combined nodes and brokers
        Context nodeToRestart = !pureControllerNodesToRestart.isEmpty() ? pureControllerNodesToRestart.iterator().next()
                : !combinedNodesToRestart.isEmpty() ? combinedNodesToRestart.iterator().next()
                : contexts.get(0);

        restartInParallel(reconciliation, time, platformClient, rollClient, agentClient, Collections.singleton(nodeToRestart), postOperationTimeoutMs, maxRestarts);
        return Collections.singletonList(nodeToRestart.nodeId());
    }

    private void restartInParallel(Reconciliation reconciliation,
                                   Time time,
                                   PlatformClient platformClient,
                                   RollClient rollClient,
                                   AgentClient agentClient,
                                   Set<Context> batch,
                                   long timeoutMs,
                                   int maxRestarts) {
        for (Context context : batch) {
            restartNode(reconciliation, time, platformClient, context, maxRestarts);
        }
        //TODO: Apply post restart delay here
        long remainingTimeoutMs = timeoutMs;
        for (Context context : batch) {
            try {
                remainingTimeoutMs = awaitState(reconciliation, time, platformClient, agentClient, context, State.READY, remainingTimeoutMs);
                if (context.currentRoles().broker()) {
                    awaitPreferred(reconciliation, time, rollClient, context, remainingTimeoutMs);
                }
            } catch (TimeoutException e) {
                LOGGER.warnCr(reconciliation, "Timed out waiting for node {} to become ready after a restart", context.nodeRef());
                if (context.numAttempts() >= maxAttempts) {
                    throw new MaxAttemptsExceededException("Cannot restart node " + context.nodeRef() +
                            " because they violate quorum health or topic availability. " +
                            "The max attempts (" + maxAttempts + ") to retry the nodes has been reached.");
                } else {
                    context.incrementNumAttempts();
                    return;
                }
            }
        }
    }

    private static void restartNode(Reconciliation reconciliation,
                                    Time time,
                                    PlatformClient platformClient,
                                    Context context,
                                    int maxRestarts) {
        if (context.numRestarts() >= maxRestarts) {
            throw new MaxRestartsExceededException("Node " + context.nodeRef() + " has been restarted " + maxRestarts + " times");
        }
        LOGGER.debugCr(reconciliation, "Node {}: Restarting", context.nodeRef());
        try {
            platformClient.restartNode(context.nodeRef(), context.reason());
        } catch (RuntimeException e) {
            LOGGER.warnCr(reconciliation, "An exception thrown during the restart of the node {}", context.nodeRef(), e);
        }
        context.incrementNumRestarts();
        context.transitionTo(State.UNKNOWN, time);
        LOGGER.debugCr(reconciliation, "Node {}: Restarted", context.nodeRef());
    }

    private static void awaitPreferred(Reconciliation reconciliation,
                                       Time time,
                                       RollClient rollClient,
                                       Context context,
                                       long timeoutMs) {
        // TODO: apply configured delay (via env variable) before triggering leader election.
        //  This should be probably passed to tryElectAllPreferredLeaders so that delay is only applied
        //  if there are topic partitions to elect, otherwise no point of delaying the process
        time.sleep(10000L, 0);
        LOGGER.debugCr(reconciliation, "Node {}: Waiting for node to be leader of all its preferred replicas", context);
        try {
            Alarm.timer(time,
                            timeoutMs,
                            () -> "Failed to elect the preferred leader " + context + " for topic partitions within " + timeoutMs)
                    .poll(1_000, () -> rollClient.tryElectAllPreferredLeaders(context.nodeRef()) == 0);
        } catch (TimeoutException e) {
            LOGGER.warnCr(reconciliation, "Timed out waiting for node to be leader for all its preferred replicas");
        }
    }

    private List<Integer> waitForNodeReadiness(List<Context> unreadyNodes) {
        if (!unreadyNodes.isEmpty()) {
            LOGGER.debugCr(reconciliation, "Waiting for nodes {} to become ready before initialising plan in case they just started", unreadyNodes);
            waitForNodeReadiness(unreadyNodes, (c, e) -> {
                if (c.numAttempts() >= maxAttempts) {
                    String restartedState = c.numRestarts() > 0 ? "restarted" : "non-restarted";
                    throw new MaxAttemptsExceededException("The max attempts (" + maxAttempts + ") to wait for " + restartedState + " node "  +  c.nodeRef() + " to become ready has been reached.");
                }
                c.incrementNumAttempts();
            });
        }
        return unreadyNodes.stream().map(Context::nodeId).collect(Collectors.toList());
    }

    private Map<Integer, Configs> getNodeConfigs(RollClient rollClient, int maxAttempts, List<Context> contexts) {
        //TODO: deal with controller node config here
        Map<Integer, Configs> nodeConfigs = null;
        try {
            nodeConfigs = rollClient.describeBrokerConfigs(contexts.stream()
                    .map(Context::nodeRef).toList());
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Error getting configs for : " + contexts, e.getCause());
            contexts.forEach(c -> {
                if (c.numAttempts() < maxAttempts) {
                    c.incrementNumAttempts();
                } else {
                    c.reason().add(RestartReason.POD_FORCE_RESTART_ON_ERROR);
                }
            });
        }

        return nodeConfigs;
    }

    private static Map<Action, List<Context>> refinePlanForReconfigurability(Reconciliation reconciliation,
                                                                             KafkaVersion kafkaVersion,
                                                                             Function<Integer, String> kafkaConfigProvider,
                                                                             String desiredLogging,
                                                                             List<Context> contexts,
                                                                             Map<Integer, Configs> nodeConfigs,
                                                                             Map<Action, List<Context>> byPlan) {
        var refinedPlan = contexts.stream().collect(Collectors.groupingBy(context -> {
            Configs configPair = nodeConfigs.get(context.nodeId());

            var diff = new KafkaBrokerConfigurationDiff(reconciliation,
                    configPair.nodeConfigs(),
                    kafkaConfigProvider.apply(context.nodeId()),
                    kafkaVersion,
                    context.nodeRef());
            context.brokerConfigDiff(diff);

            var loggingDiff = new KafkaBrokerLoggingConfigurationDiff(reconciliation, configPair.nodeLoggerConfigs(), desiredLogging);
            context.loggingDiff(loggingDiff);

            if (!diff.isEmpty()) {
                if (diff.canBeUpdatedDynamically() || !loggingDiff.isEmpty()) {
                    return Action.RECONFIGURE;
                } else {
                    context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                    return Action.RESTART;
                }
            }
            return Action.NOP;
        }));

        return Map.of(
                Action.RESTART, Stream.concat(byPlan.getOrDefault(Action.RESTART, List.of()).stream(), refinedPlan.getOrDefault(Action.RESTART, List.of()).stream()).toList(),
                Action.RECONFIGURE, refinedPlan.getOrDefault(Action.RECONFIGURE, List.of()),
                Action.NOP, Stream.concat(byPlan.getOrDefault(Action.NOP, List.of()).stream(), refinedPlan.getOrDefault(Action.NOP, List.of()).stream()).toList()
        );
    }

    private List<Integer> reconfigureNodes(List<Context> contexts) {
        //TODO: reconfigure controller node
        List<Integer> reconfiguredNode = new ArrayList<>();
        for (var context : contexts) {
            if (context.numReconfigs() >= maxReconfigs) {
                LOGGER.warnCr(reconciliation, "The maximum number of configuration attempt reached for node {}, will be restarted.", context.nodeRef());
                context.reason().add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                return List.of(context.nodeId());
            }

            try {
                reconfigureNode(reconciliation, time, rollClient, context);
            } catch (Exception e) {
                LOGGER.warnCr(reconciliation, "Failed to reconfigure {} due to {}", context.nodeRef(), e);
                context.incrementNumReconfigs();
                return List.of(context.nodeId());
            }

            time.sleep(postOperationTimeoutMs / 2, 0);
            waitForNodeReadiness(Collections.singletonList(context));
            // TODO decide whether we need an explicit healthcheck here
            //      or at least to know that the kube health check probe will have failed at the time
            //      we break to OUTER (We need to test a scenario of breaking configuration change, does this sleep catch it?)
            reconfiguredNode.add(context.nodeId());
            break;
        }
        return reconfiguredNode;
    }

    private static void reconfigureNode(Reconciliation reconciliation,
                                        Time time,
                                        RollClient rollClient,
                                        Context context) {
        LOGGER.debugCr(reconciliation, "Node {}: Reconfiguring", context.nodeRef());
        rollClient.reconfigureNode(context.nodeRef(), context.brokerConfigDiff(), context.loggingDiff());
        context.incrementNumReconfigs();
        context.transitionTo(State.UNKNOWN, time);
        LOGGER.debugCr(reconciliation, "Node {}: Reconfigured", context.nodeRef());
    }

    private List<Integer> restartNodes(List<Context> nodesToRestart) {
        // determine batches of nodes to be restarted together
        // for controller nodes, a batch with a single node will be returned
        var batch = nextBatch(reconciliation, rollClient, nodesToRestart, maxRestartBatchSize);

        // Empty batch means, there is no node that can safely restarted without violating quorum health or availability.
        if (batch.isEmpty()) {
            // check if the maxAttempt for any of the nodes has reached
            nodesToRestart.forEach(c -> {
                if (c.numAttempts() >= maxAttempts) {
                    throw new MaxAttemptsExceededException("Cannot restart nodes " + nodesToRestart.stream().map(Context::nodeRef).toList() +
                            " because they violate quorum health or topic availability. " +
                            "The max attempts (" + maxAttempts + ") to retry the nodes has been reached.");
                }
                c.incrementNumAttempts();
            });
            // sleep and retry the nodes
            time.sleep(postOperationTimeoutMs, 0);
            return nodesToRestart.stream().map(Context::nodeId).collect(Collectors.toList());
        }

        LOGGER.debugCr(reconciliation, "Restart batch: {}", batch);
        // restart a batch
        restartInParallel(reconciliation, time, platformClient, rollClient, agentClient, batch, postOperationTimeoutMs, maxRestarts);
        return batch.stream().map(Context::nodeId).collect(Collectors.toList());
    }

    /**
     * Figures out a batch of nodes that can be restarted together.
     * This method enforces the following roll order:
     * <ol>
     *     <li>Pure controller</li>
     *     <li>Combined node</li>
     *     <li>Active controller</li>
     *     <li>Broker (only this case is parallelizable)</li>
     * </ol>
     *
     * @param rollClient The roll client
     * @param nodesToRestart The ids of the nodes which need to be restarted
     * @param maxRestartBatchSize The maximum allowed size for a batch
     * @return The nodes corresponding to a subset of {@code nodeIdsNeedingRestart} that can safely be rolled together
     */
    private Set<Context> nextBatch(Reconciliation reconciliation,
                                   RollClient rollClient,
                                   List<Context> nodesToRestart,
                                   int maxRestartBatchSize) {

        var controllersToRestart = nodesToRestart.stream().filter(c -> c.currentRoles().controller()).collect(Collectors.toList());
        if  (!controllersToRestart.isEmpty()) {
            return nextControllerToRestart(reconciliation, rollClient, controllersToRestart);
        }

        return nextBatchBrokers(reconciliation, rollClient, nodesToRestart, maxRestartBatchSize);
    }

    /**
     * @param reconciliation The roll client
     * @param controllersToStart controllers to restarts
     * @return The first one from the given list of nodes that can be restarted without impacting the quorum health.
     * If there is no node that doesn't have an impact on the quorum health, an empty set is returned.
     */
    private Set<Context> nextControllerToRestart(Reconciliation reconciliation, RollClient rollClient,
                                                 List<Context> controllersToStart) {

        int activeControllerId = rollClient.activeController();
        LOGGER.debugCr(reconciliation, "The active controller is {}", activeControllerId);

        if (activeControllerId < 0) {
            // if we can't determine the active controller, we cannot safely restart a controller node
            // To retry, we return an empty set
            return Collections.emptySet();
        }

        var orderedNodes = controllersToStart.stream().sorted(Comparator.comparing((Context c) -> c.state().equals(State.READY)) // Sort by the state (ready goes to the back)
                .thenComparing(c -> c.currentRoles().broker()));  // Sort by the roles (combined goes to the back)))
        LOGGER.debugCr(reconciliation, "Checking controllers in the following order to restart: {}", controllersToStart);

        Set<Context> nextNodeToRestart = new HashSet<>();
        orderedNodes.anyMatch(c -> {
            if (c.nodeId() == activeControllerId && controllersToStart.size() != 1) {
                LOGGER.debugCr(reconciliation, "Controller node {} is the active controller, there are other controller nodes to restart", c.nodeId());
                return false;
            }

            if (isQuorumHealthyWithoutNode(reconciliation, c.nodeId(), activeControllerId, rollClient)) {
                // if this node is combined, then we have to check the availability as well
                if (c.currentRoles().broker()) {
                    Availability availability = new Availability(reconciliation, rollClient);
                    if (availability.anyPartitionWouldBeUnderReplicated(c.nodeId())) {
                        LOGGER.debugCr(reconciliation, "Combined node {} cannot be safely restarted without impacting the availability", c.nodeId());
                        return false;
                    }
                }

                LOGGER.debugCr(reconciliation, "Controller node {} can be safely restarted", c.nodeId());
                nextNodeToRestart.add(c);
                return true;
            } else {
                LOGGER.debugCr(reconciliation, "Controller node {} cannot be safely restarted without impacting quorum health", c.nodeId());
            }
            return false;
        });

        if (nextNodeToRestart.isEmpty()) LOGGER.warnCr(reconciliation, "None of the following controller nodes can be safely restarted: {}", controllersToStart);
        return nextNodeToRestart;
    }

    /**
     * Returns true if the majority of the controllers' lastCaughtUpTimestamps are within
     * the controller.quorum.fetch.timeout.ms based on the given quorum info.
     * The given controllerNeedRestarting is the one being considered to restart, therefore excluded from the check.
     *
     * The total number of controller is passed in to this method rather than using the size of the quorum followers
     * returned from the Admin. This is because when scaling down controllers, the returned quorum info from them
     * could contain inconsistent number of followers.
     */
    private boolean isQuorumHealthyWithoutNode(Reconciliation reconciliation,
                                               int controllerNeedRestarting,
                                               int activeControllerId,
                                               RollClient rollClient) {
        LOGGER.debugCr(reconciliation, "Determining the impact of restarting controller {} on quorum health", controllerNeedRestarting);
        Map<Integer, Long> quorumFollowerStates = rollClient.quorumLastCaughtUpTimestamps(contexts.stream().filter(c -> c.nodeId() == activeControllerId).map(Context::nodeRef).collect(Collectors.toSet()));
        int controllerCount = quorumFollowerStates.size();
        if (controllerCount == 1) {
            LOGGER.warnCr(reconciliation, "Performing rolling update on controller quorum with a single node. The cluster may be " +
                    "in a defective state once the rolling update is complete. It is recommended that a minimum of three controllers are used.");
            return true;
        }

        var activeController = contexts.stream().filter(c -> c.nodeId() == activeControllerId).map(Context::nodeRef).toList();
        var config = rollClient.describeControllerConfigs(activeController);
        var nodeConfigs =  (config != null) && (config.get(0) != null) ? config.get(0).nodeConfigs() : null;
        var controllerQuorumFetchTimeoutValue = (nodeConfigs != null) ? nodeConfigs.get(CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_NAME) : null;
        var controllerQuorumFetchTimeout = controllerQuorumFetchTimeoutValue == null ?
                CONTROLLER_QUORUM_FETCH_TIMEOUT_MS_CONFIG_DEFAULT : Long.parseLong(controllerQuorumFetchTimeoutValue.value());

        long leaderLastCaughtUpTimestamp = quorumFollowerStates.get(activeControllerId);

        long numOfCaughtUpControllers = quorumFollowerStates.entrySet().stream().filter(entry -> {
            int nodeId = entry.getKey();
            long lastCaughtUpTimestamp = entry.getValue();
            if (lastCaughtUpTimestamp < 0) {
                LOGGER.errorCr(reconciliation, "No valid lastCaughtUpTimestamp is found for controller {} ", nodeId);
            } else {
                LOGGER.debugCr(reconciliation, "The lastCaughtUpTimestamp for controller {} is {}", nodeId, lastCaughtUpTimestamp);
                if (nodeId == activeControllerId || (leaderLastCaughtUpTimestamp - lastCaughtUpTimestamp) < controllerQuorumFetchTimeout) {
                    if (nodeId != controllerNeedRestarting) {
                        return true;
                    }
                    LOGGER.debugCr(reconciliation, "Controller {} has caught up with the controller quorum leader", nodeId);
                } else {
                    LOGGER.debugCr(reconciliation, "Controller {} has fallen behind the controller quorum leader", nodeId);
                }
            }
            return false;
        }).count();

        if (controllerCount == 2) {
            // Only roll the controller if the other one in the quorum has caught up or is the active controller.
            if (numOfCaughtUpControllers == 1) {
                LOGGER.warnCr(reconciliation, "Performing rolling update on a controller quorum with 2 nodes. The cluster may be " +
                        "in a defective state once the rolling update is complete. It is recommended that a minimum of three controllers are used.");
                return true;
            } else {
                return false;
            }
        } else {
            boolean result =  numOfCaughtUpControllers >= (controllerCount + 2) / 2;
            if (!result) {
                LOGGER.debugCr(reconciliation, "Controller {} cannot be restarted without impacting quorum health", controllerNeedRestarting);
            }
            return result;
        }
    }

    /**
     * Returns a batch of broker nodes that have no topic partitions in common and have no impact on cluster availability if restarted.
     */
    private Set<Context> nextBatchBrokers(Reconciliation reconciliation,
                                                   RollClient rollClient,
                                                   List<Context> nodesNeedingRestart,
                                                   int maxRestartBatchSize) {

        if (nodesNeedingRestart.size() == 1) {
            return Collections.singleton(nodesNeedingRestart.get(0));
        }

        if (nodesNeedingRestart.size() < 1) {
            return Collections.emptySet();
        }

        Availability availability = new Availability(reconciliation, rollClient);

        //TODO: Implement dry run for batch rolling

        // If maxRestartBatchSize is set to 1, no point executing batching algorithm so
        // return the next available node that is ordered by the readiness state
        if (maxRestartBatchSize == 1) {
            List<Context> eligibleNodes = nodesNeedingRestart.stream()
                    .filter(context -> !availability.anyPartitionWouldBeUnderReplicated(context.nodeId()))
                    .sorted(Comparator.comparing((Context c) -> c.state().equals(State.READY)))
                    .toList();
            return eligibleNodes.size() > 0 ?  Set.of(eligibleNodes.get(0)) : Set.of();
        }

        LOGGER.debugCr(reconciliation, "Parallel batching of broker nodes is enabled. Max batch size is {}");
        List<KafkaNode> nodes = nodesNeedingRestart.stream()
                .map(c -> new KafkaNode(c.nodeId(), availability.getReplicasForNode(c.nodeId())))
                .collect(Collectors.toList());

        // Split the set of all brokers into subsets of brokers that can be rolled in parallel
        var cells = Batching.cells(reconciliation, nodes);
        int cellNum = 0;
        for (var cell: cells) {
            LOGGER.debugCr(reconciliation, "Cell {}: {}", ++cellNum, cell);
        }

        cellNum = 0;
        for (var cell: cells) {
            LOGGER.debugCr(reconciliation, "Restart-eligible cell {}: {}", ++cellNum, cell);
        }

        var batches = Batching.batchCells(reconciliation, cells, availability, maxRestartBatchSize);
        LOGGER.debugCr(reconciliation, "Batches {}", Batching.nodeIdsToString2(batches));

        var bestBatch = Batching.pickBestBatchForRestart(batches);
        LOGGER.debugCr(reconciliation, "Best batch {}", Batching.nodeIdsToString(bestBatch));

        return nodesNeedingRestart.stream().filter(c -> bestBatch.contains(c.nodeId())).collect(Collectors.toSet());
    }

}
