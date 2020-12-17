/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.WatcherException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Meter;
import io.strimzi.api.kafka.model.Spec;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.ResourceVisitor;
import io.strimzi.operator.common.model.ValidationVisitor;
import io.strimzi.operator.common.operator.resource.AbstractWatchableStatusedResourceOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.Util.async;

/**
 * A base implementation of {@link Operator}.
 *
 * <ul>
 * <li>uses the Fabric8 kubernetes API and implements
 * {@link #reconcile(Reconciliation)} by delegating to abstract {@link #createOrUpdate(Reconciliation, CustomResource)}
 * and {@link #delete(Reconciliation)} methods for subclasses to implement.
 * 
 * <li>add support for operator-side {@linkplain #validate(CustomResource) validation}.
 *     This can be used to automatically log warnings about source resources which used deprecated part of the CR API.
 *ą
 * </ul>
 * @param <T> The Java representation of the Kubernetes resource, e.g. {@code Kafka} or {@code KafkaConnect}
 * @param <O> The "Resource Operator" for the source resource type. Typically this will be some instantiation of
 *           {@link io.strimzi.operator.common.operator.resource.CrdOperator}.
 */
public abstract class AbstractOperator<
        T extends CustomResource<P, S>,
        P extends Spec,
        S extends Status,
        O extends AbstractWatchableStatusedResourceOperator<?, T, ?, ?>>
            implements Operator {

    private static final Logger log = LogManager.getLogger(AbstractOperator.class);

    protected static final int LOCK_TIMEOUT_MS = 10000;
    public static final String METRICS_PREFIX = "strimzi.";

    protected final Vertx vertx;
    protected final O resourceOperator;
    private final String kind;

    protected final MetricsProvider metrics;
    private final Counter periodicReconciliationsCounter;
    private final Counter reconciliationsCounter;
    private final Counter failedReconciliationsCounter;
    private final Counter successfulReconciliationsCounter;
    private final Counter lockedReconciliationsCounter;
    private final AtomicInteger resourceCounter;
    private final Timer reconciliationsTimer;
    private final Map<Tags, AtomicInteger> resourcesStateCounter;

    public AbstractOperator(Vertx vertx, String kind, O resourceOperator, MetricsProvider metrics) {
        this.vertx = vertx;
        this.kind = kind;
        this.resourceOperator = resourceOperator;
        this.metrics = metrics;

        // Setup metrics
        Tags metricTags = Tags.of(Tag.of("kind", kind()));

        periodicReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.periodical",
                "Number of periodical reconciliations done by the operator",
                metricTags);

        reconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations",
                "Number of reconciliations done by the operator for individual resources",
                metricTags);

        failedReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.failed",
                "Number of reconciliations done by the operator for individual resources which failed",
                metricTags);

        successfulReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.successful",
                "Number of reconciliations done by the operator for individual resources which were successful",
                metricTags);

        lockedReconciliationsCounter = metrics.counter(METRICS_PREFIX + "reconciliations.locked",
                "Number of reconciliations skipped because another reconciliation for the same resource was still running",
                metricTags);

        resourceCounter = metrics.gauge(METRICS_PREFIX + "resources",
                "Number of custom resources the operator sees",
                metricTags);

        reconciliationsTimer = metrics.timer(METRICS_PREFIX + "reconciliations.duration",
                "The time the reconciliation takes to complete",
                metricTags);

        resourcesStateCounter = new ConcurrentHashMap<>();
    }

    @Override
    public String kind() {
        return kind;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code namespace} and
     * cluster {@code name}
     *
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    private String getLockName(String namespace, String name) {
        return "lock::" + namespace + "::" + kind() + "::" + name;
    }

    /**
     * Asynchronously creates or updates the given {@code resource}.
     * This method can be called when the given {@code resource} has been created,
     * or updated and also at some regular interval while the resource continues to exist in Kubernetes.
     * The calling of this method does not imply that anything has actually changed.
     * @param reconciliation Uniquely identifies the reconciliation itself.
     * @param resource The resource to be created, or updated.
     * @return A Future which is completed once the reconciliation of the given resource instance is complete.
     */
    protected abstract Future<S> createOrUpdate(Reconciliation reconciliation, T resource);

    /**
     * Asynchronously deletes the resource identified by {@code reconciliation}.
     * Operators which only create other Kubernetes resources in order to honour their source resource can rely
     * on Kubernetes Garbage Collection to handle deletion.
     * Such operators should return a Future which completes with {@code false}.
     * Operators which handle deletion themselves should return a Future which completes with {@code true}.
     * @param reconciliation
     * @return
     */
    protected abstract Future<Boolean> delete(Reconciliation reconciliation);

    /**
     * Reconcile assembly resources in the given namespace having the given {@code name}.
     * Reconciliation works by getting the assembly resource (e.g. {@code KafkaUser})
     * in the given namespace with the given name and
     * comparing with the corresponding resource.
     * @param reconciliation The reconciliation.
     * @return A Future which is completed with the result of the reconciliation.
     */
    @Override
    @SuppressWarnings("unchecked")
    public final Future<Void> reconcile(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        reconciliationsCounter.increment();
        Timer.Sample reconciliationTimerSample = Timer.start(metrics.meterRegistry());

        Future<Void> handler = withLock(reconciliation, LOCK_TIMEOUT_MS, () -> {
            T cr = resourceOperator.get(namespace, name);

            if (cr != null) {
                Promise<Void> createOrUpdate = Promise.promise();

                if (cr.getSpec() == null)   {
                    InvalidResourceException exception = new InvalidResourceException("Spec cannot be null");

                    S status = createStatus();
                    Condition errorCondition = new ConditionBuilder()
                            .withLastTransitionTime(StatusUtils.iso8601Now())
                            .withType("NotReady")
                            .withStatus("True")
                            .withReason(exception.getClass().getSimpleName())
                            .withMessage(exception.getMessage())
                            .build();
                    status.setObservedGeneration(cr.getMetadata().getGeneration());
                    status.addCondition(errorCondition);

                    log.error("{}: {} spec cannot be null", reconciliation, cr.getMetadata().getName());
                    updateStatus(reconciliation, status).onComplete(notUsed -> {
                        createOrUpdate.fail(exception);
                    });

                    return createOrUpdate.future();
                }

                Set<Condition> unknownAndDeprecatedConditions = validate(cr);

                log.info("{}: {} {} will be checked for creation or modification", reconciliation, kind, name);

                createOrUpdate(reconciliation, cr)
                        .onComplete(res -> {
                            if (res.succeeded()) {
                                S status = res.result();

                                addWarningsToStatus(status, unknownAndDeprecatedConditions);
                                updateStatus(reconciliation, status).onComplete(statusResult -> {
                                    if (statusResult.succeeded()) {
                                        createOrUpdate.complete();
                                    } else {
                                        createOrUpdate.fail(statusResult.cause());
                                    }
                                });
                            } else {
                                if (res.cause() instanceof ReconciliationException) {
                                    ReconciliationException e = (ReconciliationException) res.cause();
                                    Status status = e.getStatus();
                                    addWarningsToStatus(status, unknownAndDeprecatedConditions);

                                    log.error("{}: createOrUpdate failed", reconciliation, e.getCause());

                                    updateStatus(reconciliation, (S) status).onComplete(statusResult -> {
                                        createOrUpdate.fail(e.getCause());
                                    });
                                } else {
                                    log.error("{}: createOrUpdate failed", reconciliation, res.cause());
                                    createOrUpdate.fail(res.cause());
                                }
                            }
                        });

                return createOrUpdate.future();
            } else {
                log.info("{}: {} {} should be deleted", reconciliation, kind, name);
                return delete(reconciliation).map(deleteResult -> {
                    if (deleteResult) {
                        log.info("{}: {} {} deleted", reconciliation, kind, name);
                    } else {
                        log.info("{}: Assembly {} or some parts of it will be deleted by garbage collection", reconciliation, name);
                    }
                    return (Void) null;
                }).recover(deleteResult -> {
                    log.error("{}: Deletion of {} {} failed", reconciliation, kind, name, deleteResult);
                    return Future.failedFuture(deleteResult);
                });
            }
        });

        Promise<Void> result = Promise.promise();
        handler.onComplete(reconcileResult -> {
            handleResult(reconciliation, reconcileResult, reconciliationTimerSample);
            result.handle(reconcileResult);
        });

        return result.future();
    }

    private void addWarningsToStatus(Status status, Set<Condition> unknownAndDeprecatedConditions)   {
        if (status != null)  {
            status.addConditions(unknownAndDeprecatedConditions);
        }
    }

    /**
     * Updates the Status field of the Kafka CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param reconciliation the reconciliation identified
     * @param desiredStatus The KafkaStatus which should be set
     *
     * @return
     */
    Future<Void> updateStatus(Reconciliation reconciliation, S desiredStatus) {
        if (desiredStatus == null)  {
            log.debug("{}: Desired status is null - status will not be updated", reconciliation);
            return Future.succeededFuture();
        }

        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        return resourceOperator.getAsync(namespace, name)
                .compose(res -> {
                    if (res != null) {
                        S currentStatus = res.getStatus();
                        StatusDiff sDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!sDiff.isEmpty()) {
                            res.setStatus(desiredStatus);

                            return resourceOperator.updateStatusAsync(res)
                                    .compose(notUsed -> {
                                        log.debug("{}: Completed status update", reconciliation);
                                        return Future.succeededFuture();
                                    }, error -> {
                                            log.error("{}: Failed to update status", reconciliation, error);
                                            return Future.failedFuture(error);
                                        });
                        } else {
                            log.debug("{}: Status did not change", reconciliation);
                            return Future.succeededFuture();
                        }
                    } else {
                        log.error("{}: Current {} resource not found", reconciliation, reconciliation.kind());
                        return Future.failedFuture("Current " + reconciliation.kind() + " resource with name " + name + " not found");
                    }
                }, error -> {
                        log.error("{}: Failed to get the current {} resource and its status", reconciliation, reconciliation.kind(), error);
                        return Future.failedFuture(error);
                    });
    }

    protected abstract S createStatus();

    /**
     * The exception by which Futures returned by {@link #withLock(Reconciliation, long, Callable)} are failed when
     * the lock cannot be acquired within the timeout.
     */
    static class UnableToAcquireLockException extends TimeoutException { }

    /**
     * Acquire the lock for the resource implied by the {@code reconciliation}
     * and call the given {@code callable} with the lock held.
     * Once the callable returns (or if it throws) release the lock and complete the returned Future.
     * If the lock cannot be acquired the given {@code callable} is not called and the returned Future is completed with {@link UnableToAcquireLockException}.
     * @param reconciliation
     * @param callable
     * @param <T>
     * @return
     */
    protected final <T> Future<T> withLock(Reconciliation reconciliation, long lockTimeoutMs, Callable<Future<T>> callable) {
        Promise<T> handler = Promise.promise();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        final String lockName = getLockName(namespace, name);
        log.debug("{}: Try to acquire lock {}", reconciliation, lockName);
        vertx.sharedData().getLockWithTimeout(lockName, lockTimeoutMs, res -> {
            if (res.succeeded()) {
                log.debug("{}: Lock {} acquired", reconciliation, lockName);
                Lock lock = res.result();
                try {
                    callable.call().onComplete(callableRes -> {
                        if (callableRes.succeeded()) {
                            handler.complete(callableRes.result());
                        } else {
                            handler.fail(callableRes.cause());
                        }

                        lock.release();
                        log.debug("{}: Lock {} released", reconciliation, lockName);
                    });
                } catch (Throwable ex) {
                    lock.release();
                    log.debug("{}: Lock {} released", reconciliation, lockName);
                    log.error("{}: Reconciliation failed", reconciliation, ex);
                    handler.fail(ex);
                }
            } else {
                log.warn("{}: Failed to acquire lock {} within {}ms.", reconciliation, lockName, lockTimeoutMs);
                handler.fail(new UnableToAcquireLockException());
            }
        });
        return handler.future();
    }

    /**
     * Validate the Custom Resource.
     * This should log at the WARN level (rather than throwing)
     * if the resource can safely be reconciled (e.g. it merely using deprecated API).
     * @param resource The custom resource
     * @throws InvalidResourceException if the resource cannot be safely reconciled.
     */
    /*test*/ Set<Condition> validate(T resource) {
        if (resource != null) {
            Set<Condition> warningConditions = new LinkedHashSet<>(0);

            ResourceVisitor.visit(resource, new ValidationVisitor(resource, log, warningConditions));

            return warningConditions;
        }

        return Collections.emptySet();
    }

    public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
        return resourceOperator.listAsync(namespace, selector())
                .map(resourceList ->
                        resourceList.stream()
                                .map(resource -> new NamespaceAndName(resource.getMetadata().getNamespace(), resource.getMetadata().getName()))
                                .collect(Collectors.toSet()));
    }

    /**
     * A selector to narrow the scope of the {@linkplain #createWatch(String, Consumer) watch}
     * and {@linkplain #allResourceNames(String) query}.
     * @return A selector.
     */
    public Optional<LabelSelector> selector() {
        return Optional.empty();
    }

    /**
     * Create Kubernetes watch.
     *
     * @param namespace Namespace where to watch for users.
     * @param onClose Callback called when the watch is closed.
     *
     * @return A future which completes when the watcher has been created.
     */
    public Future<Watch> createWatch(String namespace, Consumer<WatcherException> onClose) {
        return async(vertx, () -> resourceOperator.watch(namespace, selector(), new OperatorWatcher<>(this, namespace, onClose)));
    }

    public Consumer<WatcherException> recreateWatch(String namespace) {
        Consumer<WatcherException> kubernetesClientExceptionConsumer = new Consumer<WatcherException>() {
            @Override
            public void accept(WatcherException e) {
                if (e != null) {
                    log.error("Watcher closed with exception in namespace {}", namespace, e);
                    createWatch(namespace, this);
                } else {
                    log.info("Watcher closed in namespace {}", namespace);
                }
            }
        };
        return kubernetesClientExceptionConsumer;
    }

    /**
     * Log the reconciliation outcome.
     */
    private void handleResult(Reconciliation reconciliation, AsyncResult<Void> result, Timer.Sample reconciliationTimerSample) {
        if (result.succeeded()) {
            updateResourceState(reconciliation, true);
            successfulReconciliationsCounter.increment();
            reconciliationTimerSample.stop(reconciliationsTimer);
            log.info("{}: reconciled", reconciliation);
        } else {
            Throwable cause = result.cause();

            if (cause instanceof InvalidConfigParameterException) {
                updateResourceState(reconciliation, false);
                failedReconciliationsCounter.increment();
                reconciliationTimerSample.stop(reconciliationsTimer);
                log.warn("{}: Failed to reconcile {}", reconciliation, cause.getMessage());
            } else if (cause instanceof UnableToAcquireLockException) {
                lockedReconciliationsCounter.increment();
            } else  {
                updateResourceState(reconciliation, false);
                failedReconciliationsCounter.increment();
                reconciliationTimerSample.stop(reconciliationsTimer);
                log.warn("{}: Failed to reconcile", reconciliation, cause);
            }
        }
    }

    public Counter getPeriodicReconciliationsCounter() {
        return periodicReconciliationsCounter;
    }

    public AtomicInteger getResourceCounter() {
        return resourceCounter;
    }

    /**
     * Updates the resource state metric for the provided reconciliation which brings kind, name and namespace
     * of the custom resource.
     *
     * @param reconciliation reconciliation to use to update the resource state metric
     * @param ready if reconcile was successful and the resource is ready
     */
    private void updateResourceState(Reconciliation reconciliation, boolean ready) {
        Tags metricTags = Tags.of(
                Tag.of("kind", reconciliation.kind()),
                Tag.of("name", reconciliation.name()),
                Tag.of("resource-namespace", reconciliation.namespace()));

        T cr = resourceOperator.get(reconciliation.namespace(), reconciliation.name());
        if (cr != null) {
            resourcesStateCounter.computeIfAbsent(metricTags, tags ->
                    metrics.gauge(METRICS_PREFIX + "resource.state", "Current state of the resource: 1 ready, 0 fail", tags)
            );
            resourcesStateCounter.get(metricTags).set(ready ? 1 : 0);
            log.debug("{}: Updated metric " + METRICS_PREFIX + "resource.state{} = {}", reconciliation, metricTags, ready ? 1 : 0);
        } else {
            Optional<Meter> gauge = metrics.meterRegistry().getMeters()
                    .stream()
                    .filter(meter -> meter.getId().getName().equals(METRICS_PREFIX + "resource.state") &&
                            meter.getId().getTags().equals(metricTags.stream().collect(Collectors.toList()))
                    ).findFirst();

            if (gauge.isPresent()) {
                metrics.meterRegistry().remove(gauge.get().getId());
                resourcesStateCounter.remove(metricTags);
                log.debug("{}: Removed metric " + METRICS_PREFIX + "resource.state{}", reconciliation, metricTags);
            }
        }
    }

    /**
     * In some cases, when the ClusterRoleBinding reconciliation fails with RBAC error and the desired object is null,
     * we want to ignore the error and return success. This is used to let Strimzi work without some Cluster-wide RBAC
     * rights when the features they are needed for are not used by the user.
     *
     * @param reconcileFuture   The original reconciliation future
     * @param desired           The desired state of the resource.
     * @return                  A future which completes when the resource was reconciled.
     */
    public Future<ReconcileResult<ClusterRoleBinding>> withIgnoreRbacError(Future<ReconcileResult<ClusterRoleBinding>> reconcileFuture, ClusterRoleBinding desired) {
        return reconcileFuture.compose(
            rr -> Future.succeededFuture(),
            e -> {
                if (desired == null
                        && e.getMessage() != null
                        && e.getMessage().contains("Message: Forbidden!")) {
                    log.debug("Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required.");
                    return Future.succeededFuture();
                }
                return Future.failedFuture(e.getMessage());
            }
        );
    }
}
