/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ConnectTimeoutException;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.AutoRestartStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaConnectorOffsetsAnnotation;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.operator.VertxUtil;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.model.StatusUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.api.ResourceAnnotations.ANNO_STRIMZI_IO_CONNECTOR_OFFSETS;
import static io.strimzi.api.ResourceAnnotations.ANNO_STRIMZI_IO_RESTART_TASK;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, KafkaConnectSpec, KafkaConnectStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaConnectAssemblyOperator.class.getName());

    private final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator;
    private final ConnectBuildOperator connectBuildOperator;

    /**
     * Constructor
     *
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config) {
        this(vertx, pfa, supplier, config, connect -> new KafkaConnectApiImpl());
    }

    /**
     * Constructor which allows providing custom implementation of the Kafka Connect Client. This is used in tests.
     *
     * @param vertx                     The Vertx instance
     * @param pfa                       Platform features availability properties
     * @param supplier                  Supplies the operators for different resources
     * @param config                    ClusterOperator configuration. Used to get the user-configured image pull policy
     *                                  and the secrets.
     * @param connectClientProvider     Provider of the Kafka Connect client
     */
    protected KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider) {
        this(vertx, pfa, supplier, config, connectClientProvider, KafkaConnectCluster.REST_API_PORT);
    }

    /**
     * Constructor which allows providing custom implementation of the Kafka Connect Client and port on which the Kafka
     * Connect REST API is listening. This is used in tests.
     *
     * @param vertx                     The Vertx instance
     * @param pfa                       Platform features availability properties
     * @param supplier                  Supplies the operators for different resources
     * @param config                    ClusterOperator configuration. Used to get the user-configured image pull policy
     *                                  and the secrets.
     * @param connectClientProvider     Provider of the Kafka Connect client
     * @param port                      Port of the Kafka Connect REST API
     */
    protected KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider, int port) {
        super(vertx, pfa, KafkaConnect.RESOURCE_KIND, supplier.connectOperator, supplier, config, connectClientProvider, port);

        this.connectorOperator = supplier.kafkaConnectorOperator;
        this.connectBuildOperator = new ConnectBuildOperator(pfa, supplier, config);
    }

    @Override
    protected Future<KafkaConnectStatus> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnect) {
        KafkaConnectCluster connect;
        KafkaConnectBuild build;
        KafkaConnectStatus kafkaConnectStatus = new KafkaConnectStatus();
        try {
            connect = KafkaConnectCluster.fromCrd(reconciliation, kafkaConnect, versions, sharedEnvironmentProvider);
            build = KafkaConnectBuild.fromCrd(reconciliation, kafkaConnect, versions, sharedEnvironmentProvider);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, e);
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, e);
            return Future.failedFuture(new ReconciliationException(kafkaConnectStatus, e));
        }

        Promise<KafkaConnectStatus> createOrUpdatePromise = Promise.promise();
        String namespace = reconciliation.namespace();

        Map<String, String> podAnnotations = new HashMap<>();
        Map<String, String> controllerAnnotations = new HashMap<>();

        final boolean hasZeroReplicas = connect.getReplicas() == 0;
        final boolean useConnectorResources = isUseResources(kafkaConnect);
        final AtomicReference<String> image = new AtomicReference<>();
        final AtomicReference<String> desiredLogging = new AtomicReference<>();
        String initCrbName = KafkaConnectResources.initContainerClusterRoleBindingName(kafkaConnect.getMetadata().getName(), namespace);
        ClusterRoleBinding initCrb = connect.generateClusterRoleBinding();

        LOGGER.debugCr(reconciliation, "Creating or updating Kafka Connect cluster");

        connectServiceAccount(reconciliation, namespace, KafkaConnectResources.serviceAccountName(connect.getCluster()), connect)
                .compose(i -> connectInitClusterRoleBinding(reconciliation, initCrbName, initCrb))
                .compose(i -> connectNetworkPolicy(reconciliation, namespace, connect, isUseResources(kafkaConnect)))
                .compose(i -> manualRollingUpdate(reconciliation, connect))
                .compose(i -> podSetOperations.getAsync(reconciliation.namespace(), connect.getComponentName()))
                .compose(podSet -> connectBuildOperator.reconcile(reconciliation, namespace, podSet, build))
                .compose(buildInfo -> {
                    if (buildInfo != null) {
                        podAnnotations.put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, buildInfo.buildRevision());
                        controllerAnnotations.put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, buildInfo.buildRevision());
                        controllerAnnotations.put(Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, buildInfo.image());
                        image.set(buildInfo.image());
                    }
                    return Future.succeededFuture();
                })
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> serviceOperations.reconcile(reconciliation, namespace, connect.getComponentName(), connect.generateHeadlessService()))
                .compose(i -> generateMetricsAndLoggingConfigMap(reconciliation, connect))
                .compose(logAndMetricsConfigMap -> {
                    String logging = logAndMetricsConfigMap.getData().get(connect.logging().configMapKey());

                    if (useConnectorResources) {
                        // When connector resources are used, we do dynamic configuration update, and we need the hash to
                        // contain only settings that cannot be updated dynamically
                        podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging)));
                    } else {
                        // When connector resources are not used, we do not do dynamic logging updates, and we need the
                        // hash to cover complete logging configuration
                        podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_HASH, Util.hashStub(logging));
                    }

                    desiredLogging.set(logging);

                    podAnnotations.put(KafkaConnectCluster.ANNO_STRIMZI_IO_CONFIGURATION_HASH, Util.hashStub(logAndMetricsConfigMap.getData().get(KafkaConnectCluster.KAFKA_CONNECT_CONFIGURATION_FILENAME)));

                    return configMapOperations.reconcile(reconciliation, namespace, logAndMetricsConfigMap.getMetadata().getName(), logAndMetricsConfigMap);
                })
                .compose(i -> ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperations, connect))
                .compose(i -> connectPodDisruptionBudget(reconciliation, namespace, connect))
                .compose(i -> generateAuthHash(namespace, kafkaConnect.getSpec()))
                .compose(hash -> {
                    podAnnotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, Integer.toString(hash));
                    return Future.succeededFuture();
                })
                .compose(i -> reconcilePodSet(reconciliation, connect, podAnnotations, controllerAnnotations, image.get()))
                .compose(i -> useConnectorResources && !hasZeroReplicas ? reconcileConnectLoggers(reconciliation, KafkaConnectResources.qualifiedServiceName(reconciliation.name(), namespace), desiredLogging.get(), connect.defaultLogConfig()) : Future.succeededFuture())
                .compose(i -> useConnectorResources && !hasZeroReplicas ? reconcileAvailableConnectorPlugins(reconciliation, KafkaConnectResources.qualifiedServiceName(reconciliation.name(), namespace), kafkaConnectStatus) : Future.succeededFuture())
                .compose(i -> useConnectorResources ? reconcileConnectors(reconciliation, kafkaConnect, hasZeroReplicas) : Future.succeededFuture())
                .onComplete(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, reconciliationResult.cause());

                    if (!hasZeroReplicas) {
                        kafkaConnectStatus.setUrl(KafkaConnectResources.url(connect.getCluster(), namespace, port));
                    }

                    kafkaConnectStatus.setReplicas(connect.getReplicas());
                    kafkaConnectStatus.setLabelSelector(connect.getSelectorLabels().toSelectorString());

                    if (reconciliationResult.succeeded())   {
                        createOrUpdatePromise.complete(kafkaConnectStatus);
                    } else {
                        createOrUpdatePromise.fail(new ReconciliationException(kafkaConnectStatus, reconciliationResult.cause()));
                    }
                });

        return createOrUpdatePromise.future();
    }

    @Override
    protected KafkaConnectStatus createStatus(KafkaConnect ignored) {
        return new KafkaConnectStatus();
    }

    @Override
    public void reconcileThese(String trigger, Set<NamespaceAndName> desiredNames, String namespace, Handler<AsyncResult<Void>> handler) {
        super.reconcileThese(trigger, desiredNames, namespace, ignore -> {
            List<String> connects = desiredNames.stream().map(NamespaceAndName::getName).collect(Collectors.toList());
            LabelSelectorRequirement requirement = new LabelSelectorRequirement(Labels.STRIMZI_CLUSTER_LABEL, "In", connects);
            LabelSelector connectorsSelector = new LabelSelector(List.of(requirement), null);
            connectorOperator.listAsync(namespace, connectorsSelector)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            metrics().resetConnectorsCounters(namespace);
                            ar.result().forEach(connector -> {
                                metrics().connectorsResourceCounter(connector.getMetadata().getNamespace()).incrementAndGet();
                                if (isPaused(connector.getStatus())) {
                                    metrics().pausedConnectorsResourceCounter(connector.getMetadata().getNamespace()).incrementAndGet();
                                }
                            });
                            handler.handle(Future.succeededFuture());
                        } else {
                            handler.handle(ar.map((Void) null));
                        }
                    });
        });
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference and
     * updates the status of all KafkaConnector resources to mark that they are now orphaned (have no matching Connect
     * cluster).
     *
     * @param reconciliation    The Reconciliation identification
     *
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return updateConnectorsThatConnectClusterWasDeleted(reconciliation)
                .compose(i -> ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaConnectResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null))
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }

    /**
     * When the KafkaConnect cluster is deleted, thi method updates all KafkaConnector resources belonging to it with
     * the error that they do not have matching KafkaConnect cluster.
     *
     * @param reconciliation    Reconciliation marker
     *
     * @return  Future indicating that all connectors have been updated
     */
    private Future<Void> updateConnectorsThatConnectClusterWasDeleted(Reconciliation reconciliation) {
        // When deleting KafkaConnect we need to update the status of all its KafkaConnector
        return connectorOperator.listAsync(reconciliation.namespace(), Labels.forStrimziCluster(reconciliation.name())).compose(connectors -> {
            List<Future<Void>> connectorFutures = new ArrayList<>();
            for (KafkaConnector connector : connectors) {
                connectorFutures.add(maybeUpdateConnectorStatus(reconciliation, connector, null,
                        noConnectCluster(reconciliation.namespace(), reconciliation.name())));
            }
            return Future.join(connectorFutures);
        }).map((Void) null);
    }

    /**
     * Generates a hash from the trusted TLS certificates that can be used to spot if it has changed.
     *
     * @param namespace          Namespace of the Connect cluster
     * @param kafkaConnectSpec   KafkaConnectSpec object
     * @return                   Future for tracking the asynchronous result of generating the TLS auth hash
     */
    private Future<Integer> generateAuthHash(String namespace, KafkaConnectSpec kafkaConnectSpec) {
        KafkaClientAuthentication auth = kafkaConnectSpec.getAuthentication();
        List<CertSecretSource> trustedCertificates = kafkaConnectSpec.getTls() == null ? Collections.emptyList() : kafkaConnectSpec.getTls().getTrustedCertificates();
        return VertxUtil.authTlsHash(secretOperations, namespace, auth, trustedCertificates);
    }

    /**
     * Gets the list of available connector plugins from the Connect REST API
     *
     * @param reconciliation    Reconciliation marker
     * @param host              Kafka Connect REST API host
     * @param connectStatus     Status of the KafkaConnect custom resource (will be used to set the available connector plugins)
     *
     * @return  Future that succeeds once the list of connector plugins from the Connect REST API is obtained and stored in the status.
     */
    private Future<Void> reconcileAvailableConnectorPlugins(Reconciliation reconciliation, String host, KafkaConnectStatus connectStatus)  {
        KafkaConnectApi apiClient = connectClientProvider.apply(vertx);
        return VertxUtil.completableFutureToVertxFuture(apiClient.listConnectorPlugins(reconciliation, host, port))
                .compose(connectorPlugins -> {
                    LOGGER.debugCr(reconciliation, "Setting list of connector plugins in Kafka Connect status");
                    connectStatus.setConnectorPlugins(connectorPlugins);
                    return Future.succeededFuture();
                });
    }

    /**
     * Reconcile all the connectors selected by the given connect instance, updated each connectors status with the result.
     *
     * @param reconciliation    Reconciliation marker
     * @param connect           KafkaConnect custom resource
     * @param scaledToZero      Indicated whether the related Connect cluster is currently scaled to 0 replicas
     *
     * @return A future, failed if any of the connectors' statuses could not be updated.
     */
    private Future<Void> reconcileConnectors(Reconciliation reconciliation, KafkaConnect connect, boolean scaledToZero) {
        String connectName = connect.getMetadata().getName();
        String namespace = connect.getMetadata().getNamespace();

        if (scaledToZero)   {
            return connectorOperator.listAsync(namespace, new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build())
                .compose(connectors -> Future.join(
                    connectors.stream().map(connector -> Annotations.isReconciliationPausedWithAnnotation(connector)
                            ? maybeUpdateConnectorStatus(reconciliation, connector, null, null)
                            : maybeUpdateConnectorStatus(reconciliation, connector, null, zeroReplicas(namespace, connectName)))
                        .collect(Collectors.toList())
                )).map((Void) null);
        } else {
            String host = KafkaConnectResources.qualifiedServiceName(connectName, namespace);
            KafkaConnectApi apiClient = connectClientProvider.apply(vertx);

            return Future.join(
                    VertxUtil.completableFutureToVertxFuture(apiClient.list(reconciliation, host, port)),
                    connectorOperator.listAsync(namespace, new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build())
            ).compose(cf -> {
                List<String> runningConnectorNames = cf.resultAt(0);
                List<KafkaConnector> desiredConnectors = cf.resultAt(1);

                Set<String> deleteConnectorNames = new HashSet<>(runningConnectorNames);
                deleteConnectorNames.removeAll(desiredConnectors.stream().map(c -> c.getMetadata().getName()).collect(Collectors.toSet()));

                Future<Void> deletionFuture = deleteConnectors(reconciliation, host, apiClient, deleteConnectorNames);
                Future<Void> createOrUpdateFuture = createOrUpdateConnectors(reconciliation, host, apiClient, desiredConnectors);

                return Future.join(deletionFuture, createOrUpdateFuture).map((Void) null);
            }).recover(error -> {
                if (error instanceof ConnectTimeoutException) {
                    Promise<Void> connectorStatuses = Promise.promise();
                    LOGGER.warnCr(reconciliation, "Failed to connect to the REST API => trying to update the connector status");

                    connectorOperator.listAsync(namespace, new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build())
                            .compose(connectors -> Future.join(
                                    connectors.stream().map(connector -> maybeUpdateConnectorStatus(reconciliation, connector, null, error))
                                            .collect(Collectors.toList())
                            ))
                            .onComplete(ignore -> connectorStatuses.fail(error));

                    return connectorStatuses.future();
                } else {
                    return Future.failedFuture(error);
                }
            });
        }
    }

    private Future<Void> deleteConnectors(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, Set<String> connectorsForDeletion) {
        LOGGER.debugCr(reconciliation, "{} cluster: delete connectors: {}", kind(), connectorsForDeletion);
        return Future.join(connectorsForDeletion.stream()
                    .map(connectorName -> reconcileConnectorAndHandleResult(reconciliation, host, apiClient, true, connectorName, null))
                    .collect(Collectors.toList()))
                .mapEmpty();
    }

    private Future<Void> createOrUpdateConnectors(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, List<KafkaConnector> desiredConnectors) {
        LOGGER.debugCr(reconciliation, "{} cluster: required connectors: {}", kind(), desiredConnectors);
        return Future.join(desiredConnectors.stream()
                    .map(connector -> reconcileConnectorAndHandleResult(reconciliation, host, apiClient, true, connector.getMetadata().getName(), connector))
                    .collect(Collectors.toList()))
                .mapEmpty();
    }

    /*test*/ Future<Void> reconcileConnectorAndHandleResult(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                             boolean useResources, String connectorName, KafkaConnector connector) {
        Promise<Void> reconciliationResult = Promise.promise();

        metrics().connectorsReconciliationsCounter(reconciliation.namespace()).increment();
        Timer.Sample connectorsReconciliationsTimerSample = Timer.start(metrics().metricsProvider().meterRegistry());

        if (connector != null && Annotations.isReconciliationPausedWithAnnotation(connector)) {
            return maybeUpdateConnectorStatus(reconciliation, connector, null, null).compose(
                i -> {
                    connectorsReconciliationsTimerSample.stop(metrics().connectorsReconciliationsTimer(reconciliation.namespace()));
                    metrics().connectorsSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
                    return Future.succeededFuture();
                }
            );
        }

        reconcileConnector(reconciliation, host, apiClient, useResources, connectorName, connector)
                .onComplete(result -> {
                    if (result.succeeded() && result.result() == null)  {
                        // The reconciliation succeeded, but there is no status to be set => we complete the reconciliation and return
                        // This normally means that the connector was deleted and there is no status to be set
                        metrics().connectorsSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
                        connectorsReconciliationsTimerSample.stop(metrics().connectorsReconciliationsTimer(reconciliation.namespace()));
                        reconciliationResult.complete();
                    } else if (result.failed() && connector == null) {
                        // The reconciliation failed on connector deletion, so there is nowhere for status to be set => we complete the reconciliation and return
                        LOGGER.warnCr(reconciliation, "Error reconciling connector {}", connectorName, result.cause());
                        metrics().connectorsFailedReconciliationsCounter(reconciliation.namespace()).increment();
                        connectorsReconciliationsTimerSample.stop(metrics().connectorsReconciliationsTimer(reconciliation.namespace()));

                        // We suppress the error to not fail Connect reconciliation just because of a failing connector
                        reconciliationResult.complete();
                    } else {
                        maybeUpdateConnectorStatus(reconciliation, connector, result.result(), result.cause())
                                .onComplete(statusResult -> {
                                    connectorsReconciliationsTimerSample.stop(metrics().connectorsReconciliationsTimer(reconciliation.namespace()));

                                    if (result.succeeded() && statusResult.succeeded()) {
                                        metrics().connectorsSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
                                        reconciliationResult.complete();
                                    } else {
                                        // Reconciliation failed if either reconciliation or status update failed
                                        metrics().connectorsFailedReconciliationsCounter(reconciliation.namespace()).increment();

                                        // We suppress the error to not fail Connect reconciliation just because of a failing connector
                                        reconciliationResult.complete();
                                    }
                                });
                    }
                });

        return reconciliationResult.future();
    }

    private Future<ConnectorStatusAndConditions> reconcileConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                             boolean useResources, String connectorName, KafkaConnector connector) {
        if (connector == null) {
            if (useResources) {
                LOGGER.infoCr(reconciliation, "deleting connector: {}", connectorName);
                return VertxUtil.completableFutureToVertxFuture(apiClient.delete(reconciliation, host, port, connectorName)).mapEmpty();
            } else {
                return Future.succeededFuture();
            }
        } else {
            LOGGER.infoCr(reconciliation, "creating/updating connector: {}", connectorName);

            if (connector.getSpec() == null) {
                return Future.failedFuture(new InvalidResourceException("spec property is required"));
            }

            if (!useResources) {
                return Future.failedFuture(new NoSuchResourceException(reconciliation.kind() + " " + reconciliation.name() + " is not configured with annotation " + Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES));
            } else {
                return maybeCreateOrUpdateConnector(reconciliation, host, apiClient, connectorName, connector.getSpec(), connector);
            }
        }
    }

    /**
     * Create Kubernetes watch for KafkaConnector resources.
     *
     * @param namespace     Namespace where to watch for the resources
     *
     * @return  A future which completes when the watcher has been created
     */
    public Future<ReconnectingWatcher<KafkaConnector>> createConnectorWatch(String namespace) {
        return VertxUtil.async(vertx, () -> new ReconnectingWatcher<>(connectorOperator, KafkaConnector.RESOURCE_KIND, namespace, null, this::connectorEventHandler));
    }

    /**
     * Event handler called when the KafkaConnector watch receives an event.
     *
     * @param action    An Action describing the type of the event
     * @param resource  The resource for which the event was triggered
     */
    private void connectorEventHandler(Watcher.Action action, KafkaConnector resource) {
        String connectorName = resource.getMetadata().getName();
        String namespace = resource.getMetadata().getNamespace();
        String connectorKind = resource.getKind();
        String connectName = resource.getMetadata().getLabels() == null ? null : resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);

        switch (action) {
            case ADDED, DELETED, MODIFIED -> {
                if (connectName != null) {
                    // Check whether a KafkaConnect exists
                    resourceOperator.getAsync(namespace, connectName)
                            .compose(connect -> {
                                KafkaConnectApi apiClient = connectClientProvider.apply(vertx);
                                if (connect == null) {
                                    Reconciliation r = new Reconciliation("connector-watch", kind(),
                                            resource.getMetadata().getNamespace(), connectName);
                                    updateStatus(r, noConnectCluster(namespace, connectName), resource, connectorOperator);
                                    LOGGER.infoCr(r, "{} {} in namespace {} was {}, but Connect cluster {} does not exist", connectorKind, connectorName, namespace, action, connectName);
                                    return Future.succeededFuture();
                                } else {
                                    // grab the lock and call reconcileConnectors()
                                    // (i.e. short circuit doing a whole KafkaConnect reconciliation).
                                    Reconciliation reconciliation = new Reconciliation("connector-watch", kind(), resource.getMetadata().getNamespace(), connectName);

                                    if (!Util.matchesSelector(selector(), connect)) {
                                        LOGGER.debugCr(reconciliation, "{} {} in namespace {} was {}, but Connect cluster {} does not match label selector {} and will be ignored", connectorKind, connectorName, namespace, action, connectName, selector());
                                        return Future.succeededFuture();
                                    } else if (connect.getSpec() != null && connect.getSpec().getReplicas() == 0 && !Annotations.isReconciliationPausedWithAnnotation(resource)) {
                                        LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}, but Connect cluster {} has 0 replicas", connectorKind, connectorName, namespace, action, connectName);

                                        return withLock(reconciliation, LOCK_TIMEOUT_MS,
                                                () -> maybeUpdateConnectorStatus(reconciliation, resource, null, zeroReplicas(namespace, connectName))
                                                        .compose(reconcileResult -> {
                                                            LOGGER.infoCr(reconciliation, "reconciled");
                                                            return Future.succeededFuture();
                                                        }));
                                    } else {
                                        LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}", connectorKind, connectorName, namespace, action);

                                        return withLock(reconciliation, LOCK_TIMEOUT_MS,
                                                () -> reconcileConnectorAndHandleResult(reconciliation,
                                                                KafkaConnectResources.qualifiedServiceName(connectName, namespace), apiClient,
                                                                isUseResources(connect),
                                                                resource.getMetadata().getName(), action == Watcher.Action.DELETED ? null : resource)
                                                        .compose(reconcileResult -> {
                                                            LOGGER.infoCr(reconciliation, "reconciled");
                                                            return Future.succeededFuture(reconcileResult);
                                                        }));
                                    }
                                }
                            });
                } else {
                    updateStatus(new Reconciliation("connector-watch", kind(), resource.getMetadata().getNamespace(), null),
                            new InvalidResourceException("Resource lacks label '" + Labels.STRIMZI_CLUSTER_LABEL + "': No connect cluster in which to create this connector."), resource, connectorOperator);
                }
            }
            case ERROR ->
                    LOGGER.errorCr(new Reconciliation("connector-watch", connectorKind, connectName, namespace), "Failed {} {} in namespace {} ", connectorKind, connectorName, namespace);
            default ->
                    LOGGER.errorCr(new Reconciliation("connector-watch", connectorKind, connectName, namespace), "Unknown action: {} {} in namespace {}", connectorKind, connectorName, namespace);
        }
    }

    private Future<Void> maybeUpdateConnectorStatus(Reconciliation reconciliation, KafkaConnector connector, ConnectorStatusAndConditions connectorStatus, Throwable error) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        if (error != null) {
            LOGGER.warnCr(reconciliation, "Error reconciling connector {}", connector.getMetadata().getName(), error);
        }

        Map<String, Object> statusResult = null;
        List<String> topics = null;
        List<Condition> conditions = new ArrayList<>();
        AutoRestartStatus autoRestart = null;

        Future<Void> connectorReadiness = Future.succeededFuture();

        if (connectorStatus != null) {
            statusResult = connectorStatus.statusResult;
            JsonObject statusResultJson = new JsonObject(statusResult);
            List<String> failedTaskIds;
            if (connectorHasFailed(statusResultJson)) {
                connectorReadiness = Future.failedFuture(new Throwable("Connector has failed, see connectorStatus for more details."));
            } else if (!(failedTaskIds = failedTaskIds(statusResultJson)).isEmpty()) {
                connectorReadiness = Future.failedFuture(new Throwable(String.format("The following tasks have failed: %s, see connectorStatus for more details.", String.join(", ", failedTaskIds))));
            }
            topics = connectorStatus.topics.stream().sorted().collect(Collectors.toList());
            conditions.addAll(connectorStatus.conditions);
            autoRestart = connectorStatus.autoRestart;
        }

        Set<Condition> unknownAndDeprecatedConditions = validate(reconciliation, connector);
        conditions.addAll(unknownAndDeprecatedConditions);

        if (!Annotations.isReconciliationPausedWithAnnotation(connector)) {
            StatusUtils.setStatusConditionAndObservedGeneration(connector, status, error != null ? error : connectorReadiness.cause());
            status.setConnectorStatus(statusResult);
            status.setTasksMax(getActualTaskCount(connector, statusResult));
            status.setTopics(topics);
            status.setAutoRestart(autoRestart);
        } else {
            status.setObservedGeneration(connector.getStatus() != null ? connector.getStatus().getObservedGeneration() : 0);
            conditions.add(StatusUtils.getPausedCondition());
        }
        status.addConditions(conditions);

        return maybeUpdateStatusCommon(connectorOperator, connector, reconciliation, status,
            (connector1, status1) -> new KafkaConnectorBuilder(connector1).withStatus(status1).build());
    }

    // Methods for working with connector restarts

    /**
     * Checks whether the provided KafkaConnector resource and has the strimzi.io/restart annotation.
     *
     * @param resource          KafkaConnector resource instance to check
     * @param connectorName     Connector name of the connector to check (not used in this method, but used in other implementations)
     *
     * @return  True if the KafkaConnector resource has the strimzi.io/restart annotation. False otherwise.
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected boolean hasRestartAnnotation(CustomResource resource, String connectorName) {
        return Annotations.booleanAnnotation(resource, ANNO_STRIMZI_IO_RESTART, false);
    }

    /**
     * Return the ID of the connector task to be restarted if the provided KafkaConnector resource instance has the strimzi.io/restart-task annotation
     *
     * @param resource          KafkaConnector resource instance to check
     * @param connectorName     Connector name of the connector to check (not used in this method, but used in other implementations)
     *
     * @return  The ID of the task to be restarted if the provided KafkaConnector resource instance has the strimzi.io/restart-task annotation or -1 otherwise.
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected int getRestartTaskAnnotationTaskID(CustomResource resource, String connectorName) {
        return Annotations.intAnnotation(resource, ANNO_STRIMZI_IO_RESTART_TASK, -1);
    }

    /**
     * Patches the custom resource to remove the restart annotation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaConnector resource from which the annotation should be removed
     *
     * @return  Future that indicates the operation completion
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected Future<Void> removeRestartAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaConnector) resource, ANNO_STRIMZI_IO_RESTART);
    }

    /**
     * Patches the custom resource to remove the restart task annotation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaConnector resource from which the annotation should be removed
     *
     * @return  Future that indicates the operation completion
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected Future<Void> removeRestartTaskAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaConnector) resource, ANNO_STRIMZI_IO_RESTART_TASK);
    }

    /**
     * Patches the KafkaConnector CR to remove the supplied annotation.
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaConnector resource from which the annotation should be removed
     * @param annotationKey     Annotation that should be removed
     *
     * @return  Future that indicates the operation completion
     */
    private Future<Void> removeAnnotation(Reconciliation reconciliation, KafkaConnector resource, String annotationKey) {
        LOGGER.debugCr(reconciliation, "Removing annotation {}", annotationKey);
        KafkaConnector patchedKafkaConnector = new KafkaConnectorBuilder(resource)
                .editMetadata()
                .removeFromAnnotations(annotationKey)
                .endMetadata()
                .build();
        return connectorOperator.patchAsync(reconciliation, patchedKafkaConnector)
                .mapEmpty();
    }

    /**
     * Returns the previous auto-restart status with the information about the previous restarts (number of restarts and
     * last restart timestamp). For Kafka Connect it queries the Kubernetes API to get the latest KafkaConnector
     * resource and get the status from it.
     *
     * @param reconciliation    Reconciliation marker
     * @param connectorName     Name of the connector for which the restart should be returned (not used for Kafka
     *                          Connect, used only for Mirror Maker 2)
     * @param resource          The KafkaConnector custom resource that configures the connector
     *
     * @return  The previous auto-restart status
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected Future<AutoRestartStatus> previousAutoRestartStatus(Reconciliation reconciliation, String connectorName, CustomResource resource)  {
        return connectorOperator.getAsync(resource.getMetadata().getNamespace(), resource.getMetadata().getName())
                .compose(result -> {
                    if (result != null) {
                        return Future.succeededFuture(result.getStatus() != null ? result.getStatus().getAutoRestart() : null);
                    } else {
                        // This is unexpected, since we are reconciling the resource and therefore it should exist. So the
                        // result being null suggests some race condition. We log a warning and return null which is handled
                        // in the place calling this method.
                        LOGGER.warnCr(reconciliation, "Kafka Connector {}/{} was not found", resource.getMetadata().getNamespace(), resource.getMetadata().getName());
                        return Future.succeededFuture(null);
                    }
                });
    }

    // Methods for working with connector offsets

    /**
     * Returns the operation to perform for connector offsets of the provided custom resource.
     * For KafkaConnector returns the value of strimzi.io/connector-offsets annotation on the provided KafkaConnector.
     *
     * @param resource          KafkaConnector resource instance to check
     * @param connectorName     Name of the connector being reconciled (not used for Kafka Connect, used only for Mirror Maker 2)
     *
     * @return  The operation to perform for connector offsets of the provided KafkaConnector.
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected KafkaConnectorOffsetsAnnotation getConnectorOffsetsOperation(CustomResource resource, String connectorName) {
        String annotation = Annotations.stringAnnotation(resource, ANNO_STRIMZI_IO_CONNECTOR_OFFSETS, KafkaConnectorOffsetsAnnotation.none.toString());
        return KafkaConnectorOffsetsAnnotation.valueOf(annotation);
    }

    /**
     * Patches the custom resource to remove the connector-offsets annotation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaConnector resource from which the annotation should be removed
     *
     * @return  Future that indicates the operation completion
     */
    @SuppressWarnings({ "rawtypes" })
    @Override
    protected Future<Void> removeConnectorOffsetsAnnotations(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaConnector) resource, ANNO_STRIMZI_IO_CONNECTOR_OFFSETS);
    }

    /**
     * Returns the key to use for either writing connector offsets to a ConfigMap or fetching connector offsets
     * from a ConfigMap.
     *
     * @param connectorName Name of the connector that is being managed (not used for Kafka Connect, used only for Mirror Maker 2).
     *
     * @return The String to use when interacting with ConfigMap resources.
     */
    @Override
    protected String getConnectorOffsetsConfigMapEntryKey(String connectorName) {
        return "offsets.json";
    }

    // Static utility methods and classes

    /**
     * Update the status of the Kafka Connector custom resource
     *
     * @param reconciliation        Reconciliation marker
     * @param error                 Throwable indicating if any errors occurred during the reconciliation
     * @param kafkaConnector2       Latest version of the KafkaConnector resource where the status should be updated
     * @param connectorOperations   The KafkaConnector operations for updating the status
     */
    private static void updateStatus(Reconciliation reconciliation, Throwable error, KafkaConnector kafkaConnector2, CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperations) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnector2, status, error);
        StatusDiff diff = new StatusDiff(kafkaConnector2.getStatus(), status);
        if (!diff.isEmpty()) {
            KafkaConnector copy = new KafkaConnectorBuilder(kafkaConnector2).build();
            copy.setStatus(status);
            connectorOperations.updateStatusAsync(reconciliation, copy);
        }
    }

    /**
     * Validates the KafkaConnector resource and returns any warning conditions find during the validation
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          KafkaConnector resource which should be validated
     *
     * @return  Set with the warning conditions. Empty set if no conditions were found.
     */
    private static Set<Condition> validate(Reconciliation reconciliation, KafkaConnector resource) {
        if (resource != null) {
            return StatusUtils.validate(reconciliation, resource);
        }

        return Collections.emptySet();
    }

    /**
     * The tasksMax are mirrored in the KafkaConnector status section where they are used by the scale subresource.
     * However, .spec.tasksMax is not always set and has no default value in Strimzi (only in Kafka Connect). So when
     * it is not set, we try to count the tasks from the status. And if these are missing as well, we just set it to 0.
     *
     * @param connector         The KafkaConnector instance of the reconciled connector
     * @param statusResult      The status from the Connect REST API
     * @return                  Number of tasks which should be set in the status
     */
    @SuppressWarnings({ "rawtypes" })
    private static int getActualTaskCount(KafkaConnector connector, Map<String, Object> statusResult)  {
        if (connector.getSpec() != null
                && connector.getSpec().getTasksMax() != null)  {
            return connector.getSpec().getTasksMax();
        } else if (statusResult != null
                && statusResult.containsKey("tasks")
                && statusResult.get("tasks") instanceof List) {
            return ((List) statusResult.get("tasks")).size();
        } else {
            return 0;
        }
    }

    /**
     * Checks whether the use of KafkaConnector resources is enabled for this cluster or not
     *
     * @param connect   The Connect custom resource
     *
     * @return  True if the connector operator is enabled and KafkaConnector resources should be used. False otherwise.
     */
    private static boolean isUseResources(KafkaConnect connect) {
        return Annotations.booleanAnnotation(connect, Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, false);
    }

    /**
     * Creates an exception indicating that the KafkaConnect resource which would match given KafkaConnector resource
     * does not exist.
     *
     * @param connectNamespace  Namespace where the KafkaConnect resource is missing
     * @param connectName       Name of the missing KafkaConnect resource
     *
     * @return  NoSuchResourceException indicating that the Connect cluster is missing
     */
    private static NoSuchResourceException noConnectCluster(String connectNamespace, String connectName) {
        return new NoSuchResourceException("KafkaConnect resource '" + connectName + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + connectNamespace + ".");
    }

    /**
     * Creates an exception indicating that the connector cannot run because its Connect cluster has 0 replicas.
     *
     * @param connectNamespace  Namespace where the KafkaConnect resource exists
     * @param connectName       Name of the KafkaConnect resource
     *
     * @return  NoSuchResourceException indicating that the Connect cluster is missing
     */
    private static RuntimeException zeroReplicas(String connectNamespace, String connectName) {
        return new RuntimeException("Kafka Connect cluster '" + connectName + "' in namespace " + connectNamespace + " has 0 replicas.");
    }

    private static boolean isPaused(KafkaConnectorStatus status) {
        return status != null && status.getConditions().stream().anyMatch(condition -> "ReconciliationPaused".equals(condition.getType()));
    }
}
