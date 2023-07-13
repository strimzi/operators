/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaNodePoolList;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KRaftUtils;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static io.strimzi.operator.common.VertxUtil.async;

/**
 * Assembly operator for the Kafka custom resource. It manages the following components:
 *   - ZooKeeper cluster
 *   - Kafka cluster
 *   - Entity operator
 *   - Cruise Control
 *   - Kafka Exporter
 */
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaList, Resource<Kafka>, KafkaSpec, KafkaStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAssemblyOperator.class.getName());

    /* test */ final ClusterOperatorConfig config;
    /* test */ final ResourceOperatorSupplier supplier;

    private final FeatureGates featureGates;

    private final StatefulSetOperator stsOperations;
    private final CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> nodePoolOperator;
    protected Clock clock;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param certManager Certificate manager
     * @param passwordGenerator Password generator
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                 CertManager certManager, PasswordGenerator passwordGenerator,
                                 ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
        super(vertx, pfa, Kafka.RESOURCE_KIND, certManager, passwordGenerator,
                supplier.kafkaOperator, supplier, config);
        this.config = config;
        this.supplier = supplier;

        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.featureGates = config.featureGates();
        this.stsOperations = supplier.stsOperations;
        this.kafkaOperator = supplier.kafkaOperator;
        this.nodePoolOperator = supplier.kafkaNodePoolOperator;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.clock = Clock.systemUTC();
    }

    @Override
    public Future<KafkaStatus> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
        Promise<KafkaStatus> createOrUpdatePromise = Promise.promise();
        ReconciliationState reconcileState = createReconciliationState(reconciliation, kafkaAssembly);

        reconcile(reconcileState).onComplete(reconcileResult -> {
            KafkaStatus status = reconcileState.kafkaStatus;
            Condition condition;

            if (kafkaAssembly.getMetadata().getGeneration() != null)    {
                status.setObservedGeneration(kafkaAssembly.getMetadata().getGeneration());
            }

            if (status.getClusterId() == null
                    && kafkaAssembly.getStatus() != null
                    && kafkaAssembly.getStatus().getClusterId() != null)  {
                // If not set in the status prepared by reconciliation but set in the status previously, we copy the
                // cluster ID into the new status. This is useful for example when the reconciliation fails for some
                // reason before setting the cluster ID
                status.setClusterId(kafkaAssembly.getStatus().getClusterId());
            }

            if (reconcileResult.succeeded())    {
                condition = new ConditionBuilder()
                        .withLastTransitionTime(StatusUtils.iso8601(clock.instant()))
                        .withType("Ready")
                        .withStatus("True")
                        .build();

                status.addCondition(condition);
                createOrUpdatePromise.complete(status);
            } else {
                condition = new ConditionBuilder()
                        .withLastTransitionTime(StatusUtils.iso8601(clock.instant()))
                        .withType("NotReady")
                        .withStatus("True")
                        .withReason(reconcileResult.cause().getClass().getSimpleName())
                        .withMessage(reconcileResult.cause().getMessage())
                        .build();

                status.addCondition(condition);
                createOrUpdatePromise.fail(new ReconciliationException(status, reconcileResult.cause()));
            }
        });

        return createOrUpdatePromise.future();
    }

    Future<Void> reconcile(ReconciliationState reconcileState)  {
        Promise<Void> chainPromise = Promise.promise();

        if (featureGates.useKRaftEnabled()) {
            // Makes sure KRaft is used only with KafkaNodePool custom resources and not with virtual node pools
            if (featureGates.kafkaNodePoolsEnabled()
                    && !ReconcilerUtils.nodePoolsEnabled(reconcileState.kafkaAssembly))  {
                throw new InvalidConfigurationException("The UseKRaft feature gate can be used only together with a Kafka cluster based on the KafkaNodePool resources.");
            }

            // Validates features which are currently not supported in KRaft mode
            try {
                KRaftUtils.validateKafkaCrForKRaft(reconcileState.kafkaAssembly.getSpec(), featureGates.unidirectionalTopicOperatorEnabled());
            } catch (InvalidResourceException e)    {
                return Future.failedFuture(e);
            }
        }

        reconcileState.initialStatus()
                // Preparation steps => prepare cluster descriptions, handle CA creation or changes
                .compose(state -> state.reconcileCas(clock))
                .compose(state -> state.versionChange())

                // Run reconciliations of the different components
                .compose(state -> featureGates.useKRaftEnabled() ? Future.succeededFuture(state) : state.reconcileZooKeeper(clock))
                .compose(state -> state.reconcileKafka(clock))
                .compose(state -> state.reconcileEntityOperator(clock))
                .compose(state -> state.reconcileCruiseControl(clock))
                .compose(state -> state.reconcileKafkaExporter(clock))
                .compose(state -> state.reconcileJmxTrans())

                // Finish the reconciliation
                .map((Void) null)
                .onComplete(chainPromise);

        return chainPromise.future();
    }

    ReconciliationState createReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
        return new ReconciliationState(reconciliation, kafkaAssembly);
    }

    /**
     * Hold the mutable state during a reconciliation
     */
    class ReconciliationState {
        private final String namespace;
        private final String name;
        private final Kafka kafkaAssembly;
        private final Reconciliation reconciliation;

        /* test */ KafkaVersionChange versionChange;

        /* test */ ClusterCa clusterCa;
        /* test */ ClientsCa clientsCa;

        // Needed by Cruise control to configure the cluster, its nodes and their storage and resource configuration
        private Set<NodeRef> kafkaBrokerNodes;
        private Map<String, Storage> kafkaBrokerStorage;
        private Map<String, ResourceRequirements> kafkaBrokerResources;

        /* test */ KafkaStatus kafkaStatus = new KafkaStatus();

        ReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            this.reconciliation = reconciliation;
            this.kafkaAssembly = kafkaAssembly;
            this.namespace = kafkaAssembly.getMetadata().getNamespace();
            this.name = kafkaAssembly.getMetadata().getName();
        }

        /**
         * Updates the Status field of the Kafka CR. It diffs the desired status against the current status and calls
         * the update only when there is any difference in non-timestamp fields.
         *
         * @param desiredStatus The KafkaStatus which should be set
         *
         * @return  Future which completes when the status subresource is updated
         */
        Future<Void> updateStatus(KafkaStatus desiredStatus) {
            Promise<Void> updateStatusPromise = Promise.promise();

            kafkaOperator.getAsync(namespace, name).onComplete(getRes -> {
                if (getRes.succeeded())    {
                    Kafka kafka = getRes.result();

                    if (kafka != null) {
                        if ((Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1ALPHA1).equals(kafka.getApiVersion()))   {
                            LOGGER.warnCr(reconciliation, "The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", kafka.getApiVersion());
                            updateStatusPromise.complete();
                        } else {
                            KafkaStatus currentStatus = kafka.getStatus();

                            StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                            if (!ksDiff.isEmpty()) {
                                Kafka resourceWithNewStatus = new KafkaBuilder(kafka).withStatus(desiredStatus).build();

                                kafkaOperator.updateStatusAsync(reconciliation, resourceWithNewStatus).onComplete(updateRes -> {
                                    if (updateRes.succeeded()) {
                                        LOGGER.debugCr(reconciliation, "Completed status update");
                                        updateStatusPromise.complete();
                                    } else {
                                        LOGGER.errorCr(reconciliation, "Failed to update status", updateRes.cause());
                                        updateStatusPromise.fail(updateRes.cause());
                                    }
                                });
                            } else {
                                LOGGER.debugCr(reconciliation, "Status did not change");
                                updateStatusPromise.complete();
                            }
                        }
                    } else {
                        LOGGER.errorCr(reconciliation, "Current Kafka resource not found");
                        updateStatusPromise.fail("Current Kafka resource not found");
                    }
                } else {
                    LOGGER.errorCr(reconciliation, "Failed to get the current Kafka resource and its status", getRes.cause());
                    updateStatusPromise.fail(getRes.cause());
                }
            });

            return updateStatusPromise.future();
        }

        /**
         * Sets the initial status when the Kafka resource is created and the cluster starts deploying.
         *
         * @return  Future which returns when the initial state is set
         */
        Future<ReconciliationState> initialStatus() {
            Promise<ReconciliationState> initialStatusPromise = Promise.promise();

            kafkaOperator.getAsync(namespace, name).onComplete(getRes -> {
                if (getRes.succeeded())    {
                    Kafka kafka = getRes.result();

                    if (kafka != null && kafka.getStatus() == null) {
                        LOGGER.debugCr(reconciliation, "Setting the initial status for a new resource");

                        Condition deployingCondition = new ConditionBuilder()
                                .withLastTransitionTime(StatusUtils.iso8601(clock.instant()))
                                .withType("NotReady")
                                .withStatus("True")
                                .withReason("Creating")
                                .withMessage("Kafka cluster is being deployed")
                                .build();

                        KafkaStatus initialStatus = new KafkaStatusBuilder()
                                .addToConditions(deployingCondition)
                                .build();

                        updateStatus(initialStatus).map(this).onComplete(initialStatusPromise);
                    } else {
                        LOGGER.debugCr(reconciliation, "Status is already set. No need to set initial status");
                        initialStatusPromise.complete(this);
                    }
                } else {
                    LOGGER.errorCr(reconciliation, "Failed to get the current Kafka resource and its status", getRes.cause());
                    initialStatusPromise.fail(getRes.cause());
                }
            });

            return initialStatusPromise.future();
        }

        private Storage getOldStorage(HasMetadata sts)  {
            Storage storage = null;

            if (sts != null)    {
                String jsonStorage = Annotations.stringAnnotation(sts, Annotations.ANNO_STRIMZI_IO_STORAGE, null);

                if (jsonStorage != null)    {
                    storage = ModelUtils.decodeStorageFromJson(jsonStorage);
                }
            }

            return storage;
        }

        /**
         * Utility method to extract current number of replicas from an existing StatefulSet
         *
         * @param sts   StatefulSet from which the replicas count should be extracted
         *
         * @return      Number of replicas
         */
        private int currentReplicas(StatefulSet sts)  {
            if (sts != null && sts.getSpec() != null)   {
                return sts.getSpec().getReplicas();
            } else {
                return 0;
            }
        }

        /**
         * Utility method to extract current number of replicas from an existing StrimziPodSet
         *
         * @param podSet    PodSet from which the replicas count should be extracted
         *
         * @return          Number of replicas
         */
        private int currentReplicas(StrimziPodSet podSet)  {
            if (podSet != null && podSet.getSpec() != null && podSet.getSpec().getPods() != null)   {
                return podSet.getSpec().getPods().size();
            } else {
                return 0;
            }
        }

        /**
         * Provider method for CaReconciler. Overriding this method can be used to get mocked creator.
         *
         * @return  CaReconciler instance
         */
        CaReconciler caReconciler()   {
            return new CaReconciler(reconciliation, kafkaAssembly, config, supplier, vertx, certManager, passwordGenerator);
        }

        /**
         * Creates the CaReconciler instance and reconciles the Clients and Cluster CAs. The resulting CAs are stored
         * in the ReconciliationState and used later to reconcile the operands.
         *
         * @param clock     The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *                  That time is used for checking maintenance windows
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileCas(Clock clock)    {
            return caReconciler()
                    .reconcile(clock)
                    .compose(cas -> {
                        this.clusterCa = cas.clusterCa();
                        this.clientsCa = cas.clientsCa();
                        return Future.succeededFuture(this);
                    });
        }

        /**
         * Provider method for VersionChangeCreator. Overriding this method can be used to get mocked creator.
         *
         * @return  VersionChangeCreator instance
         */
        VersionChangeCreator versionChangeCreator()   {
            return new VersionChangeCreator(reconciliation, kafkaAssembly, config, supplier);
        }

        /**
         * Creates the KafkaVersionChange instance describing the version changes in this reconciliation.
         *
         * @return  Future with Reconciliation State
         */
        Future<ReconciliationState> versionChange()    {
            return versionChangeCreator()
                    .reconcile()
                    .compose(versionChange -> {
                        this.versionChange = versionChange;
                        return Future.succeededFuture(this);
                    });
        }

        /**
         * Provider method for ZooKeeper reconciler. Overriding this method can be used to get mocked reconciler. This
         * method has to first collect some information about the current ZooKeeper cluster such as current storage
         * configuration or current number of replicas.
         *
         * @return  Future with ZooKeeper reconciler
         */
        Future<ZooKeeperReconciler> zooKeeperReconciler()   {
            Future<StatefulSet> stsFuture = stsOperations.getAsync(namespace, KafkaResources.zookeeperStatefulSetName(name));
            Future<StrimziPodSet> podSetFuture = strimziPodSetOperator.getAsync(namespace, KafkaResources.zookeeperStatefulSetName(name));

            return Future.join(stsFuture, podSetFuture)
                    .compose(res -> {
                        StatefulSet sts = res.resultAt(0);
                        StrimziPodSet podSet = res.resultAt(1);

                        int currentReplicas = 0;
                        Storage oldStorage = null;

                        if (sts != null && podSet != null)  {
                            // Both StatefulSet and PodSet exist => we use the StrimziPodSet as that is the main controller resource
                            oldStorage = getOldStorage(podSet);
                            currentReplicas = currentReplicas(podSet);
                        } else if (podSet != null) {
                            // PodSet exists, StatefulSet does not => we create the description from the PodSet
                            oldStorage = getOldStorage(podSet);
                            currentReplicas = currentReplicas(podSet);
                        } else if (sts != null) {
                            // StatefulSet exists, PodSet does not exist => we create the description from the StatefulSet
                            oldStorage = getOldStorage(sts);
                            currentReplicas = currentReplicas(sts);
                        }

                        ZooKeeperReconciler reconciler = new ZooKeeperReconciler(
                                reconciliation,
                                vertx,
                                config,
                                supplier,
                                pfa,
                                kafkaAssembly,
                                versionChange,
                                oldStorage,
                                currentReplicas,
                                clusterCa
                        );

                        return Future.succeededFuture(reconciler);
                    });
        }

        /**
         * Run the reconciliation pipeline for the ZooKeeper
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileZooKeeper(Clock clock)    {
            return zooKeeperReconciler()
                    .compose(reconciler -> reconciler.reconcile(kafkaStatus, clock))
                    .map(this);
        }

        /**
         * Provider method for Kafka reconciler. Overriding this method can be used to get mocked reconciler. This
         * method expects that the information about current storage and replicas are collected and passed as arguments.
         * Overriding this method can be used to get mocked reconciler.
         *
         * @param nodePools         List of node pools belonging to this cluster
         * @param oldStorage        Map of current storage configurations of the running cluster. Empty if the PodSet
         *                          (or StatefulSet doesn't exist yet).
         * @param currentPods       Map of lists with the existing pod names for different StrimziPodSets. Empty if the
         *                          PodSet (or StatefulSet doesn't exist yet)
         *
         * @return  KafkaReconciler instance
         */
        KafkaReconciler kafkaReconciler(List<KafkaNodePool> nodePools, Map<String, Storage> oldStorage, Map<String, List<String>> currentPods) {
            return new KafkaReconciler(
                    reconciliation,
                    kafkaAssembly,
                    nodePools,
                    oldStorage,
                    currentPods,
                    clusterCa,
                    clientsCa,
                    versionChange,
                    config,
                    supplier,
                    pfa,
                    vertx
            );
        }

        /**
         * Provider method for Kafka reconciler. Overriding this method can be used to get mocked reconciler. This
         * method has to first collect some information about the current Kafka cluster such as current storage
         * configuration or current number of replicas.
         *
         * @return  Future with Kafka reconciler
         */
        Future<KafkaReconciler> kafkaReconciler()   {
            Labels kafkaSelectorLabels = Labels.EMPTY
                    .withStrimziKind(reconciliation.kind())
                    .withStrimziCluster(reconciliation.name())
                    .withStrimziName(KafkaResources.kafkaStatefulSetName(reconciliation.name()));

            Future<List<KafkaNodePool>> nodePoolFuture;
            if (featureGates.kafkaNodePoolsEnabled()
                    && ReconcilerUtils.nodePoolsEnabled(kafkaAssembly)) {
                // Node Pools are enabled
                nodePoolFuture = nodePoolOperator.listAsync(namespace, Labels.fromMap(Map.of(Labels.STRIMZI_CLUSTER_LABEL, name)));
            } else {
                nodePoolFuture = Future.succeededFuture(null);
            }

            Future<StatefulSet> stsFuture = stsOperations.getAsync(namespace, KafkaResources.kafkaStatefulSetName(name));
            Future<List<StrimziPodSet>> podSetFuture = strimziPodSetOperator.listAsync(namespace, kafkaSelectorLabels);

            return Future.join(stsFuture, podSetFuture, nodePoolFuture)
                    .compose(res -> {
                        StatefulSet sts = res.resultAt(0);
                        List<StrimziPodSet> podSets = res.resultAt(1);
                        List<KafkaNodePool> nodePools = res.resultAt(2);

                        if (config.featureGates().kafkaNodePoolsEnabled()
                                && ReconcilerUtils.nodePoolsEnabled(kafkaAssembly)
                                && (nodePools == null || nodePools.isEmpty()))  {
                            throw new InvalidConfigurationException("KafkaNodePools are enabled, but not pools found for Kafka cluster " + name);
                        }

                        Map<String, List<String>> currentPods = new HashMap<>();
                        Map<String, Storage> oldStorage = new HashMap<>();

                        if (podSets != null && !podSets.isEmpty())  {
                            // One or more PodSets exist => we go on and use them
                            for (StrimziPodSet podSet : podSets)    {
                                oldStorage.put(podSet.getMetadata().getName(), getOldStorage(podSet));
                                currentPods.put(podSet.getMetadata().getName(), PodSetUtils.podNames(podSet));
                            }
                        } else if (sts != null) {
                            // StatefulSet exists, PodSet does not exist => we create the description from the StatefulSet
                            oldStorage.put(sts.getMetadata().getName(), getOldStorage(sts));
                            // We generate the list of existing pod names based on the replica count
                            currentPods.put(sts.getMetadata().getName(), IntStream.range(0, sts.getSpec().getReplicas()).mapToObj(i -> KafkaResources.kafkaPodName(kafkaAssembly.getMetadata().getName(), i)).toList());
                        }

                        KafkaReconciler reconciler = kafkaReconciler(nodePools, oldStorage, currentPods);

                        // We store this for use with Cruise Control later
                        kafkaBrokerNodes = reconciler.kafkaBrokerNodes();
                        kafkaBrokerStorage = reconciler.kafkaStorage();
                        kafkaBrokerResources = reconciler.kafkaBrokerResourceRequirements();

                        return Future.succeededFuture(reconciler);
                    });
        }

        /**
         * Run the reconciliation pipeline for Kafka
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileKafka(Clock clock)    {
            return kafkaReconciler()
                    .compose(reconciler -> reconciler.reconcile(kafkaStatus, clock))
                    .map(this);
        }

        /**
         * Provider method for Kafka Exporter reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  Kafka Exporter reconciler
         */
        KafkaExporterReconciler kafkaExporterReconciler()   {
            return new KafkaExporterReconciler(
                    reconciliation,
                    config,
                    supplier,
                    kafkaAssembly,
                    versions,
                    clusterCa
            );
        }

        /**
         * Run the reconciliation pipeline for the Kafka Exporter
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileKafkaExporter(Clock clock)    {
            return kafkaExporterReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, clock)
                    .map(this);
        }

        /**
         * Provider method for JMX Trans reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  JMX Trans reconciler
         */
        JmxTransReconciler jmxTransReconciler()   {
            return new JmxTransReconciler(
                    reconciliation,
                    supplier
            );
        }

        /**
         * Run the reconciliation pipeline for the JMX Trans
         *
         * @return              Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileJmxTrans()    {
            return jmxTransReconciler()
                    .reconcile()
                    .map(this);
        }

        /**
         * Provider method for Cruise Control reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  Cruise Control reconciler
         */
        CruiseControlReconciler cruiseControlReconciler()   {
            return new CruiseControlReconciler(
                    reconciliation,
                    config,
                    supplier,
                    kafkaAssembly,
                    versions,
                    kafkaBrokerNodes,
                    kafkaBrokerStorage,
                    kafkaBrokerResources,
                    clusterCa
            );
        }

        /**
         * Run the reconciliation pipeline for the Cruise Control
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileCruiseControl(Clock clock)    {
            return cruiseControlReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, clock)
                    .map(this);
        }

        /**
         * Provider method for Entity Operator reconciler. Overriding this method can be used to get mocked reconciler.
         *
         * @return  Entity Operator reconciler
         */
        EntityOperatorReconciler entityOperatorReconciler()   {
            return new EntityOperatorReconciler(
                    reconciliation,
                    config,
                    supplier,
                    kafkaAssembly,
                    versions,
                    clusterCa
            );
        }

        /**
         * Run the reconciliation pipeline for the Entity Operator
         *
         * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
         *              That time is used for checking maintenance windows
         *
         * @return      Future with Reconciliation State
         */
        Future<ReconciliationState> reconcileEntityOperator(Clock clock)    {
            return entityOperatorReconciler()
                    .reconcile(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, clock)
                    .map(this);
        }
    }

    @Override
    protected KafkaStatus createStatus(Kafka kafka) {
        KafkaStatus status = new KafkaStatus();

        // We copy the cluster ID if set
        if (kafka.getStatus() != null && kafka.getStatus().getClusterId() != null)  {
            status.setClusterId(kafka.getStatus().getClusterId());
        }

        return status;
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     * @param reconciliation    The Reconciliation identification
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return ReconcilerUtils.withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null)
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }

    /**
     * Create Kubernetes watch for KafkaNodePool resources.
     *
     * @param namespace     Namespace where to watch for the resources.
     *
     * @return  A future which completes when the watcher has been created.
     */
    public Future<Watch> createNodePoolWatch(String namespace) {
        return async(vertx, () -> nodePoolOperator.watch(namespace, Optional.empty(), new KafkaNodePoolWatcher(namespace, selector(), this, kafkaOperator, recreateNodePoolWatch(namespace))));
    }

    /**
     * Recreates a Kubernetes watch for KafkaNodePool resources
     *
     * @param namespace     Namespace which should be watched
     *
     * @return  Consumer for a Watched exception
     */
    public Consumer<WatcherException> recreateNodePoolWatch(String namespace) {
        return new Consumer<>() {
            @Override
            public void accept(WatcherException e) {
                if (e != null) {
                    LOGGER.errorOp("KafkaNodePool watcher closed with exception in namespace {}", namespace, e);
                    createWatch(namespace, this);
                } else {
                    LOGGER.infoOp("KafkaNodePool watcher closed in namespace {}", namespace);
                }
            }
        };
    }
}
