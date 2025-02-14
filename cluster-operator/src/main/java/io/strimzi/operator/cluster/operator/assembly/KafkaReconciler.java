/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.UsedNodePoolStatus;
import io.strimzi.api.kafka.model.kafka.UsedNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatus;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddress;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.MetricsAndLogging;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
//import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NodeOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StorageClassOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.cluster.operator.resource.rolling.RackRolling;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
//import io.strimzi.operator.common.e;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NodeUtils;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_SERVER_CERT_HASH;

/**
 * Class used for reconciliation of Kafka. This class contains both the steps of the Kafka
 * reconciliation pipeline (although the steps for listener reconciliation are outsourced to the KafkaListenerReconciler)
 * and is also used to store the state between them.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class KafkaReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaReconciler.class.getName());

    // Various settings
    private final long operationTimeoutMs;
    private final int maxRestartBatchSize;
    private final boolean isNetworkPolicyGeneration;
    private final boolean isPodDisruptionBudgetGeneration;
    private final boolean isKafkaNodePoolsEnabled;
    private final List<String> maintenanceWindows;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final PlatformFeaturesAvailability pfa;
    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;
    private final List<Integer> previousNodeIds;

    // Objects used during the reconciliation
    /* test */ final Reconciliation reconciliation;
    private final KafkaCluster kafka;
    private final List<KafkaNodePool> kafkaNodePoolCrs;
    private final ClusterCa clusterCa;
    private final ClientsCa clientsCa;

    // Tools for operating and managing various resources
    private final Vertx vertx;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final ServiceOperator serviceOperator;
    private final PvcOperator pvcOperator;
    private final StorageClassOperator storageClassOperator;
    private final ConfigMapOperator configMapOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    private final PodOperator podOperator;
    private final ClusterRoleBindingOperator clusterRoleBindingOperator;
    private final RouteOperator routeOperator;
    private final IngressOperator ingressOperator;
    private final NodeOperator nodeOperator;
    private final CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> kafkaNodePoolOperator;
    private final KubernetesRestartEventPublisher eventsPublisher;
    private final AdminClientProvider adminClientProvider;
    private final KafkaAgentClientProvider kafkaAgentClientProvider;

    // State of the reconciliation => these objects might change during the reconciliation (the collection objects are
    // marked as final, but their contents is modified during the reconciliation)
    private final Set<String> fsResizingRestartRequest = new HashSet<>();

    private final boolean continueOnManualRUFailure;

    private String logging = "";
    private final Map<Integer, String> brokerLoggingHash = new HashMap<>();
    private final Map<Integer, String> brokerConfigurationHash = new HashMap<>();
    private final Map<Integer, String> kafkaServerCertificateHash = new HashMap<>();
    private final List<String> secretsToDelete = new ArrayList<>();
    /* test */ TlsPemIdentity coTlsPemIdentity;
    /* test */ KafkaListenersReconciler.ReconciliationResult listenerReconciliationResults; // Result of the listener reconciliation with the listener details

    private final KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus;

    /**
     * Constructs the Kafka reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param kafkaCr                   The Kafka custom resource
     * @param nodePools                 List of KafkaNodePool resources belonging to this cluster
     * @param kafka                     Kafka cluster instance
     * @param clusterCa                 The Cluster CA instance
     * @param clientsCa                 The Clients CA instance
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param pfa                       PlatformFeaturesAvailability describing the environment we run in
     * @param vertx                     Vert.x instance
     */
    public KafkaReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            List<KafkaNodePool> nodePools,
            KafkaCluster kafka,
            ClusterCa clusterCa,
            ClientsCa clientsCa,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            PlatformFeaturesAvailability pfa,
            Vertx vertx
    ) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.maxRestartBatchSize = config.getMaxRestartBatchSize();
        this.kafkaNodePoolCrs = nodePools;
        this.kafka = kafka;

        this.clusterCa = clusterCa;
        this.clientsCa = clientsCa;
        this.maintenanceWindows = kafkaCr.getSpec().getMaintenanceTimeWindows();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.isKafkaNodePoolsEnabled = ReconcilerUtils.nodePoolsEnabled(kafkaCr);
        this.pfa = pfa;
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.previousNodeIds = kafkaCr.getStatus() != null ? kafkaCr.getStatus().getRegisteredNodeIds() : null;
        this.isPodDisruptionBudgetGeneration = config.isPodDisruptionBudgetGeneration();
        this.kafkaAutoRebalanceStatus = kafkaCr.getStatus() != null ? kafkaCr.getStatus().getAutoRebalance() : null;

        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.serviceOperator = supplier.serviceOperations;
        this.pvcOperator = supplier.pvcOperations;
        this.storageClassOperator = supplier.storageClassOperations;
        this.configMapOperator = supplier.configMapOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.podOperator = supplier.podOperations;
        this.clusterRoleBindingOperator = supplier.clusterRoleBindingOperator;
        this.routeOperator = supplier.routeOperations;
        this.ingressOperator = supplier.ingressOperations;
        this.nodeOperator = supplier.nodeOperator;
        this.kafkaNodePoolOperator = supplier.kafkaNodePoolOperator;
        this.eventsPublisher = supplier.restartEventsPublisher;

        this.adminClientProvider = supplier.adminClientProvider;
        this.kafkaAgentClientProvider = supplier.kafkaAgentClientProvider;
        this.continueOnManualRUFailure = config.featureGates().continueOnManualRUFailureEnabled();
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param kafkaStatus   The Kafka Status class for adding conditions to it during the reconciliation
     * @param clock         The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                      That time is used for checking maintenance windows
     *
     * @return              Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
        return modelWarnings(kafkaStatus)
                .compose(i -> initClientAuthenticationCertificates())
                .compose(i -> manualPodCleaning())
                .compose(i -> networkPolicy())
                .compose(i -> updateKafkaAutoRebalanceStatus(kafkaStatus))
                .compose(i -> manualRollingUpdate())
                .compose(i -> pvcs(kafkaStatus))
                .compose(i -> serviceAccount())
                .compose(i -> initClusterRoleBinding())
                .compose(i -> scaleDown())
                .compose(i -> updateNodePoolStatuses(kafkaStatus))
                .compose(i -> listeners())
                .compose(i -> certificateSecrets(clock))
                .compose(i -> brokerConfigurationConfigMaps())
                .compose(i -> jmxSecret())
                .compose(i -> podDisruptionBudget())
                .compose(i -> podSet())
                .compose(podSetDiffs -> rollingUpdate(podSetDiffs)) // We pass the PodSet reconciliation result this way to avoid storing it in the instance
                .compose(i -> podsReady())
                .compose(i -> serviceEndpointsReady())
                .compose(i -> headlessServiceEndpointsReady())
                .compose(i -> clusterId(kafkaStatus))
                .compose(i -> defaultKafkaQuotas())
                .compose(i -> nodeUnregistration(kafkaStatus))
                .compose(i -> metadataVersion(kafkaStatus))
                .compose(i -> deletePersistentClaims())
                .compose(i -> sharedKafkaConfigurationCleanup())
                .compose(i -> deleteOldCertificateSecrets())
                // This has to run after all possible rolling updates which might move the pods to different nodes
                .compose(i -> nodePortExternalListenerStatus())
                .compose(i -> updateKafkaStatus(kafkaStatus));
    }

    private Future<Void> updateKafkaAutoRebalanceStatus(KafkaStatus kafkaStatus) {
        // gather all the desired brokers' ids across the entire cluster accounting all node pools
        Set<Integer> desiredBrokers = kafka.nodes().stream().filter(NodeRef::broker).map(NodeRef::nodeId).collect(Collectors.toSet());

        // gather all the added brokers' ids across the entire cluster accounting all node pools
        Set<Integer> addedBrokers = kafka.addedNodes().stream().filter(NodeRef::broker).map(NodeRef::nodeId).collect(Collectors.toSet());

        // if added brokers list contains all desired, it's a newly created cluster so there are no actual scaled up brokers.
        // when added brokers list has fewer nodes than desired, it actually containes the new ones for scaling up
        Set<Integer> scaledUpBrokerNodes = addedBrokers.containsAll(desiredBrokers) ? Set.of() : addedBrokers;

        KafkaRebalanceUtils.updateKafkaAutoRebalanceStatus(kafkaStatus, kafkaAutoRebalanceStatus, scaledUpBrokerNodes);

        return Future.succeededFuture();
    }

    /**
     * Takes the warning conditions from the Model and adds them in the KafkaStatus
     *
     * @param kafkaStatus   The Kafka Status where the warning conditions will be added
     *
     * @return              Completes when the warnings are added to the status object
     */
    protected Future<Void> modelWarnings(KafkaStatus kafkaStatus) {
        List<Condition> conditions = kafka.getWarningConditions();

        kafkaStatus.addConditions(conditions);

        return Future.succeededFuture();
    }

    /**
     * Initialize the TrustSet and PemAuthIdentity to be used by TLS clients during reconciliation
     *
     * @return Completes when the TrustStore and PemAuthIdentity have been created and stored in a record
     */
    protected Future<Void> initClientAuthenticationCertificates() {
        return ReconcilerUtils.coTlsPemIdentity(reconciliation, secretOperator)
                .onSuccess(coTlsPemIdentity -> this.coTlsPemIdentity = coTlsPemIdentity)
                .mapEmpty();
    }

    /**
     * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
     *
     * @return  Completes when the manual pod cleaning is done
     */
    protected Future<Void> manualPodCleaning() {
        return new ManualPodCleaner(
                reconciliation,
                kafka.getSelectorLabels(),
                strimziPodSetOperator,
                podOperator,
                pvcOperator
        ).maybeManualPodCleaning();
    }

    /**
     * Manages the network policy protecting the Kafka cluster
     *
     * @return  Completes when the network policy is successfully created or updated
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaNetworkPolicyName(reconciliation.name()), kafka.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels))
                    .mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Does manual rolling update of Kafka pods based on an annotation on the StrimziPodSet or on the Pods. Annotation
     * on StrimziPodSet level triggers rolling update of all pods. Annotation on pods triggers rolling update only of
     * the selected pods. If the annotation is present on both StrimziPodSet and one or more pods, only one rolling
     * update of all pods occurs.
     *
     * @return  Future with the result of the rolling update
     */
    protected Future<Void> manualRollingUpdate() {
        Future<List<NodeRef>> podsToRollThroughPodSetAnno = podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation();
        Future<List<NodeRef>> podsToRollThroughPodAnno = podsForManualRollingUpdateDiscoveredThroughPodAnnotations();

        return Future
                .join(podsToRollThroughPodSetAnno, podsToRollThroughPodAnno)
                .compose(result -> {
                    // We merge the lists into set to avoid duplicates
                    Set<NodeRef> nodes = new LinkedHashSet<>();
                    nodes.addAll(result.resultAt(0));
                    nodes.addAll(result.resultAt(1));

                    if (!nodes.isEmpty())   {
                        return maybeRollKafka(
                                nodes,
                                pod -> {
                                    if (pod == null) {
                                        throw new ConcurrentDeletionException("Unexpectedly pod no longer exists during roll of StrimziPodSet.");
                                    }

                                    LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation", pod.getMetadata().getName());

                                    return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                                },
                                // Pass empty advertised hostnames and ports for the nodes
                                nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, node -> Map.of())),
                                nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, node -> Map.of())),
                                false
                        ).recover(error -> {
                            if (continueOnManualRUFailure) {
                                LOGGER.warnCr(reconciliation, "Manual rolling update failed (reconciliation will be continued)", error);
                                return Future.succeededFuture();
                            } else {
                                return Future.failedFuture(error);
                            }
                        });
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Checks all Kafka PodSets and if they have the manual rolling update annotation, it will take all their nodes and
     * add them to a list for rolling update.
     *
     * @return  List with node references to nodes which should be rolled
     */
    private Future<List<NodeRef>> podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation()   {
        return strimziPodSetOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .map(podSets -> {
                    List<NodeRef> nodes = new ArrayList<>();

                    for (StrimziPodSet podSet : podSets) {
                        if (Annotations.booleanAnnotation(podSet, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            nodes.addAll(ReconcilerUtils.nodesFromPodSet(podSet));
                        }
                    }

                    if (!nodes.isEmpty()) {
                        LOGGER.debugCr(reconciliation, "Pods {} will be rolled due to manual rolling update annotation on their StrimziPodSet", nodes);
                    }

                    return nodes;
                });
    }

    /**
     * Checks all Kafka Pods and if they have the manual rolling update annotation, it will add them to a list for
     * rolling update.
     *
     * @return  List with node references to nodes which should be rolled
     */
    private Future<List<NodeRef>> podsForManualRollingUpdateDiscoveredThroughPodAnnotations()   {
        return podOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .map(pods -> {
                    List<NodeRef> nodes = new ArrayList<>();

                    for (Pod pod : pods) {
                        if (Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            nodes.add(ReconcilerUtils.nodeFromPod(pod));
                        }
                    }

                    if (!nodes.isEmpty()) {
                        LOGGER.debugCr(reconciliation, "Pods {} will be rolled due to manual rolling update annotation on the Pod", nodes);
                    }

                    return nodes;
                });
    }

    /**
     * Rolls Kafka pods if needed
     *
     * @param nodes                     List of nodes which should be considered for rolling
     * @param podNeedsRestart           Function which serves as a predicate whether to roll pod or not
     * @param kafkaAdvertisedHostnames  Map with advertised hostnames required to generate the per-broker configuration
     * @param kafkaAdvertisedPorts      Map with advertised ports required to generate the per-broker configuration
     * @param allowReconfiguration      Defines whether the rolling update should also attempt to do dynamic reconfiguration or not
     *
     * @return  Future which completes when the rolling is complete
     */
    protected Future<Void> maybeRollKafka(
            Set<NodeRef> nodes,
            Function<Pod, RestartReasons> podNeedsRestart,
            Map<Integer, Map<String, String>> kafkaAdvertisedHostnames,
            Map<Integer, Map<String, String>> kafkaAdvertisedPorts,
            boolean allowReconfiguration
    ) {
        Function<Integer, String> kafkaConfigProvider = nodeId -> kafka.generatePerBrokerConfiguration(nodeId, kafkaAdvertisedHostnames, kafkaAdvertisedPorts);
        //TODO: Change this logic to run the new roller if the feature gate for it is enabled (also add feature gate).

        var rr = RackRolling.rollingRestart(
                podOperator,
                nodes,
                reconciliation,
                // Remap the function from pod to RestartReasons to nodeId to RestartReasons
                nodeId -> podNeedsRestart.apply(podOperator.get(reconciliation.namespace(), nodes.stream().filter(nodeRef -> nodeRef.nodeId() == nodeId).collect(Collectors.toList()).get(0).podName())),
                this.coTlsPemIdentity,
                adminClientProvider,
                kafkaAgentClientProvider,
                kafkaConfigProvider,
                allowReconfiguration,
                kafka.getKafkaVersion(),
                logging,
                operationTimeoutMs,
                maxRestartBatchSize,
                3,
                3,
                10,
                eventsPublisher);

        return rr.executeRollingAsync(vertx);
//        return new KafkaRoller(
//                reconciliation,
//                vertx,
//                podOperator,
//                1_000,
//                operationTimeoutMs,
//                () -> new BackOff(250, 2, 10),
//                nodes,
//                this.coTlsPemIdentity,
//                adminClientProvider,
//                kafkaAgentClientProvider,
//                kafkaConfigProvider,
//                logging,
//                kafka.getKafkaVersion(),
//                allowReconfiguration,
//                eventsPublisher
//        ).rollingRestart(podNeedsRestart);
    }

    /**
     * Manages the PVCs needed by the Kafka cluster. This method only creates or updates the PVCs. Deletion of PVCs
     * after scale-down happens only at the end of the reconciliation when they are not used anymore.
     *
     * @param kafkaStatus   Status of the Kafka custom resource where warnings about any issues with resizing will be added
     *
     * @return  Completes when the PVCs were successfully created or updated
     */
    protected Future<Void> pvcs(KafkaStatus kafkaStatus) {
        List<PersistentVolumeClaim> pvcs = kafka.generatePersistentVolumeClaims();

        return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                .resizeAndReconcilePvcs(kafkaStatus, pvcs)
                .compose(podIdsToRestart -> {
                    for (Integer podId : podIdsToRestart) {
                        try {
                            fsResizingRestartRequest.add(kafka.nodePoolForNodeId(podId).nodeRef(podId).podName());
                        } catch (KafkaCluster.NodePoolNotFoundException e) {
                            // We might have triggered some resizing on a PVC not belonging to this cluster anymore.
                            // This could happen for example with old PVCs from removed nodes. We will ignore it with
                            // a warning.
                            LOGGER.warnCr(reconciliation, "Node with ID {} does not seem to belong to this Kafka cluster and cannot be marked for restart due to storage resizing", podId);
                        }
                    }

                    return Future.succeededFuture();
                });
    }

    /**
     * Manages the Kafka service account
     *
     * @return  Completes when the service account was successfully created or updated
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaComponentName(reconciliation.name()), kafka.generateServiceAccount())
                .mapEmpty();
    }

    /**
     * Manages the Kafka cluster role binding. When the desired Cluster Role Binding is null, and we get an RBAC error,
     * we ignore it. This is to allow users to run the operator only inside a namespace when no features requiring
     * Cluster Role Bindings are needed.
     *
     * @return  Completes when the Cluster Role Binding was successfully created or updated
     */
    protected Future<Void> initClusterRoleBinding() {
        ClusterRoleBinding desired = kafka.generateClusterRoleBinding(reconciliation.namespace());

        return ReconcilerUtils.withIgnoreRbacError(
                reconciliation,
                clusterRoleBindingOperator
                        .reconcile(
                                reconciliation,
                                KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()),
                                desired
                        ),
                desired
        ).mapEmpty();
    }

    /**
     * Scales down the Kafka cluster if needed. Kafka scale-down is done in one go.
     *
     * @return  Future which completes when the scale-down is finished
     */
    protected Future<Void> scaleDown() {
        LOGGER.debugCr(reconciliation, "Checking if Kafka scale-down is needed");

        Set<String> desiredPodNames = new HashSet<>();
        for (NodeRef node : kafka.nodes()) {
            desiredPodNames.add(node.podName());
        }

        return strimziPodSetOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(podSets -> {
                    if (podSets == null) {
                        return Future.succeededFuture();
                    } else {
                        List<Future<Void>> ops = new ArrayList<>();

                        for (StrimziPodSet podSet : podSets) {
                            List<Map<String, Object>> desiredPods = podSet.getSpec().getPods().stream()
                                    .filter(pod -> desiredPodNames.contains(PodSetUtils.mapToPod(pod).getMetadata().getName()))
                                    .collect(Collectors.toList());

                            StrimziPodSet scaledDownPodSet = new StrimziPodSetBuilder(podSet)
                                    .editSpec()
                                    .withPods(desiredPods)
                                    .endSpec()
                                    .build();

                            if (podSet.getSpec().getPods().size() > desiredPods.size())    {
                                LOGGER.infoCr(reconciliation, "Scaling down Kafka pod set {} from {} to {} replicas", podSet.getMetadata().getName(), podSet.getSpec().getPods().size(), desiredPods.size());
                                ops.add(
                                        strimziPodSetOperator
                                                .reconcile(reconciliation, reconciliation.namespace(), podSet.getMetadata().getName(), scaledDownPodSet)
                                                .map((Void) null)
                                );
                            }
                        }

                        return Future.join(ops).mapEmpty();
                    }
                });
    }

    /**
     * Utility method to create the Kafka Listener reconciler. It can be also use to inject mocked reconciler during
     * tests.
     *
     * @return  KafkaListenerReconciler instance
     */
    protected KafkaListenersReconciler listenerReconciler()   {
        return new KafkaListenersReconciler(
                reconciliation,
                kafka,
                clusterCa,
                pfa,
                operationTimeoutMs,
                secretOperator,
                serviceOperator,
                routeOperator,
                ingressOperator
        );
    }

    /**
     * Reconciles listeners of this Kafka cluster
     *
     * @return  Future which completes when listeners are reconciled
     */
    protected Future<Void> listeners()    {
        return listenerReconciler()
                .reconcile()
                .compose(result -> {
                    listenerReconciliationResults = result;
                    return Future.succeededFuture();
                });
    }

    /**
     * Generates and creates the ConfigMaps with per-broker configuration for Kafka brokers used in PodSets. It will
     * also delete the ConfigMaps for any scaled-down brokers (scale down is done before this is called in the
     * reconciliation)
     *
     * @param metricsAndLogging     Metrics and Logging configuration
     *
     * @return  Future which completes when the Kafka Configuration is prepared
     */
    protected Future<Void> perBrokerKafkaConfiguration(MetricsAndLogging metricsAndLogging) {
        return configMapOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(existingConfigMaps -> {
                    // This is used during Kafka rolling updates -> we have to store it for later
                    this.logging = kafka.logging().loggingConfiguration(reconciliation, metricsAndLogging.loggingCm());

                    List<ConfigMap> desiredConfigMaps = kafka.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, listenerReconciliationResults.advertisedHostnames, listenerReconciliationResults.advertisedPorts);
                    List<Future<?>> ops = new ArrayList<>();

                    // Delete all existing ConfigMaps which are not desired and are not the shared config map
                    List<String> desiredNames = new ArrayList<>(desiredConfigMaps.size() + 1);
                    desiredNames.add(KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name())); // We do not want to delete the shared ConfigMap, so we add it here
                    desiredNames.addAll(desiredConfigMaps.stream().map(cm -> cm.getMetadata().getName()).toList());

                    for (ConfigMap cm : existingConfigMaps) {
                        // We delete the cms not on the desired names list
                        if (!desiredNames.contains(cm.getMetadata().getName())) {
                            ops.add(configMapOperator.deleteAsync(reconciliation, reconciliation.namespace(), cm.getMetadata().getName(), true));
                        }
                    }

                    // Create / update the desired config maps
                    for (ConfigMap cm : desiredConfigMaps) {
                        String cmName = cm.getMetadata().getName();
                        int nodeId = ReconcilerUtils.getPodIndexFromPodName(cmName);
                        KafkaPool pool = kafka.nodePoolForNodeId(nodeId);

                        String nodeConfiguration = "";

                        // We collect the information needed for the annotation hash for brokers or mixed nodes.
                        // Controller-only nodes do not have advertised listener configuration and this config is not relevant for them.
                        if (pool.isBroker()) {
                            // The advertised hostname and port might change. If they change, we need to roll the pods.
                            // Here we collect their hash to trigger the rolling update. For per-broker configuration,
                            // we need just the advertised hostnames / ports for given broker.
                            nodeConfiguration = listenerReconciliationResults.advertisedHostnames
                                    .get(nodeId)
                                    .entrySet()
                                    .stream()
                                    .map(kv -> kv.getKey() + "://" + kv.getValue())
                                    .sorted()
                                    .collect(Collectors.joining(" "));
                            nodeConfiguration += listenerReconciliationResults.advertisedPorts
                                    .get(nodeId)
                                    .entrySet()
                                    .stream()
                                    .map(kv -> kv.getKey() + "://" + kv.getValue())
                                    .sorted()
                                    .collect(Collectors.joining(" "));
                            nodeConfiguration += cm.getData().getOrDefault(KafkaCluster.BROKER_LISTENERS_FILENAME, "");
                        }

                        // Changes to regular Kafka configuration are handled through the KafkaRoller which decides whether to roll the pod or not
                        // In addition to that, we have to handle changes to configuration unknown to Kafka -> different plugins (Authorization, Quotas etc.)
                        // This is captured here with the unknown configurations and the hash is used to roll the pod when it changes
                        KafkaConfiguration kc = KafkaConfiguration.unvalidated(reconciliation, cm.getData().getOrDefault(KafkaCluster.BROKER_CONFIGURATION_FILENAME, ""));

                        // We collect the configuration options related to various plugins
                        nodeConfiguration += kc.unknownConfigsWithValues(kafka.getKafkaVersion()).toString();

                        // We collect the information relevant to controller-only nodes
                        if (pool.isController() && !pool.isBroker())   {
                            // For controllers only, we extract the controller-relevant configurations and use it in the configuration annotations
                            nodeConfiguration = kc.controllerConfigsWithValues().toString();
                            // For controllers only, we use the full logging configuration in the logging annotation
                            this.brokerLoggingHash.put(nodeId, Util.hashStub(logging));
                        } else {
                            this.brokerLoggingHash.put(nodeId, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(logging)));
                        }

                        // We store hash of the broker configurations for later use in Pod and in rolling updates
                        this.brokerConfigurationHash.put(nodeId, Util.hashStub(nodeConfiguration));

                        ops.add(configMapOperator.reconcile(reconciliation, reconciliation.namespace(), cmName, cm));
                    }

                    return Future
                            .join(ops)
                            .mapEmpty();
                });
    }

    /**
     * This method is used to create or update the config maps required by the brokers. It does not do the cleanup the
     * old shared Config Map used by StatefulSets. That is done only at the end of the reconciliation. However, it would
     * delete the config maps of the scaled-down brokers since scale-down happens before this is called.
     *
     * @return  Future which completes when the Config Map(s) with configuration are created or updated
     */
    protected Future<Void> brokerConfigurationConfigMaps() {
        return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, kafka.logging(), kafka.metrics())
                .compose(metricsAndLoggingCm -> perBrokerKafkaConfiguration(metricsAndLoggingCm));
    }

    /**
     * Manages the Secrets with the node certificates used by the Kafka nodes.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Completes when the Secrets were successfully created, deleted or updated
     */
    protected Future<Void> certificateSecrets(Clock clock) {
        return secretOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels().withStrimziComponentType(KafkaCluster.COMPONENT_TYPE))
                .compose(existingSecrets -> {
                    List<Secret> desiredCertSecrets = kafka.generateCertificatesSecrets(clusterCa, clientsCa, existingSecrets,
                            listenerReconciliationResults.bootstrapDnsNames, listenerReconciliationResults.brokerDnsNames,
                            Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                    List<String> desiredCertSecretNames = desiredCertSecrets.stream().map(secret -> secret.getMetadata().getName()).toList();
                    existingSecrets.forEach(secret -> {
                        String secretName = secret.getMetadata().getName();
                        // Don't delete desired secrets or jmx secrets
                        if (!desiredCertSecretNames.contains(secretName) && !KafkaResources.kafkaJmxSecretName(reconciliation.name()).equals(secretName)) {
                            secretsToDelete.add(secretName);
                        }
                    });
                    return updateCertificateSecrets(desiredCertSecrets);
                }).mapEmpty();
    }

    /**
     * Delete old certificate Secrets that are no longer needed.
     *
     * @return Future that completes when the Secrets have been deleted.
     */
    protected Future<Void> deleteOldCertificateSecrets() {
        List<Future<Void>> deleteFutures = secretsToDelete.stream()
                .map(secretName -> {
                    LOGGER.debugCr(reconciliation, "Deleting old Secret {}/{} that is no longer used.", reconciliation.namespace(), secretName);
                    return secretOperator.deleteAsync(reconciliation, reconciliation.namespace(), secretName, false);
                }).collect(Collectors.toCollection(ArrayList::new)); // We need to collect to mutable list because we might need to add to the list more items later

        // Remove old Secret containing all certs if it exists
        @SuppressWarnings("deprecation")
        String oldSecretName = KafkaResources.kafkaSecretName(reconciliation.name());
        return secretOperator.getAsync(reconciliation.namespace(), oldSecretName)
                .compose(oldSecret -> {
                    if (oldSecret != null) {
                        LOGGER.debugCr(reconciliation, "Deleting legacy Secret {}/{} that is replaced by pod specific Secret.", reconciliation.namespace(), oldSecretName);
                        deleteFutures.add(secretOperator.deleteAsync(reconciliation, reconciliation.namespace(), oldSecretName, false));
                    }

                    return Future.join(deleteFutures).mapEmpty();
                });
    }

    /**
     * Updates the Secrets with the node certificates used by the Kafka nodes.
     *
     * @param secrets Secrets to update
     *
     * @return Future that completes when the Secrets were successfully created or updated
     */
    protected Future<Void> updateCertificateSecrets(List<Secret> secrets) {
        List<Future<Object>> reconcileFutures = secrets
                .stream()
                .map(secret -> {
                    String secretName = secret.getMetadata().getName();
                    return secretOperator.reconcile(reconciliation, reconciliation.namespace(), secretName, secret)
                            .compose(patchResult -> {
                                if (patchResult != null) {
                                    kafkaServerCertificateHash.put(
                                            ReconcilerUtils.getPodIndexFromPodName(secretName),
                                            CertUtils.getCertificateThumbprint(patchResult.resource(),
                                                    Ca.SecretEntry.CRT.asKey(secretName)
                                            ));
                                }
                                return Future.succeededFuture();
                            });
                }).toList();
        return Future.join(reconcileFutures).mapEmpty();
    }

    /**
     * Manages the secret with JMX credentials when JMX is enabled
     *
     * @return  Completes when the JMX secret is successfully created or updated
     */
    protected Future<Void> jmxSecret() {
        return ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperator, kafka);
    }

    /**
     * Manages the PodDisruptionBudgets on Kubernetes clusters which support v1 version of PDBs
     *
     * @return  Completes when the PDB was successfully created or updated
     */
    protected Future<Void> podDisruptionBudget() {
        if (isPodDisruptionBudgetGeneration) {
            return podDisruptionBudgetOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaComponentName(reconciliation.name()), kafka.generatePodDisruptionBudget())
                    .mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Prepares annotations for Kafka pods within a StrimziPodSet which are known only in the KafkaAssemblyOperator level.
     * These are later passed to KafkaCluster where there are used when creating the Pod definitions.
     *
     * @param node    The node for which the annotations are being prepared.
     *
     * @return  Map with Pod annotations
     */
    private Map<String, String> podSetPodAnnotations(NodeRef node) {
        Map<String, String> podAnnotations = new LinkedHashMap<>(9);
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(this.clusterCa.caCertGeneration()));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(this.clusterCa.caKeyGeneration()));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, String.valueOf(this.clientsCa.caCertGeneration()));
        podAnnotations.put(Annotations.ANNO_STRIMZI_LOGGING_HASH, brokerLoggingHash.get(node.nodeId()));
        podAnnotations.put(KafkaCluster.ANNO_STRIMZI_BROKER_CONFIGURATION_HASH, brokerConfigurationHash.get(node.nodeId()));
        podAnnotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, kafka.getKafkaVersion().version());

        String logMessageFormatVersion = kafka.getLogMessageFormatVersion();
        if (logMessageFormatVersion != null && !logMessageFormatVersion.isBlank()) {
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, logMessageFormatVersion);
        }

        String interBrokerProtocolVersion = kafka.getInterBrokerProtocolVersion();
        if (interBrokerProtocolVersion != null && !interBrokerProtocolVersion.isBlank()) {
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, interBrokerProtocolVersion);
        }

        podAnnotations.put(ANNO_STRIMZI_SERVER_CERT_HASH, kafkaServerCertificateHash.get(node.nodeId())); // Annotation of broker certificate hash

        // Annotations with custom cert thumbprints to help with rolling updates when they change
        if (node.broker() && !listenerReconciliationResults.customListenerCertificateThumbprints.isEmpty()) {
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, listenerReconciliationResults.customListenerCertificateThumbprints.toString());
        }

        return podAnnotations;
    }

    /**
     * Create or update the StrimziPodSet for the Kafka cluster. If the StrimziPodSet is updated with additional pods
     * (Kafka cluster scaled up), it's the StrimziPodSet controller taking care of starting up the new nodes. But this
     * method will wait for the new nodes to get ready.
     *
     * The opposite (Kafka cluster scaled down) is handled by a dedicated scaleDown() method instead.
     *
     * @return  Future which completes when the PodSet is created, updated or deleted and any new Pods reach the Ready state
     */
    protected Future<Map<String, ReconcileResult<StrimziPodSet>>> podSet() {
        return strimziPodSetOperator
                .batchReconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        kafka.generatePodSets(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, this::podSetPodAnnotations),
                        kafka.getSelectorLabels()
                )
                .compose(podSetDiff -> waitForNewNodes().map(podSetDiff));
    }

    /**
     * Waits for new nodes (pods) to get into a Ready state
     *
     * @return  Future that completes when all the new nodes are ready
     */
    private Future<Void> waitForNewNodes() {
        return ReconcilerUtils
                .podsReady(
                        reconciliation,
                        podOperator,
                        operationTimeoutMs,
                        kafka.addedNodes().stream().map(NodeRef::podName).toList()
                );
    }

    /**
     * Roles the Kafka brokers (if needed).
     *
     * @param podSetDiffs   Map with the PodSet reconciliation results
     *
     * @return  Future which completes when any of the Kafka pods which need rolling is rolled
     */
    protected Future<Void> rollingUpdate(Map<String, ReconcileResult<StrimziPodSet>> podSetDiffs) {
        return maybeRollKafka(
                kafka.nodes(),
                pod -> ReconcilerUtils.reasonsToRestartPod(
                        reconciliation,
                        podSetDiffs.get(ReconcilerUtils.getControllerNameFromPodName(pod.getMetadata().getName())).resource(),
                        pod,
                        fsResizingRestartRequest,
                        ReconcilerUtils.trackedServerCertChanged(pod, kafkaServerCertificateHash),
                        clusterCa,
                        clientsCa
                ),
                listenerReconciliationResults.advertisedHostnames,
                listenerReconciliationResults.advertisedPorts,
                true
        );
    }

    /**
     * Checks whether the Kafka pods are ready and if not, waits for them to get ready
     *
     * @return  Future which completes when all Kafka pods are ready
     */
    protected Future<Void> podsReady() {
        return ReconcilerUtils
                .podsReady(
                        reconciliation,
                        podOperator,
                        operationTimeoutMs,
                        kafka.nodes().stream().map(node -> node.podName()).toList()
                );
    }

    /**
     * Waits for readiness of the endpoints of the clients service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> serviceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.bootstrapServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Waits for readiness of the endpoints of the headless service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> headlessServiceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Get the Cluster ID of the Kafka cluster
     *
     * @return  Future which completes when the Cluster ID is retrieved and set in the status
     */
    protected Future<Void> clusterId(KafkaStatus kafkaStatus) {
        LOGGER.debugCr(reconciliation, "Attempt to get clusterId");
        return vertx.createSharedWorkerExecutor("kubernetes-ops-pool")
                .executeBlocking(() -> {
                    Admin kafkaAdmin = null;

                    try {
                        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
                        LOGGER.debugCr(reconciliation, "Creating AdminClient for clusterId using {}", bootstrapHostname);
                        kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, this.coTlsPemIdentity.pemTrustSet(), this.coTlsPemIdentity.pemAuthIdentity());
                        kafkaStatus.setClusterId(kafkaAdmin.describeCluster().clusterId().get());
                    } catch (KafkaException e) {
                        LOGGER.warnCr(reconciliation, "Kafka exception getting clusterId {}", e.getMessage());
                    } catch (InterruptedException e) {
                        LOGGER.warnCr(reconciliation, "Interrupted exception getting clusterId {}", e.getMessage());
                    } catch (ExecutionException e) {
                        LOGGER.warnCr(reconciliation, "Execution exception getting clusterId {}", e.getMessage());
                    } finally {
                        if (kafkaAdmin != null) {
                            kafkaAdmin.close();
                        }
                    }

                    return null;
                });
    }

    /**
     * Configures the default users quota in Kafka in case that the {@link QuotasPluginKafka} is used
     *
     * @return  Future which completes when the default quotas are configured
     */
    protected Future<Void> defaultKafkaQuotas() {
        return DefaultKafkaQuotasManager.reconcileDefaultUserQuotas(reconciliation, vertx, adminClientProvider, this.coTlsPemIdentity.pemTrustSet(), this.coTlsPemIdentity.pemAuthIdentity(), kafka.quotas());
    }

    /**
     * Unregisters the KRaft nodes that were removed from the Kafka cluster
     *
     * @param kafkaStatus   Kafka status for updating the list of currently registered node IDs
     *
     * @return  Future which completes when the nodes removed from the Kafka cluster are unregistered
     */
    protected Future<Void> nodeUnregistration(KafkaStatus kafkaStatus) {
        List<Integer> currentNodeIds = kafka.nodes().stream().map(NodeRef::nodeId).sorted().toList();

        if (previousNodeIds != null
                && !new HashSet<>(currentNodeIds).containsAll(previousNodeIds)) {
            // We are in KRaft mode and there are some nodes that were removed => we should unregister them
            List<Integer> nodeIdsToUnregister = new ArrayList<>(previousNodeIds);
            nodeIdsToUnregister.removeAll(currentNodeIds);

            LOGGER.infoCr(reconciliation, "Kafka nodes {} were removed from the Kafka cluster and will be unregistered", nodeIdsToUnregister);

            Promise<Void> unregistrationPromise = Promise.promise();
            KafkaNodeUnregistration.unregisterNodes(reconciliation, vertx, adminClientProvider, coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity(), nodeIdsToUnregister)
                    .onComplete(res -> {
                        if (res.succeeded()) {
                            LOGGER.infoCr(reconciliation, "Kafka nodes {} were successfully unregistered from the Kafka cluster", nodeIdsToUnregister);
                            kafkaStatus.setRegisteredNodeIds(currentNodeIds);
                        } else {
                            LOGGER.warnCr(reconciliation, "Failed to unregister Kafka nodes {} from the Kafka cluster", nodeIdsToUnregister);

                            // When the unregistration failed, we will keep the original registered node IDs to retry
                            // the unregistration for them. But we will merge it with any existing node IDs to make
                            // sure we do not lose track of them.
                            Set<Integer> updatedNodeIds = new HashSet<>(currentNodeIds);
                            updatedNodeIds.addAll(previousNodeIds);
                            kafkaStatus.setRegisteredNodeIds(updatedNodeIds.stream().sorted().toList());
                        }

                        // We complete the promise with success even if the unregistration failed as we do not want to
                        // fail the reconciliation.
                        unregistrationPromise.complete();
                    });

            return unregistrationPromise.future();
        } else {
            // We are either not in KRaft mode, or at a cluster without any information about previous nodes, or without
            // any change to the nodes => we just update the status field
            kafkaStatus.setRegisteredNodeIds(currentNodeIds);
            return Future.succeededFuture();
        }
    }

    /**
     * Manages the KRaft metadata version
     *
     * @param kafkaStatus   Kafka status used for updating the currently used metadata version
     *
     * @return  Future which completes when the KRaft metadata version is set to the current version or updated.
     */
    protected Future<Void> metadataVersion(KafkaStatus kafkaStatus) {
        return KRaftMetadataManager.maybeUpdateMetadataVersion(reconciliation, vertx, this.coTlsPemIdentity, adminClientProvider, kafka.getMetadataVersion(), kafkaStatus);
    }

    /**
     * Deletion of PVCs after the cluster is deleted is handled by owner reference and garbage collection. However,
     * this would not help after scale-downs. Therefore, we check if there are any PVCs which should not be present
     * and delete them when they are.
     *
     * This should be called only after the StrimziPodSet reconciliation, rolling update and scale-down when the PVCs
     * are not used any more by the pods.
     *
     * @return  Future which completes when the PVCs which should be deleted are deleted
     */
    protected Future<Void> deletePersistentClaims() {
        return pvcOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(pvcs -> {
                    List<String> maybeDeletePvcs = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = kafka.generatePersistentVolumeClaims().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                            .deletePersistentClaims(maybeDeletePvcs, desiredPvcs);
                });
    }

    /**
     * Deletes the ConfigMap with shared Kafka configuration. This needs to be done after migrating from StatefulSets to StrimziPodSets
     *
     * @return  Future which returns when the shared configuration config map is deleted
     */
    protected Future<Void> sharedKafkaConfigurationCleanup() {
        // We use reconcile() instead of deleteAsync() because reconcile first checks if the deletion is needed.
        // Deleting resource which likely does not exist would cause more load on the Kubernetes API then trying to get
        // it first because of the watch if it was deleted etc.
        return configMapOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name()), null)
                .mapEmpty();
    }

    /**
     * Creates the status for NodePort listeners. This can be done only now at the end and not when the other listener
     * types are done because it requires the Kafka brokers to be scheduled and running to collect their node addresses.
     * Without that, we do not know on which node would they be running.
     *
     * Note: To avoid issues with big clusters with many nodes, we first get the used nodes from the Pods and then get
     * the node information individually for each node instead of listing all nodes and then picking up the information
     * we need. This means more Kubernetes API calls, but helps us to avoid running out of memory.
     *
     * @return  Future which completes when the Listener status is created for all node port listeners
     */
    protected Future<Void> nodePortExternalListenerStatus() {
        if (!ListenersUtils.nodePortListeners(kafka.getListeners()).isEmpty())   {
            Map<Integer, String> brokerNodes = new HashMap<>();
            ConcurrentMap<String, Node> nodes = new ConcurrentHashMap<>();

            // First we collect all the broker pods we have so that we can find out on which worker nodes they run
            return podOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels().withStrimziBrokerRole(true))
                    .compose(pods -> {
                        // We collect the nodes used by the brokers upfront to avoid asking for the same node multiple times later
                        for (Pod broker : pods) {
                            if (broker.getSpec() != null && broker.getSpec().getNodeName() != null) {
                                Integer podIndex = ReconcilerUtils.getPodIndexFromPodName(broker.getMetadata().getName());
                                brokerNodes.put(podIndex, broker.getSpec().getNodeName());
                            } else {
                                // This should not happen, but to avoid some chain of errors downstream we check it and raise exception
                                LOGGER.warnCr(reconciliation, "Kafka Pod {} has no node name specified", broker.getMetadata().getName());
                                return Future.failedFuture(new RuntimeException("Kafka Pod " + broker.getMetadata().getName() + " has no node name specified"));
                            }
                        }

                        List<Future<Void>> nodeFutures = new ArrayList<>();

                        // We get the full node resource for each node with a broker
                        for (String nodeName : brokerNodes.values().stream().distinct().toList()) {
                            LOGGER.debugCr(reconciliation, "Getting information on worker node {} used by one or more brokers", nodeName);
                            Future<Void> nodeFuture = nodeOperator.getAsync(nodeName).compose(node -> {
                                if (node != null) {
                                    nodes.put(nodeName, node);
                                } else {
                                    // Node was not found, but we do not want to fail because of this as it might be just some race condition
                                    LOGGER.warnCr(reconciliation, "Worker node {} does not seem to exist", nodeName);
                                }

                                return Future.succeededFuture();
                            });
                            nodeFutures.add(nodeFuture);
                        }

                        return Future.join(nodeFutures);
                    })
                    .map(i -> {
                        // We extract the address information from the nodes
                        for (GenericKafkaListener listener : ListenersUtils.nodePortListeners(kafka.getListeners())) {
                            // Set is used to ensure each node/port is listed only once. It is later converted to List.
                            Set<ListenerAddress> statusAddresses = new HashSet<>(brokerNodes.size());

                            for (Map.Entry<Integer, String> entry : brokerNodes.entrySet())   {
                                String advertisedHost = ListenersUtils.brokerAdvertisedHost(listener, kafka.nodePoolForNodeId(entry.getKey()).nodeRef(entry.getKey()));
                                ListenerAddress address;

                                if (advertisedHost != null)    {
                                    address = new ListenerAddressBuilder()
                                            .withHost(advertisedHost)
                                            .withPort(listenerReconciliationResults.bootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                            .build();
                                } else if (nodes.get(entry.getValue()) != null) {
                                    address = new ListenerAddressBuilder()
                                            .withHost(NodeUtils.findAddress(nodes.get(entry.getValue()).getStatus().getAddresses(), ListenersUtils.preferredNodeAddressType(listener)))
                                            .withPort(listenerReconciliationResults.bootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                            .build();
                                } else {
                                    // Node was not found, but we do not want to fail because of this as it might be just some race condition
                                    LOGGER.warnCr(reconciliation, "Kafka node {} is running on an unknown node and its node port address cannot be found", entry.getKey());
                                    continue;
                                }

                                statusAddresses.add(address);
                            }

                            ListenerStatus ls = listenerReconciliationResults.listenerStatuses
                                    .stream()
                                    .filter(listenerStatus -> listener.getName().equals(listenerStatus.getName()))
                                    .findFirst()
                                    .orElseThrow(() -> new RuntimeException("Status for listener " + listener.getName() + " not found"));
                            ls.setAddresses(new ArrayList<>(statusAddresses));
                        }

                        return null;
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Updates various fields in the Kafka CR .status section such as listener information, Kafka version, metadata
     * state etc. This includes the parts of the status that do not need to be updated at a specific point in the
     * reconciliation.
     *
     * @param kafkaStatus   Kafka status where the values should be set
     *
     * @return  Future that completes once the status is updated
     */
    /* test */ Future<Void> updateKafkaStatus(KafkaStatus kafkaStatus) {
        kafkaStatus.setListeners(listenerReconciliationResults.listenerStatuses);
        kafkaStatus.setKafkaVersion(kafka.getKafkaVersion().version());
        kafkaStatus.setKafkaMetadataState(KafkaMetadataState.KRaft);

        return Future.succeededFuture();
    }

    /**
     * Updates the statuses of the used KafkaNodePools with the used node IDs. Also prepares the list of used node pools
     * for the Kafka CR status (but the Kafka status is not updated in this method).
     *
     * @param kafkaStatus   The status of the Kafka CR to add the list of node pools belonging to it
     *
     * @return  Future which completes when the statuses are set
     */
    protected Future<Void> updateNodePoolStatuses(KafkaStatus kafkaStatus) {
        if (isKafkaNodePoolsEnabled) {
            List<KafkaNodePool> updatedNodePools = new ArrayList<>();
            List<UsedNodePoolStatus> statusesForKafka = new ArrayList<>();
            Map<String, KafkaNodePoolStatus> statuses = kafka.nodePoolStatuses();

            for (KafkaNodePool nodePool : kafkaNodePoolCrs) {
                statusesForKafka.add(new UsedNodePoolStatusBuilder().withName(nodePool.getMetadata().getName()).build());

                KafkaNodePool updatedNodePool = new KafkaNodePoolBuilder(nodePool)
                        .withStatus(
                                new KafkaNodePoolStatusBuilder(statuses.get(nodePool.getMetadata().getName()))
                                        .withObservedGeneration(nodePool.getMetadata().getGeneration())
                                        .build())
                        .build();

                StatusDiff diff = new StatusDiff(nodePool.getStatus(), updatedNodePool.getStatus());

                if (!diff.isEmpty()) {
                    // Status changed => we will update it
                    updatedNodePools.add(updatedNodePool);
                }
            }

            // Sets the list of used Node Pools in the Kafka CR status
            kafkaStatus.setKafkaNodePools(statusesForKafka.stream().sorted(Comparator.comparing(UsedNodePoolStatus::getName)).toList());

            List<Future<KafkaNodePool>> statusUpdateFutures = new ArrayList<>();

            for (KafkaNodePool updatedNodePool : updatedNodePools) {
                statusUpdateFutures.add(kafkaNodePoolOperator.updateStatusAsync(reconciliation, updatedNodePool));
            }

            // Return future
            return Future.join(statusUpdateFutures)
                    .mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }
}
