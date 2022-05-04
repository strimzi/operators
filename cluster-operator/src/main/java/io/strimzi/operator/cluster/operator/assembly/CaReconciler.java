/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZooKeeperRoller;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Class used for reconciliation of Cluster and Client CAs. This class contains both the steps of the CA reconciliation
 * pipeline and is also used to store the state between them.
 */
public class CaReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CaReconciler.class.getName());

    private final Reconciliation reconciliation;
    private final Vertx vertx;
    private final long operationTimeoutMs;
    private final FeatureGates featureGates;

    private final DeploymentOperator deploymentOperator;
    private final StatefulSetOperator stsOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final SecretOperator secretOperator;
    private final PodOperator podOperator;
    private final AdminClientProvider adminClientProvider;
    private final ZookeeperLeaderFinder zookeeperLeaderFinder;
    private final CertManager certManager;
    private final PasswordGenerator passwordGenerator;

    // Fields based on the Kafka CR required for the reconciliation
    private final List<String> maintenanceWindows;
    private final OwnerReference ownerRef;
    private final CertificateAuthority clusterCaConfig;
    private final CertificateAuthority clientsCaConfig;
    private final Map<String, String> caLabels;
    private final Labels clusterOperatorSecretLabels;
    private final Map<String, String> clusterCaCertLabels;
    private final Map<String, String> clusterCaCertAnnotations;

    // Fields used to store state during the reconciliation
    private ClusterCa clusterCa;
    private ClientsCa clientsCa;
    private Secret oldCoSecret;

    /**
     * Constructs the CA reconciler which reconciles the Cluster and Client CAs
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaCr           The Kafka custom resource
     * @param config            Cluster Operator Configuration
     * @param supplier          Supplier with Kubernetes Resource Operators
     * @param vertx             Vert.x instance
     * @param certManager       Certificate Manager for managing certificates
     * @param passwordGenerator Password generator for generating passwords
     */
    public CaReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            Vertx vertx,
            CertManager certManager,
            PasswordGenerator passwordGenerator
    ) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.featureGates = config.featureGates();

        this.deploymentOperator = supplier.deploymentOperations;
        this.stsOperator = supplier.stsOperations;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.secretOperator = supplier.secretOperations;
        this.podOperator = supplier.podOperations;

        this.adminClientProvider = supplier.adminClientProvider;
        this.zookeeperLeaderFinder = supplier.zookeeperLeaderFinder;
        this.certManager = certManager;
        this.passwordGenerator = passwordGenerator;

        // Extract required information from the Kafka CR
        this.maintenanceWindows = kafkaCr.getSpec().getMaintenanceTimeWindows();
        this.ownerRef = new OwnerReferenceBuilder()
                .withApiVersion(kafkaCr.getApiVersion())
                .withKind(kafkaCr.getKind())
                .withName(kafkaCr.getMetadata().getName())
                .withUid(kafkaCr.getMetadata().getUid())
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();
        this.clusterCaConfig = kafkaCr.getSpec().getClusterCa();
        this.clientsCaConfig = kafkaCr.getSpec().getClientsCa();
        this.caLabels = Labels
                .generateDefaultLabels(kafkaCr, Labels.APPLICATION_NAME, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME)
                .toMap();
        this.clusterOperatorSecretLabels = Labels.fromResource(kafkaCr)
                .withStrimziKind(reconciliation.kind())
                .withStrimziCluster(reconciliation.name())
                .withKubernetesName(Labels.APPLICATION_NAME)
                .withKubernetesInstance(reconciliation.name())
                .withKubernetesPartOf(reconciliation.name())
                .withKubernetesManagedBy(AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        this.clusterCaCertLabels = clusterCaCertLabels(kafkaCr);
        this.clusterCaCertAnnotations = clusterCaCertAnnotations(kafkaCr);
    }

    /**
     * Utility method to extract the template labels from the Kafka CR.
     *
     * @param kafkaCr   Kafka CR
     *
     * @return  Map with the labels from the Kafka CR or empty map if the template is not set
     */
    private static Map<String, String> clusterCaCertLabels(Kafka kafkaCr)    {
        if (kafkaCr.getSpec().getKafka() != null
                && kafkaCr.getSpec().getKafka().getTemplate() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getLabels() != null) {
            return kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getLabels();
        } else {
            return Map.of();
        }
    }

    /**
     * Utility method to extract the template annotations from the Kafka CR.
     *
     * @param kafkaCr   Kafka CR
     *
     * @return  Map with the annotation from the Kafka CR or empty map if the template is not set
     */
    private static Map<String, String> clusterCaCertAnnotations(Kafka kafkaCr)    {
        if (kafkaCr.getSpec().getKafka() != null
                && kafkaCr.getSpec().getKafka().getTemplate() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getAnnotations() != null) {
            return kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getAnnotations();
        } else {
            return Map.of();
        }
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param dateSupplier  Date supplier for checking maintenance windows
     *
     * @return  Future with the CA reconciliation result containing the Cluster and Clients CAs
     */
    public Future<CaReconciliationResult> reconcile(Supplier<Date> dateSupplier)    {
        return reconcileCas(dateSupplier)
                .compose(i -> clusterOperatorSecret(dateSupplier))
                .compose(i -> rollingUpdateForNewCaKey())
                .compose(i -> maybeRemoveOldClusterCaCertificates())
                .map(i -> new CaReconciliationResult(clusterCa, clientsCa));
    }

    /**
     * Asynchronously reconciles the cluster and clients CA secrets.
     * The cluster CA secret has to have the name determined by {@link AbstractModel#clusterCaCertSecretName(String)}.
     * The clients CA secret has to have the name determined by {@link KafkaResources#clientsCaCertificateSecretName(String)}.
     * Within both the secrets the current certificate is stored under the key {@code ca.crt}
     * and the current key is stored under the key {@code ca.key}.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    Future<Void> reconcileCas(Supplier<Date> dateSupplier) {
        Promise<Void> resultPromise = Promise.promise();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    String clusterCaCertName = AbstractModel.clusterCaCertSecretName(reconciliation.name());
                    String clusterCaKeyName = AbstractModel.clusterCaKeySecretName(reconciliation.name());
                    String clientsCaCertName = KafkaResources.clientsCaCertificateSecretName(reconciliation.name());
                    String clientsCaKeyName = KafkaResources.clientsCaKeySecretName(reconciliation.name());
                    Secret clusterCaCertSecret = null;
                    Secret clusterCaKeySecret = null;
                    Secret clientsCaCertSecret = null;
                    Secret clientsCaKeySecret = null;
                    Secret brokersSecret = null;

                    List<Secret> clusterSecrets = secretOperator.list(reconciliation.namespace(), Labels.EMPTY.withStrimziKind(reconciliation.kind()).withStrimziCluster(reconciliation.name()));

                    for (Secret secret : clusterSecrets) {
                        String secretName = secret.getMetadata().getName();
                        if (secretName.equals(clusterCaCertName)) {
                            clusterCaCertSecret = secret;
                        } else if (secretName.equals(clusterCaKeyName)) {
                            clusterCaKeySecret = secret;
                        } else if (secretName.equals(clientsCaCertName)) {
                            clientsCaCertSecret = secret;
                        } else if (secretName.equals(clientsCaKeyName)) {
                            clientsCaKeySecret = secret;
                        } else if (secretName.equals(KafkaResources.kafkaSecretName(reconciliation.name()))) {
                            brokersSecret = secret;
                        }
                    }

                    // When we are not supposed to generate the CA, but it does not exist, we should just throw an error
                    checkCustomCaSecret(clusterCaConfig, clusterCaCertSecret, clusterCaKeySecret, "Cluster CA");

                    clusterCa = new ClusterCa(reconciliation, certManager, passwordGenerator, reconciliation.name(), clusterCaCertSecret,
                            clusterCaKeySecret,
                            ModelUtils.getCertificateValidity(clusterCaConfig),
                            ModelUtils.getRenewalDays(clusterCaConfig),
                            clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority(), clusterCaConfig != null ? clusterCaConfig.getCertificateExpirationPolicy() : null);
                    clusterCa.initCaSecrets(clusterSecrets);
                    clusterCa.createRenewOrReplace(
                            reconciliation.namespace(), reconciliation.name(), caLabels,
                            clusterCaCertLabels, clusterCaCertAnnotations,
                            clusterCaConfig != null && !clusterCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef,
                            Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, dateSupplier));

                    // When we are not supposed to generate the CA, but it does not exist, we should just throw an error
                    checkCustomCaSecret(clientsCaConfig, clientsCaCertSecret, clientsCaKeySecret, "Clients CA");

                    clientsCa = new ClientsCa(reconciliation, certManager,
                            passwordGenerator, clientsCaCertName,
                            clientsCaCertSecret, clientsCaKeyName,
                            clientsCaKeySecret,
                            ModelUtils.getCertificateValidity(clientsCaConfig),
                            ModelUtils.getRenewalDays(clientsCaConfig),
                            clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority(), clientsCaConfig != null ? clientsCaConfig.getCertificateExpirationPolicy() : null);
                    clientsCa.initBrokerSecret(brokersSecret);
                    clientsCa.createRenewOrReplace(reconciliation.namespace(), reconciliation.name(),
                            caLabels, Map.of(), Map.of(),
                            clientsCaConfig != null && !clientsCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef,
                            Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, dateSupplier));

                    @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                    List<Future> secretReconciliations = new ArrayList<>(2);

                    if (clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority())   {
                        Future<ReconcileResult<Secret>> clusterSecretReconciliation = secretOperator.reconcile(reconciliation, reconciliation.namespace(), clusterCaCertName, clusterCa.caCertSecret())
                                .compose(ignored -> secretOperator.reconcile(reconciliation, reconciliation.namespace(), clusterCaKeyName, clusterCa.caKeySecret()));
                        secretReconciliations.add(clusterSecretReconciliation);
                    }

                    if (clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority())   {
                        Future<ReconcileResult<Secret>> clientsSecretReconciliation = secretOperator.reconcile(reconciliation, reconciliation.namespace(), clientsCaCertName, clientsCa.caCertSecret())
                            .compose(ignored -> secretOperator.reconcile(reconciliation, reconciliation.namespace(), clientsCaKeyName, clientsCa.caKeySecret()));
                        secretReconciliations.add(clientsSecretReconciliation);
                    }

                    CompositeFuture.join(secretReconciliations).onComplete(res -> {
                        if (res.succeeded())    {
                            future.complete();
                        } else {
                            future.fail(res.cause());
                        }
                    });
                } catch (Throwable e) {
                    future.fail(e);
                }
            }, true,
            resultPromise
        );

        return resultPromise.future();
    }

    /**
     * Utility method for checking the Secret existence when custom CA is used. The custom CA is configured but the
     * secrets do not exist, it will throw InvalidConfigurationException.
     *
     * @param ca            The CA Configuration from the Custom Resource
     * @param certSecret    Secret with the certificate public key
     * @param keySecret     Secret with the certificate private key
     * @param caDescription The name of the CA for which this check is executed ("Cluster CA" or "Clients CA" - used
     *                      in the exception message)
     */
    private void checkCustomCaSecret(CertificateAuthority ca, Secret certSecret, Secret keySecret, String caDescription)   {
        if (ca != null && !ca.isGenerateCertificateAuthority() && (certSecret == null || keySecret == null))   {
            throw new InvalidResourceException(caDescription + " should not be generated, but the secrets were not found.");
        }
    }

    Future<Void> clusterOperatorSecret(Supplier<Date> dateSupplier) {
        oldCoSecret = clusterCa.clusterOperatorSecret();
        Secret secret = ModelUtils.buildSecret(
                reconciliation,
                clusterCa,
                clusterCa.clusterOperatorSecret(),
                reconciliation.namespace(),
                ClusterOperator.secretName(reconciliation.name()),
                "cluster-operator",
                "cluster-operator",
                clusterOperatorSecretLabels,
                ownerRef,
                Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, dateSupplier)
        );

        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), ClusterOperator.secretName(reconciliation.name()), secret)
                .map((Void) null);
    }

    /**
     * Perform a rolling update of the cluster so that CA certificates get added to their truststores, or expired CA
     * certificates get removed from their truststores. Note this is only necessary when the CA certificate has changed
     * due to a new CA key. It is not necessary when the CA certificate is replace while retaining the existing key.
     */
    Future<Void> rollingUpdateForNewCaKey() {
        List<String> reason = new ArrayList<>(2);

        if (clusterCa.keyReplaced()) {
            reason.add("trust new cluster CA certificate signed by new key");
        }

        if (clientsCa.keyReplaced()) {
            reason.add("trust new clients CA certificate signed by new key");
        }

        if (!reason.isEmpty()) {
            Future<Void> zkRollFuture;
            Function<Pod, List<String>> rollPodAndLogReason = pod -> {
                LOGGER.debugCr(reconciliation, "Rolling Pod {} to {}", pod.getMetadata().getName(), reason);
                return reason;
            };

            if (clusterCa.keyReplaced()) {
                // ZooKeeper is rolled only for new Cluster CA key
                Labels zkSelectorLabels = Labels.EMPTY
                        .withStrimziKind(reconciliation.kind())
                        .withStrimziCluster(reconciliation.name())
                        .withStrimziName(KafkaResources.zookeeperStatefulSetName(reconciliation.name()));

                zkRollFuture = new ZooKeeperRoller(podOperator, zookeeperLeaderFinder, operationTimeoutMs)
                        .maybeRollingUpdate(reconciliation, zkSelectorLabels, rollPodAndLogReason, clusterCa.caCertSecret(), oldCoSecret);
            } else {
                zkRollFuture = Future.succeededFuture();
            }

            return zkRollFuture
                    .compose(i -> {
                        if (featureGates.useStrimziPodSetsEnabled())   {
                            return strimziPodSetOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()))
                                    .compose(podSet -> {
                                        if (podSet != null) {
                                            return Future.succeededFuture(KafkaCluster.generatePodList(reconciliation.name(), podSet.getSpec().getPods().size()));
                                        } else {
                                            return Future.succeededFuture(List.<String>of());
                                        }
                                    });
                        } else {
                            return stsOperator.getAsync(reconciliation.namespace(), KafkaResources.kafkaStatefulSetName(reconciliation.name()))
                                    .compose(sts -> {
                                        if (sts != null)    {
                                            return Future.succeededFuture(KafkaCluster.generatePodList(reconciliation.name(), sts.getSpec().getReplicas()));
                                        } else {
                                            return Future.succeededFuture(List.<String>of());
                                        }
                                    });
                        }
                    })
                    .compose(replicas ->
                            new KafkaRoller(
                                    reconciliation,
                                    vertx,
                                    podOperator,
                                    1_000,
                                    operationTimeoutMs,
                                    () -> new BackOff(250, 2, 10),
                                    replicas,
                                    clusterCa.caCertSecret(),
                                    oldCoSecret,
                                    adminClientProvider,
                                    brokerId -> null,
                                    null,
                                    null,
                                    false
                            ).rollingRestart(rollPodAndLogReason))
                    .compose(i -> {
                        if (clusterCa.keyReplaced()) {
                            // EO, KE and CC need to be rolled only for new Cluster CA key.
                            return rollDeploymentIfExists(KafkaResources.entityOperatorDeploymentName(reconciliation.name()), reason.toString())
                                    .compose(i2 -> rollDeploymentIfExists(KafkaExporterResources.deploymentName(reconciliation.name()), reason.toString()))
                                    .compose(i2 -> rollDeploymentIfExists(CruiseControlResources.deploymentName(reconciliation.name()), reason.toString()));
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Rolls deployments when they exist. This method is used by the CA renewal to roll deployments.
     *
     * @param deploymentName    Name of the deployment which should be rolled if it exists
     * @param reasons           Reasons for which it is being rolled
     *
     * @return  Succeeded future if it succeeded, failed otherwise.
     */
    private Future<Void> rollDeploymentIfExists(String deploymentName, String reasons)  {
        return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                .compose(dep -> {
                    if (dep != null) {
                        LOGGER.infoCr(reconciliation, "Rolling Deployment {} to {}", deploymentName, reasons);
                        return deploymentOperator.rollingUpdate(reconciliation, reconciliation.namespace(), deploymentName, operationTimeoutMs);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Remove older cluster CA certificates if present in the corresponding Secret after a renewal by replacing the
     * corresponding CA private key.
     */
    Future<Void> maybeRemoveOldClusterCaCertificates() {
        // Building the selector for Kafka related components
        Labels labels =  Labels.forStrimziCluster(reconciliation.name()).withStrimziKind(Kafka.RESOURCE_KIND);

        return podOperator.listAsync(reconciliation.namespace(), labels)
                .compose(pods -> {
                    // still no Pods, a new Kafka cluster is under creation
                    if (pods.isEmpty()) {
                        return Future.succeededFuture();
                    }

                    int clusterCaCertGeneration = clusterCa.certGeneration();

                    LOGGER.debugCr(reconciliation, "Current cluster CA cert generation {}", clusterCaCertGeneration);

                    // only if all Kafka related components pods are updated to the new cluster CA cert generation,
                    // there is the possibility that we should remove the older cluster CA from the Secret and stores
                    for (Pod pod : pods) {
                        int podClusterCaCertGeneration = Annotations.intAnnotation(pod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, clusterCaCertGeneration);
                        LOGGER.debugCr(reconciliation, "Pod {} cluster CA cert generation {}", pod.getMetadata().getName(), podClusterCaCertGeneration);

                        if (clusterCaCertGeneration != podClusterCaCertGeneration) {
                            return Future.succeededFuture();
                        }
                    }

                    LOGGER.debugCr(reconciliation, "Maybe there are old cluster CA certificates to remove");
                    clusterCa.maybeDeleteOldCerts();

                    return Future.succeededFuture(clusterCa);
                })
                .compose(ca -> {
                    if (ca != null && ca.certsRemoved()) {
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), AbstractModel.clusterCaCertSecretName(reconciliation.name()), ca.caCertSecret())
                                .map((Void) null);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Helper class to pass both Cluster and Clients CA as a result of the reconciliation
     */
    public static class CaReconciliationResult {
        public final ClusterCa clusterCa;
        public final ClientsCa clientsCa;

        public CaReconciliationResult(ClusterCa clusterCa, ClientsCa clientsCa) {
            this.clusterCa = clusterCa;
            this.clientsCa = clientsCa;
        }
    }
}
