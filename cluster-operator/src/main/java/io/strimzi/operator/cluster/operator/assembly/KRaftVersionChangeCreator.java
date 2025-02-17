/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaUpgradeException;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;

import java.util.List;

import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedIVVersions;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedVersions;

/**
 * Creates the KafkaVersionChange object for a KRaft based clusters from the different versions in the Kafka CR and from the Kafka pods.
 */
public class KRaftVersionChangeCreator {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KRaftVersionChangeCreator.class.getName());

    private final Reconciliation reconciliation;
    private final KafkaVersion.Lookup versions;

    private final PodOperator podOperator;

    // Information about Kafka version from the Kafka custom resource
    private final KafkaVersion versionFromCr;
    private final String desiredMetadataVersion;
    private final String currentMetadataVersion;

    // The target versions which should be set in the result
    private KafkaVersion versionFrom;
    private KafkaVersion versionTo;

    /**
     * Constructs the KRaftVersionChangeCreator which constructs the KafkaVersionChange instance which is describing the
     * upgrade state.
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaCr           The Kafka custom resource
     * @param config            Cluster Operator Configuration
     * @param supplier          Supplier with Kubernetes Resource Operators
     */
    public KRaftVersionChangeCreator(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier
    ) {
        this.reconciliation = reconciliation;

        // Collect information from the Kafka CR
        this.versionFromCr = config.versions().supportedVersion(kafkaCr.getSpec().getKafka().getVersion());
        this.desiredMetadataVersion = kafkaCr.getSpec().getKafka().getMetadataVersion();

        this.currentMetadataVersion = kafkaCr.getStatus() != null ? kafkaCr.getStatus().getKafkaMetadataVersion() : null;

        // Store operators and Feature Gates configuration
        this.versions = config.versions();
        this.podOperator = supplier.podOperations;
    }

    /**
     * Collects the information from the Kubernetes resources and creates the KafkaVersionChange instance describing the
     * version change in this reconciliation
     *
     * @return  Future which completes with the KafkaVersionChange instance
     */
    public Future<KafkaVersionChange> reconcile()    {
        return getPods()
                .compose(this::detectToAndFromVersions)
                .compose(i -> prepareVersionChange());
    }

    /**
     * Collects any existing Kafka pods so that we can later get the version information from them.
     *
     * @return  Future which completes when the Kafka broker pods are retrieved
     */
    private Future<List<Pod>> getPods()   {
        Labels selectorLabels = Labels.forStrimziKind(Kafka.RESOURCE_KIND)
                .withStrimziCluster(reconciliation.name())
                .withStrimziName(KafkaResources.kafkaComponentName(reconciliation.name()));

        return podOperator.listAsync(reconciliation.namespace(), selectorLabels);
    }

    /**
     * Detects the current and desired Kafka versions based on the information collected from Kubernetes
     *
     * @return  Future which completes when the "to" and "from" versions are collected
     */
    private Future<Void> detectToAndFromVersions(List<Pod> pods)    {
        String lowestKafkaVersion = null;
        String highestKafkaVersion = null;

        for (Pod pod : pods)    {
            // Collect the Kafka version from the pods
            String currentVersion = Annotations.stringAnnotation(pod, ANNO_STRIMZI_IO_KAFKA_VERSION, null);

            // We find the highest and lowest used Kafka version. This is used to detect any upgrades or
            // downgrades which failed in the middle and continue with them.
            if (currentVersion != null) {
                if (highestKafkaVersion == null)  {
                    highestKafkaVersion = currentVersion;
                } else if (compareDottedVersions(highestKafkaVersion, currentVersion) < 0) {
                    highestKafkaVersion = currentVersion;
                }

                if (lowestKafkaVersion == null)  {
                    lowestKafkaVersion = currentVersion;
                } else if (compareDottedVersions(lowestKafkaVersion, currentVersion) > 0) {
                    lowestKafkaVersion = currentVersion;
                }
            }
        }

        // We decide what is the current Kafka version used and create the KafkaVersionChange object
        // describing the situation.
        if (lowestKafkaVersion == null)   {
            if (pods.isEmpty())  {
                // No pods were found. We use the desired version from the Kafka CR
                versionFrom = versionFromCr;
                versionTo = versionFromCr;
            } else {
                // Pods already exist. However, none of them contains the version annotation. This suggests they are not
                // created by any recent versions of Strimzi. Without the annotation, we cannot detect the Kafka version
                // and decide on upgrade.
                LOGGER.warnCr(reconciliation, "Kafka Pods exist, but do not contain the {} annotation to detect their version. Kafka upgrade cannot be detected.", ANNO_STRIMZI_IO_KAFKA_VERSION);
                throw new KafkaUpgradeException("Kafka Pods exist, but do not contain the " + ANNO_STRIMZI_IO_KAFKA_VERSION + " annotation to detect their version. Kafka upgrade cannot be detected.");
            }
        } else if (compareDottedVersions(highestKafkaVersion, versionFromCr.version()) > 0)    {
            // Highest Kafka version used by the brokers is higher than desired => suspected downgrade
            try {
                versionFrom = versions.version(highestKafkaVersion);
            } catch (KafkaUpgradeException exception) {
                // From version is unknown but thats ok for downgrade
                versionFrom = new KafkaVersion(highestKafkaVersion, null, null, null, false, false, null);
            }
            versionTo = versionFromCr;
        } else {
            // Highest Kafka version used by the brokers is equal or lower than desired => suspected upgrade
            versionFrom = versions.version(lowestKafkaVersion);
            versionTo = versionFromCr;
        }

        return Future.succeededFuture();
    }

    /**
     * Plans the version change and creates a KafkaVersionChange object which contains the main versions as well as the
     * metadata version.
     *
     * @return  Future with the KafkaVersionChange instance describing the Kafka version changes
     */
    private Future<KafkaVersionChange>  prepareVersionChange()  {
        String metadataVersion;

        if (versionFrom.compareTo(versionTo) == 0) { // => no version change
            LOGGER.debugCr(reconciliation, "{}: No Kafka version change", reconciliation);
            metadataVersion = metadataVersionWithoutKafkaVersionChange(reconciliation, currentMetadataVersion, desiredMetadataVersion, versionTo);
        } else {
            if (versionFrom.compareTo(versionTo) < 0) { // => is upgrade
                LOGGER.infoCr(reconciliation, "Kafka is upgrading from {} to {}", versionFrom.version(), versionTo.version());
                metadataVersion = metadataVersionAtUpgrade(reconciliation, currentMetadataVersion, versionFrom);
            } else {
                // Has to be a downgrade
                LOGGER.infoCr(reconciliation, "Kafka is downgrading from {} to {}", versionFrom.version(), versionTo.version());
                metadataVersion = metadataVersionAtDowngrade(reconciliation, currentMetadataVersion, versionTo);
            }
        }

        return Future.succeededFuture(new KafkaVersionChange(versionFrom, versionTo, null, null, metadataVersion));
    }

    /**
     * Determines what KRaft metadata version should be used when Kafka version does not change
     *
     * @param reconciliation            Reconciliation marker
     * @param currentMetadataVersion    Currently used metadata version
     * @param desiredMetadataVersion    Desired metadata version
     * @param version                   Used Kafka version
     *
     * @return  Metadata version that should be used
     */
    /* test */ static String metadataVersionWithoutKafkaVersionChange(Reconciliation reconciliation, String currentMetadataVersion, String desiredMetadataVersion, KafkaVersion version)   {
        if (desiredMetadataVersion != null)  {
            if (compareDottedIVVersions(version.metadataVersion(), desiredMetadataVersion) >= 0)    {
                return desiredMetadataVersion;
            } else if (currentMetadataVersion != null) {
                LOGGER.warnCr(reconciliation, "Desired metadata version ({}) is newer than the used Kafka version ({}). The cluster will continue to use the current metadata version ({}).", desiredMetadataVersion, version.version(), currentMetadataVersion);
                return currentMetadataVersion;
            } else {
                LOGGER.warnCr(reconciliation, "Desired metadata version ({}) is newer than the used Kafka version ({}). Default metadata version for given Kafka version ({}) will be used instead.", desiredMetadataVersion, version.version(), version.metadataVersion());
                return version.metadataVersion();
            }
        } else {
            // No user-set metadata version & no Kafka version change happening => we use the default for the Kafka version
            return version.metadataVersion();
        }
    }

    /**
     * Determines which KRaft metadata version should be used for Kafka upgrade
     *
     * @param reconciliation            Reconciliation marker
     * @param currentMetadataVersion    Currently used metadata version
     * @param versionFrom               Version we are upgrading from
     *
     * @return  Metadata version that should be used
     */
    /* test */ static String metadataVersionAtUpgrade(Reconciliation reconciliation, String currentMetadataVersion, KafkaVersion versionFrom)   {
        if (currentMetadataVersion != null) {
            if (compareDottedIVVersions(currentMetadataVersion, versionFrom.metadataVersion()) > 0)    {
                // The current metadata version is newer than the version we are upgrading from
                //     => something went completely wrong, and we should just throw an error
                LOGGER.warnCr(reconciliation, "The current metadata version ({}) has to be lower or equal to the Kafka broker version we upgrade from ({})", currentMetadataVersion, versionFrom.version());
                throw new KafkaUpgradeException("The current metadata version (" + currentMetadataVersion + ") has to be lower or equal to the Kafka broker version we upgrade from (" + versionFrom.version() + ")");
            } else {
                // We stick with the current metadata version for the first phase of the upgrade
                //     => it will be changed in the next phase (next reconciliation)
                LOGGER.infoCr(reconciliation, "The current metadata version {} will be used in the first phase of the upgrade", currentMetadataVersion);
                return currentMetadataVersion;
            }
        } else {
            // Current metadata version is missing. This should normally not happen in upgrade as it suggests
            // we are upgrading without the previous version being properly deployed. But in case it happens,
            // we use the metadata version from the older version first.
            LOGGER.warnCr(reconciliation, "The current metadata version seems to be missing during upgrade which is unexpected. The metadata version {} of the Kafka we are upgrading from will be used.", versionFrom.metadataVersion());
            return versionFrom.metadataVersion();
        }
    }

    /**
     * Determines which KRaft metadata version should be used for Kafka downgrade
     *
     * @param reconciliation            Reconciliation marker
     * @param currentMetadataVersion    Currently used metadata version
     * @param versionTo                 Version we are downgrading to
     *
     * @return  Metadata version that should be used
     */
    /* test */ static String metadataVersionAtDowngrade(Reconciliation reconciliation, String currentMetadataVersion, KafkaVersion versionTo) {
        if (currentMetadataVersion != null) {
            if (compareDottedIVVersions(currentMetadataVersion, versionTo.metadataVersion()) > 0)    {
                // The current metadata version is newer than the version we are downgrading to
                //     => something went completely wrong, and we should just throw an error
                LOGGER.warnCr(reconciliation, "The current metadata version ({}) has to be lower or equal to the Kafka broker version we are downgrading to ({})", currentMetadataVersion, versionTo.version());
                throw new KafkaUpgradeException("The current metadata version (" + currentMetadataVersion + ") has to be lower or equal to the Kafka broker version we are downgrading to (" + versionTo.version() + ")");
            } else {
                // We stick with the current metadata version for the first phase of the downgrade
                //     => it will be changed in the next phase (next reconciliation)
                LOGGER.infoCr(reconciliation, "The current metadata version {} will be used in the first phase of the downgrade", currentMetadataVersion);
                return currentMetadataVersion;
            }
        } else {
            // Current metadata version is missing. This should normally not happen in downgrade as it suggests
            // we are downgrading without the previous version being properly deployed. But in case it happens,
            // we use the metadata version from the older version first.
            LOGGER.warnCr(reconciliation, "The current metadata version seems to be missing during upgrade which is unexpected. The metadata version {} of the Kafka we are upgrading from will be used.", versionTo.metadataVersion());
            return versionTo.metadataVersion();
        }
    }
}
