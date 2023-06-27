/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
class AllNamespaceIsolatedST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(AllNamespaceIsolatedST.class);

    private void deployTestSpecificClusterOperator(final ExtensionContext extensionContext) {
        LOGGER.info("Creating Cluster Operator which will watch over all Namespaces");

        cluster.createNamespaces(CollectorElement.createCollectorElement(this.getClass().getName()), clusterOperator.getDeploymentNamespace(), Arrays.asList(PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE));

        clusterOperator = clusterOperator.defaultInstallation(extensionContext)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .createInstallation()
            .runInstallation();
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        // Strimzi is deployed with cluster-wide access in this class STRIMZI_RBAC_SCOPE=NAMESPACE won't work
        assumeFalse(Environment.isNamespaceRbacScope());

        deployTestSpecificClusterOperator(extensionContext);

        LOGGER.info("Deploying all other resources (Kafka cluster and Scrapper) for testing Namespaces");
        deployAdditionalGenericResourcesForAbstractNamespaceST(extensionContext);
    }
}
