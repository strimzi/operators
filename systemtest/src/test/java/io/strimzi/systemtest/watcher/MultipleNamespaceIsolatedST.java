/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;

@Tag(REGRESSION)
class MultipleNamespaceIsolatedST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleNamespaceIsolatedST.class);

    private void deployTestSpecificClusterOperator(final ExtensionContext extensionContext) {
        LOGGER.info("Creating Cluster Operator which will watch over multiple Namespaces");

        cluster.createNamespaces(CollectorElement.createCollectorElement(this.getClass().getName()), clusterOperator.getDeploymentNamespace(), Arrays.asList(PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE));

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                .withExtensionContext(extensionContext)
                .withNamespace(INFRA_NAMESPACE)
                .withWatchingNamespaces(String.join(",", INFRA_NAMESPACE, PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE))
                .withBindingsNamespaces(Arrays.asList(INFRA_NAMESPACE, PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE))
                .createInstallation()
                .runInstallation();
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        deployTestSpecificClusterOperator(extensionContext);

        LOGGER.info("deploy all other resources (Kafka Cluster and Scrapper) for testing Namespaces");
        deployAdditionalGenericResourcesForAbstractNamespaceST(extensionContext);
    }
}