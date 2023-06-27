/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.DeploymentResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.STRIMZI_DEPLOYMENT_NAME;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

/**
 * This suite contains tests related to StrimziPodSet and its features.<br>
 * The StrimziPodSets can be enabled by `STRIMZI_FEATURE_GATES` environment variable, and
 * they should be replacement for StatefulSets in the future.
 */
@Tag(REGRESSION)
public class PodSetIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(PodSetIsolatedST.class);

    @IsolatedTest("We are changing CO env variables in this test")
    void testPodSetOnlyReconciliation(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final Map<String, String> coPod = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME);
        final int replicas = 3;
        final int probeTimeoutSeconds = 6;

        EnvVar reconciliationEnv = new EnvVar(Environment.STRIMZI_POD_SET_RECONCILIATION_ONLY_ENV, "true", null);
        List<EnvVar> envVars = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME).getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        envVars.add(reconciliationEnv);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), replicas).build());

        LOGGER.info("Changing {} to 'true', so only SPS will be reconciled", Environment.STRIMZI_POD_SET_RECONCILIATION_ONLY_ENV);

        DeploymentResource.replaceDeployment(Constants.STRIMZI_DEPLOYMENT_NAME,
            coDep -> coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envVars), clusterOperator.getDeploymentNamespace());

        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME, 1, coPod);

        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        LOGGER.info("Changing Kafka resource configuration, the Pods should not be rolled");

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> {
                kafka.getSpec().getKafka().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(probeTimeoutSeconds).build());
            }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitForNoKafkaAndZKRollingUpdate(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaPods);

        LOGGER.info("Deleting one Kafka pod, the should be recreated");
        kubeClient().deletePodWithName(testStorage.getNamespaceName(), KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), replicas, true);

        kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        LOGGER.info("Removing {} env from CO", Environment.STRIMZI_POD_SET_RECONCILIATION_ONLY_ENV);

        envVars.remove(reconciliationEnv);
        DeploymentResource.replaceDeployment(Constants.STRIMZI_DEPLOYMENT_NAME,
            coDep -> coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envVars), clusterOperator.getDeploymentNamespace());

        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME, 1, coPod);

        LOGGER.info("Because the configuration was changed, Pods should be rolled");

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), replicas, kafkaPods);
    }

    @BeforeAll
    void setup(final ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }
}
