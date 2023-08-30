/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.jaeger;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Stack;

import static io.strimzi.systemtest.Constants.JAEGER_DEPLOYMENT_POLL;
import static io.strimzi.systemtest.Constants.JAEGER_DEPLOYMENT_TIMEOUT;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_CA_INJECTOR_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_NAMESPACE;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_WEBHOOK_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_INSTANCE_NAME;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_OPERATOR_DEPLOYMENT_NAME;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class SetupJaeger {

    private static final Logger LOGGER = LogManager.getLogger(SetupJaeger.class);

    private static final String CERT_MANAGER_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/cert-manager.yaml";
    private static final String JAEGER_INSTANCE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance.yaml";
    private static final String JAEGER_OPERATOR_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-operator.yaml";

    /**
     * Delete Jaeger instance
     */
    private static void deleteJaeger(String yamlContent) {
        cmdKubeClient().namespace(Constants.TEST_SUITE_NAMESPACE).deleteContent(yamlContent);
    }

    public static void deployJaegerOperatorAndCertManager(ExtensionContext extensionContext) {
        deployAndWaitForCertManager(extensionContext);
        deployJaegerOperator(extensionContext);
    }

    private static void deleteCertManager() {
        cmdKubeClient().delete(CERT_MANAGER_PATH);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT);
    }

    private static void deployCertManager(ExtensionContext extensionContext) {
        // create namespace `cert-manager` and add it to stack, to collect logs from it
        KubeClusterResource.getInstance().createNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName()), CERT_MANAGER_NAMESPACE);
        StUtils.copyImagePullSecrets(CERT_MANAGER_NAMESPACE);

        LOGGER.info("Deploying CertManager from {}", CERT_MANAGER_PATH);
        // because we don't want to apply CertManager's file to specific namespace, passing the empty String will do the trick
        cmdKubeClient("").apply(CERT_MANAGER_PATH);

        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem<>(SetupJaeger::deleteCertManager));
    }

    private static void waitForCertManagerDeployment() {
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT, 1);
    }

    private static void deployAndWaitForCertManager(final ExtensionContext extensionContext) {
        deployCertManager(extensionContext);
        waitForCertManagerDeployment();
    }

    private static void deployJaegerContent(ExtensionContext extensionContext) {
        TestUtils.waitFor("Jaeger deploy", JAEGER_DEPLOYMENT_POLL, JAEGER_DEPLOYMENT_TIMEOUT, () -> {
            try {
                String jaegerOperator = Files.readString(Paths.get(JAEGER_OPERATOR_PATH)).replace("observability", Constants.TEST_SUITE_NAMESPACE);

                LOGGER.info("Creating Jaeger Operator (and needed resources) from {}", JAEGER_OPERATOR_PATH);
                cmdKubeClient(Constants.TEST_SUITE_NAMESPACE).applyContent(jaegerOperator);
                ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem<>(() -> deleteJaeger(jaegerOperator)));

                return true;
            } catch (Exception e) {
                LOGGER.error("Following exception has been thrown during Jaeger Deployment: {}", e.getMessage());
                return false;
            }
        });
        DeploymentUtils.waitForDeploymentAndPodsReady(Constants.TEST_SUITE_NAMESPACE, JAEGER_OPERATOR_DEPLOYMENT_NAME, 1);
    }

    private static void deployJaegerOperator(final ExtensionContext extensionContext) {
        LOGGER.info("=== Applying Jaeger Operator install files ===");

        deployJaegerContent(extensionContext);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(Constants.NETWORK_POLICY)
            .withNewMetadata()
                .withName("jaeger-allow")
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .addNewIngress()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("app", "jaeger")
                .endPodSelector()
                .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        LOGGER.debug("Creating NetworkPolicy: {}", networkPolicy.toString());
        ResourceManager.getInstance().createResourceWithWait(extensionContext, networkPolicy);
        LOGGER.info("Network policy for jaeger successfully created");
    }

    /**
     * Install of Jaeger instance
     */
    public static void deployJaegerInstance(final ExtensionContext extensionContext, String namespaceName) {
        LOGGER.info("=== Applying jaeger instance install file ===");

        String instanceYamlContent = TestUtils.getContent(new File(JAEGER_INSTANCE_PATH), TestUtils::toYamlString);
        cmdKubeClient(namespaceName).applyContent(instanceYamlContent);

        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem<>(() -> cmdKubeClient(namespaceName).deleteContent(instanceYamlContent)));

        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, JAEGER_INSTANCE_NAME, 1);
    }
}
