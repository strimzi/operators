/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.sundr.builder.annotations.Buildable;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

@Buildable(editableEnabled = false)
public class KafkaAdminClients extends BaseClients {
    private String adminName;

    public String getAdminName() {
        return adminName;
    }

    public void setAdminName(String adminName) {
        this.adminName = adminName;
    }

    public static String getAdminClientScramConfig(String namespace, String kafkaUsername, int timeoutMs) {
        final String saslJaasConfigEncrypted = kubeClient().getSecret(namespace, kafkaUsername).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = new String(Base64.getDecoder().decode(saslJaasConfigEncrypted), StandardCharsets.US_ASCII);
        return
            "request.timeout.ms=" + timeoutMs + "\n" +
            "sasl.mechanism=SCRAM-SHA-512\n" +
            "security.protocol=" + SecurityProtocol.SASL_PLAINTEXT + "\n" +
            "sasl.jaas.config=" + saslJaasConfigDecrypted;
    }

    public Deployment defaultAdmin() {
        Map<String, String> adminLabels = new HashMap<>();
        adminLabels.put("app", adminName);
        adminLabels.put(Constants.KAFKA_ADMIN_CLIENT_LABEL_KEY, Constants.KAFKA_ADMIN_CLIENT_LABEL_VALUE);
        adminLabels.put(Constants.DEPLOYMENT_TYPE, DeploymentTypes.AdminClient.name());

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        return new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(this.getNamespaceName())
                .withLabels(adminLabels)
                .withName(this.getAdminName())
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .addToMatchLabels(adminLabels)
                .endSelector()
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(this.getAdminName())
                        .withNamespace(this.getNamespaceName())
                        .withLabels(adminLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .addNewContainer()
                            .withName(this.getAdminName())
                            .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                            .withImage(Environment.TEST_CLIENTS_IMAGE)
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(this.getBootstrapAddress())
                            .endEnv()
                            .addNewEnv()
                                .withName("ADDITIONAL_CONFIG")
                                .withValue(this.getAdditionalConfig())
                            .endEnv()
                            .addNewEnv()
                                // use custom config folder for admin-client, so we don't need to use service account etc.
                                .withName("CONFIG_FOLDER_PATH")
                                .withValue("/tmp")
                            .endEnv()
                            .withCommand("sleep")
                            .withArgs("infinity")
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }
}
