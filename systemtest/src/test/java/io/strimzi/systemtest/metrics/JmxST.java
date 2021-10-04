/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.specific.JmxUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.Map;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
public class JmxST extends AbstractST {

    public static final String NAMESPACE = "jmx-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(JmxST.class);

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaZookeeperAndKafkaConnectWithJMX(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String zkSecretName = clusterName + "-zookeeper-jmx";

        Map<String, String> jmxSecretLabels = Collections.singletonMap("my-label", "my-value");
        Map<String, String> jmxSecretAnnotations = Collections.singletonMap("my-annotation", "some-value");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editOrNewSpec()
                .editKafka()
                    .withNewJmxOptions()
                        .withAuthentication(new KafkaJmxAuthenticationPassword())
                    .endJmxOptions()
                .endKafka()
                .editOrNewZookeeper()
                    .withNewJmxOptions()
                        .withAuthentication(new KafkaJmxAuthenticationPassword())
                    .endJmxOptions()
                    .editOrNewTemplate()
                        .withNewJmxSecret()
                            .withNewMetadata()
                                .withLabels(jmxSecretLabels)
                                .withAnnotations(jmxSecretAnnotations)
                            .endMetadata()
                        .endJmxSecret()
                    .endTemplate()
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        String clientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, true)
            .editOrNewSpec()
                .withNewJmxOptions()
                    .withAuthentication(new KafkaJmxAuthenticationPassword())
                .endJmxOptions()
            .endSpec()
            .build());

        Secret jmxZkSecret = kubeClient().getSecret(namespaceName, zkSecretName);

        String kafkaResults = JmxUtils.execJmxTermAndGetResult(namespaceName, KafkaResources.brokersServiceName(clusterName), clusterName + "-kafka-jmx", clientsPodName, "bean kafka.server:type=app-info\nget -i *");
        String kafkaConnectResults = JmxUtils.execJmxTermAndGetResult(namespaceName, KafkaConnectResources.serviceName(clusterName), clusterName + "-kafka-connect-jmx", clientsPodName, "bean kafka.connect:type=app-info\nget -i *");
        String zkResults = JmxUtils.execJmxTermAndGetResult(namespaceName, KafkaResources.zookeeperHeadlessServiceName(clusterName), zkSecretName, clientsPodName, "bean org.apache.ZooKeeperService:name0=ReplicatedServer_id1\nget -i *");

        assertThat("Result from Kafka JMX doesn't contain right version of Kafka, result: " + kafkaResults, kafkaResults, containsString("version = " + Environment.ST_KAFKA_VERSION));
        assertThat("Result from KafkaConnect JMX doesn't contain right version of Kafka, result: " + kafkaConnectResults, kafkaConnectResults, containsString("version = " + Environment.ST_KAFKA_VERSION));
        assertThat("Result from Zookeeper JMX doesn't contain right quorum size, result: " + zkResults, zkResults, containsString("QuorumSize = 3"));

        LOGGER.info("Checking that Zookeeper JMX secret is created with custom labels and annotations");
        assertTrue(jmxZkSecret.getMetadata().getLabels().entrySet().containsAll(jmxSecretLabels.entrySet()));
        assertTrue(jmxZkSecret.getMetadata().getAnnotations().entrySet().containsAll(jmxSecretAnnotations.entrySet()));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();
    }
}
