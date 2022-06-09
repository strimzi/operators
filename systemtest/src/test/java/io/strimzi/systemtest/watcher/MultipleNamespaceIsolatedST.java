/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

@Tag(REGRESSION)
@IsolatedSuite
class MultipleNamespaceIsolatedST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleNamespaceIsolatedST.class);
    private String scraperPodName;

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @IsolatedTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test case")
    void testTopicOperatorWatchingOtherNamespace(ExtensionContext extensionContext) {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");
        cluster.setNamespace(SECOND_NAMESPACE);
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(SECOND_NAMESPACE, scraperPodName, KafkaResources.bootstrapServiceName(MAIN_NAMESPACE_CLUSTER_NAME));
        assertThat(topics, not(hasItems(topicName)));

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(MAIN_NAMESPACE_CLUSTER_NAME, topicName, clusterOperator.getDeploymentNamespace()).build());
        KafkaTopicResource.kafkaTopicClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(topicName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    /**
     * Test the case when Kafka will be deployed in different namespace than CO
     */
    @IsolatedTest
    void testKafkaInDifferentNsThanClusterOperator() {
        LOGGER.info("Deploying Kafka in different namespace than CO when CO watches multiple namespaces");
        checkKafkaInDiffNamespaceThanCO(MAIN_NAMESPACE_CLUSTER_NAME, SECOND_NAMESPACE);
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace across multiple namespaces
     */
    @IsolatedTest
    @Tag(MIRROR_MAKER)
    void testDeployMirrorMakerAcrossMultipleNamespace(ExtensionContext extensionContext) {
        LOGGER.info("Deploying KafkaMirrorMaker in different namespace than CO when CO watches multiple namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO(extensionContext, MAIN_NAMESPACE_CLUSTER_NAME);
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        deployTestSpecificResources(extensionContext);
    }

    private void deployTestSpecificResources(ExtensionContext extensionContext) {
        final String scraperName = MAIN_NAMESPACE_CLUSTER_NAME + "-" + Constants.SCRAPER_NAME;
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(INFRA_NAMESPACE)
            .withWatchingNamespaces(String.join(",", INFRA_NAMESPACE, SECOND_NAMESPACE))
            .withBindingsNamespaces(Arrays.asList(INFRA_NAMESPACE, SECOND_NAMESPACE))
            .createInstallation()
            .runInstallation();

        cluster.setNamespace(SECOND_NAMESPACE);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(MAIN_NAMESPACE_CLUSTER_NAME, 3)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(clusterOperator.getDeploymentNamespace())
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(SECOND_NAMESPACE, scraperName).build()
        );

        scraperPodName = kubeClient().listPodsByPrefixInName(SECOND_NAMESPACE, scraperName).get(0).getMetadata().getName();

        cluster.setNamespace(clusterOperator.getDeploymentNamespace());
    }
}
