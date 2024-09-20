/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.MixedRoleNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.VerificationUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.SANITY;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@SuiteDoc(
    description = @Desc("This test suite validates the functionality and behavior of Cruise Control across multiple Kafka scenarios. " +
        "It ensures correct operation under various configurations and conditions."),
    beforeTestSteps = {
        @Step(value = "Deploy cluster operator with default installation", expected = "Cluster operator is deployed and running")
    },
    labels = {
        @Label(value = "cruise-control"),
    }
)
public class CruiseControlST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlST.class);

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test verifying the automatic creation and configuration of Cruise Control topics with resources."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Set up Kafka brokers and Cruise Control with necessary configurations", expected = "Resources are created with the desired configurations"),
            @Step(value = "Validate Cruise Control pod's memory resource limits and JVM options", expected = "Memory limits and JVM options are correctly set on the Cruise Control pod"),
            @Step(value = "Create a Kafka topic and an AdminClient", expected = "Kafka topic and AdminClient are successfully created"),
            @Step(value = "Verify Cruise Control topics are present", expected = "Cruise Control topics are found present in the configuration")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testAutoCreationOfCruiseControlTopicsWithResources() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // number of brokers to be created and also number of default replica count for each topic created
        final int defaultBrokerReplicaCount = 3;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), defaultBrokerReplicaCount).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), defaultBrokerReplicaCount).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), defaultBrokerReplicaCount, defaultBrokerReplicaCount)
            .editOrNewSpec()
                .editKafka()
                    .addToConfig(Map.of("default.replication.factor", defaultBrokerReplicaCount))
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
                .editCruiseControl()
                    .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("300Mi"))
                        .addToRequests("memory", new Quantity("300Mi"))
                        .build())
                    .withNewJvmOptions()
                        .withXmx("200M")
                        .withXms("128M")
                        .withXx(Map.of("UseG1GC", "true"))
                    .endJvmOptions()
                .endCruiseControl()
            .endSpec()
            .build());

        String ccPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        Container container = (Container) KubeClusterResource.kubeClient().getPod(testStorage.getNamespaceName(), ccPodName).getSpec().getContainers().stream().filter(c -> c.getName().equals("cruise-control")).findFirst().get();
        assertThat(container.getResources().getLimits().get("memory"), is(new Quantity("300Mi")));
        assertThat(container.getResources().getRequests().get("memory"), is(new Quantity("300Mi")));
        VerificationUtils.assertJvmOptions(testStorage.getNamespaceName(), ccPodName, "cruise-control",
                "-Xmx200M", "-Xms128M", "-XX:+UseG1GC");

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());
        resourceManager.createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(testStorage.getClusterName())
            ).build()
        );
        final AdminClient adminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());
        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(adminClient, defaultBrokerReplicaCount);
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test the Cruise Control functionality when API security is disabled."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create a Kafka cluster with Cruise Control having API security disabled", expected = "Kafka cluster with Cruise Control and no API security is deployed"),
            @Step(value = "Create a Kafka Rebalance resource", expected = "Kafka Rebalance resource is successfully created"),
            @Step(value = "Wait for the Kafka Rebalance custom resource state to become ProposalReady", expected = "Kafka Rebalance custom resource state is ProposalReady")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testCruiseControlWithApiSecurityDisabled() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 3)
                .editOrNewSpec()
                    .editCruiseControl()
                        .addToConfig("webserver.security.enable", "false")
                        .addToConfig("webserver.ssl.enable", "false")
                    .endCruiseControl()
                .endSpec()
                .build());
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @TestDoc(
        description = @Desc("Using Kafka cluster within a single namespace to test Cruise Control with rebalance resource and refresh annotation."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create Kafka cluster", expected = "Kafka cluster with ephemeral storage is created and available"),
            @Step(value = "Deploy Kafka Rebalance resource", expected = "Kafka Rebalance resource is deployed and in NotReady state"),
            @Step(value = "Enable Cruise Control with tuned spec", expected = "Cruise Control is enabled and configured"),
            @Step(value = "Perform rolling update on broker pods", expected = "All broker pods have rolled successfully"),
            @Step(value = "Execute rebalance process", expected = "Rebalancing process executed successfully"),
            @Step(value = "Annotate Kafka Rebalance resource with 'refresh'", expected = "Kafka Rebalance resource is annotated with 'refresh' and reaches ProposalReady state"),
            @Step(value = "Execute rebalance process again", expected = "Rebalancing process re-executed successfully")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testCruiseControlWithRebalanceResourceAndRefreshAnnotation() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 3).build());
        resourceManager.createResourceWithoutWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.NotReady);

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        // CruiseControl spec is now enabled
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            // Get default CC spec with tune options and set it to existing Kafka
            Kafka kafkaUpdated = KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 3).build();
            kafka.getSpec().setCruiseControl(kafkaUpdated.getSpec().getCruiseControl());
            kafka.getSpec().setKafka(kafkaUpdated.getSpec().getKafka());
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Annotating KafkaRebalance: {} with 'refresh' anno", testStorage.getClusterName());
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.refresh);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Trying rebalancing process again");
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test that ensures Cruise Control transitions from Rebalancing to ProposalReady state when the KafkaRebalance spec is updated."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create Kafka cluster with Cruise Control", expected = "Kafka cluster with Cruise Control is created and running"),
            @Step(value = "Create KafkaRebalance resource", expected = "KafkaRebalance resource is created and running"),
            @Step(value = "Wait until KafkaRebalance is in ProposalReady state", expected = "KafkaRebalance reaches ProposalReady state"),
            @Step(value = "Annotate KafkaRebalance with 'approve'", expected = "KafkaRebalance is annotated with approval"),
            @Step(value = "Update KafkaRebalance spec to configure replication throttle", expected = "KafkaRebalance resource's spec is updated"),
            @Step(value = "Wait until KafkaRebalance returns to ProposalReady state", expected = "KafkaRebalance re-enters ProposalReady state following the update")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testCruiseControlChangesFromRebalancingtoProposalReadyWhenSpecUpdated() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 1).build());

        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Annotating KafkaRebalance: {} with 'approve' anno", testStorage.getClusterName());
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);

        // updating the KafkaRebalance resource by configuring replication throttle
        KafkaRebalanceResource.replaceKafkaRebalanceResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaRebalance -> kafkaRebalance.getSpec().setReplicationThrottle(100000));

        // the resource moved to `ProposalReady` state
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying that Cruise Control cannot be deployed with a Kafka cluster that has only one broker and ensuring that increasing the broker count resolves the configuration error."),
        steps = {
            @Step(value = "Set up the error message", expected = "Error message is set"),
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Deploy single-node Kafka with Cruise Control", expected = "Kafka and Cruise Control deployment initiated"),
            @Step(value = "Verify that the Kafka status contains the error message related to single-node configuration", expected = "Error message confirmed in Kafka status"),
            @Step(value = "Increase the Kafka nodes to 3", expected = "Kafka node count increased to 3"),
            @Step(value = "Check that the Kafka status no longer contains the single-node error message", expected = "Error message resolved")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testCruiseControlWithSingleNodeKafka() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String errMessage =  "Kafka " + testStorage.getNamespaceName() + "/" + testStorage.getClusterName() + " has invalid configuration. " +
            "Cruise Control cannot be deployed with a Kafka cluster which has only one broker. " +
                "It requires at least two Kafka brokers.";

        LOGGER.info("Deploying single node Kafka with CruiseControl");
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithoutWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 1, 1).build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), errMessage, Duration.ofMinutes(6).toMillis());

        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();

        assertThat(kafkaStatus.getConditions().stream().filter(c -> "InvalidResourceException".equals(c.getReason())).findFirst().orElse(null), is(notNullValue()));

        LOGGER.info("Increasing Kafka nodes to 3");

        int scaleTo = 3;

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(),
                knp -> knp.getSpec().setReplicas(scaleTo));
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setReplicas(3));
        }
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        kafkaStatus = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is(not(errMessage)));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verify that Kafka Cruise Control excludes specified topics and includes others."),
        steps = {
            @Step(value = "Set topic names", expected = "Topic names are set"),
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Deploy Kafka with Cruise Control enabled", expected = "Kafka cluster with Cruise Control is deployed"),
            @Step(value = "Create topics to be excluded and included", expected = "Topics 'excluded-topic-1', 'excluded-topic-2', and 'included-topic' are created"),
            @Step(value = "Create KafkaRebalance resource excluding specific topics", expected = "KafkaRebalance resource is created with 'excluded-.*' topics pattern"),
            @Step(value = "Wait for KafkaRebalance to reach ProposalReady state", expected = "KafkaRebalance reaches the ProposalReady state"),
            @Step(value = "Check optimization result for excluded and included topics", expected = "Excluded topics are in the optimization result and included topic is not"),
            @Step(value = "Approve the KafkaRebalance proposal", expected = "KafkaRebalance proposal is approved"),
            @Step(value = "Wait for KafkaRebalance to reach Ready state", expected = "KafkaRebalance reaches the Ready state")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testCruiseControlTopicExclusion() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String excludedTopic1 = "excluded-topic-1";
        final String excludedTopic2 = "excluded-topic-2";
        final String includedTopic = "included-topic";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 1).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), excludedTopic1, testStorage.getClusterName()).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), excludedTopic2, testStorage.getClusterName()).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), includedTopic, testStorage.getClusterName()).build());

        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editOrNewSpec()
                .withExcludedTopics("excluded-.*")
            .endSpec()
            .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Checking status of KafkaRebalance");
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic1));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic2));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), not(containsString(includedTopic)));

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.Ready);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test that verifies the configuration and application of custom Cruise Control replica movement strategies."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create Kafka and Cruise Control resources", expected = "Kafka and Cruise Control resources are created and deployed"),
            @Step(value = "Verify default Cruise Control replica movement strategy", expected = "Default replica movement strategy is verified in the configuration"),
            @Step(value = "Update Cruise Control configuration with non-default replica movement strategies", expected = "Cruise Control configuration is updated with new strategies"),
            @Step(value = "Ensure that Cruise Control pod is rolled due to configuration change", expected = "Cruise Control pod is rolled and new configuration is applied"),
            @Step(value = "Verify the updated Cruise Control configuration", expected = "Updated replica movement strategies are verified in Cruise Control configuration")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testCruiseControlReplicaMovementStrategy() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String replicaMovementStrategies = "default.replica.movement.strategies";
        final String newReplicaMovementStrategies = "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy," +
            "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy," +
            "com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 3).build());

        String ccPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();

        LOGGER.info("Check for default CruiseControl replicaMovementStrategy in Pod configuration file");
        Map<String, Object> actualStrategies = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName()).get().getSpec().getCruiseControl().getConfig();
        // Check that config contains only configurations for max.active.user.tasks, metric.sampling.interval.ms, cruise.control.metrics.reporter.metrics.reporting.interval.ms, metadata.max.age.ms
        assertThat(actualStrategies.size(), is(4));

        String ccConfFileContent = cmdKubeClient(testStorage.getNamespaceName()).execInPodContainer(ccPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME, "cat", TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, not(containsString(replicaMovementStrategies)));

        Map<String, String> kafkaRebalanceSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));

        Map<String, Object> ccConfigMap = new HashMap<>();
        ccConfigMap.put(replicaMovementStrategies, newReplicaMovementStrategies);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            LOGGER.info("Set non-default CruiseControl replicaMovementStrategies to KafkaRebalance resource");
            kafka.getSpec().getCruiseControl().setConfig(ccConfigMap);
        });

        LOGGER.info("Verifying that CC Pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()), 1, kafkaRebalanceSnapshot);

        ccPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();
        ccConfFileContent = cmdKubeClient(testStorage.getNamespaceName()).execInPodContainer(ccPodName, TestConstants.CRUISE_CONTROL_CONTAINER_NAME, "cat", TestConstants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, containsString(newReplicaMovementStrategies));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test ensuring the intra-broker disk balancing with Cruise Control works as expected."),
        steps = {
            @Step(value = "Initialize JBOD storage configuration", expected = "JBOD storage with specific disk sizes are initialized"),
            @Step(value = "Create Kafka broker and controller pools using the initialized storage", expected = "Kafka broker and controller pools are created and available"),
            @Step(value = "Deploy Kafka with Cruise Control enabled", expected = "Kafka deployment with Cruise Control is successfully created"),
            @Step(value = "Create Kafka Rebalance resource with disk rebalancing configured", expected = "Kafka Rebalance resource is created and configured for disk balancing"),
            @Step(value = "Wait for the Kafka Rebalance to reach the 'ProposalReady' state", expected = "Kafka Rebalance resource reaches the 'ProposalReady' state"),
            @Step(value = "Check the status of the Kafka Rebalance for intra-broker disk balancing", expected = "The 'provisionStatus' in the optimization result is 'UNDECIDED'")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testCruiseControlIntraBrokerBalancing() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String diskSize = "6Gi";

        JbodStorage jbodStorage =  new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSize).build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSize).build()
                ).build();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                    .editSpec()
                        .withStorage(jbodStorage)
                    .endSpec()
                    .build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 3)
                .editOrNewSpec()
                    .editKafka()
                        .withStorage(jbodStorage)
                    .endKafka()
                .endSpec()
                .build());
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editOrNewSpec()
                    .withRebalanceDisk(true)
                .endSpec()
                .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Checking status of KafkaRebalance");
        // The provision status should be "UNDECIDED" when doing an intra-broker disk balance because it is irrelevant to the provision status
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("provisionStatus").toString(), containsString("UNDECIDED"));
    }

    @IsolatedTest
    @MixedRoleNotSupported("Scaling a Kafka Node Pool with mixed roles is not supported yet")
    @TestDoc(
        description = @Desc("Testing the behavior of Cruise Control during both scaling up and down of Kafka brokers using node pools."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Create initial Kafka cluster setup with Cruise Control and topic", expected = "Kafka cluster, topic, and scraper pod are created successfully."),
            @Step(value = "Scale Kafka up to a higher number of brokers", expected = "Kafka brokers are scaled up to the specified number of replicas."),
            @Step(value = "Create a KafkaRebalance resource with add_brokers mode", expected = "KafkaRebalance proposal is ready and processed for adding brokers."),
            @Step(value = "Check the topic's replicas on the new brokers", expected = "Topic has replicas on one of the newly added brokers."),
            @Step(value = "Create a KafkaRebalance resource with remove_brokers mode", expected = "KafkaRebalance proposal is ready and processed for removing brokers."),
            @Step(value = "Check the topic's replicas only on initial brokers", expected = "Topic replicas are only on the initial set of brokers."),
            @Step(value = "Scale Kafka down to the initial number of brokers", expected = "Kafka brokers are scaled down to the original number of replicas.")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testCruiseControlDuringBrokerScaleUpAndDown() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);
        final int initialReplicas = 3;
        final int scaleTo = 5;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialReplicas).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialReplicas)
                    .editOrNewMetadata()
                        // controllers have Ids set in order to keep default ordering for brokers only (once we scale broker KNP)
                        .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[100-103]"))
                    .endMetadata()
                .build()
            )

        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), initialReplicas, initialReplicas).build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 10, 3).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Scaling Kafka up to {}", scaleTo);
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setReplicas(scaleTo));
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setReplicas(scaleTo));
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaleTo);

        LOGGER.info("Creating KafkaRebalance with add_brokers mode");

        // when using add_brokers mode, we can hit `ProposalReady` right after KR creation - that's why `waitReady` is set to `false` here
        resourceManager.createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.ADD_BROKERS)
                    .withBrokers(3, 4)
                .endSpec()
                .build()
        );

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(),  testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).delete();

        LOGGER.info("Checking that Topic: {} has replicas on one of the new brokers (or both)", testStorage.getTopicName());
        List<String> topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertTrue(topicReplicas.stream().anyMatch(line -> line.contains("3") || line.contains("4")));

        LOGGER.info("Creating KafkaRebalance with remove_brokers mode - it needs to be done before actual scaling down of Kafka Pods");

        // when using remove_brokers mode, we can hit `ProposalReady` right after KR creation - that's why `waitReady` is set to `false` here
        resourceManager.createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                    .withBrokers(3, 4)
                .endSpec()
                .build()
        );

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(),  testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Checking that Topic: {} has replicas only on first 3 brokers", testStorage.getTopicName());
        topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertEquals(0, (int) topicReplicas.stream().filter(line -> line.contains("3") || line.contains("4")).count());

        LOGGER.info("Scaling Kafka down to {}", initialReplicas);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setReplicas(initialReplicas));
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setReplicas(initialReplicas));
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), initialReplicas);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test the Kafka Rebalance auto-approval mechanism."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created"),
            @Step(value = "Deploy Kafka cluster with Cruise Control.", expected = "Kafka cluster with Cruise Control is deployed."),
            @Step(value = "Create KafkaRebalance resource with auto-approval enabled.", expected = "KafkaRebalance resource with auto-approval is created."),
            @Step(value = "Perform re-balancing process with auto-approval.", expected = "Re-balancing process completes successfully with auto-approval.")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
    void testKafkaRebalanceAutoApprovalMechanism() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 3).build());

        // KafkaRebalance with auto-approval
        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true")
            .endMetadata()
            .build()
        );

        KafkaRebalanceUtils.doRebalancingProcessWithAutoApproval(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @BeforeAll
    void setUp() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
