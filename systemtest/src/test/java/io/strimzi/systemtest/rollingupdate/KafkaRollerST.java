/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinity;
import io.fabric8.kubernetes.api.model.PodAffinityBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaClusterTemplate;
import io.strimzi.api.kafka.model.kafka.KafkaClusterTemplateBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolSpecBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(ROLLING_UPDATE)
public class KafkaRollerST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRollerST.class);

    @ParallelNamespaceTest
    @SuppressWarnings("deprecation") // Replicas in Kafka CR are deprecated, but some API methods are still called here
    void testKafkaDoesNotRollsWhenTopicIsUnderReplicated() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        Instant startTime = Instant.now();

        final int initialBrokerReplicaCount = 3;
        final int scaledUpBrokerReplicaCount = 4;

        final String topicNameWith3Replicas = testStorage.getTopicName() + "-3";
        final String topicNameWith4Replicas = testStorage.getTopicName() + "-4";

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialBrokerReplicaCount).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialBrokerReplicaCount).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), initialBrokerReplicaCount).build());

        LOGGER.info("Verify expected number of replicas '{}' is present in in Kafka Cluster: {}/{}", initialBrokerReplicaCount, testStorage.getNamespaceName(), testStorage.getClusterName());
        final int observedReplicas = kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getBrokerSelector()).size();
        assertEquals(initialBrokerReplicaCount, observedReplicas);

        LOGGER.info("Create kafkaTopic: {}/{} with replica on each (of 3) broker", testStorage.getNamespaceName(), topicNameWith3Replicas);
        KafkaTopic kafkaTopicWith3Replicas = KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 1, 3, 3).build();
        resourceManager.createResourceWithWait(kafkaTopicWith3Replicas);

        // setup clients
        KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage)
            .withTopicName(topicNameWith3Replicas)
            .build();

        // producing and consuming data when there are 3 brokers ensures that 'consumer_offests' topic will have all of its replicas only across first 3 brokers
        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Scale Kafka up from 3 to 4 brokers");
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp -> knp.getSpec().setReplicas(scaledUpBrokerReplicaCount));
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaledUpBrokerReplicaCount);

        LOGGER.info("Create kafkaTopic: {}/{} with replica on each broker", testStorage.getNamespaceName(), topicNameWith4Replicas);
        KafkaTopic kafkaTopicWith4Replicas = KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicNameWith4Replicas, testStorage.getClusterName(), 1, 4, 4).build();
        resourceManager.createResourceWithWait(kafkaTopicWith4Replicas);

        // last pod has index 3 (as it is 4th) or 6 (being 7th) as there are also 3 controllers
        final int scaledBrokerPodIndex = 6;
        String uid = kubeClient(testStorage.getNamespaceName()).getPodUid(KafkaResource.getKafkaPodName(testStorage.getClusterName(), KafkaNodePoolResource.getBrokerPoolName(testStorage.getClusterName()),  scaledBrokerPodIndex));
        List<Event> events = kubeClient(testStorage.getNamespaceName()).listEventsByResourceUid(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(topicNameWith4Replicas)
            .build();

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Scaling down to {}", initialBrokerReplicaCount);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp -> knp.getSpec().setReplicas(initialBrokerReplicaCount));
        KafkaNodePoolUtils.waitForKafkaNodePoolStatusUpdate(testStorage.getNamespaceName(), testStorage.getBrokerPoolName());
        assertThat("NodePool still has old number of replicas", KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getBrokerPoolName()).get().getStatus().getReplicas(), is(4));

        LOGGER.info("Scale-down should have been reverted and the cluster should be still Ready");
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), ".*Reverting scale-down.*");

        // try to perform rolling update while scale down is being prevented.
        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaledUpBrokerReplicaCount, kafkaPods);

        LOGGER.info("Remove Topic, thereby remove all partitions located on broker to be scaled down");
        resourceManager.deleteResource(kafkaTopicWith4Replicas);
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), initialBrokerReplicaCount);

        //Test that CO doesn't have any exceptions in log
        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        // TODO: currently this asserts makes a lot of tests failing, we should investigate all the failures and fix/whitelist them before enabling
        // this check again - https://github.com/strimzi/strimzi-kafka-operator/issues/9648
        //assertNoCoErrorsLogged(testStorage.getNamespaceName(), duration);
    }

    /**
     * @description This test case verifies that KafkaRoller is able to continue in reconciliations of Kafka if there is invalid KafkaTopic resource.
     *
     * @steps
     *  1. - Deploy Kafka
     *     - Kafka is up and running
     *  2. - Create KafkaTopic with 1 replica and 1 partition
     *     - KafkaTopic is in Ready state
     *  3. - Change KafkaTopic configuration to invalid value - change min.insync.replicas to 2 (higher than available replicas)
     *     - KafkaTopic config changed
     *  4. - Init Kafka rolling-update
     *     - Wait until rolling update is finished
     *  5. - Check that all pods of Kafka were rolled out
     *     - All Kafka pods were rolled out
     *
     * @usecase
     *  - KafkaRoller
     */
    @ParallelNamespaceTest
    void testKafkaTopicRFLowerThanMinInSyncReplicas() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 1, 1).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        LOGGER.info("Setting KafkaTopic's min.insync.replicas to be higher than replication factor");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getTopicName(), kafkaTopic -> kafkaTopic.getSpec().getConfig().replace("min.insync.replicas", 2));

        // Init Kafka rolling update (Do not use ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE due to race caused by STs)
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), kn -> {
            kn.getSpec()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("20m"))
                    .build());
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        assertThat(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()), is(not(brokerPods)));
    }

    @ParallelNamespaceTest
    void testKafkaPodCrashLooping() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewJvmOptions()
                        .withXx(Collections.emptyMap())
                    .endJvmOptions()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withNewJvmOptions()
                        .withXx(Collections.emptyMap())
                    .endJvmOptions()
                .endKafka()
            .endSpec()
            .build());

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().getJvmOptions().setXx(Collections.singletonMap("UseParNewGC", "true")));

        KafkaUtils.waitForKafkaNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().getJvmOptions().setXx(Collections.emptyMap()));

        // kafka should get back ready in some reasonable time frame.
        // Current timeout for wait is set to 14 minutes, which should be enough.
        // No additional checks are needed, because in case of wait failure, the test will not continue.
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @ParallelNamespaceTest
    void testKafkaPodImagePullBackOff() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        String kafkaImage = kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getBrokerSelector()).get(0).getSpec().getContainers().get(0).getImage();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setImage("quay.io/strimzi/kafka:not-existent-tag");
        });

        KafkaUtils.waitForKafkaNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        assertTrue(checkIfExactlyOneKafkaPodIsNotReady(testStorage.getNamespaceName(), testStorage.getClusterName()));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setImage(kafkaImage));

        // kafka should get back ready in some reasonable time frame.
        // Current timeout for wait is set to 14 minutes, which should be enough.
        // No additional checks are needed, because in case of wait failure, the test will not continue.
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @ParallelNamespaceTest
    void testKafkaPodPendingDueToRack() {
        // Testing this scenario
        // 1. deploy Kafka with wrong pod template (looking for nonexistent node) kafka pods should not exist
        // 2. wait for Kafka not ready, kafka pods should be in the pending state
        // 3. fix the Kafka CR, kafka pods should be in the pending state
        // 4. wait for Kafka ready, kafka pods should NOT be in the pending state
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        NodeSelectorRequirement nsr = new NodeSelectorRequirementBuilder()
                .withKey("dedicated_test")
                .withOperator("In")
                .withValues("Kafka")
                .build();

        NodeSelectorTerm nst = new NodeSelectorTermBuilder()
                .withMatchExpressions(nsr)
                .build();

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(nst)
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        PodTemplate pt = new PodTemplate();
        pt.setAffinity(affinity);

        KafkaClusterTemplate kct = new KafkaClusterTemplateBuilder()
                .withPod(pt)
                .build();

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewTemplate()
                        .withPod(pt)
                    .endTemplate()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithoutWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withTemplate(kct)
                .endKafka()
            .endSpec()
            .build());

        // pods are stable in the Pending state
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), testStorage.getBrokerPoolName()), 3);

        LOGGER.info("Removing requirement for the affinity");
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().getTemplate().getPod().setAffinity(null));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka ->
            kafka.getSpec().getKafka().getTemplate().getPod().setAffinity(null));

        // kafka should get back ready in some reasonable time frame
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    /**
     * @description This test case verifies the rolling update behavior of Kafka controller nodes under specific conditions.
     * It focuses on ensuring that changes in Kafka configuration and KafkaNodePool properties affect only the intended KafkaNodePools,
     * particularly the controller nodes, while leaving others like broker nodes unaffected.
     *
     * @steps
     *  1. - Assume that KRaft mode is enabled.
     *  2. - Create and deploy a KafkaNodePool with broker role (brokerPool) and another with controller role (controllerPool), each with 3 replicas.
     *  3. - Take snapshots of the broker and controller pods for later comparison.
     *  4. - Update a specific Kafka configuration that affects only controller nodes and verify the rolling update behavior.
     *     - Ensure that only controller nodes undergo a rolling update, while broker nodes remain unaffected.
     *  5. - Update a specific Kafka configuration that affects only broker nodes and verify the rolling update behavior.
     *     - Ensure that only broker nodes undergo a rolling update, while controller node remain unaffected.
     *  6. - Introduce a change in the controller KafkaNodePool, such as modifying pod affinity.
     *     - Observe and ensure that this change triggers another rolling update for the controller nodes.
     *  7. - Verify the rolling updates of controller nodes by comparing the snapshots taken before and after each configuration change.
     *
     * @usecase
     *  - kafka-controller-node-rolling-update
     *  - kafka-configuration-change-impact
     *  - kafka-node-pool-property-update
     *  - kafka-node-pool-management
     */
    @ParallelNamespaceTest
    void testKafkaRollingUpdatesOfSingleRoleNodePools() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final int brokerPoolReplicas = 3, controllerPoolReplicas = 3;

        resourceManager.createResourceWithoutWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerPoolReplicas).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerPoolReplicas).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );

        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPoolReplicas, true);
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerPoolReplicas, true);

        Map<String, String> brokerPoolPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPoolPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        // change Controller-only configuration inside shared Kafka configuration between KafkaNodePools and see that only controller pods rolls
        KafkaUtils.updateSpecificConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName(), "controller.quorum.election.timeout.ms", 10000);

        // only controller-role nodes rolls
        controllerPoolPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(),
            testStorage.getControllerSelector(), controllerPoolReplicas, controllerPoolPodsSnapshot);

        // broker-role nodes does not roll
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPoolPodsSnapshot);

        // change Broker-only configuration inside shared Kafka configuration between KafkaNodePools and see that only broker pods rolls
        KafkaUtils.updateSpecificConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName(), "initial.broker.registration.timeout.ms", 33500);

        // only broker-role nodes rolls
        brokerPoolPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(),
            testStorage.getBrokerSelector(), brokerPoolReplicas, brokerPoolPodsSnapshot);

        // controller-role nodes does not roll
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerPoolPodsSnapshot);

        // 2nd Rolling update triggered by PodAffinity

        // Modify pod affinity settings for the controller KafkaNodePool
        // Pod Affinity is expecting a running pod on a node with topologyKey with labels specify by LabelSelector
        PodAffinity podAffinity = new PodAffinityBuilder()
            .addNewRequiredDuringSchedulingIgnoredDuringExecution()
                .withLabelSelector(new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND).build())
                .withTopologyKey("kubernetes.io/hostname")
            .endRequiredDuringSchedulingIgnoredDuringExecution()
            .build();

        Affinity affinity = new AffinityBuilder()
            .withPodAffinity(podAffinity)
            .build();

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getControllerPoolName(),
            controllerNodePool -> controllerNodePool.setSpec(new KafkaNodePoolSpecBuilder(controllerNodePool.getSpec())
                .editOrNewTemplate()
                    .editOrNewPod()
                        .withAffinity(affinity)
                    .endPod()
                .endTemplate()
                .build())
        );

        // Expect a rolling update on the controller nodes due to the affinity change
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(),
            testStorage.getControllerSelector(), controllerPoolReplicas, controllerPoolPodsSnapshot);

        // Verify that broker nodes do not roll due to the controller KafkaNodePool affinity change
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPoolPodsSnapshot);
    }

    /**
     * @description This test case assesses the rolling update behavior of Kafka nodes in mixed-role configurations.
     * The main focus is to verify that configuration changes impacting controller roles result in rolling updates
     * of nodes serving mixed roles, while ensuring compatibility with KRaft mode and excluding OLM or Helm installations.
     *
     * @steps
     *  1. - Ensure that the environment is running in KRaft mode.
     *  2. - Create and deploy a KafkaNodePool with mixed roles (controller and broker), consisting of 6 replicas.
     *  3. - Take a snapshot of the mixed-role pods for comparison before and after the configuration change.
     *  4. - Update a specific Kafka configuration targeting controller roles.
     *  5. - Observe and verify that all mixed-role nodes undergo a rolling update in response to the configuration change.
     *  6. - Confirm the successful rolling update by comparing pod snapshots taken before and after the configuration change.
     *
     * @usecase
     *  - kafka-mixed-node-rolling-update
     *  - kafka-configuration-change-impact-on-mixed-nodes
     *  - kafka-node-pool-management-in-non-KRaft-mode
     */
    @ParallelNamespaceTest
    void testKafkaRollingUpdatesOfMixedNodes() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int mixedPoolReplicas = 6;

        resourceManager.createResourceWithoutWait(
            KafkaNodePoolTemplates.mixedPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), testStorage.getClusterName(), mixedPoolReplicas).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build()
        );

        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), testStorage.getMixedSelector(), mixedPoolReplicas, true);

        Map<String, String> mixedPoolPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMixedSelector());

        // change Controller-only configuration inside shared Kafka configuration between KafkaNodePools and see that all mixed pods rolls
        KafkaUtils.updateSpecificConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName(), "controller.quorum.fetch.timeout.ms", 10000);

        // all mixed nodes rolls
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(),
            testStorage.getMixedSelector(), mixedPoolReplicas, mixedPoolPodsSnapshot);
    }

    boolean checkIfExactlyOneKafkaPodIsNotReady(String namespaceName, String clusterName) {
        List<Pod> kafkaPods = kubeClient().listPods(namespaceName, KafkaResource.getLabelSelectorForAllKafkaPods(clusterName));
        int runningKafkaPods = (int) kafkaPods.stream().filter(pod -> pod.getStatus().getPhase().equals("Running")).count();

        return runningKafkaPods == (kafkaPods.size() - 1);
    }

    @ParallelNamespaceTest
    void testKafkaRollingUpdatesWithDedicatedControllers() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int brokerNodes = 3;
        final int controllerNodes = 3;

        LOGGER.info("Deploying KRaft cluster with dedicated controllers ({} replicas) and brokers ({} replicas).", controllerNodes, brokerNodes);

        // Create dedicated controller and broker KafkaNodePools and Kafka CR
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(),
                testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerNodes).build(),
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(),
                testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerNodes).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerNodes).build()
        );

        Map<String, String> controllerPoolPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPoolPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerPoolSelector());

        LOGGER.info("Modifying Kafka CR to enable auto.create.topics.enable=false, expecting rolling update of all nodes including controllers.");

        KafkaUtils.updateSpecificConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName(), "auto.create.topics.enable", "false");

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerNodes, controllerPoolPodsSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerPoolSelector(), brokerNodes, brokerPoolPodsSnapshot);

        // The cluster should remain Ready and CO should not be stuck with TimeoutException
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator.defaultInstallation()
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
