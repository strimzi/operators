/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KRaftMetadataStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(ROLLING_UPDATE)
class AlternativeReconcileTriggersST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(AlternativeReconcileTriggersST.class);

    @ParallelNamespaceTest
    @SuppressWarnings("checkstyle:MethodLength")
    void testManualTriggeringRollingUpdate() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // 500 messages will take 500 seconds in that case
        final int continuousClientsMessageCount = 500;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getContinuousTopicName(), testStorage.getClusterName(), 3, 3, 2).build());

        String producerAdditionConfiguration = "delivery.timeout.ms=40000\nrequest.timeout.ms=5000";

        // Add transactional id to make producer transactional
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\ntransactional.id=" + testStorage.getContinuousTopicName() + ".1");
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\nenable.idempotence=true");

        final KafkaClients continuousClients = ClientUtils.getContinuousPlainClientBuilder(testStorage)
            .withMessageCount(continuousClientsMessageCount)
            .withAdditionalConfig(producerAdditionConfiguration)
            // TODO: needed because of bug in test-clients - https://github.com/strimzi/test-clients/issues/119
            .withMessagesPerTransaction("1")
            .build();

        resourceManager.createResourceWithWait(continuousClients.producerStrimzi(), continuousClients.consumerStrimzi());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        KafkaClients instantClients = ClientUtils.getInstantTlsClientBuilder(testStorage).build();
        resourceManager.createResourceWithWait(instantClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        // rolling update for kafka
        // set annotation to trigger Kafka rolling update
        LOGGER.info("Annotate Kafka {} {} with manual rolling update annotation", StrimziPodSet.RESOURCE_KIND, testStorage.getBrokerComponentName());

        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName())
            .get(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE)), is(true));

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // wait when annotation will be removed from kafka
        TestUtils.waitFor("CO removes rolling update annotation", TestConstants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName()) == null
                        || !StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName()).containsKey(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE));

        resourceManager.createResourceWithWait(instantClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // rolling update for controller pods
        // set annotation to trigger controller rolling update
        LOGGER.info("Annotate controller: {} - {}/{} with manual rolling update annotation", StrimziPodSet.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getControllerComponentName());

        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName())
                .get(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE)), is(true));

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);

        // wait when annotation will be removed
        TestUtils.waitFor("CO removes rolling update annotation", TestConstants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName()) == null
                        || !StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName()).containsKey(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE));

        instantClients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(instantClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that controller is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), newTopicName, testStorage.getClusterName(), 1, 1).build());

        instantClients = new KafkaClientsBuilder(instantClients)
            .withTopicName(newTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(
            instantClients.producerTlsStrimzi(testStorage.getClusterName()),
            instantClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        ClientUtils.waitForContinuousClientSuccess(testStorage, continuousClientsMessageCount);
    }

    // This test is affected by https://github.com/strimzi/strimzi-kafka-operator/issues/3913 so it needs longer operation timeout set in CO

    /**
     * Test for checking that overriding of bootstrap server, triggers the rolling update and verifying that,
     * new bootstrap DNS is appended inside certificate in subject alternative names property.
     */
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testTriggerRollingUpdateAfterOverrideBootstrap() throws CertificateException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String bootstrapDns = "kafka-test.XXXX.azure.XXXX.net";

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            LOGGER.info("Adding new bootstrap dns: {} to external listeners", bootstrapDns);
            kafka.getSpec().getKafka()
                .setListeners(asList(
                    new GenericKafkaListenerBuilder()
                        .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withNewConfiguration()
                            .withNewBootstrap()
                                .withAlternativeNames(bootstrapDns)
                            .endBootstrap()
                        .endConfiguration()
                        .build()
                ));
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        for (String podName : brokerPods.keySet()) {
            Map<String, String> secretData = kubeClient().getSecret(testStorage.getNamespaceName(), podName).getData();
            String certKey = podName + ".crt";
            LOGGER.info("Encoding {} cert", certKey);
            ByteArrayInputStream publicCert = new ByteArrayInputStream(Util.decodeBytesFromBase64(secretData.get(certKey).getBytes()));
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Certificate certificate = certificateFactory.generateCertificate(publicCert);

            LOGGER.info("Verifying that new DNS is in certificate subject alternative names");
            assertThat(certificate.toString(), containsString(bootstrapDns));
        }
    }

    @ParallelNamespaceTest
    void testManualRollingUpdateForSinglePod() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Pod brokerPod = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0);
        String brokerPodName = brokerPod.getMetadata().getName();

        Pod controllerPod = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0);
        String controllerPodName = controllerPod.getMetadata().getName();

        // snapshot of one single Kafka pod
        Map<String, String> brokerSnapshot = Collections.singletonMap(brokerPodName, brokerPod.getMetadata().getUid());
        Map<String, String> controllerSnapshot = Collections.singletonMap(controllerPodName, controllerPod.getMetadata().getUid());

        LOGGER.info("Trying to roll just single broker and single controller Pod");
        kubeClient(testStorage.getNamespaceName()).editPod(brokerPodName).edit(pod -> new PodBuilder(pod)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

        // here we are waiting just to one pod's snapshot will be changed and all 3 pods ready -> if we set expectedPods to 1,
        // the check will pass immediately without waiting for all pods to be ready -> the method picks first ready pod and return true
        brokerSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerSnapshot);

        kubeClient(testStorage.getNamespaceName()).editPod(controllerPodName).edit(pod -> new PodBuilder(pod)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

        // same as above
        controllerSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerSnapshot);

        LOGGER.info("Adding anno to all broker and controller Pods");
        brokerSnapshot.keySet().forEach(podName -> {
            kubeClient(testStorage.getNamespaceName()).editPod(podName).edit(pod -> new PodBuilder(pod)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .build());
        });

        LOGGER.info("Checking if the rolling update will be successful for brokers");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerSnapshot);

        controllerSnapshot.keySet().forEach(podName -> {
            kubeClient(testStorage.getNamespaceName()).editPod(podName).edit(pod -> new PodBuilder(pod)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .build());
        });

        LOGGER.info("Checking if the rolling update will be successful for controllers");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerSnapshot);
    }

    /**
     * @description This tests tries to add and remove volume from JBOD.
     *
     * @steps
     *  1. - Persistent Kafka with Jbod storage and 3 replicas is deployed
     *     - Kafka is ready
     *  2. - KafkaTopic for continuous communication including additional properties e.g., 3 replicas and 2 min in sync replicas, is deployed
     *     - KafkaTopic is ready
     *  3. - KafkaTopic for temporary data production and consumption is created as well
     *     - KafkaTopic is ready
     *  4. - Deploy Kafka clients targeting respective KafkaTopics
     *     - Clients are running
     *  5. - Add Jbod volume to Kafka
     *     - Rolling Update on Kafka brokers is triggered and persistent volumes are claimed
     *  5. - Add another Jbod volume to Kafka
     *     - Rolling Update on Kafka brokers is triggered and persistent volumes are claimed, now 2 volumes per each Kafka Broker
     *
     * @usecase
     *  - rolling-update
     *  - jbod
     *  - persistent-volumes
     *  - persistent-volume-claims
     */
    @ParallelNamespaceTest
    @SuppressWarnings("deprecation") // Storage is deprecated, but some API methods are still called here
    void testAddingAndRemovingJbodVolumes() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // 500 messages will take 500 seconds in that case
        final int continuousClientsMessageCount = 500;

        PersistentClaimStorage vol0 = new PersistentClaimStorageBuilder().withId(0).withSize("1Gi").withDeleteClaim(true).build();
        PersistentClaimStorage vol1 = new PersistentClaimStorageBuilder().withId(1).withSize("1Gi").withDeleteClaim(true).build();

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withStorage(new JbodStorageBuilder().addToVolumes(vol0).build())
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withStorage(new JbodStorageBuilder().addToVolumes(vol0).build())
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getContinuousTopicName(), testStorage.getClusterName(), 3, 3, 2).build());

        String producerAdditionConfiguration = "delivery.timeout.ms=40000\nrequest.timeout.ms=5000";
        // Add transactional id to make producer transactional
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\ntransactional.id=" + testStorage.getContinuousTopicName() + ".1");
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\nenable.idempotence=true");

        KafkaClients kafkaBasicClientJob = ClientUtils.getContinuousPlainClientBuilder(testStorage)
            .withMessageCount(continuousClientsMessageCount)
            .withAdditionalConfig(producerAdditionConfiguration)
            // TODO: needed because of bug in test-clients - https://github.com/strimzi/test-clients/issues/119
            .withMessagesPerTransaction("1")
            .build();

        resourceManager.createResourceWithWait(kafkaBasicClientJob.producerStrimzi(), kafkaBasicClientJob.consumerStrimzi());

        // ##############################
        KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage).build();
        resourceManager.createResourceWithWait(clients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        // Add Jbod volume to Kafka => triggers RU
        LOGGER.info("Add JBOD volume to the Kafka cluster {}", testStorage.getBrokerComponentName());

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), kafkaNodePool -> {
            JbodStorage storage = (JbodStorage) kafkaNodePool.getSpec().getStorage();
            storage.getVolumes().add(vol1);
        });

        // Wait util it rolls
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        StUtils.waitUntilSupplierIsSatisfied("there are 6 Kafka volumes", () -> {
            // ensure there are 6 Kafka Volumes (2 per each of 3 broker)
            List<PersistentVolume> kafkaPvcs = kubeClient().listClaimedPersistentVolumes(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                    .filter(pv -> pv.getSpec().getClaimRef().getName().contains(testStorage.getBrokerComponentName())).toList();
            LOGGER.debug("Obtained Volumes total '{}' claimed by claims Belonging to Kafka {}", kafkaPvcs.size(), testStorage.getClusterName());
            return kafkaPvcs.size() == 6;
        });

        // Remove Jbod volume to Kafka => triggers RU
        LOGGER.info("Remove JBOD volume to the Kafka cluster {}", testStorage.getBrokerComponentName());

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), kafkaNodePool -> {
            JbodStorage storage = (JbodStorage) kafkaNodePool.getSpec().getStorage();
            storage.getVolumes().remove(vol1);
        });

        // Wait util it rolls
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // ensure there are 3 Kafka Volumes (1 per each of 3 broker)
        PersistentVolumeClaimUtils.waitForPvcCount(testStorage, 3);
        StUtils.waitUntilSupplierIsSatisfied("there are 3 Kafka volumes", () -> {
            List<PersistentVolume> kafkaPvcs = kubeClient().listClaimedPersistentVolumes(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                    .filter(pv -> pv.getSpec().getClaimRef().getName().contains(testStorage.getBrokerComponentName()) && pv.getStatus().getPhase().equals("Bound")).toList();
            LOGGER.debug("Obtained Volumes total '{}' claimed by claims Belonging to Kafka {}", kafkaPvcs.size(), testStorage.getClusterName());
            return kafkaPvcs.size() == 3;
        });


        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForContinuousClientSuccess(testStorage, continuousClientsMessageCount);
        // ##############################
    }

    /**
     * @description Tests the resilience and relocation of KRaft metadata logs in a Kafka cluster configured with JBOD storage.
     * This test verifies that Kafka can handle the simulated disk failure of a KRaft metadata volume and successfully
     * relocate the metadata log to another volume in the JBOD setup. The test ensures that the cluster remains operational
     * and that metadata is correctly managed even after a significant storage disruption.
     * Note: This test case runs only in KRaft mode.
     *
     * @steps
     * 1. Deploy a Kafka cluster with multiple JBOD volumes, specifically designating one volume for KRaft metadata.
     *    - Ensure the cluster is operational and the metadata volume is correctly used.
     * 2. Attach Kafka clients to continuously produce and consume messages, simulating normal cluster activity.
     *    - Verify consistent message traffic to ensure cluster stability.
     * 3. Simulate a disk failure by removing the KRaft metadata volume from the cluster configuration.
     *    - Trigger a rolling update and ensure the cluster starts relocating the metadata to a new volume.
     * 4. Verify that the KRaft metadata log has been successfully reassigned to a new volume with the lowest available ID.
     *    - Check the reassignment and integrity of the metadata.
     * 5. Confirm that Kafka continues to function correctly, with ongoing message production and consumption unaffected.
     *    - Ensure no data loss or interruption in service.
     *
     * @usecase
     * - jbod
     * - rolling update
     * - persistent-volumes
     * - persistent-volume-claims
     * - storage failure
     * - KRaft metadata log
     */
    @ParallelNamespaceTest
    @SuppressWarnings("deprecation") // Storage is deprecated, but some API methods are still called here
    void testJbodMetadataLogRelocation() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int numberOfKafkaReplicas = 3;

        // Setup JBOD with multiple volumes, ensuring metadata is on a non-lowest ID volume
        PersistentClaimStorage vol = new PersistentClaimStorageBuilder().withId(0).withSize("1Gi").withDeleteClaim(true).build();
        PersistentClaimStorage otherVol = new PersistentClaimStorageBuilder().withId(1).withSize("1Gi").withDeleteClaim(true).build();
        // volume with id=2 will be using for KRaft metadata storage
        PersistentClaimStorage metadataVol = new PersistentClaimStorageBuilder().withId(2).withSize("1Gi").withDeleteClaim(false)
            .withKraftMetadata(KRaftMetadataStorage.SHARED).build();

        // 300 messages will take 300 seconds in that case
        final int continuousClientsMessageCount = 300;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), numberOfKafkaReplicas)
                .editSpec()
                    .withStorage(
                        new JbodStorageBuilder()
                            .addToVolumes(vol, otherVol, metadataVol)
                            .build())
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), numberOfKafkaReplicas)
            .editSpec()
                .editKafka()
                    .withStorage(
                        new JbodStorageBuilder()
                            .addToVolumes(vol, otherVol, metadataVol)
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getContinuousTopicName(), testStorage.getClusterName(), 3, 3, 2).build());

        String producerAdditionConfiguration = "delivery.timeout.ms=40000\nrequest.timeout.ms=5000";
        // Add transactional id to make producer transactional
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\ntransactional.id=" + testStorage.getContinuousTopicName() + ".1");
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\nenable.idempotence=true");

        KafkaClients kafkaBasicClientJob = ClientUtils.getContinuousPlainClientBuilder(testStorage)
            .withMessageCount(continuousClientsMessageCount)
            .withAdditionalConfig(producerAdditionConfiguration)
            // TODO: needed because of bug in test-clients - https://github.com/strimzi/test-clients/issues/119
            .withMessagesPerTransaction("1")
            .build();

        resourceManager.createResourceWithWait(kafkaBasicClientJob.producerStrimzi(), kafkaBasicClientJob.consumerStrimzi());

        // ##############################
        KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage).build();
        resourceManager.createResourceWithWait(clients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KafkaUtils.verifyKafkaKraftMetadataLog(testStorage, 2, 3);

        // Remove Jbod KRaft volume to Kafka => triggers RU
        LOGGER.info("Remove JBOD volume (i.e., simulating disk failure for KRaft metadata volume) to the Kafka cluster {}", testStorage.getBrokerComponentName());

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), kafkaNodePool -> {
            JbodStorage storage = (JbodStorage) kafkaNodePool.getSpec().getStorage();
            storage.getVolumes().remove(metadataVol);
        });

        // Wait util it rolls
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // verify that Kraft metadata log will be re-assigned to another volume (the minimum id, which is 0 now that's why data-0)
        KafkaUtils.verifyKafkaKraftMetadataLog(testStorage, 0, 2);

        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForContinuousClientSuccess(testStorage, continuousClientsMessageCount);
        // ##############################
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}

