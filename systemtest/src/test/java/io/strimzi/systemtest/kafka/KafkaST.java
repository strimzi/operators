/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Container;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@Tag(REGRESSION)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
class KafkaST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);
    private static final String OPENSHIFT_CLUSTER_NAME = "openshift-my-cluster";


    /**
     * @description This test case verifies that Pod's resources (limits and requests), custom JVM configurations, and expected Java configuration
     * are propagated correctly to Pods, containers, and processes.
     *
     * @steps
     *  1. - Deploy Kafka and its components with custom specifications, including specifying resources and JVM configuration
     *     - Kafka and its components (ZooKeeper, Entity Operator) are deployed
     *  2. - For each of components (Kafka, ZooKeeper, Topic Operator, User Operator), verify specified configuration of JVM, resources, and also environment variables.
     *     - Each of the components has requests and limits assigned correctly, JVM, and environment variables configured according to the specification.
     *  3. - Wait for a time to observe that none of initiated components needed Rolling Update.
     *     - All of Kafka components remained in stable state.
     *
     * @usecase
     *  - JVM
     *  - configuration
     *  - resources
     *  - environment variables
     */
    @ParallelNamespaceTest
    @KRaftNotSupported("Entity Operator is not supported by KRaft mode and is used in this test class")
    void testJvmAndResources(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        ArrayList<SystemProperty> javaSystemProps = new ArrayList<>();
        javaSystemProps.add(new SystemPropertyBuilder().withName("javax.net.debug")
                .withValue("verbose").build());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1.5Gi"))
                            .addToLimits("cpu", new Quantity("1"))
                            .addToRequests("memory", new Quantity("1Gi"))
                            .addToRequests("cpu", new Quantity("50m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1g")
                        .withXms("512m")
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withResources(
                        new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1G"))
                            .addToLimits("cpu", new Quantity("0.5"))
                            .addToRequests("memory", new Quantity("0.5G"))
                            .addToRequests("cpu", new Quantity("25m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1G")
                        .withXms("512M")
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endZookeeper()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                        .withResources(
                            new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("1024Mi"))
                                .addToLimits("cpu", new Quantity("500m"))
                                .addToRequests("memory", new Quantity("384Mi"))
                                .addToRequests("cpu", new Quantity("0.025"))
                                .build())
                        .withNewJvmOptions()
                            .withXmx("2G")
                            .withXms("1024M")
                            .withJavaSystemProperties(javaSystemProps)
                        .endJvmOptions()
                    .endTopicOperator()
                    .withNewUserOperator()
                        .withResources(
                            new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("512M"))
                                .addToLimits("cpu", new Quantity("300m"))
                                .addToRequests("memory", new Quantity("256M"))
                                .addToRequests("cpu", new Quantity("30m"))
                                .build())
                        .withNewJvmOptions()
                            .withXmx("1G")
                            .withXms("512M")
                            .withJavaSystemProperties(javaSystemProps)
                        .endJvmOptions()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        // Make snapshots for Kafka cluster to make sure that there is no rolling update after CO reconciliation
        final String eoDepName = KafkaResources.entityOperatorDeploymentName(clusterName);
        final Map<String, String> zkPods = PodUtils.podSnapshot(namespaceName, zkSelector);
        final Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        final Map<String, String> eoPods = DeploymentUtils.depSnapshot(namespaceName, eoDepName);

        LOGGER.info("Verifying resources and JVM configuration of Kafka Broker Pod");
        assertResources(namespaceName, KafkaResource.getKafkaPodName(clusterName, 0), "kafka",
                "1536Mi", "1", "1Gi", "50m");
        assertExpectedJavaOpts(namespaceName, KafkaResource.getKafkaPodName(clusterName, 0), "kafka",
                "-Xmx1g", "-Xms512m", "-XX:+UseG1GC");

        LOGGER.info("Verifying resources and JVM configuration of ZooKeeper Broker Pod");
        assertResources(namespaceName, KafkaResources.zookeeperPodName(clusterName, 0), "zookeeper",
                "1G", "500m", "500M", "25m");
        assertExpectedJavaOpts(namespaceName, KafkaResources.zookeeperPodName(clusterName, 0), "zookeeper",
                "-Xmx1G", "-Xms512M", "-XX:+UseG1GC");

        LOGGER.info("Verifying resources, JVM configuration, and environment variables of Entity Operator's components");

        Optional<Pod> pod = kubeClient(namespaceName).listPods(namespaceName)
                .stream().filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(clusterName)))
                .findFirst();
        assertThat("EO Pod does not exist", pod.isPresent(), is(true));

        assertResources(namespaceName, pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "384Mi", "25m");
        assertResources(namespaceName, pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "30m");
        assertExpectedJavaOpts(namespaceName, pod.get().getMetadata().getName(), "topic-operator",
                "-Xmx2G", "-Xms1024M", null);
        assertExpectedJavaOpts(namespaceName, pod.get().getMetadata().getName(), "user-operator",
                "-Xmx1G", "-Xms512M", null);

        String eoPod = eoPods.keySet().toArray()[0].toString();
        kubeClient(namespaceName).getPod(namespaceName, eoPod).getSpec().getContainers().forEach(container -> {
            if (!container.getName().equals("tls-sidecar")) {
                LOGGER.info("Check if -D java options are present in {}", container.getName());

                String javaSystemProp = container.getEnv().stream().filter(envVar ->
                    envVar.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findFirst().orElseThrow().getValue();
                String javaOpts = container.getEnv().stream().filter(envVar ->
                    envVar.getName().equals("STRIMZI_JAVA_OPTS")).findFirst().orElseThrow().getValue();

                assertThat(javaSystemProp, is("-Djavax.net.debug=verbose"));

                if (container.getName().equals("topic-operator")) {
                    assertThat(javaOpts, is("-Xms1024M -Xmx2G"));
                }

                if (container.getName().equals("user-operator")) {
                    assertThat(javaOpts, is("-Xms512M -Xmx1G"));
                }
            }
        });

        LOGGER.info("Checking no rolling update for Kafka cluster");
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, zkSelector, zkPods);
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaPods);
        DeploymentUtils.waitForNoRollingUpdate(namespaceName, eoDepName, eoPods);
    }

    /**
     * @description This test case verifies the correct deployment of Entity Operator, i.e., including both User Operator and Topic Operator.
     * Entity Operator is firstly modified to exclude User Operator, afterwards it is modified to default configuration, which includes User Operator.
     * The next step is removal of Topic Operator itself and finally, also removing User Operator, with Topic Operator being already removed.
     *
     * @steps
     *  1. - Deploy Kafka with Entity Operator set.
     *     - Kafka is deployed, and Entity Operator consist of both Topic and User Operators
     *  2. - Remove User Operator from the Kafka specification
     *     - User Operator container is deleted
     *  3. - Set User Operator back in the Kafka specification
     *     - User Operator container is recreated
     *  4. - Remove Topic Operator from the Kafka specification
     *     - Topic Operator container is removed Entity Operator
     *  5. - Remove User Operator from the Kafka specification
     *     - Entity Operator Pod is removed, as there are no other containers present.
     *
     * @usecase
     *  - Entity Operator
     *  - Topic Operator
     *  - User Operator
     */
    @ParallelNamespaceTest
    void testRemoveComponentsFromEntityOperator(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying Kafka cluster {}", clusterName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        LOGGER.info("Remove User Operator from Entity Operator");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setUserOperator(null), namespaceName);

        if (!Environment.isKRaftModeEnabled()) {
            //Waiting when EO pod will be recreated without UO
            DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
            PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 2);

            //Checking that UO was removed
            kubeClient().listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
                pod.getSpec().getContainers().forEach(container -> {
                    assertThat(container.getName(), not(containsString("user-operator")));
                });
            });
        } else {
            DeploymentUtils.waitForDeploymentDeletion(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
        }

        LOGGER.info("Recreate User Operator");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setUserOperator(new EntityUserOperatorSpec()), namespaceName);
        //Waiting when EO pod will be recreated with UO
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

        int expectedEOContainerCount = Environment.isKRaftModeEnabled() ? 1 : 3;
        PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), expectedEOContainerCount);

        LOGGER.info("Verifying that Entity Operator and all its component are correctly recreated");
        // names of containers present in EO pod
        List<String> entityOperatorContainerNames = kubeClient().listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName))
                .get(0).getSpec().getContainers()
                .stream()
                .map(Container::getName)
                .toList();

        assertThat("user-operator container is not present in EO", entityOperatorContainerNames.stream().anyMatch(name -> name.contains("user-operator")));

        // kraft does not support Topic Operator, therefore removal and recreation of User Operator is all to be tested with kraft enabled, rest of test is without kraft
        if (!Environment.isKRaftModeEnabled()) {
            assertThat("tls-sidecar container is not present in EO", entityOperatorContainerNames.stream().anyMatch(name -> name.contains("tls-sidecar")));
            assertThat("topic-operator container is not present in EO", entityOperatorContainerNames.stream().anyMatch(name -> name.contains("topic-operator")));

            LOGGER.info("Remove Topic Operator from Entity Operator");
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setTopicOperator(null), namespaceName);
            DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
            PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

            //Checking that TO was removed
            LOGGER.info("Verifying that Topic Operator container is no longer present in Entity Operator Pod");
            kubeClient().listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
                pod.getSpec().getContainers().forEach(container -> {
                    assertThat(container.getName(), not(containsString("topic-operator")));
                });
            });

            LOGGER.info("Remove User Operator, after removed Topic Operator");
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
                k.getSpec().getEntityOperator().setUserOperator(null);
            }, namespaceName);

            // both TO and UO are unset, which means EO should not be deployed
            LOGGER.info("Waiting for deletion of Entity Operator Pod");
            PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 0);
        }
    }

    /**
     * @description This test case verifies that Kafka with persistent storage, and JBOD storage, property 'delete claim' of JBOD storage.
     *
     * @steps
     *  1. - Deploy Kafka with persistent storage and JBOD storage with 2 volumes, both of these are configured to delete their Persistent Volume Claims on Kafka cluster un-provision.
     *     - Kafka is deployed, volumes are labeled and linked to Pods correctly.
     *  2. - Verify that labels in Persistent Volume Claims are set correctly.
     *     - Persistent Volume Claims do contain expected labels and values.
     *  2. - Modify Kafka Custom Resource, specifically 'delete claim' property of its first Kafka Volume.
     *     - Kafka CR is successfully modified, annotation of according Persistent Volume Claim is changed afterwards by Cluster Operator.
     *  3. - Delete Kafka cluster.
     *     - Kafka cluster and its components are deleted, including Persistent Volume Claim of Volume with 'delete claim' property set to true.
     *  4. - Verify remaining Persistent Volume Claims.
     *     - Persistent Volume Claim referenced by volume of formerly deleted Kafka Custom Resource with property 'delete claim' set to true is still present.
     *
     * @usecase
     *  - JBOD
     *  - PVC
     *  - volume
     *  - annotations
     */
    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testKafkaJBODDeleteClaimsTrueFalse(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());
        final int kafkaReplicas = 2;
        final String diskSizeGi = "10";

        //Volume Storages (original and modified)
        PersistentClaimStorage idZeroVolumeOriginal = new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSizeGi + "Gi").build();
        PersistentClaimStorage idOneVolumeOriginal = new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSizeGi + "Gi").build();
        PersistentClaimStorage idZeroVolumeModified = new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizeGi + "Gi").build();

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(idZeroVolumeOriginal, idOneVolumeOriginal).build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaJBOD(testStorage.getClusterName(), kafkaReplicas, jbodStorage).build());
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getKafkaStatefulSetName(), kafkaReplicas, 2, diskSizeGi);

        //change value of first PVC to delete its claim once Kafka is deleted.
        LOGGER.info("Update Volume with id=0 in Kafka CR by setting 'Delete Claim' property to false");

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), resource -> {
                LOGGER.debug(resource.getMetadata().getName());
                JbodStorage jBODVolumeStorage = (JbodStorage) resource.getSpec().getStorage();
                jBODVolumeStorage.setVolumes(List.of(idZeroVolumeModified, idOneVolumeOriginal));
            }, testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), resource -> {
                LOGGER.debug(resource.getMetadata().getName());
                JbodStorage jBODVolumeStorage = (JbodStorage) resource.getSpec().getKafka().getStorage();
                jBODVolumeStorage.setVolumes(List.of(idZeroVolumeModified, idOneVolumeOriginal));
            }, testStorage.getNamespaceName());
        }

        TestUtils.waitFor("PVC(s)' annotation to change according to Kafka JBOD storage 'delete claim'", Constants.GLOBAL_POLL_INTERVAL, Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                .filter(pvc -> pvc.getMetadata().getName().startsWith("data-0") && pvc.getMetadata().getName().contains("-kafka"))
                .allMatch(volume -> "false".equals(volume.getMetadata().getAnnotations().get("strimzi.io/delete-claim")))
        );

        final int volumesCount = kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).size();

        LOGGER.info("Deleting Kafka: {}/{} cluster", testStorage.getNamespaceName(), testStorage.getClusterName());
        resourceManager.deleteResource();
        cmdKubeClient(testStorage.getNamespaceName()).deleteByName("kafka", testStorage.getClusterName());
        if (Environment.isKafkaNodePoolsEnabled()) {
            cmdKubeClient(testStorage.getNamespaceName()).deleteByName("kafkanodepool", testStorage.getKafkaNodePoolName());
        }

        LOGGER.info("Waiting for PVCs deletion");
        PersistentVolumeClaimUtils.waitForJbodStorageDeletion(testStorage.getNamespaceName(), volumesCount, testStorage.getClusterName(), List.of(idZeroVolumeModified, idOneVolumeOriginal));

        LOGGER.info("Verifying that PVC which are supposed to remain, really persist even after Kafka cluster un-deployment");
        List<String> remainingPVCNames =  kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().map(e -> e.getMetadata().getName()).toList();
        assertThat("Kafka Broker with id 0 does not preserve its JBOD storage's PVC", remainingPVCNames.stream().anyMatch(e -> e.equals("data-0-" + testStorage.getKafkaStatefulSetName() + "-0")));
        assertThat("Kafka Broker with id 1 does not preserve its JBOD storage's PVC", remainingPVCNames.stream().anyMatch(e -> e.equals("data-0-" + testStorage.getKafkaStatefulSetName() + "-1")));
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    void testRegenerateCertExternalAddressChange(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        LOGGER.info("Creating Kafka without external listener");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1).build());

        final String brokerSecret = clusterName + "-kafka-brokers";

        Secret secretsWithoutExt = kubeClient(namespaceName).getSecret(namespaceName, brokerSecret);

        LOGGER.info("Editing Kafka with external listener");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            List<GenericKafkaListener> lst = asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                            .build()
            );
            kafka.getSpec().getKafka().setListeners(lst);
        }, namespaceName);

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, PodUtils.podSnapshot(namespaceName, kafkaSelector));

        Secret secretsWithExt = kubeClient(namespaceName).getSecret(namespaceName, brokerSecret);

        LOGGER.info("Checking Secrets");
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName)).forEach(kafkaPod -> {
            String kafkaPodName = kafkaPod.getMetadata().getName();
            assertThat(secretsWithExt.getData().get(kafkaPodName + ".crt"), is(not(secretsWithoutExt.getData().get(kafkaPodName + ".crt"))));
            assertThat(secretsWithExt.getData().get(kafkaPodName + ".key"), is(not(secretsWithoutExt.getData().get(kafkaPodName + ".key"))));
        });
    }

    /**
     * @description This test case verifies the presence of expected Strimzi specific labels, also labels and annotations specified by user.
     * Some of user-specified labels are later modified (new one is added, one is modified) which triggers rolling update after which
     * all changes took place as expected.
     *
     * @steps
     *  1. - Deploy Kafka with persistent storage and specify custom labels in CR metadata, and also other labels and annotation in PVC metadata
     *     - Kafka is deployed with its default labels and all others specified by user.
     *  2. - Deploy Producer and Consumer configured to produce and consume default number of messages, to make sure Kafka works as expected
     *     - Producer and Consumer are able to produce and consume messages respectively.
     *  3. - Modify configuration of Kafka CR with addition of new labels and modification of existing
     *     - Kafka is rolling and new labels are present in Kafka CR, and managed resources
     *  4. - Deploy Producer and Consumer configured to produce and consume default number of messages, to make sure Kafka works as expected
     *     - Producer and Consumer are able to produce and consume messages respectively.
     *
     * @usecase
     *  - annotations
     *  - labels
     *  - kafka-rolling-update
     *  - persistent-storage
     */
    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    @SuppressWarnings({"checkstyle:JavaNCSS", "checkstyle:NPathComplexity", "checkstyle:MethodLength"})
    @Tag(INTERNAL_CLIENTS_USED)
    void testLabelsExistenceAndManipulation(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final int kafkaReplicas = 3;

        // label key and values to be used as part of kafka CR
        final String firstKafkaLabelKey = "first-kafka-label-key";
        final String firstKafkaLabelValue = "first-kafka-label-value";
        final String secondKafkaLabelKey = "second-kafka-label-key";
        final String secondKafkaLabelValue = "second-kafka-label-value";
        final Map<String, String> customSpecifiedLabels = new HashMap<>();
        customSpecifiedLabels.put(firstKafkaLabelKey, firstKafkaLabelValue);
        customSpecifiedLabels.put(secondKafkaLabelKey, secondKafkaLabelValue);

        // label key and value used in addition for while creating kafka CR (as part of PVCs label and annotation)
        final String pvcLabelOrAnnotationKey = "pvc-label-annotation-key";
        final String pvcLabelOrAnnotationValue = "pvc-label-annotation-value";
        final Map<String, String> customSpecifiedLabelOrAnnotationPvc = new HashMap<>();
        customSpecifiedLabelOrAnnotationPvc.put(pvcLabelOrAnnotationKey, pvcLabelOrAnnotationValue);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withLabels(customSpecifiedLabels)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewTemplate()
                        .withNewPersistentVolumeClaim()
                            .withNewMetadata()
                                .addToLabels(customSpecifiedLabelOrAnnotationPvc)
                                .addToAnnotations(customSpecifiedLabelOrAnnotationPvc)
                            .endMetadata()
                        .endPersistentVolumeClaim()
                    .endTemplate()
                    .withStorage(new JbodStorageBuilder().withVolumes(
                            new PersistentClaimStorageBuilder()
                                .withDeleteClaim(false)
                                .withId(0)
                                .withSize("20Gi")
                                .build(),
                            new PersistentClaimStorageBuilder()
                                .withDeleteClaim(true)
                                .withId(1)
                                .withSize("10Gi")
                                .build())
                            .build())
                .endKafka()
                .editZookeeper()
                    .withNewTemplate()
                        .withNewPersistentVolumeClaim()
                            .withNewMetadata()
                                .addToLabels(customSpecifiedLabelOrAnnotationPvc)
                                .addToAnnotations(customSpecifiedLabelOrAnnotationPvc)
                            .endMetadata()
                        .endPersistentVolumeClaim()
                    .endTemplate()
                    .withNewPersistentClaimStorage()
                        .withDeleteClaim(false)
                        .withSize("3Gi")
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        LOGGER.info("--> Test Strimzi related expected labels of managed kubernetes resources <--");

        LOGGER.info("---> PODS <---");

        List<Pod> pods = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getClusterName());

        for (Pod pod : pods) {
            LOGGER.info("Verifying labels of  Pod: {}/{}", pod.getMetadata().getNamespace(), pod.getMetadata().getName());
            verifyAppLabels(pod.getMetadata().getLabels());
        }

        LOGGER.info("---> STRIMZI POD SETS <---");

        Map<String, String> kafkaLabelsObtained = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        LOGGER.info("Verifying labels of StrimziPodSet of Kafka resource");
        verifyAppLabels(kafkaLabelsObtained);

        if (!Environment.isKRaftModeEnabled()) {
            Map<String, String> zooLabels = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());

            LOGGER.info("Verifying labels of StrimziPodSet of ZooKeeper resource");
            verifyAppLabels(zooLabels);
        }

        LOGGER.info("---> SERVICES <---");

        List<Service> services = kubeClient().listServices(testStorage.getNamespaceName()).stream()
            .filter(service -> service.getMetadata().getName().startsWith(testStorage.getClusterName()))
            .collect(Collectors.toList());

        for (Service service : services) {
            LOGGER.info("Verifying labels of Service: {}/{}", service.getMetadata().getNamespace(), service.getMetadata().getName());
            verifyAppLabels(service.getMetadata().getLabels());
        }

        LOGGER.info("---> SECRETS <---");

        List<Secret> secrets = kubeClient().listSecrets(testStorage.getNamespaceName()).stream()
            .filter(secret -> secret.getMetadata().getName().startsWith(testStorage.getClusterName()) && secret.getType().equals("Opaque"))
            .collect(Collectors.toList());

        for (Secret secret : secrets) {
            LOGGER.info("Verifying labels of Secret: {}/{}", secret.getMetadata().getNamespace(), secret.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(secret.getMetadata().getLabels());
        }

        LOGGER.info("---> CONFIG MAPS <---");

        List<ConfigMap> configMaps = kubeClient().listConfigMapsInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName());

        for (ConfigMap configMap : configMaps) {
            LOGGER.info("Verifying labels of ConfigMap: {}/{}", configMap.getMetadata().getNamespace(), configMap.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(configMap.getMetadata().getLabels());
        }

        LOGGER.info("---> PVC (both labels and annotation) <---");

        List<PersistentVolumeClaim> pvcs = kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(testStorage.getClusterName())).collect(Collectors.toList());

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying labels of PVC {}/{}", pvc.getMetadata().getNamespace(), pvc.getMetadata().getName());
            verifyAppLabels(pvc.getMetadata().getLabels());
        }

        LOGGER.info("---> Test Customer specified labels <--");

        LOGGER.info("---> STRIMZI POD SETS <---");

        LOGGER.info("Waiting for Kafka StrimziPodSet  labels existence {}", customSpecifiedLabels);
        StrimziPodSetUtils.waitForStrimziPodSetLabelsChange(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName(), customSpecifiedLabels);

        LOGGER.info("Getting labels from StrimziPodSet set resource");
        kafkaLabelsObtained = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        LOGGER.info("Asserting presence of custom labels which should be available in Kafka with labels {}", kafkaLabelsObtained);
        for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
            String customLabelKey = label.getKey();
            String customLabelValue = label.getValue();
            assertThat("Label exists in StrimziPodSet set with concrete value",
                customLabelValue.equals(kafkaLabelsObtained.get(customLabelKey)));
        }

        LOGGER.info("---> PVC (both labels and annotation) <---");
        for (PersistentVolumeClaim pvc : pvcs) {

            LOGGER.info("Asserting presence of custom label and annotation in PVC {}/{}", pvc.getMetadata().getNamespace(), pvc.getMetadata().getName());
            assertThat(pvc.getMetadata().getLabels().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValue));
            assertThat(pvc.getMetadata().getAnnotations().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValue));
        }

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("--> Test Customer specific labels manipulation (add, update) of Kafka CR and (update) PVC <--");

        LOGGER.info("Take a snapshot of ZooKeeper and Kafka Pods in order to wait for their respawn after rollout");
        Map<String, String> zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getZookeeperSelector());
        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        // key-value pairs modification and addition of user specified labels for kafka CR metadata
        final String firstKafkaLabelValueModified = "first-kafka-label-value-modified";
        final String thirdKafkaLabelKey = "third-kafka-label-key";
        final String thirdKafkaLabelValue = "third-kafka-label-value";
        customSpecifiedLabels.replace(firstKafkaLabelKey, firstKafkaLabelValueModified);
        customSpecifiedLabels.put(thirdKafkaLabelKey, thirdKafkaLabelValue);
        LOGGER.info("New values of labels which are to modify Kafka CR after their replacement and addition of new one are following {}", customSpecifiedLabels);

        // key-value pair modification of user specified label in managed PVCs
        final String pvcLabelOrAnnotationValueModified = "pvc-label-value-modified";
        customSpecifiedLabelOrAnnotationPvc.replace(pvcLabelOrAnnotationKey, pvcLabelOrAnnotationValueModified);
        LOGGER.info("New values of labels which are to modify label and annotation of PVC present in Kafka CR, with following values {}", customSpecifiedLabelOrAnnotationPvc);

        LOGGER.info("Edit Kafka labels in Kafka CR,as well as labels, and annotations of PVCs");
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), resource -> {
                for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
                    resource.getMetadata().getLabels().put(label.getKey(), label.getValue());
                }
                resource.getSpec().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(customSpecifiedLabelOrAnnotationPvc);
                resource.getSpec().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(customSpecifiedLabelOrAnnotationPvc);
            }, testStorage.getNamespaceName());
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), resource -> {
            for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
                resource.getMetadata().getLabels().put(label.getKey(), label.getValue());
            }
            resource.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(customSpecifiedLabelOrAnnotationPvc);
            resource.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(customSpecifiedLabelOrAnnotationPvc);
            resource.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(customSpecifiedLabelOrAnnotationPvc);
            resource.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(customSpecifiedLabelOrAnnotationPvc);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for rolling update of ZooKeeper and Kafka");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 1, zkPods);
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);

        LOGGER.info("---> PVC (both labels and annotation) <---");

        LOGGER.info("Waiting for changes in PVC labels and Kafka to become ready");
        PersistentVolumeClaimUtils.waitUntilPVCLabelsChange(testStorage.getNamespaceName(), testStorage.getClusterName(), customSpecifiedLabelOrAnnotationPvc, pvcLabelOrAnnotationKey);
        PersistentVolumeClaimUtils.waitUntilPVCAnnotationChange(testStorage.getNamespaceName(), testStorage.getClusterName(), customSpecifiedLabelOrAnnotationPvc, pvcLabelOrAnnotationKey);

        pvcs = kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(testStorage.getClusterName())).collect(Collectors.toList());
        LOGGER.info(pvcs.toString());

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying replaced PVC/{} label/{}={}, as both label and annotation", pvc.getMetadata().getName(), pvcLabelOrAnnotationKey, pvc.getMetadata().getLabels().get(pvcLabelOrAnnotationKey));

            assertThat(pvc.getMetadata().getLabels().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValueModified));
            assertThat(pvc.getMetadata().getAnnotations().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValueModified));
        }

        LOGGER.info("---> SERVICES <---");

        LOGGER.info("Waiting for Kafka Service labels changed {}", customSpecifiedLabels);
        ServiceUtils.waitForServiceLabelsChange(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()), customSpecifiedLabels);

        LOGGER.info("Verifying Kafka labels via Services");
        Service service = kubeClient().getService(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()));

        verifyPresentLabels(customSpecifiedLabels, service.getMetadata().getLabels());

        LOGGER.info("---> CONFIG MAPS <---");

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(testStorage.getClusterName(), kafkaReplicas)) {
            LOGGER.info("Waiting for Kafka ConfigMap {}/{} to have new labels: {}", testStorage.getNamespaceName(), cmName, customSpecifiedLabels);
            ConfigMapUtils.waitForConfigMapLabelsChange(testStorage.getNamespaceName(), cmName, customSpecifiedLabels);

            LOGGER.info("Verifying Kafka labels on ConfigMap {}/{}", testStorage.getNamespaceName(), cmName);
            ConfigMap configMap = kubeClient(testStorage.getNamespaceName()).getConfigMap(testStorage.getNamespaceName(), cmName);

            verifyPresentLabels(customSpecifiedLabels, configMap.getMetadata().getLabels());
        }

        LOGGER.info("---> STRIMZI POD SETS <---");

        LOGGER.info("Waiting for StrimziPodSet labels changed {}", customSpecifiedLabels);
        StrimziPodSetUtils.waitForStrimziPodSetLabelsChange(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName(), customSpecifiedLabels);

        LOGGER.info("Verifying Kafka labels via StrimziPodSet");
        verifyPresentLabels(customSpecifiedLabels, StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName()));

        LOGGER.info("Verifying via Kafka Pods");
        Map<String, String> podLabels = kubeClient().getPod(testStorage.getNamespaceName(), KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0)).getMetadata().getLabels();

        for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
            assertThat("Label exists in Kafka Pods", label.getValue().equals(podLabels.get(label.getKey())));
        }

        LOGGER.info("Produce and Consume messages to make sure Kafka cluster is not broken by labels and annotations manipulation");
        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    /**
     * @description This test case verifies correct storage of messages on disk, and their presence even after rolling update of all Kafka Pods. Test case
     * also checks if offset topic related files are present.
     *
     * @steps
     *  1. - Deploy persistent Kafka with corresponding configuration of offsets topic.
     *     - Kafka is created with expected configuration.
     *  2. - Create KafkaTopic with corresponding configuration
     *     - KafkaTopic is created with expected configuration.
     *  3. - Execute command to check presence of offsets topic related files.
     *     - Files related to Offset topic are present.
     *  4. - Produce default number of messages to already created topic.
     *     - Produced messages are present.
     *  5. - Perform rolling update on all Kafka Pods, in this case single broker.
     *     - After rolling update is completed all messages are again present, as they were successfully stored on disk.
     *
     * @usecase
     *  - data-storage
     *  - kafka-configuration
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @KRaftNotSupported("Topic Operator is not supported by KRaft mode and is used in this test class")
    void testMessagesAndConsumerOffsetFilesOnDisk(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        final Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("offsets.topic.num.partitions", "100");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1)
            .editSpec()
                .editKafka()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> kafkaPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 1, 1, testStorage.getNamespaceName()).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        TestUtils.waitFor("KafkaTopic creation inside Kafka Pod", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0), "/bin/bash",
                        "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1").out().contains(testStorage.getTopicName()));

        String topicDirNameInPod = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0), "/bin/bash",
                "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1 | sed -n '/" + testStorage.getTopicName() + "/p'").out();

        String commandToGetDataFromTopic =
                "cd /var/lib/kafka/data/kafka-log0/" + topicDirNameInPod + "/;cat 00000000000000000000.log";

        LOGGER.info("Executing command: {} in {}", commandToGetDataFromTopic, KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0));
        String topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0),
                "/bin/bash", "-c", commandToGetDataFromTopic).out();

        LOGGER.info("Topic: {} is present in Kafka Broker: {} with no data", testStorage.getTopicName(), KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0));
        assertThat("Topic contains data", topicData, emptyOrNullString());

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Verifying presence of files created to store offsets Topic");
        String commandToGetFiles = "cd /var/lib/kafka/data/kafka-log0/; ls -l | grep __consumer_offsets | wc -l";
        String result = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0),
            "/bin/bash", "-c", commandToGetFiles).out();

        assertThat("Folder kafka-log0 doesn't contain 100 files related to storing consumer offsets", Integer.parseInt(result.trim()) == 100);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0));
        topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());

        List<Pod> kafkaPods = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        for (Pod kafkaPod : kafkaPods) {
            LOGGER.info("Deleting Kafka Pod: {}/{}", testStorage.getNamespaceName(), kafkaPod.getMetadata().getName());
            kubeClient(testStorage.getNamespaceName()).deletePod(testStorage.getNamespaceName(), kafkaPod);
        }

        LOGGER.info("Waiting for Kafka rolling restart");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 1, kafkaPodsSnapshot);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0));
        topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResource.getKafkaPodName(testStorage.getClusterName(), 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());
    }

    /**
     * @description This test case verifies that Kafka (with all its components, including Zookeeper, Entity Operator, KafkaExporter, CruiseControl) configured with
     * 'withReadOnlyRootFilesystem' can be deployed and also works correctly.
     *
     * @steps
     *  1. - Deploy persistent Kafka with 3 Kafka and Zookeeper replicas, Entity Operator, CruiseControl, and KafkaExporter. Each component has configuration 'withReadOnlyRootFilesystem' set to true.
     *     - Kafka and its components are deployed.
     *  2. - Create Kafka producer and consumer.
     *     - Kafka clients are successfully created.
     *  3. - Produce and consume messages using created clients.
     *     - Messages are successfully send and received.
     *
     * @usecase
     *  - root-file-system
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(CRUISE_CONTROL)
    void testReadOnlyRootFileSystem(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewKafkaContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewZookeeperContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endZookeeperContainer()
                        .endTemplate()
                    .endZookeeper()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewTlsSidecarContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTlsSidecarContainer()
                            .withNewTopicOperatorContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTopicOperatorContainer()
                            .withNewUserOperatorContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endUserOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                    .editOrNewKafkaExporter()
                        .withNewTemplate()
                            .withNewContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endContainer()
                        .endTemplate()
                    .endKafkaExporter()
                    .editOrNewCruiseControl()
                        .withNewTemplate()
                            .withNewTlsSidecarContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTlsSidecarContainer()
                            .withNewCruiseControlContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endCruiseControlContainer()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();

        if (Environment.isKRaftModeEnabled()) {
            kafka.getSpec().getEntityOperator().getTemplate().setTopicOperatorContainer(null);
        }

        resourceManager.createResource(extensionContext, kafka);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    void testDeployUnsupportedKafka(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        String nonExistingVersion = "6.6.6";
        String nonExistingVersionMessage = "Unsupported Kafka.spec.kafka.version: " + nonExistingVersion + ". Supported versions are:.*";
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
                .editSpec()
                    .editKafka()
                        .withVersion(nonExistingVersion)
                    .endKafka()
                .endSpec()
                .build());

        LOGGER.info("Kafka with version {} deployed.", nonExistingVersion);

        KafkaUtils.waitForKafkaNotReady(testStorage.getNamespaceName(), clusterName);
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, testStorage.getNamespaceName(), nonExistingVersionMessage);

        KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(clusterName).delete();
    }

    void verifyVolumeNamesAndLabels(String namespaceName, String clusterName, String podSetName, int kafkaReplicas, int diskCountPerReplica, String diskSizeGi) {
        ArrayList<String> pvcs = new ArrayList<>();

        kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).stream()
            .filter(pvc -> pvc.getMetadata().getName().contains(podSetName))
            .forEach(volume -> {
                String volumeName = volume.getMetadata().getName();
                pvcs.add(volumeName);
                LOGGER.info("Checking labels for volume:" + volumeName);
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(Kafka.RESOURCE_KIND));
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(clusterName.concat("-kafka")));
                assertThat(volume.getSpec().getResources().getRequests().get("storage"), is(new Quantity(diskSizeGi, "Gi")));
            });

        LOGGER.info("Checking PVC names included in JBOD array");
        for (int i = 0; i < kafkaReplicas; i++) {
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(pvcs.contains("data-" + j + "-" + podSetName + "-" + i), is(true));
            }
        }

        LOGGER.info("Checking PVC on Kafka Pods");
        for (int i = 0; i < kafkaReplicas; i++) {
            ArrayList<String> dataSourcesOnPod = new ArrayList<>();
            ArrayList<String> pvcsOnPod = new ArrayList<>();

            LOGGER.info("Getting list of mounted data sources and PVCs on Kafka Pod: " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.add(kubeClient(namespaceName).getPod(namespaceName, String.join("-", podSetName, String.valueOf(i)))
                        .getSpec().getVolumes().get(j).getName());
                pvcsOnPod.add(kubeClient(namespaceName).getPod(namespaceName, String.join("-", podSetName, String.valueOf(i)))
                        .getSpec().getVolumes().get(j).getPersistentVolumeClaim().getClaimName());
            }

            LOGGER.info("Verifying mounted data sources and PVCs on Kafka Pod: " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(dataSourcesOnPod.contains("data-" + j), is(true));
                assertThat(pvcsOnPod.contains("data-" + j + "-" + podSetName + "-" + i), is(true));
            }
        }
    }

    void verifyPresentLabels(Map<String, String> labels, Map<String, String> resourceLabels) {
        for (Map.Entry<String, String> label : labels.entrySet()) {
            assertThat("Label exists with concrete value in HasMetadata(Services, CM, STS) resources",
                    label.getValue().equals(resourceLabels.get(label.getKey())));
        }
    }

    void verifyAppLabels(Map<String, String> labels) {
        LOGGER.info("Verifying labels {}", labels);
        assertThat("Label " + Labels.STRIMZI_CLUSTER_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_CLUSTER_LABEL));
        assertThat("Label " + Labels.STRIMZI_KIND_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_KIND_LABEL));
        assertThat("Label " + Labels.STRIMZI_NAME_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_NAME_LABEL));
    }

    void verifyAppLabelsForSecretsAndConfigMaps(Map<String, String> labels) {
        LOGGER.info("Verifying labels {}", labels);
        assertThat("Label " + Labels.STRIMZI_CLUSTER_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_CLUSTER_LABEL));
        assertThat("Label " + Labels.STRIMZI_KIND_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_KIND_LABEL));
    }

    protected void afterEachMayOverride(ExtensionContext extensionContext) throws Exception {
        resourceManager.deleteResources(extensionContext);

        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);

        if (KafkaResource.kafkaClient().inNamespace(namespaceName).withName(OPENSHIFT_CLUSTER_NAME).get() != null) {
            cmdKubeClient(namespaceName).deleteByName(Kafka.RESOURCE_KIND, OPENSHIFT_CLUSTER_NAME);
        }

        kubeClient(namespaceName).listPods(namespaceName).stream()
            .filter(p -> p.getMetadata().getName().startsWith(OPENSHIFT_CLUSTER_NAME))
            .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getNamespace(), p.getMetadata().getName()));

        kubeClient(namespaceName).getClient().resources(KafkaTopic.class, KafkaTopicList.class).inNamespace(namespaceName).delete();
        kubeClient(namespaceName).getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();

        testSuiteNamespaceManager.deleteParallelNamespace(extensionContext);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation(extensionContext)
                .createInstallation()
                .runInstallation();
    }
}
