/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelector;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlUserTaskStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.k8s.KubeClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.utils.specific.CruiseControlUtils.CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@Tag(ACCEPTANCE)
@ParallelSuite
public class CruiseControlApiST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlApiST.class);
    private static final String CRUISE_CONTROL_NAME = "Cruise Control";

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(CruiseControlApiST.class.getSimpleName()).stream().findFirst().get();
    private final String cruiseControlApiClusterName = "cruise-control-api-cluster-name";

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testCruiseControlBasicAPIRequests(ExtensionContext extensionContext)  {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build());

        LOGGER.info("----> CRUISE CONTROL DEPLOYMENT STATE ENDPOINT <----");

        String response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.STATE,  CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        assertThat(response, is("Unrecognized endpoint in request '/state'\n" +
            "Supported POST endpoints: [ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, STOP_PROPOSAL_EXECUTION, PAUSE_SAMPLING, " +
                "RESUME_SAMPLING, DEMOTE_BROKER, ADMIN, REVIEW, TOPIC_CONFIGURATION, RIGHTSIZE]\n"));

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE,  CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        LOGGER.info("Verifying that {} REST API is available", CRUISE_CONTROL_NAME);

        assertThat(response, not(containsString("404")));
        assertThat(response, containsString("RUNNING"));
        assertThat(response, containsString("NO_TASK_IN_PROGRESS"));

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(testStorage.getNamespaceName());

        LOGGER.info("----> KAFKA REBALANCE <----");

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.REBALANCE,  CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        assertThat(response, is("Unrecognized endpoint in request '/rebalance'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        LOGGER.info("Waiting for CC will have for enough metrics to be recorded to make a proposal ");
        CruiseControlUtils.waitForRebalanceEndpointIsReady(testStorage.getNamespaceName());

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.REBALANCE,  CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        // all goals stats that contains
        assertCCGoalsInResponse(response);

        assertThat(response, containsString("Cluster load after rebalance"));

        LOGGER.info("----> EXECUTION OF STOP PROPOSAL <----");

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STOP, CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        assertThat(response, is("Unrecognized endpoint in request '/stop_proposal_execution'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.STOP, CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        assertThat(response, containsString("Proposal execution stopped."));

        LOGGER.info("----> USER TASKS <----");

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.USER_TASKS, CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        assertThat(response, is("Unrecognized endpoint in request '/user_tasks'\n" +
            "Supported POST endpoints: [ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, STOP_PROPOSAL_EXECUTION, PAUSE_SAMPLING, " +
                "RESUME_SAMPLING, DEMOTE_BROKER, ADMIN, REVIEW, TOPIC_CONFIGURATION, RIGHTSIZE]\n"));

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.USER_TASKS,  CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        assertThat(response, containsString("GET"));
        assertThat(response, containsString(CruiseControlEndpoints.STATE.toString()));
        assertThat(response, containsString("POST"));
        assertThat(response, containsString(CruiseControlEndpoints.REBALANCE.toString()));
        assertThat(response, containsString(CruiseControlEndpoints.STOP.toString()));
        assertThat(response, containsString(CruiseControlUserTaskStatus.COMPLETED.toString()));


        LOGGER.info("Verifying that {} REST API doesn't allow HTTP requests", CRUISE_CONTROL_NAME);

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE,  CruiseControlUtils.SupportedSchemes.HTTP, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);
        assertThat(response, not(containsString("RUNNING")));
        assertThat(response, not(containsString("NO_TASK_IN_PROGRESS")));

        LOGGER.info("Verifying that {} REST API doesn't allow unauthenticated requests", CRUISE_CONTROL_NAME);

        response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE,  CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);
        assertThat(response, containsString("401 Unauthorized"));
    }

    @ParallelNamespaceTest
    void testCruiseControlBasicAPIRequestsWithApiUser(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        String namespace = testStorage.getNamespaceName();

        KubeClient kubeClient = kubeClient(namespace);

        String user1 = "user1";
        String user2 = "user2";
        String userRole = "USER";

        String secretName = "user1";
        String secretKey  = "key";
        String password   = "password";

        resourceManager.createResource(extensionContext,
            new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                .endMetadata()
                .addToData(secretKey, password)
            .build());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(cruiseControlApiClusterName, 3, 3)
            .editOrNewSpec()
                .withNewCruiseControl()
                .withApiUsers()
                    .addNewApiUser()
                        .withName(user1)
                        .withRole(userRole)
                        .withNewPassword()
                            .withNewValueFrom()
                                .withSecretKeyRef(new SecretKeySelector(secretKey, secretName, false))
                            .endValueFrom()
                        .endPassword()
                    .endApiUser()
                    .addNewApiUser()
                        .withName(user2)
                        .withRole(userRole)
                    .endApiUser()
                .endCruiseControl()
            .endSpec()
            .build());

        LOGGER.info("----> CRUISE CONTROL DEPLOYMENT STATE ENDPOINT <----");

        // Test that user can access secured REST API with custom secret
        String response = CruiseControlUtils.callApiWithCredentials(namespace, CruiseControlUtils.SupportedHttpMethods.GET,
                CruiseControlEndpoints.STATE, CruiseControlUtils.SupportedSchemes.HTTPS, user1, password, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);
        assertThat(response, not(containsString("404")));
        assertThat(response, containsString("RUNNING"));
        assertThat(response, containsString("NO_TASK_IN_PROGRESS"));

        // Test that user can access secured REST API with generated secret
        Secret generatedSecret = kubeClient.namespace(namespace).getSecret(CruiseControlResources.apiAuthUserSecretName(cruiseControlApiClusterName, user2));
        String user2Password = generatedSecret.getData().get("password");

        response = CruiseControlUtils.callApiWithCredentials(namespace, CruiseControlUtils.SupportedHttpMethods.GET,
                CruiseControlEndpoints.STATE, CruiseControlUtils.SupportedSchemes.HTTPS, user2, user2Password, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);
        assertThat(response, not(containsString("404")));
        assertThat(response, containsString("RUNNING"));
        assertThat(response, containsString("NO_TASK_IN_PROGRESS"));
    }

    @ParallelNamespaceTest
    void testCruiseControlBasicAPIRequestsWithSecurityDisabled(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        Map<String, Object> config = new HashMap<>();
        config.put("webserver.security.enable", "false");
        config.put("webserver.ssl.enable", "false");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(cruiseControlApiClusterName, 3, 3)
            .editOrNewSpec()
                .withNewCruiseControl()
                    .withConfig(config)
                .endCruiseControl()
            .endSpec()
            .build());

        LOGGER.info("----> CRUISE CONTROL DEPLOYMENT STATE ENDPOINT <----");

        String response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE, CruiseControlUtils.SupportedSchemes.HTTP, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        LOGGER.info("Verifying that {} REST API is available using HTTP request without credentials", CRUISE_CONTROL_NAME);

        assertThat(response, not(containsString("404")));
        assertThat(response, containsString("RUNNING"));
        assertThat(response, containsString("NO_TASK_IN_PROGRESS"));
    }

    @ParallelNamespaceTest
    void testCruiseControlAPIForScalingBrokersUpAndDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 5, 3).build());

        LOGGER.info("Checking if we are able to execute GET request on {} and {} endpoints", CruiseControlEndpoints.ADD_BROKER, CruiseControlEndpoints.REMOVE_BROKER);

        String response = CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.ADD_BROKER,  CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        assertThat(response, is("Unrecognized endpoint in request '/add_broker'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        response =  CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.REMOVE_BROKER,  CruiseControlUtils.SupportedSchemes.HTTPS, CRUISE_CONTROL_DEFAULT_ENDPOINT_SUFFIX);

        assertThat(response, is("Unrecognized endpoint in request '/remove_broker'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        LOGGER.info("Waiting for CC will have for enough metrics to be recorded to make a proposal ");
        CruiseControlUtils.waitForRebalanceEndpointIsReady(testStorage.getNamespaceName());

        response =  CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.ADD_BROKER,  CruiseControlUtils.SupportedSchemes.HTTPS, "?brokerid=3,4");

        assertCCGoalsInResponse(response);
        assertThat(response, containsString("Cluster load after adding broker [3, 4]"));

        response =  CruiseControlUtils.callApiWithAdminCredentials(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.REMOVE_BROKER,  CruiseControlUtils.SupportedSchemes.HTTPS, "?brokerid=3,4");

        assertCCGoalsInResponse(response);
        assertThat(response, containsString("Cluster load after removing broker [3, 4]"));
    }

    @ParallelNamespaceTest
    void testKafkaRebalanceAutoApprovalMechanism(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build());

        // KafkaRebalance with auto-approval
        resourceManager.createResource(extensionContext, KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true")
            .endMetadata()
            .build());

        KafkaRebalanceUtils.doRebalancingProcessWithAutoApproval(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND,
            testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    private void assertCCGoalsInResponse(String response) {
        assertThat(response, containsString("RackAwareGoal"));
        assertThat(response, containsString("ReplicaCapacityGoal"));
        assertThat(response, containsString("DiskCapacityGoal"));
        assertThat(response, containsString("NetworkInboundCapacityGoal"));
        assertThat(response, containsString("NetworkOutboundCapacityGoal"));
        assertThat(response, containsString("CpuCapacityGoal"));
        assertThat(response, containsString("ReplicaDistributionGoal"));
        assertThat(response, containsString("DiskUsageDistributionGoal"));
        assertThat(response, containsString("NetworkInboundUsageDistributionGoal"));
        assertThat(response, containsString("NetworkOutboundUsageDistributionGoal"));
        assertThat(response, containsString("CpuUsageDistributionGoal"));
        assertThat(response, containsString("TopicReplicaDistributionGoal"));
        assertThat(response, containsString("LeaderReplicaDistributionGoal"));
        assertThat(response, containsString("LeaderBytesInDistributionGoal"));
        assertThat(response, containsString("PreferredLeaderElectionGoal"));
    }
}
