/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SPECIFIC;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(SPECIFIC)
@Tag(REGRESSION)
public class SpecificIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(SpecificIsolatedST.class);

    @IsolatedTest
    void testClusterWideOperatorWithLimitedAccessToSpecificNamespaceViaRbacRole(final ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String namespaceWhereCreationOfCustomResourcesIsApproved = "example-1";

        // --- a) defining Role and ClusterRoles
        final Role strimziClusterOperator020 = TestUtils.configFromYaml(SetupClusterOperator.getInstance().switchClusterRolesToRolesIfNeeded(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-ClusterRole-strimzi-cluster-operator-role.yaml"), true), Role.class);

        // specify explicit namespace for Role (for ClusterRole we do not specify namespace because ClusterRole is a non-namespaced resource
        strimziClusterOperator020.getMetadata().setNamespace(namespaceWhereCreationOfCustomResourcesIsApproved);

        final ClusterRole strimziClusterOperator021 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator022 = TestUtils.configFromYaml(SetupClusterOperator.getInstance().changeLeaseNameInResourceIfNeeded(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-ClusterRole-strimzi-cluster-operator-role.yaml").getAbsolutePath()), ClusterRole.class);
        final ClusterRole strimziClusterOperator023 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/023-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator030 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRole-strimzi-kafka-broker.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator031 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-ClusterRole-strimzi-entity-operator.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator033 = TestUtils.configFromYaml(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRole-strimzi-kafka-client.yaml"), ClusterRole.class);

        final List<Role> roles = Arrays.asList(strimziClusterOperator020);
        final List<ClusterRole> clusterRoles = Arrays.asList(strimziClusterOperator021, strimziClusterOperator022,
                strimziClusterOperator023, strimziClusterOperator030, strimziClusterOperator031, strimziClusterOperator033);

        // ---- b) defining RoleBindings
        final RoleBinding strimziClusterOperator020Namespaced = TestUtils.configFromYaml(SetupClusterOperator.getInstance().switchClusterRolesToRolesIfNeeded(new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml"), true), RoleBinding.class);
        final RoleBinding strimziClusterOperator022LeaderElection = TestUtils.configFromYaml(SetupClusterOperator.getInstance().changeLeaseNameInResourceIfNeeded(new File(Constants.PATH_TO_LEASE_ROLE_BINDING).getAbsolutePath()), RoleBinding.class);

        // specify explicit namespace for RoleBindings
        strimziClusterOperator020Namespaced.getMetadata().setNamespace(namespaceWhereCreationOfCustomResourcesIsApproved);
        strimziClusterOperator022LeaderElection.getMetadata().setNamespace(clusterOperator.getDeploymentNamespace());

        // reference Cluster Operator service account in RoleBindings
        strimziClusterOperator020Namespaced.getSubjects().stream().findFirst().get().setNamespace(clusterOperator.getDeploymentNamespace());
        strimziClusterOperator022LeaderElection.getSubjects().stream().findFirst().get().setNamespace(clusterOperator.getDeploymentNamespace());

        final List<RoleBinding> roleBindings = Arrays.asList(
                strimziClusterOperator020Namespaced,
                strimziClusterOperator022LeaderElection
        );

        // ---- c) defining ClusterRoleBindings
        final List<ClusterRoleBinding> clusterRoleBindings = Arrays.asList(
            ClusterRoleBindingTemplates.getClusterOperatorWatchedCrb(clusterOperator.getClusterOperatorName(), clusterOperator.getDeploymentNamespace()),
            ClusterRoleBindingTemplates.getClusterOperatorEntityOperatorCrb(clusterOperator.getClusterOperatorName(), clusterOperator.getDeploymentNamespace())
        );

        clusterOperator.unInstall();
        // create namespace, where we will be able to deploy Custom Resources
        cluster.createNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName(), extensionContext.getRequiredTestMethod().getName()), namespaceWhereCreationOfCustomResourcesIsApproved);
        StUtils.copyImagePullSecrets(namespaceWhereCreationOfCustomResourcesIsApproved);
        clusterOperator = clusterOperator.defaultInstallation(extensionContext)
            // use our pre-defined Roles
            .withRoles(roles)
            // use our pre-defined RoleBindings
            .withRoleBindings(roleBindings)
            // use our pre-defined ClusterRoles
            .withClusterRoles(clusterRoles)
            // use our pre-defined ClusterRoleBindings
            .withClusterRoleBindings(clusterRoleBindings)
            .createInstallation()
            .runBundleInstallation();

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editMetadata()
                // this should not work
                    .withNamespace(clusterOperator.getDeploymentNamespace())
                .endMetadata()
                .build());

        // implicit verification that a user is able to deploy Kafka cluster in namespace <example-1>, where we are allowed
        // to create Custom Resources because of `*-namespaced Role`
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editMetadata()
                // this should work
                    .withNamespace(namespaceWhereCreationOfCustomResourcesIsApproved)
                .endMetadata()
                .build());

        // verify that in `infra-namespace` we are not able to deploy Kafka cluster
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getClusterName(), clusterOperator.getDeploymentNamespace(),
                ".*Forbidden!Configured service account doesn't have access.*");

        final Condition condition = KafkaResource.kafkaClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().findFirst().get();

        assertThat(condition.getReason(), CoreMatchers.is("KubernetesClientException"));
        assertThat(condition.getStatus(), CoreMatchers.is("True"));
    }

    @BeforeAll
    void setUp(final ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }
}
