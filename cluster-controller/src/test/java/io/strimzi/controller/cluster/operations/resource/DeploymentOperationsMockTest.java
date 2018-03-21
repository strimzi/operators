/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentOperationsMockTest extends
        ScalableResourceOperationsMockTest<KubernetesClient, Deployment, DeploymentList,
                DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>, Void> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<ScalableResource> resourceType() {
        return ScalableResource.class;
    }

    @Override
    protected Deployment resource() {
        return new DeploymentBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(RESOURCE_NAME).endMetadata().build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        ExtensionsAPIGroupDSL mockExt = mock(ExtensionsAPIGroupDSL.class);
        when(mockExt.deployments()).thenReturn(op);
        when(mockClient.extensions()).thenReturn(mockExt);

    }

    @Override
    protected DeploymentOperations createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new DeploymentOperations(vertx, mockClient);
    }
}
