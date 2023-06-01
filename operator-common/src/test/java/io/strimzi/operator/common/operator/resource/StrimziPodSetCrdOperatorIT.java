/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.test.TestUtils;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class StrimziPodSetCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, StrimziPodSet, StrimziPodSetList> {
    protected static final Logger LOGGER = LogManager.getLogger(StrimziPodSetCrdOperatorIT.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected StrimziPodSetOperator operator() {
        return new StrimziPodSetOperator(client, 10_000L);
    }

    @Override
    protected String getCrd() {
        return TestUtils.CRD_STRIMZI_POD_SET;
    }

    @Override
    protected String getCrdName() {
        return StrimziPodSet.CRD_NAME;
    }

    @Override
    protected String getNamespace() {
        return "strimzipodset-crd-it-namespace";
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StrimziPodSet getResource(String resourceName) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName("broker")
                    .withLabels(Map.of("role", "broker"))
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder().withName("broker").withImage("kafka:latest").build())
                .endSpec()
                .build();

        return new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("role", "broker")).build())
                    .withPods(mapper.convertValue(pod, new TypeReference<Map<String, Object>>() { }))
                .endSpec()
                .withNewStatus()
                    .withPods(1)
                    .withCurrentPods(1)
                    .withReadyPods(1)
                .endStatus()
                .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StrimziPodSet getResourceWithModifications(StrimziPodSet resourceInCluster) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName("broker2")
                    .withLabels(Map.of("role", "broker"))
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder().withName("broker").withImage("kafka:latest").build())
                .endSpec()
                .build();

        return new StrimziPodSetBuilder(resourceInCluster)
                .editSpec()
                    .addToPods(mapper.convertValue(pod, new TypeReference<Map<String, Object>>() { }))
                .endSpec()
                .build();

    }

    @Override
    protected StrimziPodSet getResourceWithNewReadyStatus(StrimziPodSet resourceInCluster) {
        return new StrimziPodSetBuilder(resourceInCluster)
                .withNewStatus()
                    .withPods(1)
                    .withCurrentPods(1)
                    .withReadyPods(1)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(VertxTestContext context, StrimziPodSet resource) {
        context.verify(() -> {
            int replicas = resource.getSpec().getPods().size();

            assertThat(resource.getStatus() != null
                    && replicas == resource.getStatus().getPods()
                    && replicas == resource.getStatus().getCurrentPods()
                    && replicas == resource.getStatus().getReadyPods(), is(true));
        });
    }

    /**
     * PodSet creation requires the status to be updated and the PodSet to be ready. This helper method updates the status once created.
     *
     * @param op            PodSet operator instance
     * @param namespace     Namespace of the PodSet resource
     * @param resourceName  Name of the PodSet resource
     */
    private void readinessHelper(StrimziPodSetOperator op, String namespace, String resourceName)  {
        LOGGER.info("Setup helper to update readiness");
        op.waitFor(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, 100L, 10_000L, (n, ns) -> operator().get(namespace, resourceName) != null)
                .thenCompose(i -> {
                    LOGGER.info("Updating readiness in helper");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, getResource(resourceName));
                });
    }

    @Test
    public void testUnreadyCreateFails(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        // We create custom operator here to use small timout
        StrimziPodSetOperator op = new StrimziPodSetOperator(client, 100L);

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .whenComplete((rr, e) -> {
                    context.verify(() -> assertThat(Util.unwrap(e), instanceOf(TimeoutException.class)));
                    async.flag();
                });
    }

    @Test
    public void testReadyCreateSucceeds(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();
        readinessHelper(op, namespace, resourceName); // Required to be able to create the resource

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .whenComplete((rrModified, e) -> assertNull(e))
                .thenCompose(rrModified -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .whenComplete((rrDeleted, e) -> {
                    assertNull(e);
                    async.flag();
                });
    }

    @Test
    public void testUpdateStatus(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();
        readinessHelper(op, namespace, resourceName); // Required to be able to create the resource

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .whenComplete((rr, e) -> assertNull(e))
                .thenCompose(i -> op.getAsync(namespace, resourceName)) // We need to get it again because of the faked readiness which would cause 409 error
                .whenComplete((podSet, e) -> assertNull(e))
                .thenCompose(podSet -> {
                    StrimziPodSet newStatus = getResourceWithNewReadyStatus(podSet);

                    LOGGER.info("Updating resource status");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
                })
                .whenComplete((podSet, e) -> assertNull(e))
                .thenCompose(podSet -> op.getAsync(namespace, resourceName))
                .whenComplete((podSet, e) -> {
                    assertNull(e);
                    context.verify(() -> assertReady(context, podSet));
                })
                .thenCompose(podSet -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .whenComplete((podsetDeleted, e) -> {
                    assertNull(e);
                    async.flag();
                });
    }

    /**
     * Tests what happens when the resource is deleted while updating the status
     *
     * @param context   Test context
     */
    @Test
    public void testUpdateStatusAfterResourceDeletedThrowsKubernetesClientException(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();
        readinessHelper(op, namespace, resourceName); // Required to be able to create the resource

        AtomicReference<StrimziPodSet> newStatus = new AtomicReference<>();

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .whenComplete((rr, e) -> assertNull(e))

                .thenCompose(rr -> {
                    LOGGER.info("Saving resource with status change prior to deletion");
                    newStatus.set(getResourceWithNewReadyStatus(op.get(namespace, resourceName)));
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .whenComplete((rr, e) -> assertNull(e))
                .thenCompose(i -> {
                    LOGGER.info("Wait for confirmed deletion");
                    return op.waitFor(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, 100L, 10_000L, (n, ns) -> operator().get(namespace, resourceName) == null);
                })
                .thenCompose(i -> {
                    LOGGER.info("Updating resource with new status - should fail");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus.get());
                })
                .whenComplete((rr, e) -> {
                    context.verify(() -> {
                        assertThat(Util.unwrap(e), instanceOf(KubernetesClientException.class));
                        async.flag();
                    });
                });
    }

    /**
     * Tests what happens when the resource is modified while updating the status
     *
     * @param context   Test context
     */
    @Test
    public void testUpdateStatusAfterResourceUpdated(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        CompletableFuture<Void> updateStatus = new CompletableFuture<>();
        readinessHelper(op, namespace, resourceName); // Required to be able to create the resource

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .whenComplete((rr, e) -> assertNull(e))
                .thenCompose(rrCreated -> {
                    StrimziPodSet updated = getResourceWithModifications(rrCreated.resource());
                    StrimziPodSet newStatus = getResourceWithNewReadyStatus(rrCreated.resource());

                    LOGGER.info("Updating resource (mocking an update due to some other reason)");
                    op.operation().inNamespace(namespace).withName(resourceName).patch(updated);

                    LOGGER.info("Updating resource status after underlying resource has changed");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
                })
                .whenComplete((rr, e) -> context.verify(() -> TestUtils.unwrap(e).ifPresentOrElse(
                        cause -> {
                            LOGGER.info("Failed as expected");
                            assertThat(cause, instanceOf(KubernetesClientException.class));
                            assertThat(((KubernetesClientException) cause).getCode(), Matchers.is(409));
                            updateStatus.complete(null);
                        },
                        () -> context.failNow("Expected an exception, but nothing was thrown"))));

        updateStatus
                .thenCompose(v -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .whenComplete((rr, e) -> {
                    assertNull(e);
                    async.flag();
                });
    }
}