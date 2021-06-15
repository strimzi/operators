/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
public abstract class AbstractResourceOperatorTest<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList<T>, R extends Resource<T>> {

    public static final String RESOURCE_NAME = "my-resource";
    public static final String NAMESPACE = "test";
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    /**
     * The type of kubernetes client to be mocked
     */
    protected abstract Class<C> clientType();

    /**
     * The type of the resource being tested
     */
    @SuppressWarnings("rawtypes")
    protected abstract Class<? extends Resource> resourceType();

    /**
     * Get a (new) test resource
     */
    protected abstract T resource();

    /**
     * Get a modified test resource to test how are changes handled
     */
    protected abstract T modifiedResource();

    /**
     * Configure the given {@code mockClient} to return the given {@code op}
     * that's appropriate for the kind of resource being tests.
     */
    protected abstract void mocker(C mockClient, MixedOperation<T, L, R> op);

    /** Create the subclass of ResourceOperation to be tested */
    protected abstract AbstractResourceOperator<C, T, L, R> createResourceOperations(Vertx vertx, C mockClient);

    /** Create the subclass of ResourceOperation to be tested with mocked readiness checks*/
    protected AbstractResourceOperator<C, T, L, R> createResourceOperationsWithMockedReadiness(Vertx vertx, C mockClient)    {
        return createResourceOperations(vertx, mockClient);
    }

    @Test
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context) {
        testCreateWhenExistsWithChangeIsAPatch(context, true);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context, boolean cascade) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(cascade ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN)).thenReturn(mockResource);
        when(mockResource.patch(any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, modifiedResource()).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).get();
            verify(mockResource).patch(any());
            verify(mockResource, never()).create(any());
            verify(mockResource, never()).create();
            verify(mockResource, never()).createOrReplace(any());
            verify(mockCms, never()).createOrReplace(any());
            context.completeNow();
        })));
    }

    @Test
    public void testCreateWhenExistsWithoutChangeIsNotAPatch(VertxTestContext context) {
        testCreateWhenExistsWithoutChangeIsNotAPatch(context, true);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testCreateWhenExistsWithoutChangeIsNotAPatch(VertxTestContext context, boolean cascade) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(cascade ? DeletionPropagation.FOREGROUND : DeletionPropagation.ORPHAN)).thenReturn(mockResource);
        when(mockResource.patch(any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource()).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).get();
            verify(mockResource, never()).patch(any());
            verify(mockResource, never()).create(any());
            verify(mockResource, never()).create();
            verify(mockResource, never()).createOrReplace(any());
            verify(mockCms, never()).createOrReplace(any());
            context.completeNow();
        })));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testExistenceCheckThrows(VertxTestContext context) {
        T resource = resource();
        RuntimeException ex = new RuntimeException();

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenThrow(ex);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.failing(e -> context.verify(() -> {
            assertThat(e, is(ex));
            context.completeNow();
        })));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testSuccessfulCreation(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);
        when(mockResource.create((T) any())).thenReturn(resource);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperationsWithMockedReadiness(vertx, mockClient);

        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.succeeding(rr -> context.verify(() -> {
            verify(mockResource).get();
            verify(mockResource).create(eq(resource));
            context.completeNow();
        })));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testCreateOrUpdateThrowsWhenCreateThrows(VertxTestContext context) {
        T resource = resource();
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");

        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(null);

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(resource.getMetadata().getName()))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(resource.getMetadata().getNamespace()))).thenReturn(mockNameable);
        when(mockResource.create((T) any())).thenThrow(ex);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource).onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e, is(ex)));
            context.completeNow();
        }));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testDeleteWhenResourceDoesNotExistIsANop(VertxTestContext context) {
        T resource = resource();
        Resource mockResource = mock(resourceType());

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
            .onComplete(context.succeeding(rr -> context.verify(() -> {
                verify(mockResource).get();
                verify(mockResource, never()).delete();
                context.completeNow();
            })));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testReconcileDeleteWhenResourceExistsStillDeletes(VertxTestContext context) {
        EditReplacePatchDeletable mockDeletable = mock(EditReplacePatchDeletable.class);
        when(mockDeletable.delete()).thenReturn(Boolean.TRUE);
        EditReplacePatchDeletable mockDeletableGrace = mock(EditReplacePatchDeletable.class);
        when(mockDeletableGrace.delete()).thenReturn(Boolean.TRUE);

        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, resource);
            return (Watch) () -> watchClosed.set(true);
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
            .onComplete(context.succeeding(rr -> context.verify(() -> {
                verify(mockDeletable).delete();
                context.completeNow();
            })));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testReconcileDeletionSuccessfullyDeletes(VertxTestContext context) {
        EditReplacePatchDeletable mockDeletable = mock(EditReplacePatchDeletable.class);
        when(mockDeletable.delete()).thenReturn(Boolean.TRUE);
        EditReplacePatchDeletable mockDeletableGrace = mock(EditReplacePatchDeletable.class);
        when(mockDeletableGrace.delete()).thenReturn(Boolean.TRUE);

        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, resource);
            return (Watch) () -> watchClosed.set(true);
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
            .onComplete(context.succeeding(rr -> context.verify(() -> {
                verify(mockDeletable).delete();
                context.completeNow();
            })));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testReconcileDeleteThrowsWhenDeletionThrows(VertxTestContext context) {
        RuntimeException ex = new RuntimeException("Testing this exception is handled correctly");
        EditReplacePatchDeletable mockDeletable = mock(EditReplacePatchDeletable.class);
        EditReplacePatchDeletable mockDeletableGrace = mock(EditReplacePatchDeletable.class);
        when(mockDeletable.delete()).thenThrow(ex);

        EditReplacePatchDeletable mockERPD = mock(EditReplacePatchDeletable.class);
        when(mockERPD.withGracePeriod(anyLong())).thenReturn(mockDeletable);

        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, resource);
            return (Watch) () -> watchClosed.set(true);
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, is(ex));
                context.completeNow();
            })));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testReconcileDeleteThrowsWhenDeletionReturnsFalse(VertxTestContext context) {
        EditReplacePatchDeletable mockDeletable = mock(EditReplacePatchDeletable.class);
        when(mockDeletable.delete()).thenReturn(Boolean.FALSE);
        EditReplacePatchDeletable mockDeletableGrace = mock(EditReplacePatchDeletable.class);
        when(mockDeletableGrace.delete()).thenReturn(Boolean.FALSE);

        T resource = resource();
        Resource mockResource = mock(resourceType());
        when(mockResource.get()).thenReturn(resource);
        when(mockResource.withPropagationPolicy(eq(DeletionPropagation.FOREGROUND))).thenReturn(mockDeletableGrace);
        when(mockDeletableGrace.withGracePeriod(anyLong())).thenReturn(mockDeletable);
        AtomicBoolean watchClosed = new AtomicBoolean(false);
        when(mockResource.watch(any())).thenAnswer(invocation -> {
            Watcher watcher = invocation.getArgument(0);
            watcher.eventReceived(Watcher.Action.DELETED, resource);
            return (Watch) () -> watchClosed.set(true);
        });

        NonNamespaceOperation mockNameable = mock(NonNamespaceOperation.class);
        when(mockNameable.withName(matches(RESOURCE_NAME))).thenReturn(mockResource);

        MixedOperation mockCms = mock(MixedOperation.class);
        when(mockCms.inNamespace(matches(NAMESPACE))).thenReturn(mockNameable);

        C mockClient = mock(clientType());
        mocker(mockClient, mockCms);

        AbstractResourceOperator<C, T, L, R> op = createResourceOperations(vertx, mockClient);

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resource.getMetadata().getNamespace(), resource.getMetadata().getName(), null)
                .onComplete(context.failing(e -> context.verify(() -> {
                    assertThat(e.getMessage(), endsWith("could not be deleted (returned false)"));
                    context.completeNow();
                })));
    }
}
