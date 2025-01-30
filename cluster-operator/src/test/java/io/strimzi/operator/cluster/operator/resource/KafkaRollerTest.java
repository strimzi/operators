/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaRollerTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaRollerTest.class);

    private static final int REPLICAS = 5;

    private static Vertx vertx;
    private List<String> restarted;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    private static int podName2Number(String podName) {
        return Integer.parseInt(podName.substring(ssName().length() + 1));
    }

    private static String clusterName() {
        return "c";
    }

    private static String ssName() {
        return "c-kafka";
    }

    private static String stsNamespace() {
        return "ns";
    }

    private static <X, E extends Throwable> Function<X, E> noException() {
        return podId -> null;
    }

    @Test
    public void testRollWithNoController(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

//    @Test
//    public void testRollTcpProbe(VertxTestContext testContext) {
//        PodOperator podOps = mockPodOps(podId -> succeededFuture());
//        AdminClientProvider mock = givenControllerFutureFailsWithTimeout();
//        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, 3), podOps,
//                noException(), null, noException(), noException(), noException(),
//                brokerId -> succeededFuture(true),
//                true, mock, mockKafkaAgentClientProvider(), true, null, 0);
//        List<String> expectedTcpProbes = List.of(
//                "c-kafka-0.c-kafka-brokers.ns.svc.cluster.local:9090",
//                "c-kafka-1.c-kafka-brokers.ns.svc.cluster.local:9090",
//                "c-kafka-2.c-kafka-brokers.ns.svc.cluster.local:9090"
//        );
//        doSuccessfulRollingRestart(testContext, kafkaRoller,
//                asList(0, 1, 2, 3, 4),
//                asList(0, 1, 2), () -> assertEquals(kafkaRoller.tcpProbes, expectedTcpProbes));
//    }

//    private static AdminClientProvider givenControllerFutureFailsWithTimeout() {
//        KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
//        controllerFuture.completeExceptionally(new TimeoutException("fail"));
//        DescribeClusterResult mockResult = mock(DescribeClusterResult.class);
//        when(mockResult.controller()).thenReturn(controllerFuture);
//        Admin admin = mock(Admin.class);
//        when(admin.describeCluster()).thenReturn(mockResult);
//        AdminClientProvider mock = mock(AdminClientProvider.class);
//        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);
//        return mock;
//    }


    @Test
    public void testRollWithPod2AsActiveController(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, 0, REPLICAS), 2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void testRollWithControllerAndBrokers(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(3, 0, 3), 3);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                asList(4, 5, 3, 0, 1, 2));
    }

    @Test
    public void pod0NotReadyAfterRollingBroker(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
            podId == 0 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-0 to become ready",
                singletonList(0));
        // TODO assert subsequent rolls
    }

    @Test
    public void pod1NotReadyAfterRolling(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                podId == 1 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        // On the first reconciliation we expect it to abort when it gets to pod 1
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-1 to become ready",
                asList(1));
        // On the next reconciliation only pod 2 (controller) would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-1 to become ready",
                emptyList());
    }

    @Test
    public void pod3NotReadyAfterRolling(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                podId == 3 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        // On the first reconciliation we expect it to abort when it gets to pod 3
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-3 to become ready",
                asList(3));
        // On the next reconciliation, we expect it to fail in the same way
        kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-3 to become ready",
                emptyList());
    }

    @Test
    public void activeControllerNotReadyAfterRolling(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                podId == 2 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, 0, REPLICAS), 2);
        // On the first reconciliation we expect it to fail when rolling the controller (i.e. as rolling all the rest)
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                asList(0, 1, 3, 4, 2));
        // On the next reconciliation only pod 2 would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, REPLICAS, 0), 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(2),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                singletonList(2));
    }

    @Test
    public void activeControllerNotReadyAfterRollingCombined(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                podId == 2 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, REPLICAS, 0), 2);
        // On the first reconciliation we expect it to fail when rolling the controller (i.e. as rolling all the rest)
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                asList(0, 1, 3, 4, 2));
        // On the next reconciliation only pod 2 would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, REPLICAS, 0), 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(2),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                singletonList(2));
    }

    public Set<NodeRef> addPodNames(int brokerReplicas, int combinedReplicas, int controllerReplicas) {
        Set<NodeRef> podNames = new LinkedHashSet<>(brokerReplicas);
        for (int brokerId = 0; brokerId < brokerReplicas; brokerId++) {
            podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), brokerId), brokerId, "broker", false, true));
        }
        int maxCombinedId = brokerReplicas + combinedReplicas;
        for (int controllerId = brokerReplicas; controllerId < maxCombinedId; controllerId++) {
            podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), controllerId), controllerId, "combined", true, true));
        }
        int maxControllerId = maxCombinedId + controllerReplicas;
        for (int controllerId = maxCombinedId; controllerId < maxControllerId; controllerId++) {
            podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), controllerId), controllerId, "controller", true, false));
        }
        return podNames;
    }

    public Set<NodeRef> addDisconnectedPodNames(int replicas) {
        Set<NodeRef> podNames = new LinkedHashSet<>(replicas);

        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 10), 10, null, false, true));
        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 200), 200, null, false, true));
        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 30), 30, null, false, true));
        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 400), 400, null, false, true));
        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 500), 500, null, false, true));
        return podNames;
    }

    @Test
    public void testRollWithDisconnectedBrokerPodNames(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addDisconnectedPodNames(REPLICAS), podOps,
                bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.stream().map(node -> node.nodeId()).toList().equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
                null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(10, 200, 30, 400, 500),
                asList(10, 200, 30, 400, 500));
    }


    @Test
    public void testRollHandlesErrorWhenOpeningBrokerAdminClient(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
            bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.stream().map(node -> node.nodeId()).toList().equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
            null, noException(), noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we force restart the node if we can't create an admin client
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testRollHandlesErrorWhenOpeningCombinedAdminClient(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, REPLICAS, 0), podOps,
                bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.stream().map(node -> node.nodeId()).toList().equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
                null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we force restart the node if we can't create an admin client
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testHandlesErrorWhenOpenControllerAdminClient(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.stream().map(node -> node.nodeId()).toList().equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
                null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 4);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we force restart the node if we can't create an admin client
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testRollHandlesErrorWhenGettingActiveController(VertxTestContext testContext) throws InterruptedException {
        int activeController = 2;
        int nonActiveController = 1;
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(), null,
            podId -> podId == nonActiveController ? new RuntimeException("Test Exception") : null, noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, activeController);
        // if we cannot get the active controller due to admin error, we should not roll any controllers
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(1),
                KafkaRoller.ForceableProblem.class, "An error while trying to determine the cluster controller from pod c-kafka-1",
                emptyList());
    }

    @Test
    public void testRollHandlesErrorWhenClosingBrokerAdminClient(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
            noException(),
            new RuntimeException("Test Exception"), noException(), noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods (errors are logged)
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testRollHandlesErrorWhenClosingControllerAdminClient(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(),
                new RuntimeException("Test Exception"), noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 3);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we did the active controller last order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 4, 3));
    }

    @Test
    public void testBrokerNotInitiallyRollable(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        AtomicInteger count = new AtomicInteger(3);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId ->
                    brokerId == 1 ? succeededFuture(count.getAndDecrement() == 0)
                            : succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 2, 3, 4, 1));
    }

    @Test
    public void testCombinedNotInitiallyRollable(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        // we expect canRoll to be called twice for each combined node
        AtomicInteger count = new AtomicInteger(5);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, REPLICAS, 0), podOps,
                noException(), null, noException(), noException(), noException(),
                combinedId ->
                    combinedId == 1 ? succeededFuture(count.getAndDecrement() < 0) : succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, 1, 2));
    }

    @Test
    public void testActiveControllerNotInitiallyRollable(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        AtomicInteger count = new AtomicInteger(3);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(), null, noException(), noException(), noException(),
            controllerId ->
                (controllerId == 1) ? succeededFuture(count.getAndDecrement() == 0) : succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, 1, 2));
    }

    @Test
    public void testBrokerNeverRollable(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId ->
                    brokerId == 1 ? succeededFuture(false)
                            : succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted
                asList(0, 2, 3, 4));
        kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId -> succeededFuture(brokerId != 1),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(1),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted
                emptyList());
    }

    @Test
    public void testCombinedNeverRollable(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, REPLICAS, 0), podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId ->
                        brokerId == 1 ? succeededFuture(false)
                                : succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted
                asList(0, 2, 3, 4));

        kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(brokerId != 1),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(1),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted
                emptyList());
    }

    @Test
    public void testControllerNeverRollable(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS),
                podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId ->
                    brokerId == 2 ? succeededFuture(false)
                            : succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-2 cannot be updated right now.",
                // We expect all non-controller pods to be rolled
                asList(0, 1, 3, 4));
        clearRestarted();
        kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
            podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId -> succeededFuture(brokerId != 2),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(2),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-2 cannot be updated right now.",
                // We expect all non-controller pods to be rolled
                emptyList());
    }

    @Test
    public void testRollHandlesErrorWhenGettingBrokerConfig(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null,
                noException(), noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
            brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 2, 3, 4, 1));
    }

    @Test
    public void testRollHandlesErrorWhenGettingCombinedConfig(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, REPLICAS, 0), podOps,
                noException(), null,
                noException(), noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
                brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 2, 3, 4, 1));
    }

    @Test
    public void testRollHandlesErrorWhenGettingConfigFromController(VertxTestContext testContext) {
        int controller = 2;
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
            noException(), null,
            noException(), noException(), podId -> podId == controller ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
            brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, controller);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, controller));
    }

    @Test
    public void testRollHandlesErrorWhenAlteringConfig(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null,
                noException(), podId -> new KafkaRoller.ForceableProblem("could not get alter exception"), noException(),
            brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods
        doSuccessfulConfigUpdate(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testSuccessfulAlteringConfigNotRoll(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null,
                noException(), noException(), noException(),
            brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should not carry on rolling the pods
        doSuccessfulConfigUpdate(testContext, kafkaRoller,
                emptyList());
    }

    @Test
    public void testSuccessfulRollingKRaftControllers(VertxTestContext testContext) {
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 3, 3),
                mockPodOps(podId -> succeededFuture()), noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                asList(0, 1, 2, 3, 4, 5));
    }

    @Test
    public void testKRaftControllerNoQuorum(VertxTestContext testContext) throws InterruptedException {
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, 3),
                mockPodOps(podId -> succeededFuture()), noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(false), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-0 cannot be updated right now.",
                asList());
    }

    @Test
    public void testControllerAndOneMoreNeverRollable(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS),
            podOps,
            noException(), null, noException(), noException(), noException(),
            brokerId -> brokerId == 2 || brokerId == 3 ? succeededFuture(false) : succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doFailingRollingRestart(testContext, kafkaRoller,
            asList(0, 1, 2, 3, 4),
            KafkaRoller.ForceableProblem.class, "Pod c-kafka-2 is the active controller and there are other pods to verify first",
            // We expect all non-controller pods to be rolled
            asList(0, 1, 4));
    }

    @Test
     public void testBrokerInRecoveryState(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );

        Map<String, Object> recoveryState = new HashMap<>();
        recoveryState.put("remainingLogsToRecover", 10);
        recoveryState.put("remainingSegmentsToRecover", 100);
        BrokerState brokerstate = new BrokerState(2, recoveryState);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 0, 3),
                podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, null, null, false, brokerstate, 3);

        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-0 is not ready because the Kafka node is performing log recovery. There are 10 logs and 100 segments left to recover.",
                asList(4, 5, 3, 1, 2));
    }

    @Test
    public void testControllerInRecoveryState(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );

        Map<String, Object> recoveryState = new HashMap<>();
        recoveryState.put("remainingLogsToRecover", 10);
        recoveryState.put("remainingSegmentsToRecover", 100);
        BrokerState brokerstate = new BrokerState(2, recoveryState);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, 3),
                podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, null, null, false, brokerstate, -1);

        doFailingRollingRestart(testContext, kafkaRoller,
                List.of(0, 1, 2),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-0 is not ready because the Kafka node is performing log recovery. There are 10 logs and 100 segments left to recover.",
                List.of(1, 2));
    }

    @Test
    public void testCombinedInRecoveryState(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 4) ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );

        Map<String, Object> recoveryState = new HashMap<>();
        recoveryState.put("remainingLogsToRecover", 10);
        recoveryState.put("remainingSegmentsToRecover", 100);
        BrokerState brokerstate = new BrokerState(2, recoveryState);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 3, 0),
                podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, null, null, false, brokerstate, 3);

        // It rolls brokers if only successfully rolled controllers
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-4 is not ready because the Kafka node is performing log recovery. There are 10 logs and 100 segments left to recover.",
                asList(5, 3));
    }

    @Test
    public void testBrokerInRunningState(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );

        BrokerState brokerstate = new BrokerState(3, null);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, null, null, false, brokerstate, -1);

        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-0 to become ready",
                asList(0));
    }

    @Test
    public void testFailGettingBrokerState(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );

        BrokerState brokerstate = new BrokerState(-1, null);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, null, null, false, brokerstate, 1);

        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-0 to become ready",
                asList(0));
    }

    @Test
    public void testFailWhenPodIsReadyThrows(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? failedFuture(new KubernetesClientException("Failed")) : succeededFuture()
        );

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, null, null, false, null, 1);

        doFailingRollingRestart(testContext, kafkaRoller,
                List.of(),
                KubernetesClientException.class, "Failed",
                List.of());
    }

    @Test
    public void testRollWithAllRoles(VertxTestContext testContext) {
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 3, 3),
                mockPodOps(podId -> succeededFuture()), noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 6);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4, 5, 6, 7, 8),
                asList(3, 4, 5, 7, 8, 6, 0, 1, 2));
    }

    @Test
    public void testRollUnreadyPodFirstAllRoles(VertxTestContext testContext) {
        AtomicBoolean pod1Unready = new AtomicBoolean(true);
        AtomicBoolean pod4Unready = new AtomicBoolean(true);
        AtomicBoolean pod7Unready = new AtomicBoolean(true);
        PodOperator podOps = mockPodOps(podId -> {
            //unready broker is 1
            if (podId == 1 && pod1Unready.getAndSet(false)) {
                return failedFuture(new TimeoutException("fail"));
            }
            //unready combined node is 4
            if (podId == 4 && pod4Unready.getAndSet(false)) {
                return failedFuture(new TimeoutException("fail"));
            }
            //unready controller is 7
            if (podId == 7 && pod7Unready.getAndSet(false)) {
                return failedFuture(new TimeoutException("fail"));
            }
            return succeededFuture();
        });
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 3, 3),
                podOps, noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4, 5, 6, 7, 8), // brokers, combined, controllers
                asList(7, 4, 3, 5, 6, 8, 1, 0, 2)); //Rolls in order: unready controllers, ready controllers, unready brokers, ready brokers
    }

    private TestingKafkaRoller rollerWithActiveController(PodOperator podOps, Set<NodeRef> nodes, int activeController) {
        return new TestingKafkaRoller(nodes, podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, activeController);
    }

    private void doSuccessfulConfigUpdate(VertxTestContext testContext, TestingKafkaRoller kafkaRoller,
                                            List<Integer> expected) {
        Checkpoint async = testContext.checkpoint();
        kafkaRoller.rollingRestart(pod -> RestartReasons.empty())
                .onComplete(testContext.succeeding(v -> {
                    testContext.verify(() -> assertThat(restarted(), is(expected)));
                    assertNoUnclosedAdminClient(testContext, kafkaRoller);
                    async.flag();
                }));
    }

    private void doSuccessfulRollingRestart(VertxTestContext testContext, TestingKafkaRoller kafkaRoller,
                                    Collection<Integer> podsToRestart,
                                    List<Integer> expected) {
        doSuccessfulRollingRestart(testContext, kafkaRoller, podsToRestart, expected, null);
    }

    private void doSuccessfulRollingRestart(VertxTestContext testContext, TestingKafkaRoller kafkaRoller,
                                            Collection<Integer> podsToRestart,
                                            List<Integer> expected, Runnable onCompletion) {
        Checkpoint async = testContext.checkpoint();
        kafkaRoller.rollingRestart(pod -> {
            if (podsToRestart.contains(podName2Number(pod.getMetadata().getName()))) {
                return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
            } else {
                return RestartReasons.empty();
            }
        })
            .onComplete(testContext.succeeding(v -> {
                testContext.verify(() -> assertThat(restarted(), is(expected)));
                assertNoUnclosedAdminClient(testContext, kafkaRoller);
                if (onCompletion != null) {
                    onCompletion.run();
                }
                async.flag();
            }));
    }

    private void assertNoUnclosedAdminClient(VertxTestContext testContext, TestingKafkaRoller kafkaRoller) {
        if (!kafkaRoller.unclosedAdminClients.isEmpty()) {
            Throwable alloc = kafkaRoller.unclosedAdminClients.values().iterator().next();
            alloc.printStackTrace(System.out);
            testContext.failNow(new Throwable(kafkaRoller.unclosedAdminClients.size() + " unclosed AdminClient instances"));
        }
    }

    private void doFailingRollingRestart(VertxTestContext testContext, TestingKafkaRoller kafkaRoller,
                                 Collection<Integer> podsToRestart,
                                 Class<? extends Throwable> exception, String message,
                                 List<Integer> expectedRestart) throws InterruptedException {
        CountDownLatch async = new CountDownLatch(1);
        kafkaRoller.rollingRestart(pod -> {
            if (podsToRestart.contains(podName2Number(pod.getMetadata().getName()))) {
                return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
            } else {
                return RestartReasons.empty();
            }
        })
            .onComplete(testContext.failing(e -> testContext.verify(() -> {
                try {
                    assertThat(e.getClass() + " is not a subclass of " + exception.getName(), e, instanceOf(exception));
                    assertThat("The exception message was not as expected", e.getMessage(), is(message));
                    assertThat("The restarted pods were not as expected", restarted(), is(expectedRestart));
                    assertNoUnclosedAdminClient(testContext, kafkaRoller);
                    testContext.completeNow();
                } finally {
                    async.countDown();
                }
            })));
        async.await();
    }

    public List<Integer> restarted() {
        return restarted.stream().map(KafkaRollerTest::podName2Number).collect(Collectors.toList());
    }

    @BeforeEach
    public void clearRestarted() {
        restarted = new ArrayList<>();
    }

    private PodOperator mockPodOps(Function<Integer, Future<Void>> readiness) {
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.get(any(), any())).thenAnswer(
                invocation -> new PodBuilder()
                        .withNewMetadata()
                        .withNamespace(invocation.getArgument(0))
                        .withName(invocation.getArgument(1))
                        .endMetadata()
                        .build()
        );
        when(podOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenAnswer(invocationOnMock ->  {
            String podName = invocationOnMock.getArgument(2);
            return readiness.apply(podName2Number(podName));
        });

        when(podOps.isReady(anyString(), anyString())).thenAnswer(invocationOnMock ->  {
            String podName = invocationOnMock.getArgument(1);
            Future<Void> ready = readiness.apply(podName2Number(podName));
            if (ready.succeeded()) {
                return true;
            } else {
                if (ready.cause() instanceof TimeoutException) {
                    return false;
                } else {
                    throw ready.cause();
                }
            }
        });
        return podOps;
    }

    private class TestingKafkaRoller extends KafkaRoller {

        int controllerCall;
        private final IdentityHashMap<Admin, Throwable> unclosedAdminClients;
        private final Function<Set<NodeRef>, RuntimeException> acOpenException;
        private final Throwable acCloseException;
        private final Function<Integer, Future<Boolean>> canRollFn;
        private final Function<Integer, Throwable> controllerException;
        private final Function<Integer, ForceableProblem> alterConfigsException;
        private final Function<Integer, ForceableProblem> getConfigsException;
        private final boolean delegateControllerCall;
        private final boolean delegateAdminClientCall;
        private final int activeController;
        private final List<String> tcpProbes = new ArrayList<>();
        private final BrokerState brokerState;

        @SuppressWarnings("checkstyle:ParameterNumber")
        private TestingKafkaRoller(Set<NodeRef> nodes,
                                   PodOperator podOps,
                                   Function<Set<NodeRef>, RuntimeException> acOpenException,
                                   Throwable acCloseException,
                                   Function<Integer, Throwable> controllerException,
                                   Function<Integer, ForceableProblem> alterConfigsException,
                                   Function<Integer, ForceableProblem> getConfigsException,
                                   Function<Integer, Future<Boolean>> canRollFn,
                                   boolean delegateControllerCall,
                                   AdminClientProvider adminClientProvider,
                                   KafkaAgentClientProvider kafkaAgentClientProvider,
                                   boolean delegateAdminClientCall, BrokerState brokerState, int activeController) {
            super(
                    new Reconciliation("test", "Kafka", stsNamespace(), clusterName()),
                    KafkaRollerTest.vertx,
                    podOps,
                    500,
                    1000,
                    () -> new BackOff(10L, 2, 4),
                    nodes,
                    new TlsPemIdentity(null, null),
                    adminClientProvider,
                    kafkaAgentClientProvider,
                    brokerId -> "",
                    "",
                    KafkaVersionTestUtils.getLatestVersion(),
                    true,
                    mock(KubernetesRestartEventPublisher.class));
            this.delegateControllerCall = delegateControllerCall;
            this.delegateAdminClientCall = delegateAdminClientCall;
            this.activeController = activeController;
            this.controllerCall = 0;
            Objects.requireNonNull(acOpenException);
            this.acOpenException = acOpenException;
            this.controllerException = controllerException;
            this.alterConfigsException = alterConfigsException;
            this.getConfigsException = getConfigsException;
            this.acCloseException = acCloseException;
            this.canRollFn = canRollFn;
            this.unclosedAdminClients = new IdentityHashMap<>();
            this.brokerState = brokerState;
        }

        @Override
        KafkaAgentClient initKafkaAgentClient() {
            return mock(KafkaAgentClient.class, invocation -> {
                if ("getBrokerState".equals(invocation.getMethod().getName())) {
                    if (brokerState == null) {
                        return new BrokerState(-1, null);
                    }
                    return brokerState;
                }
                return null;
            });
        }

        @Override
        protected Admin brokerAdminClient(Set<NodeRef> nodes) throws ForceableProblem {
            if (delegateAdminClientCall) {
                return super.brokerAdminClient(nodes);
            }
            RuntimeException exception = acOpenException.apply(nodes);
            if (exception != null) {
                throw new ForceableProblem("An error while try to create the admin client", exception);
            }
            Admin ac = mock(AdminClient.class, invocation -> {
                if ("close".equals(invocation.getMethod().getName())) {
                    Admin mock = (Admin) invocation.getMock();
                    unclosedAdminClients.remove(mock);
                    if (acCloseException != null) {
                        throw acCloseException;
                    }
                    return null;
                }
                throw new RuntimeException("Not mocked " + invocation.getMethod());
            });
            unclosedAdminClients.put(ac, new Throwable("Pod " + nodes));
            return ac;
        }

        @Override
        protected Admin controllerAdminClient(Set<NodeRef> nodes) throws ForceableProblem {
            if (delegateAdminClientCall) {
                return super.controllerAdminClient(nodes);
            }
            RuntimeException exception = acOpenException.apply(nodes);
            if (exception != null) {
                throw new ForceableProblem("An error while try to create the admin client", exception);
            }
            Admin ac = mock(AdminClient.class, invocation -> {
                if ("close".equals(invocation.getMethod().getName())) {
                    Admin mock = (Admin) invocation.getMock();
                    unclosedAdminClients.remove(mock);
                    if (acCloseException != null) {
                        throw acCloseException;
                    }
                    return null;
                }
                throw new RuntimeException("Not mocked " + invocation.getMethod());
            });
            unclosedAdminClients.put(ac, new Throwable("Pod " + nodes));
            return ac;
        }

        @Override
        protected KafkaAvailability availability(Admin ac) {
            return new KafkaAvailability(null, null) {
                @Override
                protected Future<Set<String>> topicNames() {
                    return succeededFuture(Collections.emptySet());
                }

                @Override
                protected Future<Collection<TopicDescription>> describeTopics(Set<String> names) {
                    return succeededFuture(Collections.emptySet());
                }

                @Override
                Future<Boolean> canRoll(int podId) {
                    return canRollFn.apply(podId);
                }
            };
        }

        @Override
        protected KafkaQuorumCheck quorumCheck(Admin ac, NodeRef nodeRef) {
            Admin admin = mock(Admin.class);
            DescribeMetadataQuorumResult qrmResult = mock(DescribeMetadataQuorumResult.class);
            when(admin.describeMetadataQuorum()).thenReturn(qrmResult);
            KafkaFuture<QuorumInfo> kafkaFuture = KafkaFuture.completedFuture(null);
            when(qrmResult.quorumInfo()).thenReturn(kafkaFuture);
            return new KafkaQuorumCheck(null, admin, null, 0) {
                @Override
                Future<Boolean> canRollController(int nodeId) {
                    return canRollFn.apply(nodeId);
                }
            };
        }

        @Override
        int getActiveController(NodeRef nodeRef, Admin ac, long timeout, TimeUnit unit) throws ForceableProblem, InterruptedException {
            if (delegateControllerCall) {
                return super.getActiveController(nodeRef, ac, timeout, unit);
            }
            Throwable throwable = controllerException.apply(nodeRef.nodeId());
            if (throwable != null) {
                throw new ForceableProblem("An error while trying to determine the cluster controller from pod " + nodeRef.podName(), throwable);
            } else {
                return activeController;
            }
        }

        @Override
        protected Config brokerConfig(NodeRef nodeRef) throws ForceableProblem {
            ForceableProblem problem = getConfigsException.apply(nodeRef.nodeId());
            if (problem != null) {
                throw problem;
            } else return new Config(emptyList());
        }

        @Override
        protected Config brokerLogging(int brokerId) {
            return new Config(emptyList());
        }

        @Override
        protected void dynamicUpdateBrokerConfig(NodeRef nodeRef, Admin ac, KafkaBrokerConfigurationDiff configurationDiff, KafkaBrokerLoggingConfigurationDiff logDiff) throws ForceableProblem {
            ForceableProblem problem = alterConfigsException.apply(nodeRef.nodeId());
            if (problem != null) {
                throw problem;
            }
        }

        @Override
        protected Future<Void> restart(Pod pod, RestartContext restartContext) {
            restarted.add(pod.getMetadata().getName());
            return succeededFuture();
        }

        @Override
        protected void tcpProbe(String hostname, int port) throws IOException {
            tcpProbes.add(hostname + ":" + port);
            throw new IOException("mock probe failure");
        }
    }

    // TODO Error when finding the next broker
}
