/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class BatchingTest {

    @Test
    public void testCellCreation() {
        List<Set<KafkaNode>> cells = Batching.cells(Reconciliation.DUMMY_RECONCILIATION,
                Set.of(addKafkaNode(0, addReplicas(0, "my-topic", 0, 1, 0)),
                        addKafkaNode(1, Set.of()),
                        addKafkaNode(2, addReplicas(2, "my-topic", 0, 1, 2))));

        assertEquals(cells.size(), 2);
        assertEquals("[KafkaNode[id=0, replicas=[my-topic-0]], KafkaNode[id=1, replicas=[]]]", cells.get(0).toString());
        assertEquals("[KafkaNode[id=2, replicas=[my-topic-0]]]", cells.get(1).toString());
    }

    @Test
    public void testBatchCreationWithSingleTopic() {
        Set<KafkaNode> kafkaNodes = Set.of(addKafkaNode(0, addReplicas(0, "my-topic", 0, 1, 0)),
                addKafkaNode(1, Set.of()),
                addKafkaNode(2, addReplicas(2, "my-topic", 0, 1, 2)));
        List<Set<KafkaNode>> cells = Batching.cells(Reconciliation.DUMMY_RECONCILIATION, kafkaNodes);

        Availability availability = mock(Availability.class);
        doReturn(true).when(availability).anyPartitionWouldBeUnderReplicated(0);
        doReturn(true).when(availability).anyPartitionWouldBeUnderReplicated(2);

        List<Set<Integer>> batch = Batching.batchCells(Reconciliation.DUMMY_RECONCILIATION, cells, availability, 2);

        assertEquals(batch.size(), 1);
        assertEquals("[1]", batch.get(0).toString());
    }

    @Test
    public void testBatchCreationWithMultipleTopics() {

        List<Set<KafkaNode>> cells = Batching.cells(Reconciliation.DUMMY_RECONCILIATION,
                Set.of(addKafkaNode(0, addReplicas(0, "my-topic", 0, 0, 3)),
                        addKafkaNode(3, addReplicas(3, "my-topic", 0, 0, 3)),
                        addKafkaNode(1, addReplicas(1, "my-topic-1", 1, 1, 2)),
                        addKafkaNode(2, addReplicas(2, "my-topic-1", 1, 1, 2))));

        Availability availability = mock(Availability.class);
        doReturn(true).when(availability).anyPartitionWouldBeUnderReplicated(0);
        doReturn(true).when(availability).anyPartitionWouldBeUnderReplicated(1);
        doReturn(true).when(availability).anyPartitionWouldBeUnderReplicated(2);

        List<Set<Integer>> batch = Batching.batchCells(Reconciliation.DUMMY_RECONCILIATION, cells, availability, 2);


        System.out.println(batch);
        assertEquals(batch.size(), 1);
        assertEquals("[3]", batch.get(0).toString());
    }

    static public Set<Replica> addReplicas(int brokerid, String topicName, int partition, int... isrIds) {
        Set<Replica> replicaSet = new HashSet<>();

        replicaSet.add(new Replica(addNodes(brokerid).get(0), topicName, partition, addNodes(isrIds)));

        return replicaSet;
    }

    static List<Node> addNodes(int... isrIds) {
        List<Node> nodes = new ArrayList<>();

        for (int id : isrIds) {
            nodes.add(new Node(id, "pool-kafka-" + id, 9092));
        }

        return nodes;
    }

    static public KafkaNode addKafkaNode(int nodeId, Set<Replica> replicas) {
        return new KafkaNode(nodeId, replicas);
    }
}
