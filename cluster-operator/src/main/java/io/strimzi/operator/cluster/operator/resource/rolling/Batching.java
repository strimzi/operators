/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Deal with the batching mechanism for the new Kafka roller
 */
public class Batching {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Batching.class);

    /**
     * Partition the given {@code brokers}
     * into cells that can be rolled in parallel because they
     * contain no replicas in common.
     */
    static List<Set<KafkaNode>> cells(Reconciliation reconciliation,
                                      Collection<KafkaNode> brokers) {

        // find brokers that are individually rollable
        var rollable = new LinkedHashSet<>(brokers);
        // partition the set under the equivalence relation "shares a partition with"
        Set<Set<KafkaNode>> disjoint = partitionByHasAnyReplicasInCommon(reconciliation, rollable);
        // disjoint cannot be empty, because rollable isn't empty, and disjoint is a partitioning or rollable
        // We find the biggest set of brokers which can parallel-rolled
        return disjoint.stream().sorted(Comparator.<Set<?>>comparingInt(Set::size).reversed()).toList();
    }

    private static Set<Set<KafkaNode>> partitionByHasAnyReplicasInCommon(Reconciliation reconciliation, Set<KafkaNode> rollable) {
        Set<Set<KafkaNode>> disjoint = new HashSet<>();
        for (var node : rollable) {
            var nodeReplicas = node.replicas();
            Set<Set<KafkaNode>> merge = new HashSet<>();
            for (Set<KafkaNode> cell : disjoint) {
                if (!containsAny(reconciliation, node, nodeReplicas, cell)) {
                    LOGGER.debugCr(reconciliation, "Add {} to {{}}", node.id(), idsOf(cell));
                    merge.add(cell);
                    merge.add(Set.of(node));
                    // problem is here, we're iterating over all cells (ones which we've decided should be disjoint)
                    // and we merged them in violation of that
                    // we could break here at the end of the if block (which would be correct)
                    // but it might not be optimal (in the sense of forming large cells)
                    break;
                }
            }
            if (merge.isEmpty()) {
                LOGGER.debugCr(reconciliation, "New cell: {{}}", node.id());
                disjoint.add(Set.of(node));
            } else {
                LOGGER.debugCr(reconciliation, "Merge {}", idsOf2(merge));
                for (Set<KafkaNode> r : merge) {
                    LOGGER.debugCr(reconciliation, "Remove cell: {}", idsOf(r));
                    disjoint.remove(r);
                }
                Set<KafkaNode> newCell = union(merge);
                LOGGER.debugCr(reconciliation, "New cell: {{}}", idsOf(newCell));
                disjoint.add(newCell);
            }
            LOGGER.debugCr(reconciliation, "Disjoint cells now: {}", idsOf2(disjoint));
        }
        return disjoint;
    }

    /**
     * Split the given cells into batches,
     * taking account of {@code acks=all} availability and the given maxBatchSize
     */
    static List<Set<Integer>> batchCells(Reconciliation reconciliation,
                                           List<Set<KafkaNode>> cells,
                                           Availability availability,
                                           int maxBatchSize) {
        List<Set<Integer>> result = new ArrayList<>();
        Set<Integer> unavail = new HashSet<>();
        for (var cell : cells) {
            List<Set<Integer>> availBatches = new ArrayList<>();
            for (var kafkaNode : cell) {
                if (!availability.anyPartitionWouldBeUnderReplicated(kafkaNode.id())) {
                    LOGGER.debugCr(reconciliation, "No replicas of node {} will be unavailable => add to batch",
                            kafkaNode.id());
                    var currentBatch = availBatches.isEmpty() ? null : availBatches.get(availBatches.size() - 1);
                    if (currentBatch == null || currentBatch.size() >= maxBatchSize) {
                        currentBatch = new HashSet<>();
                        availBatches.add(currentBatch);
                    }
                    currentBatch.add(kafkaNode.id());
                } else {
                    LOGGER.debugCr(reconciliation, "Some replicas of node {} will be unavailable => do not add to batch", kafkaNode.id());
                    unavail.add(kafkaNode.id());
                }
            }
            result.addAll(availBatches);
        }
        if (result.isEmpty() && !unavail.isEmpty()) {
            LOGGER.warnCr(reconciliation, "Cannot restart nodes {} without violating some topics' min.in.sync.replicas", nodeIdsToString(unavail));
        }
        return result;
    }

    static <T> T elementInIntersection(Set<T> set, Set<T> set2) {
        for (T t : set) {
            if (set2.contains(t)) {
                return t;
            }
        }
        return null;
    }

    static boolean containsAny(Reconciliation reconciliation,
                               KafkaNode node,
                               Set<Replica> nodeReplicas,
                               Set<KafkaNode> cell) {
        for (var b : cell) {
            var commonReplica = elementInIntersection(b.replicas(), nodeReplicas);
            if (commonReplica != null) {
                LOGGER.debugCr(reconciliation, "Nodes {} and {} have at least {} in common",
                        node.id(), b.id(), commonReplica);
                return true;
            }
        }
        LOGGER.debugCr(reconciliation, "Node {} has no replicas in common with any of the nodes in {}",
                node.id(), idsOf(cell));
        return false;
    }

    private static String idsOf(Collection<KafkaNode> cell) {
        return cell.stream()
                .map(kafkaNode -> Integer.toString(kafkaNode.id()))
                .collect(Collectors.joining(",", "{", "}"));
    }

    private static String idsOf2(Collection<? extends Collection<KafkaNode>> merge) {
        return merge.stream()
                .map(Batching::idsOf)
                .collect(Collectors.joining(",", "{", "}"));
    }

    /** Returns a new set that is the union of each of the sets in the given {@code merge}. I.e. flatten without duplicates. */
    private static <T> Set<T> union(Set<Set<T>> merge) {
        HashSet<T> result = new HashSet<>();
        for (var x : merge) {
            result.addAll(x);
        }
        return result;
    }


    static String nodeIdsToString2(Collection<? extends Collection<Integer>> merge) {
        return merge.stream()
                .map(Batching::nodeIdsToString)
                .collect(Collectors.joining(",", "{", "}"));
    }

    static String nodeIdsToString(Collection<Integer> cell) {
        return cell.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(",", "{", "}"));
    }

    /**
     * Pick the "best" batch to be restarted.
     * This is the largest batch of available servers
     * @return the "best" batch to be restarted
     */
    static Set<Integer> pickBestBatchForRestart(List<Set<Integer>> batches) {
        if (batches.size() < 1) {
            return Set.of();
        }

        // if none of the batches contain more than 1 node, return a set with unready node if there is any, otherwise the first set.
        if (batches.stream().filter(set -> set.size() > 1).count() < 1) {
            return batches.iterator().next();
        }

        var sorted = batches.stream().sorted(Comparator.comparing(Set::size)).toList();
        return sorted.get(sorted.size() - 1);
    }



}
