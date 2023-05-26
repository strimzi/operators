/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.UncheckedExecutionException;
import io.strimzi.operator.common.UncheckedInterruptedException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

class RollClientImpl implements RollClient {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(RackRolling.class);
    private final static int ADMIN_BATCH_SIZE = 200;
    // TODO: set to the same value of the thread blocking limit for now but we need to decide whether we need a dedicated thread for RackRolling
    private final static long ADMIN_CALL_TIMEOUT = 2000L;
    private Admin brokerAdmin = null;

    private Admin controllerAdmin = null;

    private final PemAuthIdentity pemAuthIdentity;

    private final PemTrustSet pemTrustSet;

    private final Reconciliation reconciliation;

    private final AdminClientProvider adminClientProvider;
    RollClientImpl(Reconciliation reconciliation,
                   TlsPemIdentity coTlsPemIdentity,
                   AdminClientProvider adminClientProvider) {
        this.pemTrustSet = coTlsPemIdentity.pemTrustSet();
        this.pemAuthIdentity = coTlsPemIdentity.pemAuthIdentity();
        this.reconciliation = reconciliation;
        this.adminClientProvider = adminClientProvider;
    }

    /** Return a future that completes when all the given futures complete */
    @SuppressWarnings("rawtypes")
    private static CompletableFuture<Void> allOf(List<? extends CompletableFuture<?>> futures) {
        CompletableFuture[] ts = futures.toArray(new CompletableFuture[0]);
        return CompletableFuture.allOf(ts);
    }

    /** Splits the given {@code items} into batches no larger than {@code maxBatchSize}. */
    private static <T> Set<List<T>> batch(List<T> items, int maxBatchSize) {
        Set<List<T>> allBatches = new HashSet<>();
        List<T> currentBatch = null;
        for (var topicId : items) {
            if (currentBatch == null || currentBatch.size() > maxBatchSize) {
                currentBatch = new ArrayList<>();
                allBatches.add(currentBatch);
            }
            currentBatch.add(topicId);
        }
        return allBatches;
    }

    @Override
    public void initialiseBrokerAdmin(Set<NodeRef> brokerNodes) {
        if (this.brokerAdmin == null) this.brokerAdmin = createBrokerAdminClient(brokerNodes);
    }

    @Override
    public void initialiseControllerAdmin(Set<NodeRef> controllerNodes) {
        if (this.controllerAdmin == null) this.controllerAdmin = createControllerAdminClient(controllerNodes);
    }

    @Override
    public void closeControllerAdminClient() {
        if (this.controllerAdmin != null) {
            this.controllerAdmin.close(Duration.ofSeconds(30));
        }
    }

    @Override
    public void closeBrokerAdminClient() {
        if (this.brokerAdmin != null) {
            this.brokerAdmin.close(Duration.ofSeconds(30));
        }
    }

    @Override
    public boolean canConnectToNode(NodeRef nodeRef, boolean controller) {
        try (Admin ignored = controller ? createControllerAdminClient(Collections.singleton(nodeRef)) : createBrokerAdminClient(Collections.singleton(nodeRef))) {
            return true;
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Cannot create an admin client connection to {}", nodeRef, e);
            return false;
        }
    }

    private Admin createControllerAdminClient(Set<NodeRef> controllerNodes) {
        String bootstrapHostnames = controllerNodes.stream().map(node -> DnsNameGenerator.podDnsName(reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), node.podName()) + ":" + KafkaCluster.CONTROLPLANE_PORT).collect(Collectors.joining(","));
        LOGGER.debugCr(reconciliation, "Creating an admin client with {}", bootstrapHostnames);
        try {
            return adminClientProvider.createControllerAdminClient(bootstrapHostnames, pemTrustSet, pemAuthIdentity);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create controller admin client", e.getCause());
        }
    }

    private Admin createBrokerAdminClient(Set<NodeRef> brokerNodes) {
        String bootstrapHostnames = brokerNodes.stream().map(node -> DnsNameGenerator.podDnsName(reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), node.podName()) + ":" + KafkaCluster.REPLICATION_PORT).collect(Collectors.joining(","));
        LOGGER.debugCr(reconciliation, "Creating an admin client with {}", bootstrapHostnames);
        try {
            return adminClientProvider.createAdminClient(bootstrapHostnames, pemTrustSet, pemAuthIdentity);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create broker admin client", e.getCause());
        }
    }

    @Override
    public Collection<TopicListing> listTopics() {
        try {
            return brokerAdmin.listTopics(new ListTopicsOptions().listInternal(true)).listings().get(ADMIN_CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<TopicDescription> describeTopics(List<Uuid> topicIds) {
        try {
            var topicIdBatches = batch(topicIds, ADMIN_BATCH_SIZE);
            var futures = new ArrayList<CompletableFuture<Map<Uuid, TopicDescription>>>();
            for (var topicIdBatch : topicIdBatches) {
                var mapKafkaFuture = brokerAdmin.describeTopics(TopicCollection.ofTopicIds(topicIdBatch)).allTopicIds().toCompletionStage().toCompletableFuture();
                futures.add(mapKafkaFuture);
            }
            allOf(futures).get(ADMIN_CALL_TIMEOUT, TimeUnit.MILLISECONDS);
            var topicDescriptions = futures.stream().flatMap(cf -> {
                try {
                    return cf.get().values().stream();
                } catch (InterruptedException e) {
                    throw new UncheckedInterruptedException(e);
                } catch (ExecutionException e) {
                    throw new UncheckedExecutionException(e);
                }
            });
            return topicDescriptions.toList();
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Integer, Long> quorumLastCaughtUpTimestamps(Set<NodeRef> activeControllerNodeRef) {
        // TODO: this should be changed to controllerAdmin when once KAFKA-18230 is completed and supported in Strimzi
        //  in which case, this method does not need the parameter, activeControllerNodeRef

        try (Admin admin = createControllerAdminClient(activeControllerNodeRef)) {
            DescribeMetadataQuorumResult dmqr = admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions());
            return dmqr.quorumInfo().get().voters().stream().collect(Collectors.toMap(
                    QuorumInfo.ReplicaState::replicaId,
                    state -> state.lastCaughtUpTimestamp().orElse(-1)));
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public int activeController() {
        try {
            return controllerAdmin.describeCluster().controller().get().id();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Integer> describeTopicMinIsrs(List<String> topicNames) {
        try {
            var topicIdBatches = batch(topicNames, ADMIN_BATCH_SIZE);
            var futures = new ArrayList<CompletableFuture<Map<ConfigResource, Config>>>();
            for (var topicIdBatch : topicIdBatches) {
                var mapKafkaFuture = brokerAdmin.describeConfigs(topicIdBatch.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).collect(Collectors.toSet())).all().toCompletionStage().toCompletableFuture();
                futures.add(mapKafkaFuture);
            }
            allOf(futures).get(ADMIN_CALL_TIMEOUT, TimeUnit.MILLISECONDS);
            var topicDescriptions = futures.stream().flatMap(cf -> {
                try {
                    return cf.get().entrySet().stream();
                } catch (InterruptedException e) {
                    throw new UncheckedInterruptedException(e);
                } catch (ExecutionException e) {
                    throw new UncheckedExecutionException(e);
                }
            });
            return topicDescriptions.collect(Collectors.toMap(
                    entry -> entry.getKey().name(),
                    entry -> Integer.parseInt(entry.getValue().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value())));
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reconfigureNode(NodeRef nodeRef, KafkaBrokerConfigurationDiff kafkaBrokerConfigurationDiff, KafkaBrokerLoggingConfigurationDiff kafkaBrokerLoggingConfigurationDiff) {
        Map<ConfigResource, Collection<AlterConfigOp>> updatedConfig = new HashMap<>(2);
        updatedConfig.put(Util.getBrokersConfig(nodeRef.nodeId()), kafkaBrokerConfigurationDiff.getConfigDiff());
        updatedConfig.put(Util.getBrokersLogging(nodeRef.nodeId()), kafkaBrokerLoggingConfigurationDiff.getLoggingDiff());

        AlterConfigsResult alterConfigResult = brokerAdmin.incrementalAlterConfigs(updatedConfig, new AlterConfigsOptions().timeoutMs(2000));
        KafkaFuture<Void> brokerConfigFuture = alterConfigResult.values().get(Util.getBrokersConfig(nodeRef.nodeId()));
        KafkaFuture<Void> brokerLoggingConfigFuture = alterConfigResult.values().get(Util.getBrokersLogging(nodeRef.nodeId()));

        try {
            brokerConfigFuture.get();
            brokerLoggingConfigFuture.get();
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public int tryElectAllPreferredLeaders(NodeRef nodeRef) {
        // If brokerAdmin has not been initialised yet, create an admin with the given node
        // this could happen, if there is any not_running nodes in the new reconciliation (not_running nodes get restarted before initialising an admin client)
        try (Admin admin = brokerAdmin == null ? createBrokerAdminClient(Collections.singleton(nodeRef)) : brokerAdmin) {
            // find all partitions where the node is the preferred leader
            // we could do listTopics then describe all the topics, but that would scale poorly with number of topics
            // using describe log dirs should be more efficient
            var topicsOnNode = admin.describeLogDirs(List.of(nodeRef.nodeId())).allDescriptions().get()
                    .getOrDefault(nodeRef, Map.of()).values().stream()
                    .flatMap(x -> x.replicaInfos().keySet().stream())
                    .map(TopicPartition::topic)
                    .collect(Collectors.toSet());

            var topicDescriptionsOnNode = admin.describeTopics(topicsOnNode).allTopicNames().get(ADMIN_CALL_TIMEOUT, TimeUnit.MILLISECONDS).values();
            var toElect = new HashSet<TopicPartition>();
            for (TopicDescription td : topicDescriptionsOnNode) {
                for (TopicPartitionInfo topicPartitionInfo : td.partitions()) {
                    if (!topicPartitionInfo.replicas().isEmpty()
                            && topicPartitionInfo.replicas().get(0).id() == nodeRef.nodeId() // this node is preferred leader
                            && topicPartitionInfo.leader().id() != nodeRef.nodeId()) { // this onde is not current leader
                        toElect.add(new TopicPartition(td.name(), topicPartitionInfo.partition()));
                    }
                }
            }

            var electionResults = admin.electLeaders(ElectionType.PREFERRED, toElect).partitions().get();

            long count = electionResults.values().stream()
                    .filter(Optional::isPresent)
                    .count();
            return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Integer, Configs> describeBrokerConfigs(List<NodeRef> toList) {
        return describeNodeConfigs(brokerAdmin, toList);
    }

    @Override
    public Map<Integer, Configs> describeControllerConfigs(List<NodeRef> toList) {
        return describeNodeConfigs(controllerAdmin, toList);
    }

    private Map<Integer, Configs> describeNodeConfigs(Admin admin, List<NodeRef> toList) {
        try {
            var dc = admin.describeConfigs(toList.stream().map(nodeRef -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(nodeRef.nodeId()))).toList());
            var result = dc.all().get(ADMIN_CALL_TIMEOUT, TimeUnit.MILLISECONDS);

            return toList.stream().collect(Collectors.toMap(NodeRef::nodeId,
                    nodeRef -> new Configs(result.get(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(nodeRef.nodeId()))),
                            result.get(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, String.valueOf(nodeRef.nodeId())))
                            )));
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
