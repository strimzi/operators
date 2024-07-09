/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.topic.TopicOperatorConfig;
import io.strimzi.operator.topic.TopicOperatorUtil;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient.TaskState;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.ReconcilableTopic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.ONGOING;
import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.PENDING;
import static io.strimzi.operator.topic.TopicOperatorUtil.hasReplicasChange;
import static io.strimzi.operator.topic.TopicOperatorUtil.topicNames;
import static java.lang.String.format;
import static org.apache.logging.log4j.core.util.Throwables.getRootCause;

/**
 * Handler for Cruise Control requests.
 */
public class CruiseControlHandler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CruiseControlHandler.class);
    
    private final TopicOperatorConfig config;
    private final TopicOperatorMetricsHolder metricsHolder;
    private final CruiseControlClient cruiseControlClient;

    /**
     * Create a new instance.
     *
     * @param config Topic Operator configuration.
     * @param metricsHolder Metrics holder.
     * @param cruiseControlClient Cruise Control client.
     */
    public CruiseControlHandler(TopicOperatorConfig config,
                                TopicOperatorMetricsHolder metricsHolder,
                                CruiseControlClient cruiseControlClient) {
        this.config = config;
        this.metricsHolder = metricsHolder;
        this.cruiseControlClient = cruiseControlClient;
    }

    /**
     * Send a topic_configuration request to create a task for replication factor change of one or more topics.
     * This should be called when one or more .spec.replicas changes are detected.
     * Note that this method also updates the KafkaTopic status.
     * 
     * @param reconcilableTopics Pending replicas changes.
     * @return Replicas changes with status update.
     */
    public List<ReconcilableTopic> requestPendingChanges(List<ReconcilableTopic> reconcilableTopics) {
        List<ReconcilableTopic> result = new ArrayList<>();
        if (reconcilableTopics.isEmpty()) {
            return result;
        }
        updateToPending(reconcilableTopics, "Replicas change pending");
        result.addAll(reconcilableTopics);

        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        try {
            LOGGER.debugOp("Sending topic configuration request, topics {}", topicNames(reconcilableTopics));
            var kafkaTopics = reconcilableTopics.stream().map(ReconcilableTopic::kt).collect(Collectors.toList());
            var userTaskId = cruiseControlClient.topicConfiguration(kafkaTopics);
            updateToOngoing(result, "Replicas change ongoing", userTaskId);
        } catch (Throwable t) {
            updateToFailed(result, format("Replicas change failed, %s", getRootCause(t).getMessage()));
        }
        TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::cruiseControlTopicConfig, config.enableAdditionalMetrics(), config.namespace());
        return result;
    }

    /**
     * Send a user_tasks request to check the state of ongoing replication factor changes.
     * This should be called periodically to update the active tasks cache and KafkaTopic status.
     * Note that this method also updates the KafkaTopic status.
     *
     * @param reconcilableTopics Ongoing replicas changes.
     * @return Replicas changes with status update.
     */
    public List<ReconcilableTopic> requestOngoingChanges(List<ReconcilableTopic> reconcilableTopics) {
        List<ReconcilableTopic> result = new ArrayList<>();
        if (reconcilableTopics.isEmpty()) {
            return result;
        }
        result.addAll(reconcilableTopics);
        
        var groupByUserTaskId = reconcilableTopics.stream()
            .filter(rt -> hasReplicasChange(rt.kt().getStatus()) && rt.kt().getStatus().getReplicasChange().getSessionId() != null)
            .map(rt -> new ReconcilableTopic(new Reconciliation("", KafkaTopic.RESOURCE_KIND, "", ""), rt.kt(), rt.topicName()))
            .collect(Collectors.groupingBy(rt -> rt.kt().getStatus().getReplicasChange().getSessionId(), HashMap::new, Collectors.toList()));

        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        try {
            LOGGER.debugOp("Sending user tasks request, Tasks {}", groupByUserTaskId.keySet());
            var userTasksResponse = cruiseControlClient.userTasks(groupByUserTaskId.keySet());
            if (userTasksResponse.userTasks().isEmpty()) {
                // Cruise Control restarted: reset the state because the tasks queue is not persisted
                // this may also happen when the tasks' retention time expires, or the cache becomes full
                updateToPending(result, "Task not found, Resetting the state");
            } else {
                for (var userTask : userTasksResponse.userTasks()) {
                    String userTaskId = userTask.userTaskId();
                    TaskState state = TaskState.get(userTask.status());
                    switch (state) {
                        case COMPLETED:
                            updateToCompleted(groupByUserTaskId.get(userTaskId), "Replicas change completed");
                            break;
                        case COMPLETED_WITH_ERROR:
                            updateToFailed(groupByUserTaskId.get(userTaskId), "Replicas change completed with error");
                            break;
                        case ACTIVE:
                        case IN_EXECUTION:
                            // do nothing
                            break;
                    }
                }
            }
        } catch (Throwable t) {
            updateToFailed(result, format("Replicas change failed, %s", getRootCause(t).getMessage()));
        }
        TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::cruiseControlUserTasks, config.enableAdditionalMetrics(), config.namespace());
        return result;
    }
    
    private void updateToPending(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                    .withState(PENDING).withTargetReplicas(reconcilableTopic.kt().getSpec().getReplicas()).build()).build()));
    }
    
    private void updateToOngoing(List<ReconcilableTopic> reconcilableTopics, String message, String userTaskId) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .editOrNewReplicasChange().withState(ONGOING).withSessionId(userTaskId).endReplicasChange().build()));
    }
    
    private void updateToCompleted(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withReplicasChange(null).build()));
    }
    
    private void updateToFailed(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.errorOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .editOrNewReplicasChange().withMessage(message).endReplicasChange().build()));
    }
}
