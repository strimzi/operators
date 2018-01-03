/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Controller {

    private final static Logger logger = LoggerFactory.getLogger(Controller.class);
    private final static Logger eventLogger = LoggerFactory.getLogger("Event");
    private final Kafka kafka;
    private final K8s k8s;
    private final Vertx vertx;
    private final LabelPredicate cmPredicate;
    private TopicStore topicStore;
    private final InFlight inFlight = new InFlight();

    public class ErrorEvent implements Handler<Void> {

        private final String message;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        public ErrorEvent(ControllerException exception, Handler<AsyncResult<Void>> handler) {
            this.involvedObject = exception.getInvolvedObject();
            this.message = exception.getMessage();
            this.handler = handler;
        }

        public ErrorEvent(HasMetadata involvedObject, String message, Handler<AsyncResult<Void>> handler) {
            this.involvedObject = involvedObject;
            this.message = message;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) {
            String myHost = "";
            EventBuilder evtb = new EventBuilder().withApiVersion("v1");
            if (involvedObject != null) {
                evtb.withNewInvolvedObject()
                        .withKind(involvedObject.getKind())
                        .withName(involvedObject.getMetadata().getName())
                        .withApiVersion(involvedObject.getApiVersion())
                        .withNamespace(involvedObject.getMetadata().getNamespace())
                        .withUid(involvedObject.getMetadata().getUid())
                        .endInvolvedObject();
            }
            evtb.withType("Warning")
                    .withMessage(this.getClass().getSimpleName() + " failed: " + message)
                    //.withReason("")
                    .withNewSource()
                    .withHost(myHost)
                    .withComponent(Controller.class.getName())
                    .endSource();
            Event event = evtb.build();
            k8s.createEvent(event, handler);
            eventLogger.warn(event.toString());
        }

        public String toString() {
            return "ErrorEvent(involvedObject="+involvedObject+", message="+message+")";
        }
    }


    /** Topic created in ZK */
    public class CreateConfigMap implements Handler<Void> {
        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public CreateConfigMap(Topic topic, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            ConfigMap cm = TopicSerialization.toConfigMap(topic, cmPredicate);
            inFlight.startCreatingConfigMap(cm);
            k8s.createConfigMap(cm, handler);
        }

        @Override
        public String toString() {
            return "CreateConfigMap(topicName="+topic.getTopicName()+")";
        }
    }

    /** Topic deleted in ZK */
    public class DeleteConfigMap implements Handler<Void> {

        private final TopicName topicName;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;

        public DeleteConfigMap(TopicName topicName, Handler<io.vertx.core.AsyncResult<Void>> handler) {
            this.topicName = topicName;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) {
            inFlight.startDeletingConfigMap(topicName);
            k8s.deleteConfigMap(topicName, handler);
        }

        @Override
        public String toString() {
            return "DeleteConfigMap(topicName="+topicName+")";
        }
    }

    /** Topic config modified in ZK */
    public class UpdateConfigMap implements Handler<Void> {

        private final Topic topic;
        private final Handler<io.vertx.core.AsyncResult<Void>> handler;
        private final HasMetadata involvedObject;

        public UpdateConfigMap(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
            this.involvedObject = involvedObject;
        }

        @Override
        public void handle(Void v) {
            ConfigMap cm = TopicSerialization.toConfigMap(topic, cmPredicate);
            inFlight.startUpdatingConfigMap(cm);
            k8s.updateConfigMap(cm, handler);
        }

        @Override
        public String toString() {
            return "UpdateConfigMap(topicName="+topic.getTopicName()+")";
        }
    }

    /** ConfigMap created in k8s */
    public class CreateKafkaTopic implements Handler<Void> {

        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        public CreateKafkaTopic(Topic topic,
                                HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.handler = handler;
            this.involvedObject = involvedObject;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            inFlight.startCreatingTopic(topic.getTopicName());
            kafka.createTopic(topic, ar -> {
                if (ar.succeeded()) {
                    logger.info("Created topic '{}' for ConfigMap '{}'", topic.getTopicName(), topic.getMapName());
                    handler.handle(ar);
                } else {
                    handler.handle(ar);
                    if (ar.cause() instanceof TopicExistsException) {
                        // TODO reconcile
                    } else {
                        throw new ControllerException(involvedObject, ar.cause());
                    }
                }
            });
        }

        @Override
        public String toString() {
            return "CreateKafkaTopic(topicName="+ topic.getTopicName()+")";
        }
    }

    /** ConfigMap modified in k8s */
    public class UpdateKafkaConfig implements Handler<Void> {

        private final HasMetadata involvedObject;

        private final Topic topic;
        private final Handler<AsyncResult<Void>> handler;

        public UpdateKafkaConfig(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            kafka.updateTopicConfig(topic, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString(), eventResult -> {}));
                }
                handler.handle(ar);
            });

        }

        @Override
        public String toString() {
            return "UpdateKafkaConfig(topicName="+topic.getTopicName()+")";
        }
    }

    /** ConfigMap modified in k8s */
    public class IncreaseKafkaPartitions implements Handler<Void> {

        private final HasMetadata involvedObject;

        private final Topic topic;
        private final Handler<AsyncResult<Void>> handler;

        public IncreaseKafkaPartitions(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            kafka.increasePartitions(topic, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString(), eventResult -> {}));
                }
                handler.handle(ar);
            });

        }

        @Override
        public String toString() {
            return "UpdateKafkaPartitions(topicName="+topic.getTopicName()+")";
        }
    }

    /** ConfigMap modified in k8s */
    public class ChangeReplicationFactor implements Handler<Void> {

        private final HasMetadata involvedObject;

        private final Topic topic;
        private final Handler<AsyncResult<Void>> handler;

        public ChangeReplicationFactor(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            kafka.changeReplicationFactor(topic, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString(), eventResult -> {}));
                }
                handler.handle(ar);
            });

        }

        @Override
        public String toString() {
            return "ChangeReplicationFactor(topicName="+topic.getTopicName()+")";
        }
    }

    /** ConfigMap deleted in k8s */
    public class DeleteKafkaTopic implements Handler<Void> {

        public final TopicName topicName;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        public DeleteKafkaTopic(TopicName topicName, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topicName = topicName;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            logger.info("Deleting topic '{}'", topicName);
            inFlight.startDeletingTopic(topicName);
            kafka.deleteTopic(topicName, handler);
        }

        @Override
        public String toString() {
            return "DeleteKafkaTopic(topicName="+topicName+")";
        }
    }

    public Controller(Vertx vertx, Kafka kafka,
                      K8s k8s,
                      LabelPredicate cmPredicate) {
        this.kafka = kafka;
        this.k8s = k8s;
        this.vertx = vertx;
        this.cmPredicate = cmPredicate;
    }

    public void setTopicStore(TopicStore topicStore) {
        this.topicStore = topicStore;
    }


    void reconcile(ConfigMap cm, TopicName topicName) {
        Topic k8sTopic = cm != null ? TopicSerialization.fromConfigMap(cm) : null;
        Future<Topic> topicResult = Future.future();
        Future<TopicMetadata> metadataResult = Future.future();
        kafka.topicMetadata(topicName, metadataResult.completer());
        topicStore.read(topicName, topicResult.completer());
        CompositeFuture.all(topicResult, metadataResult).setHandler(ar -> {
            Topic privateTopic = ar.result().resultAt(0);
            TopicMetadata kafkaTopicMeta = ar.result().resultAt(1);
            Topic kafkaTopic = TopicSerialization.fromTopicMetadata(kafkaTopicMeta);
            reconcile(cm, k8sTopic, kafkaTopic, privateTopic, reconcileResult -> {});
        });
    }

    /**
     * 0. Set up some persistent ZK nodes for us
     * 1. When updating CM, we also update our ZK nodes
     * 2. When updating Kafka, we also update our ZK nodes
     * 3. When reconciling we get all three versions of the Topic, k8s, kafka and privateState
     *   - If privateState doesn't exist:
     *     - If k8s doesn't exist, we reason it's been created in kafka and we create it k8s from kafka
     *     - If kafka doesn't exist, we reason it's been created in k8s, and we create it in kafka from k8s
     *     - If both exist, and are the same: That's fine
     *     - If both exist, and are different: We use whichever has the most recent mtime.
     *     - In all above cases we create privateState
     *   - If privateState does exist:
     *     - If k8s doesn't exist, we reason it was deleted, and delete kafka
     *     - If kafka doesn't exist, we reason it was delete and we delete k8s
     *     - If neither exists, we delete privateState.
     *     - If both exist then all three exist, and we need to reconcile:
     *       - We compute diff privateState->k8s and privateState->kafka and we merge the two
     *         - If there are conflicts => error
     *         - Otherwise we apply the apply the merged diff to privateState, and use that for both k8s and kafka
     *     - In all above cases we update privateState
     * Topic identification should be by uid/cxid, not by name.
     * Topic identification should be by uid/cxid, not by name.
     */
    void reconcile(HasMetadata involvedObject,
                   Topic k8sTopic, Topic kafkaTopic, Topic privateTopic, Handler<AsyncResult<Void>> reconciliationResultHandler) {
        if (privateTopic == null) {
            class CreateInTopicStoreHandler implements Handler<AsyncResult<Void>>  {

                private final Topic source;

                CreateInTopicStoreHandler(Topic source) {
                    this.source = source;
                }

                @Override
                public void handle(AsyncResult<Void> ar) {
                    // In all cases, create in privateState
                    if (ar.succeeded()) {
                        enqueue(new CreateInTopicStore(source, involvedObject, reconciliationResultHandler));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }
            }
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // All three null? This shouldn't be possible
                    logger.error("All three topics null during reconciliation. This should be impossible.");
                    return;
                } else {
                    // it's been created in Kafka => create in k8s and privateState
                    enqueue(new CreateConfigMap(kafkaTopic, new CreateInTopicStoreHandler(kafkaTopic)));

                }
            } else if (kafkaTopic == null) {
                // it's been created in k8s => create in Kafka and privateState
                enqueue(new CreateKafkaTopic(k8sTopic, involvedObject, new CreateInTopicStoreHandler(k8sTopic)));
            } else if (TopicDiff.diff(kafkaTopic, k8sTopic).isEmpty()) {
                // they're the same => do nothing
                logger.debug("k8s and kafka versions of topic '{}' are the same", kafkaTopic.getTopicName());
                enqueue(new CreateInTopicStore(kafkaTopic, involvedObject, reconciliationResultHandler));
            } else {
                // Just use kafka version, but also create a warning event
                enqueue(new ErrorEvent(involvedObject, "ConfigMap is incompatible with the topic metadata. " +
                        "The topic metadata will be treated as canonical.", ar -> {
                    if (ar.succeeded()) {
                        enqueue(new UpdateConfigMap(kafkaTopic, involvedObject, ar2 -> {
                            if (ar2.succeeded()) {
                                enqueue(new CreateInTopicStore(kafkaTopic, involvedObject, reconciliationResultHandler));
                            } else {
                                reconciliationResultHandler.handle(ar2);
                            }
                        }));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }));
            }
        } else {
            if (k8sTopic == null) {
                if (kafkaTopic == null) {
                    // delete privateState
                    enqueue(new DeleteFromTopicStore(privateTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                } else {
                    // it was deleted in k8s so delete in kafka and privateState
                    enqueue(new DeleteKafkaTopic(kafkaTopic.getTopicName(), involvedObject, ar -> {
                        if (ar.succeeded()) {
                            enqueue(new DeleteFromTopicStore(kafkaTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                        } else {
                            reconciliationResultHandler.handle(ar);
                        }
                    }));

                }
            } else if (kafkaTopic == null) {
                // it was deleted in kafka so delete in k8s and privateState
                enqueue(new DeleteConfigMap(k8sTopic.getTopicName(), ar -> {
                    if (ar.succeeded()) {
                        enqueue(new DeleteFromTopicStore(k8sTopic.getTopicName(), involvedObject, reconciliationResultHandler));
                    } else {
                        reconciliationResultHandler.handle(ar);
                    }
                }));
            } else {
                // all three exist
                update3Way(involvedObject, k8sTopic, kafkaTopic, privateTopic, reconciliationResultHandler);
            }
        }
    }

    private void update3Way(HasMetadata involvedObject, Topic k8sTopic, Topic kafkaTopic, Topic privateTopic, Handler<AsyncResult<Void>> reconciliationResultHandler) {
        TopicDiff oursKafka = TopicDiff.diff(privateTopic, kafkaTopic);
        TopicDiff oursK8s = TopicDiff.diff(privateTopic, k8sTopic);
        String conflict = oursKafka.conflict(oursK8s);
        if (conflict != null) {
            final String message = "ConfigMap and Topic both changed in a conflicting way: " + conflict;
            enqueue(new ErrorEvent(involvedObject, message, eventResult -> {}));
            reconciliationResultHandler.handle(Future.failedFuture(new Exception(message)));
        } else {
            TopicDiff merged = oursKafka.merge(oursK8s);
            Topic result = merged.apply(privateTopic);
            int partitionsDelta = merged.numPartitionsDelta();
            if (partitionsDelta < 0) {
                final String message = "Number of partitions cannot be decreased";
                enqueue(new ErrorEvent(involvedObject, message, eventResult -> {}));
                reconciliationResultHandler.handle(Future.failedFuture(new Exception(message)));
            } else {

                if (merged.changesReplicationFactor()) {
                    enqueue(new ChangeReplicationFactor(result, involvedObject, null));
                }
                // TODO What if we increase min.in.sync.replicas and the number of replicas,
                // such that the old number of replicas < the new min isr? But likewise
                // we could decrease, so order of tasks in the queue will need to change
                // depending on what the diffs are.

                // TODO replace this with compose
                enqueue(new UpdateConfigMap(result, involvedObject, ar -> {
                    Handler<Void> topicStoreHandler =
                            ignored -> enqueue(new UpdateInTopicStore(
                                    result, involvedObject, reconciliationResultHandler));
                    Handler<Void> partitionsHandler;
                    if (partitionsDelta > 0) {
                        partitionsHandler = ar4 -> enqueue(new IncreaseKafkaPartitions(result, involvedObject, ar2 -> topicStoreHandler.handle(null)));
                    } else {
                        partitionsHandler = topicStoreHandler;
                    }
                    if (merged.changesConfig()) {
                        enqueue(new UpdateKafkaConfig(result, involvedObject, ar2 -> partitionsHandler.handle(null)));
                    } else {
                        enqueue(partitionsHandler);
                    }
                }));
            }
        }
    }

    void enqueue(Handler<Void> event) {
        logger.info("Enqueuing event {}", event);
        vertx.runOnContext(event);
    }

    /** Called when a topic znode is deleted in ZK */
    public void onTopicDeleted(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        // XXX currently runs on the ZK thread, requiring a synchronized `inFlight`
        // is it better to put this check in the topic deleted event?
        // that would require exposing an API to remove()
        if (inFlight.shouldProcessDelete(topicName)) {
            enqueue(new DeleteConfigMap(topicName, ar -> {
                if (ar.succeeded()) {
                    enqueue(new DeleteFromTopicStore(topicName, null, handler));
                } else {
                    handler.handle(ar);
                }
            }));
        } else {
            handler.handle(Future.succeededFuture());
        }
    }

    public void onTopicConfigChanged(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        if (inFlight.shouldProcessTopicConfigChange(topicName)) {
            kafka.topicMetadata(topicName, metadataResult -> {
                if (metadataResult.succeeded()) {
                    Topic topic = TopicSerialization.fromTopicMetadata(metadataResult.result());
                    enqueue(new UpdateConfigMap(topic, null, ar -> {
                        if (ar.succeeded()) {
                            enqueue(new UpdateInTopicStore(
                                    topic, null, resultHandler));
                        } else {
                            resultHandler.handle(ar);
                        }
                    }));
                } else {
                    resultHandler.handle(Future.failedFuture(metadataResult.cause()));
                }
            });
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
    }

    /** Called when a topic znode is created in ZK */
    public void onTopicCreated(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        // XXX currently runs on the ZK thread, requiring a synchronized inFlight
        // is it better to put this check in the topic deleted event?
        if (inFlight.shouldProcessTopicCreate(topicName)) {
            Handler<AsyncResult<TopicMetadata>> handler = new Handler<AsyncResult<TopicMetadata>>() {
                BackOff backOff = new BackOff();

                @Override
                public void handle(AsyncResult<TopicMetadata> metadataResult) {
                    if (metadataResult.failed()) {
                        if (metadataResult.cause() instanceof UnknownTopicOrPartitionException) {
                            // In this case it is most likely that we've been notified by ZK
                            // before Kafka has finished creating the topic, so we retry
                            // with exponential backoff.
                            long delay;
                            try {
                                delay = backOff.delayMs();
                            } catch (MaxAttemptsExceededException e) {
                                resultHandler.handle(Future.failedFuture(e));
                                return;
                            }
                            if (delay < 1) {
                                // vertx won't tolerate a zero delay
                                vertx.runOnContext(timerId -> kafka.topicMetadata(topicName, this));
                            } else {
                                vertx.setTimer(TimeUnit.MILLISECONDS.convert(delay, TimeUnit.MILLISECONDS),
                                        timerId -> kafka.topicMetadata(topicName, this));
                            }
                        } else {
                            resultHandler.handle(Future.failedFuture(metadataResult.cause()));
                        }
                    } else {
                        // We now have the metadata we need to create the
                        // ConfigMap...
                        Topic topic = TopicSerialization.fromTopicMetadata(metadataResult.result());
                        enqueue(new CreateConfigMap(topic, kubeResult -> {
                            if (kubeResult.succeeded()) {
                                enqueue(new CreateInTopicStore(topic, null, resultHandler));
                            } else {
                                resultHandler.handle(kubeResult);
                            }
                        }));
                    }
                }
            };
            kafka.topicMetadata(topicName, handler);
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
    }

    /** Called when a ConfigMap is added in k8s */
    public void onConfigMapAdded(ConfigMap configMap, Handler<AsyncResult<Void>> resultHandler) {
        if (cmPredicate.test(configMap)) {
            reconcileOnCmChange(configMap, TopicSerialization.fromConfigMap(configMap), resultHandler);
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
    }

    /** Called when a ConfigMap is modified in k8s */
    public void onConfigMapModified(ConfigMap configMap, Handler<AsyncResult<Void>> handler) {
        if (cmPredicate.test(configMap)) {
            reconcileOnCmChange(configMap, TopicSerialization.fromConfigMap(configMap), handler);
        } else {
            handler.handle(Future.succeededFuture());
        }
    }

    private void reconcileOnCmChange(ConfigMap configMap, Topic k8sTopic, Handler<AsyncResult<Void>> handler) {
        TopicName topicName = new TopicName(configMap);
        Future f1 = Future.future();
        Future f2 = Future.future();
        kafka.topicMetadata(topicName, f1.completer());
        topicStore.read(topicName, f2.completer());
        CompositeFuture.all(f1, f2).setHandler(ar -> {
            if (ar.succeeded()) {
                TopicMetadata topicMetadata = ar.result().resultAt(0);
                Topic kafkaTopic = TopicSerialization.fromTopicMetadata(topicMetadata);
                Topic privateTopic = ar.result().resultAt(1);
                reconcile(configMap, k8sTopic, kafkaTopic, privateTopic, handler);
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    /** Called when a ConfigMap is deleted in k8s */
    public void onConfigMapDeleted(ConfigMap configMap, Handler<AsyncResult<Void>> handler) {
        if (cmPredicate.test(configMap)) {
            reconcileOnCmChange(configMap, null, handler);
        } else {
            handler.handle(Future.succeededFuture());
        }
    }

    private class UpdateInTopicStore implements Handler<Void> {
        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        public UpdateInTopicStore(Topic topic, HasMetadata involvedObject, Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            topicStore.update(topic, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString(), eventResult -> {}));
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "UpdateInTopicStore(topicName="+topic.getTopicName()+")";
        }
    }

    class CreateInTopicStore implements Handler<Void> {
        private final Topic topic;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        private CreateInTopicStore(Topic topic, HasMetadata involvedObject,
                                   Handler<AsyncResult<Void>> handler) {
            this.topic = topic;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            topicStore.create(topic, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString(), eventResult -> {}));
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "CreateInTopicStore(topicName="+topic.getTopicName()+")";
        }
    }

    class DeleteFromTopicStore implements Handler<Void> {
        private final TopicName topicName;
        private final HasMetadata involvedObject;
        private final Handler<AsyncResult<Void>> handler;

        private DeleteFromTopicStore(TopicName topicName, HasMetadata involvedObject,
                                     Handler<AsyncResult<Void>> handler) {
            this.topicName = topicName;
            this.involvedObject = involvedObject;
            this.handler = handler;
        }

        @Override
        public void handle(Void v) throws ControllerException {
            topicStore.delete(topicName, ar-> {
                if (ar.failed()) {
                    enqueue(new ErrorEvent(involvedObject, ar.cause().toString(), eventResult -> {}));
                }
                handler.handle(ar);
            });
        }

        @Override
        public String toString() {
            return "DeleteFromTopicStore(topicName="+topicName+")";
        }
    }
}

