/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.zk;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link Zk}
 */
public class ZkImpl implements Zk {
    private final static Logger LOGGER = LogManager.getLogger(ZkImpl.class);
    private final WorkerExecutor workerExecutor;
    
    private final Vertx vertx;
    private final ZkClient zookeeper;

    // Only accessed on the vertx context.

    private final ConcurrentHashMap<String, IZkDataListener> dataWatches = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IZkChildListener> childWatches = new ConcurrentHashMap<>();

    protected ZkImpl(Vertx vertx, ZkClient zkClient) {
        this.vertx = vertx;
        this.workerExecutor = vertx.createSharedWorkerExecutor(getClass().getName(), 4);
        this.zookeeper = zkClient;
    }
    
    @Override
    public Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler) {
        workerExecutor.<Void>executeBlocking(
            () -> {
                zookeeper.create(path, data == null ? new byte[0] : data, acls, createMode);
                return null;
            }).onComplete(handler);
        return this;
    }

    @Override
    public Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler) {
        workerExecutor.<Void>executeBlocking(
            () -> {
                zookeeper.writeData(path, data, version);
                return null;
            }).onComplete(handler);
        return this;
    }

    @Override
    public Zk disconnect(Handler<AsyncResult<Void>> handler) {
        workerExecutor.<Void>executeBlocking(
            () -> {
                zookeeper.close();
                return null;
            }).onComplete(handler);
        return this;
    }

    @Override
    public Zk getData(String path, Handler<AsyncResult<byte[]>> handler) {
        workerExecutor.executeBlocking(
            () -> (byte[]) zookeeper.readData(path)).onComplete(handler);
        return this;
    }

    static class DataWatchAdapter implements IZkDataListener {

        private final Handler<AsyncResult<byte[]>> watcher;

        public DataWatchAdapter(Handler<AsyncResult<byte[]>> watcher) {
            this.watcher = watcher;
        }

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            watcher.handle(Future.succeededFuture((byte[]) data));
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {

        }
    }

    @Override
    public Future<Zk> watchData(String path, Handler<AsyncResult<byte[]>> watcher) {
        Promise<Zk> result = Promise.promise();
        workerExecutor.executeBlocking(
            () -> {
                IZkDataListener listener = new DataWatchAdapter(watcher);
                dataWatches.put(path, listener);
                zookeeper.subscribeDataChanges(path, listener);
                return null;
            }).onComplete(ar -> {
                LOGGER.trace("watchData");
                if (ar.succeeded()) {
                    result.complete(this);
                } else {
                    result.fail(ar.cause());
                }
            });
        return result.future();
    }

    @Override
    public Zk unwatchData(String path) {
        workerExecutor.executeBlocking(
            () -> {
                IZkDataListener listener = dataWatches.remove(path);
                if (listener != null) {
                    zookeeper.unsubscribeDataChanges(path, listener);
                }
                return null;
            }).onComplete(ignored -> LOGGER.trace("unwatchData"));
        return this;
    }

    @Override
    public Zk delete(String path, int version, Handler<AsyncResult<Void>> handler) {
        Promise<Void> result = Promise.promise();
        workerExecutor.executeBlocking(
            () -> {
                try {
                    if (zookeeper.delete(path, version)) {
                        result.complete();
                    } else {
                        result.fail(new ZkNoNodeException());
                    }
                } catch (Throwable t) {
                    result.fail(t);
                }
                return null;
            }).onComplete(ar -> handler.handle(result.future()));
        return this;
    }

    @Override
    public Zk children(String path, Handler<AsyncResult<List<String>>> handler) {
        workerExecutor.executeBlocking(
            () -> zookeeper.getChildren(path))
            .onComplete(handler);
        return this;
    }

    @Override
    public Future<Zk> watchChildren(String path, Handler<AsyncResult<List<String>>> watcher) {
        Promise<Zk> result = Promise.promise();
        workerExecutor.executeBlocking(
            () -> {
                IZkChildListener listener = (parentPath, currentChilds) -> watcher.handle(Future.succeededFuture(currentChilds));
                childWatches.put(path, listener);
                return zookeeper.subscribeChildChanges(path, listener);
            }).onComplete(ar -> {
                LOGGER.trace("watchChildren");
                if (ar.succeeded()) {
                    result.complete(this);
                } else {
                    result.fail(ar.cause());
                }
            });
        return result.future();
    }

    @Override
    public Zk unwatchChildren(String path) {
        workerExecutor.executeBlocking(
            () -> {
                IZkChildListener listener = childWatches.remove(path);
                if (listener != null) {
                    zookeeper.unsubscribeChildChanges(path, listener);
                }
                return null;
            }).onComplete(ignored -> LOGGER.trace("unwatchChildren"));
        return this;
    }

    @Override
    public Future<Boolean> pathExists(String path) {
        return workerExecutor.executeBlocking(
            () -> getPathExists(path));
    }

    @Override
    public boolean getPathExists(String path) {
        return zookeeper.exists(path);
    }

    @Override
    public List<String> getChildren(String path) {
        return zookeeper.getChildren(path);
    }

    @Override
    public byte[] getData(String path) {
        return zookeeper.readData(path);
    }

    @Override
    public void delete(String path, int version) {
        zookeeper.delete(path, version);
    }
}
