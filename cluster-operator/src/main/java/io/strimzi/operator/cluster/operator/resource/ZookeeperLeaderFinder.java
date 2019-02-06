/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class for finding the leader of a ZK cluster
 */
public class ZookeeperLeaderFinder {

    private static final Logger log = LogManager.getLogger(ZookeeperLeaderFinder.class);

    static final int UNKNOWN_LEADER = -1;

    private final Vertx vertx;
    private final SecretOperator secretOperator;
    private final Supplier<BackOff> backOffSupplier;

    public ZookeeperLeaderFinder(Vertx vertx, SecretOperator secretOperator, Supplier<BackOff> backOffSupplier) {
        this.vertx = vertx;
        this.secretOperator = secretOperator;
        this.backOffSupplier = backOffSupplier;
    }

    /*test*/ NetClientOptions clientOptions(Secret coCertKeySecret, Secret clusterCaCertificateSecret) {
        return new NetClientOptions()
                .setConnectTimeout(10_000)
                .setSsl(true)
                .setHostnameVerificationAlgorithm("HTTPS")
                .setPemKeyCertOptions(keyCertOptions(coCertKeySecret))
                .setPemTrustOptions(trustOptions(clusterCaCertificateSecret));
    }

    private CertificateFactory x509Factory() {
        CertificateFactory x509;
        try {
            x509 = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new RuntimeException("No security provider supports X.509");
        }
        return x509;
    }

    /**
     * Validate the cluster CA certificate passed in the given Secret
     * and return the PemTrustOptions for trusting it.
     */
    protected PemTrustOptions trustOptions(Secret clusterCaCertificateSecret) {
        Base64.Decoder decoder = Base64.getDecoder();
        CertificateFactory x509 = x509Factory();
        byte[] certBytes = decoder.decode(clusterCaCertificateSecret.getData().get(Ca.CA_CRT));
        try {
            x509.generateCertificate(new ByteArrayInputStream(certBytes));
        } catch (CertificateException e) {
            throw corruptCertificate(clusterCaCertificateSecret, Ca.CA_CRT, e);
        }
        return new PemTrustOptions()
                .addCertValue(Buffer.buffer(certBytes));
    }

    private RuntimeException corruptCertificate(Secret secret, String certKey, CertificateException e) {
        return new RuntimeException("Bad/corrupt certificate found in data." + certKey.replace(".", "\\.") + " of Secret "
                + secret.getMetadata().getName() + " in namespace " + secret.getMetadata().getNamespace(), e);
    }

    /**
     * Validate the CO certificate and key passed in the given Secret
     * and return the PemKeyCertOptions for using it for TLS authentication.
     */
    protected PemKeyCertOptions keyCertOptions(Secret coCertKeySecret) {
        CertAndKey coCertKey = Ca.asCertAndKey(coCertKeySecret, "cluster-operator.key", "cluster-operator.crt");
        if (coCertKey == null) {
            throw missingSecretFuture(coCertKeySecret.getMetadata().getNamespace(), coCertKeySecret.getMetadata().getName());
        }
        CertificateFactory x509 = x509Factory();
        try {
            x509.generateCertificate(new ByteArrayInputStream(coCertKey.cert()));
        } catch (CertificateException e) {
            throw corruptCertificate(coCertKeySecret, "cluster-operator.crt", e);
        }
        return new PemKeyCertOptions()
                .setCertValue(Buffer.buffer(coCertKey.cert()))
                .setKeyValue(Buffer.buffer(coCertKey.key()));
    }

    /**
     * Returns a Future which completes with the the id of the Zookeeper leader.
     * An exponential backoff is used if no ZK node is leader on the attempt to find it.
     * If there is no leader after 3 attempts then the returned Future completes with {@link #UNKNOWN_LEADER}.
     */
    Future<Integer> findZookeeperLeader(String cluster, String namespace, List<Pod> pods) {
        if (pods.size() <= 1) {
            return Future.succeededFuture(pods.size() - 1);
        }
        String brokersSecretName = ClusterOperator.secretName(cluster);
        Future<Secret> brokersSecretFuture = secretOperator.getAsync(namespace, brokersSecretName);
        String clusterCaSecretName = KafkaResources.clusterCaCertificateSecretName(cluster);
        Future<Secret> clusterCaKeySecretFuture = secretOperator.getAsync(namespace, clusterCaSecretName);
        return CompositeFuture.join(brokersSecretFuture, clusterCaKeySecretFuture).compose(c -> {
            Secret brokersSecret = c.resultAt(0);
            if (brokersSecret == null) {
                return Future.failedFuture(missingSecretFuture(namespace, brokersSecretName));
            }
            Secret clusterCaCertificateSecret = c.resultAt(1);
            if (clusterCaCertificateSecret  == null) {
                return Future.failedFuture(missingSecretFuture(namespace, brokersSecretName));
            }
            try {
                NetClientOptions netClientOptions = clientOptions(brokersSecret, clusterCaCertificateSecret);
                return zookeeperLeader(cluster, namespace, pods, netClientOptions);
            } catch (Throwable e) {
                return Future.failedFuture(e);
            }
        });

    }

    private RuntimeException missingSecretFuture(String namespace, String secretName) {
        return new RuntimeException("Secret " + namespace + "/" + secretName + " does not exist");
    }

    private Future<Integer> zookeeperLeader(String cluster, String namespace, List<Pod> pods,
                                            NetClientOptions netClientOptions) {
        Future<Integer> result = Future.future();
        BackOff backOff = backOffSupplier.get();
        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long tid) {
                zookeeperLeader(pods, netClientOptions).setHandler(leader -> {
                    if (leader.succeeded()) {
                        if (leader.result() != UNKNOWN_LEADER) {
                            result.complete(leader.result());
                        } else {
                            rescheduleOrComplete(tid);
                        }
                    } else {
                        log.debug("Ignoring error", leader.cause());
                        if (backOff.done()) {
                            result.fail(leader.cause());
                        } else {
                            rescheduleOrComplete(tid);
                        }
                    }
                });
            }

            void rescheduleOrComplete(Long tid) {
                if (backOff.done()) {
                    log.warn("Giving up trying to find the leader of {}/{} after {} attempts taking {}ms",
                            namespace, cluster, backOff.maxAttempts(), backOff.totalDelayMs());
                    result.complete(UNKNOWN_LEADER);
                } else {
                    // Schedule ourselves to run again
                    long delay = backOff.delayMs();
                    log.info("No leader found for cluster {} in namespace {}; " +
                                    "backing off for {}ms (cumulative {}ms)",
                            cluster, namespace, delay, backOff.cumulativeDelayMs());
                    if (delay < 1) {
                        this.handle(tid);
                    } else {
                        vertx.setTimer(delay, this);
                    }
                }
            }
        };
        handler.handle(null);
        return result;
    }

    /**
     * Synchronously find the leader by testing each pod in the given list
     * using {@link #isLeader(Pod, NetClientOptions)}.
     */
    private Future<Integer> zookeeperLeader(List<Pod> pods, NetClientOptions netClientOptions) {
        try {
            Future<Integer> f = Future.succeededFuture(UNKNOWN_LEADER);
            for (int i = 0; i < pods.size(); i++) {
                final int podNum = i;
                Pod pod = pods.get(i);
                String podName = pod.getMetadata().getName();
                f = f.compose(leader -> {
                    if (leader == UNKNOWN_LEADER) {
                        log.debug("Checker whether {} is leader", podName);
                        return isLeader(pod, netClientOptions).map(isLeader -> {
                            if (isLeader != null && isLeader) {
                                log.info("Pod {} is leader", podName);
                                return podNum;
                            } else {
                                log.info("Pod {} is not a leader", podName);
                                return UNKNOWN_LEADER;
                            }
                        });
                    } else {
                        return Future.succeededFuture(leader);
                    }
                });
            }
            return f;
        } catch (Throwable t) {
            return Future.failedFuture(t);
        }

    }

    /**
     * Returns whether the given pod is the zookeeper leader.
     */
    protected Future<Boolean> isLeader(Pod pod, NetClientOptions netClientOptions) {
        Pattern p = Pattern.compile("^Mode: leader$", Pattern.MULTILINE);
        Future<Boolean> future = Future.future();
        String host = host(pod);
        int port = port(pod);
        log.debug("Connecting to zookeeper on {}:{}", host, port);
        vertx.createNetClient(netClientOptions)
            .connect(port, host, ar -> {
                if (ar.failed()) {
                    log.warn("ZK {}:{}: failed to connect to zookeeper:", host, port, ar.cause().getMessage());
                    future.fail(ar.cause());
                } else {
                    log.debug("ZK {}:{}: connected", host, port);
                    NetSocket socket = ar.result();
                    socket.exceptionHandler(ex -> future.fail(ex));
                    socket.closeHandler(v -> {
                        if (!future.isComplete()) {
                            log.debug("ZK {}:{}: closing socked, was not leader", host, port);
                            future.complete(Boolean.FALSE);
                        }
                    });
                    log.debug("ZK {}:{}: upgrading to TLS", host, port);
                    StringBuilder sb = new StringBuilder();
                    socket.handler(buffer -> {
                        log.trace("buffer: {}", buffer);
                        if (!future.isComplete()) {
                            String s = buffer.getString(0, buffer.length(), "UTF-8");
                            sb.append(s);
                            log.trace("appended: {}", sb);
                            Matcher m = p.matcher(sb);
                            if (m.find()) {
                                log.debug("ZK {}:{}: is leader", host, port);
                                future.complete(Boolean.TRUE);
                            }
                            int i = sb.lastIndexOf("\n");
                            if (i != -1) {
                                sb.delete(0, i + 1);
                            }
                        }
                    });
                    log.debug("ZK {}:{}: sending stat", host, port);
                    socket.write("stat");
                }

            });
        return future;
    }

    /** The hostname for connecting to zookeeper in the given pod. */
    protected String host(Pod pod) {
        return ZookeeperCluster.podDnsName(pod.getMetadata().getNamespace(), pod.getMetadata().getName());
    }

    /** The port number for connecting to zookeeper in the given pod. */
    protected int port(Pod pod) {
        return 2181;
    }
}
