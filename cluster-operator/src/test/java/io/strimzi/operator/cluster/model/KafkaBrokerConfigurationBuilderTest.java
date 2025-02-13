/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.Rack;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorization;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationOpaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpecBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafka;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafkaBuilder;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginStrimzi;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginStrimziBuilder;
import io.strimzi.api.kafka.model.kafka.tieredstorage.RemoteStorageManager;
import io.strimzi.api.kafka.model.kafka.tieredstorage.TieredStorageCustom;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.cruisecontrol.CruiseControlMetricsReporter;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.cluster.TestUtils.IsEquivalent.isEquivalent;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ParallelSuite
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class KafkaBrokerConfigurationBuilderTest {
    private final static NodeRef NODE_REF = new NodeRef("my-cluster-kafka-2", 2, "kafka", false, true);

    private final static KafkaVersion KAFKA_3_8_0 = new KafkaVersion(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, "", "", "", false, false, "");
    private final static KafkaVersion KAFKA_3_9_0 = new KafkaVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, "", "", "", false, false, "");

    @ParallelTest
    public void testBrokerId()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .build();
        // brokers don't have broker.id when in KRaft-mode, only node.id
        assertThat(configuration, not(containsString("broker.id")));
        assertThat(configuration, containsString("node.id=2"));

        NodeRef controller = new NodeRef("my-cluster-kafka-3", 3, "kafka", true, false);
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, controller)
                .build();
        // controllers don't have broker.id at all, only node.id
        assertThat(configuration, not(containsString("broker.id")));
        assertThat(configuration, containsString("node.id=3"));
    }

    @ParallelTest
    public void testKraftMixedNodes()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-kafka-0", 0, "kafka", true, true),
                new NodeRef("my-cluster-kafka-1", 1, "kafka", true, true),
                new NodeRef("my-cluster-kafka-2", 2, "kafka", true, true)
        );

        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=broker,controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-kafka-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-kafka-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090"));
    }

    @ParallelTest
    public void testKraftControllerAndBrokerNodes()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-controllers-0", 0, "controllers", true, false),
                new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false),
                new NodeRef("my-cluster-controllers-2", 2, "controllers", true, false),
                new NodeRef("my-cluster-brokers-10", 10, "brokers", false, true),
                new NodeRef("my-cluster-brokers-11", 11, "brokers", false, true),
                new NodeRef("my-cluster-brokers-12", 12, "brokers", false, true)
        );

        // Controller-only node
        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090"));

        nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 11).findFirst().get();
        // Broker-only node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .build();

        assertThat(configuration, isEquivalent("node.id=11",
                "process.roles=broker",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090"));
    }

    @ParallelTest
    public void testNoCruiseControl()  {
        // Broker configuration
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", null, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));

        // Controller configuration
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", null, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @ParallelTest
    public void testStrimziMetricsReporter() {
        StrimziMetricsReporter config = new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList("kafka_log.*", "kafka_network.*")
                .endValues()
                .build();
        StrimziMetricsReporterModel model = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(config).build());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withStrimziMetricsReporter(model)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                 "kafka.metrics.reporters=io.strimzi.kafka.metrics.YammerPrometheusMetricsReporter",
                 "prometheus.metrics.reporter.listener.enable=true",
                 "prometheus.metrics.reporter.listener=http://0.0.0.0:9404",
                 "prometheus.metrics.reporter.allowlist=kafka_log.*,kafka_network.*"));
    }

    @ParallelTest
    public void testCruiseControl()  {
        CruiseControlMetricsReporter ccMetricsReporter = new CruiseControlMetricsReporter("strimzi.cruisecontrol.metrics", 1, 1, 1);

        // Broker configuration
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", ccMetricsReporter, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                CruiseControlConfigurationParameters.METRICS_TOPIC_NAME + "=strimzi.cruisecontrol.metrics",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO + "=HTTPS",
                CruiseControlConfigurationParameters.METRICS_REPORTER_BOOTSTRAP_SERVERS + "=my-cluster-kafka-brokers:9091",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SECURITY_PROTOCOL + "=SSL",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE + "=PKCS12",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_LOCATION + "=/tmp/kafka/cluster.keystore.p12",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_PASSWORD + "=${strimzienv:CERTS_STORE_PASSWORD}",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE + "=PKCS12",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_LOCATION + "=/tmp/kafka/cluster.truststore.p12",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_PASSWORD + "=${strimzienv:CERTS_STORE_PASSWORD}",
                CruiseControlConfigurationParameters.METRICS_TOPIC_AUTO_CREATE + "=true",
                CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=1",
                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=1",
                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=1"));

        // Controller configuration
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", ccMetricsReporter, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @ParallelTest
    public void testCruiseControlCustomMetricReporterTopic()  {
        CruiseControlMetricsReporter ccMetricsReporter = new CruiseControlMetricsReporter("metric-reporter-topic", 2, 3, 4);

        // Broker configuration
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", ccMetricsReporter, true)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                CruiseControlConfigurationParameters.METRICS_TOPIC_NAME + "=metric-reporter-topic",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO + "=HTTPS",
                CruiseControlConfigurationParameters.METRICS_REPORTER_BOOTSTRAP_SERVERS + "=my-cluster-kafka-brokers:9091",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SECURITY_PROTOCOL + "=SSL",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE + "=PKCS12",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_LOCATION + "=/tmp/kafka/cluster.keystore.p12",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_PASSWORD + "=${strimzienv:CERTS_STORE_PASSWORD}",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE + "=PKCS12",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_LOCATION + "=/tmp/kafka/cluster.truststore.p12",
                CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_PASSWORD + "=${strimzienv:CERTS_STORE_PASSWORD}",
                CruiseControlConfigurationParameters.METRICS_TOPIC_AUTO_CREATE + "=true",
                CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=2",
                CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=3",
                CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=4"));

        // Controller configuration
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withCruiseControl("my-cluster", ccMetricsReporter, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @ParallelTest
    public void testNoRackAwareness()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withRackId(null)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @ParallelTest
    public void testRackIdInKRaftBrokers()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withRackId(new Rack("failure-domain.kubernetes.io/zone"))
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "broker.rack=${strimzidir:/opt/kafka/init:rack.id}"));
    }

    @ParallelTest
    public void testRackIdInKRaftMixedNode()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, new NodeRef("my-cluster-kafka-1", 1, "kafka", true, true))
                .withRackId(new Rack("failure-domain.kubernetes.io/zone"))
                .build();

        assertThat(configuration, isEquivalent("node.id=1",
                "broker.rack=${strimzidir:/opt/kafka/init:rack.id}"));
    }

    @ParallelTest
    public void testRackIdInKRaftControllers()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false))
                .withRackId(new Rack("failure-domain.kubernetes.io/zone"))
                .build();

        assertThat(configuration, isEquivalent("node.id=1"));
    }

    @ParallelTest
    public void testNoAuthorization()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", null)
                .build();

        assertThat(configuration, isEquivalent("node.id=2"));
    }

    @ParallelTest
    public void testSimpleAuthorizationWithSuperUsers()  {
        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .addToSuperUsers("jakub", "CN=kuba")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", auth)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jakub;User:CN=kuba"));
    }

    @ParallelTest
    public void testSimpleAuthorizationWithSuperUsersAndKRaft()  {
        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .addToSuperUsers("jakub", "CN=kuba")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", auth)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jakub;User:CN=kuba"));
    }

    @ParallelTest
    public void testKeycloakAuthorization() {
        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName("my-secret")
                .withCertificate("my.crt")
                .build();

        KafkaAuthorization auth = new KafkaAuthorizationKeycloakBuilder()
                .withTokenEndpointUri("http://token-endpoint-uri")
                .withClientId("my-client-id")
                .withDelegateToKafkaAcls(false)
                .withGrantsRefreshPeriodSeconds(120)
                .withGrantsRefreshPoolSize(10)
                .withGrantsMaxIdleTimeSeconds(600)
                .withGrantsGcPeriodSeconds(120)
                .withGrantsAlwaysLatest(true)
                .withTlsTrustedCertificates(cert)
                .withDisableTlsHostnameVerification(true)
                .addToSuperUsers("giada", "CN=paccu")
                .withConnectTimeoutSeconds(30)
                .withReadTimeoutSeconds(10)
                .withHttpRetries(2)
                .withEnableMetrics(true)
                .withIncludeAcceptHeader(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", auth)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=io.strimzi.kafka.oauth.server.authorizer.KeycloakAuthorizer",
                "strimzi.authorization.token.endpoint.uri=http://token-endpoint-uri",
                "strimzi.authorization.client.id=my-client-id",
                "strimzi.authorization.delegate.to.kafka.acl=false",
                "strimzi.authorization.kafka.cluster.name=my-cluster",
                "strimzi.authorization.ssl.truststore.location=/tmp/kafka/authz-keycloak.truststore.p12",
                "strimzi.authorization.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "strimzi.authorization.ssl.truststore.type=PKCS12",
                "strimzi.authorization.ssl.endpoint.identification.algorithm=",
                "strimzi.authorization.grants.refresh.period.seconds=120",
                "strimzi.authorization.grants.refresh.pool.size=10",
                "strimzi.authorization.grants.max.idle.time.seconds=600",
                "strimzi.authorization.grants.gc.period.seconds=120",
                "strimzi.authorization.reuse.grants=false",
                "strimzi.authorization.connect.timeout.seconds=30",
                "strimzi.authorization.read.timeout.seconds=10",
                "strimzi.authorization.http.retries=2",
                "strimzi.authorization.enable.metrics=true",
                "strimzi.authorization.include.accept.header=false",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:giada;User:CN=paccu"));
    }

    @ParallelTest
    public void testKeycloakAuthorizationWithDefaults() {
        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName("my-secret")
                .withCertificate("my.crt")
                .build();

        KafkaAuthorization auth = new KafkaAuthorizationKeycloakBuilder()
                .withTokenEndpointUri("http://token-endpoint-uri")
                .withClientId("my-client-id")
                .withTlsTrustedCertificates(cert)
                .withReadTimeoutSeconds(30)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", auth)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=io.strimzi.kafka.oauth.server.authorizer.KeycloakAuthorizer",
                "strimzi.authorization.token.endpoint.uri=http://token-endpoint-uri",
                "strimzi.authorization.client.id=my-client-id",
                "strimzi.authorization.delegate.to.kafka.acl=false",
                "strimzi.authorization.kafka.cluster.name=my-cluster",
                "strimzi.authorization.ssl.truststore.location=/tmp/kafka/authz-keycloak.truststore.p12",
                "strimzi.authorization.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "strimzi.authorization.ssl.truststore.type=PKCS12",
                "strimzi.authorization.ssl.endpoint.identification.algorithm=HTTPS",
                "strimzi.authorization.read.timeout.seconds=30",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi"));
    }

    @ParallelTest
    public void testOpaAuthorizationWithDefaults() {
        KafkaAuthorization auth = new KafkaAuthorizationOpaBuilder()
                .withUrl("http://opa:8181/v1/data/kafka/allow")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", auth)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer",
                "opa.authorizer.url=http://opa:8181/v1/data/kafka/allow",
                "opa.authorizer.allow.on.error=false",
                "opa.authorizer.metrics.enabled=false",
                "opa.authorizer.cache.initial.capacity=5000",
                "opa.authorizer.cache.maximum.size=50000",
                "opa.authorizer.cache.expire.after.seconds=3600",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi"));
    }

    @ParallelTest
    public void testOpaAuthorization() {
        KafkaAuthorization auth = new KafkaAuthorizationOpaBuilder()
                .withUrl("http://opa:8181/v1/data/kafka/allow")
                .withAllowOnError(true)
                .withInitialCacheCapacity(1000)
                .withMaximumCacheSize(10000)
                .withExpireAfterMs(60000)
                .addToSuperUsers("jack", "CN=conor")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withAuthorization("my-cluster", auth)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer",
                "opa.authorizer.url=http://opa:8181/v1/data/kafka/allow",
                "opa.authorizer.allow.on.error=true",
                "opa.authorizer.metrics.enabled=false",
                "opa.authorizer.cache.initial.capacity=1000",
                "opa.authorizer.cache.maximum.size=10000",
                "opa.authorizer.cache.expire.after.seconds=60",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jack;User:CN=conor"));
    }

    @ParallelTest
    public void testOpaAuthorizationWithTls() {
        CertSecretSource cert = new CertSecretSourceBuilder()
            .withSecretName("my-secret")
            .withCertificate("my.crt")
            .build();

        KafkaAuthorization auth = new KafkaAuthorizationOpaBuilder()
            .withUrl("https://opa:8181/v1/data/kafka/allow")
            .withAllowOnError(true)
            .withInitialCacheCapacity(1000)
            .withMaximumCacheSize(10000)
            .withExpireAfterMs(60000)
            .withTlsTrustedCertificates(cert)
            .addToSuperUsers("jack", "CN=conor")
            .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withAuthorization("my-cluster", auth)
            .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer",
                "opa.authorizer.url=https://opa:8181/v1/data/kafka/allow",
                "opa.authorizer.allow.on.error=true",
                "opa.authorizer.metrics.enabled=false",
                "opa.authorizer.cache.initial.capacity=1000",
                "opa.authorizer.cache.maximum.size=10000",
                "opa.authorizer.cache.expire.after.seconds=60",
                "opa.authorizer.truststore.path=/tmp/kafka/authz-opa.truststore.p12",
                "opa.authorizer.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "opa.authorizer.truststore.type=PKCS12",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi;User:jack;User:CN=conor"));
    }

    @ParallelTest
    public void testNullUserConfiguration()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(null, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka"));
    }

    @ParallelTest
    public void testNullUserConfigurationWithCcMetricsReporter()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(null, true, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "metric.reporters=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter"));
    }

    @ParallelTest
    public void testNullUserConfigurationWithStrimziMetricsReporter()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withUserConfiguration(null, false, true)
            .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "metric.reporters=" + StrimziMetricsReporterModel.KAFKA_PROMETHEUS_METRICS_REPORTER));
    }

    @ParallelTest
    public void testNullUserConfigurationWithCcAndStrimziMetricsReporters()  {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withUserConfiguration(null, true, true)
            .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "metric.reporters=" + CruiseControlMetricsReporter.CRUISE_CONTROL_METRIC_REPORTER
                        + "," + StrimziMetricsReporterModel.KAFKA_PROMETHEUS_METRICS_REPORTER));
    }

    @ParallelTest
    public void testEmptyUserConfiguration()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka"));
    }

    @ParallelTest
    public void testUserConfiguration()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("auto.create.topics.enable", "false");
        userConfiguration.put("offsets.topic.replication.factor", 3);
        userConfiguration.put("transaction.state.log.replication.factor", 3);
        userConfiguration.put("transaction.state.log.min.isr", 2);

        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "auto.create.topics.enable=false",
                "offsets.topic.replication.factor=3",
                "transaction.state.log.replication.factor=3",
                "transaction.state.log.min.isr=2"));
    }

    @ParallelTest
    public void testUserConfigurationWithConfigProviders()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("config.providers", "env");
        userConfiguration.put("config.providers.env.class", "org.apache.kafka.common.config.provider.EnvVarConfigProvider");
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        // Broker
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=env,strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider"));

        // Controller
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, new NodeRef("my-cluster-kafka-3", 3, "kafka", true, false))
                .withUserConfiguration(kafkaConfiguration, false, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=3",
                "config.providers=env,strimzienv",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider"));
    }

    @ParallelTest
    public void testUserConfigurationWithCcMetricsReporter()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("auto.create.topics.enable", "false");
        userConfiguration.put("offsets.topic.replication.factor", 3);
        userConfiguration.put("transaction.state.log.replication.factor", 3);
        userConfiguration.put("transaction.state.log.min.isr", 2);

        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, true, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "auto.create.topics.enable=false",
                "offsets.topic.replication.factor=3",
                "transaction.state.log.replication.factor=3",
                "transaction.state.log.min.isr=2",
                KafkaCluster.KAFKA_METRIC_REPORTERS_CONFIG_FIELD + "=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter"));
    }

    @ParallelTest
    public void testUserConfigurationWithCcAndOtherMetricsReporters()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("metric.reporters", "my.domain.CustomMetricReporter");

        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withUserConfiguration(kafkaConfiguration, true, false)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "metric.reporters=my.domain.CustomMetricReporter,com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter"));
    }

    @ParallelTest
    public void testUserConfigurationWithStrimziAndOtherMetricsReporters()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("metric.reporters", "my.domain.CustomMetricReporter");
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withUserConfiguration(kafkaConfiguration, false, true)
            .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "metric.reporters=my.domain.CustomMetricReporter," + StrimziMetricsReporterModel.KAFKA_PROMETHEUS_METRICS_REPORTER));
    }

    @ParallelTest
    public void testUserConfigurationWithCcAndStrimziAndOtherMetricsReporters()  {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("metric.reporters", "my.domain.CustomMetricReporter");
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withUserConfiguration(kafkaConfiguration, true, true)
            .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzifile.param.allowed.paths=/opt/kafka",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "metric.reporters=my.domain.CustomMetricReporter,"
                        + CruiseControlMetricsReporter.CRUISE_CONTROL_METRIC_REPORTER + ","
                        + StrimziMetricsReporterModel.KAFKA_PROMETHEUS_METRICS_REPORTER));
    }

    @ParallelTest
    public void testEphemeralStorageLogDirs()  {
        Storage storage = new EphemeralStorageBuilder()
                .withSizeLimit("5Gi")
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, false))
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "log.dirs=/var/lib/kafka/data/kafka-log2"));
    }

    @ParallelTest
    public void testPersistentStorageLogDirs()  {
        Storage storage = new PersistentClaimStorageBuilder()
                .withSize("1Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, false))
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "log.dirs=/var/lib/kafka/data/kafka-log2"));
    }

    @ParallelTest
    public void testJbodStorageLogDirs()  {
        SingleVolumeStorage vol1 = new PersistentClaimStorageBuilder()
                .withId(1)
                .withSize("1Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(true)
                .build();

        SingleVolumeStorage vol2 = new EphemeralStorageBuilder()
                .withId(2)
                .withSizeLimit("5Gi")
                .build();

        SingleVolumeStorage vol5 = new PersistentClaimStorageBuilder()
                .withId(5)
                .withSize("10Ti")
                .withStorageClass("aws-ebs")
                .withDeleteClaim(false)
                .build();

        Storage storage = new JbodStorageBuilder()
                .withVolumes(vol1, vol2, vol5)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withLogDirs(VolumeUtils.createVolumeMounts(storage, false))
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "log.dirs=/var/lib/kafka/data-1/kafka-log2,/var/lib/kafka/data-2/kafka-log2,/var/lib/kafka/data-5/kafka-log2"));
    }

    @ParallelTest
    public void testWithNoListeners() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", emptyList(), null, null)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testConnectionLimits()  {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9100)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                    .withMaxConnections(100)
                    .withMaxConnectionCreationRate(10)
                .endConfiguration()
                .build();

        GenericKafkaListener listener2 = new GenericKafkaListenerBuilder()
                .withName("listener2")
                .withPort(9101)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                    .withMaxConnections(1000)
                    .withMaxConnectionCreationRate(50)
                .endConfiguration()
                .build();

        GenericKafkaListener listener3 = new GenericKafkaListenerBuilder()
                .withName("listener3")
                .withPort(9102)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewConfiguration()
                .endConfiguration()
                .build();

        GenericKafkaListener listener4 = new GenericKafkaListenerBuilder()
                .withName("listener4")
                .withPort(9103)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", asList(listener1, listener2, listener3, listener4), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listener.name.listener1-9100.max.connections=100",
                "listener.name.listener1-9100.max.connection.creation.rate=10",
                "listener.name.listener2-9101.max.connections=1000",
                "listener.name.listener2-9101.max.connection.creation.rate=50",
                "listeners=REPLICATION-9091://0.0.0.0:9091,LISTENER1-9100://0.0.0.0:9100,LISTENER2-9101://0.0.0.0:9101,LISTENER3-9102://0.0.0.0:9102,LISTENER4-9103://0.0.0.0:9103",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,LISTENER1-9100://dummy-advertised-address:1919,LISTENER2-9101://dummy-advertised-address:1919,LISTENER3-9102://dummy-advertised-address:1919,LISTENER4-9103://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,LISTENER1-9100:PLAINTEXT,LISTENER2-9101:PLAINTEXT,LISTENER3-9102:PLAINTEXT,LISTENER4-9103:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testWithPlainListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testKraftListenersMixedNodes()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-kafka-0", 0, "kafka", true, true),
                new NodeRef("my-cluster-kafka-1", 1, "kafka", true, true),
                new NodeRef("my-cluster-kafka-2", 2, "kafka", true, true)
        );

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=broker,controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-kafka-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-kafka-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testKraftListenersMixedNodesWithVersion3_9()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-kafka-0", 0, "kafka", true, true),
                new NodeRef("my-cluster-kafka-1", 1, "kafka", true, true),
                new NodeRef("my-cluster-kafka-2", 2, "kafka", true, true)
        );

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        // KRaft controller or mixed node with version 3.9 or later should have advertised listeners configured with controller listener
        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=broker,controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-kafka-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-kafka-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testKraftListenersBrokerAndControllerNodes()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-controllers-0", 0, "controllers", true, false),
                new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false),
                new NodeRef("my-cluster-controllers-2", 2, "controllers", true, false),
                new NodeRef("my-cluster-brokers-10", 10, "brokers", false, true),
                new NodeRef("my-cluster-brokers-11", 11, "brokers", false, true),
                new NodeRef("my-cluster-brokers-12", 12, "brokers", false, true)
        );

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        // Controller-only node
        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));

        nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 11).findFirst().get();
        // Broker-only node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=11",
                "process.roles=broker",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testKraftListenersBrokerAndControllerNodesWithVersion3_9()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-controllers-0", 0, "controllers", true, false),
                new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false),
                new NodeRef("my-cluster-controllers-2", 2, "controllers", true, false),
                new NodeRef("my-cluster-brokers-10", 10, "brokers", false, true),
                new NodeRef("my-cluster-brokers-11", 11, "brokers", false, true),
                new NodeRef("my-cluster-brokers-12", 12, "brokers", false, true)
        );

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        // Controller-only node
        NodeRef nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 2).findFirst().get();
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=controller",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc:9090",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));

        nodeRef = nodes.stream().filter(nr -> nr.nodeId() == 11).findFirst().get();
        // Broker-only node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, nodeRef)
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=11",
                "process.roles=broker",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testKraftOauthBrokerControllerAndMixedNodes()  {
        Set<NodeRef> nodes = Set.of(
            new NodeRef("my-cluster-controllers-0", 0, "controllers", true, false),
            new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false),
            new NodeRef("my-cluster-controllers-2", 2, "controllers", true, false),
            new NodeRef("my-cluster-brokers-10", 10, "brokers", false, true),
            new NodeRef("my-cluster-brokers-11", 11, "brokers", false, true),
            new NodeRef("my-cluster-brokers-12", 12, "brokers", false, true),
            new NodeRef("my-cluster-kafka-13", 13, "kafka", true, true),
            new NodeRef("my-cluster-kafka-14", 14, "kafka", true, true),
            new NodeRef("my-cluster-kafka-15", 15, "kafka", true, true)
        );

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
            .withName("plain")
            .withPort(9092)
            .withType(KafkaListenerType.INTERNAL)
            .withTls(false)
            .withNewKafkaListenerAuthenticationOAuth()
                .withValidIssuerUri("http://valid-issuer")
                .withJwksEndpointUri("http://jwks")
                .withEnableECDSA(true)
                .withUserNameClaim("preferred_username")
                .withGroupsClaim("$.groups")
                .withGroupsClaimDelimiter(";")
                .withMaxSecondsWithoutReauthentication(3600)
                .withJwksMinRefreshPauseSeconds(5)
                .withEnablePlain(true)
                .withTokenEndpointUri("http://token")
                .withConnectTimeoutSeconds(30)
                .withReadTimeoutSeconds(30)
                .withEnableMetrics(true)
                .withIncludeAcceptHeader(false)
            .endKafkaListenerAuthenticationOAuth()
            .build();

        // Controller-only node
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION,
                nodes.stream().filter(nodeRef -> nodeRef.nodeId() == 2).findFirst().get())
            .withKRaft("my-cluster", "my-namespace", nodes)
            .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
            .build();

        assertThat(configuration, isEquivalent("node.id=2",
            "process.roles=controller",
            "controller.listener.names=CONTROLPLANE-9090",
            "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,13@my-cluster-kafka-13.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,14@my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,15@my-cluster-kafka-15.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
            "listener.name.controlplane-9090.ssl.client.auth=required",
            "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
            "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
            "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
            "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
            "listeners=CONTROLPLANE-9090://0.0.0.0:9090",
            "listener.security.protocol.map=CONTROLPLANE-9090:SSL",
            "sasl.enabled.mechanisms=",
            "ssl.endpoint.identification.algorithm=HTTPS",
            "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));

        // Broker-only node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION,
                nodes.stream().filter(nodeRef -> nodeRef.nodeId() == 11).findFirst().get())
            .withKRaft("my-cluster", "my-namespace", nodes)
            .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
            .build();

        assertThat(configuration, isEquivalent("node.id=11",
            "process.roles=broker",
            "controller.listener.names=CONTROLPLANE-9090",
            "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,13@my-cluster-kafka-13.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,14@my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,15@my-cluster-kafka-15.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
            "listener.name.controlplane-9090.ssl.client.auth=required",
            "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
            "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
            "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
            "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
            "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
            "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.replication-9091.ssl.keystore.type=PKCS12",
            "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
            "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.replication-9091.ssl.truststore.type=PKCS12",
            "listener.name.replication-9091.ssl.client.auth=required",
            "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
            "advertised.listeners=REPLICATION-9091://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9092",
            "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
            "inter.broker.listener.name=REPLICATION-9091",
            "sasl.enabled.mechanisms=",
            "ssl.endpoint.identification.algorithm=HTTPS",
            "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
            "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
            "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\";",
            "listener.name.plain-9092.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
            "listener.name.plain-9092.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\" oauth.token.endpoint.uri=\"http://token\";",
            "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER,PLAIN",
            "listener.name.plain-9092.connections.max.reauth.ms=3600000"));

        // Mixed node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION,
                nodes.stream().filter(nodeRef -> nodeRef.nodeId() == 14).findFirst().get())
            .withKRaft("my-cluster", "my-namespace", nodes)
            .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
            .build();

        assertThat(configuration, isEquivalent("node.id=14",
            "process.roles=broker,controller",
            "controller.listener.names=CONTROLPLANE-9090",
            "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,13@my-cluster-kafka-13.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,14@my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,15@my-cluster-kafka-15.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
            "listener.name.controlplane-9090.ssl.client.auth=required",
            "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
            "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
            "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
            "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
            "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
            "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.replication-9091.ssl.keystore.type=PKCS12",
            "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
            "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "listener.name.replication-9091.ssl.truststore.type=PKCS12",
            "listener.name.replication-9091.ssl.client.auth=required",
            "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
            "advertised.listeners=REPLICATION-9091://my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc:9092",
            "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
            "inter.broker.listener.name=REPLICATION-9091",
            "sasl.enabled.mechanisms=",
            "ssl.endpoint.identification.algorithm=HTTPS",
            "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
            "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
            "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\";",
            "listener.name.plain-9092.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
            "listener.name.plain-9092.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\" oauth.token.endpoint.uri=\"http://token\";",
            "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER,PLAIN",
            "listener.name.plain-9092.connections.max.reauth.ms=3600000"));
    }

    @ParallelTest
    public void testKraftOauthBrokerControllerAndMixedNodesWithVersion3_9()  {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-controllers-0", 0, "controllers", true, false),
                new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false),
                new NodeRef("my-cluster-controllers-2", 2, "controllers", true, false),
                new NodeRef("my-cluster-brokers-10", 10, "brokers", false, true),
                new NodeRef("my-cluster-brokers-11", 11, "brokers", false, true),
                new NodeRef("my-cluster-brokers-12", 12, "brokers", false, true),
                new NodeRef("my-cluster-kafka-13", 13, "kafka", true, true),
                new NodeRef("my-cluster-kafka-14", 14, "kafka", true, true),
                new NodeRef("my-cluster-kafka-15", 15, "kafka", true, true)
        );

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                .withValidIssuerUri("http://valid-issuer")
                .withJwksEndpointUri("http://jwks")
                .withEnableECDSA(true)
                .withUserNameClaim("preferred_username")
                .withGroupsClaim("$.groups")
                .withGroupsClaimDelimiter(";")
                .withMaxSecondsWithoutReauthentication(3600)
                .withJwksMinRefreshPauseSeconds(5)
                .withEnablePlain(true)
                .withTokenEndpointUri("http://token")
                .withConnectTimeoutSeconds(30)
                .withReadTimeoutSeconds(30)
                .withEnableMetrics(true)
                .withIncludeAcceptHeader(false)
                .endKafkaListenerAuthenticationOAuth()
                .build();

        // Controller-only node
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION,
                nodes.stream().filter(nodeRef -> nodeRef.nodeId() == 2).findFirst().get())
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "process.roles=controller",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc:9090",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,13@my-cluster-kafka-13.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,14@my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,15@my-cluster-kafka-15.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));

        // Broker-only node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION,
                nodes.stream().filter(nodeRef -> nodeRef.nodeId() == 11).findFirst().get())
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=11",
                "process.roles=broker",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,13@my-cluster-kafka-13.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,14@my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,15@my-cluster-kafka-15.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-brokers-11.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\";",
                "listener.name.plain-9092.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
                "listener.name.plain-9092.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\" oauth.token.endpoint.uri=\"http://token\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER,PLAIN",
                "listener.name.plain-9092.connections.max.reauth.ms=3600000"));

        // Mixed node
        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION,
                nodes.stream().filter(nodeRef -> nodeRef.nodeId() == 14).findFirst().get())
                .withKRaft("my-cluster", "my-namespace", nodes)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc", listenerId -> "9092")
                .build();

        assertThat(configuration, isEquivalent("node.id=14",
                "process.roles=broker,controller",
                "controller.listener.names=CONTROLPLANE-9090",
                "controller.quorum.voters=0@my-cluster-controllers-0.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,1@my-cluster-controllers-1.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,2@my-cluster-controllers-2.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,13@my-cluster-kafka-13.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,14@my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090,15@my-cluster-kafka-15.my-cluster-kafka-brokers.my-namespace.svc.cluster.local:9090",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=CONTROLPLANE-9090://0.0.0.0:9090,REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=CONTROLPLANE-9090://my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc:9090,REPLICATION-9091://my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://my-cluster-kafka-14.my-cluster-kafka-brokers.my-namespace.svc:9092",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\";",
                "listener.name.plain-9092.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
                "listener.name.plain-9092.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\" oauth.token.endpoint.uri=\"http://token\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER,PLAIN",
                "listener.name.plain-9092.connections.max.reauth.ms=3600000"));
    }

    @ParallelTest
    public void testWithPlainListenersWithSaslAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationScramSha512Auth()
                .endKafkaListenerAuthenticationScramSha512Auth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required ;",
                "listener.name.plain-9092.sasl.enabled.mechanisms=SCRAM-SHA-512"));
    }

    @ParallelTest
    public void testWithTlsListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.tls-9093.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.tls-9093.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithTlsListenersWithTlsAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.client.auth=required",
                "listener.name.tls-9093.ssl.truststore.location=/tmp/kafka/clients.truststore.p12",
                "listener.name.tls-9093.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.tls-9093.ssl.truststore.type=PKCS12",
                "listener.name.tls-9093.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.tls-9093.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.tls-9093.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithTlsListenersWithCustomCerts()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("my-secret")
                        .withKey("my.key")
                        .withCertificate("my.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,TLS-9093://0.0.0.0:9093",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,TLS-9093://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,TLS-9093:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.tls-9093.ssl.keystore.location=/tmp/kafka/custom-tls-9093.keystore.p12",
                "listener.name.tls-9093.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.tls-9093.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalRouteListenersWithoutAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalRouteListenersWithTlsAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.client.auth=required",
                "listener.name.external-9094.ssl.truststore.location=/tmp/kafka/clients.truststore.p12",
                "listener.name.external-9094.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.truststore.type=PKCS12",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalRouteListenersWithSaslAuth()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewKafkaListenerAuthenticationScramSha512Auth()
                .endKafkaListenerAuthenticationScramSha512Auth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SASL_SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required ;",
                "listener.name.external-9094.sasl.enabled.mechanisms=SCRAM-SHA-512",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalRouteListenersWithCustomCerts()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .withNewConfiguration()
                    .withNewBrokerCertChainAndKey()
                        .withSecretName("my-secret")
                        .withKey("my.key")
                        .withCertificate("my.crt")
                    .endBrokerCertChainAndKey()
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/custom-external-9094.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersLoadBalancerWithTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(true)
                .build();
      
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testPerBrokerWithExternalListeners()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "my-lb.com", listenerId -> "9094")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://my-lb.com:9094",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersLoadBalancerWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.LOADBALANCER)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testWithExternalListenersNodePortWithTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(true)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersNodePortWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testPerBrokerWithExternalListenersNodePortWithoutTls()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "${strimzienv:STRIMZI_NODEPORT_DEFAULT_ADDRESS}", listenerId -> "31234")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://${strimzienv:STRIMZI_NODEPORT_DEFAULT_ADDRESS}:31234",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testWithExternalListenersIngress()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withHost("broker-0.mytld.com")
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.INGRESS)
                .withTls(true)
                .withNewConfiguration()
                    .withControllerClass("nginx-ingress")
                    .withNewBootstrap()
                        .withHost("bootstrap.mytld.com")
                    .endBootstrap()
                    .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersClusterIPWithTLS()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost("ingress.mytld.com")
                .withAdvertisedPort(12345)
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withTls(true)
                .withNewConfiguration()
                .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.external-9094.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.external-9094.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.external-9094.ssl.keystore.type=PKCS12"));
    }

    @ParallelTest
    public void testWithExternalListenersClusterIPWithoutTLS()  {
        GenericKafkaListenerConfigurationBroker broker = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost("ingress.mytld.com")
                .withAdvertisedPort(12345)
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.CLUSTER_IP)
                .withTls(false)
                .withNewConfiguration()
                .withBrokers(broker)
                .endConfiguration()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,EXTERNAL-9094://0.0.0.0:9094",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,EXTERNAL-9094://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,EXTERNAL-9094:PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testOauthConfiguration()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks")
                    .withServerBearerTokenLocation("/var/run/secrets/kubernetes.io/serviceaccount/token")
                    .withEnableECDSA(true)
                    .withUserNameClaim("preferred_username")
                    .withUserNamePrefix("user-")
                    .withFallbackUserNameClaim("client_id")
                    .withFallbackUserNamePrefix("service-account-")
                    .withGroupsClaim("$.groups")
                    .withGroupsClaimDelimiter(";")
                    .withMaxSecondsWithoutReauthentication(3600)
                    .withJwksMinRefreshPauseSeconds(5)
                    .withEnablePlain(true)
                    .withTokenEndpointUri("http://token")
                    .withConnectTimeoutSeconds(30)
                    .withReadTimeoutSeconds(30)
                    .withEnableMetrics(true)
                    .withIncludeAcceptHeader(false)
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.server.bearer.token.location=\"/var/run/secrets/kubernetes.io/serviceaccount/token\" oauth.username.claim=\"preferred_username\" oauth.username.prefix=\"user-\" oauth.fallback.username.claim=\"client_id\" oauth.fallback.username.prefix=\"service-account-\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\";",
                "listener.name.plain-9092.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
                "listener.name.plain-9092.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.server.bearer.token.location=\"/var/run/secrets/kubernetes.io/serviceaccount/token\" oauth.username.claim=\"preferred_username\" oauth.username.prefix=\"user-\" oauth.fallback.username.claim=\"client_id\" oauth.fallback.username.prefix=\"service-account-\" oauth.groups.claim=\"$.groups\" oauth.groups.claim.delimiter=\";\" oauth.connect.timeout.seconds=\"30\" oauth.read.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.include.accept.header=\"false\" oauth.config.id=\"PLAIN-9092\" oauth.token.endpoint.uri=\"http://token\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER,PLAIN",
                "listener.name.plain-9092.connections.max.reauth.ms=3600000"));
    }

    @ParallelTest
    public void testOauthConfigurationWithPlainOnly()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                .withValidIssuerUri("http://valid-issuer")
                .withJwksEndpointUri("http://jwks")
                .withUserNameClaim("preferred_username")
                .withMaxSecondsWithoutReauthentication(3600)
                .withJwksMinRefreshPauseSeconds(5)
                .withEnablePlain(true)
                .withEnableOauthBearer(false)
                .withTokenEndpointUri("http://token")
                .withClientAudience("kafka")
                .withClientScope("messaging")
                .withConnectTimeoutSeconds(30)
                .withEnableMetrics(true)
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
                "listener.name.plain-9092.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler",
                "listener.name.plain-9092.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required oauth.valid.issuer.uri=\"http://valid-issuer\" oauth.scope=\"messaging\" oauth.audience=\"kafka\" oauth.jwks.endpoint.uri=\"http://jwks\" oauth.jwks.refresh.min.pause.seconds=\"5\" oauth.username.claim=\"preferred_username\" oauth.connect.timeout.seconds=\"30\" oauth.enable.metrics=\"true\" oauth.config.id=\"PLAIN-9092\" oauth.token.endpoint.uri=\"http://token\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=PLAIN",
                "listener.name.plain-9092.connections.max.reauth.ms=3600000"));
    }

    @ParallelTest
    public void testOauthConfigurationWithoutOptions()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_9_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.config.id=\"PLAIN-9092\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));
    }

    @ParallelTest
    public void testOauthConfigurationWithTlsConfig()  {
        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName("my-secret")
                .withCertificate("my.crt")
                .build();

        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                    .withValidIssuerUri("https://valid-issuer")
                    .withJwksEndpointUri("https://jwks")
                    .withEnableECDSA(true)
                    .withUserNameClaim("preferred_username")
                    .withDisableTlsHostnameVerification(true)
                    .withTlsTrustedCertificates(cert)
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.valid.issuer.uri=\"https://valid-issuer\" oauth.jwks.endpoint.uri=\"https://jwks\" oauth.username.claim=\"preferred_username\" oauth.ssl.endpoint.identification.algorithm=\"\" oauth.config.id=\"PLAIN-9092\" oauth.ssl.truststore.location=\"/tmp/kafka/oauth-plain-9092.truststore.p12\" oauth.ssl.truststore.password=\"${strimzienv:CERTS_STORE_PASSWORD}\" oauth.ssl.truststore.type=\"PKCS12\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));
    }

    @ParallelTest
    public void testOauthConfigurationWithClientSecret()  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationOAuth()
                    .withValidIssuerUri("https://valid-issuer")
                    .withIntrospectionEndpointUri("https://intro")
                    .withCheckAudience(true)
                    .withCustomClaimCheck("'kafka-user' in @.roles.client-roles.kafka")
                    .withClientId("my-oauth-client")
                    .withNewClientSecret()
                        .withSecretName("my-secret")
                        .withKey("client-secret")
                    .endClientSecret()
                .endKafkaListenerAuthenticationOAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listeners=REPLICATION-9091://0.0.0.0:9091,PLAIN-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,PLAIN-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.plain-9092.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                "listener.name.plain-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" oauth.client.id=\"my-oauth-client\" oauth.valid.issuer.uri=\"https://valid-issuer\" oauth.check.audience=\"true\" oauth.custom.claim.check=\"'kafka-user' in @.roles.client-roles.kafka\" oauth.introspection.endpoint.uri=\"https://intro\" oauth.config.id=\"PLAIN-9092\" oauth.client.secret=\"${strimzienv:STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET}\";",
                "listener.name.plain-9092.sasl.enabled.mechanisms=OAUTHBEARER",
                "principal.builder.class=io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));
    }

    @ParallelTest
    public void testOAuthOptions()  {
        KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                .withValidIssuerUri("http://valid-issuer")
                .withCheckIssuer(false)
                .withCheckAudience(true)
                .withJwksEndpointUri("http://jwks-endpoint")
                .withIntrospectionEndpointUri("http://introspection-endpoint")
                .withServerBearerTokenLocation("/var/run/secrets/kubernetes.io/serviceaccount/token")
                .withUserInfoEndpointUri("http://userinfo-endpoint")
                .withJwksExpirySeconds(160)
                .withJwksRefreshSeconds(50)
                .withJwksMinRefreshPauseSeconds(5)
                .withJwksIgnoreKeyUse()
                .withEnableECDSA(true)
                .withUserNameClaim("preferred_username")
                .withUserNamePrefix("user-")
                .withFallbackUserNameClaim("client_id")
                .withFallbackUserNamePrefix("client-account-")
                .withCheckAccessTokenType(false)
                .withClientId("my-kafka-id")
                .withAccessTokenIsJwt(false)
                .withValidTokenType("access_token")
                .withDisableTlsHostnameVerification(true)
                .withMaxSecondsWithoutReauthentication(3600)
                .withEnablePlain(true)
                .withTokenEndpointUri("http://token")
                .withCustomClaimCheck("@.aud && @.aud == 'something'")
                .withConnectTimeoutSeconds(30)
                .withReadTimeoutSeconds(60)
                .withHttpRetries(2)
                .withHttpRetryPauseMs(500)
                .withClientAudience("kafka")
                .withClientScope("messaging")
                .withEnableMetrics(true)
                .withFailFast(false)
                .withIncludeAcceptHeader(false)
                .build();

        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put(ServerConfig.OAUTH_CLIENT_ID, "my-kafka-id");
        expectedOptions.put(ServerConfig.OAUTH_VALID_ISSUER_URI, "http://valid-issuer");
        expectedOptions.put(ServerConfig.OAUTH_CHECK_ISSUER, String.valueOf(false));
        expectedOptions.put(ServerConfig.OAUTH_CHECK_AUDIENCE, String.valueOf(true));
        expectedOptions.put(ServerConfig.OAUTH_CUSTOM_CLAIM_CHECK, "@.aud && @.aud == 'something'");
        expectedOptions.put(ServerConfig.OAUTH_SCOPE, "messaging");
        expectedOptions.put(ServerConfig.OAUTH_AUDIENCE, "kafka");
        expectedOptions.put(ServerConfig.OAUTH_JWKS_ENDPOINT_URI, "http://jwks-endpoint");
        expectedOptions.put(ServerConfig.OAUTH_JWKS_REFRESH_SECONDS, "50");
        expectedOptions.put(ServerConfig.OAUTH_JWKS_EXPIRY_SECONDS, "160");
        expectedOptions.put(ServerConfig.OAUTH_JWKS_REFRESH_MIN_PAUSE_SECONDS, "5");
        expectedOptions.put(ServerConfig.OAUTH_JWKS_IGNORE_KEY_USE, String.valueOf(true));
        expectedOptions.put(ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, "http://introspection-endpoint");
        expectedOptions.put(ServerConfig.OAUTH_SERVER_BEARER_TOKEN_LOCATION, "/var/run/secrets/kubernetes.io/serviceaccount/token");
        expectedOptions.put(ServerConfig.OAUTH_USERINFO_ENDPOINT_URI, "http://userinfo-endpoint");
        expectedOptions.put(ServerConfig.OAUTH_USERNAME_CLAIM, "preferred_username");
        expectedOptions.put(ServerConfig.OAUTH_USERNAME_PREFIX, "user-");
        expectedOptions.put(ServerConfig.OAUTH_FALLBACK_USERNAME_CLAIM, "client_id");
        expectedOptions.put(ServerConfig.OAUTH_FALLBACK_USERNAME_PREFIX, "client-account-");
        expectedOptions.put(ServerConfig.OAUTH_ACCESS_TOKEN_IS_JWT, String.valueOf(false));
        expectedOptions.put(ServerConfig.OAUTH_CHECK_ACCESS_TOKEN_TYPE, String.valueOf(false));
        expectedOptions.put(ServerConfig.OAUTH_VALID_TOKEN_TYPE, "access_token");
        expectedOptions.put(ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");
        expectedOptions.put(ServerConfig.OAUTH_CONNECT_TIMEOUT_SECONDS, "30");
        expectedOptions.put(ServerConfig.OAUTH_READ_TIMEOUT_SECONDS, "60");
        expectedOptions.put(ServerConfig.OAUTH_HTTP_RETRIES, "2");
        expectedOptions.put(ServerConfig.OAUTH_HTTP_RETRY_PAUSE_MILLIS, "500");
        expectedOptions.put(ServerConfig.OAUTH_ENABLE_METRICS, String.valueOf(true));
        expectedOptions.put(ServerConfig.OAUTH_FAIL_FAST, String.valueOf(false));
        expectedOptions.put(ServerConfig.OAUTH_INCLUDE_ACCEPT_HEADER, String.valueOf(false));

        // enablePlain and tokenEndpointUri are handled separately from getOAuthOptions
        Map<String, String> actualOptions = KafkaBrokerConfigurationBuilder.getOAuthOptions(auth);

        assertThat(actualOptions, is(equalTo(expectedOptions)));
    }

    @ParallelTest
    public void testCustomAuthConfigSetProtocolMapCorrectlyForsSslSasl() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_SSL"));
    }

    @ParallelTest
    public void testCustomAuthConfigSetProtocolMapCorrectlyForPlainSasl() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_PLAINTEXT"));
    }


    @ParallelTest
    public void testCustomAuthConfigSetProtocolMapCorrectlyForPlain() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(false)
                .withListenerConfig(Map.of())
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, containsString("listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:PLAINTEXT"));
    }

    @ParallelTest
    public void testCustomAuthConfigRemovesForbiddenPrefixes() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(false)
                .withListenerConfig(Map.of("ssl.keystore.path", "foo"))
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, not(containsString("ssl.keystore.path")));
    }

    @ParallelTest
    public void testCustomAuthConfigPrefixesUserProvidedConfig() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationCustomAuth()
                .withSasl(true)
                .withListenerConfig(Map.of("oauthbearer.sasl.client.callback.handler.class", "client.class",
                        "oauthbearer.sasl.server.callback.handler.class", "server.class",
                        "oauthbearer.sasl.login.callback.handler.class", "login.class",
                        "oauthbearer.connections.max.reauth.ms", 999999999,
                        "sasl.enabled.mechanisms", "oauthbearer",
                        "oauthbearer.sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;"))
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listener.name.custom-listener-9092.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.custom-listener-9092.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.custom-listener-9092.ssl.keystore.type=PKCS12",
                "listeners=REPLICATION-9091://0.0.0.0:9091,CUSTOM-LISTENER-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,CUSTOM-LISTENER-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SASL_SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS",
                "listener.name.custom-listener-9092.sasl.enabled.mechanisms=oauthbearer",
                "listener.name.custom-listener-9092.oauthbearer.sasl.client.callback.handler.class=client.class",
                "listener.name.custom-listener-9092.oauthbearer.sasl.server.callback.handler.class=server.class",
                "listener.name.custom-listener-9092.oauthbearer.sasl.login.callback.handler.class=login.class",
                "listener.name.custom-listener-9092.oauthbearer.connections.max.reauth.ms=999999999",
                "listener.name.custom-listener-9092.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;"));
    }

    @ParallelTest
    public void testCustomTlsAuth() {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("CUSTOM-LISTENER")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withNewKafkaListenerAuthenticationCustomAuth()
                    .withSasl(false)
                    .withListenerConfig(Map.of("ssl.client.auth", "required",
                            "ssl.principal.mapping.rules", "RULE:^CN=(.*?),(.*)$/CN=$1/",
                            "ssl.truststore.location", "/opt/kafka/custom-authn-secrets/custom-listener-external-9094/custom-truststore/ca.crt",
                            "ssl.truststore.type", "PEM"))
                .endKafkaListenerAuthenticationCustomAuth()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withListeners("my-cluster", KAFKA_3_8_0, "my-namespace", singletonList(listener), listenerId -> "dummy-advertised-address", listenerId -> "1919")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "listener.name.controlplane-9090.ssl.client.auth=required",
                "listener.name.controlplane-9090.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.controlplane-9090.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.keystore.type=PKCS12",
                "listener.name.controlplane-9090.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.controlplane-9090.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.controlplane-9090.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.replication-9091.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.keystore.type=PKCS12",
                "listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "listener.name.replication-9091.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.replication-9091.ssl.truststore.type=PKCS12",
                "listener.name.replication-9091.ssl.client.auth=required",
                "listener.name.custom-listener-9092.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "listener.name.custom-listener-9092.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "listener.name.custom-listener-9092.ssl.keystore.type=PKCS12",
                "listener.name.custom-listener-9092.ssl.truststore.location=/opt/kafka/custom-authn-secrets/custom-listener-external-9094/custom-truststore/ca.crt",
                "listener.name.custom-listener-9092.ssl.truststore.type=PEM",
                "listener.name.custom-listener-9092.ssl.client.auth=required",
                "listener.name.custom-listener-9092.ssl.principal.mapping.rules=RULE:^CN=(.*?),(.*)$/CN=$1/",
                "listeners=REPLICATION-9091://0.0.0.0:9091,CUSTOM-LISTENER-9092://0.0.0.0:9092",
                "advertised.listeners=REPLICATION-9091://my-cluster-kafka-2.my-cluster-kafka-brokers.my-namespace.svc:9091,CUSTOM-LISTENER-9092://dummy-advertised-address:1919",
                "listener.security.protocol.map=CONTROLPLANE-9090:SSL,REPLICATION-9091:SSL,CUSTOM-LISTENER-9092:SSL",
                "inter.broker.listener.name=REPLICATION-9091",
                "sasl.enabled.mechanisms=",
                "ssl.endpoint.identification.algorithm=HTTPS"));
    }

    @ParallelTest
    public void testOAuthDefaultOptions()  {
        KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                .build();

        Map<String, String> actualOptions = KafkaBrokerConfigurationBuilder.getOAuthOptions(auth);
        assertThat(actualOptions, is(equalTo(Collections.emptyMap())));
    }

    @ParallelTest
    public void testWithTieredStorage() {
        TieredStorageCustom tieredStorage = new TieredStorageCustom();
        RemoteStorageManager rsm = new RemoteStorageManager();
        rsm.setClassName("com.example.kafka.tiered.storage.s3.S3RemoteStorageManager");
        rsm.setClassPath("/opt/kafka/plugins/tiered-storage-s3/*");
        Map<String, String> rsmConfigs = new HashMap<>();
        rsmConfigs.put("storage.bucket.name", "my-bucket");
        rsm.setConfig(rsmConfigs);
        tieredStorage.setRemoteStorageManager(rsm);
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withTieredStorage("test-cluster-1", tieredStorage)
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "remote.log.storage.system.enable=true",
                "remote.log.metadata.manager.impl.prefix=rlmm.config.",
                "remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager",
                "remote.log.metadata.manager.listener.name=REPLICATION-9091",
                "rlmm.config.remote.log.metadata.common.client.bootstrap.servers=test-cluster-1-kafka-brokers:9091",
                "rlmm.config.remote.log.metadata.common.client.security.protocol=SSL",
                "rlmm.config.remote.log.metadata.common.client.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "rlmm.config.remote.log.metadata.common.client.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "rlmm.config.remote.log.metadata.common.client.ssl.keystore.type=PKCS12",
                "rlmm.config.remote.log.metadata.common.client.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "rlmm.config.remote.log.metadata.common.client.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "rlmm.config.remote.log.metadata.common.client.ssl.truststore.type=PKCS12",
                "remote.log.storage.manager.class.name=com.example.kafka.tiered.storage.s3.S3RemoteStorageManager",
                "remote.log.storage.manager.class.path=/opt/kafka/plugins/tiered-storage-s3/*",
                "remote.log.storage.manager.impl.prefix=rsm.config.",
                "rsm.config.storage.bucket.name=my-bucket"
        ));
    }

    @ParallelTest
    public void testSimpleAuthorizationOnMigration() {
        NodeRef broker = new NodeRef("my-cluster-brokers-0", 0, "brokers", false, true);
        NodeRef controller = new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false);

        KafkaAuthorization auth = new KafkaAuthorizationSimpleBuilder()
                .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, controller)
                .withAuthorization("my-cluster", auth)
                .build();
        assertThat(configuration, isEquivalent("node.id=1",
                "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
                "super.users=User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-topic-operator,O=io.strimzi;User:CN=my-cluster-entity-user-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:CN=my-cluster-cruise-control,O=io.strimzi;User:CN=cluster-operator,O=io.strimzi"));

        configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, broker)
                .withAuthorization("my-cluster", auth)
                .build();
        assertThat(configuration, containsString("authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"));
    }

    @ParallelTest
    public void testWithStrimziQuotas() {
        QuotasPluginStrimzi quotasPluginStrimzi = new QuotasPluginStrimziBuilder()
            .withConsumerByteRate(1000L)
            .withProducerByteRate(1000L)
            .withExcludedPrincipals("User:my-user1", "User:my-user2")
            .withMinAvailableBytesPerVolume(200000L)
            .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withQuotas("my-personal-cluster", quotasPluginStrimzi)
            .build();

        assertThat(configuration, isEquivalent("node.id=2",
            "client.quota.callback.class=io.strimzi.kafka.quotas.StaticQuotaCallback",
            "client.quota.callback.static.kafka.admin.bootstrap.servers=my-personal-cluster-kafka-brokers:9091",
            "client.quota.callback.static.kafka.admin.security.protocol=SSL",
            "client.quota.callback.static.kafka.admin.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
            "client.quota.callback.static.kafka.admin.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "client.quota.callback.static.kafka.admin.ssl.keystore.type=PKCS12",
            "client.quota.callback.static.kafka.admin.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
            "client.quota.callback.static.kafka.admin.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
            "client.quota.callback.static.kafka.admin.ssl.truststore.type=PKCS12",
            "client.quota.callback.static.produce=1000",
            "client.quota.callback.static.fetch=1000",
            "client.quota.callback.static.storage.per.volume.limit.min.available.bytes=200000",
            "client.quota.callback.static.excluded.principal.name.list=User:CN=my-personal-cluster-kafka,O=io.strimzi;User:CN=my-personal-cluster-cruise-control,O=io.strimzi;User:my-user1;User:my-user2"
        ));
    }

    @ParallelTest
    public void testWithNullQuotas() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withQuotas("my-personal-cluster", null)
            .build();

        assertThat(configuration, not(containsString("client.quota.callback.class")));
        assertThat(configuration, not(containsString("client.quota.callback.static")));
    }

    @ParallelTest
    public void testWithKafkaQuotas() {
        QuotasPluginKafka quotasPluginKafka = new QuotasPluginKafkaBuilder()
            .withConsumerByteRate(1000L)
            .withProducerByteRate(1000L)
            .withRequestPercentage(33)
            .withControllerMutationRate(0.5)
            .build();

        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
            .withQuotas("my-personal-cluster", quotasPluginKafka)
            .build();

        assertThat(configuration, not(containsString("client.quota.callback.class")));
        assertThat(configuration, not(containsString("client.quota.callback.static")));
    }

    @ParallelTest
    public void testWithKRaftMetadataLogDir() {
        String configuration = new KafkaBrokerConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withKRaftMetadataLogDir("/my/kraft/metadata")
                .build();

        assertThat(configuration, isEquivalent("node.id=2",
                "metadata.log.dir=/my/kraft/metadata/kafka-log2"
        ));
    }
}
