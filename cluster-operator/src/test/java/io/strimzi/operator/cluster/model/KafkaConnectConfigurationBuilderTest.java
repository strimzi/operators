/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.ClientTlsBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlainBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256Builder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512Builder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.operator.cluster.TestUtils.IsEquivalent.isEquivalent;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
class KafkaConnectConfigurationBuilderTest {

    private static final String BOOTSTRAP_SERVERS = "my-cluster-kafka-bootstrap:9092";

    @ParallelTest
    public void testBuild()  {
        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS).build();
        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT"
        ));
    }

    @ParallelTest
    public void testWithTls() {
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withTls(clientTls)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SSL",
                "producer.security.protocol=SSL",
                "consumer.security.protocol=SSL",
                "admin.security.protocol=SSL",
                "ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "ssl.truststore.type=PKCS12",
                "producer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "producer.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "consumer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "consumer.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "admin.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "admin.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}"
        ));
    }

    @ParallelTest
    public void testWithTlsAndClientAuthentication() {
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        KafkaClientAuthenticationTls tlsAuth = new KafkaClientAuthenticationTlsBuilder()
                .withNewCertificateAndKey()
                    .withSecretName("tls-keystore")
                    .withCertificate("pem-content")
                .endCertificateAndKey()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withTls(clientTls)
                .withAuthentication(tlsAuth)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SSL",
                "producer.security.protocol=SSL",
                "consumer.security.protocol=SSL",
                "admin.security.protocol=SSL",
                "ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "ssl.truststore.type=PKCS12",
                "producer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "producer.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "consumer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "consumer.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "admin.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "admin.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "ssl.keystore.type=PKCS12",
                "producer.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "producer.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "producer.ssl.keystore.type=PKCS12",
                "consumer.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "consumer.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "consumer.ssl.keystore.type=PKCS12",
                "admin.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12",
                "admin.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "admin.ssl.keystore.type=PKCS12"
        ));
    }

    @ParallelTest
    public void testWithPlainAndSaslMechanism() {
        KafkaClientAuthenticationPlain authPlain = new KafkaClientAuthenticationPlainBuilder()
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withAuthentication(authPlain)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_PLAINTEXT",
                "producer.security.protocol=SASL_PLAINTEXT",
                "consumer.security.protocol=SASL_PLAINTEXT",
                "admin.security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=PLAIN",
                "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "producer.sasl.mechanism=PLAIN",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "consumer.sasl.mechanism=PLAIN",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "admin.sasl.mechanism=PLAIN",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};"
        ));
    }

    @ParallelTest
    public void testWithTlsAndSaslMechanism() {
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        KafkaClientAuthenticationPlain authPlain = new KafkaClientAuthenticationPlainBuilder()
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withTls(clientTls)
                .withAuthentication(authPlain)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_SSL",
                "producer.security.protocol=SASL_SSL",
                "consumer.security.protocol=SASL_SSL",
                "admin.security.protocol=SASL_SSL",
                "ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "ssl.truststore.type=PKCS12",
                "producer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "producer.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "consumer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "consumer.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "admin.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "admin.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "sasl.mechanism=PLAIN",
                "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "producer.sasl.mechanism=PLAIN",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "consumer.sasl.mechanism=PLAIN",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "admin.sasl.mechanism=PLAIN",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};"
        ));
    }

    @ParallelTest
    public void testWithPlainAndScramSha256() {
        KafkaClientAuthenticationScramSha256 authScramSha256 = new KafkaClientAuthenticationScramSha256Builder()
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withAuthentication(authScramSha256)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_PLAINTEXT",
                "producer.security.protocol=SASL_PLAINTEXT",
                "consumer.security.protocol=SASL_PLAINTEXT",
                "admin.security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=SCRAM-SHA-256",
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "producer.sasl.mechanism=SCRAM-SHA-256",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "consumer.sasl.mechanism=SCRAM-SHA-256",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "admin.sasl.mechanism=SCRAM-SHA-256",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};"
        ));
    }

    @ParallelTest
    public void testWithTlsAndScramSha256() {
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        KafkaClientAuthenticationScramSha256 authScramSha256 = new KafkaClientAuthenticationScramSha256Builder()
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withTls(clientTls)
                .withAuthentication(authScramSha256)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_SSL",
                "producer.security.protocol=SASL_SSL",
                "consumer.security.protocol=SASL_SSL",
                "admin.security.protocol=SASL_SSL",
                "ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "ssl.truststore.type=PKCS12",
                "producer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "producer.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "consumer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "consumer.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "admin.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12",
                "admin.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "sasl.mechanism=SCRAM-SHA-256",
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "producer.sasl.mechanism=SCRAM-SHA-256",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "consumer.sasl.mechanism=SCRAM-SHA-256",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "admin.sasl.mechanism=SCRAM-SHA-256",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};"
        ));
    }

    @ParallelTest
    public void testWithPlainAndScramSha512() {
        KafkaClientAuthenticationScramSha512 authScramSha512 = new KafkaClientAuthenticationScramSha512Builder()
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withAuthentication(authScramSha512)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_PLAINTEXT",
                "producer.security.protocol=SASL_PLAINTEXT",
                "consumer.security.protocol=SASL_PLAINTEXT",
                "admin.security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=SCRAM-SHA-512",
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "producer.sasl.mechanism=SCRAM-SHA-512",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "consumer.sasl.mechanism=SCRAM-SHA-512",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};",
                "admin.sasl.mechanism=SCRAM-SHA-512",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_CONNECT_SASL_USERNAME} password=${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key};"
        ));
    }

    @ParallelTest
    public void testWithAuthOauth() {
        KafkaClientAuthenticationOAuth authOAuth = new KafkaClientAuthenticationOAuthBuilder()
                .withClientId("oauth-client-id")
                .withTokenEndpointUri("http://token-endpoint-uri")
                .withUsername("oauth-username")
                .withNewClientSecret()
                    .withSecretName("my-client-secret-secret")
                    .withKey("my-client-secret-key")
                .endClientSecret()
                .withNewRefreshToken()
                    .withSecretName("my-refresh-token-secret")
                    .withKey("my-refresh-token-key")
                .endRefreshToken()
                .withNewAccessToken()
                    .withSecretName("my-access-token-secret")
                    .withKey("my-access-token-key")
                .endAccessToken()
                .withNewPasswordSecret()
                    .withSecretName("my-password-secret-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .addNewTlsTrustedCertificate()
                    .withSecretName("my-tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTlsTrustedCertificate()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withAuthentication(authOAuth)
                .build();

        String saslJaasConfig = "sasl.jaas.config=" +
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ${strimzienv:KAFKA_CONNECT_OAUTH_CONFIG}" +
                " oauth.client.secret=${strimzienv:KAFKA_CONNECT_OAUTH_CLIENT_SECRET}" +
                " oauth.refresh.token=${strimzienv:KAFKA_CONNECT_OAUTH_REFRESH_TOKEN}" +
                " oauth.access.token=${strimzienv:KAFKA_CONNECT_OAUTH_ACCESS_TOKEN}" +
                " oauth.password.grant.password=${strimzienv:KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD}" +
                " oauth.ssl.truststore.location=\"/tmp/kafka/oauth.truststore.p12\" oauth.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD} oauth.ssl.truststore.type=\"PKCS12\";";

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_PLAINTEXT",
                "producer.security.protocol=SASL_PLAINTEXT",
                "consumer.security.protocol=SASL_PLAINTEXT",
                "admin.security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=OAUTHBEARER",
                saslJaasConfig,
                "sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "producer.sasl.mechanism=OAUTHBEARER",
                "producer." + saslJaasConfig,
                "producer.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "consumer.sasl.mechanism=OAUTHBEARER",
                "consumer." + saslJaasConfig,
                "consumer.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "admin.sasl.mechanism=OAUTHBEARER",
                "admin." + saslJaasConfig,
                "admin.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"
                ));
    }

    @ParallelTest
    public void testWithRackId() {
        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withRackId()
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "consumer.client.rack=${strimzidir:/opt/kafka/init:rack.id}"
        ));

    }

    @ParallelTest
    public void testWithConfigProviders() {
        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withConfigurations(null)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka"
        ));
    }

    @ParallelTest
    public void testWithUserProvidedAndDefaultConfigurations() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("myconfig", "abc");
        userConfiguration.put("myconfig2", 123);
        KafkaConnectConfiguration configurations = new KafkaConnectConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withConfigurations(configurations)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "config.providers=strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "myconfig=abc",
                "myconfig2=123",
                "group.id=connect-cluster",
                "offset.storage.topic=connect-cluster-offsets",
                "config.storage.topic=connect-cluster-configs",
                "status.storage.topic=connect-cluster-status",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "value.converter=org.apache.kafka.connect.json.JsonConverter")
        );
    }

    @ParallelTest
    public void testWithUserProvidedConfigMaps() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("config.providers", "userenv");
        userConfiguration.put("config.providers.userenv.class", "org.apache.kafka.common.config.provider.EnvVarConfigProvider");
        KafkaConnectConfiguration configurations = new KafkaConnectConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withConfigurations(configurations)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "config.providers=userenv,strimzienv,strimzifile,strimzidir",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.userenv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "group.id=connect-cluster",
                "offset.storage.topic=connect-cluster-offsets",
                "config.storage.topic=connect-cluster-configs",
                "status.storage.topic=connect-cluster-status",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "value.converter=org.apache.kafka.connect.json.JsonConverter")
        );
    }

    @ParallelTest
    public void testWithRestListeners() {
        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withRestListeners(8083)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "rest.advertised.host.name=${strimzienv:ADVERTISED_HOSTNAME}",
                "rest.advertised.port=8083"
        ));
    }

    @ParallelTest
    public void withPluginPath() {
        String configuration = new KafkaConnectConfigurationBuilder(BOOTSTRAP_SERVERS)
                .withPluginPath().build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "plugin.path=/opt/kafka/plugins"
        ));
    }
}