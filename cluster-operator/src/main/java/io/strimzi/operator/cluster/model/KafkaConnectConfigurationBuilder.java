/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;


import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;

import java.io.PrintWriter;
import java.io.StringWriter;

import static io.strimzi.operator.cluster.model.KafkaConnectCluster.PASSWORD_VOLUME_MOUNT;

/**
 * This class is used to generate the Connect configuration template. The template is later passed using a config map to
 * the connect pods. The scripts in the container images will fill in the variables in the template and use the
 * configuration file. This class is using the builder pattern to make it easy to test the different parts etc. To
 * generate the configuration file, it is using the PrintWriter.
 */
public class KafkaConnectConfigurationBuilder {
    // Names of environment variables expanded through config providers inside Connect node
    private final static String PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:CERTS_STORE_PASSWORD}";
    private final static String PLACEHOLDER_SASL_USERNAME_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:KAFKA_CONNECT_SASL_USERNAME}";
    // the SASL password file template includes: <volume_mount>/<secret_name>/<password_file>
    private static final String PLACEHOLDER_SASL_PASSWORD_FILE_TEMPLATE_CONFIG_PROVIDER_DIR = "${strimzidir:%s%s:%s}";

    private final StringWriter stringWriter = new StringWriter();
    private final PrintWriter writer = new PrintWriter(stringWriter);
    private String securityProtocol = "PLAINTEXT";

    /**
     * Connect configuration template constructor
     *
     * @param bootstrapServers  Kafka cluster bootstrap servers to connect to
     */
    public KafkaConnectConfigurationBuilder(String bootstrapServers) {
        printHeader();
        configureBootstrapServers(bootstrapServers);
    }

    /**
     * Renders the Kafka cluster bootstrap servers configuration
     *
     * @param bootstrapServers  Kafka cluster bootstrap servers to connect to
     */
    private void configureBootstrapServers(String bootstrapServers) {
        printSectionHeader("Bootstrap servers");
        writer.println("bootstrap.servers=" + bootstrapServers);
        writer.println();
    }

    /**
     * Configure the Kafka security protocol to be used
     * This internal method is used when the configuration is build, because the security protocol depends on
     * TLS and SASL authentication configurations and if they are set
     */
    private void configureSecurityProtocol() {
        printSectionHeader("Kafka Security protocol");
        writer.println("security.protocol=" + securityProtocol);
        writer.println("producer.security.protocol=" + securityProtocol);
        writer.println("consumer.security.protocol=" + securityProtocol);
        writer.println("admin.security.protocol=" + securityProtocol);
        writer.println();
    }

    /**
     * Adds the TLS/SSL configuration for connecting to the Kafka cluster.
     * The configuration includes the trusted certificates store for TLS connection (server authentication)
     *
     * @param tls   client TLS configuration
     * @return  the builder instance
     */
    public KafkaConnectConfigurationBuilder withTls(ClientTls tls) {
        if (tls != null) {
            securityProtocol = "SSL";

            if (tls.getTrustedCertificates() != null && !tls.getTrustedCertificates().isEmpty()) {
                printSectionHeader("TLS / SSL");
                writer.println("ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
                writer.println("ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
                writer.println("ssl.truststore.type=PKCS12");

                writer.println("producer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
                writer.println("producer.ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);

                writer.println("consumer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
                writer.println("consumer.ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);

                writer.println("admin.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
                writer.println("admin.ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);

                writer.println();
            }

        }
        return this;
    }

    /**
     * Adds keystore configuration for mTLS (client authentication)
     * or the SASL configuration for client authentication to the Kafka cluster
     *
     * @param authentication authentication configuration
     * @return  the builder instance
     */
    public KafkaConnectConfigurationBuilder withAuthentication(KafkaClientAuthentication authentication) {
        if (authentication != null) {
            printSectionHeader("Authentication configuration");
            // configuring mTLS (client TLS authentication) if TLS client authentication is set
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth && tlsAuth.getCertificateAndKey() != null) {
                writer.println("ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
                writer.println("ssl.keystore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
                writer.println("ssl.keystore.type=PKCS12");

                writer.println("producer.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
                writer.println("producer.ssl.keystore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
                writer.println("producer.ssl.keystore.type=PKCS12");

                writer.println("consumer.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
                writer.println("consumer.ssl.keystore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
                writer.println("consumer.ssl.keystore.type=PKCS12");

                writer.println("admin.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
                writer.println("admin.ssl.keystore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
                writer.println("admin.ssl.keystore.type=PKCS12");
                // otherwise SASL or OAuth is going to be used for authentication
            } else {
                securityProtocol = securityProtocol.equals("SSL") ? "SASL_SSL" : "SASL_PLAINTEXT";
                String saslMechanism = null;
                StringBuilder jaasConfig = new StringBuilder();
                String oauthCallbackClass = "";
                String producerOauthCallbackClass = "";
                String consumerOauthCallbackClass = "";
                String adminOauthCallbackClass = "";

                if (authentication instanceof KafkaClientAuthenticationPlain passwordAuth) {
                    saslMechanism = "PLAIN";
                    String passwordFilePath = String.format(PLACEHOLDER_SASL_PASSWORD_FILE_TEMPLATE_CONFIG_PROVIDER_DIR, PASSWORD_VOLUME_MOUNT, passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getPassword());
                    jaasConfig.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=" + PLACEHOLDER_SASL_USERNAME_CONFIG_PROVIDER_ENV_VAR + " password=" + passwordFilePath + ";");
                } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {
                    if (scramAuth.getType().equals(KafkaClientAuthenticationScramSha256.TYPE_SCRAM_SHA_256)) {
                        saslMechanism = "SCRAM-SHA-256";
                    } else if (scramAuth.getType().equals(KafkaClientAuthenticationScramSha512.TYPE_SCRAM_SHA_512)) {
                        saslMechanism = "SCRAM-SHA-512";
                    }
                    String passwordFilePath = String.format(PLACEHOLDER_SASL_PASSWORD_FILE_TEMPLATE_CONFIG_PROVIDER_DIR, PASSWORD_VOLUME_MOUNT, scramAuth.getPasswordSecret().getSecretName(), scramAuth.getPasswordSecret().getPassword());
                    jaasConfig.append("org.apache.kafka.common.security.scram.ScramLoginModule required username=" + PLACEHOLDER_SASL_USERNAME_CONFIG_PROVIDER_ENV_VAR + " password=" + passwordFilePath + ";");
                } else if (authentication instanceof KafkaClientAuthenticationOAuth oauth) {
                    saslMechanism = "OAUTHBEARER";
                    jaasConfig.append("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ${strimzienv:KAFKA_CONNECT_OAUTH_CONFIG}");

                    if (oauth.getClientSecret() != null) {
                        jaasConfig.append(" oauth.client.secret=${strimzienv:KAFKA_CONNECT_OAUTH_CLIENT_SECRET}");
                    }

                    if (oauth.getRefreshToken() != null) {
                        jaasConfig.append(" oauth.refresh.token=${strimzienv:KAFKA_CONNECT_OAUTH_REFRESH_TOKEN}");
                    }

                    if (oauth.getAccessToken() != null) {
                        jaasConfig.append(" oauth.access.token=${strimzienv:KAFKA_CONNECT_OAUTH_ACCESS_TOKEN}");
                    }

                    if (oauth.getPasswordSecret() != null) {
                        jaasConfig.append(" oauth.password.grant.password=${strimzienv:KAFKA_CONNECT_OAUTH_PASSWORD_GRANT_PASSWORD}");
                    }

                    if (oauth.getClientAssertion() != null) {
                        jaasConfig.append(" oauth.client.assertion=${strimzienv:KAFKA_CONNECT_OAUTH_CLIENT_ASSERTION}");
                    }

                    if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
                        jaasConfig.append(" oauth.ssl.truststore.location=\"/tmp/kafka/oauth.truststore.p12\" oauth.ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR + " oauth.ssl.truststore.type=\"PKCS12\"");
                    }

                    jaasConfig.append(";");
                    oauthCallbackClass = "sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler";
                    producerOauthCallbackClass = "producer." + oauthCallbackClass;
                    consumerOauthCallbackClass = "consumer." + oauthCallbackClass;
                    adminOauthCallbackClass = "admin." + oauthCallbackClass;

                }
                writer.println("sasl.mechanism=" + saslMechanism);
                writer.println("sasl.jaas.config=" + jaasConfig);
                writer.println(oauthCallbackClass);

                writer.println("producer.sasl.mechanism=" + saslMechanism);
                writer.println("producer.sasl.jaas.config=" + jaasConfig);
                writer.println(producerOauthCallbackClass);

                writer.println("consumer.sasl.mechanism=" + saslMechanism);
                writer.println("consumer.sasl.jaas.config=" + jaasConfig);
                writer.println(consumerOauthCallbackClass);

                writer.println("admin.sasl.mechanism=" + saslMechanism);
                writer.println("admin.sasl.jaas.config=" + jaasConfig);
                writer.println(adminOauthCallbackClass);

                writer.println();
            }
        }
        return this;
    }

    /**
     * Adds consumer client rack {@code rack.id}. The rack ID will be set in the container based on the value of the
     * rack.id file (if it exists). This file is generated by the init-container used when rack awareness is enabled.
     *
     * @return Returns the builder instance
     */
    public KafkaConnectConfigurationBuilder withRackId()   {
        printSectionHeader("Additional information");
        writer.println("consumer.client.rack=${strimzidir:/opt/kafka/init:rack.id}");
        writer.println();
        return this;
    }

    /**
     * Configures the Kafka Connect configuration providers
     *
     * @param userConfig    the user configuration to extract the possible user-provided config provider configuration from it
     */
    public void configProviders(AbstractConfiguration userConfig) {
        printSectionHeader("Config providers");
        String strimziConfigProviders = "strimzienv,strimzifile,strimzidir";

        if (userConfig != null && !userConfig.getConfiguration().isEmpty() && userConfig.getConfigOption("config.providers") != null) {
            writer.println("# Configuration providers configured by the user and by Strimzi");
            writer.println("config.providers=" + userConfig.getConfigOption("config.providers") + "," + strimziConfigProviders);
            userConfig.removeConfigOption("config.providers");
        } else {
            writer.println("# Configuration providers configured by Strimzi");
            writer.println("config.providers=" + strimziConfigProviders);
        }

        writer.println("config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider");
        writer.println("config.providers.strimzienv.param.allowlist.pattern=.*");
        writer.println("config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider");
        writer.println("config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider");
        writer.println("config.providers.strimzidir.param.allowed.paths=/opt/kafka");
        writer.println();
    }

    /**
     * Adds user provided Kafka Connect configurations.
     *
     * @param configurations   User provided Kafka Connect configurations
     *
     * @return Returns the builder instance
     */
    public KafkaConnectConfigurationBuilder withConfigurations(AbstractConfiguration configurations) {
        configProviders(configurations);

        if (configurations != null && !configurations.getConfiguration().isEmpty()) {
            printSectionHeader("Provided configurations");
            writer.println(configurations.getConfiguration());
            writer.println();
        }
        return this;
    }

    /**
     * Configures the rest API listener.
     *
     * @param port Rest API port
     * @return Returns the builder instance
     */
    public KafkaConnectConfigurationBuilder withRestListeners(int port)  {
        printSectionHeader("REST Listeners");
        writer.println("rest.advertised.host.name=${strimzienv:ADVERTISED_HOSTNAME}");
        writer.println("rest.advertised.port=" + port);
        writer.println();

        return this;
    }

    /**
     * Configures plugins.
     *
     * @return Returns the builder instance
     */
    public KafkaConnectConfigurationBuilder withPluginPath() {
        printSectionHeader("Plugins");
        writer.println("plugin.path=/opt/kafka/plugins");
        writer.println();

        return  this;
    }


    /**
     * Internal method which prints the section header into the configuration file. This makes it more human-readable
     * when looking for issues in running pods etc.
     *
     * @param sectionName   Name of the section for which is this header printed
     */
    private void printSectionHeader(String sectionName)   {
        writer.println("##########");
        writer.println("# " + sectionName);
        writer.println("##########");
    }

    /**
     * Prints the file header which is on the beginning of the configuration file.
     */
    private void printHeader()   {
        writer.println("##############################");
        writer.println("##############################");
        writer.println("# This file is automatically generated by the Strimzi Cluster Operator");
        writer.println("# Any changes to this file will be ignored and overwritten!");
        writer.println("##############################");
        writer.println("##############################");
        writer.println();
    }

    /**
     * Generates the configuration template as String
     *
     * @return String with the Kafka connect configuration template
     */
    public String build()  {
        configureSecurityProtocol();
        return stringWriter.toString();
    }
}
