/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.GenericSecretSource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utils for working with different authentication types
 */
public class AuthenticationUtils {
    /**
     * Key for a SASL username
     */
    public static final String SASL_USERNAME = "SASL_USERNAME";

    /**
     * Key for a SASL mechanism
     */
    public static final String SASL_MECHANISM = "SASL_MECHANISM";

    private static final String TLS_AUTH_CERT = "TLS_AUTH_CERT";
    private static final String TLS_AUTH_KEY = "TLS_AUTH_KEY";
    private static final String SASL_PASSWORD_FILE = "SASL_PASSWORD_FILE";
    private static final String OAUTH_CONFIG = "OAUTH_CONFIG";

    /**
     * Validates Kafka client authentication for all components based on Apache Kafka clients.
     *
     * @param authentication    The authentication object from CRD
     * @param tls   Indicates whether TLS is enabled or not
     *
     * @return  A warning message
     */
    @SuppressWarnings("BooleanExpressionComplexity")
    public static String validateClientAuthentication(KafkaClientAuthentication authentication, boolean tls) {
        String warnMsg = "";
        if (authentication != null)   {
            if (authentication instanceof KafkaClientAuthenticationTls auth) {
                if (auth.getCertificateAndKey() != null) {
                    if (!tls) {
                        warnMsg = "TLS configuration missing: related TLS client authentication will not work properly";
                    }
                } else {
                    throw new InvalidResourceException("TLS Client authentication selected, but no certificate and key configured.");
                }
            } else if (authentication instanceof KafkaClientAuthenticationScram auth)    {
                if (auth.getUsername() == null || auth.getPasswordSecret() == null) {
                    throw new InvalidResourceException(String.format("%s authentication selected, but username or password configuration is missing.", auth.getType().toUpperCase(Locale.ENGLISH)));
                }
            } else if (authentication instanceof KafkaClientAuthenticationPlain auth) {
                if (auth.getUsername() == null || auth.getPasswordSecret() == null) {
                    throw new InvalidResourceException("PLAIN authentication selected, but username or password configuration is missing.");
                }
            } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                validateClientAuthenticationOAuth((KafkaClientAuthenticationOAuth) authentication);
            }
        }
        return warnMsg;
    }

    private static void validateClientAuthenticationOAuth(KafkaClientAuthenticationOAuth auth) {
        boolean accessTokenCase = auth.getAccessToken() != null || auth.getAccessTokenLocation() != null;
        boolean accessTokenLocationCase = auth.getAccessTokenLocation() != null;
        boolean refreshTokenCase = auth.getTokenEndpointUri() != null && auth.getClientId() != null && auth.getRefreshToken() != null;
        boolean clientSecretCase = auth.getTokenEndpointUri() != null && auth.getClientId() != null && auth.getClientSecret() != null;
        boolean passwordGrantCase = auth.getTokenEndpointUri() != null && auth.getClientId() != null && auth.getUsername() != null && auth.getPasswordSecret() != null;
        boolean clientAssertionCase = auth.getTokenEndpointUri() != null && auth.getClientId() != null && (auth.getClientAssertion() != null || auth.getClientAssertionLocation() != null);

        // If not one of valid cases throw exception
        if (!(accessTokenCase || accessTokenLocationCase || refreshTokenCase || clientSecretCase || passwordGrantCase || clientAssertionCase)) {
            throw new InvalidResourceException("OAUTH authentication selected, but some options are missing. You have to specify one of the following combinations: [accessToken], [accessTokenLocation], [tokenEndpointUri, clientId, refreshToken], [tokenEndpointUri, clientId, clientSecret], [tokenEndpointUri, clientId, clientAssertion], [tokenEndpointUri, clientId, clientAssertionLocation], [tokenEndpointUri, username, password, clientId].");
        }

        // Additional validation
        ArrayList<String> errors = new ArrayList<>();
        checkValueGreaterThanZero(errors, "connectTimeoutSeconds", auth.getConnectTimeoutSeconds());
        checkValueGreaterThanZero(errors, "readTimeoutSeconds", auth.getReadTimeoutSeconds());
        checkValueGreaterOrEqualZero(errors, "httpRetries", auth.getHttpRetries());
        checkValueGreaterOrEqualZero(errors, "httpRetryPauseMs", auth.getHttpRetryPauseMs());

        if (errors.size() > 0) {
            throw new InvalidResourceException("OAUTH authentication selected, but some options are invalid. " + errors);
        }
    }

    private static void checkValueGreaterThanZero(ArrayList<String> errors, String name, Integer value) {
        if (value != null && value <= 0) {
            errors.add("If specified, '" + name + "' has to be greater than 0");
        }
    }

    private static void checkValueGreaterOrEqualZero(ArrayList<String> errors, String name, Integer value) {
        if (value != null && value < 0) {
            errors.add("If specified, '" + name + "' has to be greater or equal 0");
        }
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components
     * @param authentication    Authentication object from CRD
     * @param volumeList    List where the volumes will be added
     * @param oauthVolumeNamePrefix Prefix used for OAuth volumes
     * @param isOpenShift   Indicates whether we run on OpenShift or not
     */
    public static void configureClientAuthenticationVolumes(KafkaClientAuthentication authentication, List<Volume> volumeList, String oauthVolumeNamePrefix, boolean isOpenShift)   {
        configureClientAuthenticationVolumes(authentication, volumeList, oauthVolumeNamePrefix, isOpenShift, "", false);
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components
     *
     * @param authentication    Authentication object from CRD
     * @param volumeList    List where the volumes will be added
     * @param oauthVolumeNamePrefix Prefix used for OAuth volumes
     * @param isOpenShift   Indicates whether we run on OpenShift or not
     * @param volumeNamePrefix Prefix used for volume names
     * @param createOAuthSecretVolumes   Indicates whether OAuth secret volumes will be added to the list
     */
    public static void configureClientAuthenticationVolumes(KafkaClientAuthentication authentication, List<Volume> volumeList, String oauthVolumeNamePrefix, boolean isOpenShift, String volumeNamePrefix, boolean createOAuthSecretVolumes)   {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth) {
                addNewVolume(volumeList, volumeNamePrefix, tlsAuth.getCertificateAndKey().getSecretName(), isOpenShift);
            } else if (authentication instanceof KafkaClientAuthenticationPlain passwordAuth) {
                addNewVolume(volumeList, volumeNamePrefix, passwordAuth.getPasswordSecret().getSecretName(), isOpenShift);
            } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {
                addNewVolume(volumeList, volumeNamePrefix, scramAuth.getPasswordSecret().getSecretName(), isOpenShift);
            } else if (authentication instanceof KafkaClientAuthenticationOAuth oauth) {
                CertUtils.createTrustedCertificatesVolumes(volumeList, oauth.getTlsTrustedCertificates(), isOpenShift, oauthVolumeNamePrefix);

                if (createOAuthSecretVolumes) {
                    if (oauth.getClientSecret() != null) {
                        addNewVolume(volumeList, volumeNamePrefix, oauth.getClientSecret().getSecretName(), isOpenShift);
                    }
                    if (oauth.getClientAssertion() != null) {
                        addNewVolume(volumeList, volumeNamePrefix, oauth.getClientAssertion().getSecretName(), isOpenShift);
                    }
                    if (oauth.getAccessToken() != null) {
                        addNewVolume(volumeList, volumeNamePrefix, oauth.getAccessToken().getSecretName(), isOpenShift);
                    }
                    if (oauth.getRefreshToken() != null) {
                        addNewVolume(volumeList, volumeNamePrefix, oauth.getRefreshToken().getSecretName(), isOpenShift);
                    }
                    if (oauth.getPasswordSecret() != null) {
                        addNewVolume(volumeList, volumeNamePrefix, oauth.getPasswordSecret().getSecretName(), isOpenShift);
                    }
                }
            }
        }
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components, checking that the named volume has not already been
     * created.
     */
    private static void addNewVolume(List<Volume> volumeList, String volumeNamePrefix, String secretName, boolean isOpenShift) {
        // skipping if a volume with same name was already added
        if (volumeList.stream().noneMatch(v -> v.getName().equals(volumeNamePrefix + secretName))) {
            volumeList.add(VolumeUtils.createSecretVolume(volumeNamePrefix + secretName, secretName, isOpenShift));
        }
    }

    /**
     * Creates the VolumeMounts used for authentication of Kafka client based components
     * @param authentication    Authentication object from CRD
     * @param volumeMountList    List where the volumes will be added
     * @param tlsVolumeMount    Path where the TLS certs should be mounted
     * @param passwordVolumeMount   Path where passwords should be mounted
     * @param oauthVolumeMount      Path where the OAuth certificates would be mounted
     * @param oauthVolumeNamePrefix Prefix used for OAuth volume names
     */
    public static void configureClientAuthenticationVolumeMounts(KafkaClientAuthentication authentication, List<VolumeMount> volumeMountList, String tlsVolumeMount, String passwordVolumeMount, String oauthVolumeMount, String oauthVolumeNamePrefix) {
        configureClientAuthenticationVolumeMounts(authentication, volumeMountList, tlsVolumeMount, passwordVolumeMount, oauthVolumeMount, oauthVolumeNamePrefix, "", false, null);
    }

    /**
     * Creates the VolumeMounts used for authentication of Kafka client based components
     * @param authentication    Authentication object from CRD
     * @param volumeMountList    List where the volume mounts will be added
     * @param tlsVolumeMount    Path where the TLS certs should be mounted
     * @param passwordVolumeMount   Path where passwords should be mounted
     * @param oauthCertsVolumeMount Path where the OAuth certificates would be mounted
     * @param oauthVolumeNamePrefix Prefix used for OAuth volume names
     * @param volumeNamePrefix Prefix used for volume mount names
     * @param mountOAuthSecretVolumes Indicates whether OAuth secret volume mounts will be added to the list
     * @param oauthSecretsVolumeMount Path where the OAuth secrets would be mounted
     */
    public static void configureClientAuthenticationVolumeMounts(KafkaClientAuthentication authentication, List<VolumeMount> volumeMountList, String tlsVolumeMount, String passwordVolumeMount, String oauthCertsVolumeMount, String oauthVolumeNamePrefix, String volumeNamePrefix, boolean mountOAuthSecretVolumes, String oauthSecretsVolumeMount) {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth) {

                // skipping if a volume mount with same Secret name was already added
                if (volumeMountList.stream().noneMatch(vm -> vm.getName().equals(volumeNamePrefix + tlsAuth.getCertificateAndKey().getSecretName()))) {
                    volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + tlsAuth.getCertificateAndKey().getSecretName(),
                            tlsVolumeMount + tlsAuth.getCertificateAndKey().getSecretName()));
                }
            } else if (authentication instanceof KafkaClientAuthenticationPlain passwordAuth) {
                volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + passwordAuth.getPasswordSecret().getSecretName(), passwordVolumeMount + passwordAuth.getPasswordSecret().getSecretName()));
            } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {
                volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + scramAuth.getPasswordSecret().getSecretName(), passwordVolumeMount + scramAuth.getPasswordSecret().getSecretName()));
            } else if (authentication instanceof KafkaClientAuthenticationOAuth oauth) {
                CertUtils.createTrustedCertificatesVolumeMounts(volumeMountList, oauth.getTlsTrustedCertificates(), oauthCertsVolumeMount, oauthVolumeNamePrefix);
            
                if (mountOAuthSecretVolumes) {
                    if (oauth.getClientSecret() != null) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + oauth.getClientSecret().getSecretName(), oauthSecretsVolumeMount + oauth.getClientSecret().getSecretName()));
                    }
                    if (oauth.getClientAssertion() != null) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + oauth.getClientAssertion().getSecretName(), oauthSecretsVolumeMount + oauth.getClientAssertion().getSecretName()));
                    }
                    if (oauth.getAccessToken() != null) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + oauth.getAccessToken().getSecretName(), oauthSecretsVolumeMount + oauth.getAccessToken().getSecretName()));
                    }
                    if (oauth.getRefreshToken() != null) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + oauth.getRefreshToken().getSecretName(), oauthSecretsVolumeMount + oauth.getRefreshToken().getSecretName()));
                    }
                    if (oauth.getPasswordSecret() != null) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + oauth.getPasswordSecret().getSecretName(), oauthSecretsVolumeMount + oauth.getPasswordSecret().getSecretName()));
                    }
                }
            }
        }
    }

    /**
     * Configured environment variables related to authentication in Kafka clients within Strimzi-based containers
     *
     * @param authentication    Authentication object with auth configuration
     * @param varList   List where the new environment variables should be added
     * @param envVarNamer   Function for naming the environment variables (ech components is using different names)
     */
    public static void configureClientAuthenticationEnvVars(KafkaClientAuthentication authentication, List<EnvVar> varList, Function<String, String> envVarNamer)   {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth) {
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(TLS_AUTH_CERT), String.format("%s/%s", tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getCertificate())));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(TLS_AUTH_KEY), String.format("%s/%s", tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getKey())));
            } else if (authentication instanceof KafkaClientAuthenticationPlain passwordAuth) {
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_USERNAME), passwordAuth.getUsername()));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_PASSWORD_FILE), String.format("%s/%s", passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getPassword())));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_MECHANISM), KafkaClientAuthenticationPlain.TYPE_PLAIN));
            } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_USERNAME), scramAuth.getUsername()));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_PASSWORD_FILE), String.format("%s/%s", scramAuth.getPasswordSecret().getSecretName(), scramAuth.getPasswordSecret().getPassword())));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_MECHANISM), scramAuth.getType()));
            } else if (authentication instanceof KafkaClientAuthenticationOAuth oauth) {
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_MECHANISM), KafkaClientAuthenticationOAuth.TYPE_OAUTH));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(OAUTH_CONFIG),
                        oauthJaasOptions(oauth).entrySet().stream()
                                .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                                .collect(Collectors.joining(" "))));
                if (oauth.getClientSecret() != null) {
                    varList.add(ContainerUtils.createEnvVarFromSecret(envVarNamer.apply("OAUTH_CLIENT_SECRET"), oauth.getClientSecret().getSecretName(), oauth.getClientSecret().getKey()));
                }
                if (oauth.getClientAssertion() != null) {
                    varList.add(ContainerUtils.createEnvVarFromSecret(envVarNamer.apply("OAUTH_CLIENT_ASSERTION"), oauth.getClientAssertion().getSecretName(), oauth.getClientAssertion().getKey()));
                }
                if (oauth.getAccessToken() != null) {
                    varList.add(ContainerUtils.createEnvVarFromSecret(envVarNamer.apply("OAUTH_ACCESS_TOKEN"), oauth.getAccessToken().getSecretName(), oauth.getAccessToken().getKey()));
                }
                if (oauth.getRefreshToken() != null) {
                    varList.add(ContainerUtils.createEnvVarFromSecret(envVarNamer.apply("OAUTH_REFRESH_TOKEN"), oauth.getRefreshToken().getSecretName(), oauth.getRefreshToken().getKey()));
                }
                if (oauth.getPasswordSecret() != null) {
                    varList.add(ContainerUtils.createEnvVarFromSecret(envVarNamer.apply("OAUTH_PASSWORD_GRANT_PASSWORD"), oauth.getPasswordSecret().getSecretName(), oauth.getPasswordSecret().getPassword()));
                }
                if (oauth.getCustomOauthCallbackClass() != null) {
                    varList.add(ContainerUtils.createEnvVar(KafkaConnectCluster.ENV_VAR_CUSTOM_OAUTH_CALLBACK_CLASS, oauth.getCustomOauthCallbackClass()));
                }

                if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
                    varList.add(ContainerUtils.createEnvVar(envVarNamer.apply("OAUTH_TRUSTED_CERTS"), CertUtils.trustedCertsEnvVar(oauth.getTlsTrustedCertificates())));
                }
            }
        }
    }

    /**
     * @param oauth The client OAuth authentication configuration.
     * @return The OAuth JAAS configuration options.
     */
    public static Map<String, String> oauthJaasOptions(KafkaClientAuthenticationOAuth oauth) {
        Map<String, String> options = new LinkedHashMap<>();
        addOption(options, ClientConfig.OAUTH_CLIENT_ID, oauth.getClientId());
        addOption(options, ClientConfig.OAUTH_PASSWORD_GRANT_USERNAME, oauth.getUsername());
        addOption(options, ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, oauth.getTokenEndpointUri());
        addOption(options, ClientConfig.OAUTH_SCOPE, oauth.getScope());
        addOption(options, ClientConfig.OAUTH_AUDIENCE, oauth.getAudience());
        if (oauth.isDisableTlsHostnameVerification()) {
            options.put(ClientConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");
        }
        if (!oauth.isAccessTokenIsJwt()) {
            options.put(ClientConfig.OAUTH_ACCESS_TOKEN_IS_JWT, "false");
        }
        addOptionIfGreaterThanZero(options, ClientConfig.OAUTH_MAX_TOKEN_EXPIRY_SECONDS, oauth.getMaxTokenExpirySeconds());
        addOptionIfGreaterThanZero(options, ClientConfig.OAUTH_CONNECT_TIMEOUT_SECONDS, oauth.getConnectTimeoutSeconds());
        addOptionIfGreaterThanZero(options, ClientConfig.OAUTH_READ_TIMEOUT_SECONDS, oauth.getReadTimeoutSeconds());
        addOptionIfGreaterThanZero(options, ClientConfig.OAUTH_HTTP_RETRIES, oauth.getHttpRetries());
        addOptionIfGreaterThanZero(options, ClientConfig.OAUTH_HTTP_RETRY_PAUSE_MILLIS, oauth.getHttpRetryPauseMs());
        if (oauth.isEnableMetrics()) {
            options.put(ClientConfig.OAUTH_ENABLE_METRICS, "true");
        }
        if (!oauth.isIncludeAcceptHeader()) {
            options.put(ClientConfig.OAUTH_INCLUDE_ACCEPT_HEADER, "false");
        }
        addOption(options, ClientConfig.OAUTH_ACCESS_TOKEN_LOCATION, oauth.getAccessTokenLocation());
        addOption(options, ClientConfig.OAUTH_CLIENT_ASSERTION_LOCATION, oauth.getClientAssertionLocation());
        addOption(options, ClientConfig.OAUTH_CLIENT_ASSERTION_TYPE, oauth.getClientAssertionType());

        Map<String, String> saslExtensions = oauth.getSaslExtensions();
        if (saslExtensions != null && !saslExtensions.isEmpty()) {
            for (Map.Entry<String, String> ent: saslExtensions.entrySet()) {
                addOption(options, ClientConfig.OAUTH_SASL_EXTENSION_PREFIX + ent.getKey(), ent.getValue());
            }
        }
        return options;
    }

    private static void addOptionIfGreaterThanZero(Map<String, String> options, String name, Integer value) {
        if (value != null && value > 0) {
            options.put(name, value.toString());
        }
    }

    private static void addOption(Map<String, String> options, String name, String value) {
        if (value != null) {
            options.put(name, value);
        }
    }

    /**
     * Generates volumes needed for generic secrets needed for custom authentication.
     *
     * @param volumeNamePrefix    Prefix for naming the secret volumes
     * @param genericSecretSources   List of generic secrets which should be mounted
     * @param isOpenShift   Flag whether we are on OpenShift or not
     *
     * @return List of new Volumes
     */
    public static List<Volume> configureGenericSecretVolumes(String volumeNamePrefix, List<GenericSecretSource> genericSecretSources, boolean isOpenShift)   {
        List<Volume> newVolumes = new ArrayList<>();

        if (genericSecretSources != null && genericSecretSources.size() > 0) {
            int i = 0;

            for (GenericSecretSource genericSecretSource : genericSecretSources) {
                Map<String, String> items = Collections.singletonMap(genericSecretSource.getKey(), genericSecretSource.getKey());
                String volumeName = String.format("%s-%d", volumeNamePrefix, i);
                Volume vol = VolumeUtils.createSecretVolume(volumeName, genericSecretSource.getSecretName(), items, isOpenShift);
                newVolumes.add(vol);
                i++;
            }
        }

        return newVolumes;
    }

    /**
     * Generates volume mounts needed for generic secrets that are being mounted.
     *
     * @param volumeNamePrefix   Prefix which was used to name the secret volumes
     * @param genericSecretSources   List of generic secrets that should be mounted
     * @param baseVolumeMount   The Base volume into which the certificates should be mounted
     *
     * @return List of new VolumeMounts
     */
    public static List<VolumeMount> configureGenericSecretVolumeMounts(String volumeNamePrefix, List<GenericSecretSource> genericSecretSources, String baseVolumeMount)   {
        List<VolumeMount> newVolumeMounts = new ArrayList<>();

        if (genericSecretSources != null && genericSecretSources.size() > 0) {
            int i = 0;

            for (GenericSecretSource genericSecretSource : genericSecretSources) {
                String volumeName = String.format("%s-%d", volumeNamePrefix, i);
                newVolumeMounts.add(VolumeUtils.createVolumeMount(volumeName, String.format("%s/%s", baseVolumeMount, genericSecretSource.getSecretName())));
                i++;
            }
        }
        return newVolumeMounts;
    }

    /**
     * Generates a JAAS configuration string based on the provided module name and options.
     * The flag will always be "required".
     *
     * @param moduleName The name of the JAAS module to be configured.
     * @param options A Map containing the options to be set for the JAAS module.
     *               The options are represented as key-value pairs, where both the key and
     *               the value must be non-null String objects.
     * @return The JAAS configuration.
     * @throws IllegalArgumentException If the moduleName is empty, or it contains '=' or ';',
     *                                  or if any key or value in the options map is empty,
     *                                  or they contain '=' or ';'.
     */
    public static String jaasConfig(String moduleName, Map<String, String> options) {
        StringJoiner joiner = new StringJoiner(" ");
        for (Entry<String, String> entry : options.entrySet()) {
            String key = Objects.requireNonNull(entry.getKey());
            String value = Objects.requireNonNull(entry.getValue());
            if (key.contains("=") || key.contains(";")) {
                throw new IllegalArgumentException("Keys must not contain '=' or ';'");
            }
            if (moduleName.isEmpty() || moduleName.contains(";") || moduleName.contains("=")) {
                throw new IllegalArgumentException("module name must be not empty and must not contain '=' or ';'");
            } else {
                joiner.add(key + "=\"" + value + "\"");
            }
        }
        return moduleName + " required " + joiner + ";";
    }
}
