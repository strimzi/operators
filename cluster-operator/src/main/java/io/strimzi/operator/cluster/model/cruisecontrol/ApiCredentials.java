/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.ApiUsers;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.HashLoginServiceApiUsers;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.AUTH_FILE_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.HEALTHCHECK_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.HEALTHCHECK_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.REBALANCE_OPERATOR_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.REBALANCE_OPERATOR_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME_KEY;

/**
 * Uses information in a Kafka Custom Resource to generate a API credentials configuration file to be used for
 * authenticating to Cruise Control's REST API.
 */
public class ApiCredentials {
    private static final List<String> FORBIDDEN_USERNAMES = Arrays.asList(
            HEALTHCHECK_USERNAME,
            REBALANCE_OPERATOR_USERNAME,
            TOPIC_OPERATOR_USERNAME
    );

    private final ApiUsers apiUsers;
    private final String userManagedApiSecretName;
    private final String userManagedApiSecretKey;
    private final String namespace;
    private final String cluster;
    private final Labels labels;
    private final OwnerReference ownerReference;

    /**
     * Constructs the Api Credentials Model for managing API users for Cruise Control API.
     *
     * @param namespace         Namespace of the cluster
     * @param cluster           Name of the cluster to which this component belongs
     * @param labels            Labels for Cruise Control instance
     * @param ownerReference    Owner reference for Cruise Control instance
     * @param ccSpec         Custom resource section configuring Cruise Control API users
     */
    public ApiCredentials(String namespace, String cluster, Labels labels, OwnerReference ownerReference, CruiseControlSpec ccSpec) {
        this.namespace = namespace;
        this.cluster = cluster;
        this.labels = labels;
        this.ownerReference = ownerReference;
        this.apiUsers = ccSpec.getApiUsers();

        if (apiUsers != null) {
            this.userManagedApiSecretName = apiUsers.getValueFrom().getSecretKeyRef().getName();
            this.userManagedApiSecretKey = apiUsers.getValueFrom().getSecretKeyRef().getKey();
        } else {
            this.userManagedApiSecretName = null;
            this.userManagedApiSecretKey = null;
        }
    }

    /**
     * @return  Returns user-managed API credentials secret name
     */
    public String getUserManagedApiSecretName() {
        return this.userManagedApiSecretName;
    }

    /**
     * @return  Returns user-managed API credentials secret key
     */
    /* test */ String getUserManagedApiSecretKey() {
        return this.userManagedApiSecretKey;
    }

    /**
     * @return  Returns ApiUsers object
     */
    /* test */ ApiUsers getApiUsers() {
        return this.apiUsers;
    }

    /**
     * Parses auth data from existing Topic Operator API user secret into map of API user entries.
     *
     * @param secret API user secret
     *
     * @return Map of API user entries containing user-managed API user credentials
     */
    /* test */ static Map<String, ApiUsers.UserEntry> generateToManagedApiCredentials(Secret secret) {
        Map<String, ApiUsers.UserEntry> entries = new HashMap<>();
        if (secret != null) {
            if (secret.getData().containsKey(TOPIC_OPERATOR_USERNAME_KEY) && secret.getData().containsKey(TOPIC_OPERATOR_PASSWORD_KEY)) {
                String username = Util.decodeFromBase64(secret.getData().get(TOPIC_OPERATOR_USERNAME_KEY));
                String password = Util.decodeFromBase64(secret.getData().get(TOPIC_OPERATOR_PASSWORD_KEY));
                entries.put(username, new ApiUsers.UserEntry(username, password, ApiUsers.Role.ADMIN));
            }
        }
        return entries;
    }

    /**
     * Parses auth data from existing user-managed API user secret into List of API user entries.
     *
     * @param secret API user secret
     * @param secretKey API user secret key
     * @param apiUsers API users config
     *
     * @return Map of API user entries containing user-managed API user credentials
     */
    /* test */ static Map<String, ApiUsers.UserEntry> generateUserManagedApiCredentials(Secret secret, String secretKey, ApiUsers apiUsers) {
        Map<String, ApiUsers.UserEntry> entries = new HashMap<>();
        if (secret != null) {
            if (secretKey != null && secret.getData().containsKey(secretKey)) {
                String credentialsAsString = Util.decodeFromBase64(secret.getData().get(secretKey));
                entries.putAll(apiUsers.parseEntriesFromString(credentialsAsString));
            }
        }
        for (ApiUsers.UserEntry entry : entries.values()) {
            if (FORBIDDEN_USERNAMES.contains(entry.getUsername())) {
                throw new InvalidConfigurationException("The following usernames for Cruise Control API are forbidden: " + FORBIDDEN_USERNAMES
                        + " User provided Cruise Control API credentials contain illegal username: " + entry.getUsername());
            } else if (entry.getRole() == ApiUsers.Role.ADMIN) {
                throw new InvalidConfigurationException("The following roles for Cruise Control API are forbidden: " + ApiUsers.Role.ADMIN
                        + " User provided Cruise Control API credentials contain contains illegal role: " +  entry.getRole());
            }
        }
        return entries;
    }

    /**
     * Parses auth data from existing Cluster Operator managed API user secret into map of API user entries.
     *
     * @param passwordGenerator The password generator for API users
     * @param secret API user secret
     * @param apiUsers API users config
     *
     * @return Map of API user entries containing Strimzi-managed API user credentials
     */
    /* test */ static Map<String, ApiUsers.UserEntry> generateCoManagedApiCredentials(PasswordGenerator passwordGenerator, Secret secret, ApiUsers apiUsers) {
        Map<String, ApiUsers.UserEntry> entries = new HashMap<>();

        if (secret != null) {
            if (secret.getData().containsKey(AUTH_FILE_KEY)) {
                String credentialsAsString = Util.decodeFromBase64(secret.getData().get(AUTH_FILE_KEY));
                entries.putAll(apiUsers.parseEntriesFromString(credentialsAsString));
            }
        }

        if (!entries.containsKey(REBALANCE_OPERATOR_USERNAME)) {
            entries.put(REBALANCE_OPERATOR_USERNAME, new ApiUsers.UserEntry(REBALANCE_OPERATOR_USERNAME, passwordGenerator.generate(), ApiUsers.Role.ADMIN));
        }

        if (!entries.containsKey(HEALTHCHECK_USERNAME)) {
            entries.put(HEALTHCHECK_USERNAME, new ApiUsers.UserEntry(HEALTHCHECK_USERNAME, passwordGenerator.generate(), ApiUsers.Role.USER));
        }

        return entries;
    }

    /**
     * Creates map with API usernames, passwords, and credentials file for Strimzi-managed API users secret.
     *
     * @param passwordGenerator the password generator used for creating new credentials.
     * @param cruiseControlApiSecret the existing Cruise Control API secret containing all API credentials.
     * @param userManagedApiSecret the secret managed by the user, containing user-defined API credentials.
     * @param topicOperatorManagedApiSecret the secret managed by the topic operator, containing credentials for the topic operator.
     *
     * @return Map with API usernames, passwords, and credentials file for Strimzi-managed CC API secret.
     */
    private Map<String, String> generateMapWithApiCredentials(PasswordGenerator passwordGenerator,
                                                              Secret cruiseControlApiSecret,
                                                              Secret userManagedApiSecret,
                                                              Secret topicOperatorManagedApiSecret) {
        ApiUsers apiUsers = this.apiUsers == null ? new HashLoginServiceApiUsers() : this.apiUsers;
        Map<String, ApiUsers.UserEntry> apiCredentials = new HashMap<>();
        apiCredentials.putAll(generateCoManagedApiCredentials(passwordGenerator, cruiseControlApiSecret, apiUsers));
        apiCredentials.putAll(generateUserManagedApiCredentials(userManagedApiSecret, userManagedApiSecretKey, apiUsers));
        apiCredentials.putAll(generateToManagedApiCredentials(topicOperatorManagedApiSecret));

        Map<String, String> data = new HashMap<>(3);
        data.put(REBALANCE_OPERATOR_PASSWORD_KEY, Util.encodeToBase64(apiCredentials.get(REBALANCE_OPERATOR_USERNAME).getPassword()));
        data.put(HEALTHCHECK_PASSWORD_KEY, Util.encodeToBase64(apiCredentials.get(HEALTHCHECK_USERNAME).getPassword()));
        data.put(AUTH_FILE_KEY, Util.encodeToBase64(apiUsers.generateApiAuthFileAsString(apiCredentials)));
        return data;
    }

    /**
     * Generates a new API secret for Cruise Control by aggregating credentials from various sources.
     * This method collects API credentials from three potential sources:
     *   (1) Old Cruise Control API secret
     *   (2) User-managed API secret
     *   (3) Topic operator-managed API secret.
     * It uses these credentials to create a comprehensive map of API credentials, which is then used to generate a new API secret
     * for Cruise Control.
     *
     * @param passwordGenerator the password generator used for creating new credentials.
     * @param oldCruiseControlApiSecret the existing Cruise Control API secret, containing previously stored credentials.
     * @param userManagedApiSecret the secret managed by the user, containing user-defined API credentials.
     * @param topicOperatorManagedApiSecret the secret managed by the topic operator, containing credentials for the topic operator.
     * @return a new Secret object containing the aggregated API credentials for Cruise Control.
     */
    public Secret generateApiSecret(PasswordGenerator passwordGenerator,
                                    Secret oldCruiseControlApiSecret,
                                    Secret userManagedApiSecret,
                                    Secret topicOperatorManagedApiSecret) {
        if (this.apiUsers != null && userManagedApiSecret == null) {
            throw new InvalidResourceException("The configuration of the Cruise Control REST API users " +
                    "references a secret: " +  "\"" +  userManagedApiSecretName + "\" that does not exist.");
        }
        Map<String, String> mapWithApiCredentials = generateMapWithApiCredentials(passwordGenerator,
                oldCruiseControlApiSecret, userManagedApiSecret, topicOperatorManagedApiSecret);
        return ModelUtils.createSecret(CruiseControlResources.apiSecretName(cluster), namespace, labels, ownerReference,
                mapWithApiCredentials, Collections.emptyMap(), Collections.emptyMap());
    }
}
