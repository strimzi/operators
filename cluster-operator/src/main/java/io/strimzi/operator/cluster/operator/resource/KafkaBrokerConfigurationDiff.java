/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.model.NodeRef;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.model.AbstractJsonDiff;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

/**
 The algorithm:
 *  1. Create a map from the supplied desired String
 *  2. Fill placeholders (e.g. ${BROKER_ID}) in desired map as the broker's {@code kafka_config_generator.sh} would
 *  3a. Loop over all entries. If the entry is in IGNORABLE_PROPERTIES or entry.value from desired is equal to entry.value from current, do nothing
 *      else add it to the diff
 *  3b. If entry was removed from desired, add it to the diff with null value.
 *  3c. If custom entry was removed, delete property
 */
public class KafkaBrokerConfigurationDiff extends AbstractJsonDiff {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaBrokerConfigurationDiff.class);
    private static final String PLACE_HOLDER = Pattern.quote("STRIMZI_BROKER_ID");

    private final Reconciliation reconciliation;
    private final Collection<AlterConfigOp> diff;
    private final Map<String, ConfigModel> configModel;

    /**
     * These options are skipped because they contain placeholders
     * 909[1-4] is for skipping all (internal, plain, secured, external) listeners properties
     */
    public static final Pattern IGNORABLE_PROPERTIES = Pattern.compile(
            "^(broker\\.id"
            + "|.*-[0-9]{2,5}\\.ssl\\.keystore\\.location"
            + "|.*-[0-9]{2,5}\\.ssl\\.keystore\\.password"
            + "|.*-[0-9]{2,5}\\.ssl\\.keystore\\.type"
            + "|.*-[0-9]{2,5}\\.ssl\\.truststore\\.location"
            + "|.*-[0-9]{2,5}\\.ssl\\.truststore\\.password"
            + "|.*-[0-9]{2,5}\\.ssl\\.truststore\\.type"
            + "|.*-[0-9]{2,5}\\.ssl\\.client\\.auth"
            + "|.*-[0-9]{2,5}\\.scram-sha-512\\.sasl\\.jaas\\.config"
            + "|.*-[0-9]{2,5}\\.sasl\\.enabled\\.mechanisms"
            + "|advertised\\.listeners"
            + "|zookeeper\\.connect"
            + "|zookeeper\\.ssl\\..*"
            + "|zookeeper\\.clientCnxnSocket"
            + "|broker\\.rack)$");

    /**
     * KRaft controller configuration options are skipped if it is not combined node
     */
    private static final Pattern IGNORABLE_CONTROLLER_PROPERTIES = Pattern.compile("controller\\.quorum\\..*");
    /**
     * Constructor
     *
     * @param reconciliation    Reconciliation marker
     * @param brokerConfigs     Broker configuration from Kafka Admin API
     * @param desired           Desired configuration
     * @param kafkaVersion      Kafka version
     * @param nodeRef           Node reference
     */
    protected KafkaBrokerConfigurationDiff(Reconciliation reconciliation, Config brokerConfigs, String desired, KafkaVersion kafkaVersion, NodeRef nodeRef) {
        this.reconciliation = reconciliation;
        this.configModel = KafkaConfiguration.readConfigModel(kafkaVersion);
        this.diff = diff(nodeRef, desired, brokerConfigs, configModel);
    }

    private static void fillPlaceholderValue(Map<String, String> orderedProperties, String value) {
        orderedProperties.entrySet().forEach(entry -> {
            String v = entry.getValue().replaceAll("\\$\\{" + PLACE_HOLDER + "}", value);
            entry.setValue(v);
        });
    }

    /**
     * @return  Returns true if the configuration can be updated dynamically
     */
    protected boolean canBeUpdatedDynamically() {
        boolean result = true;
        for (AlterConfigOp entry : diff) {
            if (isEntryReadOnly(entry.configEntry())) {
                result = false;
                LOGGER.infoCr(reconciliation, "Configuration can't be updated dynamically due to: {}", entry);
                break;
            }
        }
        return result;
    }

    /**
     * @param entry tested ConfigEntry
     * @return true if the entry is READ_ONLY
     */
    private boolean isEntryReadOnly(ConfigEntry entry) {
        return configModel.get(entry.name()).getScope().equals(Scope.READ_ONLY);
    }

    /**
     * Returns configuration difference
     * @return Collection of AlterConfigOp containing difference between current and desired configuration
     */
    protected Collection<AlterConfigOp> getConfigDiff() {
        return diff;
    }

    /**
     * @return The number of broker configs which are different.
     */
    protected int getDiffSize() {
        return diff.size();
    }

    private static boolean isIgnorableProperty(String key, boolean isCombined) {
        // If node is both KRaft controller and broker (combined), do not ignore controller config properties.
        // Otherwise, always ignore KRaft controller config properties.
        if (isCombined) {
            return IGNORABLE_PROPERTIES.matcher(key).matches();
        } else {
            return IGNORABLE_PROPERTIES.matcher(key).matches() || IGNORABLE_CONTROLLER_PROPERTIES.matcher(key).matches();
        }
    }

    /**
     * Computes diff between two maps. Entries in IGNORABLE_PROPERTIES are skipped
     * @param nodeRef node reference of compared broker
     * @param desired desired configuration, may be null if the related ConfigMap does not exist yet or no changes are required
     * @param brokerConfigs current configuration
     * @param configModel default configuration for {@code kafkaVersion} of broker
     * @return Collection of AlterConfigOp containing all entries which were changed from current in desired configuration
     */
    private Collection<AlterConfigOp> diff(NodeRef nodeRef, String desired,
                                                  Config brokerConfigs,
                                                  Map<String, ConfigModel> configModel) {
        if (brokerConfigs == null || desired == null) {
            return Collections.emptyList();
        }
        Map<String, String> currentMap;

        Collection<AlterConfigOp> updatedCE = new ArrayList<>();

        currentMap = brokerConfigs.entries().stream().collect(
            Collectors.toMap(
                ConfigEntry::name,
                configEntry -> configEntry.value() == null ? "null" : configEntry.value()));

        OrderedProperties orderedProperties = new OrderedProperties();
        orderedProperties.addStringPairs(desired);
        Map<String, String> desiredMap = orderedProperties.asMap();

        int brokerId = nodeRef.nodeId();
        fillPlaceholderValue(desiredMap, Integer.toString(brokerId));

        JsonNode source = PATCH_MAPPER.valueToTree(currentMap);
        JsonNode target = PATCH_MAPPER.valueToTree(desiredMap);
        JsonNode jsonDiff = JsonDiff.asJson(source, target);

        for (JsonNode d : jsonDiff) {
            String pathValue = d.get("path").asText();
            String pathValueWithoutSlash = pathValue.substring(1);

            Optional<ConfigEntry> optEntry = brokerConfigs.entries().stream()
                    .filter(configEntry -> configEntry.name().equals(pathValueWithoutSlash))
                    .findFirst();

            String op = d.get("op").asText();
            boolean isCombined = nodeRef.controller() && nodeRef.broker();
            if (optEntry.isPresent()) {
                ConfigEntry entry = optEntry.get();
                if ("remove".equals(op)) {
                    removeProperty(configModel, updatedCE, pathValueWithoutSlash, entry, isCombined);
                } else if ("replace".equals(op)) {
                    // entry is in the current, desired is updated value
                    updateOrAdd(entry.name(), configModel, desiredMap, updatedCE, isCombined);
                }
            } else {
                if ("add".equals(op)) {
                    // entry is not in the current, it is added
                    updateOrAdd(pathValueWithoutSlash, configModel, desiredMap, updatedCE, isCombined);
                }
            }

            if ("remove".equals(op)) {
                // there is a lot of properties set by default - not having them in desired causes very noisy log output
                LOGGER.traceCr(reconciliation, "Kafka Broker {} Config Differs : {}", brokerId, d);
                LOGGER.traceCr(reconciliation, "Current Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(source, pathValue));
                LOGGER.traceCr(reconciliation, "Desired Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(target, pathValue));
            } else {
                LOGGER.debugCr(reconciliation, "Kafka Broker {} Config Differs : {}", brokerId, d);
                LOGGER.debugCr(reconciliation, "Current Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(source, pathValue));
                LOGGER.debugCr(reconciliation, "Desired Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(target, pathValue));
            }
        }

        return updatedCE;
    }

    private void updateOrAdd(String propertyName, Map<String, ConfigModel> configModel, Map<String, String> desiredMap, Collection<AlterConfigOp> updatedCE, boolean isCombined) {
        if (!isIgnorableProperty(propertyName, isCombined)) {
            if (isCustomEntry(propertyName, configModel)) {
                LOGGER.traceCr(reconciliation, "custom property {} has been updated/added {}", propertyName, desiredMap.get(propertyName));
            } else {
                LOGGER.traceCr(reconciliation, "property {} has been updated/added {}", propertyName, desiredMap.get(propertyName));
                updatedCE.add(new AlterConfigOp(new ConfigEntry(propertyName, desiredMap.get(propertyName)), AlterConfigOp.OpType.SET));
            }
        } else {
            LOGGER.traceCr(reconciliation, "{} is ignorable, not considering");
        }
    }

    private void removeProperty(Map<String, ConfigModel> configModel, Collection<AlterConfigOp> updatedCE, String pathValueWithoutSlash, ConfigEntry entry, boolean isCombined) {
        if (isCustomEntry(entry.name(), configModel)) {
            // we are deleting custom option
            LOGGER.traceCr(reconciliation, "removing custom property {}", entry.name());
        } else if (entry.isDefault()) {
            // entry is in current, is not in desired, is default -> it uses default value, skip.
            // Some default properties do not have set ConfigEntry.ConfigSource.DEFAULT_CONFIG and thus
            // we are removing property. That might cause redundant RU. To fix this we would have to add defaultValue
            // to the configModel
            LOGGER.traceCr(reconciliation, "{} not set in desired, using default value", entry.name());
        } else {
            // entry is in current, is not in desired, is not default -> it was using non-default value and was removed
            // if the entry was custom, it should be deleted
            if (!isIgnorableProperty(pathValueWithoutSlash, isCombined)) {
                updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, null), AlterConfigOp.OpType.DELETE));
                LOGGER.infoCr(reconciliation, "{} not set in desired, unsetting back to default {}", entry.name(), "deleted entry");
            } else {
                LOGGER.traceCr(reconciliation, "{} is ignorable, not considering as removed");
            }
        }
    }

    /**
     * @return whether the current config and the desired config are identical (thus, no update is necessary).
     */
    @Override
    public boolean isEmpty() {
        return  diff.size() == 0;
    }

    /**
     * For some reason not all default entries have set ConfigEntry.ConfigSource.DEFAULT_CONFIG so we need to compare
     * @param entryName tested ConfigEntry
     * @param configModel configModel
     * @return true if entry is custom (not default)
     */
    private static boolean isCustomEntry(String entryName, Map<String, ConfigModel> configModel) {
        return !configModel.containsKey(entryName);
    }

}
