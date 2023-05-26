/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.AbstractJsonDiff;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Diffs Kafka broker logging configuration
 */
public class KafkaBrokerLoggingConfigurationDiff extends AbstractJsonDiff {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaBrokerLoggingConfigurationDiff.class);
    private final Collection<AlterConfigOp> diff;
    private final Reconciliation reconciliation;

    /**
     * Constructor
     *
     * @param reconciliation Reconciliation marker
     * @param brokerConfigs  Current broker configuration from Kafka Admin API
     * @param desired        Desired logging configuration
     */
    public KafkaBrokerLoggingConfigurationDiff(Reconciliation reconciliation, Config brokerConfigs, String desired) {
        this.reconciliation = reconciliation;
        this.diff = diff(desired, brokerConfigs);
    }

    /**
     * Returns logging difference
     * @return Collection of AlterConfigOp containing difference between current and desired logging configuration
     */
    public Collection<AlterConfigOp> getLoggingDiff() {
        return diff;
    }

    /**
     * @return The number of broker configs which are different.
     */
    protected int getDiffSize() {
        return diff.size();
    }

    /**
     * Computes logging diff
     *
     * @param desired       desired logging configuration, may be null if the related ConfigMap does not exist yet or no changes are required
     * @param brokerConfigs current configuration
     * @return Collection of AlterConfigOp containing all entries which were changed from current in desired configuration
     */
    private Collection<AlterConfigOp> diff(String desired, Config brokerConfigs) {
        if (brokerConfigs == null || desired == null) {
            return Collections.emptyList();
        }

        Collection<AlterConfigOp> updatedCE = new ArrayList<>();

        Map<String, String> desiredMap = readLog4jConfig(desired);
        desiredMap.computeIfAbsent("root", k -> LoggingLevel.WARN.name());

        LoggingLevelResolver levelResolver = new LoggingLevelResolver(reconciliation, desiredMap);

        for (ConfigEntry entry: brokerConfigs.entries()) {
            LoggingLevel desiredLevel;
            try {
                desiredLevel = levelResolver.resolveLevel(entry.name());
            } catch (IllegalArgumentException e) {
                LOGGER.warnCr(reconciliation, "Skipping {} - it is configured with an unsupported value (\"{}\")", entry.name(), e.getMessage());
                continue;
            }

            if (!desiredLevel.name().equals(entry.value())) {
                updatedCE.add(new AlterConfigOp(new ConfigEntry(entry.name(), desiredLevel.name()), AlterConfigOp.OpType.SET));
                LOGGER.traceCr(reconciliation, "{} has an outdated value. Setting to {}", entry.name(), desiredLevel.name());
            }
        }

        for (Map.Entry<String, String> ent: desiredMap.entrySet()) {
            String name = ent.getKey();
            ConfigEntry configEntry = brokerConfigs.get(name);
            if (configEntry == null) {
                String level = LoggingLevel.nameOrDefault(LoggingLevel.ofLog4jConfig(reconciliation, ent.getValue()), LoggingLevel.WARN);
                updatedCE.add(new AlterConfigOp(new ConfigEntry(name, level), AlterConfigOp.OpType.SET));
                LOGGER.traceCr(reconciliation, "{} not set. Setting to {}", name, level);
            }
        }

        return updatedCE;
    }

    protected Map<String, String> readLog4jConfig(String config) {
        Map<String, String> parsed = new LinkedHashMap<>();
        Map<String, String> env = new HashMap<>();
        BufferedReader firstPassReader = new BufferedReader(new StringReader(config));
        BufferedReader reader = new BufferedReader(new StringReader(config));
        String line;

        // first pass to lookup the var definitions
        try {
            while ((line = firstPassReader.readLine()) != null) {
                // skip comments
                if (line.startsWith("#")) continue;

                // ignore empty lines
                line = line.trim();
                if (line.length() == 0) continue;

                // everything that does not start with 'log4j.' is a variable definition
                if (!line.startsWith("log4j.")) {
                    int foundIdx = line.indexOf("=");
                    if (foundIdx >= 0) {
                        env.put(line.substring(0, foundIdx).trim(), line.substring(foundIdx + 1).trim());
                    } else {
                        env.put(line.trim(), "");
                    }
                    LOGGER.debugCr(reconciliation, "Treating the line as ENV var declaration: {}", line);
                }
            }
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Failed to parse logging configuration: " + config, e);
            return Collections.emptyMap();
        }

        try {
            while ((line = reader.readLine()) != null) {
                // skip comments
                if (line.startsWith("#")) continue;

                // ignore empty lines
                line = line.trim();
                if (line.length() == 0) continue;

                // we ignore appenders (log4j.appender.*)
                // and only handle loggers (log4j.logger.*)
                if (line.startsWith("log4j.logger.")) {
                    int startIdx = "log4j.logger.".length();
                    int endIdx = line.indexOf("=", startIdx);
                    if (endIdx == -1) {
                        LOGGER.debugCr(reconciliation, "Skipping log4j.logger.* declaration without level: {}", line);
                        continue;
                    }
                    String name = line.substring(startIdx, endIdx).trim();
                    String value = line.substring(endIdx + 1).split(",")[0].trim();

                    value = Util.expandVar(value, env);
                    parsed.put(name, value);

                } else if (line.startsWith("log4j.rootLogger=")) {
                    int startIdx = "log4j.rootLogger=".length();
                    parsed.put("root", Util.expandVar(line.substring(startIdx).split(",")[0].trim(), env));

                } else {
                    LOGGER.debugCr(reconciliation, "Skipping log4j line: {}", line);
                }
            }
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Failed to parse logging configuration: " + config, e);
            return Collections.emptyMap();
        }
        return parsed;
    }

    /**
     * @return whether the current config and the desired config are identical (thus, no update is necessary).
     */
    @Override
    public boolean isEmpty() {
        return diff.size() == 0;
    }

    /**
     * This internal class calculates the logging level of an arbitrary category based on the logging configuration.
     *
     * It takes Log4j properties configuration in the form of a map of key:value pairs,
     * where key is the category name, and the value is whatever comes to the right of '=' sign in log4j.properties,
     * which is either a logging level, or a logging level followed by a comma, and followed by the appender name.
     */
    private static class LoggingLevelResolver {

        private final Map<String, String> config;
        private final Reconciliation reconciliation;

        LoggingLevelResolver(Reconciliation reconciliation, Map<String, String> loggingConfig) {
            this.reconciliation = reconciliation;
            this.config = loggingConfig;
        }

        /**
         * The method that returns the logging level of the category
         * based on logging configuration, taking inheritance into account.
         *
         * For example, if looking for a logging level for 'io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder',
         * the following configuration lookups are performed until one is found:
         * <ul>
         *     <li>io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder</li>
         *     <li>io.strimzi.kafka.oauth.server</li>
         *     <li>io.strimzi.kafka.oauth</li>
         *     <li>io.strimzi.kafka</li>
         *     <li>io.strimzi</li>
         *     <li>io</li>
         *     <li>root</li>
         * </ul>
         *
         * When the configured level is ALL, the actual level returned will be TRACE.
         * Similarly when the configured level is OFF, the actual level returned will be FATAL.
         *
         * If logging level can't be parsed from the configuration (unsupported or badly formatted)
         * the method returns WARN level. The rationale is that making a configuration mistake in logging level
         * should not accidentally trigger a massive amount of logging.
         *
         * @param name The logging category name
         * @return The logging level compatible with dynamic logging update
         */
        LoggingLevel resolveLevel(String name) {
            String level = config.get(name);
            if (level != null) {
                LoggingLevel result = LoggingLevel.ofLog4jConfig(reconciliation, level);
                return result != null ? result : LoggingLevel.WARN;
            }

            int endIdx = name.length();
            while (endIdx > -1) {
                endIdx = name.lastIndexOf('.', endIdx);
                if (endIdx == -1) {
                    level = config.get("root");
                } else {
                    level = config.get(name.substring(0, endIdx));
                }
                if (level != null) {
                    LoggingLevel result = LoggingLevel.ofLog4jConfig(reconciliation, level);
                    return result != null ? result : LoggingLevel.WARN;
                }
                endIdx -= 1;
            }
            // still here? Not even root logger defined?
            return LoggingLevel.WARN;
        }
    }

    private enum LoggingLevel {
        OFF,
        FATAL,
        ERROR,
        WARN,
        INFO,
        DEBUG,
        TRACE,
        ALL;

        static LoggingLevel ofLog4jConfig(Reconciliation reconciliation, String value) {
            if (value != null && !"".equals(value)) {
                String v = value.split(",")[0].trim();
                if ("ALL".equals(v)) {
                    return TRACE;
                } else if ("OFF".equals(v)) {
                    return FATAL;
                } else {
                    try {
                        return valueOf(v);
                    } catch (RuntimeException e) {
                        LOGGER.warnCr(reconciliation, "Invalid logging level: {}. Using WARN as a failover.", v);
                    }
                }
            }
            return null;
        }

        static String nameOrDefault(LoggingLevel level, LoggingLevel failover) {
            return level != null ? level.name() : failover.name();
        }
    }
}
