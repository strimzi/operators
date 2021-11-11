/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.build.kafka.metadata;

import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.kafka.config.model.Type;
import kafka.api.ApiVersion;
import kafka.api.ApiVersion$;
import kafka.api.ApiVersionValidator$;
import kafka.server.DynamicBrokerConfig$;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.raft.RaftConfig;
import scala.collection.Iterator;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

@SuppressWarnings("unchecked")
public class KafkaConfigModelGenerator extends CommonConfigModelGenerator {

    @Override
    protected Map<String, ConfigModel> configs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        ConfigDef def = brokerConfigs();
        Map<String, String> dynamicUpdates = brokerDynamicUpdates();
        Method getConfigValueMethod = def.getClass().getDeclaredMethod("getConfigValue", ConfigDef.ConfigKey.class, String.class);
        getConfigValueMethod.setAccessible(true);

        Method sortedConfigs = ConfigDef.class.getDeclaredMethod("sortedConfigs");
        sortedConfigs.setAccessible(true);

        List<ConfigDef.ConfigKey> keys = (List) sortedConfigs.invoke(def);
        Map<String, ConfigModel> result = new TreeMap<>();
        for (ConfigDef.ConfigKey key : keys) {
            String configName = String.valueOf(getConfigValueMethod.invoke(def, key, "Name"));
            Type type = parseType(String.valueOf(getConfigValueMethod.invoke(def, key, "Type")));
            Scope scope = parseScope(dynamicUpdates.getOrDefault(key.name, "read-only"));
            ConfigModel descriptor = new ConfigModel();
            descriptor.setType(type);
            descriptor.setScope(scope);

            if (key.validator instanceof ConfigDef.Range) {
                descriptor = range(key, descriptor);
            } else if (key.validator instanceof ConfigDef.ValidString) {
                descriptor.setValues(enumer(key.validator));
            } else if (key.validator instanceof ConfigDef.ValidList) {
                descriptor.setItems(validList(key));
            } else if (key.validator instanceof ApiVersionValidator$) {
                Iterator<ApiVersion> iterator = ApiVersion$.MODULE$.allVersions().iterator();
                LinkedHashSet<String> versions = new LinkedHashSet<>();
                while (iterator.hasNext()) {
                    ApiVersion next = iterator.next();
                    ApiVersion$.MODULE$.apply(next.shortVersion());
                    versions.add(Pattern.quote(next.shortVersion()) + "(\\.[0-9]+)*");
                    ApiVersion$.MODULE$.apply(next.version());
                    versions.add(Pattern.quote(next.version()));
                }
                descriptor.setPattern(String.join("|", versions));
            } else if (key.validator instanceof ConfigDef.NonEmptyString) {
                descriptor.setPattern(".+");
            } else if (key.validator instanceof RaftConfig.ControllerQuorumVotersValidator) {
                continue;
            } else if (key.validator != null) {
                throw new IllegalStateException("Invalid validator class " + key.validator.getClass() + " for option " + configName);
            }

            result.put(configName, descriptor);
        }
        return result;
    }

    static Map<String, String> brokerDynamicUpdates() {
        return DynamicBrokerConfig$.MODULE$.dynamicConfigUpdateModes();
    }

    protected ConfigDef brokerConfigs() {
        try {
            Field instance = getField(KafkaConfig$.class, "MODULE$");
            KafkaConfig$ x = (KafkaConfig$) instance.get(null);
            Field config = getOneOfFields(KafkaConfig$.class, "kafka$server$KafkaConfig$$configDef", "configDef");
            return (ConfigDef) config.get(x);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


}
