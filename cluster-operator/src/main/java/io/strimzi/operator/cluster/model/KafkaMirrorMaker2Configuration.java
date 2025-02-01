/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.operator.common.Reconciliation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for handling Kafka MirrorMaker 2 connect configuration passed by the user
 */
public class KafkaMirrorMaker2Configuration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_PREFIX_EXCEPTIONS;
    private static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaMirrorMaker2ClusterSpec.FORBIDDEN_PREFIXES);
        FORBIDDEN_PREFIX_EXCEPTIONS = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaMirrorMaker2ClusterSpec.FORBIDDEN_PREFIX_EXCEPTIONS);

        DEFAULTS = new HashMap<>(9);
        DEFAULTS.put("group.id", "mirrormaker2-cluster");
        DEFAULTS.put("offset.storage.topic", "mirrormaker2-cluster-offsets");
        DEFAULTS.put("config.storage.topic", "mirrormaker2-cluster-configs");
        DEFAULTS.put("status.storage.topic", "mirrormaker2-cluster-status");
        DEFAULTS.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        DEFAULTS.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        DEFAULTS.put("header.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to
     * create configuration from ConfigMap / CRD.
     *
     * @param reconciliation The reconciliation
     * @param jsonOptions Json object with configuration options as key ad value
     *                    pairs.
     */
    public KafkaMirrorMaker2Configuration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIX_EXCEPTIONS, List.of(), DEFAULTS);
    }
}
