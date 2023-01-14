/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

/**
 * Interface used for template objects which allow to configure Kubernetes metadata. This is used to have a shard
 * methods for getting the template values.
 */
public interface HasMetadataTemplate {
    MetadataTemplate getMetadata();
    void setMetadata(MetadataTemplate metadata);
}
