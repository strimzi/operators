/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.status.HasStatus;
import io.strimzi.api.kafka.model.status.KafkaUserStatus;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.Inline;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

@JsonDeserialize
@Crd(
        apiVersion = KafkaUser.CRD_API_VERSION,
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = KafkaUser.RESOURCE_KIND,
                        plural = KafkaUser.RESOURCE_PLURAL,
                        shortNames = {KafkaUser.SHORT_NAME}
                ),
                group = KafkaUser.RESOURCE_GROUP,
                scope = KafkaUser.SCOPE,
                version = KafkaUser.V1BETA1,
                versions = {
                        @Crd.Spec.Version(
                                name = KafkaUser.V1BETA1,
                                served = true,
                                storage = true
                        ),
                        @Crd.Spec.Version(
                                name = KafkaUser.V1ALPHA1,
                                served = true,
                                storage = false
                        )
                },
                subresources = @Crd.Spec.Subresources(
                        status = @Crd.Spec.Subresources.Status()
                ),
                additionalPrinterColumns = {
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Authentication",
                                description = "How the user is authenticated",
                                jsonPath = ".spec.authentication.type",
                                type = "string"
                        ),
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Authorization",
                                description = "How the user is authorised",
                                jsonPath = ".spec.authorization.type",
                                type = "string"
                        )
                }

        )
)
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder",
        inline = @Inline(type = Doneable.class, prefix = "Doneable", value = "done")
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
@EqualsAndHashCode
public class KafkaUser extends CustomResource implements UnknownPropertyPreserving, HasStatus<KafkaUserStatus> {

    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1ALPHA1 = "v1alpha1";
    public static final String V1BETA1 = "v1beta1";
    public static final List<String> VERSIONS = unmodifiableList(asList(V1BETA1, V1ALPHA1));
    public static final String RESOURCE_KIND = "KafkaUser";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = "kafka.strimzi.io";
    public static final String RESOURCE_PLURAL = "kafkausers";
    public static final String RESOURCE_SINGULAR = "kafkauser";
    public static final String CRD_API_VERSION = "apiextensions.k8s.io/v1beta1";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "ku";
    public static final List<String> RESOURCE_SHORTNAMES = singletonList(SHORT_NAME);

    private String apiVersion;
    private ObjectMeta metadata;
    private KafkaUserSpec spec;
    private Map<String, Object> additionalProperties = new HashMap<>(0);
    private KafkaUserStatus status;

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    @Override
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    @Override
    public ObjectMeta getMetadata() {
        return super.getMetadata();
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        super.setMetadata(metadata);
    }

    @Description("The specification of the user.")
    public KafkaUserSpec getSpec() {
        return spec;
    }

    public void setSpec(KafkaUserSpec spec) {
        this.spec = spec;
    }

    @Override
    @Description("The status of the Kafka User.")
    public KafkaUserStatus getStatus() {
        return status;
    }

    public void setStatus(KafkaUserStatus status) {
        this.status = status;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        YAMLMapper mapper = new YAMLMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
