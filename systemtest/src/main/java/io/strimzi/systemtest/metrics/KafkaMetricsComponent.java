/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;

public class KafkaMetricsComponent implements MetricsComponent {

    private String namespaceName;
    private String componentName;

    public static KafkaMetricsComponent create(final String namespaceName, final String componentName) {
        return new KafkaMetricsComponent(namespaceName, componentName);
    }

    private KafkaMetricsComponent(String namespaceName, String componentName) {
        this.namespaceName = namespaceName;
        this.componentName = componentName;
    }

    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.COMPONENTS_METRICS_PORT;
    }

    @Override
    public String getDefaultMetricsPath() {
        return "/metrics";
    }

    @Override
    public LabelSelector getLabelSelector() {
        return ResourceManager.kubeClient().getDeploymentSelectors(namespaceName, StrimziPodSetResource.getBrokerComponentName(componentName));

    }
}
