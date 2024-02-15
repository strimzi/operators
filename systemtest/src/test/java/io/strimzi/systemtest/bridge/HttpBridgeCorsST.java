/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeHttpCors;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Tag(BRIDGE)
@Tag(REGRESSION)
public class HttpBridgeCorsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeCorsST.class);
    private static final String ALLOWED_ORIGIN = "https://strimzi.io";
    private static final String NOT_ALLOWED_ORIGIN = "https://evil.io";

    private String scraperPodName;
    private String bridgeUrl;

    @ParallelTest
    void testCorsOriginAllowed() {
        final String kafkaBridgeUser = "bridge-user-example";
        final String groupId = ClientUtils.generateRandomConsumerGroup();

        JsonObject config = new JsonObject();
        config.put("name", kafkaBridgeUser);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put("Origin", ALLOWED_ORIGIN);
        additionalHeaders.put("Access-Control-Request-Method", HttpMethod.POST.toString());

        String url = bridgeUrl + "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription";
        String headers = BridgeUtils.addHeadersToString(additionalHeaders, TestConstants.KAFKA_BRIDGE_JSON_JSON);
        String response = cmdKubeClient().namespace(Environment.TEST_SUITE_NAMESPACE).execInPod(scraperPodName, "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.OPTIONS, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        String responseAllowHeaders = BridgeUtils.getHeaderValue("access-control-allow-headers", response);
        LOGGER.info("Checking if response from Bridge is correct");

        assertThat(response.contains("200 OK") || response.contains("204 No Content"), is(true));
        assertThat(BridgeUtils.getHeaderValue("access-control-allow-origin", response), is(ALLOWED_ORIGIN));
        assertThat(responseAllowHeaders, containsString("access-control-allow-origin"));
        assertThat(responseAllowHeaders, containsString("origin"));
        assertThat(responseAllowHeaders, containsString("x-requested-with"));
        assertThat(responseAllowHeaders, containsString("content-type"));
        assertThat(responseAllowHeaders, containsString("access-control-allow-methods"));
        assertThat(responseAllowHeaders, containsString("accept"));
        assertThat(BridgeUtils.getHeaderValue("access-control-allow-methods", response), containsString(HttpMethod.POST.toString()));

        url = bridgeUrl + "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription";
        headers = BridgeUtils.addHeadersToString(Collections.singletonMap("Origin", ALLOWED_ORIGIN));
        response = cmdKubeClient().namespace(Environment.TEST_SUITE_NAMESPACE).execInPod(scraperPodName, "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.GET, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        assertThat(response, containsString("404"));
    }

    @ParallelTest
    void testCorsForbidden() {
        final String kafkaBridgeUser = "bridge-user-example";
        final String groupId = ClientUtils.generateRandomConsumerGroup();

        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put("Origin", NOT_ALLOWED_ORIGIN);
        additionalHeaders.put("Access-Control-Request-Method", HttpMethod.POST.toString());

        String url = bridgeUrl + "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription";
        String headers = BridgeUtils.addHeadersToString(additionalHeaders);
        String response = cmdKubeClient().namespace(Environment.TEST_SUITE_NAMESPACE).execInPod(scraperPodName, "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.OPTIONS, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        LOGGER.info("Checking if response from Bridge is correct");
        assertThat(response, containsString("403"));
        assertThat(response, containsString("CORS Rejected - Invalid origin"));

        additionalHeaders.remove("Access-Control-Request-Method", HttpMethod.POST.toString());
        headers = BridgeUtils.addHeadersToString(additionalHeaders);
        response = cmdKubeClient().namespace(Environment.TEST_SUITE_NAMESPACE).execInPod(scraperPodName, "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.POST, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        LOGGER.info("Checking if response from Bridge is correct");
        assertThat(response, containsString("403"));
        assertThat(response, containsString("CORS Rejected - Invalid origin"));
    }

    @BeforeAll
    void beforeAll() {
        clusterOperator = clusterOperator.defaultInstallation()
                .createInstallation()
                .runInstallation();

        String httpBridgeCorsClusterName = "http-bridge-cors-cluster-name";

        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(httpBridgeCorsClusterName, 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        String scraperName = Environment.TEST_SUITE_NAMESPACE + "-shared-" + TestConstants.SCRAPER_NAME;

        resourceManager.createResourceWithWait(ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, scraperName).build());
        scraperPodName = kubeClient(Environment.TEST_SUITE_NAMESPACE).listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, scraperName).get(0).getMetadata().getName();

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridgeWithCors(httpBridgeCorsClusterName, KafkaResources.plainBootstrapAddress(httpBridgeCorsClusterName),
            1, ALLOWED_ORIGIN, null)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        KafkaBridgeHttpCors kafkaBridgeHttpCors = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(httpBridgeCorsClusterName).get().getSpec().getHttp().getCors();
        LOGGER.info("Bridge with the following CORS settings {}", kafkaBridgeHttpCors.toString());

        bridgeUrl = KafkaBridgeResources.url(httpBridgeCorsClusterName, Environment.TEST_SUITE_NAMESPACE, TestConstants.HTTP_BRIDGE_DEFAULT_PORT);
    }
}
