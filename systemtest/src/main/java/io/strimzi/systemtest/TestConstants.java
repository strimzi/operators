/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.TestUtils;

import java.time.Duration;
import java.util.Map;

/**
 * Interface for keep global constants used across system tests.
 */
public interface TestConstants {
    long TIMEOUT_FOR_RESOURCE_RECOVERY = Duration.ofMinutes(6).toMillis();
    long TIMEOUT_FOR_MIRROR_MAKER_2_COPY_MESSAGES_BETWEEN_BROKERS = Duration.ofMinutes(7).toMillis();
    long TIMEOUT_FOR_LOG = Duration.ofMinutes(2).toMillis();
    long POLL_INTERVAL_FOR_RESOURCE_READINESS = Duration.ofSeconds(1).toMillis();
    long POLL_INTERVAL_FOR_RESOURCE_DELETION = Duration.ofSeconds(5).toMillis();
    long WAIT_FOR_ROLLING_UPDATE_INTERVAL = Duration.ofSeconds(5).toMillis();

    long TIMEOUT_FOR_SEND_RECEIVE_MSG = Duration.ofSeconds(60).toMillis();
    long TIMEOUT_FOR_CLUSTER_STABLE = Duration.ofMinutes(20).toMillis();

    long TIMEOUT_TEARDOWN = Duration.ofSeconds(10).toMillis();
    long GLOBAL_TIMEOUT = Duration.ofMinutes(5).toMillis();
    long GLOBAL_TIMEOUT_SHORT = Duration.ofMinutes(2).toMillis();
    long GLOBAL_TIMEOUT_LONG = Duration.ofMinutes(10).toMillis();
    long GLOBAL_CMD_CLIENT_TIMEOUT = Duration.ofMinutes(5).toMillis();
    long GLOBAL_STATUS_TIMEOUT = Duration.ofMinutes(3).toMillis();
    long GLOBAL_POLL_INTERVAL = Duration.ofSeconds(1).toMillis();
    long GLOBAL_POLL_INTERVAL_5_SECS = Duration.ofSeconds(5).toMillis();
    long GLOBAL_POLL_INTERVAL_MEDIUM = Duration.ofSeconds(10).toMillis();
    long GLOBAL_POLL_INTERVAL_LONG = Duration.ofSeconds(30).toMillis();
    long PRODUCER_TIMEOUT = Duration.ofSeconds(25).toMillis();
    long METRICS_COLLECT_TIMEOUT = Duration.ofMinutes(1).toMillis();

    long GLOBAL_TRACING_POLL = Duration.ofSeconds(30).toMillis();

    long API_CRUISE_CONTROL_POLL = Duration.ofSeconds(5).toMillis();
    long API_CRUISE_CONTROL_TIMEOUT = Duration.ofMinutes(10).toMillis();
    long GLOBAL_CRUISE_CONTROL_TIMEOUT = Duration.ofMinutes(5).toMillis();
    long GLOBAL_CRUISE_CONTROL_TIMEOUT_LONG = Duration.ofMinutes(10).toMillis();
    long CRUISE_CONTROL_TRAIN_MODEL_TIMEOUT = Duration.ofMinutes(8).toMillis();

    long OLM_UPGRADE_INSTALL_PLAN_TIMEOUT = Duration.ofMinutes(15).toMillis();
    long OLM_UPGRADE_INSTALL_PLAN_POLL = Duration.ofMinutes(1).toMillis();

    long GLOBAL_CLIENTS_POLL = Duration.ofSeconds(15).toMillis();
    long GLOBAL_CLIENTS_TIMEOUT = Duration.ofMinutes(2).toMillis();
    long GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT = Duration.ofSeconds(10).toMillis();

    long CO_OPERATION_TIMEOUT_DEFAULT = Duration.ofMinutes(5).toMillis();
    long CO_OPERATION_TIMEOUT_SHORT = Duration.ofSeconds(30).toMillis();
    long CO_OPERATION_TIMEOUT_MEDIUM = Duration.ofMinutes(2).toMillis();
    long RECONCILIATION_INTERVAL = Duration.ofSeconds(30).toMillis();
    long SAFETY_RECONCILIATION_INTERVAL = (RECONCILIATION_INTERVAL + Duration.ofSeconds(10).toMillis()) * 2;
    long LOGGING_RELOADING_INTERVAL = Duration.ofSeconds(30).toMillis();
    long CC_LOG_CONFIG_RELOAD = Duration.ofSeconds(5).toMillis();

    // Keycloak
    long KEYCLOAK_DEPLOYMENT_POLL = Duration.ofSeconds(5).toMillis();
    long KEYCLOAK_DEPLOYMENT_TIMEOUT = Duration.ofMinutes(10).toMillis();
    long CA_CERT_VALIDITY_DELAY = 10;

    // stability count ensures that after some reconciliation we have some additional time
    int GLOBAL_STABILITY_OFFSET_COUNT = 20;
    // it is replacement instead of checking logs for reconciliation using dynamic waiting on some change for some period of time
    int GLOBAL_RECONCILIATION_COUNT = (int) ((RECONCILIATION_INTERVAL / GLOBAL_POLL_INTERVAL) + GLOBAL_STABILITY_OFFSET_COUNT);

    long THROTTLING_EXCEPTION_TIMEOUT = Duration.ofMinutes(10).toMillis();

    // sometimes each call `curl -X GET http://localhost:8083/connectors` could take in maximum 13s, and we do 50 calls; meaning (13s * 50)/60 ~= 11m
    long KAFKA_CONNECTOR_STABILITY_TIMEOUT = Duration.ofMinutes(12).toMillis();

    // Jaeger
    long JAEGER_DEPLOYMENT_TIMEOUT = Duration.ofMinutes(4).toMillis();
    long JAEGER_DEPLOYMENT_POLL = Duration.ofMinutes(1).toMillis();

    /**
     * Constants for KafkaConnect EchoSink plugin
     */
    String ECHO_SINK_CONNECTOR_NAME = "echo-sink-connector";
    String ECHO_SINK_CLASS_NAME = "cz.scholz.kafka.connect.echosink.EchoSinkConnector";
    String ECHO_SINK_TGZ_URL = "https://github.com/scholzj/echo-sink/archive/1.6.0.tar.gz";
    String ECHO_SINK_TGZ_CHECKSUM = "19b8d501ce0627cff2770ee489e59c205ac81263e771aa11b5848c2c289d917cda22f1fc7fc693a91bad63181787d7c48791796f1a33f8f75d594aefebf1e684";
    String ECHO_SINK_JAR_URL = "https://github.com/scholzj/echo-sink/releases/download/1.6.0/echo-sink-1.6.0.jar";
    String ECHO_SINK_JAR_CHECKSUM = "3f30d48079578f9f2d0a097ed9a7088773b135dff3dc8e70d87f8422c073adc1181cb41d823c1d1472b0447a337e4877e535daa34ca8ef21d608f8ee6f5e4a9c";
    String ECHO_SINK_FILE_NAME = "echo-sink-test.jar";
    String ECHO_SINK_JAR_WRONG_CHECKSUM = "f1f167902325062efc8c755647bc1b782b2b067a87a6e507ff7a3f6205803220";

    /**
     * Scraper pod labels
     */
    String SCRAPER_LABEL_KEY = "user-test-app";
    String SCRAPER_LABEL_VALUE = "scraper";

    String SCRAPER_NAME = "scraper";

    /**
     * Constants for Kafka clients labels
     */
    String KAFKA_CLIENTS_LABEL_KEY = "user-test-app";
    String KAFKA_ADMIN_CLIENT_LABEL_KEY = "user-test-admin-app";
    String ADMIN_CLIENT_NAME = "admin-client";

    String KAFKA_CLIENTS_LABEL_VALUE = "kafka-clients";
    String KAFKA_ADMIN_CLIENT_LABEL_VALUE = "kafka-clients";
    String KAFKA_BRIDGE_CLIENTS_LABEL_VALUE = "kafka-clients";

    String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    String ALWAYS_IMAGE_PULL_POLICY = "Always";
    String IF_NOT_PRESENT_IMAGE_PULL_POLICY = "IfNotPresent";

    /**
     * Drain Cleaner related constants
     */
    String DRAIN_CLEANER_DEPLOYMENT_NAME = "strimzi-drain-cleaner";
    String DRAIN_CLEANER_NAMESPACE = "strimzi-drain-cleaner";

    /**
     * Deployment labels related constants
     */
    String APP_POD_LABEL = "app";
    String APP_KUBERNETES_INSTANCE_LABEL = "app.kubernetes.io/instance";
    String APP_CONTROLLER_LABEL = "controlled-by";

    /**
     * Cluster operator config images
     */
    String KAFKA_IMAGE_MAP = "STRIMZI_KAFKA_IMAGES";
    String TO_IMAGE = "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE";
    String UO_IMAGE = "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE";
    String KAFKA_INIT_IMAGE = "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE";
    String KAFKA_MIRROR_MAKER_2_IMAGE_MAP = "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES";
    String KAFKA_CONNECT_IMAGE_MAP = "STRIMZI_KAFKA_CONNECT_IMAGES";
    /**
     * Constants for specific ports
     */
    int COMPONENTS_METRICS_PORT = 9404;
    int CLUSTER_OPERATOR_METRICS_PORT = 8080;
    int USER_OPERATOR_METRICS_PORT = 8081;
    int TOPIC_OPERATOR_METRICS_PORT = 8080;
    int KAFKA_BRIDGE_METRICS_PORT = 8080;
    int JMX_PORT = 9999;

    String DEPLOYMENT = "Deployment";
    String DEPLOYMENT_TYPE = "deployment-type";
    String SERVICE = "Service";
    String CONFIG_MAP = "ConfigMap";
    String LEASE = "Lease";
    String SERVICE_ACCOUNT = "ServiceAccount";
    String CLUSTER_ROLE = "ClusterRole";
    String CLUSTER_ROLE_BINDING = "ClusterRoleBinding";
    String CUSTOM_RESOURCE_DEFINITION = "CustomResourceDefinition";
    String CUSTOM_RESOURCE_DEFINITION_SHORT = "Crd";
    String ROLE_BINDING = "RoleBinding";
    String ROLE = "Role";
    String DEPLOYMENT_CONFIG = "DeploymentConfig";
    String SECRET = "Secret";
    String KAFKA_EXPORTER_DEPLOYMENT = "KafkaWithExporter";
    String KAFKA_CRUISE_CONTROL_DEPLOYMENT = "KafkaWithCruiseControl";
    String STATEFUL_SET = "StatefulSet";
    String POD = "Pod";
    String NETWORK_POLICY = "NetworkPolicy";
    String JOB = "Job";
    String VALIDATION_WEBHOOK_CONFIG = "ValidatingWebhookConfiguration";
    String REPLICA_SET = "ReplicaSet";
    String SUBSCRIPTION = "Subscription";
    String OPERATOR_GROUP = "OperatorGroup";
    String BUILD_CONFIG = "BuildConfig";
    String IMAGE_STREAM = "ImageStream";
    String INSTALL_PLAN = "InstallPlan";
    String CLUSTER_SERVICE_VERSION = "ClusterServiceVersion";

    /**
     * KafkaBridge JSON encoding with JSON embedded format
     */
    String KAFKA_BRIDGE_JSON_JSON = "application/vnd.kafka.json.v2+json";
    String DEFAULT_SINK_FILE_PATH = "/tmp/test-file-sink.txt";

    int HTTP_BRIDGE_DEFAULT_PORT = 8080;
    int HTTPS_KEYCLOAK_DEFAULT_NODE_PORT = 32481;

    /**
     * Basic paths to examples
     */
    String PATH_TO_PACKAGING = TestUtils.USER_PATH + "/../packaging";
    String PATH_TO_PACKAGING_EXAMPLES = PATH_TO_PACKAGING + "/examples";
    String PATH_TO_PACKAGING_INSTALL_FILES = PATH_TO_PACKAGING + "/install";

    /**
     * File paths for metrics YAMLs
     */
    String PATH_TO_KAFKA_METRICS_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/metrics/kafka-metrics.yaml";

    String METRICS_CONFIG_YAML_NAME = "metrics-config.yml";
    String METRICS_CONFIG_JSON_NAME = "metrics-config.json";

    String PATH_TO_KAFKA_CONNECT_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/connect/kafka-connect.yaml";
    String PATH_TO_KAFKA_CONNECT_METRICS_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/metrics/kafka-connect-metrics.yaml";
    String PATH_TO_KAFKA_BRIDGE_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/bridge/kafka-bridge.yaml";
    String PATH_TO_KAFKA_REBALANCE_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/cruise-control/kafka-rebalance-full.yaml";
    String PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/cruise-control/kafka-cruise-control.yaml";
    String PATH_TO_KAFKA_CRUISE_CONTROL_METRICS_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/metrics/kafka-cruise-control-metrics.yaml";
    String PATH_TO_KAFKA_TOPIC_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml";
    String PATH_TO_KAFKA_CONNECTOR_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/connect/source-connector.yaml";
    String PATH_TO_KAFKA_MIRROR_MAKER_2_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/mirror-maker/kafka-mirror-maker-2.yaml";
    String PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/metrics/kafka-mirror-maker-2-metrics.yaml";

    String TEST_CASE_NAME_LABEL = "test.case";
    String TEST_SUITE_NAME_LABEL = "test.suite";

    /**
     * CruiseControl related parameters
     */
    String CRUISE_CONTROL_NAME = "Cruise Control";
    String CRUISE_CONTROL_CONTAINER_NAME = "cruise-control";
    String CRUISE_CONTROL_CONFIGURATION_ENV = "CRUISE_CONTROL_CONFIGURATION";
    String CRUISE_CONTROL_CONFIGURATION_FILE_PATH = "/tmp/cruisecontrol.properties";
    String CRUISE_CONTROL_LOG_FILE_PATH = "/opt/cruise-control/custom-config/log4j2.properties";

    /**
     * Default listeners names
     */
    String PLAIN_LISTENER_DEFAULT_NAME = "plain";
    String TLS_LISTENER_DEFAULT_NAME = "tls";
    String EXTERNAL_LISTENER_DEFAULT_NAME = "external";
    String CLUSTER_IP_LISTENER_DEFAULT_NAME = "clusterip";

    /**
     * Loadbalancer finalizer config
     */
    String LOAD_BALANCER_CLEANUP = "service.kubernetes.io/load-balancer-cleanup";

    // main namespace for Cluster Operator deployment
    String CO_NAMESPACE = "co-namespace";

    /**
     * Auxiliary variables for storing data across our tests
     */
    String NAMESPACE_KEY = "NAMESPACE_NAME";
    String PREPARE_OPERATOR_ENV_KEY = "PREPARE_OPERATOR_ENV";

    // Count of test messages that needs to be sent during the test
    int MESSAGE_COUNT = 100;
    int CONTINUOUS_MESSAGE_COUNT = 200;

    /**
     * Auxiliary variable for cluster operator deployment
     */
    String WATCH_ALL_NAMESPACES = "*";

    String TEST_NAME_KEY = "TEST_NAME";
    String CLUSTER_KEY = "CLUSTER_NAME";
    String BROKER_POOL_KEY = "BROKER_POOL";
    String CONTROLLER_POOL_KEY = "CONTROLLER_POOL";
    String MIXED_POOL_KEY = "MIXED_POOL";
    String SOURCE_CLUSTER_KEY = "SOURCE_CLUSTER_NAME";
    String SOURCE_BROKER_POOL_KEY = "SOURCE_BROKER_POOL";
    String SOURCE_CONTROLLER_POOL_KEY = "SOURCE_CONTROLLER_POOL";
    String TARGET_CLUSTER_KEY = "TARGET_CLUSTER_NAME";
    String TARGET_BROKER_POOL_KEY = "TARGET_BROKER_POOL";
    String TARGET_CONTROLLER_POOL_KEY = "TARGET_CONTROLLER_POOL";
    String TOPIC_KEY = "TOPIC_NAME";
    String CONTINUOUS_TOPIC_KEY = "CONTINUOUS_TOPIC_NAME";
    String TARGET_TOPIC_KEY = "TARGET_TOPIC_NAME";
    String MIRRORED_SOURCE_TOPIC_KEY = "MIRRORED_SOURCE_TOPIC_NAME";
    String STREAM_TOPIC_KEY = "STREAM_TOPIC_NAME";
    String SCRAPER_KEY = "SCRAPER_NAME";
    String PRODUCER_KEY = "PRODUCER_NAME";
    String CONTINUOUS_PRODUCER_KEY = "CONTINUOUS_PRODUCER_NAME";
    String CONSUMER_KEY = "CONSUMER_NAME";
    String CONTINUOUS_CONSUMER_KEY = "CONTINUOUS_CONSUMER_NAME";
    String ADMIN_KEY = "ADMIN_NAME";
    String USER_NAME_KEY = "USER_NAME";
    String SOURCE_USER_NAME_KEY = "SOURCE_USER_NAME";
    String TARGET_USER_NAME_KEY = "TARGET_USER_NAME";
    String KAFKA_USER_NAME_KEY = "KAFKA_USER_NAME";
    String ENTITY_OPERATOR_NAME_KEY = "ENTITY_OPERATOR_NAME";
    String BROKER_COMPONENT_NAME_KEY = "BROKER_COMPONENT_NAME";
    String CONTROLLER_COMPONENT_NAME_KEY = "CONTROLLER_COMPONENT_NAME";
    String MIXED_COMPONENT_NAME_KEY = "MIXED_COMPONENT_NAME";
    String SCRAPER_POD_KEY = "SCRAPER_POD_NAME";
    String KAFKA_TRACING_CLIENT_KEY = "KAFKA_TRACING_CLIENT";
    String BROKER_SELECTOR_KEY = "BROKER_SELECTOR";
    String BROKER_POOL_SELECTOR_KEY = "BROKER_POOL_SELECTOR";
    String CONTROLLER_POOL_SELECTOR_KEY = "CONTROLLER_POOL_SELECTOR";
    String MIXED_POOL_SELECTOR_KEY = "MIXED_POOL_SELECTOR";
    String CONTROLLER_SELECTOR_KEY = "CONTROLLER_SELECTOR";
    String MIXED_SELECTOR_KEY = "MIXED_SELECTOR";
    String KAFKA_CONNECT_SELECTOR_KEY = "KAFKA_CONNECT_SELECTOR";
    String MM2_SELECTOR_KEY = "MM2_SELECTOR";
    String MESSAGE_COUNT_KEY = "MESSAGE_COUNT";
    String CONTINUOUS_MESSAGE_COUNT_KEY = "CONTINUOUS_MESSAGE_COUNT";
    String TEST_EXECUTION_START_TIME_KEY = "TEST_EXECUTION_START_TIME";

    /**
     * Lease related resources - ClusterRole, Role, RoleBinding
     */
    String PATH_TO_LEASE_CLUSTER_ROLE = PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-ClusterRole-strimzi-cluster-operator-role.yaml";
    // Path after change of ClusterRole -> Role in our SetupClusterOperator class
    String PATH_TO_LEASE_ROLE = PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-Role-strimzi-cluster-operator-role.yaml";
    String PATH_TO_LEASE_ROLE_BINDING = PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-RoleBinding-strimzi-cluster-operator.yaml";
    Map<String, String> LEASE_FILES_AND_RESOURCES = Map.of(
        CLUSTER_ROLE, PATH_TO_LEASE_CLUSTER_ROLE,
        ROLE, PATH_TO_LEASE_ROLE,
        ROLE_BINDING, PATH_TO_LEASE_ROLE_BINDING
    );

    /**
     * Cluster Operator resources config
     */
    String CO_REQUESTS_MEMORY = "512Mi";
    String CO_REQUESTS_CPU = "200m";
    String CO_LIMITS_MEMORY = "512Mi";
    String CO_LIMITS_CPU = "1000m";

    /**
     * Connect build image name
     */
    String ST_CONNECT_BUILD_IMAGE_NAME = "strimzi-sts-connect-build";

    /**
     * Persistent Volume related
     */
    String PVC_PHASE_BOUND = "Bound";

    /**
     * NodePool's name prefix based on role
     */
    String MIXED_ROLE_PREFIX = "m-";
    String BROKER_ROLE_PREFIX = "b-";
    String CONTROLLER_ROLE_PREFIX = "c-";

    /**
     * Container runtime constants
     */
    String DOCKER = "docker";
    String PODMAN = "podman";
}
