# CHANGELOG

## 0.46.0

* Support for ZooKeeper-based Apache Kafka clusters and for KRaft migration has been removed
* Support for MirrorMaker 1 has been removed
* Support for storage class overrides has been removed
* Added support to configure `dnsPolicy` and `dnsConfig` using the `template` sections.
* Store Kafka node certificates in separate Secrets, one Secret per pod.
* Allow configuring `ssl.principal.mapping.rules` and custom trusted CAs in Kafka brokers with `type: custom` authentication
* Moved HTTP bridge configuration to the ConfigMap setup by the operator.
* Dependency updates (Vert.x 4.5.12, Netty 4.1.117.Final)
* Moved Kafka Connect configuration to the ConfigMap created by the operator.
* Update Kafka Exporter to [1.9.0](https://github.com/danielqsj/kafka_exporter/releases/tag/v1.9.0)

### Major changes, deprecations and removals

* **Support for ZooKeeper-based clusters and for migration from ZooKeeper-based clusters to KRaft has been removed.**
  **Please make sure all your clusters are using KRaft before upgrading to Strimzi 0.46.0 or newer!**
* Support for MirrorMaker 1 has been removed.
  Please make sure to migrate to MirrorMaker 2 before upgrading to Strimzi 0.46 or newer.
* [Strimzi EnvVar Configuration Provider](https://github.com/strimzi/kafka-env-var-config-provider) (deprecated in Strimzi 0.38.0) and [Strimzi MirrorMaker 2 Extensions](https://github.com/strimzi/mirror-maker-2-extensions) (deprecated in Strimzi 0.28.0) plugins were removed from Strimzi container images.
  Please use the Apache Kafka [EnvVarConfigProvider](https://github.com/strimzi/kafka-env-var-config-provider?tab=readme-ov-file#deprecation-notice) and [Identity Replication Policy](https://github.com/strimzi/mirror-maker-2-extensions?tab=readme-ov-file#identity-replication-policy) instead.
* When using Kafka Connect or Kafka MirrorMaker 2 operands and upgrading from Strimzi 0.38 or older, make sure the `StableConnectIdentities` feature gate is enabled and `StrimziPodSets` are used before upgrading.
* When using the Kafka operand and upgrading from Strimzi 0.34 or older, make sure the `UseStrimziPodSets` feature gate is enabled and `StrimziPodSet` resources are used before upgrading.
* The storage overrides for configuring per-broker storage class are not supported anymore.
  If you are using the storage overrides, you should instead use multiple KafkaNodePool resources with a different storage class each.
  For more details about migrating from storage overrides, please follow the [documentation](https://strimzi.io/docs/operators/0.45.0/full/deploying.html#con-config-storage-zookeeper-str).
* Removed the `statefulset.kubernetes.io/pod-name` label from pods and external listeners Kubernetes Services.
  * If you have any custom setup leveraging such label, please use the `strimzi.io/pod-name` one instead.
* The `secrets` list for mounting additional Kubernetes Secrets in `type: custom` authentication was deprecated and will be removed in the future.
  Please use the template section to configure additional volumes instead.

## 0.45.0

* Add support for Kafka 3.9.0 and 3.8.1. 
  Remove support for Kafka 3.7.0 and 3.7.1
* Ability to move data between JBOD disks using Cruise Control.
* Allow using custom certificates for communication with container registry in Kafka Connect Build with Kaniko 
* Only roll pods once for ClientsCa cert renewal.
  This change also means the restart reason ClientCaCertKeyReplaced is removed and either CaCertRenewed or CaCertHasOldGeneration will be used.
* Allow rolling update for new cluster CA trust (during Cluster CA key replacement) to continue where it left off before interruption without rolling all pods again.
* Update HTTP bridge to 0.31.1
* Add support for mounting CSI volumes using the `template` sections

### Major changes, deprecations and removals

* **Strimzi 0.45 is the last minor Strimzi version with support for ZooKeeper-based Apache Kafka clusters and MirrorMaker 1 deployments.**
  **Please make sure to [migrate to KRaft](https://strimzi.io/docs/operators/latest/full/deploying.html#assembly-kraft-mode-str) and MirrorMaker 2 before upgrading to Strimzi 0.46 or newer.**
* **Strimzi 0.45 is the last Strimzi version to include the [Strimzi EnvVar Configuration Provider](https://github.com/strimzi/kafka-env-var-config-provider) (deprecated in Strimzi 0.38.0) and [Strimzi MirrorMaker 2 Extensions](https://github.com/strimzi/mirror-maker-2-extensions) (deprecated in Strimzi 0.28.0).**
  Please use the Apache Kafka [EnvVarConfigProvider](https://github.com/strimzi/kafka-env-var-config-provider?tab=readme-ov-file#deprecation-notice) and [Identity Replication Policy](https://github.com/strimzi/mirror-maker-2-extensions?tab=readme-ov-file#identity-replication-policy) instead. 

## 0.44.0

* Add the "Unmanaged" KafkaTopic status update.
* The `ContinueReconciliationOnManualRollingUpdateFailure` feature gate moves to beta stage and is enabled by default.
  If needed, `ContinueReconciliationOnManualRollingUpdateFailure` can be disabled in the feature gates configuration in the Cluster Operator.
* Add support for managing connector offsets via KafkaConnector and KafkaMirrorMaker2 custom resources.
* Add support for templating `host` and `advertisedHost` fields in listener configuration.
* Allow configuration of environment variables based on Config Map or Secret for every container in the container template sections.
* Add support for disabling the generation of PodDisruptionBudget resources by the Cluster Operator.
* Add support for running an automatic rebalancing, via Cruise Control, when the cluster is scaled down or up:
  * after a scaling up, the operator triggers an auto-rebalancing for moving some of the existing partitions to the newly added brokers.
  * before scaling down, and if the brokers to remove are hosting partitions, the operator triggers an auto-rebalancing to these partitions off the brokers to make them free to be removed.
* Strimzi Access Operator 0.1.0 added to the installation files and examples

### Changes, deprecations and removals

* **From Strimzi 0.44.0 on, we support only Kubernetes 1.25 and newer.**
  Kubernetes 1.23 and 1.24 are not supported anymore.
* When finalizers are enabled (default), the Topic Operator will no longer restore finalizers on unmanaged `KafkaTopic` resources if they are removed, aligning the behavior with paused topics, where finalizers are also not restored.
  This change matches user expectations.
* The External Configuration (`.spec.externalConfiguration`) in `KafkaConnect` and `KafkaMirrorMaker2` resources is deprecated and will be removed in the future.
  Please use the environment variables, additional volumes and volume mounts in Pod and container templates instead.
* The Strimzi Canary installation files were removed based on the [_Strimzi proposal 086_](https://github.com/strimzi/proposals/blob/main/086-archive-canary.md) as the project was discontinued and archived.

## 0.43.0

* Add support for Apache Kafka 3.8.0.
  Remove support for Apache Kafka 3.6.0, 3.6.1, and 3.6.2.
* Added alerts for Connectors/Tasks in failed state.
* Support for specifying additional volumes and volume mounts in Strimzi custom resources
* Strimzi Drain Cleaner updated to 1.2.0 (included in the Strimzi installation files)
* Additional OAuth configuration options have been added for 'oauth' authentication on the listener and the client. 
  On the listener `serverBearerTokenLocation` and `userNamePrefix` have been added. 
  On the client `accessTokenLocation`, `clientAssertion`, `clientAssertionLocation`, `clientAssertionType`, and `saslExtensions` have been added.
* Add support for custom Cruise Control API users
* Update HTTP bridge to latest 0.30.0 release
* Unregistration of KRaft nodes after scale-down
* Update Kafka Exporter to [1.8.0](https://github.com/danielqsj/kafka_exporter/releases/tag/v1.8.0) and update the Grafana dashboard to work with it

### Changes, deprecations and removals

* The storage overrides for configuring per-broker storage class are deprecated and will be removed in the future.
  If you are using the storage overrides, you should migrate to KafkaNodePool resources and use multiple node pools with a different storage class each. 
* Strimzi 0.43.0 (and any of its patch releases) is the last Strimzi version with support for Kubernetes 1.23 and 1.24.
  From Strimzi 0.44.0 on, we will support only Kubernetes 1.25 and newer.

## 0.42.0

* Add support for Kafka 3.7.1
* The `UseKRaft` feature gate moves to GA stage and is permanently enabled without the possibility to disable it.
  To use KRaft (ZooKeeper-less Apache Kafka), you still need to use the `strimzi.io/kraft: enabled` annotation on the `Kafka` custom resources or migrate from an existing ZooKeeper-based cluster.
* Update the base image used by Strimzi containers from UBI8 to UBI9
* Add support for filename patterns when configuring trusted certificates
* Enhance `KafkaBridge` resource with consumer inactivity timeout and HTTP consumer/producer enablement.
* Add support for feature gates to User and Topic Operators
* Add support for setting `publishNotReadyAddresses` on services for listener types other than internal.
* Update HTTP bridge to latest 0.29.0 release
* Uncommented and enabled (by default) KRaft-related metrics in the `kafka-metrics.yaml` example file.
* Added support for configuring the quotas plugin with type `strimzi` or `kafka` in the `Kafka` custom resource.
  The Strimzi Quotas plugin version was updated to 0.3.1.

### Changes, deprecations and removals

* The `reconciliationIntervalSeconds` configuration for the Topic and User Operators is deprecated, and will be removed when upgrading schemas to v1.
  Use `reconciliationIntervalMs` converting the value to milliseconds.
* Usage of Strimzi Quotas plugin version 0.2.0 is not supported, the plugin was updated to 0.3.1 and changed significantly.
  Additionally, from Strimzi 0.42.0 the plugin should be configured in `.spec.kafka.quotas` section - the configuration of the plugin inside `.spec.kafka.config` is ignored and should be removed.

## 0.41.0

* Add support for Apache Kafka 3.6.2
* Provide metrics to monitor certificates expiration as well as modified `Strimzi Operators` dashboard to include certificate expiration per cluster.
* Add support for JBOD storage in KRaft mode.
  (Note: JBOD support in KRaft mode is considered early-access in Apache Kafka 3.7.x)
* Added support for topic replication factor change to the Unidirectional Topic Operator when Cruise Control integration is enabled.
* The `KafkaNodePools` feature gate moves to GA stage and is permanently enabled without the possibility to disable it.
  To use the Kafka Node Pool resources, you still need to use the `strimzi.io/node-pools: enabled` annotation on the `Kafka` custom resources.
* Added support for configuring the `externalIPs` field in node port type services.
* The `UnidirectionalTopicOperator` feature gate moves to GA stage and is permanently enabled without the possibility to disable it.
  If the topics whose names start with `strimzi-store-topic` and `strimzi-topic-operator` still exist, you can delete them.
* Don't allow MirrorMaker2 mirrors with target set to something else than the connect cluster. 
* Added support for custom SASL config in standalone Topic Operator deployment to support alternate access controllers (i.e. `AWS_MSK_IAM`)
* Remove Angular dependent plugins from Grafana example dashboard. This makes our dashboard compatible with Grafana 7.4.5 and higher.
* Continue reconciliation after failed manual rolling update using the `strimzi.io/manual-rolling-update` annotation (when the `ContinueReconciliationOnManualRollingUpdateFailure` feature gate is enabled).

### Changes, deprecations and removals

* The `tlsSidecar` configuration for the Entity Operator is now deprecated and will be ignored.
* The `zookeeperSessionTimeoutSeconds` and `topicMetadataMaxAttempts` configurations for the Entity Topic Operator have been removed and will be ignored.

## 0.40.0

* Add support for Apache Kafka 3.7.0.
  Remove support for Apache Kafka 3.5.0, 3.5.1, and 3.5.2.
* The `UseKRaft` feature gate moves to beta stage and is enabled by default.
  If needed, `UseKRaft` can be disabled in the feature gates configuration in the Cluster Operator.
* Add support for ZooKeeper to KRaft migration by enabling the users to move from using ZooKeeper to store metadata to use KRaft.
* Add support for moving from dedicated controller-only KRaft nodes to mixed KRaft nodes
* Fix NullPointerException from missing listenerConfig when using custom auth
* Added support for Kafka Exporter `offset.show-all` parameter
* Prevent removal of the `broker` process role from KRaft mixed-nodes that have assigned partition-replicas
* Improve broker scale-down prevention to continue in reconciliation when scale-down cannot be executed
* Added support for Tiered Storage by enabling the configuration of custom storage plugins through the Kafka custom resource.
* Update HTTP bridge to latest 0.28.0 release

### Changes, deprecations and removals

* **From Strimzi 0.40.0 on, we support only Kubernetes 1.23 and newer.**
  Kubernetes 1.21 and 1.22 are not supported anymore.
* [#9508](https://github.com/strimzi/strimzi-kafka-operator/pull/9508) fixes the Strimzi CRDs and their definitions of labels and annotations.
  This change brings our CRDs in-sync with the Kubernetes APIs.
  After this fix, the label and annotation definitions in our CRDs (for example in the `template` sections) cannot contain integer values anymore and have to always use string values.
  If your custom resources use an integer value, for example:
  ```
  template:
    apiService:
      metadata:
        annotations:
          discovery.myapigateway.io/port: 8080
  ```
  You might get an error similar to this when applying the resource:
  ```
  * spec.template.apiService.metadata.annotations.discovery.myapigateway.io/port: Invalid value: "integer": spec.template.apiService.metadata.annotations.discovery.myapigateway.io/port in body must be of type string: "integer"
  ```
  To fix the issue, just use a string value instead of an integer:
  ```
  template:
    apiService:
      metadata:
        annotations:
          discovery.myapigateway.io/port: "8080"
  ```
* Support for the JmxTrans component is now completely removed.
  If you are upgrading from Strimzi 0.34 or earlier and have JmxTrans enabled in `.spec.jmxTrans` of the `Kafka` custom resource, you should disable it before the upgrade or delete it manually after the upgrade is complete.
* The `api` module was refactored and classes were moved to new packages.
* Strimzi Drain Cleaner 1.1.0 (included in the Strimzi 0.40.0 installation files) changes the way it handles Kubernetes eviction requests.
  It denies them instead of allowing them.
  This new behavior does not require the `PodDisruptionBudget` to be set to `maxUnavailable: 0`.
  We expect this to improve the compatibility with various tools used for scaling Kubernetes clusters such as [Karpenter](https://karpenter.sh/).
  If you observe any problems with your toolchain or just want to stick with the previous behavior, you can use the `STRIMZI_DENY_EVICTION` environment variable and set it to `false` to switch back to the old (legacy) mode.

## 0.39.0

* Add support for Apache Kafka 3.5.2 and 3.6.1
* The `StableConnectIdentities` feature gate moves to GA stage and is now permanently enabled without the possibility to disable it.
  All Connect and Mirror Maker 2 operands will now use StrimziPodSets.
* The `KafkaNodePools` feature gate moves to beta stage and is enabled by default.
  If needed, `KafkaNodePools` can be disabled in the feature gates configuration in the Cluster Operator.
* The `UnidirectionalTopicOperator` feature gate moves to beta stage and is enabled by default.
  If needed, `UnidirectionalTopicOperator` can be disabled in the feature gates configuration in the Cluster Operator.
* Improved Kafka Connect metrics and dashboard example files
* Allow specifying and managing KRaft metadata version
* Add support for KRaft to KRaft upgrades (Apache Kafka upgrades for the KRaft based clusters)
* Improved Kafka Mirror Maker 2 dashboard example file

### Changes, deprecations and removals

* The `StableConnectIdentities` feature gate moves to GA stage and cannot be disabled anymore.
  When using Connect or Mirror Maker 2 operands, direct downgrade to Strimzi versions older than 0.34 is not supported anymore.
  You have to first downgrade to Strimzi version between 0.34 to 0.38, disable the `StableConnectIdentities` feature gate, and only then downgrade to an older Strimzi version.
* Strimzi 0.39.0 (and any of its patch releases) is the last Strimzi version with support for Kubernetes 1.21 and 1.22.
  From Strimzi 0.40.0 on, we will support only Kubernetes 1.23 and newer.

## 0.38.0

* Add support for Apache Kafka 3.6.0 and drop support for 3.4.0 and 3.4.1
* Sign containers using `cosign`
* Generate and publish Software Bill of Materials (SBOMs) of Strimzi containers
* Add support for stopping connectors according to [Strimzi Proposal #54](https://github.com/strimzi/proposals/blob/main/054-stopping-kafka-connect-connectors.md)
* Allow manual rolling of Kafka Connect and Kafka Mirror Maker 2 pods using the `strimzi.io/manual-rolling-update` annotation (supported only when `StableConnectIdentities` feature gate is enabled) 
* Make sure brokers are empty before scaling them down
* Update Cruise Control to 2.5.128
* Add support for pausing reconciliations to the Unidirectional Topic Operator
* Allow running ZooKeeper and KRaft based Apache Kafka clusters in parallel when the `+UseKRaft` feature gate is enabled
* Add support for metrics to the Unidirectional Topic Operator
* Added the `includeAcceptHeader` option to OAuth client and listener authentication configuration and to `keycloak` authorization. If set to `false` it turns off sending of `Accept` header when communicating with OAuth / OIDC authorization server. This feature is enabled by the updated Strimzi Kafka OAuth library (0.14.0).
* Update HTTP bridge to latest 0.27.0 release

### Changes, deprecations and removals

* The `Kafka.KafkaStatus.ListenerStatus.type` property has been deprecated for a long time, and now we do not use it anymore.
  The current plan is to completely remove this property in the next schema version.
  If needed, you can use the `Kafka.KafkaStatus.ListenerStatus.name` property, which has the same value.
* Added `strimzi.io/kraft` annotation to be applied on `Kafka` custom resource, together with the `+UseKRaft` feature gate enabled, to declare a ZooKeeper or KRaft based cluster.
  * if `enabled` the `Kafka` resource defines a KRaft-based cluster.
  * if `disabled`, missing or any other value, the operator handle the `Kafka` resource as a ZooKeeper-based cluster.
* The `io.strimzi.kafka.EnvVarConfigProvider` configuration provider is now deprecated and will be removed in Strimzi 0.42. Users should migrate to Kafka's implementation, `org.apache.kafka.common.config.provider.EnvVarConfigProvider`, which is a drop-in replacement.
  For example:
  ```yaml
  config:
    # ...
    config.providers: env
    config.providers.env.class: io.strimzi.kafka.EnvVarConfigProvider
    # ...
  ```
  becomes
  ```yaml
  config:
    # ...
    config.providers: env
    config.providers.env.class: org.apache.kafka.common.config.provider.EnvVarConfigProvider
    # ...
  ```
  
## 0.37.0

* The `StableConnectIdentites` feature gate moves to beta stage.
  By default, StrimziPodSets are used for Kafka Connect and Kafka Mirror Maker 2.
  If needed, `StableConnectIdentites` can be disabled in the feature gates configuration in the Cluster Operator.
* Support for the `ppc64le` platform
* Added version fields to the `Kafka` custom resource status to track install and upgrade state
* Support for infinite auto-restarts of Kafka Connect and Kafka Mirror Maker 2 connectors

### Changes, deprecations and removals

* **Removed support for OpenTracing**:
  * The `tracing.type: jaeger` configuration, in `KafkaConnect`, `KafkaMirrorMaker`, `KafkaMirrorMaker2` and `KafkaBridge` resources, is not supported anymore.
  * The OpenTelemetry based tracing is the only available by using `tracing.type: opentelemetry`.
* **The default behavior of the Kafka Connect connector auto-restart has changed.**
  When the auto-restart feature is enabled in `KafkaConnector` or `KafkaMirrorMaker2` custom resources, it will now continue to restart the connectors indefinitely rather than stopping after 7 restarts, as previously.
  If you want to use the original behaviour, use the `.spec.autoRestart.maxRestarts` option to configure the maximum number of restarts.
  For example:
  ```yaml
  apiVersion: kafka.strimzi.io/v1beta2
  kind: KafkaConnector
  metadata:
    labels:
      strimzi.io/cluster: my-connect
    name: echo-sink-connector
  spec:
    # ...
    autoRestart:
      enabled: true
      maxRestarts: 7
    # ...
  ```
* **The automatic configuration of Cruise Control CPU capacity has been changed in this release**:
  * There are three ways to configure Cruise Control CPU capacity values:
    * `.spec.cruiseControl.brokerCapacity` (for all brokers)
    * `.spec.cruiseControl.brokerCapacity.overrides` (per broker)
    * Kafka resource requests and limits (for all brokers).
  * The precedence of which Cruise Control CPU capacity configuration is used has been changed.
  * In previous Strimzi versions, the Kafka resource limit (if set) took precedence, regardless if any other CPU configurations were set.
    * For example:
      * (1) Kafka resource limits
      * (2) `.spec.cruiseControl.brokerCapacity.overrides`
      * (3) `.spec.cruiseControl.brokerCapacity`
  * This previous behavior was identified as a bug and was fixed in this Strimzi release.
  * Going forward, the brokerCapacity overrides per broker take top precedence, then general brokerCapacity configuration, and then the Kafka resource requests, then the Kafka resource limits.
    * For example:
      * (1) `.spec.cruiseControl.brokerCapacity.overrides`
      * (2) `.spec.cruiseControl.brokerCapacity`
      * (3) Kafka resource requests
      * (4) Kafka resource limits
    * When none of Cruise Control CPU capacity configurations mentioned above are configured, CPU capacity will be set to `1`.
 as any _override_ value configured in the `.spec.cruiseControl` section of the `Kafka` custom resource.

## 0.36.1

* Add support for Apache Kafka 3.5.1

## 0.36.0

* Add support for Apache Kafka 3.4.1 and 3.5.0, and remove support for 3.3.1 and 3.3.2
* Enable SCRAM-SHA authentication in KRaft mode (supported in Apache Kafka 3.5.0 and newer)
* Add support for insecure flag in Maven artifacts in Kafka Connect Build
* Update Kafka Exporter to [1.7.0](https://github.com/danielqsj/kafka_exporter/releases/tag/v1.7.0)
* Improve Kafka rolling update to avoid rolling broker in log recovery
* Added support for Kafka Exporter topic exclude and consumer group exclude parameters
* Update Kaniko container builder to 1.12.1
* Add support for _Kafka node pools_ according to [Strimzi Proposal #50](https://github.com/strimzi/proposals/blob/main/050-Kafka-Node-Pools.md)
* Add support for _Unidirectional Topic Operator_ according to [Strimzi Proposal #51](https://github.com/strimzi/proposals/blob/main/051-unidirectional-topic-operator.md)
* Update OpenTelemetry 1.19.0
* Fixed ordering of JVM performance options [#8579](https://github.com/strimzi/strimzi-kafka-operator/issues/8579)
* Log a warning when a KafkaTopic has no spec [#8465](https://github.com/strimzi/strimzi-kafka-operator/issues/8465)
* Updated Strimzi OAuth library to 0.13.0 with better support for KRaft

### Changes, deprecations and removals

* **From Strimzi 0.36.0 on, we support only Kubernetes 1.21 and newer.**
  Kubernetes 1.19 and 1.20 are not supported anymore.
* Enabling the `UseKRaft` feature gate is now possible only together with the `KafkaNodePools` feature gate.
  To deploy a Kafka cluster in the KRaft mode, you have to use the `KafkaNodePool` resources.
* The Helm Chart repository at `https://strimzi.io/charts/` is now deprecated.
  Please use the Helm Chart OCI artifacts from our [Helm Chart OCI repository instead](https://quay.io/organization/strimzi-helm).
* Option `customClaimCheck` of 'oauth' authentication which relies on JsonPath changed the handling of equal comparison against `null` as the behaviour was buggy and is now fixed in the updated version of JsonPath library [OAuth #196](https://github.com/strimzi/strimzi-kafka-oauth/pull/196)

## 0.35.0

* Redesigned the Cluster and User Operator configuration to make it more efficient and flexible
* Allow multiple imagePullSecrets in the Strimzi Helm chart
* Remove support for JMX Trans
* Move feature gate `UseStrimziPodSets` to GA and remove support for StatefulSets
* Add flag to load Grafana dashboards from Helm Chart

### Changes, deprecations and removals

* Strimzi 0.35.0 (and any possible patch releases) is the last Strimzi version with support for Kubernetes 1.19 and 1.20.
  From Strimzi 0.36.0 on, we will support only Kubernetes 1.21 and newer.
* Support for JMX Trans has been removed in Strimzi 0.35.0.
  If you have JMX Trans enabled in your `Kafka` custom resource in the `.spec.jmxTrans` section, you should remove it.
  If you upgrade to Strimzi 0.35.0 or newer with JMX Trans deployed / enabled in the `Kafka` custom resource, Strimzi will be automatically deleted after the upgrade.
* The feature gate `UseStrimziPodSets` has graduated to GA and cannot be disabled anymore.
  The StatefulSet template properties in the `Kafka` custom resource in `.spec.zookeeper.template.statefulSet` and `.spec.kafka.template.statefulSet` are deprecated and will be ignored.
  You should remove them from your custom resources.

## 0.34.0

* Add support for Kafka 3.4.0 and remove support for Kafka 3.2.x
* Stable Pod identities for Kafka Connect and MirrorMaker 2 (Feature Gate `StableConnectIdentities`)
* Use JDK HTTP client in the Kubernetes client instead of the OkHttp client
* Add truststore configuration for HTTPS connections to OPA server
* Add image digest support in Helm chart
* Added the `httpRetries` and `httpRetryPauseMs` options to OAuth authentication configuration. They are set to `0` by default - no retries, no backoff between retries. Also added analogous `httpRetries` option in the `keycloak` authorization configuration. These features are enabled by the updated Strimzi Kafka OAuth library (0.12.0).

## 0.33.0

* Add support for Kafka 3.3.2
* Support loadBalancerClass attribute in service with type loadBalancer
* Support for automatically restarting failed Connect or Mirror Maker 2 connectors
* Redesign of Strimzi User Operator to improve its scalability
* Use Java 17 as the runtime for all containers and language level for all modules except `api`, `crd-generator`, `crd-annotations`, and `test`
* Improved FIPS (Federal Information Processing Standards) support
* Upgrade Vert.x to 4.3.5
* Moved from using the Jaeger exporter to OTLP exporter by default
* Kafka Exporter support for `Recreate` deployment strategy
* `ImageStream` validation for Kafka Connect builds on OpenShift
* Support for configuring the metadata for the Role / RoleBinding of Entity Operator
* Add liveness and readiness probes specifically for nodes running in KRaft combined mode
* Upgrade HTTP bridge to latest 0.24.0 release

### Known issues

* The TLS passthrough feature of the Ingress-NGINX Controller for Kubernetes is not compatible with some new TLS features supported by Java 17 such as the _session tickets extension_.
  If you use `type: ingress` listener with enabled mTLS authentication, we recommend you to test if your clients are affected or not.
  If needed, you can also disable the _session ticket extension_ in the Kafka brokers in your `Kafka` custom resource by setting the `jdk.tls.server.enableSessionTicketExtension` Java system property to `false`:
  ```yaml
  apiVersion: kafka.strimzi.io/v1beta2
  kind: Kafka
  metadata:
    # ...
  spec:
    # ...
    kafka:
      jvmOptions:
        javaSystemProperties:
          - name: jdk.tls.server.enableSessionTicketExtension
            value: "false"
    # ...
  ```
  For more details, see [kubernetes/ingress-nginx#9540](https://github.com/kubernetes/ingress-nginx/issues/9540).

### Changes, deprecations and removals

* The `UseStrimziPodSet` feature gate will move to GA in Strimzi 0.35.
  Support for StatefulSets will be removed from Strimzi right after the 0.34 release.
  Please use the Strimzi 0.33 release to test StrimziPodSets in your environment and report any major or blocking issues before the StatefulSet support is removed.
* The default length of any new SCRAM-SHA-512 passwords will be 32 characters instead of 12 characters used in the previous Strimzi versions.
  Existing passwords will not be affected by this change until they are regenerated (for example because the user secret is deleted).
  If you want to keep using the original password length, you can set it using the `STRIMZI_SCRAM_SHA_PASSWORD_LENGTH` environment variable in `.spec.entityOperator.template.userOperatorContainer.env` in the `Kafka` custom resource or in the `Deployment` of the standalone User Operator.
  ```yaml
  userOperatorContainer:
    env:
      - name: STRIMZI_SCRAM_SHA_PASSWORD_LENGTH
        value: "12"
  ```
* In previous versions, the `ssl.secure.random.implementation` option in Kafka brokers was always set to `SHA1PRNG`.
  From Strimzi 0.33 on, it is using the default SecureRandom implementation from the Java Runtime.
  If you want to keep using `SHA1PRNG` as your SecureRandom, you can configure it in `.spec.kafka.config` in your `Kafka` custom resource.
* Support for JmxTrans in Strimzi is deprecated. 
  It is currently planned to be removed in Strimzi 0.35.0.
* Support for `type: jaeger` tracing based on Jaeger clients and OpenTracing API was deprecated in the Strimzi 0.31 release.
  As the Jaeger clients are retired and the OpenTracing project is archived, we cannot guarantee their support for future versions.
  In Strimzi 0.32 and 0.33, we added support for OpenTelemetry tracing as a replacement.
  If possible, we will maintain the support for `type: jaeger` tracing until June 2023 and remove it afterwards.
  Please migrate to OpenTelemetry as soon as possible.
* When OpenTelemetry is enabled for tracing, starting from this release, the operator configures the OTLP exporter instead of the Jaeger one by default.
  The Jaeger exporter is even not included in the Kafka images anymore, so if you want to use it you have to add the binary by yourself.
  The `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable has to be used instead of the `OTEL_EXPORTER_JAEGER_ENDPOINT` in order to specify the OTLP endpoint to send traces to.
  If you are using Jaeger as the backend system for tracing, you need to have 1.35 release at least which is the first one exposing an OTLP endpoint.

## 0.32.0

* Add support for Kafka 3.3.1 and remove support for Kafka 3.1.0, 3.1.1, and 3.1.2
* Update Open Policy Agent (OPA) Authorizer to 1.5.0
* Update KafkaConnector CR status so the 'NotReady' condition is added if the connector or any tasks are reporting a 'FAILED' state.
* Add auto-approval mechanism on KafkaRebalance resource when an optimization proposal is ready
* The `ControlPlaneListener` feature gate moves to GA
* Add client rack-awareness support to Strimzi Bridge pods
* Add support for OpenTelemetry for distributed tracing
  * Kafka Connect, Mirror Maker, Mirror Maker 2 and Strimzi Bridge can be configured to use OpenTelemetry
  * Using Jaeger exporter by default for backward compatibility
* Updated JMX Exporter dependency to 0.17.2
* ZookeeperRoller considers unready pods
* Support multiple operations per ACLRule
* Upgrade Vert.x to 4.3.4
* Add `cluster-ip` listener. We can use it with a tcp port configuration in an ingress controller to expose kafka with an optional tls encryption and a single LoadBalancer.
* Update Strimzi OAuth library to 0.11.0

### Changes, deprecations and removals

* **From 0.32.0 on, Strimzi supports only Kubernetes version 1.19 and newer.**
* A connector or task failing triggers a 'NotReady' condition to be added to the KafkaConnector CR status. This is different from previous versions where the CR would report 'Ready' even if the connector or a task had failed.
* The `ClusterRole` from file `020-ClusterRole-strimzi-cluster-operator-role.yaml` was split into two separate roles:
  * The original `strimzi-cluster-operator-namespaced` `ClusterRole` in the file `020-ClusterRole-strimzi-cluster-operator-role.yaml` contains the rights related to the resources created based on some Strimzi custom resources.
  * The new `strimzi-cluster-operator-watched` `ClusterRole` in the file `023-ClusterRole-strimzi-cluster-operator-role.yaml` contains the rights required to watch and manage the Strimzi custom resources.
  
  When deploying the Strimzi Cluster Operator as cluster-wide, the `strimzi-cluster-operator-watched` `ClusterRole` needs to be always granted at the cluster level.
  But the `strimzi-cluster-operator-namespaced` `ClusterRole` might be granted only for the namespaces where any custom resources are created.
* The `ControlPlaneListener` feature gate moves to GA. 
  Direct upgrade from Strimzi 0.22 or earlier is not possible anymore.
  You have to upgrade first to one of the Strimzi versions between 0.22 and 0.32 before upgrading to Strimzi 0.32 or newer.
  Please follow the docs for more details.  
* The `spec.authorization.acls[*].operation` field in the `KafkaUser` resource has been deprecated in favour of the field
  `spec.authorization.acls[*].operations` which allows to set multiple operations per ACLRule.

## 0.31.1

* Kafka 3.1.2 and 3.2.3 (fixes CVE-2022-34917)
* Make `sasl.server.max.receive.size` broker option user configurable
* Documentation improvements
* Configuring number of operator replicas through the Strimzi Helm Chart
* Update Strimzi Kafka Bridge to 0.22.1

## 0.31.0

* Add support for Kafka 3.2.1
* Update Kaniko builder to 1.9.0 and Maven builder to 1.14
* Update Kafka Exporter to 1.6.0 
* Pluggable Pod Security Profiles with built-in support for _restricted_ Kubernetes Security Profile
* Add support for leader election and running multiple operator replicas (1 active leader replicas and one or more stand-by replicas)
* Update Strimzi Kafka Bridge to 0.22.0
* Add support for IPv6 addresses being used in Strimzi issued certificates
* Make it easier to wait for custom resource readiness when using the Strimzi api module
* Add StrimziPodSet reconciliation metrics

### Deprecations and removals

* Strimzi 0.31.0 (and any possible patch releases) is the last Strimzi version with support for Kubernetes 1.16, 1.17 and 1.18.
  From Strimzi 0.32.0 on, we will support only Kubernetes 1.19 and newer.
  The supported Kubernetes versions will be re-evaluated again in Q1/2023.
* The `type: jaeger` tracing support based on Jaeger clients and OpenTracing API is now deprecated.
  Because the Jaeger clients are retired and the OpenTracing project is archived, we cannot guarantee their support for future Kafka versions.
  In the future, we plan to replace it with a new tracing feature based on the OpenTelemetry project.

## 0.30.0

* Remove Kafka 3.0.0 and 3.0.1
* Add support for `simple` authorization and for the User Operator to the experimental `UseKRaft` feature gate
  _(Note: Due to [KAFKA-13909](https://issues.apache.org/jira/browse/KAFKA-13909), broker restarts currently don't work when authorization is enabled.)_
* Add network capacity overrides for Cruise Control capacity config
* The `ServiceAccountPatching` feature gate moves to GA.
  It cannot be disabled anymore and will be permanently enabled.
* The `UseStrimziPodSets` feature gate moves to beta stage.
  By default, StrimziPodSets are used instead of StatefulSets.
  If needed, `UseStrimziPodSets` can be disabled in the feature gates configuration in the Cluster Operator.
* Use better encryption and digest algorithms when creating the PKCS12 stores.
  For existing clusters, the certificates will not be updated during upgrade but only next time the PKCS12 store is created. 
* Add CPU capacity overrides for Cruise Control capacity config
* Use CustomResource existing spec and status to fix Quarkus native build's serialization
* Update JMX Exporter to version 0.17.0
* Operator emits Kubernetes Events to explain why it restarted a Kafka broker
* Better configurability of the Kafka Admin client in the User Operator
* Update Strimzi Kafka Bridge to 0.21.6

## 0.29.0

* Add support for Apache Kafka 3.0.1, 3.1.1 and 3.2.0
* Increase the size of the `/tmp` volumes to 5Mi to allow unpacking of compression libraries
* Use `/healthz` endpoint for Kafka Exporter health checks
* Renew user certificates in User Operator only during maintenance windows
* Ensure Topic Operator using Kafka Streams state store can start up successfully 
* Update Cruise Control to 2.5.89
* Remove TLS sidecar from Cruise Control pod. Cruise Control is now configured to not using ZooKeeper, so the TLS sidecar is not needed anymore.
* Allow Cruise Control topic names to be configured
* Add support for `spec.rack.topologyKey` property in Mirror Maker 2 to enable "fetch from the closest replica" feature.
* Support for the s390x platform
  _(The s390x support is currently considered as experimental. We are not aware of any issues, but the s390x build doesn't at this point undergo the same level of testing as the AMD64 container images.)_
* Update Strimzi Kafka Bridge to 0.21.5
* Added rebalancing modes on the `KafkaRebalance` custom resource
  * `full`: this mode runs a full rebalance moving replicas across all the brokers in the cluster. This is the default one if not specified.
  * `add-brokers`: after scaling up the cluster, this mode is used to move replicas to the newly added brokers specified in the custom resource.
  * `remove-brokers`: this mode is used to move replicas off the brokers that are going to be removed, before scaling down the cluster.
* **Experimental** KRaft mode (ZooKeeper-less Kafka) which can be enabled using the `UseKRaft` feature gate.
  **Important: Use it for development and testing only!**

### Changes, deprecations and removals

* Since the Cruise Control TLS sidecar has been removed, the related configuration options `.spec.cruiseControl.tlsSidecar` and `.spec.cruiseControl.template.tlsSidecar` in the Kafka custom resource are now deprecated.

## 0.28.0

* Add support for Kafka 3.1.0; remove Kafka 2.8.0 and 2.8.1
* Add support for `StrimziPodSet` resources (disabled by default through the `UseStrimziPodSets` feature gate)
* Update Open Policy Agent authorizer to 1.4.0 and add support for enabling metrics
* Support custom authentication mechanisms in Kafka listeners
* Intra-broker disk balancing using Cruise Control
* Add connector context to the default logging configuration in Kafka Connect and Kafka Mirror Maker 2
* Added the option `createBootstrapService` in the Kafka Spec to disable the creation of the bootstrap service for the Load Balancer Type Listener. It will save the cost of one load balancer resource, specially in the public cloud.
* Added the `connectTimeoutSeconds` and `readTimeoutSeconds` options to OAuth authentication configuration. The default connect and read timeouts are set to 60 seconds (previously there was no timeout). Also added `groupsClaim` and `groupsClaimDelimiter` options in the listener configuration of Kafka Spec to allow extracting group information from JWT token at authentication time, and making it available to the custom authorizer. These features are enabled by the updated Strimzi Kafka OAuth library (0.10.0).
* Add support for disabling the FIPS mode in OpenJDK
* Fix renewing your own CA certificates [#5466](https://github.com/strimzi/strimzi-kafka-operator/issues/5466)
* Update Strimzi Kafka Bridge to 0.21.4
* Update Cruise Control to 2.5.82

### Changes, deprecations and removals

* The Strimzi Identity Replication Policy (class `io.strimzi.kafka.connect.mirror.IdentityReplicationPolicy`) is now deprecated and will be removed in the future.
  Please update to Kafka's own Identity Replication Policy (class `org.apache.kafka.connect.mirror.IdentityReplicationPolicy`).
* The `type` field in `ListenerStatus` has been deprecated and will be removed in the future.
* The `disk` and `cpuUtilization` fields in the `spec.cruiseControl.capacity` section of the Kafka resource have been deprecated, are ignored, and will be removed in the future.

## 0.27.0

* Multi-arch container images with support for x86_64 / AMD64 and AArch64 / ARM64 platforms
  _(The support AArch64 is currently considered as experimental. We are not aware of any issues, but the AArch64 build doesn't at this point undergo the same level of testing as the AMD64 container images.)_
* Added the option to configure the Cluster Operator's Zookeeper admin client session timeout via an new env var: `STRIMZI_ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS`
* The `ControlPlaneListener` and `ServiceAccountPatching` feature gates are now in the _beta_ phase and are enabled by default.
* Allow setting any extra environment variables for the Cluster Operator container through Helm using a new `extraEnvs` value.
* Added SCRAM-SHA-256 authentication for Kafka clients
* Update OPA Authorizer to 1.3.0
* Update to Cruise Control version 2.5.79
* Update Log4j2 to 2.17.0

### Changes, deprecations and removals

* The `ControlPlaneListener` feature gate is now enabled by default.
  When upgrading from Strimzi 0.22 or earlier, you have to disable the `ControlPlaneListener` feature gate when upgrading the cluster operator to make sure the Kafka cluster stays available during the upgrade.
  When downgrading to Strimzi 0.22 or earlier, you have to disable the `ControlPlaneListener` feature gate before downgrading the cluster operator to make sure the Kafka cluster stays available during the downgrade.

## 0.26.0

* Add support for Kafka 2.8.1 and 3.0.0; remove Kafka 2.7.0 and 2.7.1
* Update the Open Policy Agent Authorizer to version [1.1.0](https://github.com/Bisnode/opa-kafka-plugin/releases/tag/v1.1.0)
* Expose JMX port on Zookeeper nodes via a headless service.
* Allow configuring labels and annotations for JMX authentication secrets
* Enable Cruise Control anomaly.detection configurations
* Add support for building connector images from the Maven coordinates
* Allow Kafka Connect Build artifacts to be downloaded from insecure servers (#5542)
* Add option to specify pull secret in Kafka Connect Build on OpenShift (#5631)
* Configurable authentication, authorization, and SSL for Cruise Control API
* Update to Cruise Control version 2.5.73
* Allow to configure `/tmp` volume size via Pod template. By default `1Mi` is used.

### Changes, deprecations and removals

* imageRepositoryOverride,imageRegistryOverride and imageTagOverride are now removed from values.yaml. defaultImageRepository, defaultImageRegistry and defaultImageTag values are introduced in helm charts which sets the default registry, repository and tags for the images. Now the registry, repository and tag for a single image can be configured as per the requirement.
* The OpenShift Templates were removed from the examples and are no longer supported (#5548)
* Kafka MirrorMaker 1 has been deprecated in Apache Kafka 3.0.0 and will be removed in Apache Kafka 4.0.0.
  As a result, the `KafkaMirrorMaker` custom resource which is used to deploy Kafka MirrorMaker 1 has been deprecated in Strimzi as well. (#5617)
  The `KafkaMirrorMaker` resource will be removed from Strimzi when we adopt Apache Kafka 4.0.0.
  As a replacement, use the `KafkaMirrorMaker2` custom resource with the [`IdentityReplicationPolicy`](https://strimzi.io/docs/operators/latest/using.html#unidirectional_replication_activepassive).

## 0.25.0

* Move from Scala 2.12 to Scala 2.13. (#5192)
* Open Policy Agent authorizer updated to a new version supporting Scala 2.13. See the _Changes, deprecations and removals_ sections for more details. (#5192)
* Allow a custom password to be set for SCRAM-SHA-512 users by referencing a secret in the `KafkaUser` resource
* Add support for [EnvVar Configuration Provider for Apache Kafka](https://github.com/strimzi/kafka-env-var-config-provider)
* Add support for `tls-external` authentication to User Operator to allow management of ACLs and Quotas for TLS users with user certificates generated externally (#5249) 
* Support for disabling the automatic generation of network policies by the Cluster Operator. Set the Cluster Operator's `STRIMZI_NETWORK_POLICY_GENERATION` environment variable to `false` to disable network policies. (#5258)
* Update User Operator to use Admin API for managing SCRAM-SHA-512 users
* Configure fixed size limit for `emptyDir` volumes used for temporary files (#5340)
* Update Strimzi Kafka Bridge to 0.20.2

### Changes, deprecations and removals

* The `KafkaConnectS2I` resource has been removed and is no longer supported by the operator.
  Please use the [migration guide](https://strimzi.io/docs/operators/0.24.0/full/using.html#proc-migrating-kafka-connect-s2i-str) to migrate your `KafkaConnectS2I` deployments to [`KafkaConnect` Build](https://strimzi.io/docs/operators/latest/full/deploying.html#creating-new-image-using-kafka-connect-build-str) instead.
* The Open Policy Agent authorizer has been updated to a new version that supports Scala 2.13.
  The new release introduces a new format of the input data sent to the Open Policy Agent server.
  For more information about the new format and how to migrate from the old version, see the [OPA Kafka plugin v1.0.0 release notes](https://github.com/Bisnode/opa-kafka-plugin/releases/tag/v1.0.0).
* User Operator now uses Kafka Admin API to manage SCRAM-SHA-512 credentials.
  All operations done by the User Operator now use Kafka Admin API and connect directly to Kafka instead of ZooKeeper.
  As a result, the environment variables `STRIMZI_ZOOKEEPER_CONNECT` and `STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS` were removed from the User Operator configuration.
* All `emptyDir` volumes used by Strimzi for temporary files have now configured a fixed size limit.
* Annotate Cluster Operator resource metrics with a namespace label

## 0.24.0

* Add support for [Kubernetes Configuration Provider for Apache Kafka](https://github.com/strimzi/kafka-kubernetes-config-provider)
* Use Red Hat UBI8 base image
* Add support for Kafka 2.7.1 and remove support for 2.6.0, 2.6.1, and 2.6.2
* Support for patching of service accounts and configuring their labels and annotations. The feature is disabled by default and enabled using the new `ServiceAccountPatching` feature gate.
* Added support for configuring cluster-operator's worker thread pool size that is used for various sync and async tasks
* Add Kafka Quotas plugin with produce, consume, and storage quotas
* Support pausing reconciliation of KafkaTopic CR with annotation `strimzi.io/pause-reconciliation`
* Update cruise control to 2.5.57
* Update to Strimzi Kafka Bridge to 0.20.0
* Support for broker load information added to the rebalance optimization proposal. Information on the load difference, before and after a rebalance is stored in a ConfigMap
* Add support for selectively changing the verbosity of logging for individual CRs, using markers.
* Added support for `controller_mutation_rate' quota. Creation/Deletion of topics and creation of partitions can be configured through this.
* Use newer version of Kafka Exporter with different bugfixes 

### Changes, deprecations and removals

* The deprecated `KafkaConnectS2I` custom resource will be removed after the 0.24.0 release. 
  Please use the [migration guide](https://strimzi.io/docs/operators/latest/full/using.html#proc-migrating-kafka-connect-s2i-str) to migrate your `KafkaConnectS2I` deployments to [`KafkaConnect` Build](https://strimzi.io/docs/operators/latest/full/deploying.html#creating-new-image-using-kafka-connect-build-str) instead.
* The fields `topicsBlacklistPattern` and `groupsBlacklistPattern` in the `KafkaMirrorMaker2` resource are deprecated and will be removed in the future.
  They are replaced by new fields `topicsExcludePattern` and `groupsExcludePattern`.
* The field `whitelist` in the `KafkaMirrorMaker` resource is deprecated and will be removed in the future.
  It is replaced with a new field `include`.
* `bind-utils` removed from containers to improve security posture.
* Kafka Connect Build now uses hashes to name downloaded artifact files. Previously, it was using the last segment of the download URL.
  If your artifact requires a specific name, you can use the new `type: other` artifact and its `fileName` field.
* The option `enableECDSA` of Kafka CR `authentication` of type `oauth` has been deprecated and is ignored. 
  ECDSA token signature support is now always enabled without the need for Strimzi Cluster Operator installing the BouncyCastle JCE crypto provider. 
  BouncyCastle library is no longer packaged with Strimzi Kafka images.

## 0.23.0

* Add support for Kafka 2.8.0 and 2.6.2, remove support for Kafka 2.5.x
* Make it possible to configure maximum number of connections and maximum connection creation rate in listener configuration
* Add support for configuring finalizers for `loadbalancer` type listeners
* Use dedicated Service Account for Kafka Connect Build on Kubernetes 
* Remove direct ZooKeeper access for handling user quotas in the User Operator. Add usage of Admin Client API instead.
* Migrate to CRD v1 (required by Kubernetes 1.22+)
* Support for configuring custom Authorizer implementation 
* Changed Reconciliation interval for Topic Operator from 90 to 120 seconds (to keep it the same as for other operators)
* Changed Zookeeper session timeout default value to 18 seconds for Topic and User Operators (for improved resiliency)
* Removed requirement for replicas and partitions KafkaTopic spec making these parameters optional
* Support to configure a custom filter for parent CR's labels propagation into subresources 
* Allow disabling service links (environment variables describing Kubernetes services) in Pod template
* Update Kaniko executor to 1.6.0
* Add support for separate control plane listener (disabled by default, available via the `ControlPlaneListener` feature gate)
* Support for Dual Stack networking

### Changes, deprecations and removals

* Strimzi API versions `v1alpha1` and `v1beta1` were removed from all Strimzi custom resources apart from `KafkaTopic` and `KafkaUser` (use `v1beta2` versions instead)
* The following annotations have been removed and cannot be used anymore:
  * `cluster.operator.strimzi.io/delete-claim` (used internally only - replaced by `strimzi.io/delete-claim`)
  * `operator.strimzi.io/generation` (used internally only - replaced by `strimzi.io/generation`)
  * `operator.strimzi.io/delete-pod-and-pvc` (use `strimzi.io/delete-pod-and-pvc` instead)
  * `operator.strimzi.io/manual-rolling-update` (use `strimzi.io/manual-rolling-update` instead)
* When the `class` field is configured in the `configuration` section of an Ingress-type listener, Strimzi will not automatically set the deprecated `kubernetes.io/ingress.class` annotation anymore. In case you still need this annotation, you can set it manually in the listener configuration using the [`annotations` field](https://strimzi.io/docs/operators/latest/full/using.html#property-listener-config-annotations-reference) or in the [`.spec.kafka.template` section](https://strimzi.io/docs/operators/latest/full/using.html#type-KafkaClusterTemplate-reference).
* The `.spec.kafkaExporter.template.service` section in the `Kafka` custom resource has been deprecated and will be removed in the next API version (the service itself was removed several releases ago).

## 0.22.0

* Add `v1beta2` version for all resources. `v1beta2` removes all deprecated fields.
* Add annotations that enable the operator to restart Kafka Connect connectors or tasks. The annotations can be applied to the KafkaConnector and the KafkaMirrorMaker2 custom resources.
* Add additional configuration options for the Kaniko executor used by the Kafka Connect Build on Kubernetes
* Add support for JMX options configuration of all Kafka Connect (KC, KC2SI, MM2)
* Update Strimzi Kafka OAuth to version 0.7 and add support for new features:
  * OAuth authentication over SASL PLAIN mechanism
  * Checking token audience
  * Validating tokens using JSONPath filter queries to perform custom checks
* Fix Cruise Control crash loop when updating container configurations
* Configure external logging `ConfigMap` name and key.
* Add support for configuring labels and annotations in ClusterRoleBindings created as part of Kafka and Kafka Connect clusters
* Add support for Ingress v1 in Kubernetes 1.19 and newer
* Add support for Kafka 2.6.1
* List topics used by a Kafka Connect connector in the `.status` section of the `KafkaConnector` custom resource
* Bump Cruise Control to v2.5.37 for Kafka 2.7 support. Note this new version of Cruise Control uses `Log4j 2` and is supported by dynamic logging configuration (where logging properties are defined in a ConfigMap). However, existing `Log4j` configurations must be updated to `Log4j 2` configurations.
* Support pausing reconciliation of CR with annotation `strimzi.io/pause-reconciliation`

### Changes, deprecations and removals

* In the past, when no Ingress class was specified in the Ingress-type listener in the Kafka custom resource, the 
  `kubernetes.io/ingress.class` annotation was automatically set to `nginx`. Because of the support for the new 
  IngressClass resource and the new `ingressClassName` field in the Ingress resource, the default value will not be set 
  anymore. Please use the `class` field in `.spec.kafka.listeners[].configuration` to specify the class name.
* The `KafkaConnectS2I` custom resource is deprecated and will be removed in the future. You can use the new [`KafkaConnect` build feature](https://strimzi.io/docs/operators/latest/full/deploying.html#creating-new-image-using-kafka-connect-build-str) instead.
* Removed support for Helm2 charts as that version is now unsupported. There is no longer the need for separate `helm2` and `helm3` binaries, only `helm` (version 3) is required.
* The following annotations are deprecated for a long time and will be removed in 0.23.0:
  * `cluster.operator.strimzi.io/delete-claim` (used internally only - replaced by `strimzi.io/delete-claim`)
  * `operator.strimzi.io/generation` (used internally only - replaced by `strimzi.io/generation`)
  * `operator.strimzi.io/delete-pod-and-pvc` (use `strimzi.io/delete-pod-and-pvc` instead)
  * `operator.strimzi.io/manual-rolling-update` (use `strimzi.io/manual-rolling-update` instead)
* External logging configuration has changed. `spec.logging.name` is deprecated. Moved to `spec.logging.valueFrom.configMapKeyRef.name`. Key in the `ConfigMap` is configurable via `spec.logging.valueFrom.configMapKeyRef.key`.
  * from
  ```
  logging:
    type: external
    name: my-config-map
  ```
  * to
  ```
  logging:
    type: external
    valueFrom:
      configMapKeyRef:
        name: my-config-map
        key: my-key
  ``` 
* Existing Cruise Control logging configurations must be updated from `Log4j` syntax to `Log4j 2` syntax.
  * For existing inline configurations, replace the `cruisecontrol.root.logger` property with `rootLogger.level`.
  * For existing external configurations, replace the existing configuration with a new configuration file named `log4j2.properties` using `log4j 2` syntax.

## 0.21.0

* Add support for declarative management of connector plugins in Kafka Connect CR 
* Add `inter.broker.protocol.version` to the default configuration in example YAMLs
* Add support for `secretPrefix` property for User Operator to prefix all secret names created from KafkaUser resource.
* Allow configuring labels and annotations for Cluster CA certificate secrets
* Add the JAAS configuration string in the sasl.jaas.config property to the generated secrets for KafkaUser with SCRAM-SHA-512 authentication.
* Strimzi `test-container` has been renamed to `strimzi-test-container` to make the name more clear
* Updated the CPU usage metric in the Kafka, ZooKeeper and Cruise Control dashboards to include the CPU kernel time (other than the current user time)
* Allow disabling ownerReference on CA secrets
* Make it possible to run Strimzi operators and operands with read-only root filesystem
* Move from Docker Hub to Quay.io as our container registry
* Add possibility to configure DeploymentStrategy for Kafka Connect, Kafka Mirror Maker (1 and 2), and Kafka Bridge
* Support passing metrics configuration as an external ConfigMap
* Enable CORS configuration for Cruise Control
* Add support for rolling individual Kafka or ZooKeeper pods through the Cluster Operator using an annotation
* Add support for Topology Spread Constraints in Pod templates
* Make Kafka `cluster-id` (KIP-78) available on Kafka CRD status
* Add support for Kafka 2.7.0

### Deprecations and removals
* The `metrics` field in the Strimzi custom resources has been deprecated and will be removed in the future. For configuring metrics, use the new `metricsConfig` field and pass the configuration via ConfigMap.

## 0.20.0

**Note: This is the last version of Strimzi that will support Kubernetes 1.11 and higher. Future versions will drop support for Kubernetes 1.11-1.15 and support only Kubernetes 1.16 and higher.**

* Add support for Kafka 2.5.1 and 2.6.0. Remove support for 2.4.0 and 2.4.1
* Remove TLS sidecars from Kafka pods => Kafka now uses native TLS to connect to ZooKeeper
* Updated to Cruise Control 2.5.11, which adds Kafka 2.6.0 support and fixes a previous issue with CPU utilization statistics for containers. As a result, the CpuCapacityGoal has now been enabled.
* Cruise Control metrics integration:
  * Enable metrics JMX exporter configuration in the `cruiseControl` property of the Kafka custom resource
  * New Grafana dashboard for the Cruise Control metrics
* Configure Cluster Operator logging using ConfigMap instead of environment variable and support dynamic changes  
* Switch to use the `AclAuthorizer` class for the `simple` Kafka authorization type. `AclAuthorizer` contains new features such as the ability to control the amount of authorization logs in the broker logs.
* Support dynamically changeable logging configuration of Kafka Connect and Kafka Connect S2I
* Support dynamically changeable logging configuration of Kafka brokers
* Support dynamically changeable logging configuration of Kafka MirrorMaker2
* Add support for `client.rack` property for Kafka Connect to use `fetch from closest replica` feature. 
* Refactored operators Grafana dashboard
  * Fixed bug on maximum reconcile time graph
  * Removed the avarage reconcile time graph
  * Rearranged graphs
* Make `listeners` configurable as an array and add support for more different listeners in single cluster
* Add support for configuring `hostAliases` in Pod templates
* Add new resource state metric in the operators for reflecting the reconcile result on a specific resource
* Add improvements for `oauth` authentication, and `keycloak` authorization:
  * Support for re-authentication was added, which also enforces access token lifespan on the Kafka client session
  * Permission changes through Keycloak Authorization Services are now detected by Kafka Brokers

### Deprecations and removals

#### Redesign of the `.spec.kafka.listeners` section

The `.spec.kafka.listeners` section of the Kafka CRD has been redesigned to allow configuring more different listeners.
The old `listeners` object which allowed only configuration of one`plain`, one `tls`, and one `external` listener is now deprecated and will be removed in the future.
It is replaced with an array allowing configuration of multiple different listeners:

```yaml
listeners:
  - name: local
    port: 9092
    type: internal
    tls: true
  - name: external1
    port: 9093
    type: loadbalancer
    tls: true
  - name: external2
    port: 9094
    type: nodeport
    tls: true
```

This change includes some other changes:
* The `tls` field is now required.
* The former `overrides` section is now merged with the `configuration` section.
* The `dnsAnnotations` field has been renamed to `annotations` since we found out it has wider use.
* Configuration of `loadBalancerSourceRanges` and `externalTrafficPolicy` has been moved into listener configuration. Its use in the `template` section is now deprecated.
* For `type: internal` listeners, you can now use the flag `useServiceDnsDomain` to define whether they should use the fully qualified DNS names including the cluster service suffix (usually `.cluster.local`). This option defaults to false.
* All listeners now support configuring the advertised hostname and port.
* `preferredAddressType` has been removed to `preferredNodePortAddressType`.

To convert the old format into the new format with backwards compatibility, you should use following names and types:
* For the old `plain` listener, use the name `plain`, port `9092` and type `internal`.
* For the old `tls` listener, use the name `tls`, port `9093` and type `internal`.
* For the old `external` listener, use the name `external`, port `9094`.

For example the following old configuration:

```yaml
listeners:
  plain:
    # ...
  tls: 
    # ...
  external:
    type: loadbalancer 
    # ...
```

Will look like this in the new format:

```yaml
listeners:
  - name: plain
    port: 9092
    type: internal
    tls: false
  - name: tls
    port: 9093
    type: internal
    tls: true
  - name: external
    port: 9094
    type: loadbalancer
    tls: true
```

#### Removal of monitoring port on Kafka and ZooKeeper related services

The `PodMonitor` resource is now used instead of the `ServiceMonitor` for scraping metrics from Kafka, ZooKeeper, Kafka Connect and so on.
For this reason, we have removed the monitoring port `tcp-prometheus` (9404) on all the services where it is declared (Kafka bootstrap, ZooKeeper client and so on).
It was already deprecated in the previous 0.19.0 release.
Together with it we have also removed the Prometheus annotations from the services. If you want to add them, you can use the templates.
See here https://strimzi.io/docs/operators/master/using.html#assembly-customizing-kubernetes-resources-str for more details about templates usage.
Finally, the Kafka Exporter service was has been removed because it was used just for the monitoring port.

#### Deprecation of Kafka TLS sidecar configuration

Since the Kafka TLS sidecar has been removed, the related configuration options in the Kafka custom resource are now deprecated:
* `.spec.kafka.tlsSidecar`
* `.spec.kafka.template.tlsSidecar`

## 0.19.0

* Add support for authorization using Open Policy Agent
* Add support for scale subresource to make scaling of following resources easier:
  * KafkaConnect
  * KafkaConnectS2I
  * KafkaBridge
  * KafkaMirrorMaker
  * KafkaMirrorMaker2
  * KafkaConnector 
* Remove deprecated `Kafka.spec.topicOperator` classes and deployment logic
* Use Java 11 as the Java runtime
* Removed the need to manually create Cruise Control metrics topics if topic auto creation is disabled.
* Migration to Helm 3
* Refactored the format of the `KafkaRebalance` resource's status. The state of the rebalance is now displayed in the associated `Condition`'s `type` field rather than the `status` field. This was done so that the information would display correctly in various Kubernetes tools.
* Added performance tuning options to the `KafkaRebalance` CR and the ability to define a regular expression that will exclude matching topics from a rebalance optimization proposal.
* Use Strimzi Kafka Bridge 0.18.0
* Make it possible to configure labels and annotations for secrets created by the User Operator
* Strimzi Kafka Bridge metrics integration:
  * enable/disable metrics in the KafkaBridge custom resource
  * new Grafana dashboard for the bridge metrics
* Support dynamically changeable logging in the Entity Operator and Kafka Bridge 
* Extended the Grafana example dashboard for Kafka Connect to provide more relevant information

### Deprecations and removals

#### Deprecation of Helm v2 chart

The Helm v2 support will end soon. 
Bug fixing should stop on August 13th 2020 and security fixes on November 13th.
See https://helm.sh/blog/covid-19-extending-helm-v2-bug-fixes/ for more details.

In sync with that, the Helm v2 chart of Strimzi Cluster Operator is now deprecated and will be removed in the future as Helm v2 support ends.
Since Strimzi 0.19.0, we have a new chart for Helm v3 which can be used instead.

#### Removal of v1alpha1 versions of several custom resources

In Strimzi 0.12.0, the `v1alpha1` versions of the following resources have been deprecated and replaced by `v1beta1`:
* `Kafka`
* `KafkaConnect`
* `KafkaConnectS2I`
* `KafkaMirrorMaker`
* `KafkaTopic`
* `KafkaUser`

In the next release, the `v1alpha1` versions of these resources will be removed. 
Please follow the guide for upgrading the resources: https://strimzi.io/docs/operators/latest/full/deploying.html#assembly-upgrade-resources-str.

#### Removal deprecated cadvisor metric labels

The `pod_name` and `container_name` labels provided on the cadvisor metrics are now just `pod` and `container` starting from Kubernetes 1.16.
We removed the old ones from the Prometheus scraping configuration/alerts and on the Kafka and ZooKeeper dashboard as well.
It means that the charts related to memory and CPU usage are not going to work on Kuvbernetes version previous 1.14.
For more information on what is changed: https://github.com/strimzi/strimzi-kafka-operator/pull/3312

#### Deprecation of monitoring port on Kafka and ZooKeeper related services

The `PodMonitor` resource is now used instead of the `ServiceMonitor` for scraping metrics from Kafka, ZooKeeper, Kafka Connect and so on.
For this reason, we are deprecating the monitoring port `tcp-prometheus` (9404) on all the services where it is declared (Kafka bootstrap, ZooKeeper client and so on).
This port will be removed in the next release.
Together with it we will also remove the Prometheus annotation from the service.

#### Removal warning of Cluster Operator log level

Because of the new Cluster Operator dynamic logging configuration via [PR#3328](https://github.com/strimzi/strimzi-kafka-operator/pull/3328) we are going to remove the `STRIMZI_LOG_LEVEL` environment variable from the Cluster Operator deployment YAML file in the 0.20.0 release.

## 0.18.0

* Add possibility to set Java System Properties for User Operator and Topic Operator via `Kafka` CR.
* Make it possible to configure PodManagementPolicy for StatefulSets
* Update build system to use `yq` version 3 (https://github.com/mikefarah/yq)
* Add more metrics to Cluster and User Operators
* New Grafana dashboard for Operator monitoring 
* Allow `ssl.cipher.suites`, `ssl.protocol` and `ssl.enabled.protocols` to be configurable for Kafka and the different components supported by Strimzi
* Add support for user configurable SecurityContext for each Strimzi container
* Allow standalone User Operator to modify status on KafkaUser
* Add support for Kafka 2.4.1
* Add support for Kafka 2.5.0
* Remove TLS sidecars from ZooKeeper pods, using native ZooKeeper TLS support instead
* Add metrics for Topic Operator
* Use Strimzi Kafka Bridge 0.16.0
* Add support for CORS in the HTTP Kafka Bridge
* Pass HTTP Proxy configuration from operator to operands
* Add Cruise Control support, KafkaRebalance resource and rebalance operator

## 0.17.0

* Add possibility to set Java System Properties via CR yaml
* Add support for Mirror Maker 2.0
* Add Jmxtrans deployment
* Add public keys of TLS listeners to the status section of the Kafka CR
* Add support for using a Kafka authorizer backed by Keycloak Authorization Services

## 0.16.0

* Add support for Kafka 2.4.0 and upgrade from Zookeeper 3.4.x to 3.5.x
* Drop support for Kafka 2.2.1 and 2.3.0
* Add KafkaConnector resource and connector operator
* Let user choose which node address will be used as advertised host (`ExternalDNS`, `ExternalIP`, `InternalDNS`, `InternalIP` or `Hostname`)
* Add support for tini
* When not explicitly configured by the user in `jvmOptions`, `-Xmx` option is calculated from memory requests rather than from memory limits
* Expose JMX port on Kafka brokers via an internal service
* Add support for `externalTrafficPolicy` and `loadBalancerSourceRanges` properties on loadbalancer and nodeport type services
* Add support for user quotas
* Add support for Istio protocol selection in service port names  
Note: Strimzi is essentially adding a `tcp-` prefix to the port names in Kafka services and headless services.  
(e.g clientstls -> tcp-clientstls)
* Add service discovery labels and annotations
* Add possibility to specify custom server certificates to TLS based listeners

## 0.15.0

* Drop support for Kafka 2.1.0, 2.1.1, and 2.2.0
* Add support for Kafka 2.3.1
* Improved Kafka rolling update
* Improve Kafka Exporter Grafana dashboard
* Add sizeLimit option to ephemeral storage (#1505)
* Add `schedulerName` to `podTemplate` (#2114)
* Allow overriding the auto-detected Kubernetes version
* Garbage Collection (GC) logging disabled by default
* Providing PKCS12 truststore and password in the cluster and clients CA certificates Secrets
* Providing PKCS12 keystore and password in the TLS based KafkaUser related Secret

## 0.14.0

* Add support for configuring Ingress class (#1716)
* Add support for setting custom environment variables in all containers
* Add liveness and readiness checks to Mirror Maker
* Allow configuring loadBalancerIP for LoadBalancer type services
* Allow setting labels and annotations for Persistent Volume Claims
* Add support for Jaeger tracing in Kafka Mirror Maker and Kafka Connect
* Add support for deploying Kafka Exporter
* Add initial support for OAuth authentication

## 0.13.0

* Allow users to manually configure ACL rules (for example, using `kafka-acls.sh`) for special Kafka users `*` and `ANONYMOUS` without them being deleted by the User Operator
* Add support for configuring a Priority Class name for Pods deployed by Strimzi
* Add support for Kafka 2.3.0
* Add support for Kafka User resource status
* Add support for Kafka Connect resource status
* Add support for Kafka Connect S2I resource status
* Add support for Kafka Bridge resource status
* Add support for Kafka Mirror Maker resource status
* Add support for DNS annotations to `nodeport` type external listeners

## 0.12.0

* **Drop support for Kubernetes 1.9 and 1.10 and OpenShift 3.9 and 3.10.**
**Versions supported since Strimzi 0.12.0 are Kubernetes 1.11 and higher and OpenShift 3.11 and higher.** 
**This was required because the CRD versioning and CRD subresources support.** 
* Added support for Kafka 2.2.0 and 2.1.1, dropped support for Kafka 2.0.0 and 2.0.1
* Persistent storage improvements
  * Add resizing of persistent volumes
  * Allow to specify different storage class for every broker
  * Adding and removing volumes in Jbod Storage
* Custom Resources improvements
  * New CRD version `v1beta1`. See documentation for more information about upgrading from `v1alpha1` to `v1beta1`.
  * Log at the warn level when a custom resource uses deprecated or unknown properties
  * Add initial support for the `status` sub-resource in the `Kafka` custom resource 
* Add support for [Strimzi Kafka Bridge](https://github.com/strimzi/strimzi-kafka-bridge) for HTTP protocol
* Reduce the number of container images needed to run Strimzi to just two: `kafka` and `operator`.
* Add support for unprivileged users to install the operator with Helm
* Support experimental off-cluster access using Kubernetes Nginx Ingress
* Add ability to configure Image Pull Secrets for all pods in Cluster Operator
* Support for SASL PLAIN mechanism in Kafka Connect and Mirror Maker (for use with non-Strimzi Kafka cluster)

## 0.11.0

* Add support for JBOD storage for Kafka brokers
* Allow users to configure the default ImagePullPolicy
* Add Prometheus alerting
    * Resources for alert manager deployment and configuration
    * Alerting rules with alert examples from Kafka and Zookeeper metrics
* Enrich configuration options for off cluster access
* Support for watching all namespaces
* Operator Lifecycle Manager integration

## 0.10.0

* Support for Kafka 2.1.0
* Support for Kafka upgrades
* Add healthchecks to TLS sidecars
* Add support for new fields in the Pod template: terminationGracePeriod, securityContext and imagePullSecrets
* Rename annotations to use the `strimzi.io` domain consistently (The old annotations are deprecated, but still functional):
    * `cluster.operator.strimzi.io/delete-claim` → `strimzi.io/delete-claim` 
    * `operator.strimzi.io/manual-rolling-update` → `strimzi.io/manual-rolling-update` 
    * `operator.strimzi.io/delete-pod-and-pvc` → `strimzi.io/delete-pod-and-pvc`
    * `operator.strimzi.io/generation` → `strimzi.io/generation`
* Add support for mounting Secrets and Config Maps into Kafka Connect and Kafka Connect S2I
* Add support for NetworkPolicy peers in listener configurations
* Make sure the TLS sidecar pods shutdown only after the main container
* Add support for Pod Disruption Budgets

## 0.9.0

* Add possibility to label and annotate different resources (#1039)
* Add support for TransactionalID in KafkaUser resource
* Update to Kafka 2.0.1
* Add maintenance time windows support for allowing CA certificates renewal rolling update started only in specific times (#1117)  
* Add support for upgrading between Kafka versions (#1103). This removes support for `STRIMZI_DEFAULT_KAFKA_IMAGE` environment variable in the Cluster Operator, replacing it with `STRIMZI_KAFKA_IMAGES`.  


## 0.8.2

* Run images under group 0 to avoid storage issues

## 0.8.1

* Fix certificate renewal issues

## 0.8.0

* Support for unencrypted connections on LoadBalancers and NodePorts.
* Better support for TLS hostname verification for external connections
* Certificate renewal / expiration
* Mirror Maker operator
* Triggering rolling update / pod deletion manually

## 0.7.0

* Exposing Kafka to the outside using:
  * OpenShift Routes
  * LoadBalancers
  * NodePorts
* Use less wide RBAC permissions (`ClusterRoleBindings` where converted to `RoleBindings` where possible)
* Support for SASL authentication using the SCRAM-SHA-512 mechanism added to Kafka Connect and Kafka Connect with S2I support 
* Network policies for managing access to Zookeeper ports and Kafka replication ports
* Use OwnerReference and Kubernetes garbage collection feature to delete resources and to track the ownership

## 0.6.0

* Helm chart for Strimzi Cluster Operator
* Topic Operator moving to Custom Resources instead of Config Maps
* Make it possible to enabled and disable:
  * Listeners
  * Authorization
  * Authentication
* Configure Kafka _super users_ (`super.users` field in Kafka configuration)
* User Operator
  * Managing users and their ACL rights
* Added new Entity Operator for deploying:
  * User Operator
  * Topic Operator
* Deploying the Topic Operator outside of the new Entity Operator is now deprecated
* Kafka 2.0.0
* Kafka Connect:
  * Added TLS support for connecting to the Kafka cluster
  * Added TLS client authentication when connecting to the Kafka cluster 

## 0.5.0

* The Cluster Operator now manages RBAC resource for managed resources:
    * `ServiceAccount` and `ClusterRoleBindings` for Kafka pods
    * `ServiceAccount` and `RoleBindings` for the Topic Operator pods
* Renaming of Kubernetes services (Backwards incompatible!)
  * Kubernetes services for Kafka, Kafka Connect and Zookeeper have been renamed to better correspond to their purpose
  * `xxx-kafka` -> `xxx-kafka-bootstrap`
  * `xxx-kafka-headless` -> `xxx-kafka-brokers`
  * `xxx-zookeeper` -> `xxx-zookeeper-client`
  * `xxx-zookeeper-headless` -> `xxx-zookeeper-nodes`
  * `xxx-connect` -> `xxx-connect-api`
* Cluster Operator moving to Custom Resources instead of Config Maps
* TLS support has been added to Kafka, Zookeeper and Topic Operator. The following channels are now encrypted:
    * Zookeeper cluster communication
    * Kafka cluster commbunication
    * Communication between Kafka and Zookeeper
    * Communication between Topic Operator and Kafka / Zookeeper
* Logging configuration for Kafka, Kafka Connect and Zookeeper
* Add support for [Pod Affinity and Anti-Affinity](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity)
* Add support for [Tolerations](https://v1-9.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#toleration-v1-core)
* Configuring different JVM options
* Support for broker rack in Kafka

## 0.4.0

* Better configurability of Kafka, Kafka Connect, Zookeeper
* Support for Kubernetes request and limits
* Support for JVM memory configuration of all components
* Controllers renamed to operators
* Improved log verbosity of Cluster Operator
* Update to Kafka 1.1.0
