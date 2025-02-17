/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.imageBuild.ImageBuild;
import io.strimzi.systemtest.resources.minio.SetupMinio;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.specific.ContainerRuntimeUtils;
import io.strimzi.systemtest.utils.specific.MinioUtils;
import io.strimzi.test.TestUtils;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.Collections;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.TIERED_STORAGE;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@MicroShiftNotSupported("We are using Kaniko and OpenShift builds to build Kafka image with TS. To make it working on Microshift we will invest much time with not much additional value.")
@Tag(REGRESSION)
@Tag(TIERED_STORAGE)
@SuiteDoc(
    description = @Desc("This test suite covers scenarios for Tiered Storage integration implemented within Strimzi."),
    beforeTestSteps = {
        @Step(value = "Create test namespace.", expected = "Namespace is created."),
        @Step(value = "Build Kafka image based on passed parameters like image full name, base image, Dockerfile path (via Kaniko or OpenShift build), and include the Aiven Tiered Storage plugin from (<a href=\"https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main\">tiered-storage-for-apache-kafka</a>).", expected = "Kafka image is built with the Aiven Tiered Storage plugin integrated."),
        @Step(value = "Deploy Minio in test namespace and init the client inside the Minio pod.", expected = "Minio is deployed and client is initialized."),
        @Step(value = "Init bucket in Minio for purposes of these tests.", expected = "Bucket is initialized in Minio."),
        @Step(value = "Deploy Cluster Operator.", expected = "Cluster Operator is deployed.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class TieredStorageST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(TieredStorageST.class);

    private static final String IMAGE_NAME = "kafka-tiered-storage";
    private static final String TIERED_STORAGE_DOCKERFILE = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tiered-storage/Dockerfile";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String BUILT_IMAGE_TAG = "latest";
    private TestStorage suiteStorage;

    @ParallelTest
    @TestDoc(
        description = @Desc("This testcase is focused on testing of Tiered Storage integration implemented within Strimzi. The tests use Aiven Tiered Storage plugin (<a href=\"https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main\">tiered-storage-for-apache-kafka</a>)."),
        steps = {
            @Step(value = "Deploys KafkaNodePool resource with PV of size 10Gi.", expected = "KafkaNodePool resource is deployed successfully with specified configuration."),
            @Step(value = "Deploy Kafka CustomResource with Tiered Storage configuration pointing to Minio S3, using a built Kafka image. Reduce the `remote.log.manager.task.interval.ms` and `log.retention.check.interval.ms` to minimize delays during log uploads and deletions.", expected = "Kafka CustomResource is deployed successfully with optimized intervals to speed up log uploads and local log deletions."),
            @Step(value = "Creates topic with enabled Tiered Storage sync with size of segments set to 10mb (this is needed to speed up the sync).", expected = "Topic is created successfully with Tiered Storage enabled and segment size of 10mb."),
            @Step(value = "Starts continuous producer to send data to Kafka.", expected = "Continuous producer starts sending data to Kafka."),
            @Step(value = "Wait until Minio size is not empty (contains data from Kafka).", expected = "Minio contains data from Kafka.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testTieredStorageWithAivenPlugin() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("10Gi")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        resourceManager.createResourceWithWait(KafkaTemplates.kafka(suiteStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withImage(Environment.getImageOutputRegistry(suiteStorage.getNamespaceName(), IMAGE_NAME, BUILT_IMAGE_TAG))
                    .withNewTieredStorageCustomTiered()
                        .withNewRemoteStorageManager()
                            .withClassName("io.aiven.kafka.tieredstorage.RemoteStorageManager")
                            .withClassPath("/opt/kafka/plugins/tiered-storage/*")
                            .addToConfig("storage.backend.class", "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
                            .addToConfig("chunk.size", "4194304")
                            // s3 config
                            .addToConfig("storage.s3.endpoint.url",
                                    "http://" + SetupMinio.MINIO + "." + suiteStorage.getNamespaceName() + ".svc.cluster.local:" + SetupMinio.MINIO_PORT)
                            .addToConfig("storage.s3.bucket.name", BUCKET_NAME)
                            .addToConfig("storage.s3.region", "us-east-1")
                            .addToConfig("storage.s3.path.style.access.enabled", "true")
                            .addToConfig("storage.aws.access.key.id", SetupMinio.ADMIN_CREDS)
                            .addToConfig("storage.aws.secret.access.key", SetupMinio.ADMIN_CREDS)
                        .endRemoteStorageManager()
                    .endTieredStorageCustomTiered()
                    // reduce the interval to speed up the test
                    .addToConfig("remote.log.manager.task.interval.ms", 5000)
                    .addToConfig("log.retention.check.interval.ms", 5000)
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(suiteStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName())
            .editSpec()
                .addToConfig("file.delete.delay.ms", 1000)
                .addToConfig("local.retention.ms", 1000)
                // Allow tiered storage sync
                .addToConfig("remote.storage.enable", true)
                // Bytes retention set to 1024mb
                .addToConfig("retention.bytes", 1073741824)
                .addToConfig("retention.ms", 86400000)
                // Segment size is set to 10mb to make it quickier to sync data to Minio
                .addToConfig("segment.bytes", 1048576)
            .endSpec()
            .build());

        final KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage)
            .withMessageCount(10000)
            .withDelayMs(1)
            .withMessage(String.join("", Collections.nCopies(5000, "#")))
            .build();

        resourceManager.createResourceWithWait(clients.producerStrimzi());

        MinioUtils.waitForDataInMinio(suiteStorage.getNamespaceName(), BUCKET_NAME);

        // Create admin-client to check offsets
        resourceManager.createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(testStorage.getClusterName())
            ).build()
        );
        final AdminClient adminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());

        TestUtils.waitFor("earliest-local offset to be higher than 0",
            TestConstants.GLOBAL_POLL_INTERVAL_5_SECS, TestConstants.GLOBAL_TIMEOUT_LONG,
            () -> {
                // Fetch earliest-local offsets
                // Check that data are not present locally, earliest-local offset should be higher than 0
                String offsetData = adminClient.fetchOffsets(testStorage.getTopicName(), String.valueOf(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP));
                long earliestLocalOffset = 0;
                try {
                    earliestLocalOffset = AdminClientUtils.getPartitionsOffset(offsetData, "0");
                    LOGGER.info("earliest-local offset for topic {} is {}", testStorage.getTopicName(), earliestLocalOffset);
                } catch (JsonProcessingException e) {
                    return false;
                }
                return earliestLocalOffset > 0;
            });

        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Delete data
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().getConfig().put("retention.ms", 10000)
        );

        MinioUtils.waitForNoDataInMinio(suiteStorage.getNamespaceName(), BUCKET_NAME);
    }

    @BeforeAll
    void setup() throws IOException {
        // we skip test case for kind + podman
        assumeFalse(cluster.isKind() && ContainerRuntimeUtils.getRuntime().equals(TestConstants.PODMAN));

        suiteStorage = new TestStorage(ResourceManager.getTestContext());
        
        NamespaceManager.getInstance().createNamespaceAndPrepare(suiteStorage.getNamespaceName());
        cluster.setNamespace(suiteStorage.getNamespaceName());

        ImageBuild.buildImage(suiteStorage.getNamespaceName(), IMAGE_NAME, TIERED_STORAGE_DOCKERFILE, BUILT_IMAGE_TAG, Environment.KAFKA_TIERED_STORAGE_BASE_IMAGE);
        SetupMinio.deployMinio(suiteStorage.getNamespaceName());
        SetupMinio.createBucket(suiteStorage.getNamespaceName(), BUCKET_NAME);

        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}
