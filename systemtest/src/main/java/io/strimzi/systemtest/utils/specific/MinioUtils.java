/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.minio.SetupMinio;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MinioUtils {
    private static final Logger LOGGER = LogManager.getLogger(SetupMinio.class);

    private MinioUtils() {

    }

    /**
     * Collect data from Minio about usage of a specific bucket
     * @param namespace
     * @param bucketName
     * @return Overall statistics about the bucket in String format
     */
    public static String getBucketSizeInfo(String namespace, String bucketName) {
        final String minioPod = ResourceManager.kubeClient().listPods(namespace, Map.of(TestConstants.APP_POD_LABEL, SetupMinio.MINIO)).get(0).getMetadata().getName();

        return ResourceManager.cmdKubeClient().namespace(namespace).execInPod(minioPod,
            "mc",
            "stat",
            "local/" + bucketName).out();

    }

    /**
     * Parse out total size of bucket from the information about usage.
     * @param bucketInfo String containing all stat info about bucket
     * @return Map consists of parsed size and it's unit
     */
    private static Map<String, Object> parseTotalSize(String bucketInfo) {
        Pattern pattern = Pattern.compile("Total size:\\s*(?<size>[\\d.]+)\\s*(?<unit>.*)");
        Matcher matcher = pattern.matcher(bucketInfo);

        if (matcher.find()) {
            return Map.of("size", Double.parseDouble(matcher.group("size")), "unit", matcher.group("unit"));
        } else {
            throw new IllegalArgumentException("Total size not found in the provided string");
        }
    }

    /**
     * Wait until size of the bucket is not 0 B.
     * @param namespace Minio location
     * @param bucketName bucket name
     */
    public static void waitForDataInMinio(String namespace, String bucketName) {
        TestUtils.waitFor("data sync from Kafka to Minio", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT_LONG, () -> {
            String bucketSizeInfo = getBucketSizeInfo(namespace, bucketName);
            Map<String, Object> parsedSize = parseTotalSize(bucketSizeInfo);
            double bucketSize = (Double) parsedSize.get("size");
            LOGGER.info("Collected bucket size: {} {}", bucketSize, parsedSize.get("unit"));
            LOGGER.debug("Collected bucket info:\n{}", bucketSizeInfo);

            return bucketSize > 0;
        });
    }

    /**
     * Wait until size of the bucket is 0 B.
     * @param namespace Minio location
     * @param bucketName bucket name
     */
    public static void waitForNoDataInMinio(String namespace, String bucketName) {
        TestUtils.waitFor("data deletion in Minio", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT_LONG, () -> {
            String bucketSizeInfo = getBucketSizeInfo(namespace, bucketName);
            Map<String, Object> parsedSize = parseTotalSize(bucketSizeInfo);
            double bucketSize = (Double) parsedSize.get("size");
            LOGGER.info("Collected bucket size: {} {}", bucketSize, parsedSize.get("unit"));
            LOGGER.debug("Collected bucket info:\n{}", bucketSizeInfo);

            return bucketSize == 0;
        });
    }
}
