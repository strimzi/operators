/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;

/**
 * Class for scaling Zookeeper 3.5 using the ZookeeperAdmin client
 */
public class ZookeeperScaler implements AutoCloseable {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZookeeperScaler.class);

    private final ZooKeeperAdminProvider zooAdminProvider;

    private final String zookeeperConnectionString;

    private final Function<Integer, String> zkNodeAddress;

    private final long operationTimeoutMs;

    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;

    private final String trustStorePassword;
    private File trustStoreFile;

    private final String keyStorePassword;
    private File keyStoreFile;

    private final Reconciliation reconciliation;

    /**
     * ZookeeperScaler constructor
     *
     * @param reconciliation                The reconciliation
     * @param zookeeperConnectionString     Connection string to connect to the right Zookeeper
     * @param zkNodeAddress                 Function for generating the Zookeeper node addresses
     * @param clusterCaCertSecret           Secret with Kafka cluster CA public key
     * @param coKeySecret                   Secret with Cluster Operator public and private key
     * @param operationTimeoutMs            Operation timeout
     *
     */
    protected ZookeeperScaler(Reconciliation reconciliation, ZooKeeperAdminProvider zooAdminProvider, String zookeeperConnectionString, Function<Integer, String> zkNodeAddress, Secret clusterCaCertSecret, Secret coKeySecret, long operationTimeoutMs) {
        this.reconciliation = reconciliation;

        LOGGER.debugCr(reconciliation, "Creating Zookeeper Scaler for cluster {}", zookeeperConnectionString);

        this.zooAdminProvider = zooAdminProvider;
        this.zookeeperConnectionString = zookeeperConnectionString;
        this.zkNodeAddress = zkNodeAddress;
        this.operationTimeoutMs = operationTimeoutMs;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;

        // Setup truststore from PEM file in cluster CA secret
        // We cannot use P12 because of custom CAs which for simplicity provide only PEM
        PasswordGenerator pg = new PasswordGenerator(12);
        trustStorePassword = pg.generate();

        // Setup keystore from PKCS12 in cluster-operator secret
        keyStorePassword = new String(Util.decodeFromSecret(coKeySecret, "cluster-operator.password"), StandardCharsets.US_ASCII);
    }

    /**
     * Scales Zookeeper to defined number of instances.
     * It generates new configuration according to the desired number of nodes and updates Zookeeper configuration.
     *
     * @param scaleTo   Number of Zookeeper nodes which should be used by the cluster
     * @throws InterruptedException Because this can block for a while
     */
    public void scale(int scaleTo) throws InterruptedException {
        var config = getClientConfig();
        var zkAdmin = connect(config);
        try {
            var servers = getCurrentConfig(zkAdmin);
            scaleTo(zkAdmin, servers, scaleTo);
        } finally {
            closeConnection(zkAdmin);
        }
    }

    /**
     * Close the ZookeeperScaler instance. This deletes the certificate files.
     */
    @Override
    public void close() {
        if (trustStoreFile != null) {
            if (!trustStoreFile.delete())   {
                LOGGER.debugCr(reconciliation, "Failed to delete file {}", trustStoreFile);
            }
        }

        if (keyStoreFile != null)   {
            if (!keyStoreFile.delete())   {
                LOGGER.debugCr(reconciliation, "Failed to delete file {}", keyStoreFile);
            }
        }
    }

    /**
     * Internal method used to create the Zookeeper Admin client and connect it to Zookeeper
     *
     * @return The admin client
     */
    private ZooKeeperAdmin connect(ZKClientConfig clientConfig) throws InterruptedException {
        try {
            ZooKeeperAdmin zkAdmin = zooAdminProvider.createZookeeperAdmin(
                this.zookeeperConnectionString,
                10_000,
                watchedEvent -> LOGGER.debugCr(reconciliation, "Received event {} from ZooKeeperAdmin client connected to {}", watchedEvent, zookeeperConnectionString),
                clientConfig);

            boolean success = Util.await(reconciliation, "connect to zk", 1_000, operationTimeoutMs,
                () -> zkAdmin.getState().isAlive() && zkAdmin.getState().isConnected());
            if (!success) {
                String message = String.format("Failed to connect to Zookeeper %s. Connection was not ready in %d ms.", zookeeperConnectionString, operationTimeoutMs);
                LOGGER.warnCr(reconciliation, message);
                closeConnection(zkAdmin);
                throw new ZookeeperScalingException(message);
            }
            return zkAdmin;
        } catch (IOException e)   {
            LOGGER.warnCr(reconciliation, "Failed to connect to {} to scale Zookeeper", zookeeperConnectionString, e);
            throw new ZookeeperScalingException("Failed to connect to Zookeeper " + zookeeperConnectionString, e);
        }
    }

    /**
     * Internal method to scale Zookeeper up or down or check configuration. It will:
     *     1) Compare the current configuration with the desired configuration
     *     2) Update the configuration if needed
     *
     * @param currentServers    Current list of servers from Zookeeper cluster
     * @param scaleTo           Desired scale
     */
    private void scaleTo(ZooKeeperAdmin zkAdmin, Map<String, String> currentServers, int scaleTo) throws InterruptedException {
        Map<String, String> desiredServers = generateConfig(scaleTo, zkNodeAddress);

        if (isDifferent(currentServers, desiredServers))    {
            LOGGER.debugCr(reconciliation, "The Zookeeper server configuration needs to be updated");
            updateConfig(zkAdmin, desiredServers);
        } else {
            LOGGER.debugCr(reconciliation, "The Zookeeper server configuration is already up to date");
        }
    }

    /**
     * Gets the current configuration from Zookeeper.
     *
     * @return  Future containing Map with the current Zookeeper configuration
     */
    private Map<String, String> getCurrentConfig(ZooKeeperAdmin zkAdmin)    {
        try {
            byte[] config = zkAdmin.getConfig(false, null);
            Map<String, String> servers = parseConfig(config);
            LOGGER.debugCr(reconciliation, "Current Zookeeper configuration is {}", servers);
            return servers;
        } catch (KeeperException | InterruptedException e)    {
            LOGGER.warnCr(reconciliation, "Failed to get current Zookeeper server configuration", e);
            throw new ZookeeperScalingException("Failed to get current Zookeeper server configuration", e);
        }
    }

    /**
     * Updates the configuration in the Zookeeper cluster
     *
     * @param newServers    New configuration which will be used for the update
     * @return              The updated configuration
     */
    private Map<String, String> updateConfig(ZooKeeperAdmin zkAdmin, Map<String, String> newServers) throws InterruptedException {
        try {
            LOGGER.debugCr(reconciliation, "Updating Zookeeper configuration to {}", newServers);
            byte[] newConfig = zkAdmin.reconfigure(null, null, serversMapToList(newServers), -1, null);
            Map<String, String> servers = parseConfig(newConfig);

            LOGGER.debugCr(reconciliation, "New Zookeeper configuration is {}", servers);
            return servers;
        } catch (KeeperException e)    {
            LOGGER.warnCr(reconciliation, "Failed to update Zookeeper server configuration", e);
            throw new ZookeeperScalingException("Failed to update Zookeeper server configuration", e);
        }
    }

    /**
     * Closes the Zookeeper connection
     */
    private void closeConnection(ZooKeeperAdmin zkAdmin) {
        try {
            zkAdmin.close((int) operationTimeoutMs);
        } catch (Exception e) {
            LOGGER.warnCr(reconciliation, "Failed to close the ZooKeeperAdmin", e);
        }
    }

    /**
     * Generates the TLS configuration for Zookeeper.
     *
     * @return The current config
     */
    private ZKClientConfig getClientConfig()  {
        try {
            ZKClientConfig clientConfig = new ZKClientConfig();

            trustStoreFile = Util.createFileTrustStore(getClass().getName(), "p12", Ca.cert(clusterCaCertSecret, Ca.CA_CRT), trustStorePassword.toCharArray());
            keyStoreFile = Util.createFileStore(getClass().getName(), "p12", Util.decodeFromSecret(coKeySecret, "cluster-operator.p12"));

            clientConfig.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
            clientConfig.setProperty("zookeeper.client.secure", "true");
            clientConfig.setProperty("zookeeper.ssl.trustStore.location", trustStoreFile.getAbsolutePath());
            clientConfig.setProperty("zookeeper.ssl.trustStore.password", trustStorePassword);
            clientConfig.setProperty("zookeeper.ssl.trustStore.type", "PKCS12");
            clientConfig.setProperty("zookeeper.ssl.keyStore.location", keyStoreFile.getAbsolutePath());
            clientConfig.setProperty("zookeeper.ssl.keyStore.password", keyStorePassword);
            clientConfig.setProperty("zookeeper.ssl.keyStore.type", "PKCS12");
            clientConfig.setProperty("zookeeper.request.timeout", String.valueOf(operationTimeoutMs));

            return clientConfig;
        } catch (Exception e)    {
            LOGGER.warnCr(reconciliation, "Failed to create Zookeeper client configuration", e);
            throw new ZookeeperScalingException("Failed to create Zookeeper client configuration", e);
        }
    }

    /**
     * Converts the map with configuration to List of Strings which is the format in which the ZookeeperAdmin client
     * expects the new configuration.
     *
     * @param servers   Map with Zookeeper configuration
     * @return          List with Zookeeper configuration
     */
    /*test*/ static List<String> serversMapToList(Map<String, String> servers)  {
        List<String> serversList = new ArrayList<>(servers.size());

        for (var entry : servers.entrySet())  {
            serversList.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
        }

        return serversList;
    }

    /**
     * Parse the byte array we get from Zookeeper into a map we use internally. The returned Map will container only
     * the server entries from the Zookeeper configuration. Other entries such as version will be ignored.
     *
     * @param byteConfig    byte[] from Zookeeper client
     * @return              Map with Zookeeper configuration
     */
    /*test*/ static Map<String, String> parseConfig(byte[] byteConfig) {
        String config = new String(byteConfig, StandardCharsets.US_ASCII);

        Map<String, String> configMap = Util.parseMap(config);

        Map<String, String> serverMap = new HashMap<>(configMap.size() - 1);

        for (Map.Entry<String, String> entry : configMap.entrySet())    {
            if (entry.getKey().startsWith("server."))   {
                serverMap.put(entry.getKey(), entry.getValue());
            }
        }

        return serverMap;
    }

    /**
     * Checks whether two Zookeeper configurations are different or not. We will change the configuration only if it
     * differs to minimize the load.
     *
     * @param current   Map with current configuration
     * @param desired   Map with desired configuration
     * @return          True if the configurations differ and should be updated. False otherwise.
     */
    /*test*/ static boolean isDifferent(Map<String, String> current, Map<String, String> desired)    {
        return !current.equals(desired);
    }

    /**
     * Generates a map with Zookeeper configuration
     *
     * @param scale     Number of nodes which the Zookeeper cluster should have
     * @return          Map with configuration
     */
    /*test*/ static Map<String, String> generateConfig(int scale, Function<Integer, String> zkNodeAddress)   {
        Map<String, String> servers = new HashMap<>(scale);

        for (int i = 0; i < scale; i++) {
            // The Zookeeper server IDs starts with 1, but pod index starts from 0
            String key = String.format("server.%d", i + 1);
            String value = String.format("%s:%d:%d:participant;127.0.0.1:%d", zkNodeAddress.apply(i), ZookeeperCluster.CLUSTERING_PORT, ZookeeperCluster.LEADER_ELECTION_PORT, ZookeeperCluster.CLIENT_PLAINTEXT_PORT);

            servers.put(key, value);
        }

        return servers;
    }
}
