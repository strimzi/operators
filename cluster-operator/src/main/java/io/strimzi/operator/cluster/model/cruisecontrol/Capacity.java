/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.balancing.BrokerCapacityOverride;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * Uses information in a Kafka Custom Resource to generate a capacity configuration file to be used for
 * Cruise Control's Broker Capacity File Resolver.
 *
 *
 * For example, it takes a Kafka Custom Resource like the following:
 *
 * spec:
 *  kafka:
 *     replicas: 3
 *     storage:
 *       type: jbod
 *       volumes:
 *       - id: 0
 *         type: persistent-claim
 *         size: 100Gi
 *         deleteClaim: false
 *       - id: 1
 *         type: persistent-claim
 *         size: 200Gi
 *         deleteClaim: false
 *  cruiseControl:
 *    brokerCapacity:
 *     inboundNetwork: 10000KB/s
 *     outboundNetwork: 10000KB/s
 *     overrides:
 *       - brokers: [0]
 *         outboundNetwork: 40000KB/s
 *       - brokers: [1, 2]
 *         inboundNetwork: 60000KB/s
 *         outboundNetwork: 20000KB/s
 *
 * and uses the information to create Cruise Control BrokerCapacityFileResolver config file like the following:
 *
 * {
 *   "brokerCapacities":[
 *     {
 *       "brokerId": "-1",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log-1": "100000",
 *             "/var/lib/kafka1/kafka-log-1": "200000"
 *          },
 *         "CPU": "100",
 *         "NW_IN": "10000",
 *         "NW_OUT": "10000"
 *       },
 *       "doc": "This is the default capacity. Capacity unit used for disk is in MB, cpu is in percentage, network throughput is in KB."
 *     },
 *     {
 *       "brokerId": "0",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log0": "100000",
 *             "/var/lib/kafka1/kafka-log0": "200000"
 *          },
 *         "CPU": "100",
 *         "NW_IN": "10000",
 *         "NW_OUT": "40000"
 *       },
 *       "doc": "Capacity for Broker 0"
 *     },
 *     {
 *       "brokerId": "1",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log1": "100000",
 *             "/var/lib/kafka1/kafka-log1": "200000"
 *           },
 *         "CPU": "100",
 *         "NW_IN": "60000",
 *         "NW_OUT": "20000"
 *       },
 *       "doc": "Capacity for Broker 1"
 *     },
 *       "brokerId": "2",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log2": "100000",
 *             "/var/lib/kafka1/kafka-log2": "200000"
 *           },
 *         "CPU": "100",
 *         "NW_IN": "60000",
 *         "NW_OUT": "20000"
 *       },
 *       "doc": "Capacity for Broker 2"
 *     }
 *   ]
 * }
 */
public class Capacity {
    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Capacity.class.getName());

    private final TreeMap<Integer, BrokerCapacity> capacityEntries;

    public static final String CAPACITIES_KEY = "brokerCapacities";
    private static final String BROKER_ID_KEY = "brokerId";
    public static final String CAPACITY_KEY = "capacity";
    public static final String DISK_KEY = "DISK";
    private static final String CPU_KEY = "CPU";
    private static final String INBOUND_NETWORK_KEY = "NW_IN";
    private static final String OUTBOUND_NETWORK_KEY = "NW_OUT";
    private static final String DOC_KEY = "doc";

    private final int replicas;
    private final Storage storage;

    public Capacity(KafkaSpec spec, Storage storage) {
        io.strimzi.api.kafka.model.balancing.BrokerCapacity bc = spec.getCruiseControl().getBrokerCapacity();

        this.capacityEntries = new TreeMap<>();
        this.replicas = spec.getKafka().getReplicas();
        this.storage = storage;

        processCapacityEntries(bc);
    }

    public static String processCpu() {
        return BrokerCapacity.DEFAULT_CPU_UTILIZATION_CAPACITY;
    }

    public static DiskCapacity processDisk(Storage storage, int brokerId) {
        if (storage instanceof JbodStorage) {
            return generateJbodDiskCapacity(storage, brokerId);
        } else {
            return generateDiskCapacity(storage);
        }
    }

    public static String processInboundNetwork(io.strimzi.api.kafka.model.balancing.BrokerCapacity bc, BrokerCapacityOverride override) {
        if (override != null && override.getInboundNetwork() != null) {
            return getThroughputInKiB(override.getInboundNetwork());
        } else if (bc != null && bc.getInboundNetwork() != null) {
            return getThroughputInKiB(bc.getInboundNetwork());
        } else {
            return BrokerCapacity.DEFAULT_INBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND;
        }
    }

    public static String processOutboundNetwork(io.strimzi.api.kafka.model.balancing.BrokerCapacity bc, BrokerCapacityOverride override) {
        if (override != null && override.getOutboundNetwork() != null) {
            return getThroughputInKiB(override.getOutboundNetwork());
        } else if (bc != null && bc.getOutboundNetwork() != null) {
            return getThroughputInKiB(bc.getOutboundNetwork());
        } else {
            return BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND;
        }
    }

    /**
     * Generate JBOD disk capacity configuration for a broker using the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @param brokerId Id of the broker
     * @return Disk capacity configuration value for broker brokerId
     */
    private static DiskCapacity generateJbodDiskCapacity(Storage storage, int brokerId) {
        DiskCapacity disks = new DiskCapacity();
        String size = "";

        for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
            String name = VolumeUtils.createVolumePrefix(volume.getId(), true);
            String path = AbstractModel.KAFKA_MOUNT_PATH + "/" + name + "/" + AbstractModel.KAFKA_LOG_DIR + brokerId;

            if (volume instanceof PersistentClaimStorage) {
                size = ((PersistentClaimStorage) volume).getSize();
            } else if (volume instanceof EphemeralStorage) {
                size = ((EphemeralStorage) volume).getSizeLimit();
            }
            disks.add(path, String.valueOf(getSizeInMiB(size)));
        }
        return disks;
    }

    /**
     * Generate total disk capacity using the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @return Disk capacity per broker
     */
    public static DiskCapacity generateDiskCapacity(Storage storage) {
        if (storage instanceof PersistentClaimStorage) {
            return DiskCapacity.of(getSizeInMiB(((PersistentClaimStorage) storage).getSize()));
        } else if (storage instanceof EphemeralStorage) {
            if (((EphemeralStorage) storage).getSizeLimit() != null) {
                return DiskCapacity.of(getSizeInMiB(((EphemeralStorage) storage).getSizeLimit()));
            } else {
                return DiskCapacity.of(BrokerCapacity.DEFAULT_DISK_CAPACITY_IN_MIB);
            }
        } else {
            throw new IllegalStateException("The declared storage '" + storage.getType() + "' is not supported");
        }
    }

    /*
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi},
     * into the equivalent number of mebibytes represented as a String.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of mebibytes.
     */
    public static String getSizeInMiB(String size) {
        if (size == null) {
            return BrokerCapacity.DEFAULT_DISK_CAPACITY_IN_MIB;
        }
        return String.valueOf(StorageUtils.convertTo(size, "Mi"));
    }

    /*
     * Parse Strimzi representation of throughput, such as {@code 10000KB/s},
     * into the equivalent number of kibibytes represented as a String.
     *
     * @param throughput The String representation of the throughput.
     * @return The equivalent number of kibibytes.
     */
    public static String getThroughputInKiB(String throughput) {
        String size = throughput.substring(0, throughput.indexOf("B"));
        return String.valueOf(StorageUtils.convertTo(size, "Ki"));
    }

    private void processCapacityEntries(io.strimzi.api.kafka.model.balancing.BrokerCapacity brokerCapacity) {
        String cpu = processCpu();
        DiskCapacity disk = processDisk(storage, BrokerCapacity.DEFAULT_BROKER_ID);
        String inboundNetwork = processInboundNetwork(brokerCapacity, null);
        String outboundNetwork = processOutboundNetwork(brokerCapacity, null);

        // Default broker entry
        BrokerCapacity defaultBrokerCapacity = new BrokerCapacity(BrokerCapacity.DEFAULT_BROKER_ID, cpu, disk, inboundNetwork, outboundNetwork);
        capacityEntries.put(BrokerCapacity.DEFAULT_BROKER_ID, defaultBrokerCapacity);

        if (storage instanceof JbodStorage) {
            // A capacity configuration for a cluster with a JBOD configuration
            // requires a distinct broker capacity entry for every broker because the
            // Kafka volume paths are not homogeneous across brokers and include
            // the broker pod index in their names.
            for (int id = 0; id < replicas; id++) {
                disk = processDisk(storage, id);
                BrokerCapacity broker = new BrokerCapacity(id, cpu, disk, inboundNetwork, outboundNetwork);
                capacityEntries.put(id, broker);
            }
        }

        if (brokerCapacity != null) {
            // For checking for duplicate brokerIds
            Set<Integer> overrideIds = new HashSet<>();
            List<BrokerCapacityOverride> overrides = brokerCapacity.getOverrides();
            // Override broker entries
            if (overrides != null) {
                if (overrides.isEmpty()) {
                    LOGGER.warnOp("Ignoring empty overrides list");
                } else {
                    for (BrokerCapacityOverride override : overrides) {
                        List<Integer> ids = override.getBrokers();
                        inboundNetwork = processInboundNetwork(brokerCapacity, override);
                        outboundNetwork = processOutboundNetwork(brokerCapacity, override);
                        for (int id : ids) {
                            if (id == BrokerCapacity.DEFAULT_BROKER_ID) {
                                LOGGER.warnOp("Ignoring broker capacity override with illegal broker id -1.");
                            } else {
                                if (capacityEntries.containsKey(id)) {
                                    if (overrideIds.add(id)) {
                                        BrokerCapacity brokerCapacityEntry = capacityEntries.get(id);
                                        brokerCapacityEntry.setInboundNetwork(inboundNetwork);
                                        brokerCapacityEntry.setOutboundNetwork(outboundNetwork);
                                    } else {
                                        LOGGER.warnOp("Duplicate broker id %d found in overrides, using first occurrence.", id);
                                    }
                                } else {
                                    BrokerCapacity brokerCapacityEntry = new BrokerCapacity(id, cpu, disk, inboundNetwork, outboundNetwork);
                                    capacityEntries.put(id, brokerCapacityEntry);
                                    overrideIds.add(id);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Generate broker capacity entry for capacity configuration.
     *
     * @param brokerCapacity Broker capacity object
     * @return Broker entry as a JsonObject
     */
    private JsonObject generateBrokerCapacity(BrokerCapacity brokerCapacity) {
        return new JsonObject()
            .put(BROKER_ID_KEY, brokerCapacity.getId())
            .put(CAPACITY_KEY, new JsonObject()
                .put(DISK_KEY, brokerCapacity.getDisk().getJson())
                .put(CPU_KEY, brokerCapacity.getCpu())
                .put(INBOUND_NETWORK_KEY, brokerCapacity.getInboundNetwork())
                .put(OUTBOUND_NETWORK_KEY, brokerCapacity.getOutboundNetwork())
            )
            .put(DOC_KEY, brokerCapacity.getDoc());
    }

    /**
     * Generate a capacity configuration for cluster
     *
     * @return Cruise Control capacity configuration as a JsonObject
     */
    public JsonObject generateCapacityConfig() {
        JsonArray brokerList = new JsonArray();
        for (BrokerCapacity brokerCapacity : capacityEntries.values()) {
            JsonObject brokerEntry = generateBrokerCapacity(brokerCapacity);
            brokerList.add(brokerEntry);
        }

        JsonObject config = new JsonObject();
        config.put("brokerCapacities", brokerList);

        return config;
    }

    public String toString() {
        return generateCapacityConfig().encodePrettily();
    }

    public TreeMap<Integer, BrokerCapacity> getCapacityEntries() {
        return capacityEntries;
    }
}
