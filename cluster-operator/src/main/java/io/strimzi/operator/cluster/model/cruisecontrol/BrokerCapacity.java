/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

public class BrokerCapacity {
    // CC allows specifying a generic "default" broker entry in the capacity configuration to apply to all brokers without a specific broker entry.
    // CC designates the id of this default broker entry as "-1".
    public static final int DEFAULT_BROKER_ID = -1;
    public static final String DEFAULT_BROKER_DOC = "This is the default capacity. Capacity unit used for disk is in MiB, cpu is in number of cores, network throughput is in KiB.";
    public static final String DEFAULT_CPU_CORE_CAPACITY = "1";
    public static final String DEFAULT_DISK_CAPACITY_IN_MIB = "100000";
    public static final String DEFAULT_INBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND = "10000";
    public static final String DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND = "10000";

    private int id;
    private CpuCapacity cpu;
    private DiskCapacity disk;
    private String inboundNetwork;
    private String outboundNetwork;
    private final String doc;

    public BrokerCapacity(int brokerId, CpuCapacity cpu, DiskCapacity disk, String inboundNetwork, String outboundNetwork) {
        this.id = brokerId;
        this.cpu = cpu;
        this.disk = disk;
        this.inboundNetwork = inboundNetwork;
        this.outboundNetwork = outboundNetwork;
        this.doc = brokerId == -1 ? DEFAULT_BROKER_DOC : "Capacity for Broker " + brokerId;
    }

    public Integer getId() {
        return id;
    }

    public CpuCapacity getCpu() {
        return cpu;
    }

    public DiskCapacity getDisk() {
        return disk;
    }

    public String getInboundNetwork() {
        return inboundNetwork;
    }

    public String getOutboundNetwork() {
        return outboundNetwork;
    }

    public String getDoc() {
        return doc;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setCpu(CpuCapacity cpu) {
        this.cpu = cpu;
    }

    public void setDisk(DiskCapacity disk) {
        this.disk = disk;
    }

    public void setInboundNetwork(String inboundNetwork) {
        this.inboundNetwork = inboundNetwork;
    }

    public void setOutboundNetwork(String outboundNetwork) {
        this.outboundNetwork = outboundNetwork;
    }
}
