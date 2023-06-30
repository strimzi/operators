#!/usr/bin/env bash
set -e

# Write the config file
cat <<EOF
# The directory where the snapshot is stored.
dataDir=${ZOOKEEPER_DATA_DIR}

# Other options
4lw.commands.whitelist=*
standaloneEnabled=false
reconfigEnabled=true

# TLS options
serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
ssl.clientAuth=need
ssl.quorum.clientAuth=need
secureClientPort=2181
sslQuorum=true

ssl.trustStore.location=/tmp/zookeeper/cluster.truststore.p12
ssl.trustStore.password=${CERTS_STORE_PASSWORD}
ssl.trustStore.type=PKCS12
ssl.quorum.trustStore.location=/tmp/zookeeper/cluster.truststore.p12
ssl.quorum.trustStore.password=${CERTS_STORE_PASSWORD}
ssl.quorum.trustStore.type=PKCS12

ssl.keyStore.location=/tmp/zookeeper/cluster.keystore.p12
ssl.keyStore.password=${CERTS_STORE_PASSWORD}
ssl.keyStore.type=PKCS12
ssl.quorum.keyStore.location=/tmp/zookeeper/cluster.keystore.p12
ssl.quorum.keyStore.password=${CERTS_STORE_PASSWORD}
ssl.quorum.keyStore.type=PKCS12

# Provided configuration
${ZOOKEEPER_CONFIGURATION}

# Zookeeper nodes configuration
EOF

# Setting self IP as 0.0.0.0 to workaround the slow DNS issue.
# For single node case, we cannot set to 0.0.0.0 since ZooKeeper will fail when looking for next candidate in case of issue.
# See: https://issues.apache.org/jira/browse/ZOOKEEPER-4708
NODE=1
while [[ $NODE -le $ZOOKEEPER_NODE_COUNT ]]; do
    if [[ $NODE -eq $ZOOKEEPER_ID ]] && [[ $ZOOKEEPER_NODE_COUNT -gt 1 ]]; then
      echo "server.${NODE}=0.0.0.0:2888:3888:participant;127.0.0.1:12181"
    else
      echo "server.${NODE}=${BASE_HOSTNAME}-$((NODE-1)).${BASE_FQDN}:2888:3888:participant;127.0.0.1:12181"
    fi
    (( NODE=NODE+1 ))
done
