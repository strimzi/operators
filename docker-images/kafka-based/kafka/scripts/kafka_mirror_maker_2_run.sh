#!/usr/bin/env bash
set -e
set +x

# Generate temporary keystore password
MIRRORMAKER_2_CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
export MIRRORMAKER_2_CERTS_STORE_PASSWORD

# Create dir where keystores and truststores will be stored
mkdir -p /tmp/kafka/clusters

# Import cluster certificates into keystores and truststores
echo "Preparing MirrorMaker 2 cluster truststores and keystores"

declare -A TLS_AUTH_CERTS
if [ -n "$KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS" ]; then
    IFS=$'\n' read -rd '' -a TLS_AUTH_CERTS_CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS" || true
    for cluster in "${TLS_AUTH_CERTS_CLUSTERS[@]}"
    do
        IFS='=' read -ra TLS_AUTH_CERT_CLUSTER <<< "${cluster}" || true
        TLS_AUTH_CERTS["${TLS_AUTH_CERT_CLUSTER[0]}"]="${TLS_AUTH_CERT_CLUSTER[1]}"
    done
fi

declare -A TLS_AUTH_KEYS
if [ -n "$KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS" ]; then
    IFS=$'\n' read -rd '' -a TLS_AUTH_KEYS_CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS" || true
    for cluster in "${TLS_AUTH_KEYS_CLUSTERS[@]}"
    do
        IFS='=' read -ra TLS_AUTH_KEY_CLUSTER <<< "${cluster}" || true
        TLS_AUTH_KEYS["${TLS_AUTH_KEY_CLUSTER[0]}"]="${TLS_AUTH_KEY_CLUSTER[1]}"
    done
fi

declare -A TRUSTED_CERTS
if [ -n "$KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS" ]; then
    IFS=$'\n' read -rd '' -a TRUSTED_CERTS_CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS" || true
    for cluster in "${TRUSTED_CERTS_CLUSTERS[@]}"
    do
        IFS='=' read -ra TRUSTED_CERTS_CLUSTER <<< "${cluster}" || true
        TRUSTED_CERTS["${TRUSTED_CERTS_CLUSTER[0]}"]="${TRUSTED_CERTS_CLUSTER[1]}"
    done
fi

declare -A OAUTH_TRUSTED_CERTS
if [ -n "$KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS_CLUSTERS" ]; then
    IFS=$'\n' read -rd '' -a OAUTH_TRUSTED_CERTS_CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS_CLUSTERS" || true
    for cluster in "${OAUTH_TRUSTED_CERTS_CLUSTERS[@]}"
    do
        IFS='=' read -ra OAUTH_TRUSTED_CERTS_CLUSTERS <<< "${cluster}" || true
        OAUTH_TRUSTED_CERTS["${OAUTH_TRUSTED_CERTS_CLUSTERS[0]}"]="${OAUTH_TRUSTED_CERTS_CLUSTERS[1]}"
    done
fi

if [ -n "$KAFKA_MIRRORMAKER_2_CLUSTERS" ]; then
    IFS=';' read -ra CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_CLUSTERS" || true
    for clusterAlias in "${CLUSTERS[@]}"
    do
        echo "Preparing MirrorMaker 2 truststores and keystores for cluster ${clusterAlias}"
        echo "  with trusted certs ${TRUSTED_CERTS["${clusterAlias}"]}"
        echo "  with tls auth certs ${TLS_AUTH_CERTS["${clusterAlias}"]}"
        echo "  with tls auth keys ${TLS_AUTH_KEYS["${clusterAlias}"]}"
        echo "  with OAuth trusted certs ${OAUTH_TRUSTED_CERTS["${clusterAlias}"]}"
        # $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
        ./kafka_mirror_maker_2_tls_prepare_certificates.sh \
            "${TRUSTED_CERTS["${clusterAlias}"]}" \
            "${TLS_AUTH_CERTS["${clusterAlias}"]}" \
            "${TLS_AUTH_KEYS["${clusterAlias}"]}" \
            "/tmp/kafka/clusters/${clusterAlias}.truststore.p12" \
            "/tmp/kafka/clusters/${clusterAlias}.keystore.p12" \
            "/opt/kafka/mm2-certs/${clusterAlias}" \
            "${OAUTH_TRUSTED_CERTS["${clusterAlias}"]}" \
            "/opt/kafka/mm2-oauth-certs/${clusterAlias}" \
            "/tmp/kafka/clusters/${clusterAlias}-oauth.truststore.p12"
    done
    echo "Preparing MirrorMaker 2 cluster truststores is complete"
fi

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

# Disable FIPS if needed
if [ "$FIPS_MODE" = "disabled" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} -Dcom.redhat.fips=false"
fi

exec ./kafka_connect_run.sh
