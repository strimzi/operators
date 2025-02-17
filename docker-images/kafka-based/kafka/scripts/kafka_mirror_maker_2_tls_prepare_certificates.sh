#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

# $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
trusted_certs="$1"
tls_auth_cert="$2"
tls_auth_key="$3"
truststore_path="$4"
keystore_path="$5"
certs_key_path="$6"
oauth_trusted_certs="$7"
oauth_certs_path="$8"
oauth_truststore_path="$9"

if [ -n "$trusted_certs" ]; then
    echo "Preparing $truststore_path truststore"
    prepare_truststore "$truststore_path" "$MIRRORMAKER_2_CERTS_STORE_PASSWORD" "$certs_key_path" "$trusted_certs"
fi

if [ -n "$tls_auth_cert" ] && [ -n "$tls_auth_key" ]; then
    echo "Preparing keystore"
    rm -f "$keystore_path"
    create_keystore_without_ca_file "$keystore_path" "$MIRRORMAKER_2_CERTS_STORE_PASSWORD" "$certs_key_path/$tls_auth_cert" "$certs_key_path/$tls_auth_key" "$tls_auth_cert"
    echo "Preparing keystore is complete"
fi

if [ -n "$oauth_trusted_certs" ]; then
    echo "Preparing $oauth_truststore_path truststore for OAuth"
    prepare_truststore "$oauth_truststore_path" "$MIRRORMAKER_2_CERTS_STORE_PASSWORD" "$oauth_certs_path" "$oauth_trusted_certs"
fi