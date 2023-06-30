#!/usr/bin/env bash
set -e

# Clean-up /tmp directory from files which might have remained from previous container restart
# We ignore any errors which might be caused by files injected by different agents which we do not have the rights to delete
rm -rfv /tmp/* || true

if [ -f /opt/topic-operator/custom-config/log4j2.properties ];
then
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/topic-operator/custom-config/log4j2.properties"
fi

# The java.util.logging.manager is set because of OkHttp client which is using JUL logging
export JAVA_OPTS="${JAVA_OPTS} -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

if [ -n "$STRIMZI_JAVA_OPTS" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JAVA_OPTS}"
fi

if [ "$STRIMZI_TLS_ENABLED" = "true" ] || [ "$STRIMZI_SECURITY_PROTOCOL" = "SSL" ] || [ "$STRIMZI_SECURITY_PROTOCOL" = "SASL_SSL" ]; then
    # Generate temporary keystore password
    CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
    export CERTS_STORE_PASSWORD

    mkdir -p /tmp/topic-operator

    # Import certificates into keystore and truststore
    "${STRIMZI_HOME}/bin/tls_prepare_certificates.sh"

    if [ "$STRIMZI_PUBLIC_CA" != "true" ]; then
        if [ -z "$STRIMZI_TRUSTSTORE_LOCATION" ]; then
            export STRIMZI_TRUSTSTORE_LOCATION=/tmp/topic-operator/replication.truststore.p12
            export STRIMZI_TRUSTSTORE_PASSWORD="$CERTS_STORE_PASSWORD"
        fi
    fi

    if [ "$STRIMZI_TLS_AUTH_ENABLED" != "false" ]; then
        if [ -z "$STRIMZI_KEYSTORE_LOCATION" ]; then
            export STRIMZI_KEYSTORE_LOCATION=/tmp/topic-operator/replication.keystore.p12
            export STRIMZI_KEYSTORE_PASSWORD="$CERTS_STORE_PASSWORD"
        fi
    fi
fi

export JAVA_CLASSPATH=$JAVA_CLASSPATH:lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@

if [ -n "${STRIMZI_ZOOKEEPER_CONNECT}" ]; then
  # If ZooKeeper connection details are provided then use the old bidirectional TO
  export JAVA_MAIN=io.strimzi.operator.topic.Main
else
  # Otherwise use the new unidirectional TO
  # See https://github.com/strimzi/proposals/blob/main/051-unidirectional-topic-operator.md
  export JAVA_MAIN=io.strimzi.operator.topic.v2.TopicOperatorMain
fi
exec "${STRIMZI_HOME}/bin/launch_java.sh"
