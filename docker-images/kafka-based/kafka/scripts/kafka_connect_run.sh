#!/usr/bin/env bash
set -e
set +x

# Prepare hostname - for StrimziPodSets we use the Pod DNS name assigned through the headless service
ADVERTISED_HOSTNAME=$(hostname -f | cut -d "." -f1-4)
export ADVERTISED_HOSTNAME

# Generate temporary keystore password
CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
export CERTS_STORE_PASSWORD

# Create dir where keystores and truststores will be stored
mkdir -p /tmp/kafka

# Import certificates into keystore and truststore
./kafka_connect_tls_prepare_certificates.sh

# Generate and print the config file
echo "Starting Kafka Connect with configuration:"
tee /tmp/strimzi-connect.properties < "/opt/kafka/custom-config/kafka-connect.properties" | sed -e 's/sasl.jaas.config=.*/sasl.jaas.config=[hidden]/g' -e 's/password=.*/password=[hidden]/g'
echo ""

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_CONNECT_METRICS_ENABLED" = "true" ]; then
    KAFKA_OPTS="${KAFKA_OPTS} -javaagent:$(ls "$JMX_EXPORTER_HOME"/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/custom-config/metrics-config.json"
    export KAFKA_OPTS
fi

. ./set_kafka_jmx_options.sh "${STRIMZI_JMX_ENABLED}" "${STRIMZI_JMX_USERNAME}" "${STRIMZI_JMX_PASSWORD}"

# enabling Tracing agent (initializes tracing) as Java agent
if [ "$STRIMZI_TRACING" = "jaeger" ] || [ "$STRIMZI_TRACING" = "opentelemetry" ]; then
    KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls "$KAFKA_HOME"/libs/tracing-agent*.jar)=$STRIMZI_TRACING"
    export KAFKA_OPTS
    if [ "$STRIMZI_TRACING" = "opentelemetry" ] && [ -z "$OTEL_TRACES_EXPORTER" ]; then
      # auto-set OTLP exporter
      export OTEL_TRACES_EXPORTER="otlp"
    fi
fi

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

# Disable FIPS if needed
if [ "$FIPS_MODE" = "disabled" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} -Dcom.redhat.fips=false"
fi

# Configure heap based on the available resources if needed
. ./dynamic_resources.sh

# Configure Garbage Collection logging
. ./set_kafka_gc_options.sh

set -x

# starting Kafka server with final configuration
exec /usr/bin/tini -w -e 143 -- "${KAFKA_HOME}/bin/connect-distributed.sh" /tmp/strimzi-connect.properties
