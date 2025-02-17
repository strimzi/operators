FROM strimzi/base:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

ARG KAFKA_DIST_DIR
ARG KAFKA_VERSION
ARG THIRD_PARTY_LIBS
ARG strimzi_version
ARG TARGETOS
ARG TARGETARCH

LABEL name='kafka' \
    version="${strimzi_version}-${KAFKA_VERSION}" \
    release="${strimzi_version}" \
    summary="Kafka ${KAFKA_VERSION} image of the Strimzi Kafka Operator project." \
    description="Kafka image used by the Strimzi Kafka Operator to run Kafka ${KAFKA_VERSION} clusters."

RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y gettext nmap-ncat net-tools unzip hostname findutils tar \
    && microdnf clean all -y

# Add kafka user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 kafka

#####
# Add Kafka
#####
ENV KAFKA_HOME=/opt/kafka
ENV KAFKA_VERSION=${KAFKA_VERSION}
ENV STRIMZI_VERSION=${strimzi_version}

COPY $KAFKA_DIST_DIR $KAFKA_HOME
COPY ./scripts/ $KAFKA_HOME
RUN mkdir $KAFKA_HOME/plugins

#####
# Add Kafka Exporter
#####
ENV KAFKA_EXPORTER_HOME=/opt/kafka-exporter
ENV KAFKA_EXPORTER_VERSION=1.9.0
ENV KAFKA_EXPORTER_CHECKSUM_AMD64="2bf79378221e7662c054d5dafaa48c0a77b8d6036d31d5fde3aaa1d2049e5619f6f9d21c2e2a0944e38a916fc12fbcb8e200ec5daf4f9019d6a35bc85f79eb39  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_ARM64="d57cd7478f632cd05e8d5296377da59dfe934e154c1c6ccda9f109137c740132e17fc358d4cb29523b0f575a72ac6b7e2bd87b6ecb9363bba86035a8b14df9e4  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_PPC64LE="f39618b082630f314939accb05622471191fd627cf3e7d07bfbafa69cc850f1f4987e64433eb3ec44e7dbb235cc4d3867b06c89a833085dd9a65f3f0bebf18e9  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_S390X="58bac418666f525ef255049d3a21f508dbe3c138696b6183c1cef21d4fee747dde2bc6d2cd7c94f3c9a0d0185a42b67b01eb61cad66acb6e4254ba6b46cd259a kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz"

RUN set -ex; \
    if [[ "${TARGETOS}/${TARGETARCH}" = "linux/arm64" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_ARM64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz*; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/ppc64le" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_PPC64LE > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz*; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/s390x" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_S390X > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz*; \
    else \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_AMD64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz*; \
    fi

COPY ./exporter-scripts $KAFKA_EXPORTER_HOME

#####
# Add Prometheus JMX Exporter
#####
ENV JMX_EXPORTER_HOME=/opt/prometheus-jmx-exporter
ENV JMX_EXPORTER_VERSION=1.1.0
ENV JMX_EXPORTER_CHECKSUM="63f9ff5297c6add9ecd19633a54300ffcec0df2e09afca7212e80506b801ea20aa009efc4b582b6ed8a4616b1ed68c98e30cc5b9dbf2cf6fe68b7cf2e3c5db3c  jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

RUN set -ex; \
    curl -LO https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar; \
    echo $JMX_EXPORTER_CHECKSUM > jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512; \
    sha512sum --check jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512; \
    mkdir $JMX_EXPORTER_HOME; \
    mv jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar $JMX_EXPORTER_HOME/; \
    rm -f jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512;

#####
# Add Strimzi agents
#####
COPY ./tmp/kafka-agent-${STRIMZI_VERSION}.jar ${KAFKA_HOME}/libs/
COPY ./tmp/tracing-agent-${STRIMZI_VERSION}.jar ${KAFKA_HOME}/libs/

#####
# Add 3rd party libs
#####
COPY tmp/thirdparty-libs-${THIRD_PARTY_LIBS}/ ${KAFKA_HOME}/libs/

#####
# Add Cruise Control
#####
ENV CRUISE_CONTROL_HOME=/opt/cruise-control
RUN mkdir $CRUISE_CONTROL_HOME
COPY tmp/cc/ ${CRUISE_CONTROL_HOME}/libs/
COPY ./cruise-control-scripts $CRUISE_CONTROL_HOME

WORKDIR $KAFKA_HOME

USER 1001
