#!/usr/bin/env bash

set -x

RELEASE_VERSION=$(cat release.version)

cat packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml

# The commands 1) and 2) are used for change tag in installation files for CO. For each branch only of them match and apply changes
# 1) The following command is applied only for branches, where release.version contains *SNAPSHOT* (main + others)
sed -i "s#:latest#:${DOCKER_TAG}#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
# 2) The following command is applied only for release branches, where release.version contains a final version like 0.29.0
sed -i "s#:${RELEASE_VERSION}#:${DOCKER_TAG}#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml

# Change registry and org
sed -i "s#/opt/${DOCKER_REGISTRY}#/opt#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#quay.io/strimzi/kafka:#${DOCKER_REGISTRY}/${DOCKER_ORG}/kafka:#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#quay.io/strimzi/operator#${DOCKER_REGISTRY}/${DOCKER_ORG}/operator#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#quay.io/strimzi/jmxtrans#${DOCKER_REGISTRY}/${DOCKER_ORG}/jmxtrans#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml

sed -i "s#strimzi/kafka:latest#${DOCKER_ORG}/kafka:${DOCKER_TAG}#g" systemtest/src/test/resources/upgrade/BundleUpgrade.yaml
sed -i "s#strimzi/operator:latest#${DOCKER_ORG}/operator:${DOCKER_TAG}#g" systemtest/src/test/resources/upgrade/BundleUpgrade.yaml

cat packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
cat systemtest/src/test/resources/upgrade/BundleUpgrade.yaml
cat systemtest/src/test/resources/upgrade/BundleDowngrade.yaml

