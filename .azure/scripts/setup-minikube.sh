#!/usr/bin/env bash
set -xe
set -o errexit

rm -rf ~/.kube

KUBE_VERSION=${KUBE_VERSION:-1.21.0}
MINIKUBE_REGISTRY_IMAGE=${REGISTRY_IMAGE:-"registry"}
COPY_DOCKER_LOGIN=${COPY_DOCKER_LOGIN:-"false"}

DEFAULT_CLUSTER_MEMORY=$(free -m | grep "Mem" | awk '{print $2}')
DEFAULT_CLUSTER_CPU=$(awk '$1~/cpu[0-9]/{usage=($2+$4)*100/($2+$4+$5); print $1": "usage"%"}' /proc/stat | wc -l)

CLUSTER_MEMORY=${CLUSTER_MEMORY:-$DEFAULT_CLUSTER_MEMORY}
CLUSTER_CPU=${CLUSTER_CPU:-$DEFAULT_CLUSTER_CPU}

echo "[INFO] CLUSTER_MEMORY: ${CLUSTER_MEMORY}"
echo "[INFO] CLUSTER_CPU: ${CLUSTER_CPU}"

# note that IPv6 is only supported on kind (i.e., minikube does not support it). Also we assume that when you set this flag
# to true then you meet requirements (i.) net.ipv6.conf.all.disable_ipv6 = 0 (ii. you have installed CNI supporting IPv6)
IP_FAMILY=${IP_FAMILY:-"ipv4"}

ARCH=$1
if [ -z "$ARCH" ]; then
    ARCH="amd64"
fi

function install_kubectl {
    if [ "${TEST_KUBECTL_VERSION:-latest}" = "latest" ]; then
        TEST_KUBECTL_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    fi
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${TEST_KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl && chmod +x kubectl
    sudo cp kubectl /usr/local/bin
}

function label_node {
	# It should work for all clusters
	for nodeName in $(kubectl get nodes -o custom-columns=:.metadata.name --no-headers);
	do
		echo ${nodeName};
		kubectl label node ${nodeName} rack-key=zone;
	done
}

function install_kubernetes_provisioner {
      if [ "${TEST_MINIKUBE_VERSION:-latest}" = "latest" ]; then
          TEST_KUBERNETES_URL=https://storage.googleapis.com/minikube/releases/latest/minikube-linux-${ARCH}
      else
          TEST_KUBERNETES_URL=https://github.com/kubernetes/minikube/releases/download/${TEST_MINIKUBE_VERSION}/minikube-linux-${ARCH}
      fi

      if [ "$KUBE_VERSION" != "latest" ] && [ "$KUBE_VERSION" != "stable" ]; then
          KUBE_VERSION="v${KUBE_VERSION}"
      fi

      curl -Lo $TEST_CLUSTER ${TEST_KUBERNETES_URL} && chmod +x $TEST_CLUSTER
      sudo cp $TEST_CLUSTER /usr/local/bin
}

function create_cluster_role_binding_admin {
    kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=kube-system:default
}

function setup_kube_directory {
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config
}

function setup_registry_proxy {
    if [ "$ARCH" = "s390x" ]; then
        git clone -b v1.9.11 --depth 1 https://github.com/kubernetes/kubernetes.git
        sed -i 's/:1.11/:1.22.1/' kubernetes/cluster/addons/registry/images/Dockerfile
        docker build --pull -t gcr.io/google_containers/kube-registry-proxy:0.4-${ARCH} kubernetes/cluster/addons/registry/images/
        minikube image load ${ARCH}/registry:2.8.2 gcr.io/google_containers/kube-registry-proxy:0.4-${ARCH}
        minikube addons enable registry --images="Registry=${ARCH}/registry:2.8.2,KubeRegistryProxy=google_containers/kube-registry-proxy:0.4-${ARCH}"
        rm -rf kubernetes
    elif [[ "$ARCH" = "ppc64le" ]]; then
        git clone -b v1.9.11 --depth 1 https://github.com/kubernetes/kubernetes.git
        sed -i 's/:1.11/:1.22.1/' kubernetes/cluster/addons/registry/images/Dockerfile
        docker build --pull -t gcr.io/google_containers/kube-registry-proxy:0.4-${ARCH} kubernetes/cluster/addons/registry/images/
        minikube image load ${ARCH}/registry:2.8.2 gcr.io/google_containers/kube-registry-proxy:0.4-${ARCH}
        minikube addons enable registry --images="Registry=${ARCH}/registry:2.8.0-beta.1,KubeRegistryProxy=google_containers/kube-registry-proxy:0.4-${ARCH}"
        rm -rf kubernetes
    elif [[ "$ARCH" = "arm64" ]]; then
        git clone -b v1.9.11 --depth 1 https://github.com/kubernetes/kubernetes.git
        sed -i 's/:1.11/:1.25.0/' kubernetes/cluster/addons/registry/images/Dockerfile
        minikube image build -t google_containers/kube-registry-proxy:0.5-SNAPSHOT kubernetes/cluster/addons/registry/images/
        minikube addons enable registry --images="Registry=arm64v8/registry:2.8.2,KubeRegistryProxy=google_containers/kube-registry-proxy:0.5-SNAPSHOT"
        rm -rf kubernetes
    else
        minikube addons enable registry
    fi

    minikube addons enable registry-aliases
}

function add_docker_hub_credentials_to_kubernetes {
    # Add Docker hub credentials to Minikube
    if [ "$COPY_DOCKER_LOGIN" = "true" ]
    then
      set +ex

      docker exec $1 bash -c "echo '$(cat $HOME/.docker/config.json)'| sudo tee -a /var/lib/kubelet/config.json > /dev/null && sudo systemctl restart kubelet"

      set -ex
    fi
}

setup_kube_directory
install_kubectl
install_kubernetes_provisioner

if [ "$TEST_CLUSTER" = "minikube" ]; then
    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true

    docker run -d -p 5000:5000 ${MINIKUBE_REGISTRY_IMAGE}

    export KUBECONFIG=$HOME/.kube/config
    # We can turn on network polices support by adding the following options --network-plugin=cni --cni=calico
    # We have to allow trafic for ITS when NPs are turned on
    # We can allow NP after Strimzi#4092 which should fix some issues on STs side
    minikube start --vm-driver=docker --kubernetes-version=${KUBE_VERSION} \
      --insecure-registry=localhost:5000 --extra-config=apiserver.authorization-mode=Node,RBAC \
      --cpus=${CLUSTER_CPU} --memory=${CLUSTER_MEMORY} --force

    if [ $? -ne 0 ]
    then
        echo "Minikube failed to start or RBAC could not be properly set up"
        exit 1
    fi

    minikube addons enable default-storageclass
    add_docker_hub_credentials_to_kubernetes "$TEST_CLUSTER"
    setup_registry_proxy
else
    echo "Unsupported TEST_CLUSTER '$TEST_CLUSTER'"
    exit 1
fi

create_cluster_role_binding_admin
label_node
