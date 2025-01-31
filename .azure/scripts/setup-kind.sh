#!/usr/bin/env bash
set -xe

rm -rf ~/.kube

# There is a bug in 0.24.0 - https://github.com/kubernetes-sigs/kind/issues/3713
KIND_VERSION=${KIND_VERSION:-"v0.23.0"}
# To properly upgrade Kind version check the releases in github https://github.com/kubernetes-sigs/kind/releases and use proper image based on Kind version
KIND_NODE_IMAGE=${KIND_NODE_IMAGE:-"kindest/node:v1.25.16@sha256:5da57dfc290ac3599e775e63b8b6c49c0c85d3fec771cd7d55b45fae14b38d3b"}
COPY_DOCKER_LOGIN=${COPY_DOCKER_LOGIN:-"false"}
DOCKER_CMD="${DOCKER_CMD:-docker}"

KIND_CLUSTER_NAME="kind-cluster"

# note that IPv6 is only supported on kind (i.e., minikube does not support it). Also we assume that when you set this flag
# to true then you meet requirements (i.) net.ipv6.conf.all.disable_ipv6 = 0 (ii. you have installed CNI supporting IPv6)
IP_FAMILY=${IP_FAMILY:-"ipv4"}

ARCH=$1
if [ -z "$ARCH" ]; then
    ARCH="amd64"
fi

function is_docker() {
    [[ "$DOCKER_CMD" == "docker" ]]
}

function is_podman() {
    [[ "$DOCKER_CMD" == "podman" ]]
}

function install_kubectl {
    if [ "${TEST_KUBECTL_VERSION:-latest}" = "latest" ]; then
        TEST_KUBECTL_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    fi
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${TEST_KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl && chmod +x kubectl
    sudo cp kubectl /usr/local/bin

    sudo ln -s /usr/local/bin/kubectl /usr/bin/kubectl
}

function label_node {
	# It should work for all clusters
	for nodeName in $($SUDO kubectl get nodes -o custom-columns=:.metadata.name --no-headers);
	do
		echo ${nodeName};
		$SUDO kubectl label node ${nodeName} rack-key=zone;
	done
}

function install_kubernetes_provisioner {

    KIND_URL=https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-${ARCH}

    curl -Lo kind ${KIND_URL} && chmod +x kind

    # Move the binary to a globally accessible location
    sudo mv kind /usr/local/bin/kind
    sudo ln -s /usr/local/bin/kind /usr/bin/kind

    if command -v kind >/dev/null 2>&1; then
        echo "Kind installed successfully at $(command -v kind)"
        kind version
    else
        echo "Error: Kind installation failed."
        exit 1
    fi
}

function create_cluster_role_binding_admin {
    $SUDO kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=kube-system:default
}

: '
@brief: Set up Kubernetes configuration directory and file.
@note: Ensures $HOME/.kube directory and $HOME/.kube/config file exist.
'
function setup_kube_directory {
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config
}

: '
@brief: Fixes the "ip6_tables" issue for Podman.
@note: Ensures the required kernel modules are loaded.
'
function load_ip6_tables_module_to_kernel {
    if is_podman; then
        echo "Ensuring ip6_tables kernel module is loaded..."
        if ! lsmod | grep -q ip6_tables; then
            sudo modprobe ip6_tables || {
                echo "Error: Failed to load ip6_tables module. Ensure your kernel supports it."
                exit 1
            }
        fi
    fi
}

: '
@brief: Updates container runtime (Docker/Podman) configurations to allow insecure local registries.
@param:
        1) registry_address - IPv4 registry address, e.g., "192.168.0.2:5000"
        2) ula_fixed_ipv6 - Unique local address prefix for IPv6-based setups
        3) reg_port - Port where the registry is running
        4) registry_dns - DNS name for registry
@global:
        IP_FAMILY - Determines whether to configure IPv4 or IPv6 settings
@note: Adjusts the container runtime’s config files (/etc/docker/daemon.json
       or /etc/containers/registries.conf) and restarts the respective service.
'
function updateContainerRuntimeConfiguration {
    local registry_address="$1"
    local ula_fixed_ipv6="$2"
    local reg_port="$3"
    local registry_dns="$4"

    local docker_config podman_config

    # Build configurations depending on IP family
    if [[ "$IP_FAMILY" == "ipv6" ]]; then
        docker_config=$(cat <<EOF
{
  "insecure-registries": ["[${ula_fixed_ipv6}::1]:${reg_port}", "${registry_dns}:${reg_port}"],
  "experimental": true,
  "ip6tables": true,
  "fixed-cidr-v6": "${ula_fixed_ipv6}::/80"
}
EOF
)
        podman_config=$(cat <<EOF
[registries.insecure]
registries = ["[${ula_fixed_ipv6}::1]:${reg_port}", "${registry_dns}:${reg_port}"]
EOF
)
    else
        docker_config=$(cat <<EOF
{
  "insecure-registries": ["${registry_address}"]
}
EOF
)
        podman_config=$(cat <<EOF
[[registry]]
location = "${registry_address}"
insecure = true
EOF
)
    fi

    if is_docker; then
        echo "$docker_config" | sudo tee /etc/docker/daemon.json
        sudo systemctl restart docker
    else
        # Ensure the podman_config is inserted correctly after unqualified-search-registries
        sudo awk -v config="$podman_config" '
        BEGIN { inserted = 0 }
        /unqualified-search-registries/ && !inserted {
            print $0
            print config
            inserted = 1
            next
        }
        { print $0 }
        ' /etc/containers/registries.conf > /tmp/registries.conf

        sudo mv /tmp/registries.conf /etc/containers/registries.conf
        sudo systemctl restart podman
    fi
}

: '
@brief: Increases the inotify user watches and user instances limits on a Linux system.
@param: None.
@global: None.
@note: Inotify is a Linux subsystem used for file system event notifications. This function
       helps adjust the limits for applications or services that monitor a large number
       of files or directories.
       This is specifically needed for multi-node control plane cluster
       https://github.com/kubernetes-sigs/kind/issues/2744#issuecomment-1127808069
'
function adjust_inotify_limits {
    # Increase the inotify user watches limit
    echo "Setting fs.inotify.max_user_watches to 655360..."
    echo fs.inotify.max_user_watches=655360 | sudo tee -a /etc/sysctl.conf

    # Increase the inotify user instances limit
    echo "Setting fs.inotify.max_user_instances to 1280..."
    echo fs.inotify.max_user_instances=1280 | sudo tee -a /etc/sysctl.conf

    # Reload the system configuration settings
    echo "Reloading sysctl settings..."
    sudo sysctl -p

    echo "Inotify limits adjusted successfully."
}

: '
@brief: Configures Docker or Podman networking for the KIND cluster.
@param:
        1) registry_name - Name of the local registry container.
        2) network_name - Name of the KIND network.
@global:
        DOCKER_CMD - container runtime CLI.
@note:  None.
'
function configure_container_runtime_networking {
    local registry_name="$1"
    local network_name="$2"

    if [ "$($SUDO $DOCKER_CMD inspect -f='{{.NetworkSettings.Networks.kind}}' "${registry_name}" 2>/dev/null)" = '<nil>' ]; then
        if is_podman; then
            if ! $SUDO $DOCKER_CMD network exists "${network_name}"; then
                $SUDO $DOCKER_CMD network create "${network_name}"
            fi
            $SUDO $DOCKER_CMD network connect "${network_name}" "${registry_name}"
        else
            $SUDO $DOCKER_CMD network connect "${network_name}" "${registry_name}"
        fi
    fi
}

: '
@brief: Creates a KIND cluster configuration and provisions the cluster.
@param:
        1) control_planes - Number of control-plane nodes.
@global:
        KIND_CLUSTER_NAME - Name of the KIND cluster.
        KIND_NODE_IMAGE   - KIND node image to use.
        IP_FAMILY         - Determines whether to configure IPv4 or IPv6 settings
        SUDO              - Start rootful or rootless kind
@note: Writes the cluster configuration to a temporary file before creating the cluster.
'
function create_kind_cluster {
    local control_planes="$1"

# Start the cluster configuration
    cat <<EOF > /tmp/kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
EOF

    # Add control-plane nodes
    for i in $(seq 1 "$control_planes"); do
        echo "    - role: control-plane" >> /tmp/kind-config.yaml
    done

    # Add worker nodes
    cat <<EOF >> /tmp/kind-config.yaml
    - role: worker
    - role: worker
    - role: worker
EOF

    # Add specific containerd configuration for IPv4/IPv6
    if [[ "$IP_FAMILY" == "ipv6" ]]; then
        cat <<EOF >> /tmp/kind-config.yaml
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."myregistry.local:5001"]
      endpoint = ["http://myregistry.local:5001"]
EOF
    else
        cat <<EOF >> /tmp/kind-config.yaml
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "/etc/containerd/certs.d"
EOF
    fi

    # Add networking configuration
    cat <<EOF >> /tmp/kind-config.yaml
networking:
  ipFamily: $IP_FAMILY
EOF


    # Create the KIND cluster
    $SUDO kind create cluster \
        --image "$KIND_NODE_IMAGE" \
        --name "$KIND_CLUSTER_NAME" \
        --config=/tmp/kind-config.yaml

    echo "KIND cluster '${KIND_CLUSTER_NAME}' created successfully with IP family '${IP_FAMILY}'."
}

# if podman we use rootful (otherwise rootless)
SUDO=$(is_podman && echo "sudo" || echo "")

setup_kube_directory
install_kubectl
install_kubernetes_provisioner
adjust_inotify_limits
load_ip6_tables_module_to_kernel

reg_name='kind-registry'
reg_port='5001'
# by default using podman we have to use single control-plane because of https://github.com/kubernetes-sigs/kind/issues/2858
control_planes=1

if is_docker; then
    control_planes=3
fi

if [[ "$IP_FAMILY" = "ipv4" || "$IP_FAMILY" = "dual" ]]; then
    hostname=$(hostname --ip-address | grep -oE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' | awk '$1 != "127.0.0.1" { print $1 }' | head -1)

    # update insecure registries
    updateContainerRuntimeConfiguration "${hostname}:${reg_port}"

    # Create kind cluster with containerd registry config dir enabled
    # TODO: kind will eventually enable this by default and this patch will
    # be unnecessary.
    #
    # See:
    # https://github.com/kubernetes-sigs/kind/issues/2875
    # https://github.com/containerd/containerd/blob/main/docs/cri/config.md#registry-configuration
    # See: https://github.com/containerd/containerd/blob/main/docs/hosts.md
    create_kind_cluster ${control_planes}
    # run local container registry
    if [ "$($SUDO $DOCKER_CMD inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        $SUDO $DOCKER_CMD run \
          -d --restart=always -p "${hostname}:${reg_port}:5000" --name "${reg_name}" \
          registry:2
    fi

    # Add the registry config to the nodes
    #
    # This is necessary because localhost resolves to loopback addresses that are
    # network-namespace local.
    # In other words: localhost in the container is not localhost on the host.
    #
    # We want a consistent name that works from both ends, so we tell containerd to
    # alias localhost:${reg_port} to the registry container when pulling images
    # note: kind get nodes (default name `kind` and with specifying new name we have to use --name <cluster-name>
    # See https://kind.sigs.k8s.io/docs/user/local-registry/
    REGISTRY_DIR="/etc/containerd/certs.d/${hostname}:${reg_port}"

    for node in $($SUDO kind get nodes --name "${KIND_CLUSTER_NAME}"); do
        echo "Executing command in node:${node}"
        $SUDO $DOCKER_CMD exec "${node}" mkdir -p "${REGISTRY_DIR}"
        cat <<EOF | $SUDO $DOCKER_CMD exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
    [host."http://${reg_name}:5000"]
EOF
    done

elif [[ "$IP_FAMILY" = "ipv6" ]]; then
    # for ipv6 configuration
    ula_fixed_ipv6="fd01:2345:6789"
    registry_dns="myregistry.local"

    # manually assign an IPv6 address to eth0 interface
    sudo ip -6 addr add "${ula_fixed_ipv6}"::1/64 dev eth0

     # use ULA (i.e., Unique Local Address), which offers a similar "private" scope as link-local
    # but without the interface dependency and some of the other challenges of link-local addresses.
    # (link-local starts as fe80::) but we will use ULA fd01
    updateContainerRuntimeConfiguration "" "${ula_fixed_ipv6}" "${reg_port}" "${registry_dns}"
    create_kind_cluster ${control_planes}

    # run local container registry
    if [ "$($SUDO $DOCKER_CMD inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        $SUDO $DOCKER_CMD run \
          -d --restart=always -p "[${ula_fixed_ipv6}::1]:${reg_port}:5000" --name "${reg_name}" \
          registry:2
    fi
    # we need to also make a DNS record for docker tag because it seems that such version does not support []:: format
    echo "${ula_fixed_ipv6}::1    ${registry_dns}" >> /etc/hosts

    # note: kind get nodes (default name `kind` and with specifying new name we have to use --name <cluster-name>
    # See https://kind.sigs.k8s.io/docs/user/local-registry/
    for node in $($SUDO kind get nodes --name "${KIND_CLUSTER_NAME}"); do
        echo "Executing command in node:${node}"
        cat <<EOF | $SUDO $DOCKER_CMD exec -i "${node}" cp /dev/stdin "/etc/hosts"
${ula_fixed_ipv6}::1    ${registry_dns}
EOF
    done
fi

configure_container_runtime_networking "${reg_name}" "${network_name}"
create_cluster_role_binding_admin
label_node
