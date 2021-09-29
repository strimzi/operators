#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
    shopt -s expand_aliases
    alias rm="grm"
    alias echo="gecho"
    alias dirname="gdirname"
    alias grep="ggrep"
    alias readlink="greadlink"
    alias tar="gtar"
    alias sed="gsed"
    alias date="gdate"
    alias ls="gls"
fi
__HOME="" && pushd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" >/dev/null \
    && { __HOME=$PWD; popd >/dev/null; } && readonly __HOME
trap __cleanup EXIT

RSYNC_POD_NAME=""
WAIT_TIMEOUT_SEC=300
ZK_REPLICAS=0
ZK_PVC=()
ZK_PVC_SIZE=""
ZK_PVC_CLASS=""
KAFKA_REPLICAS=0
KAFKA_PVC=()
KAFKA_PVC_SIZE=""
KAFKA_PVC_CLASS=""
COMMAND=""
CLEANUP=true
CONFIRM=true
NAMESPACE=""
CLUSTER_NAME=""
TARGET_FILE=""
SOURCE_FILE=""
BACKUP_PATH=""
BACKUP_NAME=""
CUSTOM_CM=""
CUSTOM_SE=""

__start_cluster() {
    if [[ -n $NAMESPACE && -n $CLUSTER_NAME ]]; then
        echo "Starting cluster $CLUSTER_NAME"
        if [[ $COMMAND == "backup" && -n $ZK_REPLICAS && -n $KAFKA_REPLICAS ]]; then
            local zoo_ss="$(kubectl -n $NAMESPACE get statefulset $CLUSTER_NAME-zookeeper -o name --ignore-not-found)"
            if [[ -n $zoo_ss ]]; then
                kubectl -n $NAMESPACE scale $zoo_ss --replicas $ZK_REPLICAS
                __wait_for condition=ready pod strimzi.io/name=$CLUSTER_NAME-zookeeper
            fi
            local kafka_ss="$(kubectl -n $NAMESPACE get statefulset $CLUSTER_NAME-kafka -o name --ignore-not-found)"
            if [[ -n $kafka_ss ]]; then
                kubectl -n $NAMESPACE scale $kafka_ss --replicas $KAFKA_REPLICAS
                __wait_for condition=ready pod strimzi.io/name=$CLUSTER_NAME-kafka
            fi
            local eo_deploy="$(kubectl -n $NAMESPACE get deploy $CLUSTER_NAME-entity-operator -o name --ignore-not-found)"
            if [[ -n $eo_deploy ]]; then
                kubectl -n $NAMESPACE scale $eo_deploy --replicas 1
                __wait_for condition=ready pod strimzi.io/name=$CLUSTER_NAME-entity-operator
            fi
            local ke_deploy="$(kubectl -n $NAMESPACE get deploy $CLUSTER_NAME-kafka-exporter -o name --ignore-not-found)"
            if [[ -n $ke_deploy ]]; then
                kubectl -n $NAMESPACE scale $ke_deploy --replicas 1
                __wait_for condition=ready pod strimzi.io/name=$CLUSTER_NAME-kafka-exporter
            fi
        fi
        if [[ $COMMAND == "backup" || $COMMAND == "restore" ]]; then
            local co_deploy="$(kubectl -n $NAMESPACE get deploy strimzi-cluster-operator -o name --ignore-not-found)"
            if [[ -n $co_deploy ]]; then
                kubectl -n $NAMESPACE scale $co_deploy --replicas 1
                __wait_for condition=ready pod strimzi.io/kind=cluster-operator
            fi
            if [[ $COMMAND == "restore" ]]; then
                local i=0
                local wait_cmd="__wait_for condition=ready pod strimzi.io/name=$CLUSTER_NAME-kafka"
                echo "Waiting for Kafka to be ready"
                while [[ ! $($wait_cmd) && $i -lt $WAIT_TIMEOUT_SEC ]]; do
                    i=$((i+1))
                    sleep 1
                done
                $wait_cmd
            fi
        fi
    fi
}

__stop_cluster() {
    if [[ -n $NAMESPACE && -n $CLUSTER_NAME ]]; then
        echo "Stopping cluster $CLUSTER_NAME"
        local co_deploy="$(kubectl -n $NAMESPACE get deploy strimzi-cluster-operator -o name --ignore-not-found)"
        if [[ -n $co_deploy ]]; then
            kubectl -n $NAMESPACE scale $co_deploy --replicas 0
            __wait_for delete pod strimzi.io/kind=cluster-operator
        else
            echo "No local operator found, make sure no cluster-wide operator is watching this namespace"
            __confirm
        fi
        local eo_deploy="$(kubectl -n $NAMESPACE get deploy $CLUSTER_NAME-entity-operator -o name --ignore-not-found)"
        if [[ -n $eo_deploy ]]; then
            kubectl -n $NAMESPACE scale $eo_deploy --replicas 0
            __wait_for delete pod strimzi.io/name=$CLUSTER_NAME-entity-operator
        fi
        local ke_deploy="$(kubectl -n $NAMESPACE get deploy $CLUSTER_NAME-kafka-exporter -o name --ignore-not-found)"
        if [[ -n $ke_deploy ]]; then
            kubectl -n $NAMESPACE scale $ke_deploy --replicas 0
            __wait_for delete pod strimzi.io/name=$CLUSTER_NAME-kafka-exporter
        fi
        local kafka_ss="$(kubectl -n $NAMESPACE get statefulset $CLUSTER_NAME-kafka -o name --ignore-not-found)"
        if [[ -n $kafka_ss ]]; then
            kubectl -n $NAMESPACE scale $kafka_ss --replicas 0
            __wait_for delete pod strimzi.io/name=$CLUSTER_NAME-kafka
        fi
        local zoo_ss="$(kubectl -n $NAMESPACE get statefulset $CLUSTER_NAME-zookeeper -o name --ignore-not-found)"
        if [[ -n $zoo_ss ]]; then
            kubectl -n $NAMESPACE scale $zoo_ss --replicas 0
            __wait_for delete pod strimzi.io/name=$CLUSTER_NAME-zookeeper
        fi
    fi
}

__cleanup() {
    if [[ -n $COMMAND && $CLEANUP == true ]]; then
        kubectl -n $NAMESPACE delete pod $RSYNC_POD_NAME 2>/dev/null ||true
        __start_cluster
    fi
}

__error() {
    echo "$@" 1>&2
    exit 1
}

__confirm() {
    read -p "Please confirm (y/n) " reply
    if [[ ! $reply =~ ^[Yy]$ ]]; then
        CLEANUP=false
        exit 0
    fi
}

__check_kube_conn() {
    kubectl version --request-timeout=10s 1>/dev/null
}

__wait_for() {
    local condition="$1"
    local type="$2"
    local label="$3"
    if [[ -n $condition && -n $type && -n $label ]]; then
        local resource=$(kubectl -n $NAMESPACE get $type -l $label -o name --ignore-not-found)
        if [[ -n $resource ]]; then
            echo "Waiting for $condition of $type with label $label"
            kubectl -n $NAMESPACE wait $type -l $label --for="$condition" --timeout="${WAIT_TIMEOUT_SEC}s"
        fi
    else
        __error "Missing required parameters"
    fi
}

__export_env() {
    local dest_path="$1"
    if [[ -n $dest_path ]]; then
        echo "Exporting environment"
        local zk_label="strimzi.io/name=$CLUSTER_NAME-zookeeper"
        ZK_REPLICAS=$(kubectl -n $NAMESPACE get kafka $CLUSTER_NAME -o yaml | yq eval ".spec.zookeeper.replicas" -)
        mapfile -t ZK_PVC < <(kubectl -n $NAMESPACE get pvc -l $zk_label -o yaml | yq eval ".items[].metadata.name" -)
        ZK_PVC_SIZE=$(kubectl -n $NAMESPACE get pvc -l $zk_label -o yaml | yq eval ".items[0].spec.resources.requests.storage" -)
        ZK_PVC_CLASS=$(kubectl -n $NAMESPACE get pvc -l $zk_label -o yaml | yq eval ".items[0].spec.storageClassName" -)
        local kafka_label="strimzi.io/name=$CLUSTER_NAME-kafka"
        KAFKA_REPLICAS=$(kubectl -n $NAMESPACE get kafka $CLUSTER_NAME -o yaml | yq eval ".spec.kafka.replicas" -)
        mapfile -t KAFKA_PVC < <(kubectl -n $NAMESPACE get pvc -l $kafka_label -o yaml | yq eval ".items[].metadata.name" -)
        KAFKA_PVC_SIZE=$(kubectl -n $NAMESPACE get pvc -l $kafka_label -o yaml | yq eval ".items[0].spec.resources.requests.storage" -)
        KAFKA_PVC_CLASS=$(kubectl -n $NAMESPACE get pvc -l $kafka_label -o yaml | yq eval ".items[0].spec.storageClassName" -)
        declare -px ZK_REPLICAS ZK_PVC ZK_PVC_SIZE ZK_PVC_CLASS KAFKA_REPLICAS KAFKA_PVC KAFKA_PVC_SIZE KAFKA_PVC_CLASS > "$dest_path"
    else
        __error "Missing required parameters"
    fi
}

__export_res() {
    local res_type="$1"
    local res_id="$2"
    local dest_path="$3"
    if [[ -n $res_type && -n $res_id && -n $dest_path ]]; then
        local resources=$(kubectl -n $NAMESPACE get $res_type -l $res_id -o name --ignore-not-found ||true)
        if [[ -z $resources ]]; then
            resources=$(kubectl -n $NAMESPACE get $res_type $res_id -o name --ignore-not-found 2>/dev/null ||true)
        fi
        if [[ -n $resources ]]; then
            echo "Exporting $res_type $res_id"
            # delete runtime metadata expression
            local exp="del(.metadata.namespace, .items[].metadata.namespace, \
                .metadata.resourceVersion, .items[].metadata.resourceVersion, \
                .metadata.selfLink, .items[].metadata.selfLink, \
                .metadata.uid, .items[].metadata.uid, \
                .status, .items[].status)"
            local file_name=$(printf $res_type-$res_id | sed 's/\//-/g;s/ //g')
            kubectl -n $NAMESPACE get $resources -o yaml | yq eval "$exp" - > $dest_path/$file_name.yaml
        fi
    else
        __error "Missing required parameters"
    fi
}

__rsync() {
    local source="$1"
    local target="$2"
    if [[ -n $source && -n $target ]]; then
        echo "Rsync from $source to $target"
        local flags="--no-check-device --no-acls --no-xattrs --no-same-owner --warning=no-file-changed"
        if [[ $source != "$BACKUP_PATH/"* ]]; then
            # download from pod to local (backup)
            flags="$flags --exclude=data/version-2/{currentEpoch,acceptedEpoch}"
            local patch=$(sed "s@\$name@$source@g" $__HOME/templates/patch.json)
            kubectl -n $NAMESPACE run $RSYNC_POD_NAME --image "dummy" --restart "Never" --overrides "$patch"
            __wait_for condition=ready pod run="$RSYNC_POD_NAME"
            # ignore the sporadic file changed error
            kubectl -n $NAMESPACE exec -i $RSYNC_POD_NAME -- sh -c "tar $flags -C /data -c ." \
              | tar $flags -C $target -xv -f - && if [[ $? == 1 ]]; then exit 0; fi
            kubectl -n $NAMESPACE delete pod $RSYNC_POD_NAME
        else
            # upload from local to pod (restore)
            local patch=$(sed "s@\$name@$target@g" $__HOME/templates/patch.json)
            kubectl -n $NAMESPACE run $RSYNC_POD_NAME --image "dummy" --restart "Never" --overrides "$patch"
            __wait_for condition=ready pod run="$RSYNC_POD_NAME"
            tar $flags -C $source -c . \
              | kubectl -n $NAMESPACE exec -i $RSYNC_POD_NAME -- sh -c "tar $flags -C /data -xv -f -"
            kubectl -n $NAMESPACE delete pod $RSYNC_POD_NAME
        fi
    else
        __error "Missing required parameters"
    fi
}

__create_pvc() {
    local name="$1"
    local size="$2"
    local class="${3-}"
    if [[ -n $name && -n $size ]]; then
        local exp="s/\$name/$name/g; s/\$size/$size/g; /storageClassName/d"
        if [[ $class != "null" ]]; then
            exp="s/\$name/$name/g; s/\$size/$size/g; s/\$class/$class/g"
        fi
        echo "Creating pvc $name of size $size"
        sed "$exp" $__HOME/templates/pvc.yaml | kubectl -n $NAMESPACE create -f -
    else
        __error "Missing required parameters"
    fi
}

__compress() {
    local source_dir="$1"
    local target_file="$2"
    if [[ -n $source_dir && -n $target_file ]]; then
        local current_dir=$(pwd)
        cd $source_dir
        echo "Compressing $source_dir to $target_file"
        zip -FSqr $BACKUP_NAME *
        mv $BACKUP_NAME $BACKUP_PATH
        cd $current_dir
    else
        __error "Missing required parameters"
    fi
}

__uncompress() {
    local source_file="$1"
    local target_dir="$2"
    if [[ -n $source_file && -n $target_dir ]]; then
        echo "Uncompressing $source_file to $target_dir"
        mkdir -p $target_dir
        unzip -qo $source_file -d $target_dir
        chmod -R ugo+rwx $target_dir
    else
        __error "Missing required parameters"
    fi
}

backup() {
    if [[ -n $NAMESPACE && -n $CLUSTER_NAME && -n $TARGET_FILE ]]; then
        if [[ -d "$(dirname $TARGET_FILE)" ]]; then
            # init context
            readonly RSYNC_POD_NAME="$CLUSTER_NAME-backup"
            local tmp="$BACKUP_PATH/$NAMESPACE/$CLUSTER_NAME"
            if [[ ! -z "$(ls -A $tmp)" ]]; then
              CLEANUP=false
              __error "Non empty directory: $tmp"
            fi
            __check_kube_conn
            if [[ $CONFIRM == true ]]; then
                echo "Backup of cluster $NAMESPACE/$CLUSTER_NAME"
                echo "The cluster won't be available for the entire duration of the process"
                __confirm
            fi
            echo "Starting backup"
            mkdir -p $tmp/resources $tmp/data
            __export_env $tmp/env
            __stop_cluster

            # export resources
            __export_res kafka $CLUSTER_NAME $tmp/resources
            __export_res kafkatopics strimzi.io/cluster=$CLUSTER_NAME $tmp/resources
            __export_res kafkausers strimzi.io/cluster=$CLUSTER_NAME $tmp/resources
            __export_res kafkarebalances strimzi.io/cluster=$CLUSTER_NAME $tmp/resources
            # cluster configmap and secrets
            __export_res configmaps app.kubernetes.io/instance=$CLUSTER_NAME $tmp/resources
            __export_res secrets app.kubernetes.io/instance=$CLUSTER_NAME $tmp/resources
            # custom configmap and secrets
            if [[ -n $CUSTOM_CM ]]; then
                for name in $(printf $CUSTOM_CM | sed "s/,/ /g"); do
                    __export_res configmap $name $tmp/resources
                done
            fi
            if [[ -n $CUSTOM_SE ]]; then
                for name in $(printf $CUSTOM_SE | sed "s/,/ /g"); do
                    __export_res secret $name $tmp/resources
                done
            fi

            # for each PVC, rsync data from PV to backup
            for name in "${ZK_PVC[@]}"; do
                local path="$tmp/data/$name"
                mkdir -p $path
                __rsync $name $path
            done
            for name in "${KAFKA_PVC[@]}"; do
                local path="$tmp/data/$name"
                mkdir -p $path
                __rsync $name $path
            done

            # create the archive
            __compress $tmp $TARGET_FILE
        else
            __error "$(dirname $TARGET_FILE) not found"
        fi
    else
        CLEANUP=false
        __error "Specify namespace, cluster name and target file to backup"
    fi
}

restore() {
    if  [[ -n $NAMESPACE && -n $CLUSTER_NAME && -f $SOURCE_FILE ]]; then
        if [[ -f $SOURCE_FILE ]]; then
            # init context
            readonly RSYNC_POD_NAME="$CLUSTER_NAME-restore"
            local tmp="$BACKUP_PATH/$NAMESPACE/$CLUSTER_NAME"
            __check_kube_conn
            if [[ $CONFIRM == true ]]; then
                echo "Restore of cluster $NAMESPACE/$CLUSTER_NAME"
                __confirm
            fi
            __uncompress $SOURCE_FILE $tmp
            source $tmp/env
            __stop_cluster

            # for each PVC, create it and rsync data from backup to PV
            for name in "${ZK_PVC[@]}"; do
                __create_pvc $name $ZK_PVC_SIZE $ZK_PVC_CLASS
                __rsync $tmp/data/$name/. $name
            done
            for name in "${KAFKA_PVC[@]}"; do
                __create_pvc $name $KAFKA_PVC_SIZE $KAFKA_PVC_CLASS
                __rsync $tmp/data/$name/. $name
            done

            # import resources
            # KafkaTopic resources must be created *before*
            # deploying the Topic Operator or it will delete them
            kubectl -n $NAMESPACE apply -f $tmp/resources
        else
            __error "$SOURCE_FILE file not found"
        fi
    else
        CLEANUP=false
        __error "Specify namespace, cluster name and source file to restore"
    fi
}

readonly USAGE="
Usage: $0 [commands] [options]

Commands:
  backup   Cluster backup
  restore  Cluster restore

Options:
  -y  Skip confirmation step
  -n  Cluster namespace
  -c  Cluster name
  -t  Target file path
  -s  Source file path
  -m  Custom config maps (-m cm0,cm1,cm2)
  -x  Custom secrets (-x se0,se1,se2)

Examples:
  # Full backup with custom cm and secrets
  $0 backup -n test -c my-cluster \\
    -t /tmp/my-cluster.zip \\
    -m kafka-metrics,log4j-properties \\
    -x ext-listener-crt

  # Restore to a new namespace
  $0 restore -n test-new -c my-cluster \\
    -s /tmp/my-cluster.zip
"
readonly OPTIONS="${@}"
readonly ARGUMENTS=($OPTIONS)
i=0
for argument in $OPTIONS; do
    i=$(($i+1))
    case $argument in
        -y)
            export CONFIRM=false
            readonly CONFIRM
            ;;
        -n)
            export NAMESPACE=${ARGUMENTS[i]}
            readonly NAMESPACE
            ;;
        -c)
            export CLUSTER_NAME=${ARGUMENTS[i]}
            readonly CLUSTER_NAME
            ;;
        -t)
            export TARGET_FILE=${ARGUMENTS[i]}
            readonly TARGET_FILE
            export BACKUP_PATH=$(dirname $TARGET_FILE)
            readonly BACKUP_PATH
            export BACKUP_NAME=$(basename $TARGET_FILE)
            readonly BACKUP_NAME
            ;;
        -s)
            export SOURCE_FILE=${ARGUMENTS[i]}
            readonly SOURCE_FILE
            export BACKUP_PATH=$(dirname $SOURCE_FILE)
            readonly BACKUP_PATH
            export BACKUP_NAME=$(basename $SOURCE_FILE)
            readonly BACKUP_NAME
            ;;
        -m)
            export CUSTOM_CM=${ARGUMENTS[i]}
            readonly CUSTOM_CM
            ;;
        -x)
            export CUSTOM_SE=${ARGUMENTS[i]}
            readonly CUSTOM_SE
            ;;
    esac
done
readonly COMMAND="${1-}"
case "$COMMAND" in
    backup)
        backup
        ;;
    restore)
        restore
        ;;
    *)
        __error "$USAGE"
        ;;
esac
