#!/bin/bash
# GPU工具函数库

# 获取DCGM Pod和节点信息
get_dcgm_pods_with_nodes() {
    kubectl get pods -n kube-system -l k8s-app=dcgm-server -o json | jq -r '.items[] | "\(.metadata.name)|\(.spec.nodeName)"'
}

# 获取节点的Instance ID
get_instance_id() {
    local node_name=$1
    kubectl get node "$node_name" -o jsonpath='{.spec.providerID}' | sed 's/.*\///'
}

# 获取节点的详细信息
get_node_details() {
    local node_name=$1
    local instance_id=$(get_instance_id "$node_name")
    local instance_type=$(kubectl get node "$node_name" -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}')
    local zone=$(kubectl get node "$node_name" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
    
    echo "节点名称: $node_name"
    echo "Instance ID: $instance_id"
    echo "Instance类型: $instance_type"
    echo "可用区: $zone"
}

# 显示当前集群和节点信息
show_cluster_info() {
    log_info "显示集群信息..."
    
    echo "=== 集群上下文 ==="
    kubectl config current-context 2>/dev/null || echo "无法获取当前上下文"
    
    echo -e "\n=== 节点列表 ==="
    kubectl get nodes -o wide 2>/dev/null || echo "无法获取节点列表"
    
    echo -e "\n=== AWS身份信息 ==="
    aws sts get-caller-identity 2>/dev/null || echo "无法获取AWS身份信息"
}


query_dcgm_metrics_multi_gpu() {
    local pod=$1
    local node_name=$2
    
    local metrics=$(kubectl exec -n kube-system "$pod" -- bash -c "
        echo '============================================================================='
        echo '=== NODE_INFO ==='
        echo 'Node: $node_name'
        
        echo '=== GPU_COUNT ==='
        dcgmi discovery -l 2>/dev/null | head -1 | grep -o '[0-9]\+' | head -1
        
        echo '=== ECC_DATA ==='
        # 查询所有GPU的ECC错误
        dcgmi dmon -e 319 -c 1 2>/dev/null | grep -v '^#' | grep 'GPU'
        
        echo '=== XID_DATA ==='
        # 查询所有GPU的XID错误
        dcgmi dmon -e 230 -c 1 2>/dev/null | grep -v '^#' | grep 'GPU'
        
        echo '=== XID_DETAILS ==='
        # 获取系统日志中的XID详细信息
        dmesg | grep -i 'xid' | tail -20 2>/dev/null

        echo '=== HEALTH_DATA ==='
        # 查询所有GPU的健康状态
        dcgmi health -c 2>/dev/null
        
        echo '=== GPU_INFO ==='
        # 获取所有GPU的详细信息
        dcgmi discovery -l 2>/dev/null


    " 2>/dev/null)
    
    echo "$metrics"
}