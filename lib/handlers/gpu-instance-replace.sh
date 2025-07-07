#!/bin/bash

# GPU实例重启脚本 - 包含测试和实际执行功能
# 支持函数调用和独立执行两种模式

set -euo pipefail

# 日志函数
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1" >&2
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" >&2
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARN: $1" >&2
}

# 实际重启函数
replace_gpu_instance() {
    local node_name="$1"
    local instance_id="$2"
    local wait_time="${3:-30}"
    
    log_info "开始替换GPU实例 - 节点: $node_name, 实例: $instance_id, 等待时间: ${wait_time}秒"
    
    # Cordon
    log_info "Start cordoning Node: ${node_name}"
    kubectl cordon ${node_name}
    if kubectl cordon "${node_name}"; then
        log_info "  - Node ${node_name} cordoned successfully"
    else
        log_warn "  - Failed to cordon node ${node_name}, manual intervention required"
        return 1
    fi

    log_info "开始安全驱逐节点上的Pod"
    # 首先尝试正常驱逐，给足够时间让Pod优雅关闭
    if kubectl drain "$node_name" --ignore-daemonsets  --grace-period=30 --delete-emptydir-data --timeout=600s; then
        log_info "节点驱逐成功"
    else
        log_warn "正常驱逐超时，尝试强制驱逐"
        # 如果正常驱逐失败，尝试强制驱逐
        if kubectl drain "$node_name" --ignore-daemonsets --delete-emptydir-data --force --timeout=300s; then
            log_info "强制驱逐成功"
        else
            log_error "驱逐失败，但继续执行重启操作"
        fi
    fi
    
    # 重启实例
    log_info "Replacing EC2 Node: "$node_name" | $instance_id"
    if ! aws autoscaling terminate-instance-in-auto-scaling-group --instance-id ${instance_id} --no-should-decrement-desired-capacity; then
        log_error "Replace Fail, Terminating"
        return 1
    fi
    

    log_info "Exit and Wait instance Replacing (${wait_time}sec)..."
    sleep ${wait_time}

}



# 主函数
main() {
    local mode="${1:-}"
    
    case "$mode" in
        "run")
            if [[ $# -lt 3 ]]; then
                echo "执行模式用法: $0 run <node-name> <instance-id> [wait-time]"
                exit 1
            fi
            replace_gpu_instance "$2" "$3" "$4"
            ;;
        *)
            echo "GPU-instance-replace.sh requires param to execute"
            exit 1
            ;;
    esac
}

# 直接执行检查
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi