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
reboot_gpu_instance() {
    local node_name="$1"
    local instance_id="$2"
    local wait_time="${3:-300}"
    
    log_info "开始重启GPU实例 - 节点: $node_name, 实例: $instance_id, 等待时间: ${wait_time}秒"
    
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
        if kubectl drain "$node_name" --ignore-daemonsets --delete-emptydir-data --force --timeout=120s; then
            log_info "强制驱逐成功"
        else
            log_error "驱逐失败，但继续执行重启操作"
        fi
    fi
    
    # 重启实例
    log_info "重启EC2实例: $instance_id"
    if ! aws ec2 reboot-instances --instance-ids "$instance_id"; then
        log_error "重启命令失败"
        return 1
    fi
    
    # 等待实例重启完成
    log_info "等待实例重启完成 (${wait_time}秒)..."
    sleep ${wait_time}

    # 恢复调度 - drain操作会自动cordon节点，重启后需要uncordon
    log_info "Recovering orchestraion for Node: $node_name"
    if kubectl uncordon "$node_name"; then
        log_info "  - uncordon Success, Wait ${wait_time} sec for pod re-scheduling"
        sleep ${wait_time}
    else
        log_warn "  - Recover Failed and Need manually process"
        return 1
    fi
    
    # 最终状态检查
    local instance_state
    instance_state=$(aws ec2 describe-instances --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo "unknown")
    echo "instance_state - $instance_state"

    local instance_ready
    instance_ready=$(kubectl get node "$node_name" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "Unknown")
    echo "instance_ready - $instance_ready"

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
            reboot_gpu_instance "$2" "$3" "$4"
            ;;
        *)
            echo "GPU-instance-reboot.sh requires param to execute"
            exit 1
            ;;
    esac
}

# 直接执行检查
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi