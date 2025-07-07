# 250701-eks-gpu-cluster-resilience


## 概述

基于 EKS 构建的 GPU 集群 Resilience 示例方案，通过 DCGM Daemonset 进行监控信息持续获取，利用 Claude 3.7 进行日志分析及分类，并通过Kubeflow实现任务自动恢复。同时可按需接入通知及告警机制。

## 系统架构

### 1. AWS基础设施
- **EKS集群**: 运行GPU工作负载的托管Kubernetes集群
- **CloudWatch**: 指标收集和日志管理
- **Amazon Bedrock Claude 3.7**: LLM智能分析服务
- **FSx for Lustre**: 高速共享文件存储
- **（Optional）Lambda函数**: 云端错误处理逻辑
- **（Optional）SNS**: 通知服务


### 2. GPU监控
- **DCGM Server Pods**: 节点DaemonSet，提供GPU指标及错误日志
- **DCGM Monitor**: 主监控脚本，定期收集和分析GPU指标
- **LLM指标处理器**: 使用Bedrock Claude分析及分类错误类型，避免复杂正则匹配
- **排除管理器**: 防止重复处理同一故障节点
- **GPU Utils**: 获取节点信息及DCGM Server Pod Metric

### 3. 错误检测和分类层
系统能够智能识别和分类以下GPU错误类型：
- **XID_CRITICAL_<xid_code>**: 严重的GPU错误，需要立即替换实例
- **XID_WARNING_<xid_code>**: GPU警告，需要监控但可能不需要立即处理
- **ECC_ERROR**: GPU ECC内存错误
- **GPU_HEALTH_WARNING/ERROR**: GPU整体健康状态异常
- **HEALTHY**: GPU状态正常

### 4. 故障处理层
根据错误类型执行相应的恢复策略：
- **节点隔离 (Cordon)**: 防止新Pod调度到故障节点
- **Pod驱逐 (Drain)**: 安全地将Pod迁移到其他节点
- **实例重启 (Reboot)**: 对于可恢复的错误进行重启
- **实例替换 (Replace)**: 对于严重硬件故障进行实例替换
- **恢复隔离 (UnCordon)**: 恢复调度到已修复的节点

### 5. 通知和反馈
- **SNS主题通知**: (Template)可发送结构化通知消息
- **Webhook通知**: (Template)可集成三方通知系统
- **CloudWatch Metric**: 分节点及错误类型记录异常信息，可配置报警


## 监控及处理流程
​```mermaid
flowchart TD
    A[监控开始] --> A1{检查实例排除列表}
    subgraph 异常监控
    A1 --> |已排除（处理中）| B0[继续监控记录日志]
    A1 --> |不在排除列表| B[从DCGM Server收集监控指标]
    B --> C{LLM日志分析
    （自定义异常分类）}
    end
    
    C -->|HEALTHY| F[继续监控记录日志]
    
    C -->|OTHERS| G{是否严重错误?}
    G -->|否| H[上报CloudWatch
    + 继续监控]
        
    G -->|是| L[上报CloudWatch
    + 执行处理策略]
    subgraph 故障处理
    L --> O[节点隔离]
    O --> P[Pod驱逐]
    
    P --> Q{需要替换?}
    Q -->|是| R[实例替换]
    Q -->|否| S[实例重启]
    end

    R --> A
    S --> A

    style 异常监控 font-size:16px,font-weight:bold
    style 故障处理 fill:#ffcccc,stroke:#ff0000,stroke-width:2px,color:#333,font-size:16px,font-weight:bold
​```


## 配置和部署

### 部署步骤
1. 在已有EKS集群配置DCGM DaemonSet
2. 部署示例方案至EC2或Fargate

### 监控指标（可定制）
- GPU ECC错误计数
- GPU XID错误计数和类型
- GPU健康状态

### 环境变量
```bash
export MONITOR_INTERVAL=300         # 监控间隔(秒)
export LOG_LEVEL=DEBUG              # 日志级别
export WEBHOOK_URL=<webhook_url>    # Webhook通知URL
export LAMBDA_FUNCTION=<func_name>  # Lambda函数名
export SNS_TOPIC_ARN=<topic_arn>    # SNS主题ARN
```

## 文件结构

```
250701-eks-gpu-cluster-resilience/
├── dcgm-monitor-modular.py          # 主监控脚本
├── gpu_monitor_exclusions.json     # 排除列表文件
├── test-handlers.py                 # 测试脚本
├── lib/
│   ├── exclusion_manager.py         # 排除管理器
│   ├── metrics_processor_llm.py     # LLM指标处理器
│   ├── gpu-utils.sh                 # GPU工具库
│   └── handlers/
│       ├── error_dispatch.py        # 错误分发器
│       ├── lambda-gpu-error-handler.py  # Lambda处理器
│       ├── sns_handler.py           # SNS处理器
│       ├── webhook-receiver.py      # Webhook接收器
│       ├── gpu-instance-reboot.sh   # 实例重启脚本
└───────└── gpu-instance-replace.sh  # 实例替换脚本

```

## 使用场景

1. **生产环境GPU集群监控**: 24/7监控GPU健康状态
2. **自动故障恢复**: 无人值守的故障处理
3. **成本优化**: 无需预置备用实例
4. **灵活性**: 可简易自定义故障分类，无需编写正则日志解析，快速进行对应处理
