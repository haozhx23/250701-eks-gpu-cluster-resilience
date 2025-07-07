#!/usr/bin/env python3
"""
SNS通知处理器
用于发送GPU错误的SNS通知
"""

import json
import boto3
import logging
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class SNSHandler:
    """SNS通知处理器"""
    
    def __init__(self, topic_arn: Optional[str] = None):
        """
        初始化SNS处理器
        
        Args:
            topic_arn: SNS Topic ARN
        """
        self.topic_arn = topic_arn
        try:
            self.sns_client = boto3.client('sns')
        except Exception as e:
            logger.error(f"SNS客户端初始化失败: {e}")
            self.sns_client = None
    
    def send_notification(self, error_type: str, node_name: str, 
                         instance_id: str, error_details: str, 
                         cluster_name: str = "unknown") -> Dict:
        """
        发送SNS通知
        
        Args:
            error_type: 错误类型
            node_name: 节点名称
            instance_id: 实例ID
            error_details: 错误详情
            cluster_name: 集群名称
            
        Returns:
            处理结果字典
        """
        if not self.sns_client:
            return {
                'status': 'error',
                'message': 'SNS客户端未初始化'
            }
        
        if not self.topic_arn:
            return {
                'status': 'error',
                'message': 'SNS Topic ARN未配置'
            }
        
        try:
            # 构建消息内容
            message = self._build_message(error_type, node_name, instance_id, 
                                        error_details, cluster_name)
            
            # 构建主题
            subject = f"GPU错误警报 - {error_type} on {instance_id}"
            
            # 发送通知
            response = self.sns_client.publish(
                TopicArn=self.topic_arn,
                Message=message,
                Subject=subject
            )
            
            logger.info(f"SNS通知发送成功: MessageId={response['MessageId']}")
            
            return {
                'status': 'success',
                'message': 'SNS通知发送成功',
                'message_id': response['MessageId']
            }
            
        except Exception as e:
            logger.error(f"SNS通知发送失败: {e}")
            return {
                'status': 'error',
                'message': f'SNS通知发送失败: {str(e)}'
            }
    
    def _build_message(self, error_type: str, node_name: str, 
                      instance_id: str, error_details: str, 
                      cluster_name: str) -> str:
        """构建通知消息"""
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        # 根据错误类型设置紧急程度
        urgency = self._get_urgency_level(error_type)
        
        message = f"""
🚨 GPU错误警报 [{urgency}]

⏰ 时间: {timestamp}
🏢 集群: {cluster_name}
🖥️  节点: {node_name}
🆔 实例ID: {instance_id}
❌ 错误类型: {error_type}
📝 错误详情: {error_details}

{self._get_action_recommendations(error_type)}

---
此消息由GPU监控系统自动发送
        """.strip()
        
        return message
    
    def _get_urgency_level(self, error_type: str) -> str:
        """根据错误类型获取紧急程度"""
        critical_errors = [
            'XID_CRITICAL_999', 'XID_CRITICAL_79', 'XID_CRITICAL_74',
            'ECC_ERROR', 'GPU_HEALTH_ERROR'
        ]
        
        warning_errors = [
            'XID_WARNING_43', 'XID_WARNING_62', 'XID_WARNING_31',
            'GPU_HEALTH_WARNING'
        ]
        
        if error_type in critical_errors:
            return "🔴 CRITICAL"
        elif error_type in warning_errors:
            return "🟡 WARNING"
        else:
            return "🔵 INFO"
    
    def _get_action_recommendations(self, error_type: str) -> str:
        """根据错误类型获取行动建议"""
        recommendations = {
            'XID_CRITICAL_999': """
🔧 建议行动:
• 立即检查GPU硬件状态
• 考虑重启实例
• 联系硬件支持团队""",
            
            'XID_CRITICAL_79': """
🔧 建议行动:
• 检查GPU驱动程序
• 验证CUDA版本兼容性
• 考虑重启实例""",
            
            'XID_CRITICAL_74': """
🔧 建议行动:
• 检查GPU内存使用情况
• 验证应用程序内存管理
• 监控后续错误""",
            
            'ECC_ERROR': """
🔧 建议行动:
• 立即标记节点维护
• 不要重启，等待人工检查
• 联系硬件团队进行内存检测""",
            
            'GPU_HEALTH_ERROR': """
🔧 建议行动:
• 检查GPU温度和功耗
• 验证冷却系统
• 考虑降低工作负载""",
            
            'GPU_HEALTH_WARNING': """
🔧 建议行动:
• 继续监控GPU状态
• 检查系统日志
• 准备维护计划"""
        }
        
        return recommendations.get(error_type, """
🔧 建议行动:
• 检查系统日志获取更多信息
• 监控后续错误模式
• 必要时联系技术支持""")


def main():
    """命令行接口"""
    import sys
    import os
    
    if len(sys.argv) < 5:
        print("用法: python sns_handler.py <topic_arn> <error_type> <node_name> <instance_id> <error_details> [cluster_name]")
        sys.exit(1)
    
    topic_arn = sys.argv[1]
    error_type = sys.argv[2]
    node_name = sys.argv[3]
    instance_id = sys.argv[4]
    error_details = sys.argv[5]
    cluster_name = sys.argv[6] if len(sys.argv) > 6 else "unknown"
    
    # 创建SNS处理器
    handler = SNSHandler(topic_arn)
    
    # 发送通知
    result = handler.send_notification(error_type, node_name, instance_id, 
                                     error_details, cluster_name)
    
    # 输出结果
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # 设置退出码
    sys.exit(0 if result['status'] == 'success' else 1)


if __name__ == '__main__':
    main()
