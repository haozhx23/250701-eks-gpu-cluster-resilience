import json
import boto3
import logging
from datetime import datetime

# 配置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS客户端
ec2 = boto3.client('ec2')
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

def lambda_handler(event, context):
    """
    GPU错误处理Lambda函数
    当DCGM监控检测到GPU错误时触发
    """
    
    try:
        # 解析输入事件
        error_type = event.get('error_type')
        node_name = event.get('node_name')
        instance_id = event.get('instance_id')
        error_details = event.get('error_details')
        cluster_name = event.get('cluster_name')
        timestamp = event.get('timestamp')
        
        logger.info(f"处理GPU错误: {error_type} on {node_name} ({instance_id})")
        
        # 根据错误类型执行不同的处理逻辑
        response = handle_gpu_error(error_type, node_name, instance_id, error_details, cluster_name)
        
        # 发送CloudWatch指标
        send_cloudwatch_metrics(error_type, node_name, instance_id, cluster_name)
        
        # 可选：发送SNS通知
        send_sns_notification(error_type, node_name, instance_id, error_details, cluster_name)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'GPU错误处理完成',
                'error_type': error_type,
                'instance_id': instance_id,
                'actions_taken': response
            })
        }
        
    except Exception as e:
        logger.error(f"处理GPU错误时发生异常: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

def handle_gpu_error(error_type, node_name, instance_id, error_details, cluster_name):
    """根据错误类型执行相应的处理逻辑"""
    
    actions_taken = []
    
    if error_type == 'XID_ERROR':
        # XID错误处理
        logger.info(f"处理XID错误: {instance_id}")
        
        # 1. 获取实例信息
        instance_info = get_instance_info(instance_id)
        actions_taken.append(f"获取实例信息: {instance_info.get('InstanceType', 'Unknown')}")
        
        # 2. 检查实例状态
        instance_state = instance_info.get('State', {}).get('Name', 'unknown')
        actions_taken.append(f"实例状态: {instance_state}")
        
        # 3. 可选：重启实例 (根据配置决定)
        if should_reboot_instance(error_details):
            logger.info(f"重启实例: {instance_id}")
            ec2.reboot_instances(InstanceIds=[instance_id])
            actions_taken.append("实例重启已触发")
        
        # 4. 标记实例进行进一步检查
        tag_instance_for_maintenance(instance_id, error_type, error_details)
        actions_taken.append("实例已标记为维护状态")
        
    elif error_type == 'ECC_ERROR':
        # ECC错误处理 (更严重，需要人工干预)
        logger.warning(f"检测到ECC错误: {instance_id}")
        
        # 标记实例需要立即关注
        tag_instance_for_maintenance(instance_id, error_type, error_details, urgent=True)
        actions_taken.append("实例已标记为紧急维护状态")
        
        # 不自动重启，等待人工检查
        actions_taken.append("等待人工检查 - 未执行自动重启")
        
    elif error_type == 'HEALTH_WARNING':
        # 健康警告处理
        logger.info(f"处理健康警告: {instance_id}")
        
        # 记录警告，继续监控
        actions_taken.append("健康警告已记录，继续监控")
        
    return actions_taken

def get_instance_info(instance_id):
    """获取EC2实例信息"""
    try:
        response = ec2.describe_instances(InstanceIds=[instance_id])
        return response['Reservations'][0]['Instances'][0]
    except Exception as e:
        logger.error(f"获取实例信息失败: {str(e)}")
        return {}

def should_reboot_instance(error_details):
    """判断是否应该重启实例"""
    # 这里可以添加更复杂的逻辑
    # 例如：检查错误频率、时间等
    return True  # 简单示例：总是重启

def tag_instance_for_maintenance(instance_id, error_type, error_details, urgent=False):
    """为实例添加维护标签"""
    try:
        tags = [
            {
                'Key': 'GPUErrorDetected',
                'Value': 'true'
            },
            {
                'Key': 'GPUErrorType',
                'Value': error_type
            },
            {
                'Key': 'GPUErrorTime',
                'Value': datetime.utcnow().isoformat()
            },
            {
                'Key': 'GPUErrorDetails',
                'Value': error_details[:255]  # 限制长度
            }
        ]
        
        if urgent:
            tags.append({
                'Key': 'MaintenanceUrgency',
                'Value': 'HIGH'
            })
        
        ec2.create_tags(Resources=[instance_id], Tags=tags)
        logger.info(f"实例 {instance_id} 已添加维护标签")
        
    except Exception as e:
        logger.error(f"添加标签失败: {str(e)}")

def send_cloudwatch_metrics(error_type, node_name, instance_id, cluster_name):
    """发送CloudWatch自定义指标"""
    try:
        cloudwatch.put_metric_data(
            Namespace='GPU/Monitoring',
            MetricData=[
                {
                    'MetricName': 'GPUError',
                    'Dimensions': [
                        {
                            'Name': 'ErrorType',
                            'Value': error_type
                        },
                        {
                            'Name': 'InstanceId',
                            'Value': instance_id
                        },
                        {
                            'Name': 'ClusterName',
                            'Value': cluster_name
                        }
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
        )
        logger.info("CloudWatch指标已发送")
    except Exception as e:
        logger.error(f"发送CloudWatch指标失败: {str(e)}")

def send_sns_notification(error_type, node_name, instance_id, error_details, cluster_name):
    """发送SNS通知 (可选)"""
    try:
        # 需要配置SNS Topic ARN
        topic_arn = "arn:aws:sns:region:account:gpu-alerts"  # 替换为实际的Topic ARN
        
        message = f"""
GPU错误警报

集群: {cluster_name}
节点: {node_name}
实例ID: {instance_id}
错误类型: {error_type}
错误详情: {error_details}
时间: {datetime.utcnow().isoformat()}

请检查实例状态并采取必要的维护措施。
        """
        
        sns.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=f"GPU错误警报 - {error_type} on {instance_id}"
        )
        logger.info("SNS通知已发送")
    except Exception as e:
        logger.error(f"发送SNS通知失败: {str(e)}")
