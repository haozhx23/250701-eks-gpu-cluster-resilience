#!/usr/bin/env python3
"""
自动化处理器封装
将原来的automation.sh功能封装成Python，整合所有handlers
"""

import json
import subprocess
import datetime
import os
import sys
import logging
from typing import Dict, Optional
import boto3
import requests

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ErrorHandlerDispatch:
    """自动化处理器 - 封装所有handler能力"""
    
    def __init__(self):
        """初始化处理器"""
        self.handlers_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 从环境变量获取配置
        self.webhook_url = os.getenv('WEBHOOK_URL', '')
        self.lambda_function = os.getenv('LAMBDA_FUNCTION', '')
        self.sns_topic_arn = os.getenv('SNS_TOPIC_ARN', '')
        
        # 初始化AWS客户端
        self._init_aws_clients()
    
    def _init_aws_clients(self):
        """初始化AWS客户端"""
        try:
            self.lambda_client = boto3.client('lambda')
            self.cloudwatch_client = boto3.client('cloudwatch')
            logger.info("Boto3 Clients Init Success")
        except Exception as e:
            logger.warning(f"Boto3 Clients Init Failed: {e}")
            self.lambda_client = None
            self.cloudwatch_client = None

    def call_replace_script(self, node_name: str, instance_id: str, wait_time: int = 1800) -> Dict:

        try:
            script_path = os.path.join(self.handlers_dir, 'gpu-instance-replace.sh')
            cmd = [script_path, "run", node_name, instance_id, str(wait_time)]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return {
                'status': 'started',
                'pid': process.pid,
                'process': process  # 返回进程对象以便后续操作
            }
                    
        except Exception as e:
            return {'status': 'error', 'message': f'Triggering Instance Replace Error: {str(e)}'}

    def call_reboot_script(self, node_name: str, instance_id: str, wait_time: int = 900, async_mode: bool = True) -> Dict:

        try:
            script_path = os.path.join(self.handlers_dir, 'gpu-instance-reboot.sh')
            cmd = [script_path, "run", node_name, instance_id, str(wait_time)]
            
            if async_mode:
                # 非阻塞模式
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                return {
                    'status': 'started',
                    'pid': process.pid,
                    'process': process  # 返回进程对象以便后续操作
                }
            else:
                # 阻塞模式 - 等待完成
                exe_result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                return exe_result
                    
        except Exception as e:
            return {'status': 'error', 'message': f'Instance Reboot Exception: {str(e)}'}
    
    def call_shell_handler(self, error_type: str, node_name: str, 
                          instance_id: str, error_details: str) -> Dict:
        """调用Shell处理器 (gpu-error-handler.sh)"""
        try:
            logger.info("📡 调用Shell处理器")
            
            handler_script = os.path.join(self.handlers_dir, 'gpu-error-handler.sh')
            if not os.path.exists(handler_script):
                return {
                    'status': 'error',
                    'message': f'Shell处理器不存在: {handler_script}'
                }
            
            # 调用Shell处理器函数
            cmd = f'source {handler_script} && handle_gpu_error "{error_type}" "{node_name}" "{instance_id}" "{error_details}"'
            
            result = subprocess.run(['bash', '-c', cmd], 
                                  capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                return {
                    'status': 'success',
                    'message': 'Shell处理器执行成功',
                    'output': result.stdout
                }
            else:
                return {
                    'status': 'error',
                    'message': f'Shell处理器执行失败: {result.stderr}',
                    'output': result.stdout
                }
                
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Shell处理器异常: {str(e)}'
            }
    
    def call_lambda_handler(self, error_type: str, node_name: str, 
                           instance_id: str, error_details: str) -> Dict:
        """调用Lambda处理器"""
        if not self.lambda_function:
            return {
                'status': 'skipped',
                'message': 'Lambda函数名未配置'
            }
        
        if not self.lambda_client:
            return {
                'status': 'error',
                'message': 'Lambda客户端未初始化'
            }
        
        try:
            logger.info(f"调用Lambda函数: {self.lambda_function}")
            
            # 构建事件数据
            event_data = self._build_event_data(error_type, node_name, instance_id, error_details)
            
            response = self.lambda_client.invoke(
                FunctionName=self.lambda_function,
                Payload=json.dumps(event_data),
                InvocationType='RequestResponse'
            )
            
            payload = json.loads(response['Payload'].read())
            
            if response['StatusCode'] == 200:
                return {
                    'status': 'success',
                    'message': 'Lambda函数调用成功',
                    'response': payload
                }
            else:
                return {
                    'status': 'error',
                    'message': f'Lambda函数调用失败: {payload}',
                    'status_code': response['StatusCode']
                }
                
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Lambda函数调用异常: {str(e)}'
            }
    
    def call_webhook_handler(self, error_type: str, node_name: str, 
                            instance_id: str, error_details: str) -> Dict:
        """调用Webhook处理器"""
        if not self.webhook_url:
            return {
                'status': 'skipped',
                'message': 'Webhook URL未配置'
            }
        
        try:
            logger.info(f"📡 发送Webhook到: {self.webhook_url}")
            
            # 构建事件数据
            event_data = self._build_event_data(error_type, node_name, instance_id, error_details)
            
            response = requests.post(
                self.webhook_url,
                json=event_data,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                return {
                    'status': 'success',
                    'message': 'Webhook发送成功',
                    'response': response.text
                }
            else:
                return {
                    'status': 'error',
                    'message': f'Webhook发送失败: HTTP {response.status_code}',
                    'response': response.text
                }
                
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Webhook发送异常: {str(e)}'
            }
    
    def call_sns_handler(self, error_type: str, node_name: str, 
                        instance_id: str, error_details: str) -> Dict:
        """调用SNS处理器"""
        if not self.sns_topic_arn:
            return {
                'status': 'skipped',
                'message': 'SNS Topic ARN未配置'
            }
        
        try:
            logger.info(f"📧 调用SNS处理器")
            
            # 调用SNS处理器脚本
            sns_handler_script = os.path.join(self.handlers_dir, 'sns_handler.py')
            
            if not os.path.exists(sns_handler_script):
                return {
                    'status': 'error',
                    'message': f'SNS处理器不存在: {sns_handler_script}'
                }
            
            # 获取集群名称
            cluster_name = self._get_cluster_name()
            
            cmd = [
                'python3', sns_handler_script,
                self.sns_topic_arn, error_type, node_name, 
                instance_id, error_details, cluster_name
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # 解析返回的JSON结果
                try:
                    response_data = json.loads(result.stdout)
                    return response_data
                except json.JSONDecodeError:
                    return {
                        'status': 'success',
                        'message': 'SNS处理器执行成功',
                        'output': result.stdout
                    }
            else:
                return {
                    'status': 'error',
                    'message': f'SNS处理器执行失败: {result.stderr}',
                    'output': result.stdout
                }
                
        except Exception as e:
            return {
                'status': 'error',
                'message': f'SNS处理器异常: {str(e)}'
            }
    
    def send_cloudwatch_metrics(self, error_type: str, error_count: int, node_name: str, instance_id: str) -> Dict:
        """发送CloudWatch指标"""        
        try:
            logger.info("Sending CloudWatch Metrics...")
            
            self.cloudwatch_client.put_metric_data(
                Namespace='testGPU/testMonitoring',
                MetricData=[
                    {
                        'MetricName': 'GPUErrorByType',
                        'Value': error_count,
                        'Unit': 'Count',
                        'Dimensions': [
                            {'Name': 'NodeName', 'Value': node_name},
                            {'Name': 'InstanceId', 'Value': instance_id},
                            {'Name': 'ErrorType', 'Value': error_type}
                        ]
                    }
                ]
            )
            
            return {
                'status': 'success',
                'message': 'CloudWatch Metric Sent Success'
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'CloudWatch Metric Sent Exception: {str(e)}'
            }
    
    def _build_event_data(self, error_type: str, node_name: str, 
                         instance_id: str, error_details: str) -> Dict:
        """构建事件数据"""
        return {
            'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
            'error_type': error_type,
            'node_name': node_name,
            'instance_id': instance_id,
            'error_details': error_details,
            'cluster_name': self._get_cluster_name()
        }
    
    def _get_cluster_name(self) -> str:
        """获取集群名称"""
        try:
            result = subprocess.run(['kubectl', 'config', 'current-context'], 
                                  capture_output=True, text=True)
            return result.stdout.strip() if result.returncode == 0 else 'unknown'
        except Exception:
            return 'unknown'


def main():
    """命令行接口 - 提供各种handler的调用示例"""
    
    if len(sys.argv) < 2:
        print("""
GPU错误自动化处理器

用法:
  python automation.py <handler_type> <error_type> <node_name> <instance_id> <error_details>

Handler类型:
  shell     - 调用Shell处理器 (gpu-error-handler.sh)
  lambda    - 调用Lambda处理器
  webhook   - 调用Webhook处理器  
  sns       - 调用SNS处理器
  cloudwatch - 发送CloudWatch指标
  all       - 调用所有配置的处理器

示例:
  # 调用Shell处理器
  python automation.py shell XID_CRITICAL_999 worker-node-1 i-1234567890abcdef0 "GPU XID error detected"
  
  # 调用Lambda处理器
  python automation.py lambda ECC_ERROR worker-node-2 i-0987654321fedcba0 "ECC memory error"
  
  # 调用Webhook处理器
  python automation.py webhook GPU_HEALTH_WARNING worker-node-3 i-abcdef1234567890 "GPU temperature high"
  
  # 调用SNS处理器
  python automation.py sns XID_CRITICAL_79 worker-node-4 i-fedcba0987654321 "GPU driver error"
  
  # 发送CloudWatch指标
  python automation.py cloudwatch GPU_HEALTH_ERROR worker-node-5 i-1357924680abcdef "GPU health check failed"
  
  # 调用所有处理器
  python automation.py all XID_CRITICAL_999 worker-node-1 i-1234567890abcdef0 "Critical GPU error"

环境变量配置:
  WEBHOOK_URL      - Webhook接收端URL
  LAMBDA_FUNCTION  - Lambda函数名
  SNS_TOPIC_ARN    - SNS Topic ARN
        """)
        sys.exit(1)
    
    handler_type = sys.argv[1]
    
    if len(sys.argv) < 6:
        print("错误: 参数不足")
        print("需要: <handler_type> <error_type> <node_name> <instance_id> <error_details>")
        sys.exit(1)
    
    error_type = sys.argv[2]
    node_name = sys.argv[3]
    instance_id = sys.argv[4]
    error_details = sys.argv[5]
    
    # 创建自动化处理器
    automation = ErrorHandlerDispatch()
    
    results = {}
    
    # 根据handler类型执行相应的处理器
    if handler_type == 'shell':
        results['shell'] = automation.call_shell_handler(error_type, node_name, instance_id, error_details)
    
    elif handler_type == 'lambda':
        results['lambda'] = automation.call_lambda_handler(error_type, node_name, instance_id, error_details)
    
    elif handler_type == 'webhook':
        results['webhook'] = automation.call_webhook_handler(error_type, node_name, instance_id, error_details)
    
    elif handler_type == 'sns':
        results['sns'] = automation.call_sns_handler(error_type, node_name, instance_id, error_details)
    
    elif handler_type == 'cloudwatch':
        results['cloudwatch'] = automation.send_cloudwatch_metrics(error_type, node_name, instance_id)
    
    elif handler_type == 'all':
        # 调用所有处理器
        results['shell'] = automation.call_shell_handler(error_type, node_name, instance_id, error_details)
        results['lambda'] = automation.call_lambda_handler(error_type, node_name, instance_id, error_details)
        results['webhook'] = automation.call_webhook_handler(error_type, node_name, instance_id, error_details)
        results['sns'] = automation.call_sns_handler(error_type, node_name, instance_id, error_details)
        results['cloudwatch'] = automation.send_cloudwatch_metrics(error_type, node_name, instance_id)
    
    else:
        print(f"错误: 未知的handler类型: {handler_type}")
        print("支持的类型: shell, lambda, webhook, sns, cloudwatch, all")
        sys.exit(1)
    
    # 输出结果
    print(json.dumps(results, indent=2, ensure_ascii=False))
    
    # 检查是否有失败的处理器
    has_error = any(result.get('status') == 'error' for result in results.values())
    sys.exit(1 if has_error else 0)


if __name__ == '__main__':
    main()
