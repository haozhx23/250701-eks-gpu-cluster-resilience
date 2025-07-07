#!/usr/bin/env python3
"""
DCGM GPU监控脚本 - Python版本
"""

import os
import sys
import time
import signal
import subprocess
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

# 添加lib目录到Python路径
script_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(script_dir, 'lib')
sys.path.insert(0, lib_dir)

from exclusion_manager import ExclusionManager
from lib.handlers.error_dispatch import ErrorHandlerDispatch

class DCGMMonitor:
    def __init__(self):
        """初始化DCGM监控器"""
        self.script_dir = script_dir
        self.lib_dir = lib_dir
        
        # 配置参数
        self.monitor_interval = int(os.getenv('MONITOR_INTERVAL', '30'))
        self.log_level = os.getenv('LOG_LEVEL', 'DEBUG')
        self.webhook_url = os.getenv('WEBHOOK_URL', '')
        self.lambda_function = os.getenv('LAMBDA_FUNCTION', '')
        
        # 初始化排除管理器
        self.exclusion_manager = ExclusionManager()

                
        self.error_handlers = ErrorHandlerDispatch()

        
        # 设置日志
        self.logger = self._setup_logger()
        
        # 信号处理
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        self.running = True
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger(f'DCGMMonitor.{self.__class__.__name__}')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
            # 设置日志级别
            level = getattr(logging, self.log_level.upper(), logging.INFO)
            logger.setLevel(level)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        self.logger.info("STOP Signal received and Stopping service...")
        self.running = False
    
    def _run_shell_command(self, command: str, shell_file: str = None) -> Tuple[bool, str]:
        """
        执行shell命令或调用shell函数
        
        Args:
            command: 要执行的命令
            shell_file: 如果需要source特定文件，指定文件路径
        
        Returns:
            Tuple[bool, str]: (是否成功, 输出内容)
        """
        try:
            if shell_file:
                # 需要source文件后执行命令
                full_command = f"bash -c 'source {shell_file} && {command}'"
            else:
                full_command = command
            
            self.logger.debug(f"执行命令: {full_command}")
            
            result = subprocess.run(
                full_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=60  # 60秒超时
            )
            
            if result.returncode != 0 and result.stderr:
                self.logger.debug(f"命令stderr: {result.stderr}")
            
            return result.returncode == 0, result.stdout.strip()
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"命令执行超时: {command}")
            return False, ""
        except Exception as e:
            self.logger.error(f"执行命令失败: {command}, 错误: {e}")
            return False, ""
    
    def get_dcgm_pods_with_nodes(self) -> list:
        """获取DCGM Pod和节点信息"""
        gpu_utils_file = os.path.join(self.lib_dir, 'gpu-utils.sh')
        
        # 检查文件是否存在
        if not os.path.exists(gpu_utils_file):
            self.logger.error(f"GPU工具文件不存在: {gpu_utils_file}")
            return []
        
        self.logger.debug(f"调用shell函数: get_dcgm_pods_with_nodes from {gpu_utils_file}")
        success, output = self._run_shell_command('get_dcgm_pods_with_nodes', gpu_utils_file)
        
        self.logger.debug(f"Shell函数调用结果: success={success}, output_length={len(output) if output else 0}")
        
        if not success:
            self.logger.error("无法获取DCGM Pod信息")
            return []
        
        if not output:
            self.logger.warning("Shell函数返回空输出")
            return []
        
        pods_info = []
        for line in output.split('\n'):
            line = line.strip()
            if '|' in line:
                parts = line.split('|')
                if len(parts) >= 2:
                    pod_name = parts[0].strip()
                    node_name = parts[1].strip()
                    if pod_name and node_name:
                        pods_info.append((pod_name, node_name))
                        self.logger.debug(f"找到Pod: {pod_name} on {node_name}")
        
        self.logger.info(f"找到 {len(pods_info)} 个DCGM Pod")
        return pods_info
    
    def get_instance_id(self, node_name: str) -> str:
        """获取节点的实例ID"""
        gpu_utils_file = os.path.join(self.lib_dir, 'gpu-utils.sh')
        success, output = self._run_shell_command(f'get_instance_id "{node_name}"', gpu_utils_file)
        
        if success and output:
            return output.strip()
        else:
            self.logger.warning(f"无法获取节点 {node_name} 的实例ID")
            return node_name  # fallback to node name
    
    def query_dcgm_metrics(self, pod_name: str, node_name: str) -> Optional[str]:
        """查询DCGM指标"""
        gpu_utils_file = os.path.join(self.lib_dir, 'gpu-utils.sh')
        command = f'query_dcgm_metrics_multi_gpu "{pod_name}" "{node_name}"'
        
        self.logger.debug(f"查询DCGM指标: {command}")
        success, output = self._run_shell_command(command, gpu_utils_file)
        
        if success:
            self.logger.debug(f"成功获取指标，输出长度: {len(output)}")
            return output
        else:
            self.logger.error(f"无法查询Pod {pod_name} 的指标")
            # 添加更详细的错误信息
            self.logger.debug(f"查询失败的输出: {output}")
            return None
    
    def process_metrics(self, metrics: str, node_name: str, instance_id: str) -> int:
        """
        处理指标数据
        
        Returns:
            int: 错误级别 (0=正常, 1=警告, 2=错误)
        """
        metrics_processor_file = os.path.join(self.lib_dir, 'metrics-processor-multi-gpu.sh')
        
        # 设置环境变量
        env = os.environ.copy()
        env['LOG_LEVEL'] = self.log_level
        env['WEBHOOK_URL'] = self.webhook_url
        env['LAMBDA_FUNCTION'] = self.lambda_function
        
        try:
            # 创建临时文件存储metrics数据
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
                f.write(metrics)
                temp_file = f.name
            
            # 调用metrics处理函数
            command = f'bash -c \'source "{metrics_processor_file}" && process_metrics "$(cat {temp_file})" "{node_name}" "{instance_id}"\''
            
            self.logger.debug(f"处理指标命令: {command}")
            
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                env=env,
                timeout=120
            )
            
            # 清理临时文件
            os.unlink(temp_file)
            
            # 输出处理结果
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                self.logger.warning(f"处理指标时的警告: {result.stderr}")
            
            # 根据输出判断错误级别
            output = result.stdout.lower()
            if "严重问题" in output or "紧急处理" in output:
                return 2  # 错误
            elif "警告" in output or "监控告警" in output:
                return 1  # 警告
            else:
                return 0  # 正常
                
        except Exception as e:
            self.logger.error(f"处理指标时发生异常: {e}")
            return 0
    

    def process_metrics_llm(self, metrics: str) -> dict:
        from lib.metrics_processor_llm import parse_gpu_metric_info
        parse_result = parse_gpu_metric_info(metrics)

        if isinstance(parse_result, dict):
            # parse_result['node_name'] = node_name
            # parse_result['instance_id'] = instance_id
            self.logger.debug(f"Parsed dict metric by LLM: {parse_result}")
            return parse_result
        else:
            raise ValueError(f"LLM Result Parse Failed: {parse_result}")

        

    def handle_gpu_error(self, error_level: int, node_name: str, instance_id: str, error_type: str = "UNKNOWN"):
        """
        处理GPU错误，添加到排除列表
        
        Args:
            error_level: 错误级别
            node_name: 节点名称
            instance_id: 实例ID
            error_type: 错误类型
        """
        if error_level >= 2:  # 严重错误
            # 添加到排除列表，排除时间更长
            timeout = 3600  # 1小时
            self.exclusion_manager.add_exclusion(instance_id, node_name, f"CRITICAL_{error_type}", timeout)
            self.logger.critical(f"🚨 严重GPU错误，实例 {instance_id} 已暂停监控1小时")
            
        elif error_level >= 1:  # 警告
            # 添加到排除列表，排除时间较短
            timeout = 1800  # 30分钟
            self.exclusion_manager.add_exclusion(instance_id, node_name, f"WARNING_{error_type}", timeout)
            self.logger.warning(f"⚠️  GPU警告，实例 {instance_id} 已暂停监控30分钟")

    # def reboot_gpu_instance_async(self, node_name, instance_id, wait_time=1800):
    #     script_path = "./lib/handlers/gpu-instance-reboot-test.sh"
    #     cmd = [script_path, "run", node_name, instance_id, str(wait_time)]
        
    #     try:
    #         process = subprocess.Popen(
    #             cmd,
    #             stdout=subprocess.PIPE,
    #             stderr=subprocess.PIPE,
    #             text=True
    #         )
            
    #         print(f"重启已触发，PID: {process.pid}, Node Name: {node_name}")
    #         return process.pid
            
    #     except Exception as e:
    #         print(f"启动失败: {e}")
    #         return None

    def handle_gpu_error(self, parsed_error: dict, node_name: str, instance_id: str):
        # {'error_class': 'XID_ERROR', 'error_count': 1, 'error_gpu_id': 0}
        error_class = parsed_error['error_class']
        error_count = parsed_error['error_count']
        
        if "HEALTHY" != error_class:
            result = self.error_handlers.send_cloudwatch_metrics(error_class, error_count, node_name, instance_id)

            if result.get('status') == 'success':
                self.logger.debug(f"Customized CloudWatch Metric Sent.")
            else:
                self.logger.debug(f"Customized CloudWatch Metric Sent Failed: {result.get('message', 'Unknown error')}")
            

        if "XID" in error_class and error_count > 5:
            # timeout = 1800
            timeout = 900
            self.exclusion_manager.add_exclusion(instance_id, node_name, f"{error_class}", timeout)
            self.logger.critical(f"CRITICAL GPU ERROR {instance_id} - {parsed_error}; Pause monitoring {timeout}s for processing.")

            result = self.error_handlers.call_replace_script(node_name, instance_id, timeout)

        elif "XID" in error_class and error_count <= 5:
            
            timeout = 200
            self.exclusion_manager.add_exclusion(instance_id, node_name, f"{error_class}", timeout)
            self.logger.error(f"OTHER GPU ERROR {instance_id} - {parsed_error}; Pause monitoring {timeout}s for processing.")

            result = self.error_handlers.call_reboot_script(node_name, instance_id, 30, True)

        else:
            self.logger.debug(f"{error_class} detected & keep running")
            

    ## 
    def monitor_single_node(self, pod_name: str, node_name: str) -> bool:
        """
        监控单个节点
        
        Returns:
            bool: 是否成功监控
        """
        instance_id = self.get_instance_id(node_name)
        
        # 检查是否应该监控此实例
        should_monitor, skip_reason = self.exclusion_manager.should_monitor(instance_id, node_name)
        
        if not should_monitor:
            # 实例被排除，跳过监控
            return True
        
        self.logger.info(f"📡 监控节点: {node_name} ({instance_id}) - Pod: {pod_name}")
        
        # 查询指标
        metrics = self.query_dcgm_metrics(pod_name, node_name)

        self.logger.debug(f"query_dcgm_metrics description text: \n{metrics}")

        if not metrics:
            self.logger.error(f"Failed get metrics from {pod_name}.")
            return False
        
        # # 处理指标
        # error_level = self.process_metrics(metrics, node_name, instance_id)
        
        # # 如果有错误，添加到排除列表
        # if error_level > 0:
        #     self.handle_gpu_error(error_level, node_name, instance_id)

        error_detail = self.process_metrics_llm(metrics)
        if "HEALTHY" != error_detail['error_class']:
            self.handle_gpu_error(error_detail, node_name, instance_id)
        
        return True
    
    def monitor_all_nodes(self):
        """监控所有GPU节点"""
        self.logger.info("Scan all GPU Nodes ...")
        
        # 获取所有DCGM Pod信息
        pods_info = self.get_dcgm_pods_with_nodes()
        
        if not pods_info:
            self.logger.warning("DCGM Pods NOT Found")
            return
        
        # 监控每个节点
        for pod_name, node_name in pods_info:
            try:
                self.monitor_single_node(pod_name, node_name)
                print("---")
            except Exception as e:
                self.logger.error(f"Exception when observing node - {node_name} : {e}")
    
    def show_startup_info(self):
        print("🚀 DCGM GPU Oberving Start ...")
        print(f"Observation Time Interval: {self.monitor_interval} secs")
        print(f"Observation Logging Level: {self.log_level}")
        
        if self.lambda_function:
            print(f"Lambda Function: {self.lambda_function}")
        if self.webhook_url:
            print(f"Webhook URL: {self.webhook_url}")
        
        print("")
    
    def run(self):
        """主监控循环"""
        self.show_startup_info()
        
        while self.running:
            time.sleep(5)

            try:
                self.monitor_all_nodes()
                
                if self.running:  # 检查是否仍在运行
                    self.logger.info(f"Wait {self.monitor_interval} secs Interval ...")
                    time.sleep(self.monitor_interval)
                    
            except KeyboardInterrupt:
                self.logger.info("Received keyboard interrupt signal..")
                break
            except Exception as e:
                self.logger.error(f"exception occurred in monitoring loop: {e}")
                if self.running:
                    time.sleep(10)
        
        self.logger.info("Main Monitoring Stopped")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='DCGM GPU监控器 - Python版本')
    parser.add_argument('--interval', type=int, help='监控间隔（秒）')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='日志级别')
    parser.add_argument('--webhook-url', help='Webhook URL')
    parser.add_argument('--lambda-function', help='Lambda函数名称')
    
    # 排除管理命令
    parser.add_argument('--exclusion-cmd', choices=['list', 'cleanup', 'pause', 'resume'], 
                       help='排除管理命令')
    parser.add_argument('--instance-id', help='实例ID（用于排除管理）')
    parser.add_argument('--node-name', help='节点名称（用于排除管理）')
    parser.add_argument('--reason', help='暂停原因（用于排除管理）')
    
    args = parser.parse_args()
    
    # 处理排除管理命令
    if args.exclusion_cmd:
        manager = ExclusionManager()
        
        if args.exclusion_cmd == 'list':
            manager.show_status()
        elif args.exclusion_cmd == 'cleanup':
            count = manager.cleanup_expired()
            print(f"清理了 {count} 个过期记录")
        elif args.exclusion_cmd == 'pause':
            if not args.instance_id or not args.node_name:
                print("暂停监控需要 --instance-id 和 --node-name 参数")
                return 1
            reason = args.reason or "MANUAL_PAUSE"
            manager.pause_instance(args.instance_id, args.node_name, reason)
        elif args.exclusion_cmd == 'resume':
            if not args.instance_id:
                print("恢复监控需要 --instance-id 参数")
                return 1
            manager.resume_instance(args.instance_id)
        
        return 0
    
    # 设置环境变量（如果通过命令行参数提供）
    if args.interval:
        os.environ['MONITOR_INTERVAL'] = str(args.interval)
    if args.log_level:
        os.environ['LOG_LEVEL'] = args.log_level
    if args.webhook_url:
        os.environ['WEBHOOK_URL'] = args.webhook_url
    if args.lambda_function:
        os.environ['LAMBDA_FUNCTION'] = args.lambda_function
    
    # 启动监控
    monitor = DCGMMonitor()
    try:
        monitor.run()
        return 0
    except Exception as e:
        print(f"Observation Launch Failed: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
