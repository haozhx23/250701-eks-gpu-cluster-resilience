#!/usr/bin/env python3
"""
简单的Webhook接收端 - 用于演示GPU错误通知
在实际环境中，这可能是你的Slack、Teams或自定义API端点
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import datetime

class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # 读取请求数据
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        
        try:
            # 解析JSON数据
            data = json.loads(post_data.decode('utf-8'))
            
            # 处理GPU错误通知
            print(f"\n🚨 收到GPU错误通知 - {datetime.datetime.now()}")
            print(f"错误类型: {data.get('error_type', 'Unknown')}")
            print(f"节点名称: {data.get('node_name', 'Unknown')}")
            print(f"实例ID: {data.get('instance_id', 'Unknown')}")
            print(f"错误详情: {data.get('error_details', 'Unknown')}")
            print(f"集群名称: {data.get('cluster_name', 'Unknown')}")
            print(f"时间戳: {data.get('timestamp', 'Unknown')}")
            
            # 模拟处理逻辑
            error_type = data.get('error_type')
            if error_type == 'XID_ERROR':
                print("📋 执行XID错误处理流程...")
                print("   - 记录错误日志")
                print("   - 准备重启实例")
                print("   - 发送运维通知")
            elif error_type == 'ECC_ERROR':
                print("📋 执行ECC错误处理流程...")
                print("   - 标记节点维护")
                print("   - 创建紧急工单")
                print("   - 通知硬件团队")
            
            # 返回成功响应
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "received", "message": "GPU error processed"}).encode())
            
        except Exception as e:
            print(f"❌ 处理webhook请求失败: {e}")
            self.send_response(400)
            self.end_headers()
    
    def log_message(self, format, *args):
        # 禁用默认日志输出
        pass

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8080), WebhookHandler)
    print("🌐 Webhook接收端启动在 http://localhost:8080")
    print("等待GPU错误通知...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n📴 Webhook服务器停止")
        server.shutdown()
