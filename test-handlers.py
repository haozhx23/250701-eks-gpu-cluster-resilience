
import random, time

## Trigger Handling & CW logging
from lib.handlers.error_dispatch import ErrorHandlerDispatch
error_handlers = ErrorHandlerDispatch()


parsed_error = {'error_class': 'XID_ERROR', 'error_count': 1, 'error_gpu_id': 0}
error_class = parsed_error['error_class']
error_count = parsed_error['error_count']

node_name = "test-worker-node-1"
instance_id = "i-1234567890abcdef0"

error_types = [
    'XID_ERROR',
    'ECC_ERROR', 
    'XID_CRITICAL_99',
    'XID_CRITICAL_79',
    'XID_CRITICAL_74',
    'XID_WARNING_43',
    'XID_WARNING_62',
    'GPU_HEALTH_ERROR',
    'GPU_HEALTH_WARNING'
]

for _ in range(5):

    error_class = random.choice(error_types)
    error_count = random.randint(1, 10)
    error_gpu_id = random.randint(0, 7)  # 假设最多8个GPU

    result = error_handlers.send_cloudwatch_metrics(error_class, error_count, node_name, instance_id)

    if result.get('status') == 'success':
        print(f"   ✅ CloudWatch指标发送成功")
    else:
        print(f"   ❌ CloudWatch指标发送失败: {result.get('message', 'Unknown error')}")

    # time.sleep(10)


## Test reboot



node_name="ip-10-11-132-118.ec2.internal"
instance_id="i-00cca484c3a2e151b"
# ./gpu-instance-reboot.sh run $node_name $instance_id 30

# reboot_trigger_result = error_handlers.call_reboot_script(node_name, instance_id, 30, True)
# # reboot_trigger_result = error_handlers.call_reboot_script(node_name, instance_id, 300, False)
# print(f"reboot_trigger_result - {reboot_trigger_result}")
# process = reboot_trigger_result['process']

# poll_result = process.poll()
# if poll_result is None:
#     print("✅ 进程仍在运行中（异步执行成功）")
#     stdout, stderr = process.communicate(timeout=100)
#     print(f"进程已完成")
#     print(f"stdout: {stdout}")
#     print(f"stderr: {stderr}")
#     print(f"返回码: {process.returncode}")



node_name="ip-10-11-141-95.ec2.internal"
instance_id="i-0f7e9fd6fa5227c7c"

replace_trigger_result = error_handlers.call_replace_script(node_name, instance_id, 30)
print(f"replace_trigger_result - {replace_trigger_result}")
process = replace_trigger_result['process']

poll_result = process.poll()
if poll_result is None:
    print("✅ 进程仍在运行中（异步执行成功）")
    stdout, stderr = process.communicate(timeout=100)
    print(f"进程已完成")
    print(f"stdout: {stdout}")
    print(f"stderr: {stderr}")
    print(f"返回码: {process.returncode}")

# NAME                            STATUS   ROLES    AGE   VERSION
# Initial
# ip-10-11-141-95.ec2.internal    Ready    <none>   28h   v1.31.7-eks-473151a

# Stage 1: cordoned
# ip-10-11-141-95.ec2.internal-473151a    Ready,SchedulingDisabled   <none>   28h   v1.31.7-eks

# Stage 2: new instance started
# ip-10-11-140-247.ec2.internal   Ready                         <none>   28s   v1.31.7-e
# ks-473151a
# ip-10-11-141-95.ec2.internal    NotReady,SchedulingDisabled   <none>   28h   v1.31.7-e
# ks-473151a

# Stage 3: new instance ready
# ip-10-11-140-247.ec2.internal   Ready                         <none>   2m21s   v1.31.7-eks-4731
# 51a


'''
NAMESPACE                  NAME                                                              READY   STATUS    RESTARTS       AGE
amazon-cloudwatch          amazon-cloudwatch-observability-controller-manager-5c95888bw85l   1/1     Running   0              4m56s
amazon-cloudwatch          cloudwatch-agent-6tqjw                                            1/1     Running   0              3m43s
amazon-cloudwatch          cloudwatch-agent-7jxjf                                            1/1     Running   0              28h
amazon-cloudwatch          cloudwatch-agent-fbwmm                                            1/1     Running   11 (42m ago)   28h
amazon-cloudwatch          dcgm-exporter-7jdtw                                               1/1     Running   0              3m42s
amazon-cloudwatch          dcgm-exporter-cxpwr                                               1/1     Running   0              28h
amazon-cloudwatch          dcgm-exporter-z4p9t                                               1/1     Running   11 (42m ago)   28h
amazon-cloudwatch          fluent-bit-5vg5n                                                  1/1     Running   11 (42m ago)   28h
amazon-cloudwatch          fluent-bit-7zggr                                                  1/1     Running   0              28h
amazon-cloudwatch          fluent-bit-jgn6h                                                  1/1     Running   0              3m43s
external-dns               external-dns-694b55fbb9-cvgp4                                     1/1     Running   0              4m56s
kube-state-metrics         kube-state-metrics-6946d75cc4-jt8j4                               1/1     Running   0              4m56s
kube-system                aws-efa-k8s-device-plugin-6bwp9                                   1/1     Running   0              3m15s
kube-system                aws-efa-k8s-device-plugin-brzcv                                   1/1     Running   11 (42m ago)   28h
kube-system                aws-efa-k8s-device-plugin-mdd98                                   1/1     Running   0              28h
kube-system                aws-node-5fxtw                                                    2/2     Running   0              3m43s
kube-system                aws-node-9rkhr                                                    2/2     Running   0              28h
kube-system                aws-node-jtjtf                                                    2/2     Running   22 (42m ago)   28h
kube-system                coredns-789f8477df-rwcrn                                          1/1     Running   0              4m56s
kube-system                coredns-789f8477df-vlm6j                                          1/1     Running   0              4m50s
kube-system                dcgm-server-2gttt                                                 1/1     Running   0              3m16s
kube-system                dcgm-server-8nk94                                                 1/1     Running   0              28h
kube-system                dcgm-server-zx5gc                                                 1/1     Running   11 (40m ago)   28h
kube-system                eks-node-monitoring-agent-qqb76                                   1/1     Running   32 (42m ago)   28h
kube-system                eks-node-monitoring-agent-rcr2n                                   1/1     Running   0              3m43s
kube-system                eks-node-monitoring-agent-w4gr7                                   1/1     Running   21 (68m ago)   28h
kube-system                eks-pod-identity-agent-gvhxz                                      1/1     Running   0              3m43s
kube-system                eks-pod-identity-agent-wvpfx                                      1/1     Running   11 (42m ago)   28h
kube-system                eks-pod-identity-agent-z45k7                                      1/1     Running   0              28h
kube-system                fsx-csi-controller-5777b95dd-cj6cx                                4/4     Running   0              4m55s
kube-system                fsx-csi-controller-5777b95dd-k992w                                4/4     Running   0              4m55s
kube-system                fsx-csi-node-dt549                                                3/3     Running   0              3m43s
kube-system                fsx-csi-node-k9d7p                                                3/3     Running   0              28h
kube-system                fsx-csi-node-vpxvh                                                3/3     Running   33 (42m ago)   28h
kube-system                kube-proxy-5jdvf                                                  1/1     Running   0              3m43s
kube-system                kube-proxy-f4w2f                                                  1/1     Running   11 (42m ago)   28h
kube-system                kube-proxy-knk4w                                                  1/1     Running   0              28h
kube-system                metrics-server-6bcc46f9d6-fd6cm                                   1/1     Running   0              4m56s
kube-system                metrics-server-6bcc46f9d6-mfc4g                                   1/1     Running   0              4m20s
kube-system                nvidia-device-plugin-daemonset-8sw4d                              1/1     Running   0              28h
kube-system                nvidia-device-plugin-daemonset-hvz7s                              1/1     Running   0              3m16s
kube-system                nvidia-device-plugin-daemonset-nc6mx                              1/1     Running   11 (42m ago)   28h
kubeflow-system            jobset-controller-manager-78c56848fc-chsb8                        1/1     Running   0              4m56s
kubeflow-system            kubeflow-trainer-controller-manager-5888cf8498-9dh4s              1/1     Running   0              4m56s
kubeflow                   training-operator-7f8bfd56f-g7l5q                                 1/1     Running   0              4m56s
prometheus-node-exporter   prometheus-node-exporter-fsdd4                                    1/1     Running   11 (42m ago)   28h
prometheus-node-exporter   prometheus-node-exporter-msghc                                    1/1     Running   0              3m43s
prometheus-node-exporter   prometheus-node-exporter-sw2ms                                    1/1     Running   0              28h
'''