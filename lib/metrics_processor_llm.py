import boto3
import json

bedrock = boto3.client("bedrock-runtime")

def parse_gpu_metric_info(metric_data: str,
                        model_id = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
    ):

    output_format = '''{"error_class": "above class code", "error_count": integer error count, "error_gpu_id": error gpu id}'''

    user_template = '''这里是我从我GPU集群某一个节点的DCGM server上获取的信息，包含Node名称，gpu数量，每个GPU的ecc错误数量，xid错误数量，以及可能存在的具体的XID错误码。

```
{metric_data}
```

同时，我有一组针对不同错误类型的分类如下：

XID_CRITICAL_999
XID_CRITICAL_79
XID_CRITICAL_74
XID_WARNING_43
XID_WARNING_62
XID_WARNING_31
XID_ERROR
ECC_ERROR
GPU_HEALTH_WARNING
GPU_HEALTH_ERROR
HEALTHY

现在，请基于我从DCGM server上获取的信息，帮我将该日志划分到以上的几个类别中，并且输出错误的数量。
注意，
1. 如果有XID错误但没有明确的XID错误码，直接归类至XID_ERROR；
2. 如果有多个错误类型，只考虑严重程度最高的；
3. 如果有多个GPU有错误，只考虑错误数量最多的；
4. 输出json格式，包含字段及格式见示例：{output_format}；
5. 输出的json格式放在<error_class></error_class>中；
6. 除了json之外，不要解释且不要输出任何其他内容。
'''

    # {"role": "user", "content": "Analyze this dataset for anomalies: <dataset>{{DATASET}}</dataset>"}

    chat_payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "temperature": 0.1,
        "system": "You are a GPU Cluster Operation Expert!",
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": user_template.format(metric_data=metric_data, output_format=output_format)}],
            },
            {
                "role": "assistant",
                "content": [{"type": "text", "text": "<error_class>"}]
            }
        ],
    }


    resp_invoke = bedrock.invoke_model(
        body=json.dumps(chat_payload), modelId=model_id
    )

    errclass = json.loads(resp_invoke['body'].read())
    output_text = errclass['content'][0]['text']
    output_data = output_text.replace("</error_class>","")

    try:
        output_dict = json.loads(output_data)
    except (ValueError, SyntaxError) as e:
        print(output_text)
        print(f"Parse LLM output error {e}")
    
    # print(errclass)
    # print(output_text)
    # print(output_dict)

    return output_dict




if __name__ == "__main__":

    data1 = '''=== NODE_INFO ===
    Node: ip-10-11-141-95.ec2.internal
    === GPU_COUNT ===
    4
    === ECC_DATA ===
    GPU 0      0                       
    GPU 1      0                       
    GPU 2      4                       
    GPU 3      0                       
    === XID_DATA ===
    GPU 0      1            
    GPU 1      0            
    GPU 2      0            
    GPU 3      0            
    === XID_DETAILS ===
    === HEALTH_DATA ===
    +---------------------------+----------------------------------------------------------+
    | Health Monitor Report                                                                |
    +===========================+==========================================================+
    | Overall Health            | Healthy                                                  |
    +---------------------------+----------------------------------------------------------+
    === GPU_INFO ===
    4 GPUs found.
    +--------+----------------------------------------------------------------------+
    | GPU ID | Device Information                                                   |
    +--------+----------------------------------------------------------------------+
    | 0      | Name: NVIDIA L4                                                      |
    |        | PCI Bus ID: 00000000:38:00.0                                         |
    |        | Device UUID: GPU-4698f441-07d6-1a48-d7e5-22864f64f5bb                |
    +--------+----------------------------------------------------------------------+
    | 1      | Name: NVIDIA L4                                                      |
    |        | PCI Bus ID: 00000000:3A:00.0                                         |
    |        | Device UUID: GPU-59bc4725-b484-64a5-8da6-306a93249ae4                |
    +--------+----------------------------------------------------------------------+
    | 2      | Name: NVIDIA L4                                                      |
    |        | PCI Bus ID: 00000000:3C:00.0                                         |
    |        | Device UUID: GPU-3dbc8d3e-58ad-18aa-9448-e45d1473e7b9                |
    +--------+----------------------------------------------------------------------+
    | 3      | Name: NVIDIA L4                                                      |
    |        | PCI Bus ID: 00000000:3E:00.0                                         |
    |        | Device UUID: GPU-f64f5abf-20e5-2329-ef45-e0101be43006                |
    +--------+----------------------------------------------------------------------+
    0 NvSwitches found.
    +-----------+
    | Switch ID |
    +-----------+
    +-----------+
    0 ConnectX found.
    +----------+
    | ConnectX |
    +----------+
    +----------+
    0 CPUs found.
    +--------+----------------------------------------------------------------------+
    | CPU ID | Device Information                                                   |
    +--------+----------------------------------------------------------------------+
    +--------+----------------------------------------------------------------------+
    '''

    errclassdict = parse_gpu_metric_info(data1)
    print(errclassdict)