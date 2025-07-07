#!/bin/bash
set -e

# sudo rm -rf /dfsx/checkpoints

echo "Installing dependencies..."
pip install transformers datasets

echo "Starting distributed training..."
exec /usr/local/bin/torchrun \
    --nproc_per_node=4 \
    --nnodes=2 \
    /dfsx/FSDP-k8s/src/train.py \
    --max_context_width=1024 \
    --num_key_value_heads=2 \
    --intermediate_size=1024 \
    --hidden_width=512 \
    --num_layers=8 \
    --num_heads=8 \
    --model_type=llama_v3 \
    --tokenizer=hf-internal-testing/llama-tokenizer \
    --checkpoint_freq=50 \
    --validation_freq=100 \
    --max_steps=5000 \
    --checkpoint_dir=/dfsx/checkpoints \
    --dataset=allenai/c4 \
    --dataset_config_name=en \
    --resume_from_checkpoint=/dfsx/checkpoints \
    --train_batch_size=1 \
    --val_batch_size=1 \
    --sharding_strategy=full \
    --offload_activations=1
