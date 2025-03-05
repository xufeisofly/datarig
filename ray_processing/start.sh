#!/bin/bash

# 本地启动一个 ray
ray start --head --port 6379

mkdir -p /root/dataprocess/dclm_output/1b-1x/c4

python ray_processing/process.py \
  --readable_name refinedweb_test \
  --raw_data_dirpath "/root/dataprocess/dclm_pool/1b-1x/" \
  --output_dir "/root/dataprocess/dclm_output/1b-1x" \
  --config_path "baselines/baselines_configs/refinedweb.yaml" \
  --source_name cc
