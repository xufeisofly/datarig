# Process layer

generate tasks.json
```
python3 task_asigning/asign_task.py --parent_dir "oss://si002558te8h/dclm/pool/"
```

## Run by docker

```
docker run --rm -it dclm-sci \
  --readable_name sci_test \
  --output_dir "oss://si002558te8h/dclm/output" \
  --config_path "baselines/baselines_configs/sci.yaml" \
  --source_name cc \
  --overwrite
```

or specifiy certain input shard
```
docker run --rm -it dclm-sci \
  --readable_name sci_test_2 \
  --raw_data_dirpath "oss://si002558te8h/dclm/pool" \
  --output_dir "oss://si002558te8h/dclm/output" \
  --config_path "baselines/baselines_configs/sci.yaml" \
  --source_name cc \
  --overwrite
```

## Run by k8s

```
export KUBECONFIG=/path/to/your/kubeconfig
kubectl create secret docker-registry zj-docker-registry \
  --docker-server=cr.registry.res.cloud.zhejianglab.com \
  --docker-username=xxx \
  --docker-password=xxx \
  --docker-email=xxx
  
kubectl apply -f deployment.yaml  
```

# Dedup layer

## Directly run
```
echo "export RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup" >> ~/.bashrc
echo "export RUSTUP_DIST_SERVER=https://mirrors.tuna.tsinghua.edu.cn/rustup" >> ~/.bashrc

// 安装 rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

export PATH="$HOME/.cargo/bin:$PATH"

sudo apt-get install -y libssl-dev
sudo apt install pkg-config

// 编译
cd dedup/bff && cargo build --release

cargo run --release bff \
   --inputs oss://si002558te8h/dclm/output/dclm_baseline_refinedweb/processed_data \
   --output-directory oss://si002558te8h/dclm/output/dclm_baseline_refinedweb/deduped_data \
   --expected-ngram-count 5 \
   --fp-rate 0.01 \
   --min-ngram-size 13 \
   --max-ngram-size 13 \
   --filtering-threshold 0.8 \
   --remove-type old-both \
   --annotate 
```

## Run by docker

```
docker run --rm -it \
  -e OSS_ACCESS_KEY_ID=xxx \
  -e OSS_ACCESS_KEY_SECRET=xxx \
  dedup-bff \
  --inputs oss://si002558te8h/dclm/output/sci_test/CC-MAIN-2014-10/sci/processed_data \
  --output-directory oss://si002558te8h/dclm/output/sci_test/CC-MAIN-2014-10/sci/deduped_data \
  --expected-ngram-count 5 \
  --fp-rate 0.01 \
  --min-ngram-size 13 \
  --max-ngram-size 13 \
  --filtering-threshold 0.8 \
  --remove-type old-both \
  --annotate
```


