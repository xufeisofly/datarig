# 使用方式

## 单机

```
cargo run --release -- \
  --input "oss://si002558te8h/dclm/output/c4_noclean/" \
  --output "oss://si002558te8h/dclm/output/c4_filtered/"
```

## 分布式

基于 redis 任务调度，需要首先配置好 master 节点，然后

配置 worker 节点列表 server_list.txt

然后配置 worker 节点基础环境
```
./config.sh
```

使用脚本生成任务，如下脚本每 40 个文件一个任务
```
python task_asigning/asign_task.py \
  --parent_dir "oss://si002558te8h/dclm/output/c4_noclean/" \
  --mode "process" \
  --chunk_size 40 \
  --queue_id "quality" \
  --use_redis_task
  
=> Success: 10 tasks generated // 生成 10 个任务
```

在 master 节点上运行
```
./run.sh \
  --output "oss://si002558te8h/dclm/output/rust_distribute_test2/" \
  --queue-id "quality" \
  --use-redis-task
```

此命令会在 worker 节点和 master 节点上打开 tmux new -s datarig 然后运行 quality-rs 质量过滤程序，可在每个节点上运行如下命令进行日志查看
```
tmux attach -t datarig
```

使用 scripts 中的脚本进行全局进度查看
```
cd datarig/
python scripts/sync_queue.py --queue_id quality
```
