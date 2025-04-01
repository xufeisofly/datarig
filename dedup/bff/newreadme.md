# DCLM 数据处理系统

## 系统概述

DCLM（Data Cleaning and Language Model预处理）系统是一个用于大规模数据处理和去重的工具，专为语言模型训练数据准备而设计。系统由三个主要组件构成：任务分配器、基于布隆过滤器的去重处理器和文件采样工具。系统主要基于OSS对象存储服务运行，支持处理TB级数据集。

## 系统组件

### 1. 任务分配器 (asign_task.py)

此脚本用于扫描输入目录结构并创建任务列表，将大型数据集分割成可管理的任务块。

#### 主要功能：
- 递归扫描OSS存储桶中的目录结构
- 支持按指定大小（chunk_size）对数据进行分片
- 生成任务描述文件，供处理节点获取任务

#### 任务文件格式：
```json
{
  "shard_dir": "oss://path/",
  "file_range": [0, 10],
  "worker": null,
  "is_temp": false,
  "files": [],
  "original_shard_dir": null
}
```

### 2. 布隆过滤器去重处理器 (main.rs)

使用Rust实现的高性能数据处理工具，通过布隆过滤器检测并移除重复内容。

#### 主要功能：
- 支持多种去重策略（exact、substring、old-both等）
- 使用布隆过滤器高效检测重复内容
- 分布式任务处理架构
- 支持多线程并行处理
- 可选的标注模式（不删除重复内容，仅标记它们）

## 使用方法

### 步骤1：任务分配

使用`asign_task.py`脚本扫描数据目录并生成任务文件：

```bash
python3 task_asigning/asign_task.py \
  --parent_dir "oss://path/" \
  --mode "dedup" \
  --chunk_size 2000 \
  --tasks_file_path "oss://path/dedup_tasks.jsonl"
```

参数说明：
- `--parent_dir`: 要处理的数据目录
- `--mode`: 处理模式，支持"process"和"dedup"
- `--chunk_size`: 每个任务处理的文件数量，-1表示不分片
- `--tasks_file_path`: 输出的任务文件路径

### 步骤2：执行去重处理

使用布隆过滤器去重工具处理任务：

```bash
cargo run --release bff \
  --tasks-file oss://path/dedup_tasks.jsonl \
  --output-directory oss://output/ \
  --expected-ngram-count 1000000 \
  --fp-rate 0.01 \
  --min-ngram-size 13 \
  --max-ngram-size 13 \
  --filtering-threshold 0.8 \
  --remove-type old-both \
  --annotate
```

参数说明：
- `--tasks-file`: 任务文件路径
- `--output-directory`: 处理后数据输出目录
- `--expected-ngram-count`: 预期的n-gram数量，用于布隆过滤器优化
- `--fp-rate`: 布隆过滤器的误判率
- `--min-ngram-size`/`--max-ngram-size`: n-gram大小范围
- `--filtering-threshold`: 重复过滤阈值，取值0-1
- `--remove-type`: 去重策略，支持exact、substring、both、old-both等
- `--annotate`: 启用标注模式，不删除重复内容，而是添加标记

## 去重策略说明

- `exact`: 完全匹配去重，整个文档匹配才去重
- `substring`: 子串去重，检测并移除文档中的重复子串
- `both`: 先进行文档级别去重，然后对未去重的文档进行段落级别去重
- `old-both`: 传统的混合去重策略，性能较好
- `naive-both`: 简化的混合去重策略

## 注意事项

1. 任务系统使用OSS锁机制确保分布式环境下的任务不会被多个工作节点重复处理
2. 布隆过滤器参数需要根据数据规模合理设置，以平衡内存使用和去重效果
3. 处理大规模数据时，建议使用较大的chunk_size值进行任务分片
4. 完成的任务会被标记为"finished"并写入到相应的完成任务文件中

## 任务状态监控

系统会自动记录任务处理状态，包括：
- 待处理：尚未分配给工作节点的任务
- 处理中：已分配但尚未完成的任务
- 已完成：成功处理的任务
- 失败：处理过程中出错的任务

### 3. 文件采样工具 (sample.py)

用于从OSS存储中按比例采样文件，支持基于学科（subject）分类的数据集，确保采样后的数据集大小满足要求。

#### 主要功能：
- 支持按学科（subject=xxx格式的文件夹）进行采样
- 提供两种采样模式：均衡模式（balance）和按比例模式（proportional）
- 自动估算压缩文件（.jsonl.gz）的解压后大小
- 确保采样后的数据集总大小不超过指定值
- 保持原始数据的目录结构

#### 使用方法：

```bash
python3 sample.py \
  --input_dir "oss://your-bucket/input-path/" \
  --output_dir "oss://your-bucket/output-path/" \
  --total_size_gb 10.0 \
  --seed 42 \
  --mode "proportional"
```

参数说明：
- `--input_dir`: 输入OSS路径，包含subject=xxx格式的子文件夹
- `--output_dir`: 输出OSS路径，将保持原始的subject目录结构
- `--total_size_gb`: 采样目标解压后总大小(GB)
- `--seed`: 随机种子，用于确保采样结果可复现
- `--mode`: 采样模式，支持两种选项：
  - `balance`: 均衡各学科大小，尽量使每个学科获得相同的数据量
  - `proportional`: 按原始比例采样，保持各学科数据量的原始比例

#### 工作流程：
1. 扫描输入目录下所有符合"subject=xxx"格式的子文件夹
2. 统计每个subject文件夹中的.jsonl.gz文件
3. 通过采样估算压缩文件的平均压缩率
4. 基于采样模式（balance或proportional）计算每个学科的目标大小
5. 对每个学科文件夹进行采样，直到达到目标大小
6. 将采样的文件复制到目标位置，保持原始的目录结构
