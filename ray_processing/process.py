import boto3
import time
import os
import argparse
from tqdm import tqdm
from yaml import safe_load
import glob
import subprocess
import json
from datetime import datetime
from typing import Any, Dict, Tuple, List
from baselines.core import process_single_file
from baselines.core.processor import split_large_file
from baselines.core.file_utils import read_jsonl, write_jsonl, delete_file, is_exists, is_s3, is_oss, get_file_size
from baselines.oss import oss
from baselines.oss.lock import DEFAULT_LOCK_FILE, get_worker_key
from baselines.lock.distri_lock import LockFactory
from baselines.redis import redis
from ray_processing import GLOBAL_FUNCTIONS
from ray_processing.utils import generate_untokenized_dataset_json, get_source_ref, get_source_ref_by_key
from task_asigning.asign_task import DEFAULT_TASKS_FILE_PATH
from baselines.task_queue.task import TaskItem
from baselines.task_queue.task_queue import TaskQueue

import ray
import traceback
import warnings


RAY_CHUNK_SUCCESS = 1
RAY_CHUNK_FAILURE = 0
LOCAL_CHUNK = "local"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source_ref_paths", help="paths to untokenized datasets refs, comma or space separated", type=str, nargs="+"
    )
    parser.add_argument(
        "--raw_data_dirpath",
        help="the path to the top data directory in the data hierarchy (from which to mirror the output path)",
    )
    parser.add_argument(
        "--task_file_path",
        type=str,
        default=DEFAULT_TASKS_FILE_PATH,
        help="task json file path",
    )
    parser.add_argument(
        "--queue_id",
        type=str,
        default="default",
        help="task queue id",
    )    
    parser.add_argument(
        "--oss_lock_file",
        type=str,
        default=DEFAULT_LOCK_FILE,
        help="oss lock file path",
    )
    parser.add_argument(
        "--oss_temp_dir",
        type=str,
        default="oss://si002558te8h/dclm/temp_files/",
        help="oss temp file dir",
    )    
    parser.add_argument(
        "--max_file_size_mb",
        type=int,
        default=1024,
        help="max file size mb",
    )    
    parser.add_argument("--shard_list_file", type=str, default=None, help="Path to a file containing a list of input shards.")
    parser.add_argument(
        "--shard_list_filters", type=str, nargs='+', help="List of substrings to filter the input shard list by."
    )
    parser.add_argument("--output_dir", required=True, help="Path to the output dir of the processed file.")
    parser.add_argument(
        "--readable_name", required=True, type=str, help="name given to tokenized dataset and reference json file name"
    )
    parser.add_argument(
        "--config_path",
        default="baselines/baselines_configs/c4.yaml",
        help="Path to the YAML file specifying the baseline.",
    )
    parser.add_argument(
        "--source_name", type=str, default="dcnlp_beta_pool", help="The name of the source of the jsonl file."
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="If > 1, will use a process pool with that many workers."
    )
    parser.add_argument("--overwrite", action="store_true", help="If set to true, will overwrite results.")
    parser.add_argument("--use_task", action="store_true", help="使用 task json 文件分配任务，否则直接使用 raw_data_dirpath.")
    parser.add_argument("--use_redis_task", action="store_true", help="use task 为 true 时，use_redis_task 会使用 redis 作为消息队列，否则使用 oss 文件.")    
    parser.add_argument("--retry_tasks", action="store_true", help="是否重新运行之前运行过的 tasks json")
    parser.add_argument("--output_has_dataset_name", action="store_true", help="output 目录中携带 dataset 名称")
    parser.add_argument("--ray_address", type=str, default="localhost:6379")
    parser.add_argument("--num_shards", type=int, default=None, help="Run on the first number of shards (for debugging)")
    parser.add_argument(
        "--ignore_failures", action="store_true", help="Skip steps if there are partial failures. Use sparingly."
    )
    parser.add_argument("--ray_use_working_dir", action='store_true', help="Working directory for ray.")
    parser.add_argument("--ray_num_cpus", type=int, default=1, help="Number of CPUs to use for each ray task.")
    parser.add_argument("--chunk_size", type=int, default=1, help="Number of temp files per task when splitting large files.")

    return parser.parse_args()


# Right now, this is just how I get clear space in /tmp which quickly gets filled by s3 reads
@ray.remote(max_calls=3)
def process_local_chunk(
        config_data, raw_data_dirpath, jsonl_relpath, source_name, base_output_path, workers, overwrite, max_file_size_mb=1024, shard_dir=None, oss_temp_dir=None, task_file_path=None, lock_file=None, chunk_size=1, use_redis_task=True, queue_id='default'
):
    try: 
        # 先检查是否为大文件需要拆分
        input_path = os.path.join(raw_data_dirpath, jsonl_relpath)
        file_size_mb = get_file_size(input_path) / (1024 * 1024)
        
        if file_size_mb > max_file_size_mb:
            print(f"文件大小为 {file_size_mb:.2f}MB，超过 {max_file_size_mb}MB，将进行文件切分处理")
            # 切分文件并保存到OSS临时目录
            temp_files = split_large_file(input_path, max_file_size_mb, oss_temp_dir)
            print(f"文件已切分为{len(temp_files)}个子文件，临时存储在{oss_temp_dir}")
            
            # 创建新任务添加到队列
            tasks_to_add = []

            if use_redis_task:
                for i in range(0, len(temp_files), chunk_size):
                    batch_files = temp_files[i:i+chunk_size]
                    task = TaskItem(
                        shard_dir=oss_temp_dir,
                        file_range=[0,-1],
                        is_temp=True,
                        worker=None,
                        original_shard_dir=shard_dir,
                        files=batch_files,
                    )
                    tasks_to_add.append(task)
                add_task_to_queue_redis(tasks_to_add, queue_id=queue_id)
            else:
                for i in range(0, len(temp_files), chunk_size):
                    batch_files = temp_files[i:i+chunk_size]
                    tasks_to_add.append({
                        "is_temp": True,
                        "shard_dir": oss_temp_dir, 
                        "file_range": [0,-1],
                        "worker": None,
                        "original_shard_dir": shard_dir, 
                        "files": batch_files,
                    })
                
                add_task_to_queue(tasks_to_add, task_file_path=task_file_path, lock_file=lock_file)
            print(f"已添加 {len(tasks_to_add)} 个临时文件任务到队列")
            return RAY_CHUNK_SUCCESS, 0, 0
        
        # 对于正常大小的文件，正常处理
        _, _, pages_in, pages_out, _ = process_single_file(
            config_data=config_data,
            raw_data_dirpath=raw_data_dirpath,
            jsonl_relpath=jsonl_relpath,
            source_name=source_name,
            base_output_path=base_output_path,
            workers=workers,
            overwrite=overwrite,
            max_file_size_mb=max_file_size_mb,  # 传递参数但在process_single_file中不会重复拆分
            is_temp_file=False  # 由任务中的is_temp决定，而不是在这里传递
        )
        return RAY_CHUNK_SUCCESS, pages_in, pages_out
    except Exception as e:
        print(f"process file failed ==== : {e}")
        traceback.print_exc()
        return RAY_CHUNK_FAILURE, 0, 0

def to_iterator(obj_ids, batch_size=100):
    while obj_ids:
        done, obj_ids = ray.wait(obj_ids, num_returns=min(batch_size, len(obj_ids)))
        for d in done:
            yield ray.get(d)


def list_shard_files(data_dirpath, num_shards=None, shard_list_file=None, shard_list_filters=None, file_range=None):
    assert bool(shard_list_file) ^ bool(data_dirpath), "Either shard_list_file or data_dirpath must be provided, but not both."
    
    def get_files_in_directory(data_dirpath):
        # 获取指定目录下的所有文件
        files = [f for f in os.listdir(data_dirpath) if all(s not in f for s in ['stats', 'global_stats.json'])]
        return files


    if shard_list_file is not None:
        with open(shard_list_file, "r") as f:
            shard_files = f.read().splitlines()
    elif is_s3(data_dirpath):
        s3 = boto3.resource('s3')
        bucket_name, path_within_bucket = data_dirpath.replace("s3://","").split("/", 1)
        path_within_bucket = path_within_bucket if path_within_bucket.endswith("/") else f'{path_within_bucket}/'
        bucket = s3.Bucket(bucket_name)
        shard_files = [x.key.replace(path_within_bucket, "") 
                       for x in bucket.objects.filter(Prefix=path_within_bucket) 
                       if all(s not in x.key for s in ['/stats/', 'global_stats.jsonl'])]
    elif is_oss(data_dirpath):
        bucket_name, path_within_bucket = data_dirpath.replace("oss://", "").split("/", 1)
        path_within_bucket = path_within_bucket if path_within_bucket.endswith("/") else f'{path_within_bucket}/'
        bucket = oss.Bucket(bucket_name)
        shard_files = [x.key.replace(path_within_bucket, "")
                       for x in oss.get_all_objects_iter(bucket, path_within_bucket)
                       if all(s not in x.key for s in ['/stats/', 'global_stats.jsonl']) and not x.key.endswith('/')]
    else:
        shard_files = get_files_in_directory(data_dirpath=data_dirpath)

    if num_shards is not None:
        shard_files = shard_files[:num_shards]

    if shard_list_filters is not None:
        shard_files = [s for s in shard_files if any(f in s for f in shard_list_filters)]

    if file_range is not None:
        shard_files = shard_files[file_range[0]: file_range[1]]

    return shard_files


def get_task_item_redis(queue_id='default'):
    queue = TaskQueue(redis.Client, queue_id=queue_id)
    task, all_finished = queue.acquire_task(worker=get_worker_key()), queue.all_finished()
    return task, all_finished


def get_task_item(retry_tasks=False, task_file_path=DEFAULT_TASKS_FILE_PATH, lock_file=DEFAULT_LOCK_FILE):
    asigned_task = None
    lock = LockFactory().create(redis.Client, lock_key=lock_file)
    all_finished = True
    # 分布式锁允许 1 hour 超时时间
    if lock.acquire_or_block(timeout=7200):
        # 改写 tasks.json 文件，领取任务
        try:
            task_items = list(read_jsonl(task_file_path))
            for i, task_item in enumerate(task_items):
                # 若存在没有完成的 task，记录下来
                if task_item['worker'] is None or \
                   task_item['worker']['status'] != "finished":
                    all_finished = False
                
                if not retry_tasks and (task_item['worker'] is not None and task_item['worker']['status'] != 'failed'):
                    continue
                elif retry_tasks and task_item['worker']['status'] == 'processed':
                    continue
                asigned_task = task_item
                task_items[i]['worker'] = {
                    'key': get_worker_key(),
                    'status': 'processing',
                    'process_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                break

            if asigned_task is None:
                lock.release()
                return None, all_finished

            write_jsonl(task_items, task_file_path)
            lock.release()
            return TaskItem(asigned_task['shard_dir'],
                            asigned_task['file_range'],
                            is_temp=asigned_task['is_temp'],
                            files=asigned_task['files'],
                            original_shard_dir=asigned_task.get('original_shard_dir', None)), False
        except BaseException as e:
            print(f"get task item failed: {e}")
            lock.release()
            return None, False
    else:
        print(f"Worker {get_worker_key()} could not acquire the lock within timeout.")
        return None, False    

def mark_task_item_finished_redis(task_item: TaskItem, queue_id='default'):
    queue = TaskQueue(redis.Client, queue_id=queue_id)
    queue.complete_task(task_item)
    return task_item.to_dict()
    
    
def mark_task_item_finished(shard_dir: str, file_range, task_file_path=DEFAULT_TASKS_FILE_PATH, lock_file=DEFAULT_LOCK_FILE, files=None):
    lock = LockFactory().create(redis.Client, lock_key=lock_file)
    # 分布式锁允许 1 hour 超时时间
    matched_task = None
    if lock.acquire_or_block(timeout=7200):
        # 改写 tasks.json 文件，领取任务
        try:
            task_items = list(read_jsonl(task_file_path))
            for i, task_item in enumerate(task_items):
                # 判断匹配方式：如果提供了files，按files匹配；否则按shard_dir和file_range匹配
                if files and "files" in task_item and set(files) == set(task_item["files"]):
                    task_items[i]['worker'] = {
                        'key': task_items[i]['worker'].get('key'),
                        'status': 'finished',
                        'process_time': task_items[i]['worker'].get('process_time'),
                        'finish_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    matched_task = task_item  # 保存匹配的任务
                    del task_items[i]
                    break
                elif task_item['shard_dir'] == shard_dir and task_item['file_range'] == file_range:
                    task_items[i]['worker'] = {
                        'key': task_items[i]['worker'].get('key'),
                        'status': 'finished',
                        'process_time': task_items[i]['worker'].get('process_time'),
                        'finish_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    matched_task = task_item  # 保存匹配的任务
                    del task_items[i]
                    break

            print("[=== MARK FINISH TASK ===] {}".format(matched_task))
            write_jsonl(task_items, task_file_path)

            # write to finished_tasks.json
            fin_task_file = oss.finished_task_file(task_file_path)
            if is_exists(fin_task_file):
                fin_task_items = list(read_jsonl(fin_task_file))
            else:
                fin_task_items = []

            if matched_task is not None:
                fin_task_items.append(matched_task)
            write_jsonl(fin_task_items, fin_task_file)            

            lock.release()
            return matched_task  # 返回匹配的任务信息
        except BaseException as e:
            print(f"标记任务完成失败: {e}")
            lock.release()
    else:
        print(f"Worker {get_worker_key()} could not acquire the lock within timeout.")
    return None

def mark_task_item_failed_redis(task_item: TaskItem, queue_id):
    queue = TaskQueue(redis.Client, queue_id=queue_id)
    queue.requeue_task(task_item)

def mark_task_item_failed(shard_dir: str, file_range, task_file_path=DEFAULT_TASKS_FILE_PATH, lock_file=DEFAULT_LOCK_FILE, files=None):
    lock = LockFactory().create(redis.Client, lock_key=lock_file)
    matched_task = None
    if lock.acquire_or_block(timeout=7200):
        try:
            task_items = list(read_jsonl(task_file_path))
            total = len(task_items)
            for i, task_item in enumerate(task_items):
                # 判断匹配方式：如果提供了files，按files匹配；否则按shard_dir和file_range匹配
                if files and "files" in task_item and set(files) == set(task_item["files"]):
                    task_items[i]['worker'] = {
                        'key': task_items[i]['worker'].get('key'),
                        'status': 'failed',
                        'process_time': task_items[i]['worker'].get('process_time'),
                        'fail_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    matched_task = task_item
                    del task_items[i]
                    break
                elif task_item['shard_dir'] == shard_dir and task_item['file_range'] == file_range:
                    task_items[i]['worker'] = {
                        'key': task_items[i]['worker'].get('key'),
                        'status': 'failed',
                        'process_time': task_items[i]['worker'].get('process_time'),
                        'fail_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    matched_task = task_item
                    del task_items[i]
                    break

            if matched_task:
                task_items.append(matched_task)
                
            print("[=== MARK FAILED TASK ===] {}".format(matched_task))                
            write_jsonl(task_items, task_file_path)

            lock.release()
            return matched_task
        except BaseException as e:
            print(f"标记任务完成失败: {e}")
            lock.release()
    else:
        print(f"Worker {get_worker_key()} could not acquire the lock within timeout.")
    return None

        
def process_all():
    args = parse_args()
    
    with_init = True 
    while args.use_task:
        if args.use_redis_task:
            task_item, all_finished = get_task_item_redis(args.queue_id)
        else:
            task_item, all_finished = get_task_item(args.retry_tasks,
                                                    task_file_path=args.task_file_path,
                                                    lock_file=args.oss_lock_file)
        if task_item is None:
            if not all_finished:
                time.sleep(10)
                continue
            else:
                return
        process_task_item(args, task_item, with_init)
        with_init = False
        time.sleep(1)
    process_task_item(args, None, with_init)

def add_task_to_queue(tasks: List[dict], task_file_path=DEFAULT_TASKS_FILE_PATH, lock_file=DEFAULT_LOCK_FILE) -> bool:
    """
    将新任务批量添加到任务队列的最前面
    
    :param tasks: 要添加的任务字典列表，例如 [{"shard_dir": "oss://...", "files": ["file1.json", "file2.json"], "worker": None, "is_temp": True}]
    :return: bool, 添加是否成功
    """
    lock = LockFactory().create(redis.Client, lock_key=lock_file)
    
    # 获取分布式锁，超时时间 1 小时
    if not lock.acquire_or_block(timeout=7200):
        print(f"Worker {get_worker_key()} 无法在超时时间内获取锁。")
        return False
    
    try:
        ori_tasks = list(read_jsonl(task_file_path))
        print(f"读取到 {len(ori_tasks)} 个任务")
        
        # 将新任务插入到任务列表开头
        new_tasks = tasks + ori_tasks
        print(f"添加 {len(tasks)} 个新任务后，任务数量: {len(new_tasks)}")
        
        write_jsonl(new_tasks, task_file_path)
        lock.release()
        return True   
    except json.JSONDecodeError as e:
        print(f"JSON 解析错误: {e}")
        lock.release()
        return False
    except IOError as e:
        print(f"文件读写错误: {e}")
        lock.release()
        return False
    except Exception as e:
        print(f"添加任务到队列时发生未知错误: {e}")
        lock.release()
        return False

def add_task_to_queue_redis(tasks: List[TaskItem], queue_id='default') -> bool:
    queue = TaskQueue(redis.Client, queue_id=queue_id)
    for task in tasks:
        queue.put_task(task)
    return True
    

def process_task_item(args, task_item: TaskItem|None, with_init=True):
    print("======== task: {}".format(task_item))
    os.environ["RAY_LOG_TO_STDERR"] = "1"
    
    task_input_dirpath, shard_name = '', ''
    origin_dataset_name = ''
    # json_path 文件用于检测该 input 是否曾经跑完
    json_path = f"exp_data/datasets/untokenized/{args.readable_name}.json"
    file_range = None
    files = None
    is_temp = False
    shard_dir = None
    
    if task_item is not None:
        origin_shard_dir = task_item.get_original_shard_dir()
        shard_dir = task_item.get_shard_dir() if not origin_shard_dir else origin_shard_dir
        shard_name = shard_dir.split('/')[-2] if '/' in shard_dir else shard_dir
        origin_dataset_name = shard_dir.split('/')[-3]
        task_input_dirpath = task_item.get_shard_dir()
        file_range = task_item.get_file_range()
        files = task_item.get_files() if hasattr(task_item, 'get_files') else []
        is_temp = task_item.is_temp if hasattr(task_item, 'is_temp') else False
        json_path = f"exp_data/datasets/untokenized/{args.readable_name}_{shard_name}.json"
        print(f"task shard dir: {shard_dir}")    
    
    if not args.overwrite:
        assert not os.path.exists(
            json_path
        ), f"{json_path} already exists. Try changing --readable_name or deleting the existing file."

    source_refs = None
    if args.source_ref_paths is not None:
        source_ref_paths = [p.strip() for paths in args.source_ref_paths for p in paths.split(",") if p.strip()]
        source_refs = [get_source_ref(s) for s in source_ref_paths]
        assert len(source_refs) == 1, "For now only one source is supported"
        args.raw_data_dirpath = source_refs[0]["dataset_url"]
    else:
        source_ref = get_source_ref_by_key(args.raw_data_dirpath, "dataset_url")
        source_refs = [source_ref] if source_ref else []
    
    if with_init:
        if args.ray_use_working_dir:
            ray.init(address=args.ray_address, runtime_env={"working_dir": "./", "excludes": ["tests/"],
                                                            "env_vars": {
                                                                "REDIS_HOST": os.getenv("REDIS_HOST"),
                                                                "REDIS_PORT": os.getenv("REDIS_PORT"),
                                                            }})
        else:
            ray.init(address=args.ray_address, runtime_env={
                "env_vars": {
                    "REDIS_HOST": os.getenv("REDIS_HOST"),
                    "REDIS_PORT": os.getenv("REDIS_PORT"),
                }
            })

    config_path = args.config_path
    
    def get_output_dir(output, shard_name, with_dataset_name=False):
        if shard_name == '':
            return os.path.join(output, args.readable_name)
        if origin_dataset_name == '' or not with_dataset_name:
            # output dir: oss://si002558te8h/dclm/output/sci_test/CC-MAIN-2014-11/
            return os.path.join(output, args.readable_name, shard_name)
        # output dir: oss://si002558te8h/dclm/output/sci_test/Math/CC-MAIN-2014-11/
        return os.path.join(output, args.readable_name, origin_dataset_name, shard_name)    
    
    output_dir = get_output_dir(args.output_dir, shard_name, args.output_has_dataset_name)
    source_name = args.source_name
    
    # base output path 去掉 config_name，使用自定义的 readable name 去区分不同的 pipeline
    base_output_path = output_dir

    # Collect the global stats file, which is used to record / resume a data processing pipeline
    global_stats_path = os.path.join(base_output_path, "global_stats.jsonl")
    global_stats = []
    if is_exists(global_stats_path):
        if args.overwrite:
            delete_file(global_stats_path)
        else:
            try:
                global_stats = list(read_jsonl(global_stats_path))
            except BaseException:
                global_stats = []

    # Process the yaml file into chunks of either contiguous local functions OR single global functions
    with open(config_path, "r") as yaml_file:
        config_data = safe_load(yaml_file)
        config_data = {v["source"]: v for v in config_data}
    source_data = config_data[source_name]
    steps = source_data["steps"]

    chunks = []  # Contains either the global function specification or LOCAL_CHUNK for a local chunk
    prev_step_global = True  # Keeps track of whether the last step seen was global
    for s in steps:
        if "func" in s and s["func"] in GLOBAL_FUNCTIONS:
            if len(chunks) == 0:
                raise Exception("Using a global operation as the first step is not currently supported.")
            chunks.append(s)
            prev_step_global = True
        else:
            if prev_step_global:
                chunks.append(LOCAL_CHUNK)
            prev_step_global = False

    # Begin processing the chunks
    true_start = time.time()
    working_dir = task_input_dirpath if task_input_dirpath != '' else args.raw_data_dirpath

    overwrite = args.overwrite

    task_success = True
    for i, c in enumerate(chunks):
        chunk_start = time.time()
        step_name = LOCAL_CHUNK if c == LOCAL_CHUNK else c["func"]
        resumed_chunk = False
        
        # 获取当前目录下的文件
        if files and is_temp:
            # 如果任务中指定了文件列表，就使用这些文件 
            shard_files = []
            for f in files:
                shard_files.append(os.path.basename(f))
        else:
            # 否则，从目录中获取文件列表
            shard_list_filters = args.shard_list_filters
            shard_files = list_shard_files(working_dir, args.num_shards, args.shard_list_file, shard_list_filters, file_range=file_range)

        print("[=== DEALING SHARD FILES ===]: {}".format(shard_files))
        if not shard_files:
            print(f"No files found in {working_dir}")
            break
            
        shard_extension = os.path.splitext(shard_files[0])[-1][1:]

        # If chunk has already been processed according to global stats, then skip it
        if i < len(global_stats) and step_name == global_stats[i]["name"]:
            num_failures = global_stats[i].get("num_failures", 0)
            if num_failures == 0 or args.ignore_failures:
                if num_failures > 0:
                    warnings.warn(
                        f"{num_failures} failures are being ignored, which may significantly and unpredictably impact final results."
                    )
                print(f"Skipping chunk {i} with name {step_name}")
                working_dir = global_stats[i]["working_dir"]
                continue
            elif num_failures > 0 and not args.overwrite:
                resumed_chunk = True
                working_dir = global_stats[i - 1]["working_dir"] if i > 0 else working_dir

        print(f"Starting chunk {i} with name {step_name}, # of input jsonls = {len(shard_files)}")

        if resumed_chunk:
            shard_files = global_stats[i]["failed_shards"]

        # Process the chunk according to whether it is local or global
        if c == LOCAL_CHUNK:
            # 不在这里检测大文件并拆分，只使用Ray处理文件
            ret = []
            for idx, jsonl_relpath in enumerate(shard_files):
                ret.append(
                    process_local_chunk.options(num_cpus=args.ray_num_cpus).remote(
                        config_data, working_dir, jsonl_relpath, source_name, base_output_path, args.workers, overwrite, args.max_file_size_mb, shard_dir=shard_dir, oss_temp_dir=args.oss_temp_dir,
                        task_file_path=args.task_file_path,
                        lock_file=args.oss_lock_file,
                        chunk_size=args.chunk_size,
                        use_redis_task=args.use_redis_task,
                        queue_id=args.queue_id,
                    )
                )
            
            if ret:
                for x in tqdm(to_iterator(ret), total=len(ret)):
                    pass

                ret = ray.get(ret)
                successes = sum(r[0] for r in ret)
                failures = len(ret) - successes
                pages_in = sum(r[1] for r in ret)
                pages_out = sum(r[2] for r in ret)
                failed_shards = [s for i, s in enumerate(shard_files) if i < len(ret) and ret[i][0] == RAY_CHUNK_FAILURE]

                # Make sure the working_dir has processed_data/ at the end
                working_dir = os.path.join(base_output_path, "processed_data/")

                # If resuming a chunk that partially errored, update the global stats instead of appending a new row
                if resumed_chunk:
                    global_stats = global_stats[: i + 1]
                    global_stats[i]["resumptions"] += 1
                    global_stats[i]["secs"] += time.time() - chunk_start
                    global_stats[i]["pages_in"] += sum(r[1] for i, r in enumerate(ret) if i < len(ret))
                    global_stats[i]["pages_out"] += sum(r[2] for i, r in enumerate(ret) if i < len(ret))
                    global_stats[i].update(
                        {"num_successes": successes, "num_failures": failures, "failed_shards": failed_shards}
                    )
                else:
                    global_stats.append(
                        {
                            "name": LOCAL_CHUNK,
                            "secs": time.time() - chunk_start,
                            "num_successes": successes,
                            "num_failures": failures,
                            "pages_in": pages_in,
                            "pages_out": pages_out,
                            "working_dir": working_dir,
                            "resumptions": 0,
                            "failed_shards": failed_shards,
                        }
                    )

                overwrite = False
                write_jsonl(global_stats, global_stats_path, "w")

                if failures > 0:
                    task_success = False
                    warnings.warn(
                        f"Local chunk failed on {failures} shards out of {len(ret)}. This may significantly and unpredictably affect final results. Re-running this local chunk by using the same yaml config and turning off the --ignore_failures flag."
                    )
                    if not args.ignore_failures:
                        raise Exception(f"Exiting due to local failures. ")
        else:
            step = c
            kwargs = {k: v for k, v in step.items() if k not in ["func"]}

            # Assumption: Global functions will return a working directory
            working_dir = GLOBAL_FUNCTIONS[step["func"]](working_dir, shard_files, base_output_path, **kwargs)
            global_stats.append({"name": step["func"], "secs": time.time() - chunk_start, "working_dir": working_dir})

            # If the last step and working_dir is not already the desired base_output_path, make sure to sync
            if i == len(chunks) - 1 and base_output_path != working_dir:
                print(f"Final sync required back to desired ouput path: from {working_dir} to {base_output_path}")
                sync_list = ["aws", "s3", "sync", working_dir, base_output_path]
                process = subprocess.Popen(sync_list)
                process.wait()
            write_jsonl(global_stats, global_stats_path, "w")

        print("Chunk time: " + str(time.time() - chunk_start))
    print("Total time: " + str(time.time() - true_start))

    # Generate the dataset reference json
    # dataset_json = generate_untokenized_dataset_json(args, source_refs, base_output_path, data_key=shard_extension)
    # with open(json_path, "w") as ref_file:
    #     json.dump(dataset_json, ref_file, indent=4)
    
    if task_item is not None:    
        # 更新：接收返回的任务信息
        if task_success:
            if args.use_redis_task:
                updated_task = mark_task_item_finished_redis(task_item, args.queue_id)
            else:
                updated_task = mark_task_item_finished(shard_dir, file_range,
                                                       task_file_path=args.task_file_path,
                                                       lock_file=args.oss_lock_file,
                                                       files=files)
        else:
            if args.use_redis_task:
                mark_task_item_failed_redis(task_item, args.queue_id)
            else:
                mark_task_item_failed(shard_dir, file_range,
                                      task_file_path=args.task_file_path,
                                      lock_file=args.oss_lock_file,
                                      files=files)
            # 失败则不删除临时文件，用于下次重试
            updated_task = False
    
        # 使用更新后的任务信息 - 如果有返回值的话
        if updated_task:
            # 从任务信息中获取最新的 is_temp 和 files 信息
            is_temp = updated_task.get("is_temp", False)
            task_files = updated_task.get("files", [])

            # 如果是临时文件任务，删除指定的临时文件
            if is_temp and task_files:
                print(f"[=== DELETE TEMP FILES START ===]: {task_files}")
                try:
                    for file_path in task_files:
                        if is_oss(file_path):
                            # 直接删除OSS文件
                            bucket_name, path = oss.split_file_path(file_path)
                            bucket = oss.Bucket(bucket_name)
                            bucket.delete_object(path)
                        else:
                            # 删除本地文件
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            print(f"[=== DELETE TEMP FILES SUCCESS ===]")
                except Exception as e:
                    print(f"[=== DELETE TEMP FILES FAILED ===]: {e}")

if __name__ == "__main__":
    process_all()
    exit(0)
