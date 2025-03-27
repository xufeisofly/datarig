# -*- coding: utf-8 -*-
import os
import argparse
import json
from baselines.core.file_utils import write_jsonl
from baselines.oss import oss
from typing import List

"""
可以用这个脚本生成 task 用于分片，也可以自己写 tasks.json 文件
asign_task.py parses the dclm input directory and split tasks, generating a tasks.json file
for all the ray_process.py to accept.

{
  tasks: [
    {
      "shard_dir": "oss://si002558te8h/dclm/origin/CC-MAIN-2014-51/",
      "file_range": [0, -1],
      "worker": {
        "key": "192.168.0.2_828282",
        "status": "processing" // processing/success/fail
      } // or null
    }, {
      // ...
    }
  ]
}
"""

class TaskItem:
    def __init__(self, shard_dir, file_range: List[int], worker=None, is_temp=False, files=None, original_shard_dir=None) -> None:
        self._shard_dir = shard_dir
        self._file_range = file_range
        self._worker = worker
        self.is_temp = is_temp  # 添加 is_temp 属性
        self._files = files or []  # 添加 files 属性，默认为空列表
        self._original_shard_dir = original_shard_dir

    def get_shard_dir(self):
        return self._shard_dir

    def get_original_shard_dir(self):
        return self._original_shard_dir

    def get_file_range(self):
        return self._file_range
        
    def get_files(self):
        return self._files

    def to_dict(self) -> dict:
        return {
            "shard_dir": self._shard_dir,
            "file_range": self._file_range,
            "worker": self._worker,
            "is_temp": self.is_temp,  # 在字典中包含 is_temp
            "files": self._files  # 在字典中包含 files
        }

def create_task_items(shard_dir: str, mode: str, chunk_size: int) -> List[dict]:
    tasks = []
    bucket_name, path = oss.split_file_path(shard_dir) 
    bucket = oss.Bucket(bucket_name)
    
    # 仅在 dedup 模式下处理 subject= 开头的目录
    if mode == 'dedup':
        dir_basename = os.path.basename(path.rstrip('/'))
        if dir_basename.startswith('subject='):
            # 处理 subject 目录下的 processed_data 文件夹
            processed_data_path = os.path.join(path, 'processed_data')
            # if oss.folder_exists(bucket, processed_data_path):
            processed_files = oss.get_sub_files(bucket, processed_data_path)
            processed_data_dir = os.path.join("oss://" + bucket_name, processed_data_path)
            
            if len(processed_files) > 0:
                if chunk_size == -1:
                    # dedup 模式下不写入 files 信息
                    tasks.append(TaskItem(processed_data_dir, [0, -1], 
                                        original_shard_dir=shard_dir).to_dict())
                else:
                    total = len(processed_files)
                    start = 0
                    while start < total:
                        end = start + chunk_size
                        if end >= total:
                            end = total
                        file_range = [start, end]
                        # dedup 模式下不写入 files 信息
                        tasks.append(TaskItem(processed_data_dir, file_range,
                                            original_shard_dir=shard_dir).to_dict())
                        start += chunk_size
            
            # dedup 模式下处理完 processed_data 就返回，不处理其他子目录
            return tasks
    
    # 如果不是 dedup 模式或者不是 subject= 目录，按原来的逻辑处理
    file_paths = oss.get_sub_files(bucket, path)
    if len(file_paths) > 0:
        if chunk_size == -1:
            tasks.append(TaskItem(shard_dir, [0, -1], files=file_paths).to_dict())
        else:
            total = len(file_paths)
            start = 0
            while start < total:
                end = start+chunk_size
                if end >= total:
                    end = total
                file_range = [start, end]
                tasks.append(TaskItem(shard_dir, file_range, files=[]).to_dict())
                start += chunk_size

    # 递归处理子目录
    sub_dirs = oss.get_sub_folders(bucket, path)
    if len(sub_dirs) == 0:
        return tasks

    for sub_dir in sub_dirs:
        sub_dir = os.path.join("oss://" + bucket_name, sub_dir)
        tasks += create_task_items(sub_dir, mode, chunk_size)
    return tasks

    
def asign_task(parent_dir: str, tasks_file_path: str, mode: str='process', chunk_size=-1):
    all_task_items = create_task_items(parent_dir, mode, chunk_size)
        
    data = all_task_items
    write_jsonl(data, tasks_file_path)
        
    task_bucket_name, task_file = oss.split_file_path(tasks_file_path)
    existed = oss.Bucket(task_bucket_name).object_exists(task_file)

    if existed:
        print(f"Success: {len(all_task_items)} tasks generated")
    else:
        print(f"Failed")

    bucket_name, fin_path = oss.split_file_path(oss.finished_task_file(tasks_file_path)) 
    oss.Bucket(bucket_name).delete_object(fin_path)

        
DEFAULT_TASKS_FILE_PATH = "oss://si002558te8h/dclm/pool/process_tasks.jsonl"
DEFAULT_PARENT_DIR = "oss://si002558te8h/dclm/origin/"


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent_dir", help="", type=str, default=DEFAULT_PARENT_DIR)
    parser.add_argument("--tasks_file_path", help="", type=str, default=DEFAULT_TASKS_FILE_PATH)
    parser.add_argument("--chunk_size", help="", type=int, default=-1)
    parser.add_argument("--mode", help="process/dedup", type=str, default='process')
    args = parser.parse_args()    
    asign_task(args.parent_dir, args.tasks_file_path, args.mode, args.chunk_size)
