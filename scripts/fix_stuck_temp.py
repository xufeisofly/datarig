# -*- coding: utf-8 -*-
import json
import logging
from baselines.oss import oss
from baselines.oss.lock import SimpleOSSLock, DEFAULT_LOCK_FILE
from baselines.core.file_utils import is_exists, write_jsonl, read_jsonl
from datetime import datetime
from task_asigning.asign_task import DEFAULT_TASKS_FILE_PATH
# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# 设置阈值
THRESHOLD_STR = "2025-03-28 04:30:00"
THRESHOLD = datetime.strptime(THRESHOLD_STR, "%Y-%m-%d %H:%M:%S")


def filter_jsonl(file_path: str, is_temp: bool):
    for record in read_jsonl(file_path):
        worker = record.get("worker", {})
        if worker is None:
            continue
        status = worker.get("status")
        process_time_str = worker.get("process_time")

        # 仅对 status 为 "processing" 并且 process_time 存在的记录进行判断
        if status == "processing" and process_time_str and record['is_temp'] == is_temp:
            try:
                process_time = datetime.strptime(process_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"时间格式错误: {e}")
                continue

            if process_time < THRESHOLD:
                yield record

def check_temp_file_exist(input_file):
    # 替换为你的 JSONL 文件路径
    data = list(filter_jsonl(input_file, is_temp=True))

    invalid_f = []
    for record in data:
        files = record['files']
        for f in files:
            if not is_exists(f):
                invalid_f.append(f)

    return invalid_f

def recover_temp_file_tasks(task_file, invalid_fs=[]):
    lock = SimpleOSSLock(DEFAULT_LOCK_FILE)    
    try:
        temp_tasks_to_recover = list(filter_jsonl(task_file, is_temp=True))
        all_tasks = list(read_jsonl(task_file))

        def task_item_doesnt_have_invalid_files(task_item, invalid_fs):
            for task_file in task_item['files']:
                if task_file in invalid_fs:
                    return False
            return True

        def is_task_to_recover(task_item, invalid_fs):
            for t in temp_tasks_to_recover:
                if task_item == t:
                    if task_item_doesnt_have_invalid_files(task_item, invalid_fs):
                        return True
            return False

        count = 0
        for i, task_item in enumerate(all_tasks):
            if is_task_to_recover(task_item, invalid_fs):
                all_tasks[i]['worker'] = None
                count += 1

        write_jsonl(all_tasks, task_file)
        lock.release()
        return count
    except BaseException as e:
        print(e)
        lock.release()
        return -1

def recover_ori_file_tasks(task_file):
    lock = SimpleOSSLock(DEFAULT_LOCK_FILE)    
    try:
        temp_tasks_to_recover = list(filter_jsonl(task_file, is_temp=False))
        all_tasks = list(read_jsonl(task_file))

        def is_task_to_recover(task_item):
            for t in temp_tasks_to_recover:
                if task_item == t:
                    return True
            return False

        count = 0
        for i, task_item in enumerate(all_tasks):
            if is_task_to_recover(task_item):
                all_tasks[i]['worker'] = None
                count += 1

        write_jsonl(all_tasks, task_file)
        lock.release()
        return count
    except BaseException as e:
        print(e)
        lock.release()
        return -1


if __name__ == '__main__':
    file_path = DEFAULT_TASKS_FILE_PATH
    invalid_fs = check_temp_file_exist(file_path)
    
    print("====start")
    ret = recover_temp_file_tasks(file_path, invalid_fs)
    print("====end", ret)
    ret = recover_ori_file_tasks(file_path)
    print("====end", ret)

    # 救场用，有时候 finished_process_tasks.jsonl 会出现 null 导致崩溃，需要重新传一个
    # fin_path = './finished_process_tasks.jsonl'
    # dir_path = "oss://si002558te8h/dclm/"
    # bucket_name, path = oss.split_file_path(dir_path)
    # bucket = oss.Bucket(bucket_name)
    # oss.upload_file_to_oss(fin_path, path, bucket)
