import json
from datetime import datetime
from baselines.core import read_jsonl
from baselines.core.file_utils import write_jsonl

# 设置阈值
THRESHOLD_STR = "2025-03-25 18:47:48"
THRESHOLD = datetime.strptime(THRESHOLD_STR, "%Y-%m-%d %H:%M:%S")


def filter_jsonl(file_path: str):
    for record in read_jsonl(file_path):
        worker = record.get("worker", {})
        if worker is None:
            continue
        status = worker.get("status")
        process_time_str = worker.get("process_time")

        # 仅对 status 为 "processing" 并且 process_time 存在的记录进行判断
        if status == "processing" and process_time_str:
            try:
                process_time = datetime.strptime(process_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"时间格式错误: {e}")
                continue

            if process_time < THRESHOLD:
                yield record

if __name__ == '__main__':
    # 替换为你的 JSONL 文件路径
    input_file = "./process_tasks.jsonl"
    data = list(filter_jsonl(input_file))
    write_jsonl(data, './overtime_tasks.jsonl')
        
