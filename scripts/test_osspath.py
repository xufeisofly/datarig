# -*- coding: utf-8 -*-
import os
import json

from baselines.oss import oss
from baselines.core.file_utils import read_jsonl
from ray_processing.process import finished_task_file


if __name__ == '__main__':
    file_path = "oss://si002558te8h/dclm/process_tasks.json"
    fin_file_path = finished_task_file(file_path)

    with open("./process_tasks.json", "w") as localf:
        with oss.OSSPath(file_path).open("r") as f:
            data = json.loads(f.read())
            json.dump(data, localf, indent=4)

    with open("./finished_process_tasks.json", "w") as localf:
        with oss.OSSPath(fin_file_path).open("r") as f:
            data = json.loads(f.read())
            json.dump(data, localf, indent=4)    
    
    
    # list(read_jsonl(file_path))
