import time
import logging
from baselines.task_queue.task_queue import TaskQueue
from baselines.redis import redis
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    

if __name__ == '__main__':
    queue = TaskQueue(redis.Client, queue_id='default')
    file_path = './task_queue.jsonl'
    queue.download_to_jsonl(file_path)
