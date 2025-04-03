import time
import logging
from baselines.task_queue.task_queue import TaskQueue
from baselines.redis import redis
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    

if __name__ == '__main__':
    queue = TaskQueue(redis.Client, queue_id='default')
    while True:
        queue.download_to_jsonl('./task_queue.jsonl')
        queue.download_processing_to_jsonl('./processing_task_queue.jsonl')
        queue.download_finished_to_jsonl('./finished_task_queue.jsonl')
        print(f"pending: {queue.sizeof(queue.queue)}")
        print(f"processing: {queue.sizeof(queue.processing_queue)}")
        print(f"finished: {queue.sizeof(queue.finished_queue)}")
        time.sleep(10)
