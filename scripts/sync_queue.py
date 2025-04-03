import time
import logging
from baselines.task_queue.task_queue import TaskQueue
from baselines.redis import redis
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    

if __name__ == '__main__':
    queue = TaskQueue(redis.Client, queue_id='default')
    while True:
        tmp_folder = "oss://si002558te8h/dclm/temp_dir_500/"
        bucket_name, path = oss.split_file_path(tmp_folder)
        bucket = oss.Bucket(bucket_name)
        files = oss.get_sub_files(bucket, path)
        logging.info("==============")
        logging.info("temp files: {}".format(len(files)))

        
        queue.download_to_jsonl('./task_queue.jsonl')
        queue.download_processing_to_jsonl('./processing_task_queue.jsonl')
        queue.download_finished_to_jsonl('./finished_task_queue.jsonl')
        logging.info(f"pending: {queue.sizeof(queue.queue)} | processing: {queue.sizeof(queue.processing_queue)} | finished: {queue.sizeof(queue.finished_queue)}")
        time.sleep(10)
