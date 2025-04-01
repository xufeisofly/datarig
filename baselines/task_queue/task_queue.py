import time
import redis
import json
from task_asigning.asign_task import TaskItem

TASK_QUEUE_NAME = 'task_queue'
PROCESSING_QUEUE = 'processing_queue'
PROCESSING_KEY_PREFIX = 'processing:'
TASK_TIMEOUT = 7200


class TaskQueue:
    def __init__(self, redis_client: redis.Redis, queue_id="default") -> None:
        self._redis_client = redis_client
        self._queue_name = queue_id + "_" + TASK_QUEUE_NAME
        self._processing_queue = queue_id + "_" + PROCESSING_QUEUE
        self._processing_prefix = queue_id + "_" + PROCESSING_KEY_PREFIX

    def acquire_task(self, timeout=10) -> TaskItem|None:
        task = self._redis_client.brpoplpush(self._queue_name, self._processing_queue, timeout)
        if task:
            task = task.decode()
            task_id = json.loads(task).get("id")
            if task_id:
                self._redis_client.setex(f"{self._processing_prefix}{task_id}", TASK_TIMEOUT, task)
                job = json.loads(task)
                return TaskItem(job['shard_dir'],
                                job['file_range'],
                                job['is_temp'],
                                job['files'],
                                job.get('original_shard_dir', None))
        return None
        

    def put_task(self, task: TaskItem):
        self._redis_client.lpush(self._queue_name, json.dumps(task.to_dict()))

    def complete_task(self, task: TaskItem):
        task_id = task.get_id()
        if task_id:
            self._redis_client.lrem(PROCESSING_QUEUE, 0, task)
            self._redis_client.delete(f"{self._processing_prefix}{task_id}")        

    def size(self):
        self._redis_client.llen(self._queue_name)
        
