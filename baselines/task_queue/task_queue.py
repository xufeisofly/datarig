from baselines.core.file_utils import write_jsonl
import redis
import json
from baselines.task_queue.task import TaskItem

TASK_QUEUE_NAME = 'task_queue'
PROCESSING_QUEUE = 'processing_queue'
FINISHED_QUEUE = 'finished_queue'
PROCESSING_KEY_PREFIX = 'processing:'
TASK_TIMEOUT = 7200


class TaskQueue:
    def __init__(self, redis_client: redis.Redis, queue_id="default") -> None:
        self._redis_client = redis_client
        self._queue_name = queue_id + "_" + TASK_QUEUE_NAME
        self._processing_queue = queue_id + "_" + PROCESSING_QUEUE
        self._finished_queue = queue_id + "_" + FINISHED_QUEUE
        self._processing_prefix = queue_id + "_" + PROCESSING_KEY_PREFIX

    def clear(self):
        self._redis_client.delete(self._queue_name)
        for task in self._redis_client.lrange(self._processing_queue, 0, -1):
            task = task.decode()
            task_id = json.loads(task).get("id")
            key = self.get_processing_task_key(task_id)
            self._redis_client.delete(key)
        self._redis_client.delete(self._processing_queue)                
        self._redis_client.delete(self._finished_queue)

    def acquire_task(self, timeout=10, worker=None) -> TaskItem|None:
        task = self._redis_client.brpoplpush(self._queue_name, self._processing_queue, timeout)
        if task:
            task = task.decode()
            task_id = json.loads(task).get("id")
            if task_id:
                self._redis_client.setex(self.get_processing_task_key(task_id), TASK_TIMEOUT, worker)
                job = json.loads(task)
                return TaskItem(shard_dir=job['shard_dir'],
                                file_range=job['file_range'],
                                worker=job.get('worker', None),
                                is_temp=job['is_temp'],
                                files=job['files'],
                                original_shard_dir=job.get('original_shard_dir', None))
        return None

    @property
    def pending_queue(self):
        return self._queue_name

    @property
    def processing_queue(self):
        return self._processing_queue

    @property
    def finished_queue(self):
        return self._finished_queue

    def put_task(self, task: TaskItem):
        self._redis_client.lpush(self._queue_name, task.to_json())

    def put_task_to_head(self, task: TaskItem):
        self._redis_client.rpush(self._queue_name, task.to_json())

    def complete_task(self, task: TaskItem):
        task_id = task.get_id()
        removed_count = self._redis_client.lrem(self._processing_queue, 0, task.to_json())
        if removed_count > 0:
            self._redis_client.lpush(self._finished_queue, task.to_json())
            self._redis_client.delete(self.get_processing_task_key(task_id))

    def requeue_task(self, task: TaskItem):
        task_id = task.get_id()
        removed_count = self._redis_client.lrem(self._processing_queue, 0, task.to_json())
        if removed_count > 0:        
            self._redis_client.lpush(self._queue_name, task.to_json())
            self._redis_client.delete(self.get_processing_task_key(task_id))

    def all_finished(self) -> bool:
        return self._redis_client.llen(self._processing_queue) == 0

    def requeue_expired_tasks(self):
        for task in self._redis_client.lrange(self._processing_queue, 0, -1):
            task = task.decode()
            task_id = json.loads(task).get("id")
            key = self.get_processing_task_key(task_id)
            if not self._redis_client.exists(key):
                print(f"Requeuing expired task: {task}")
                self._redis_client.lrem(self._processing_queue, 0, task)
                self._redis_client.lpush(self._queue_name, task)

    def get_processing_task_key(self, task_id):
        return f"{self._processing_prefix}{task_id}"

    def sizeof(self, queue):
        return self._redis_client.llen(queue)        

    def download_to_jsonl(self, queue, file_path):
        data = []
        for task in self._redis_client.lrange(queue, 0, -1):
            task = task.decode()
            data.append(json.loads(task))

        write_jsonl(data, file_path)        
        
