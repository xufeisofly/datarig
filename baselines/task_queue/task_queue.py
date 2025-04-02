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

    def acquire_task(self, timeout=10, worker=None) -> TaskItem|None:
        task = self._redis_client.brpoplpush(self._queue_name, self._processing_queue, timeout)
        if task:
            task = task.decode()
            task_id = json.loads(task).get("id")
            if task_id:
                self._redis_client.setex(self.get_processing_task_key(task_id), TASK_TIMEOUT, worker)
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
            self._redis_client.lrem(self._processing_queue, 0, task)
            self._redis_client.lpush(self._finished_queue, json.dumps(task.to_dict()))
            self._redis_client.delete(self.get_processing_task_key(task_id))

    def requeue_task(self, task: TaskItem):
        task_id = task.get_id()
        key = self.get_processing_task_key(task_id)
        if not self._redis_client.exists(key):
            print(f"Requeuing task: {task.to_dict()}")
            self._redis_client.lrem(self._processing_queue, 0, task)
            self._redis_client.lpush(self._queue_name, task)        

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

    def size(self):
        self._redis_client.llen(self._queue_name)

    def download_to_jsonl(self, file_path):
        data = []
        for task in self._redis_client.lrange(self._queue_name, 0, -1):
            task = task.decode()
            data.append(json.loads(task))

        write_jsonl(data, file_path)

    def download_processing_to_jsonl(self, file_path):
        data = []
        for task in self._redis_client.lrange(self._processing_queue, 0, -1):
            task = task.decode()
            data.append(json.loads(task))

        write_jsonl(data, file_path)

    def download_finished_to_jsonl(self, file_path):
        data = []
        for task in self._redis_client.lrange(self._finished_queue, 0, -1):
            task = task.decode()
            data.append(json.loads(task))

        write_jsonl(data, file_path)        
        
