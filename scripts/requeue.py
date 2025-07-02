import argparse

from baselines.redis import redis
from baselines.task_queue.task_queue import TaskQueue


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("queue_id", help="queue_id", type=str)
    args = parser.parse_args()
    
    queue_id = args.queue_id
    queue = TaskQueue(redis.Client, queue_id=queue_id)
    queue.requeue_tasks()

    print("===== done")
