import os
import multiprocessing
import argparse
from baselines.oss.lock import SimpleOSSLock, DEFAULT_LOCK_FILE
from baselines.lock.distri_lock import LockFactory
from baselines.redis import redis

file_path = "./num.txt"

def worker(lock_file: str, index: int, timeout: int):
    lock = LockFactory(mode='redis').create(redis.Client, lock_key=lock_file)
    print(f"Worker {index} (PID: {os.getpid()}) trying to acquire lock...")
    if lock.acquire_or_block(timeout=timeout):
        print(f"Worker {index} (PID: {os.getpid()}) acquired the lock.")
        # 模拟执行任务，休眠 1～3 秒之间
        if not os.path.exists(file_path):
            with open(file_path, "w") as f:
                f.write("0")

        ret = 0
        with open(file_path, "r") as f:
            ret = int(f.read())

        with open(file_path, "w") as f:
            f.write(str(ret+1))

        if lock.release():
            print(f"Worker {index} (PID: {os.getpid()}) released the lock.")
        else:
            print(f"Worker {index} (PID: {os.getpid()}) failed to release the lock.")
    else:
        print(f"Worker {index} (PID: {os.getpid()}) could not acquire the lock within timeout.")

        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", help="", type=int, default=5)
    parser.add_argument("--timeout", help="", type=int, default=60)
    args = parser.parse_args()
    processes = []
    for i in range(args.num_workers):
        p = multiprocessing.Process(target=worker, args=(DEFAULT_LOCK_FILE, i, args.timeout))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()

    ret = 0
    with open(file_path, "r") as f:
        ret = int(f.read())

    print("Distributed lock test complete.")        
    if ret == args.num_workers:
        print("=== Success! ===")
    else:
        print(f"=== Failed! expected: {args.num_workers}, actual: {ret} ===")
    
