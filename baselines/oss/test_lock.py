import os
import random
import time
import multiprocessing
from baselines.oss.lock import SimpleOSSLock, DEFAULT_LOCK_FILE

def worker(lock_file: str, index: int):
    lock = SimpleOSSLock(lock_file)
    print(f"Worker {index} (PID: {os.getpid()}) trying to acquire lock...")
    if lock.acquire_or_block(timeout_ms=5000):
        print(f"Worker {index} (PID: {os.getpid()}) acquired the lock.")
        # 模拟执行任务，休眠 1～3 秒之间
        work_time = random.uniform(1, 3)
        print(f"Worker {index} (PID: {os.getpid()}) working for {work_time:.2f} seconds...")
        time.sleep(work_time)
        if lock.release():
            print(f"Worker {index} (PID: {os.getpid()}) released the lock.")
        else:
            print(f"Worker {index} (PID: {os.getpid()}) failed to release the lock.")
    else:
        print(f"Worker {index} (PID: {os.getpid()}) could not acquire the lock within timeout.")

        
if __name__ == "__main__":
    num_workers = 5  # 启动5个子进程
    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker, args=(DEFAULT_LOCK_FILE, i))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    print("Distributed lock test complete.")
