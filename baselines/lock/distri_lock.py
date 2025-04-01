# -*- coding: utf-8 -*-
import os
import time
import redis
import socket
from baselines.oss.lock import DEFAULT_LOCK_FILE, SimpleOSSLock
from abc import ABC, abstractmethod


def get_local_ip():
    """获取本机局域网 IP 地址，避免返回 127.0.0.1"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 连接到一个公共地址，不需要实际发送数据
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

def get_worker_key():
    return f"{get_local_ip()}_{os.getpid()}"

class DistriLock(ABC):
    @abstractmethod
    def acquire(self) -> bool:
        pass

    @abstractmethod
    def acquire_or_block(self, timeout=100) -> bool:
        pass

    @abstractmethod
    def release(self) -> bool:
        pass
    

class RedisLock(DistriLock):
    def __init__(self, redis_client: redis.Redis, lock_key: str, lock_timeout: int = 60) -> None:
        """
        Initialize the Redis lock.
        
        :param redis_client: The Redis client instance.
        :param lock_key: The Redis key used for the lock.
        :param lock_timeout: Timeout in seconds for the lock to expire automatically.
        """
        self.redis_client = redis_client
        self.lock_key = lock_key
        self.lock_timeout = lock_timeout  # Set lock expiration time in seconds
        self.local_ip = get_local_ip()
        self.process_id = os.getpid()
        self.lock_value = f"locked_{self.local_ip}_{self.process_id}"

    def acquire(self) -> bool:
        """
        Try to acquire the lock using the Redis SET command with NX (set if not exists)
        and EX (set expiration).
        """
        try:
            # SETNX (SET if not exists) with EXPIRE time (lock timeout)
            return self.redis_client.set(self.lock_key, self.lock_value, nx=True, ex=self.lock_timeout)
        except Exception as e:
            print(f"Failed to acquire lock: {e}")
            return False

    def acquire_or_block(self, timeout = 100) -> bool:
        """
        Block and keep trying to acquire the lock until the timeout.
        If timeout is -1, it will keep trying indefinitely.
        """
        if timeout == -1:
            while True:
                if self.acquire():
                    return True
                time.sleep(2)
        else:
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.acquire():
                    return True
                time.sleep(2)
        return False

    def release(self) -> bool:
        """
        Release the lock by deleting the Redis key.
        """
        try:
            # Only release if the lock value matches, ensuring it's the current lock holder.
            lock_content = self.redis_client.get(self.lock_key)
            if lock_content and lock_content.decode("utf-8") == self.lock_value:
                self.redis_client.delete(self.lock_key)
                return True
            else:
                return False
        except Exception as e:
            print(f"Failed to release lock: {e}")
            return False    

    

class LockFactory:
    def __init__(self, mode='redis'):
        self._mode = mode

    def create(self, redis_client: redis.Redis, lock_key: str, lock_timeout: int=60, lock_file=DEFAULT_LOCK_FILE):
        if self._mode == 'redis':
            return RedisLock(redis_client, lock_key, lock_timeout)
        return SimpleOSSLock(lock_file)
