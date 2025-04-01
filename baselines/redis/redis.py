import redis
import os

# 创建连接池
pool = redis.ConnectionPool(host=os.getenv("REDIS_HOST"), port=int(os.getenv("REDIS_PORT")), db=0)

# 创建 Redis 客户端，使用连接池
Client = redis.Redis(connection_pool=pool)
