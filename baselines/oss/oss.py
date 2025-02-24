# -*- coding: utf-8 -*-
import oss2
import os
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 检查环境变量是否已设置
required_env_vars = ['OSS_ACCESS_KEY_ID', 'OSS_ACCESS_KEY_SECRET']
for var in required_env_vars:
    if var not in os.environ:
        logging.error(f"Environment variable {var} is not set.")
        exit(1)

# 设置Endpoint和Region
endpoint = "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com/"
region = "cn-hangzhou"

auth = oss2.Auth(os.getenv("OSS_ACCESS_KEY_ID"), os.getenv("OSS_ACCESS_KEY_SECRET"))


def Bucket(name) -> oss2.Bucket:
    return oss2.Bucket(auth, endpoint, name, region=region)


ZJ_Bucket = Bucket("download2")
result = ZJ_Bucket.list_objects(prefix='cc-warc2024/', max_keys=10)

file_name = 'cc-warc2024/dclm_pool/1b-1x/CC_shard_00000000.jsonl.zst'
ZJ_Bucket.object_exists(file_name)

