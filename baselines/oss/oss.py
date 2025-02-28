# -*- coding: utf-8 -*-
from typing import BinaryIO, Union
import oss2
import os
import logging
from io import BufferedWriter, BytesIO, BufferedReader

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 设置Endpoint和Region
endpoint = "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com/"
region = "cn-hangzhou"

# auth = oss2.Auth(os.getenv("OSS_ACCESS_KEY_ID"), os.getenv("OSS_ACCESS_KEY_SECRET"))
auth = oss2.Auth("D3XdW8uT66mdJ3TO", "LpBPv841EkOhqNV8tiaknQfWJJm1uP")


def Bucket(name) -> oss2.Bucket:
    return oss2.Bucket(auth, endpoint, name, region=region)


# ZJ_Bucket = Bucket("si002558te8h")
# result = ZJ_Bucket.list_objects(prefix='cc-warc2024/', max_keys=10)

# file_name = 'cc-warc2024/dclm_pool/1b-1x/CC_shard_00000000.jsonl.zst'
# ZJ_Bucket.object_exists(file_name)


def split_file_path(file_path):
    bucket_name, path = file_path.replace("oss://", "").split("/", 1)
    return bucket_name, path

        
class OSSPath:
    def __init__(self, file_path):
        bucket_name, path = split_file_path(file_path)
        bucket = Bucket(bucket_name)
        # 初始化 OSS 配置信息
        self.bucket = bucket
        self.path = path  # OSS 文件路径
        
    def open(self, mode) -> Union['OSSWriteStream', 'OSSReadStream']:
        if mode == "rb":
            return OSSReadStream(self.bucket, self.path)
        return OSSWriteStream(self.bucket, self.path, BytesIO())


class OSSReadStream(BytesIO):
    def __init__(self, bucket: oss2.Bucket, path: str):
        self.bucket = bucket
        self.path = path
        obj = self.bucket.get_object(self.path)  # 获取 OSS 对象
        super().__init__(obj.read())
        
    
class OSSWriteStream():
    def __init__(self, bucket, path, output):
        self.bucket = bucket
        self.path = path
        self.output = output
        
    def write(self, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        self.output.write(data)
        
    def close(self):
        # 关闭压缩流并上传数据到 OSS
        self.upload_to_oss()

    def flush(self):
        # 提供一个 flush 方法来确保数据写入
        self.output.flush()        

    def upload_to_oss(self):
        # 将缓冲区的内容上传到 OSS
        self.output.seek(0)  # 重置读取位置
        self.bucket.put_object(self.path, self.output.read())  # 上传数据

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False        

