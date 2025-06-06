# -*- coding: utf-8 -*-
from typing import Union
import oss2
import oss2.resumable
import time
import tempfile

import logging
import os
from io import BytesIO
from requests.exceptions import ChunkedEncodingError

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 设置Endpoint和Region
endpoint = "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com/"
region = "cn-hangzhou"

# auth = oss2.Auth(os.getenv("OSS_ACCESS_KEY_ID"), os.getenv("OSS_ACCESS_KEY_SECRET"))
auth = oss2.Auth("D3XdW8uT66mdJ3TO", "LpBPv841EkOhqNV8tiaknQfWJJm1uP")


def Bucket(name) -> oss2.Bucket:
    return oss2.Bucket(auth, endpoint, name, region=region, connect_timeout=(600, 600))


# ZJ_Bucket = Bucket("si002558te8h")
# result = ZJ_Bucket.list_objects(prefix='cc-warc2024/', max_keys=10)

# file_name = 'cc-warc2024/dclm_pool/1b-1x/CC_shard_00000000.jsonl.zst'
# ZJ_Bucket.object_exists(file_name)

def get_all_objects_iter(bucket, prefix):
    start_after = ''
    while True:
        result = bucket.list_objects_v2(prefix=prefix, start_after=start_after)
        objects = result.object_list
        if not objects:
            break
        start_after = objects[-1].key
        for o in objects:
            yield o


def get_all_prefixes_iter(bucket, prefix, delimiter='/'):
    start_after = ''
    while True:
        result = bucket.list_objects_v2(prefix=prefix, start_after=start_after, delimiter=delimiter)
        prefixes = result.prefix_list
        if not prefixes:
            break
        start_after = prefixes[-1]
        for p in prefixes:
            yield p


def split_file_path(file_path):
    bucket_name, path = file_path.replace("oss://", "").split("/", 1)
    return bucket_name, path


def join_file_path(bucket_name: str, path: str) -> str:
    return f"oss://{bucket_name}/{path}"


def get_sub_folders(bucket, dir_path):
    if not dir_path.endswith('/'):
        dir_path += '/'    
    rets = list(get_all_prefixes_iter(bucket, dir_path))
    return rets

def get_sub_files(bucket, dir_path):
    if not dir_path.endswith('/'):
        dir_path += '/'
    rets = list(get_all_objects_iter(bucket, dir_path))
    subfolders = get_sub_folders(bucket, dir_path)
    files = [ret.key for ret in rets if not ret.key.endswith('/')]

    all_files = []

    def belong_to_folders(f, dirs):
        for folder in dirs:
            if folder in f:
                return True
        return False
    
    for f in files:
        if belong_to_folders(f, subfolders):
            continue
        all_files.append(f)
    return all_files
    
        
class OSSPath:
    def __init__(self, file_path, resumable_write=True):
        bucket_name, path = split_file_path(file_path)
        bucket = Bucket(bucket_name)
        # 初始化 OSS 配置信息
        self.bucket = bucket
        self.path = path  # OSS 文件路径
        self.resumable_write = resumable_write
        
    def open(self, mode) -> Union['OSSWriteStream', 'OSSReadStream']:
        if mode in ["rb", "r"]:
            return OSSReadStream(self.bucket, self.path)
        elif mode in ["wb", "w", "a"]:
            return OSSWriteStream(self.bucket, self.path, BytesIO(), mode=mode, resumable=self.resumable_write)
        raise ValueError(f"invalid mode: {mode}")


class OSSReadStream:
    def __init__(self, bucket: oss2.Bucket, path: str, chunk_size: int = 256 * 1024):
        """
        :param bucket: oss2.Bucket 实例
        :param path: OSS 上的文件路径
        :param chunk_size: 每次读取的字节数，默认为 64KB
        """
        self.bucket = bucket
        self.path = path
        self.chunk_size = chunk_size
        # 获取 OSS 对象的响应流，不一次性读取所有数据
        self.resp = self.bucket.get_object(self.path)
        self._closed = False

    def read(self, amt: int = None) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if amt is None:
            amt = self.chunk_size
        return self.resp.read(amt)

    def readable(self) -> bool:
        return True
    
    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def flush(self):
        # 只读流不需要实际 flush 操作，但必须实现该方法以兼容 TextIOWrapper
        pass    

    def close(self):
        if not self._closed:
            self._closed = True
    
    @property
    def closed(self) -> bool:
        return self._closed

    def __iter__(self):
        while True:
            data = self.read(self.chunk_size)
            if not data:
                break
            yield data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()    
        
    
class OSSWriteStream():
    def __init__(self, bucket, path, output, mode="w", resumable=True):
        self.bucket = bucket
        self.path = path
        self.output = output
        self.mode = mode
        self.resumable = resumable

        if self.mode == "a":
            try:
                self.current_data = self.bucket.get_object(self.path).read()
                self.output.write(self.current_data)
            except oss2.exceptions.NoSuchKey:
                self.current_data = b''        
        
    def write(self, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        self.output.write(data)
        
    def close(self):
        if self.resumable:
            self.upload_to_oss_resumable()
        else:
            self.upload_to_oss()

    def flush(self):
        self.output.flush()        

    def upload_to_oss(self):
        # 将缓冲区的内容上传到 OSS
        self.output.seek(0)  # 重置读取位置
        self.bucket.put_object(self.path, self.output.read())  # 上传数据

    def upload_to_oss_resumable(self):
        self.output.seek(0)
        data = self.output.read()
        
        # 写入临时文件
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(data)
            tmp_file_path = tmp.name

        try:
            to_dir = os.path.dirname(self.path)
            upload_file_resumable(tmp_file_path, to_dir, self.bucket,
                                  new_filename=os.path.basename(self.path))
        except Exception as e:
            raise e
        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)        

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False        


def is_object_exist(bucket, path):
    return bucket.object_exists(path)


def upload_file_to_oss(file_path, to_dir, bucket):
    """
    上传单个文件到 OSS，file_path 为本地文件的完整路径，
    to_dir 为 OSS 中目标目录，bucket 为 OSS Bucket 实例。
    """
    print("==== start upload file: {}".format(file_path))
    file_name = os.path.basename(file_path)
    oss_file_path = os.path.join(to_dir, file_name)
    if is_object_exist(bucket, oss_file_path):
        print("==== upload file already exists: {}".format(oss_file_path))
        return
    bucket.put_object_from_file(oss_file_path, file_path)
    print("==== finish upload file: {}".format(oss_file_path))


def upload_file_resumable(file_path, to_dir, bucket, new_filename=None):
    print("==== start upload file: {}".format(file_path))
    if new_filename is None:
        file_name = os.path.basename(file_path)
    else:
        file_name = new_filename
    oss_file_path = os.path.join(to_dir, file_name)
    if is_object_exist(bucket, oss_file_path):
        print("==== upload file already exists: {}".format(oss_file_path))
        return
    oss2.resumable_upload(bucket, oss_file_path, file_path)
    print("==== finish upload file: {}".format(oss_file_path))    


def download_file(oss_path, to_dir, bucket) -> str:
    print("==== start download file: {}".format(oss_path))
    file_name = os.path.basename(oss_path)
    local_file_path = os.path.join(to_dir, file_name)
    if os.path.isfile(local_file_path):
        print("==== download file already exists: {}".format(local_file_path))
        return local_file_path
    bucket.get_object_to_file(oss_path, local_file_path)
    print("==== finish download file: {}".format(local_file_path))
    return local_file_path

def download_file_resumable(oss_path, to_dir, bucket):
    print("==== start download file: {}".format(oss_path))
    file_name = os.path.basename(oss_path)
    local_file_path = os.path.join(to_dir, file_name)
    if os.path.isfile(local_file_path):
        print("==== download file already exists: {}".format(local_file_path))
        return local_file_path
    
    oss2.resumable_download(bucket, oss_path, local_file_path)
    print("==== finish download file: {}".format(local_file_path))
    return local_file_path

def download_file_resumable_with_retry(oss_path, to_dir, bucket, retries=5, delay=5):
    for attempt in range(1, retries+1):
        try:
            return download_file_resumable(oss_path, to_dir, bucket)
        except ChunkedEncodingError as e:
            print(f"下载出错，尝试 {attempt}/{retries} 次重试，错误：{e}")
            local_file_path = os.path.join(to_dir, os.path.basename(oss_path))
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
            time.sleep(delay)
    raise Exception("重试多次后仍无法成功下载。")

def finished_task_file(task_file_path):
    filename = os.path.basename(task_file_path)
    return task_file_path.replace(filename, "finished_"+filename)
