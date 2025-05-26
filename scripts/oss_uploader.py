import oss2
import hashlib
import os
from tqdm import tqdm
from io import BytesIO
from baselines.oss import oss
import concurrent.futures

class OSSUploader:
    def __init__(self, bucket_name):
        """
        初始化OSS客户端
        """
        # self.auth = oss2.Auth(access_key_id, access_key_secret)
        # self.bucket = oss2.Bucket(self.auth, endpoint, bucket_name)
        self.bucket = oss.Bucket(bucket_name)
        self.chunk_size = 4 * 1024 * 1024 * 1024  # 4GB（保留1GB余量）

    @staticmethod
    def calculate_md5(file_path, chunk_size=8192):
        """计算本地文件的MD5校验和"""
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                md5.update(chunk)
        return md5.hexdigest()

    def upload_large_file(self, local_path, oss_key):
        """
        分片上传大文件到OSS
        :param local_path: 本地文件路径
        :param oss_key: OSS目标路径（格式：bucket/key）
        """
        try:
            # 解析OSS路径
            if oss_key.startswith("oss://"):
                oss_key = oss_key[6:]
            bucket_name, object_key = oss_key.split('/', 1)

            # 初始化分片上传
            upload_id = self.bucket.init_multipart_upload(object_key).upload_id
            parts = []
            part_number = 1

            # 分片上传参数
            current_size = 0
            buffer = b''
            file_size = os.path.getsize(local_path)
            md5_total = hashlib.md5()

            # 分片上传进度条
            with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading") as pbar:
                with open(local_path, 'rb') as f:
                    while True:
                        chunk = f.read(self.chunk_size)
                        if not chunk:
                            break
                        
                        buffer += chunk
                        current_size += len(chunk)
                        md5_total.update(chunk)
                        pbar.update(len(chunk))

                        # 当缓冲区达到分片大小时上传
                        if current_size >= self.chunk_size:
                            # 上传当前分片
                            result = self.bucket.upload_part(
                                object_key, 
                                upload_id, 
                                part_number, 
                                BytesIO(buffer)
                            )
                            parts.append(oss2.models.PartInfo(
                                part_number, 
                                result.etag
                            ))
                            part_number += 1
                            buffer = b''
                            current_size = 0

                    # 上传剩余数据
                    if buffer:
                        result = self.bucket.upload_part(
                            object_key, 
                            upload_id, 
                            part_number, 
                            BytesIO(buffer)
                        )
                        parts.append(oss2.models.PartInfo(
                            part_number, 
                            result.etag
                        ))

            # 完成分片上传
            self.bucket.complete_multipart_upload(object_key, upload_id, parts)
            return True


        except Exception as e:
            print(f"Upload failed: {str(e)}")
            # 中止上传
            self.bucket.abort_multipart_upload(oss_key, upload_id, parts)
            return False

# 使用示例
if __name__ == "__main__":
    # 配置信息
    oss_dir = "oss://train1/basemodel-subjet-data-processed/hpc-processed/r3_mixed_dedupe_sample/"
    local_dir = "/root/dataprocess_nas/zh/mixed/"
    bucket_name, path = oss.split_file_path(oss_dir)
    BUCKET_NAME = bucket_name
    
    # 初始化上传器
    uploader = OSSUploader(BUCKET_NAME)
    finish_file_name = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for file_name in os.listdir(local_dir):
            if file_name in finish_file_name:
                continue
            print(f"{local_dir}{file_name}")
            print(f"{oss_dir}{file_name}")
            futures.append(executor.submit(uploader.upload_large_file, f"{local_dir}{file_name}", f"{oss_dir}{file_name}"))

        for future in concurrent.futures.as_completed(futures):
            future.result()
    # 上传本地大文件
    # file_name = "Agriculture_3000000"
    # local_file = f"/root/dataprocess_nas/zh/recall/{file_name}.jsonl"
    # oss_path = f"oss://train1/basemodel-subjet-data-processed/hpc-processed/r3_recall_dedupe_sample/{file_name}.jsonl"
    
    # # 执行上传
    # success = uploader.upload_large_file(local_file, oss_path)
    
    # if success:
    #     print(f"File {file_name} uploaded successfully!")
    # else:
    #     print("File upload failed!")