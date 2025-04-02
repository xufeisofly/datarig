import json
import hashlib
from typing import List

class TaskItem:
    def __init__(self, shard_dir, file_range: List[int], worker=None, is_temp=False, files=None, original_shard_dir=None) -> None:
        self._shard_dir = shard_dir
        self._file_range = file_range
        self._worker = worker
        self.is_temp = is_temp  # 添加 is_temp 属性
        self._files = files or []  # 添加 files 属性，默认为空列表
        self._original_shard_dir = original_shard_dir
        self._id = self._generate_id()

    def _generate_id(self):
        hash_input = {
            "shard_dir": self._shard_dir,
            "file_range": self._file_range,
            "files": self._files,
            "original_shard_dir": self._original_shard_dir
        }
        s = json.dumps(hash_input, sort_keys=True)
        return hashlib.md5(s.encode()).hexdigest()        

    def get_id(self):
        return self._id
    
    def get_shard_dir(self):
        return self._shard_dir

    def get_original_shard_dir(self):
        return self._original_shard_dir

    def get_file_range(self):
        return self._file_range
        
    def get_files(self):
        return self._files

    def to_dict(self) -> dict:
        return {
            "id": self._id,
            "shard_dir": self._shard_dir,
            "file_range": self._file_range,
            "worker": self._worker,
            "is_temp": self.is_temp,
            "files": self._files,
            "original_shard_dir": self._original_shard_dir,
        }
