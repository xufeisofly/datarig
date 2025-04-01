# -*- coding: utf-8 -*-
import os
import argparse
import random
import logging
from baselines.oss import oss
from typing import List, Dict, Tuple
import re

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def list_subject_folders(parent_dir: str) -> List[str]:
    """获取所有subject=xxx的子文件夹"""
    bucket_name, path = oss.split_file_path(parent_dir)
    bucket = oss.Bucket(bucket_name)
    
    subject_folders = []
    sub_dirs = oss.get_sub_folders(bucket, path)
    
    for sub_dir in sub_dirs:
        dir_basename = os.path.basename(sub_dir.rstrip('/'))
        if dir_basename.startswith('subject='):
            full_path = os.path.join("oss://" + bucket_name, sub_dir)
            subject_folders.append(full_path)
    
    return subject_folders

def get_jsonl_gz_files(folder_path: str) -> List[str]:
    """获取文件夹中所有的.jsonl.gz文件"""
    bucket_name, path = oss.split_file_path(folder_path)
    bucket = oss.Bucket(bucket_name)
    
    all_files = oss.get_sub_files(bucket, path)
    jsonl_gz_files = [f for f in all_files if f.endswith('.jsonl.gz')]
    
    return jsonl_gz_files

def get_file_size(file_path: str) -> int:
    """获取OSS文件大小（压缩后大小）"""
    bucket_name, path = oss.split_file_path(file_path)
    bucket = oss.Bucket(bucket_name)
    
    try:
        object_info = bucket.get_object_meta(path)
        return int(object_info.headers.get('Content-Length', 0))
    except Exception as e:
        logger.error(f"获取文件大小失败: {file_path}, 错误: {e}")
        return 0

def get_uncompressed_size(file_path: str) -> int:
    """获取.gz文件的解压后大小"""
    bucket_name, object_key = oss.split_file_path(file_path)
    bucket = oss.Bucket(bucket_name)
    try:
        # 下载最后8字节，其中最后4字节是解压后大小（mod 2^32）
        result = bucket.get_object(object_key, byte_range=(None, 8))
        data = result.read()
        if len(data) < 8:
            logger.warning(f"无法获取文件 {file_path} 的解压后大小")
            return 0
        uncompressed_size = int.from_bytes(data[-4:], byteorder='little', signed=False)
        return uncompressed_size
    except Exception as e:
        logger.error(f"获取文件 {file_path} 解压后大小失败: {e}")
        return 0

def sample_files(parent_dir: str, output_dir: str, total_size_gb: float, seed: int = 42, mode: str = "proportional") -> None:
    """按比例采样文件，确保解压后总大小不超过指定值
    
    Args:
        parent_dir: 输入OSS路径
        output_dir: 输出OSS路径
        total_size_gb: 采样目标解压后总大小(GB)
        seed: 随机种子
        mode: 采样模式，可选 "balance" 或 "proportional"
    """
    random.seed(seed)
    
    # 1. 获取所有subject文件夹
    logger.info(f"开始扫描文件夹: {parent_dir}")
    subject_folders = list_subject_folders(parent_dir)
    logger.info(f"找到 {len(subject_folders)} 个subject文件夹")
    
    # 2. 统计每个文件夹的文件
    folder_files: Dict[str, List[str]] = {}
    for folder in subject_folders:
        files = get_jsonl_gz_files(folder)
        folder_files[folder] = files
        logger.info(f"文件夹 {folder} 包含 {len(files)} 个jsonl.gz文件")
    
    # 收集所有文件路径及大小信息
    all_files = []
    file_sizes = {}  # 存储压缩后文件大小
    for folder in subject_folders:
        bucket_name, _ = oss.split_file_path(folder)
        for file in folder_files[folder]:
            file_path = f"oss://{bucket_name}/{file}"
            all_files.append(file_path)
            file_sizes[file_path] = get_file_size(file_path)
    
    if not all_files:
        logger.error("未找到任何jsonl.gz文件")
        return
    
    # 3. 通过采样计算平均压缩率
    N = 100  # 采样文件数量
    sample_files = random.sample(all_files, min(N, len(all_files)))
    compression_ratios = []
    for file_path in sample_files:
        compressed_size = get_file_size(file_path)
        uncompressed_size = get_uncompressed_size(file_path)
        if uncompressed_size > 0 and compressed_size > 0:
            ratio = compressed_size / uncompressed_size
            compression_ratios.append(ratio)
    
    if compression_ratios:
        avg_compression_ratio = sum(compression_ratios) / len(compression_ratios)
        logger.info(f"估算平均压缩率: {avg_compression_ratio:.4f}")
    else:
        # 如果无法计算压缩率，使用默认值
        avg_compression_ratio = 0.3451
        logger.warning(f"无法计算平均压缩率，使用默认值: {avg_compression_ratio:.4f}")
    
    # 4. 估算每个subject文件夹的解压后总大小
    folder_files_info = {}  # 存储每个文件夹中文件的详细信息
    folder_uncompressed_sizes = {}  # 每个文件夹的预估解压后总大小
    folder_file_count = {}  # 每个文件夹的文件数量
    
    for folder in subject_folders:
        bucket_name, _ = oss.split_file_path(folder)
        folder_files_info[folder] = []
        total_compressed_size = 0
        file_count = 0
        
        for file in folder_files[folder]:
            file_path = f"oss://{bucket_name}/{file}"
            compressed_size = file_sizes[file_path]
            uncompressed_size = compressed_size / avg_compression_ratio
            
            folder_files_info[folder].append({
                'path': file_path,
                'compressed_size': compressed_size,
                'uncompressed_size': uncompressed_size
            })
            
            total_compressed_size += compressed_size
            file_count += 1
        
        estimated_uncompressed_size = total_compressed_size / avg_compression_ratio
        folder_uncompressed_sizes[folder] = estimated_uncompressed_size
        folder_file_count[folder] = file_count
        
        logger.info(f"文件夹 {folder} 估算解压后大小: {estimated_uncompressed_size / (1024 * 1024 * 1024):.2f}GB, 文件数: {file_count}")
    
    total_uncompressed_size = sum(folder_uncompressed_sizes.values())
    logger.info(f"估算总解压后大小: {total_uncompressed_size / (1024 * 1024 * 1024):.2f}GB")
    
    # 5. 根据模式计算每个文件夹的目标大小
    target_total_uncompressed_size = total_size_gb * 1024 * 1024 * 1024
    folder_target_sizes = {}  # 每个文件夹的目标大小
    
    if mode == "balance":
        # Balance模式: 平均分配空间给每个学科
        non_empty_folders = [f for f in subject_folders if folder_uncompressed_sizes[f] > 0]
        if non_empty_folders:
            avg_size_per_folder = target_total_uncompressed_size / len(non_empty_folders)
            
            # 第一轮分配：为每个文件夹分配最多avg_size_per_folder的大小
            remaining_size = target_total_uncompressed_size
            for folder in non_empty_folders:
                folder_size = folder_uncompressed_sizes[folder]
                if folder_size <= avg_size_per_folder:
                    # 如果文件夹小于平均大小，全部使用
                    folder_target_sizes[folder] = folder_size
                    remaining_size -= folder_size
                else:
                    # 否则只使用平均大小
                    folder_target_sizes[folder] = avg_size_per_folder
                    remaining_size -= avg_size_per_folder
            
            # 第二轮分配：将剩余空间分配给有余量的文件夹
            if remaining_size > 0:
                folders_with_capacity = [f for f in non_empty_folders 
                                         if folder_uncompressed_sizes[f] > folder_target_sizes[f]]
                
                while remaining_size > 0 and folders_with_capacity:
                    additional_avg = remaining_size / len(folders_with_capacity)
                    next_folders = []
                    
                    for folder in folders_with_capacity:
                        max_additional = folder_uncompressed_sizes[folder] - folder_target_sizes[folder]
                        actual_additional = min(additional_avg, max_additional)
                        
                        folder_target_sizes[folder] += actual_additional
                        remaining_size -= actual_additional
                        
                        if folder_target_sizes[folder] < folder_uncompressed_sizes[folder]:
                            next_folders.append(folder)
                    
                    if not next_folders or abs(remaining_size) < 1024:  # 如果没有后续文件夹或剩余空间很小，退出循环
                        break
                    
                    folders_with_capacity = next_folders
        
        logger.info(f"Balance模式: 分配总大小 {sum(folder_target_sizes.values()) / (1024 * 1024 * 1024):.2f}GB")
        
    else:  # proportional模式
        # 按原始比例分配空间
        for folder in subject_folders:
            if total_uncompressed_size > 0:
                ratio = folder_uncompressed_sizes[folder] / total_uncompressed_size
                folder_target_sizes[folder] = target_total_uncompressed_size * ratio
            else:
                folder_target_sizes[folder] = 0
        
        logger.info(f"Proportional模式: 分配总大小 {sum(folder_target_sizes.values()) / (1024 * 1024 * 1024):.2f}GB")
    
    # 6. 按大小采样文件
    sampled_files_by_folder: Dict[str, List[str]] = {}
    
    for folder in subject_folders:
        files_info = folder_files_info[folder]
        target_size = folder_target_sizes.get(folder, 0)
        
        if target_size <= 0 or not files_info:
            sampled_files_by_folder[folder] = []
            continue
        
        # 计算采样比例
        folder_size = folder_uncompressed_sizes[folder]
        ratio = min(1.0, target_size / folder_size) if folder_size > 0 else 0
        
        # 对文件随机排序
        random.shuffle(files_info)
        
        # 按大小累计采样
        sampled_files = []
        current_size = 0
        
        for file_info in files_info:
            if current_size >= target_size:
                break
            
            sampled_files.append(file_info['path'])
            current_size += file_info['uncompressed_size']
        
        sampled_files_by_folder[folder] = sampled_files
        logger.info(f"从文件夹 {folder} 采样 {len(sampled_files)}/{len(files_info)} 个文件, " 
                   f"大小: {current_size / (1024 * 1024 * 1024):.2f}GB / {folder_size / (1024 * 1024 * 1024):.2f}GB")
    
    # 7. 复制文件到目标位置
    output_bucket_name, output_path = oss.split_file_path(output_dir)
    output_bucket = oss.Bucket(output_bucket_name)
    
    total_copied_uncompressed_size = 0
    
    for folder, sampled_files in sampled_files_by_folder.items():
        # 提取subject名称
        path = oss.split_file_path(folder)[1]
        subject_match = re.search(r'subject=([^/]+)', path)
        if subject_match:
            subject_name = subject_match.group(1)
        else:
            logger.warning(f"无法从 {folder} 提取subject名称")
            continue
        
        # 创建对应的输出子文件夹
        folder_output_dir = os.path.join(output_path, f"subject={subject_name}")
        
        for file_path in sampled_files:
            source_bucket_name, source_object_key = oss.split_file_path(file_path)
            target_object_key = os.path.join(folder_output_dir, os.path.basename(source_object_key))
            
            try:
                compressed_size = file_sizes[file_path]
                uncompressed_size = compressed_size / avg_compression_ratio
                output_bucket.copy_object(source_bucket_name, source_object_key, target_object_key)
                total_copied_uncompressed_size += uncompressed_size
            except Exception as e:
                logger.error(f"复制文件失败: {source_object_key}, 错误: {e}")
    
    logger.info(f"采样完成! 估算采样文件解压后总大小: {total_copied_uncompressed_size / (1024 * 1024 * 1024):.2f}GB")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='按比例采样OSS文件夹中的jsonl.gz文件')
    parser.add_argument('--input_dir', type=str, required=True, help='输入OSS路径，包含subject=xxx子文件夹')
    parser.add_argument('--output_dir', type=str, required=True, help='输出OSS路径')
    parser.add_argument('--total_size_gb', type=float, default=1.0, help='采样目标解压后总大小(GB)')
    parser.add_argument('--seed', type=int, default=42, help='随机种子')
    parser.add_argument('--mode', type=str, default='proportional', choices=['balance', 'proportional'],
                        help='采样模式：balance(均衡各学科大小)或proportional(按原始比例)')
    
    args = parser.parse_args()
    
    sample_files(args.input_dir, args.output_dir, args.total_size_gb, args.seed, args.mode)