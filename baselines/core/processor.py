import logging
import multiprocessing
import os
import os.path
import time
from datetime import datetime
from typing import Any, Dict, Tuple, List

from yaml import safe_load

from baselines.core.factories import get_mapper, get_aggregator, get_transform
from baselines.core.file_utils import is_oss, read_jsonl, write_jsonl, makedirs_if_missing, delete_file, is_exists, get_file_size
from baselines.core.constants import PROCESS_SETUP_KEY_NAME, PROCESS_END_KEY_NAME, COMMIT_KEY_NAME, GLOBAL_FUNCTIONS
from baselines.oss.oss import OSSPath

logger = logging.getLogger(__name__)

# Set the logging level
logger.setLevel(logging.DEBUG)  # For example, set level to DEBUG

# Create a StreamHandler for stdout
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)  # Optionally set a different level for stdout

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handler.setFormatter(formatter)

# Add the stdout handler to the logger
logger.addHandler(stdout_handler)

COMMIT_COMMAND = 'commit'

# Indices in the mapper counters (what happened to each processed page)
ERRORS_INDEX = -1
REMOVED_INDEX = 0
KEPT_INDEX = 1
SPLIT_INDEX = 2


def commit(pages, stats, output_path, stats_output_path):
    print(f"committing pages to {output_path} (and stats to {stats_output_path})")
    t = time.time()
    write_jsonl(pages, output_path)

    stats.append({'name': COMMIT_KEY_NAME, 'secs': time.time() - t})
    write_jsonl(stats, stats_output_path, 'a')

    stats.clear()
    print(f"commit took {time.time() - t} seconds")


def _get_output_paths(base_output_path, jsonl_relpath):
    # TODO - need to allow overwrite?
    file_ext = os.path.splitext(jsonl_relpath)[-1][1:]
    file_ext = f'jsonl.{file_ext}' if file_ext in ['zstd', 'zst', 'gz'] else file_ext
    jsonl_relpath = jsonl_relpath.replace("_processed.jsonl", ".jsonl")    # To allow for overwrite if continuning from intermediate
    shard_name = jsonl_relpath.split(".jsonl")[0]
    shard_name = shard_name.split("/")[-1] # xufeisofly
    out_path = os.path.join(base_output_path, 'processed_data', shard_name + f'_processed.{file_ext}')
    stats_out_path = os.path.join(base_output_path, 'stats', shard_name + '_stats.jsonl')

    makedirs_if_missing(os.path.dirname(out_path))
    makedirs_if_missing(os.path.dirname(stats_out_path))
    return out_path, stats_out_path

def _is_step_stats(line):
    """
    True iff this is a step stats line (and not a general info one)
    """
    return line['name'] not in {PROCESS_SETUP_KEY_NAME, PROCESS_END_KEY_NAME, COMMIT_KEY_NAME}

def split_large_file(input_path: str, max_size_mb: int = 1024, temp_dir: str = "oss://si002558te8h/dclm/temp_files") -> List[str]:
    """
    将大文件切分成多个小文件，每个不超过指定大小，并存储到OSS临时目录。
    
    :param input_path: 输入文件路径
    :param max_size_mb: 最大文件大小（MB）
    :param temp_dir: OSS临时目录路径
    :return: 切分后的临时文件路径列表
    """
    import json  # 添加json模块用于序列化字典
    
    max_size_bytes = max_size_mb * 1024 * 1024
    file_size = get_file_size(input_path)
    
    if file_size <= max_size_bytes:
        return [input_path]  # 文件不需要切分
    
    print(f"文件大小({file_size/1024/1024:.2f}MB)超过{max_size_mb}MB，进行切分处理")
    
    # 确保临时目录存在
    if not is_oss(temp_dir):
    #     temp_dir = f"{temp_dir.rstrip('/')}/{datetime.now().strftime('%Y%m%d%H%M%S')}"
    # else:
    #     temp_dir = os.path.join(temp_dir, f"{datetime.now().strftime('%Y%m%d%H%M%S')}")
        makedirs_if_missing(temp_dir)

    print(f"使用临时目录: {temp_dir}")
    
    base_filename = os.path.basename(input_path)
    file_name, file_ext = os.path.splitext(base_filename)
    
    temp_files = []
    chunk_idx = 0
    line_buffer = []
    current_size = 0
    buffer_size_bytes = 0

    # 使用 read_jsonl 读取文件，无论是本地、S3 还是 OSS 都能正确读取
    for line in read_jsonl(input_path):
        # 将读取的行添加到缓冲区
        line_buffer.append(line)
        # 估算当前缓冲区的大小
        buffer_size_bytes += len(json.dumps(line).encode('utf-8')) if isinstance(line, dict) else 1024
        
        # 当缓冲区大小接近最大限制，写入临时文件
        if buffer_size_bytes >= max_size_bytes - (1024*1024*max_size_mb*0.1):
            chunk_path = f"{temp_dir}/{file_name}_chunk{chunk_idx}{file_ext}"
            print(f"写入切分文件 {chunk_idx+1}: {chunk_path}")
            
            # 修改：先将内容写入本地临时文件，然后一次性上传
            if is_oss(temp_dir):
                # 创建本地临时文件
                import tempfile
                local_temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', suffix=f'_chunk{chunk_idx}{file_ext}')
                try:
                    # 将内容写入本地临时文件 - 修复：将字典转为JSON字符串
                    for l in line_buffer:
                        json_str = json.dumps(l) if isinstance(l, dict) else str(l)
                        local_temp_file.write(json_str + "\n")
                    local_temp_file.close()
                    
                    # 修复：正确使用OSSPath上传文件
                    with open(local_temp_file.name, 'rb') as f:
                        content = f.read()
                        with OSSPath(chunk_path).open('wb') as oss_file:
                            oss_file.write(content)
                    
                    print(f"成功上传切分文件到OSS: {chunk_path}")
                finally:
                    # 删除本地临时文件
                    if os.path.exists(local_temp_file.name):
                        os.unlink(local_temp_file.name)
            else:
                with open(chunk_path, 'w', encoding='utf-8') as outfile:
                    for l in line_buffer:
                        json_str = json.dumps(l) if isinstance(l, dict) else str(l)
                        outfile.write(json_str + "\n")
                        
            temp_files.append(chunk_path)
            chunk_idx += 1
            line_buffer = []
            buffer_size_bytes = 0

    # 写入最后剩余的内容
    if line_buffer:
        chunk_path = f"{temp_dir}/{file_name}_chunk{chunk_idx}{file_ext}"
        print(f"写入最后一个切分文件: {chunk_path}")
        
        # 修改：对最后一个文件也使用相同的方法一次性上传
        if is_oss(temp_dir):
            # 创建本地临时文件
            import tempfile
            local_temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', suffix=f'_chunk{chunk_idx}{file_ext}')
            try:
                # 将内容写入本地临时文件 - 修复：将字典转为JSON字符串
                for l in line_buffer:
                    json_str = json.dumps(l) if isinstance(l, dict) else str(l)
                    local_temp_file.write(json_str + "\n")
                local_temp_file.close()
                
                # 修复：正确使用OSSPath上传文件
                with open(local_temp_file.name, 'rb') as f:
                    content = f.read()
                    with OSSPath(chunk_path).open('wb') as oss_file:
                        oss_file.write(content)
                
                print(f"成功上传最后一个切分文件到OSS: {chunk_path}")
            finally:
                # 删除本地临时文件
                if os.path.exists(local_temp_file.name):
                    os.unlink(local_temp_file.name)
        else:
            with open(chunk_path, 'w', encoding='utf-8') as outfile:
                for l in line_buffer:
                    json_str = json.dumps(l) if isinstance(l, dict) else str(l)
                    outfile.write(json_str + "\n")
                    
        temp_files.append(chunk_path)

    print(f"文件已切分为{len(temp_files)}个子文件")
    return temp_files


def process_single_file(config_data: Dict[str, Any], raw_data_dirpath: str, jsonl_relpath: str, source_name: str, 
                        base_output_path: str, workers: int = 1, overwrite: bool = False, max_file_size_mb: int = 1024, 
                        temp_dir: str = None, is_temp_file: bool = False) -> Tuple[str, str, int, int, List[str]]:
    """
    :param config_data: A processed config (from yaml) that specifies the steps to be taken
    :param raw_data_dirpath: The path to the top data directory in the data hierarchy (from which to mirror
                                the output path)
    :param jsonl_relpath: path to the input jsonl file with the text to be processed, relative to raw_data_dirpath
    :param source_name: The name of the source of the jsonl file
    :param base_output_path: path to the output directory where to save the resulting jsonl file with the
                        processed text as well as the stats
    :param workers: number of workers to use for multiprocessing. if 1, will use a single process
    :param overwrite: if True, will overwrite the output file if it already exists, otherwise will continue from
                        where previous runs stopped
    :param max_file_size_mb: 最大允许的文件大小，超过此大小将拆分
    :param temp_dir: OSS临时目录，拆分的大文件将存储在这里
    :param is_temp_file: 是否是临时文件，临时文件处理完成后会被删除
    :return: 输出路径，统计路径，输入页面数，输出页面数，临时文件列表（如果生成了临时文件）
    """
    start_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    t0 = time.time()

    # Assumption #1 - we do not write the output after every step, but only at the end/on a commit step
    assert source_name in config_data, f"Source {source_name} not found in YAML file."
    config_data = config_data[source_name]

    # Assumption #2 - we keep the entire input lines in memory
    t1 = time.time()
    input_path = os.path.join(raw_data_dirpath, jsonl_relpath)
    
    # 设置默认临时目录（如果未提供）
    if temp_dir is None:
        temp_dir = "oss://si002558te8h/dclm/temp_files"
    
    # 检查文件大小并决定是否需要切分
    file_size_mb = get_file_size(input_path) / (1024 * 1024)
    
    temp_files = []
    if file_size_mb > max_file_size_mb and not is_temp_file:
        print(f"文件大小为 {file_size_mb:.2f}MB，超过 {max_file_size_mb}MB，将进行文件切分处理")
        # 切分文件并保存到OSS临时目录
        temp_files = split_large_file(input_path, max_file_size_mb, temp_dir)
        print(f"文件已切分为{len(temp_files)}个子文件，临时存储在{temp_dir}")
        
        # 返回空结果和临时文件列表，让上层函数处理这些拆分的文件
        return "", "", 0, 0, temp_files
    
    print(f"处理文件: {input_path}")
    pages = list(read_jsonl(input_path))
    print(f"读取完成，共 {len(pages)} 条记录")
    jsonl_load_secs = time.time() - t1

    # Assumption #3 - for each input shard, there is a specific stats file that accompanies it
    output_path, stats_output_path = _get_output_paths(base_output_path, jsonl_relpath)

    # If a jsonl is empty (e.g., due to another chunk), the page will be skipped  
    num_pages_in = len(pages)
    if num_pages_in == 0:
        print(f"Input data file at {input_path} is empty.")
        # 如果是临时文件，处理完成后删除
        if is_temp_file and is_exists(input_path):
            delete_file(input_path)
            print(f"临时文件 {input_path} 已删除")
        return output_path, stats_output_path, 0, 0, []

    # load stats file
    t2 = time.time()
    old_stats, stats = [], []  # we will use this for fault tolerance - if the stats file exists, we will
    # skip the steps that were already done

    if is_exists(stats_output_path):
        if overwrite:
            print(f"Stats file {stats_output_path} already exists, but overwrite is set to true, so deleting.")
            delete_file(stats_output_path)
        else:
            print(f"Stats file {stats_output_path} already exists, loading.")
            old_stats = list(read_jsonl(stats_output_path))

    stats_load_secs = time.time() - t2
    first_run = len(old_stats) == 0  # i.e. not a continuation
    graceful_continuation = len(old_stats) >= 4 and old_stats[-2]['name'] == PROCESS_END_KEY_NAME and \
                            old_stats[-1]['name'] == COMMIT_KEY_NAME  # i.e. the last run did at least 1 step +
    if not first_run:
        old_stats = [line for line in old_stats if _is_step_stats(line)]  # remove the setup, commit and end messages
    # (start message, end message and commit message) and the execution ended gracefully
    stats.append({'name': PROCESS_SETUP_KEY_NAME,
                  'jsonl_load_secs': jsonl_load_secs,
                  'stats_load_secs': stats_load_secs,
                  'jsonl_path': jsonl_relpath,
                  'source_name': source_name,
                  'output_path': output_path,
                  'stats_output_path': stats_output_path,
                  'workers': workers,
                  'first_run': first_run,
                  'graceful_continuation': graceful_continuation,
                  'execution_start_time': start_time})

    i = 0
    commit_steps = executed_steps = skipped_steps = 0
    early_exit = False
    updated = False
    for step in config_data['steps']:
        if step == COMMIT_COMMAND:
            if not updated:
                print("No steps were executed, skipping commit.")
                continue
            commit_steps += 1
            commit(pages, stats, output_path, stats_output_path)
            updated = False
            continue

        # skip step if was done already
        if i < len(old_stats):
            assert old_stats[i]['name'] == step[
                'func'], f"Step {step} does not match step {old_stats[i]['name']} in stats file."
            print(f"Skipping step {step} since it was already done.")
            i += 1
            skipped_steps += 1
            continue
        elif step['func'] in GLOBAL_FUNCTIONS:
            early_exit = True
            break

        executed_steps += 1
        n_pages_before = len(pages)
        counters = {ERRORS_INDEX: 0, REMOVED_INDEX: 0, KEPT_INDEX: 0, SPLIT_INDEX: 0}
        new_pages = []
        execution_times = []
        step_stats = {}

        t4 = time.time()
        if workers > 1:
            apply_partial_func_parallel(counters, execution_times, new_pages, pages, step, workers)
        else:
            partial_func = get_mapper(**step, _profile=True, _safe=True)
            apply_partial_func_sequential(counters, execution_times, new_pages, pages, partial_func)
            del partial_func
        if counters[ERRORS_INDEX] == len(pages):
            raise RuntimeError(f"Step {step} failed on all pages.")
        t5 = time.time()

        n_pages_after = len(new_pages)
        step_stats['name'] = step['func']
        step_stats['pages_in'] = len(pages)
        step_stats['pages_out'] = len(new_pages)
        step_stats['secs'] = sum(execution_times)
        step_stats['secs/page'] = step_stats['secs'] / len(pages)
        step_stats['errors'] = counters[ERRORS_INDEX]
        step_stats['removed'] = counters[REMOVED_INDEX]
        step_stats['kept'] = counters[KEPT_INDEX]
        step_stats['split'] = counters[SPLIT_INDEX]
        step_stats['total_secs'] = t5 - t4
        step_stats['workers'] = workers

        print(f"Step {step['func']} removed {counters[REMOVED_INDEX]} pages, split {counters[SPLIT_INDEX]} "
                    f"pages, and kept {counters[KEPT_INDEX]} pages. {counters[ERRORS_INDEX]} errors were detected. "
                    f"In total, {n_pages_before} pages before and {n_pages_after} pages after.")
        pages = new_pages

        if len(pages) == 0:
            print("No pages left, stopping.")
            updated = True
            break

        if '_aggregate' in step:
            for agg_key, agg_def in step['_aggregate'].items():
                t_agg = time.time()
                if isinstance(agg_def, str):
                    agg_def = {'type': agg_def}
                agg_type = agg_def['type']
                agg = get_aggregator(agg_type)
                vals = [p[agg_key] for p in pages]
                if 'transform' in agg_def:
                    transform_name = agg_def['transform']
                    transform_args = {k: v for k, v in agg_def.items() if k != 'transform' and k != 'type'}
                    transform = get_transform(transform_name, **transform_args)
                    vals = [transform(v) for v in vals]
                agg_res = agg(vals)
                print(f'Aggregation result for {agg_key} is {agg_res}')
                agg_res['_secs'] = time.time() - t_agg
                step_stats[agg_key] = agg_res
        stats.append(step_stats)

        updated = True

    stats.append({'name': PROCESS_END_KEY_NAME,
                  'total_secs': time.time() - t0,
                  'executed_steps': executed_steps,
                  'skipped_steps': skipped_steps,
                  'commit_steps': commit_steps,
                  'finished_processing': not early_exit,
                  'execution_end_time': datetime.now().strftime("%d/%m/%Y %H:%M:%S")})

    print('Finished processing all steps, committing.')
    if updated:
        commit(pages, stats, output_path, stats_output_path)

    # 如果是临时文件，处理完成后删除
    if is_temp_file and is_exists(input_path):
        delete_file(input_path)
        print(f"临时文件 {input_path} 已删除")

    return output_path, stats_output_path, num_pages_in, len(pages), []


def _parse_func_results(results_gen, counters, execution_times, new_pages):
    for result, profiling_info in results_gen:
        execution_times.append(profiling_info.execution_time)
        if isinstance(result, list):
            counters[min(len(result), 2)] += 1  # 0 is removed, 1 is kept and 2 is split
            new_pages.extend(result)
        else:
            counters[ERRORS_INDEX] += 1  # error
            print(result)


def apply_partial_func_sequential(counters, execution_times, new_pages, pages, partial_func):
    _parse_func_results(map(partial_func, pages), counters, execution_times, new_pages)


def _worker(worker_num, step, pages, input_queue, output_list):
    print(f"Worker {worker_num} has started processing jobs.")
    # Create the partial function for each item in the YAML
    # Assumption - if the function has some intiialization to do, it was done as part of a cluster creation and the overhead will be amortized across pages
    partial_func = get_mapper(**step, _profile=True, _safe=True)
    while True:
        try:
            # Get a job from the input queue.
            page_ind = input_queue.get()

            if page_ind is None:
                # If the job is None, this signals the worker to terminate.
                break

            result = partial_func(pages[page_ind])

            # Put the result in the output queue.
            output_list.append(result)
        except Exception as e:
            print(f"Error occurred in MP apply of func: {e}")

    print(f"Worker {worker_num} has finished processing jobs.")


def apply_partial_func_parallel(counters, execution_times, new_pages, pages, step, n_workers):
    # Create the input queue.
    input_queue = multiprocessing.Queue()

    # Create the shared list.
    manager = multiprocessing.Manager()
    shared_list = manager.list()

    # Create the workers.
    workers = [multiprocessing.Process(target=_worker, args=(i, step, pages, input_queue, shared_list)) for i in
               range(n_workers)]

    # Start the workers.
    for w in workers:
        w.start()

    # Add jobs to the input queue.
    for i in range(len(pages)):
        input_queue.put(i)

    # Add a None job to the end of the queue for each worker, to signal them to terminate.
    for _ in range(n_workers):
        input_queue.put(None)

    # Wait for all workers to finish.
    for w in workers:
        w.join()

    _parse_func_results(shared_list, counters, execution_times, new_pages)
