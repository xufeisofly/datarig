import os
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import concurrent.futures

        
if __name__ == '__main__':
    # oss_dir = "oss://si002558te8h/dclm/output/zhanghong/"
    # target_oss_dir = "oss://si002558te8h/dclm/output/r3_math_2b/"
    # bucket_name, dir_path = oss.split_file_path(oss_dir)
    # bucket = oss.Bucket(bucket_name)

    # sub_files = oss.get_sub_files(bucket, dir_path)

    # files = []
    # for sub_file in sub_files:
    #     filename = os.path.basename(sub_file)
    #     filename = os.path.splitext(filename)[0]
    #     # num = int(filename.split('_')[-1])

    #     # if num <= 14:
    #     #     files.append(sub_file)
    #     if 'Mathematics' in filename:
    #         files.append(sub_file)

    # bucket_name, target_dir = oss.split_file_path(target_oss_dir)
    
    
    # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    #     futures = []
        
    #     for f in files:
    #         futures.append(executor.submit(process, f, target_dir, bucket_name, bucket))

    #         for future in concurrent.futures.as_completed(futures):
    #             key = future.result()
    #             print(f"done: {key}")


    folder_path = '/root/dataprocess_nas/zh/removed/'
    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

    def process(file_path):
        filename = os.path.basename(file_path)
        filename = os.path.splitext(filename)[0]
        if filename == 'Mathematics_3000000' or filename == 'Engineering_3000000':
            return
        target_folder = os.path.join('/root/dataprocess_nas/xf/data/low_quality/', filename.split('_')[0])
        os.makedirs(target_folder, exist_ok=True)

        if len(os.listdir(target_folder)) == 101:
            return
        
        print(f"==== start {filename}")
                
        lines = []
        for i, line in enumerate(read_jsonl(file_path)):
            lines.append(line)
            if i % 30000 == 0:
                print("======== save", filename)
                target_file = os.path.join(target_folder, filename+f"_{str(i)}.jsonl")
                if not os.path.isfile(target_file):
                    write_jsonl(lines, target_file)
                lines = []
                
        write_jsonl(lines, os.path.join(target_folder, filename+"_end.jsonl"))
        print(f"==== done {filename}")        
    

    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        futures = []
        
        for file_path in all_files:
        # process(file_path)
            futures.append(executor.submit(process, file_path))

        for future in concurrent.futures.as_completed(futures):
            key = future.result()
            print(f"done: {key}")
            
        
    
