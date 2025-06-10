import os
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import concurrent.futures

        
if __name__ == '__main__':
    folder_path = '/root/dataprocess_nas/dataprocess/public/fineweb/fineweb-sample-10BT/sample_jsonl/'
    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

    def process(file_path):
        filename = os.path.basename(file_path)
        filename = os.path.splitext(filename)[0]
        target_folder = os.path.join('/root/dataprocess_nas/xf/data/fineweb_10b/', filename.split('_')[0])
        os.makedirs(target_folder, exist_ok=True)

        print(f"==== start {filename}")
                
        lines = []
        for i, line in enumerate(read_jsonl(file_path)):
            line['text'] = line['txt']
            del line['txt']
            lines.append(line)
            if i % 300000 == 0:
                print("======== save", filename)
                target_file = os.path.join(target_folder, filename+f"_{str(i)}.jsonl")
                if not os.path.isfile(target_file):
                    write_jsonl(lines, target_file)
                lines = []
                
        write_jsonl(lines, os.path.join(target_folder, filename+"_end.jsonl"))
        print(f"==== done {filename}")        
    

    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        futures = []
        
        for file_path in all_files:
        # process(file_path)
            futures.append(executor.submit(process, file_path))

        for future in concurrent.futures.as_completed(futures):
            key = future.result()
            print(f"done: {key}")
            
        
    
