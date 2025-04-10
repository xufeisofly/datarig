from baselines.oss import oss
import os
import concurrent.futures

def main():
    oss_target_dir = "oss://train1/basemodel-subjet-data-processed/r2_processed/dclm/"
    oss_source_dir = "oss://si002558te8h/dclm/output/r2_formal/dclm/"
    bucket_name, target_dir = oss.split_file_path(oss_target_dir)
    target_bucket = oss.Bucket(bucket_name)


    def process(f, target_key, source_bucket_name):
        filename = os.path.basename(f)
        target_bucket.copy_object(source_bucket_name, f, os.path.join(target_key, filename))
        return target_key


    for subject in ['subject=AppliedMathematics', 'subject=AppliedPhysics', 'subject=OrganicChemistry']:
        oss_source_path = os.path.join(oss_source_dir, subject, "processed_data")
        source_bucket_name, source_dir = oss.split_file_path(oss_source_path)
        source_bucket = oss.Bucket(source_bucket_name) 
        files = oss.get_sub_files(source_bucket, source_dir)

        target_key = os.path.join(target_dir, subject)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
        
            for f in files:
                futures.append(executor.submit(process, f, target_key, source_bucket_name))

            for future in concurrent.futures.as_completed(futures):
                key = future.result()
                print(f"done: {key}")
        
        
if __name__ == '__main__':
    main()
    
