#!/usr/bin/env python3
import subprocess
import time
from datetime import datetime

def get_nodes_memory():
    """调用 kubectl top nodes 获取节点内存使用情况"""
    try:
        output = subprocess.check_output(["kubectl", "top", "nodes"], encoding="utf-8")
        return output
    except subprocess.CalledProcessError as e:
        return f"Error: {e}"

def get_pods_memory():
    """调用 kubectl top nodes 获取节点内存使用情况"""
    try:
        output = subprocess.check_output(["kubectl", "top", "pods"], encoding="utf-8")
        return output
    except subprocess.CalledProcessError as e:
        return f"Error: {e}"    

def main():
    log_file = "top.log"
    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mem_usage = get_nodes_memory()
        pod_mem_usage = get_pods_memory()
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"Timestamp: {timestamp}\n")
            f.write(mem_usage)
            f.write("\n")
            f.write(pod_mem_usage)
            f.write("\n")            
        time.sleep(10)

if __name__ == '__main__':
    main()
