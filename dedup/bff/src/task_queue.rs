use redis::{Commands, RedisResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};

const TASK_QUEUE_NAME: &str = "task_queue";
const PROCESSING_QUEUE: &str = "processing_queue";
const FINISHED_QUEUE: &str = "finished_queue";
const PROCESSING_KEY_PREFIX: &str = "processing:";
const TASK_TIMEOUT: usize = 3600; // 单位：秒

/// 示例任务结构体，根据实际情况修改字段
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskItem {
    /// 任务 id 字段
    pub id: String,
    /// 分片目录
    pub shard_dir: String,
    /// 文件范围信息（这里采用 Value，可根据实际情况修改）
    pub file_range: Value,
    /// 可选的 worker 标识
    pub worker: Option<String>,
    /// 是否临时任务
    pub is_temp: bool,
    /// 任务相关文件（同样这里用 Value 代替具体类型）
    pub files: Value,
    /// 原始分片目录（可选）
    pub original_shard_dir: Option<String>,
    pub expected_ngram_count: Option<usize>,
}

impl TaskItem {
    /// 将任务序列化为 JSON 字符串
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
    /// 获取任务 id
    pub fn get_id(&self) -> &str {
        &self.id
    }
}

/// 任务队列封装，保存了 Redis 连接和各个队列名称
pub struct TaskQueue {
    conn: redis::Connection,
    queue_name: String,
    processing_queue: String,
    finished_queue: String,
    processing_prefix: String,
}

impl TaskQueue {
    /// 根据传入的 Redis 连接和队列 id 构建 TaskQueue 实例
    pub fn new(conn: redis::Connection, queue_id: &str) -> Self {
        let queue_name = format!("{}_{}", queue_id, TASK_QUEUE_NAME);
        let processing_queue = format!("{}_{}", queue_id, PROCESSING_QUEUE);
        let finished_queue = format!("{}_{}", queue_id, FINISHED_QUEUE);
        let processing_prefix = format!("{}_{}", queue_id, PROCESSING_KEY_PREFIX);
        TaskQueue {
            conn,
            queue_name,
            processing_queue,
            finished_queue,
            processing_prefix,
        }
    }

    /// 清空任务队列，包括待处理、处理中和已完成队列，
    /// 同时删除每个任务对应的 processing 键。
    pub fn clear(&mut self) -> RedisResult<()> {
        let _: () = self.conn.del(&self.queue_name)?;
        let tasks: Vec<String> = self.conn.lrange(&self.processing_queue, 0, -1)?;
        for task_str in tasks {
            // 将 Redis 中的字符串任务反序列化为 JSON 对象
            if let Ok(task_json) = serde_json::from_str::<Value>(&task_str) {
                if let Some(task_id) = task_json.get("id").and_then(|v| v.as_str()) {
                    let key = self.get_processing_task_key(task_id);
                    let _: () = self.conn.del(key)?;
                }
            }
        }
        let _: () = self.conn.del(&self.processing_queue)?;
        let _: () = self.conn.del(&self.finished_queue)?;
        Ok(())
    }

    /// 阻塞方式尝试获取一个任务（模拟 Python 中 brpoplpush）。
    /// 若获取到任务，则设置该任务的超时并返回反序列化后的 TaskItem。
    pub fn acquire_task(
        &mut self,
        timeout: usize,
        worker: Option<&str>,
    ) -> RedisResult<Option<TaskItem>> {
        let task_opt: Option<String> = redis::cmd("BRPOPLPUSH")
            .arg(&self.queue_name)
            .arg(&self.processing_queue)
            .arg(timeout)
            .query(&mut self.conn)?;
        if let Some(task_str) = task_opt {
            if let Ok(task_json) = serde_json::from_str::<Value>(&task_str) {
                if let Some(task_id) = task_json.get("id").and_then(|v| v.as_str()) {
                    // 设置该任务的处理超时（即 TASK_TIMEOUT 秒后失效）
                    let key = self.get_processing_task_key(task_id);
                    // worker 为空则使用空字符串
                    let worker_val = worker.unwrap_or("");
                    let _: () = self.conn.set_ex(key, worker_val, TASK_TIMEOUT)?;
                    // 反序列化为 TaskItem
                    let task_item: TaskItem = serde_json::from_value(task_json);
                    return Ok(Some(task_item));
                }
            }
        }
        Ok(None)
    }

    /// getter: 待处理队列名称
    pub fn pending_queue(&self) -> &str {
        &self.queue_name
    }

    /// getter: 处理中队列名称
    pub fn processing_queue(&self) -> &str {
        &self.processing_queue
    }

    /// getter: 已完成队列名称
    pub fn finished_queue(&self) -> &str {
        &self.finished_queue
    }

    /// 将任务推入待处理队列（使用 LPUSH）
    pub fn put_task(&mut self, task: &TaskItem) -> RedisResult<()> {
        let json_str = task.to_json();
        let _: () = self.conn.lpush(&self.queue_name, json_str)?;
        Ok(())
    }

    /// 将任务推入队列尾部（使用 RPUSH，相当于放到队头）
    pub fn put_task_to_head(&mut self, task: &TaskItem) -> RedisResult<()> {
        let json_str = task.to_json();
        let _: () = self.conn.rpush(&self.queue_name, json_str)?;
        Ok(())
    }

    /// 完成任务：在处理中队列中移除任务，
    /// 若移除成功则将任务推入已完成队列，并删除对应的 processing 键。
    pub fn complete_task(&mut self, task: &TaskItem) -> RedisResult<()> {
        let task_json = task.to_json();
        let task_id = task.get_id();
        let removed_count: isize = self.conn.lrem(&self.processing_queue, 0, &task_json)?;
        if removed_count > 0 {
            let _: () = self.conn.lpush(&self.finished_queue, &task_json)?;
            let key = self.get_processing_task_key(task_id);
            let _: () = self.conn.del(key)?;
        }
        Ok(())
    }

    /// 将任务重新入队：从处理中队列移除该任务，
    /// 若移除成功则将任务推回待处理队列，并删除 processing 键。
    pub fn requeue_task(&mut self, task: &TaskItem) -> RedisResult<()> {
        let task_json = task.to_json();
        let task_id = task.get_id();
        let removed_count: isize = self.conn.lrem(&self.processing_queue, 0, &task_json)?;
        if removed_count > 0 {
            let _: () = self.conn.lpush(&self.queue_name, &task_json)?;
            let key = self.get_processing_task_key(task_id);
            let _: () = self.conn.del(key)?;
        }
        Ok(())
    }

    /// 判断处理中队列是否已全部处理完（即长度为 0）
    pub fn all_finished(&mut self) -> RedisResult<bool> {
        let len: isize = self.conn.llen(&self.processing_queue)?;
        Ok(len == 0)
    }

    /// 将所有正在处理的任务重新入队
    pub fn requeue_tasks(&mut self) -> RedisResult<()> {
        let tasks: Vec<String> = self.conn.lrange(&self.processing_queue, 0, -1)?;
        for task_str in tasks {
            if let Ok(task_json) = serde_json::from_str::<Value>(&task_str) {
                if let Some(task_id) = task_json.get("id").and_then(|v| v.as_str()) {
                    let removed_count: isize =
                        self.conn.lrem(&self.processing_queue, 0, &task_str)?;
                    if removed_count > 0 {
                        let _: () = self.conn.lpush(&self.queue_name, &task_str)?;
                        let key = self.get_processing_task_key(task_id);
                        let _: () = self.conn.del(key)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// 对于过期的任务（processing 键已不存在）重新入队
    pub fn requeue_expired_tasks(&mut self) -> RedisResult<()> {
        let tasks: Vec<String> = self.conn.lrange(&self.processing_queue, 0, -1)?;
        for task_str in tasks {
            if let Ok(task_json) = serde_json::from_str::<Value>(&task_str) {
                if let Some(task_id) = task_json.get("id").and_then(|v| v.as_str()) {
                    let key = self.get_processing_task_key(task_id);
                    let exists: bool = self.conn.exists(&key)?;
                    if !exists {
                        println!("Requeuing expired task: {}", task_str);
                        let _: () = self.conn.lrem(&self.processing_queue, 0, &task_str)?;
                        let _: () = self.conn.lpush(&self.queue_name, &task_str)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// 根据 task_id 生成对应的 processing 键
    pub fn get_processing_task_key(&self, task_id: &str) -> String {
        format!("{}{}", self.processing_prefix, task_id)
    }

    /// 获取指定队列的长度
    pub fn sizeof(&mut self, queue: &str) -> RedisResult<isize> {
        self.conn.llen(queue)
    }

    /// 返回队列中所有任务（反序列化为 serde_json::Value）
    pub fn iterator(&mut self, queue: &str) -> RedisResult<Vec<Value>> {
        let tasks: Vec<String> = self.conn.lrange(queue, 0, -1)?;
        let mut result = Vec::new();
        for task_str in tasks {
            if let Ok(task_json) = serde_json::from_str::<Value>(&task_str) {
                result.push(task_json);
            }
        }
        Ok(result)
    }

    /// 将指定队列中的所有任务写入 JSONL 文件（每行一个 JSON 文本）
    pub fn download_to_jsonl(
        &mut self,
        queue: &str,
        file_path: &str,
    ) -> Result<(), Box<dyn Error>> {
        let tasks: Vec<String> = self.conn.lrange(queue, 0, -1)?;
        let mut data = Vec::new();
        for task_str in tasks {
            let task_json: Value = serde_json::from_str(&task_str)?;
            data.push(task_json);
        }
        write_jsonl(&data, file_path)?;
        Ok(())
    }
}

/// 辅助函数：将 JSON 数组写入 JSONL 文件
pub fn write_jsonl(data: &[Value], file_path: &str) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);
    for item in data {
        let line = serde_json::to_string(item)?;
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}
