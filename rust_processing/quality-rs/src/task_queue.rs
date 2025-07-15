use once_cell::sync::Lazy;
use redis::Client;
use redis::Connection;
use redis::{Commands, RedisResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::sync::{Arc, Mutex};

static REDIS_CONN: Lazy<Arc<Mutex<redis::Connection>>> = Lazy::new(|| {
    let redis_host = env::var("REDIS_HOST").unwrap();
    let redis_port = env::var("REDIS_PORT").unwrap();
    let connection_str = format!("redis://{}:{}/", redis_host, redis_port);
    let client = Client::open(connection_str).expect("创建 Redis 客户端失败");
    let conn = client.get_connection().expect("获取 Redis 连接失败");
    Arc::new(Mutex::new(conn))
});

const TASK_QUEUE_NAME: &str = "task_queue";
const PROCESSING_QUEUE: &str = "processing_queue";
const FINISHED_QUEUE: &str = "finished_queue";
const PROCESSING_KEY_PREFIX: &str = "processing:";
const TASK_TIMEOUT: usize = 3600; // 单位：秒

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskWorker {
    pub key: Option<String>,
    pub status: Option<String>,
    pub process_time: Option<String>,
    pub finish_time: Option<String>,
}

/// 示例任务结构体，根据实际情况修改字段
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskItem {
    /// 任务 id 字段
    pub id: String,
    /// 分片目录
    pub shard_dir: String,
    /// 文件范围信息（这里采用 Value，可根据实际情况修改）
    pub file_range: Vec<i32>,
    /// 可选的 worker 标识
    pub worker: Option<TaskWorker>,
    /// 是否临时任务
    pub is_temp: Option<bool>,
    /// 任务相关文件（同样这里用 Value 代替具体类型）
    pub files: Option<Vec<String>>,
    /// 原始分片目录（可选）
    pub original_shard_dir: Option<String>,
    pub expected_ngram_count: Option<usize>,
}

impl TaskItem {
    /// 将任务序列化为 JSON 字符串
    #[allow(dead_code)]
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
    conn: Arc<Mutex<Connection>>,
    queue_name: String,
    processing_queue: String,
    finished_queue: String,
    processing_prefix: String,
}

impl TaskQueue {
    /// 根据传入的 Redis 连接和队列 id 构建 TaskQueue 实例
    pub fn new(queue_id: &str) -> Self {
        let queue_name = format!("{}_{}", queue_id, TASK_QUEUE_NAME);
        let processing_queue = format!("{}_{}", queue_id, PROCESSING_QUEUE);
        let finished_queue = format!("{}_{}", queue_id, FINISHED_QUEUE);
        let processing_prefix = format!("{}_{}", queue_id, PROCESSING_KEY_PREFIX);
        let conn = REDIS_CONN.clone();
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
    #[allow(dead_code)]
    pub fn clear(&mut self) -> RedisResult<()> {
        let _: () = self.conn.lock().unwrap().del(&self.queue_name)?;
        let tasks: Vec<String> = self
            .conn
            .lock()
            .unwrap()
            .lrange(&self.processing_queue, 0, -1)?;
        for task_str in tasks {
            // 将 Redis 中的字符串任务反序列化为 JSON 对象
            if let Ok(task_json) = serde_json::from_str::<Value>(&task_str) {
                if let Some(task_id) = task_json.get("id").and_then(|v| v.as_str()) {
                    let key = self.get_processing_task_key(task_id);
                    let _: () = self.conn.lock().unwrap().del(key)?;
                }
            }
        }
        let _: () = self.conn.lock().unwrap().del(&self.processing_queue)?;
        let _: () = self.conn.lock().unwrap().del(&self.finished_queue)?;
        Ok(())
    }

    pub fn acquire_task(
        &mut self,
        timeout: usize,
        worker: Option<&str>,
    ) -> RedisResult<Option<TaskItem>> {
        // 获取锁
        let mut conn = self.conn.lock().unwrap();
        let task_opt: Option<String> = redis::cmd("BRPOPLPUSH")
            .arg(&self.queue_name)
            .arg(&self.processing_queue)
            .arg(timeout)
            .query(&mut *conn)?;
        drop(conn); // 如果后面需要多次调用，可以在需要时重新获取锁

        if let Some(task_str) = task_opt {
            if let Ok(task_json) = serde_json::from_str::<Value>(&task_str) {
                if let Some(task_id) = task_json.get("id").and_then(|v| v.as_str()) {
                    let key = self.get_processing_task_key(task_id);
                    // 再次获取锁
                    let _: () = self.conn.lock().unwrap().set_ex(
                        key,
                        worker.unwrap_or(""),
                        TASK_TIMEOUT,
                    )?;
                    let task_item: TaskItem = serde_json::from_value(task_json).map_err(|e| {
                        redis::RedisError::from((
                            redis::ErrorKind::TypeError,
                            "serde_json deserialization error",
                            e.to_string(),
                        ))
                    })?;
                    return Ok(Some(task_item));
                }
            }
        }
        Ok(None)
    }

    /// getter: 待处理队列名称
    #[allow(dead_code)]
    pub fn pending_queue(&self) -> &str {
        &self.queue_name
    }

    /// getter: 处理中队列名称
    #[allow(dead_code)]
    pub fn processing_queue(&self) -> &str {
        &self.processing_queue
    }

    /// getter: 已完成队列名称
    #[allow(dead_code)]
    pub fn finished_queue(&self) -> &str {
        &self.finished_queue
    }

    /// 将任务推入待处理队列（使用 LPUSH）
    #[allow(dead_code)]
    pub fn put_task(&mut self, task: &TaskItem) -> RedisResult<()> {
        let json_str = task.to_json();
        let _: () = self
            .conn
            .lock()
            .unwrap()
            .lpush(&self.queue_name, json_str)?;
        Ok(())
    }

    /// 将任务推入队列尾部（使用 RPUSH，相当于放到队头）
    #[allow(dead_code)]
    pub fn put_task_to_head(&mut self, task: &TaskItem) -> RedisResult<()> {
        let json_str = task.to_json();
        let _: () = self
            .conn
            .lock()
            .unwrap()
            .rpush(&self.queue_name, json_str)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn complete_task(&mut self, task: &TaskItem) -> RedisResult<()> {
        let task_id = task.get_id();
        // 获取 Redis 连接
        let mut conn = self.conn.lock().unwrap();
        // 读取 processing 队列中的所有任务
        let tasks: Vec<String> = conn.lrange(&self.processing_queue, 0, -1)?;
        // 遍历任务列表，查找 id 与指定 task_id 匹配的任务
        for task_str in tasks {
            if let Ok(task_json) = serde_json::from_str::<serde_json::Value>(&task_str) {
                if let Some(id) = task_json.get("id").and_then(|v| v.as_str()) {
                    if id == task_id {
                        // 找到匹配任务后，删除它（LREM 删除与 task_str 完全匹配的项）
                        let removed_count: isize =
                            conn.lrem(&self.processing_queue, 0, &task_str)?;
                        if removed_count > 0 {
                            // 将删除的任务推入 finished 队列
                            let _: () = conn.lpush(&self.finished_queue, &task_str)?;
                            // 删除该任务对应的 processing 键
                            let key = self.get_processing_task_key(task_id);
                            let _: () = conn.del(key)?;
                        }
                        // 删除找到后终止循环
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn requeue_task(&mut self, task: &TaskItem) -> RedisResult<()> {
        let task_id = task.get_id();
        let mut conn = self.conn.lock().unwrap();
        // 从 processing 队列中获取所有任务
        let tasks: Vec<String> = conn.lrange(&self.processing_queue, 0, -1)?;
        // 遍历任务列表，查找 id 与给定 task_id 匹配的任务
        for task_str in tasks {
            if let Ok(task_json) = serde_json::from_str::<serde_json::Value>(&task_str) {
                if let Some(id) = task_json.get("id").and_then(|v| v.as_str()) {
                    if id == task_id {
                        // 找到匹配任务，尝试从 processing 队列中删除
                        let removed_count: isize =
                            conn.lrem(&self.processing_queue, 0, &task_str)?;
                        if removed_count > 0 {
                            // 将该任务重新入队 pending 队列
                            let _: () = conn.lpush(&self.queue_name, &task_str)?;
                            // 删除对应的 processing key
                            let key = self.get_processing_task_key(task_id);
                            let _: () = conn.del(key)?;
                        }
                        // 找到并处理匹配的任务后跳出循环
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// 判断处理中队列是否已全部处理完（即长度为 0）
    #[allow(dead_code)]
    pub fn all_finished(&mut self) -> RedisResult<bool> {
        let len: isize = self.conn.lock().unwrap().llen(&self.processing_queue)?;
        Ok(len == 0)
    }

    /// 根据 task_id 生成对应的 processing 键
    #[allow(dead_code)]
    pub fn get_processing_task_key(&self, task_id: &str) -> String {
        format!("{}{}", self.processing_prefix, task_id)
    }
}
