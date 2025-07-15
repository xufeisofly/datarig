use crate::oss;
use crate::task_queue::{TaskItem, TaskQueue};
use color_eyre::eyre::Result;

pub async fn get_task_item_redis(
    queue_id: &str,
) -> Result<(Option<TaskItem>, bool), anyhow::Error> {
    let mut queue = TaskQueue::new(queue_id);
    let worker_key = oss::get_worker_key();
    let task = queue.acquire_task(10, Some(worker_key.as_str()))?;

    let all_finished = queue.all_finished()?;
    Ok((task, all_finished))
}

pub async fn mark_task_item_finished_redis(
    task_item: &TaskItem,
    queue_id: &str,
) -> Result<(), anyhow::Error> {
    let mut queue = TaskQueue::new(queue_id);
    queue.complete_task(task_item)?;
    Ok(())
}

pub async fn mark_task_item_failed_redis(
    task_item: &TaskItem,
    queue_id: &str,
) -> Result<(), anyhow::Error> {
    let mut queue = TaskQueue::new(queue_id);
    queue.requeue_task(task_item)?;
    Ok(())
}
