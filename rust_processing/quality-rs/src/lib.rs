use anyhow::Error;
use color_eyre::eyre::Result;
use flate2::read::MultiGzDecoder;
use indexmap::IndexMap;
use indicatif::{ProgressBar, ProgressStyle};
use io::expand_dirs;
use log::{error, info};
use oss::split_oss_path;
use oss::{get_reader_from_oss, is_oss};
use oss_rust_sdk::async_object::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::available_parallelism;
use std::thread::sleep;
use std::time::{Duration, Instant};
use threadpool::ThreadPool;

use zstd::stream::read::Decoder as ZstDecoder;

pub mod filter;
mod io;
mod oss;
mod task;
mod task_queue;
mod util;

use filter::FilterStat;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn process_tasks(
    output: &PathBuf,
    filters: &Arc<Vec<Box<dyn filter::Filter>>>,
    threads: &usize,
    queue_id: &str,
    no_progress_bar: bool,
) -> Result<(), Error> {
    let mut processed_any = false;
    loop {
        let (task_item, all_finished) = task::get_task_item_redis(queue_id)?;

        if task_item.is_none() {
            if !all_finished && !processed_any {
                info!("Waiting for tasks...");
                sleep(Duration::from_secs(10));
                continue;
            } else {
                info!("All tasks finished!");
                break;
            }
        }

        let task = task_item.unwrap();
        info!("Processing task: {}", task.get_id());

        processed_any = true;

        let file_range = task.file_range.clone();
        let shard_dir = PathBuf::from(&task.shard_dir);

        let all_files = expand_dirs(vec![shard_dir.clone()], None).unwrap();
        let mut files_to_process = Vec::new();

        match &task.files {
            Some(task_files) if !task_files.is_empty() => {
                for file in task_files {
                    files_to_process.push(PathBuf::from(file));
                }
            }
            _ => {
                let start = file_range[0] as usize;
                let end = if file_range[1] == -1 {
                    all_files.len()
                } else {
                    file_range[1] as usize
                };

                if end <= all_files.len() {
                    files_to_process.extend_from_slice(&all_files[start..end]);
                } else if start < all_files.len() {
                    files_to_process.extend_from_slice(&all_files[start..]);
                }
            }
        }

        let result = process_files(files_to_process, output, filters, threads, no_progress_bar);
        match result {
            Ok(_) => {
                info!("Task completed!");
                task::mark_task_item_finished_redis(&task, queue_id)?;
            }
            Err(e) => {
                error!("Task failed: {:?}", e);
                task::mark_task_item_failed_redis(&task, queue_id)?;
            }
        }

        sleep(Duration::from_secs(1));
    }

    Ok(())
}

fn process_input(
    input: Vec<PathBuf>,
    output: &PathBuf,
    filters: &Arc<Vec<Box<dyn filter::Filter>>>,
    threads: &usize,
    no_progress_bar: bool,
) -> Result<()> {
    let input_files = expand_dirs(input, None).unwrap();
    process_files(input_files, output, filters, threads, no_progress_bar)
}

fn process_files(
    input_files: Vec<PathBuf>,
    output: &PathBuf,
    filters: &Arc<Vec<Box<dyn filter::Filter>>>,
    threads: &usize,
    no_progress_bar: bool,
) -> Result<()> {
    // Setup progress bar
    let pbar = ProgressBar::new(input_files.len() as u64)
        .with_style(
            ProgressStyle::with_template(
                "Files {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]",
            ).unwrap()
        );
    let pbar = Arc::new(Mutex::new(pbar));
    if !no_progress_bar {
        pbar.lock().unwrap().inc(0); // Makes pbar show up with 0/N files complete
    }

    let loop_start_time = Instant::now();
    let threadpool = ThreadPool::new(*threads);

    // let filters: Arc<Vec<Box<dyn filter::Filter>>> = register_filters();

    for input_file in input_files.into_iter() {
        let filters = Arc::clone(&filters);
        let (output_file, stat_file) = get_output_filename(&input_file, output);
        let pbar_option: Option<Arc<Mutex<ProgressBar>>> = if no_progress_bar {
            None
        } else {
            Some(pbar.clone())
        };

        threadpool.execute(move || {
            if no_progress_bar {
                info!("Processing {input_file:?}...");
            }

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            let result = rt.block_on(quality_filtering(
                input_file.clone(),
                output_file.clone(),
                stat_file.clone(),
                &filters,
                pbar_option.clone(),
            ));

            match result {
                Ok(_) => {}
                Err(err) => {
                    error!("Error processing {:?}; {:?}", input_file, err);
                }
            }
        });
    }

    threadpool.join();
    info!(
        "Complete filtering all files in {:?} seconds",
        loop_start_time.elapsed().as_secs()
    );

    Ok(())
}

fn get_output_filename(input_filename: &PathBuf, output_directory: &PathBuf) -> (PathBuf, PathBuf) {
    let file_name = input_filename.file_name().unwrap();
    (
        output_directory
            .clone()
            .join("processed_data")
            .join(file_name),
        output_directory.clone().join("stats").join(file_name),
    )
}

async fn quality_filtering(
    input_file: PathBuf,
    output_file: PathBuf,
    stat_file: PathBuf,
    filters: &[Box<dyn filter::Filter>],
    pbar_option: Option<Arc<Mutex<ProgressBar>>>,
) -> Result<(), Error> {
    let docs: Box<dyn Iterator<Item = Result<String, Error>>> = if is_oss(&input_file) {
        Box::new(
            get_reader_from_oss(input_file, None)
                .await
                .unwrap()
                .lines()
                .map(|r| r.map_err(Error::from)),
        )
    } else {
        let ext = input_file.extension().unwrap().to_str().unwrap();
        let input_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&input_file)?;

        match ext {
            "zstd" | "zst" => Box::new(
                BufReader::with_capacity(1024 * 1024, ZstDecoder::new(input_file).unwrap())
                    .lines()
                    .map(|r| r.map_err(Error::from)),
            ),
            "gz" => Box::new(
                BufReader::with_capacity(1024 * 1024, MultiGzDecoder::new(input_file))
                    .lines()
                    .map(|r| r.map_err(Error::from)),
            ),
            _ => Box::new(
                BufReader::with_capacity(1024 * 1024, input_file)
                    .lines()
                    .map(|r| r.map_err(Error::from)),
            ),
        }
    };
    let mut output_data: Vec<u8> = Vec::new();
    let mut fully_skipped = 0;
    let mut count = 0;

    let mut stat_collector: IndexMap<String, FilterStat> = IndexMap::new();
    for doc in docs {
        let doc = doc?;
        count += 1;
        let mut data: Value = serde_json::from_str(&doc).unwrap();

        let process_result = process_data(&mut data, &filters, &mut stat_collector);

        match process_result {
            Ok(true) => {
                output_data.extend(serde_json::to_vec(&data).unwrap());
                output_data.extend(b"\n");
            }
            Ok(false) => fully_skipped += 1,
            Err(_) => {}
        }
    }

    let stat_data: Vec<u8> = serde_json::to_vec_pretty(&stat_collector)?;
    log::debug!("filtering file in {:?}", stat_collector);

    let output_data = io::compress_data(output_data, &output_file);

    if fully_skipped < count {
        if is_oss(&output_file) {
            let (output_bucket, output_key) = split_oss_path(output_file);
            let (_, stat_key) = split_oss_path(stat_file);
            let client = oss::get_bucket(output_bucket);
            let mut headers = HashMap::new();
            headers.insert("content-type", "text/plain");

            let data: &[u8] = &output_data;
            let _ = client
                .put_object(data, output_key.clone(), headers.clone(), None)
                .await;

            let stat_data: &[u8] = &stat_data;
            let _ = client
                .put_object(stat_data, stat_key.clone(), headers.clone(), None)
                .await;
        } else {
            let mut output_file = OpenOptions::new()
                .read(false)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&output_file)?;
            output_file.write_all(&output_data)?;

            let mut stat_file = OpenOptions::new()
                .read(false)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&stat_file)?;
            stat_file.write_all(&stat_data)?;
        }
    }

    match pbar_option {
        Some(pbar) => {
            let pb = pbar.lock().unwrap();
            pb.inc(1);
        }
        None => (),
    }

    Ok(())
}

pub fn process_data(
    data: &mut Value,
    filters: &[Box<dyn filter::Filter>],
    stat_collector: &mut IndexMap<String, FilterStat>,
) -> Result<bool, Error> {
    for f in filters.iter() {
        let start_time = Instant::now();

        let result = f.filter(data);

        let execution_time = start_time.elapsed().as_millis() as i64;
        let stat = stat_collector
            .entry(f.name().to_string())
            .or_insert_with(|| FilterStat::new());
        stat.execution_time += execution_time;
        stat.page_in += 1;

        if result? {
            stat.page_out += 1;
        } else {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn run(
    input: Vec<PathBuf>,
    output: &PathBuf,
    filters: &Arc<Vec<Box<dyn filter::Filter>>>,
    threads: &usize,
    queue_id: &str,
    use_redis_task: bool,
    no_progress_bar: bool,
) -> Result<()> {
    util::print_banner();
    env_logger::init();

    let threads: &usize = if *threads == 0 {
        &available_parallelism().unwrap().get()
    } else {
        threads
    };

    if use_redis_task {
        let _ = process_tasks(output, filters, threads, queue_id, no_progress_bar);
    } else {
        let _ = process_input(input, output, filters, threads, no_progress_bar);
    }

    Ok(())
}
