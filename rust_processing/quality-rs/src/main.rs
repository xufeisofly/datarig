use anyhow::Error;
use clap::Parser;
use color_eyre::eyre::Result;
use flate2::read::MultiGzDecoder;
use indexmap::IndexMap;
use indicatif::{ProgressBar, ProgressStyle};
use io::expand_dirs;
use log::{error, info};
use oss::split_oss_path;
use oss::{get_reader_from_oss, is_oss};
use oss_rust_sdk::async_object::*;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::available_parallelism;
use std::time::Instant;
use threadpool::ThreadPool;
use util::print_banner;

use zstd::stream::read::Decoder as ZstDecoder;

mod filter;
mod io;
mod oss;
mod util;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/*======================================================
=                              ARGS                    =
======================================================*/

#[derive(Parser, Debug)]
struct Args {
    #[arg(required = true, long)]
    input: Vec<PathBuf>,

    #[arg(required = true, long)]
    output: PathBuf,

    #[arg(long, default_value_t = 0)]
    threads: usize,
}

#[derive(Debug, Serialize)]
struct FilterStat {
    execution_time: i64,
    page_in: i64,
    page_out: i64,
}

impl FilterStat {
    fn new() -> Self {
        Self {
            execution_time: 0,
            page_in: 0,
            page_out: 0,
        }
    }
}

fn register_filters() -> Arc<Vec<Box<dyn filter::Filter>>> {
    let filters: Arc<Vec<Box<dyn filter::Filter>>> = Arc::new(vec![
        Box::new(filter::LineRemovalModifier {
            max_removed_ratio: -1.0,
            max_uppercase_ratio: 0.99,
            min_word_cnt_per_line: 3,
            lang: "en".to_string(),
        }),
        Box::new(filter::CacheTokenFilter {
            lang: "en".to_string(),
        }),
        Box::new(filter::GopherRepetitionFilter {
            dup_line_frac: 0.3,
            dup_para_frac: 0.3,
            dup_line_char_frac: 0.2,
            dup_para_char_frac: 0.2,
            top_n_grams: vec![(2, 0.2), (3, 0.18), (4, 0.16)],
            dup_n_grams: vec![
                (5, 0.15),
                (6, 0.14),
                (7, 0.13),
                (8, 0.12),
                (9, 0.11),
                (10, 0.10),
            ],
            lang: "en".to_string(),
        }),
        Box::new(filter::GopherQualityFilter {
            min_doc_words: 50,
            max_doc_words: 100000,
            min_avg_word_length: 3,
            max_avg_word_length: 10,
            max_symbol_word_ratio: 0.1,
            max_bullet_lines_ratio: 0.9,
            max_ellipsis_lines_ratio: 0.3,
            max_non_alpha_words_ratio: 0.8,
            min_stop_words: 2,
            lang: "en".to_string(),
        }),
        Box::new(filter::FinewebQualityFilter {
            line_punct_thr: 0.12,
            short_line_length: 30,
            short_line_thr: 0.67,
            char_duplicates_ratio: 0.1,
            new_line_ratio: 0.3,
        }),
        Box::new(filter::UncacheTokenFilter {}),
    ]);
    filters
}

fn process_files(
    input: Vec<PathBuf>,
    output: &PathBuf,
    threads: &usize,
    no_progress_bar: bool,
) -> Result<()> {
    let input_files = expand_dirs(input, None).unwrap();
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

    let filters: Arc<Vec<Box<dyn filter::Filter>>> = register_filters();

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

fn process_data(
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

/*==============================================================
=                         MAIN BLOCK                           =
==============================================================*/

fn main() -> Result<()> {
    print_banner();
    env_logger::init();

    let args = Args::parse();

    let threads = if args.threads == 0 {
        available_parallelism().unwrap().get()
    } else {
        args.threads
    };

    let _ = process_files(args.input, &args.output, &threads, false);

    Ok(())
}
