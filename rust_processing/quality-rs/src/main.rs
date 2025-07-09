use anyhow::{Error, Result};
use clap::Parser;
use flate2::read::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use io::expand_dirs;
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
use std::time::Instant;
use threadpool::ThreadPool;
use tokio::time::Instant as TokioInstant;

use zstd::stream::read::Decoder as ZstDecoder;

mod filter;
mod io;
mod oss;
mod util;

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

    for input_file in input_files.into_iter() {
        let output_file = get_output_filename(&input_file, output);
        let pbar_option: Option<Arc<Mutex<ProgressBar>>> = if no_progress_bar {
            None
        } else {
            Some(pbar.clone())
        };

        threadpool.execute(move || {
            if no_progress_bar {
                println!("Processing {input_file:?}...");
            }

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            let result = rt.block_on(quality_filtering(
                input_file.clone(),
                output_file.clone(),
                pbar_option.clone(),
            ));

            match result {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("Error processing {:?}; {:?}", input_file, err);
                }
            }
        });
    }

    threadpool.join();
    println!(
        "Complete filtering all files in {:?} seconds",
        loop_start_time.elapsed().as_secs()
    );

    Ok(())
}

fn get_output_filename(input_filename: &PathBuf, output_directory: &PathBuf) -> PathBuf {
    let file_name = input_filename.file_name().unwrap();
    output_directory.clone().join(file_name)
}

async fn quality_filtering(
    input_file: PathBuf,
    output_file: PathBuf,
    pbar_option: Option<Arc<Mutex<ProgressBar>>>,
) -> Result<(), Error> {
    let start_time = TokioInstant::now();
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

    for doc in docs {
        let doc = doc?;
        count += 1;
        let mut data: Value = serde_json::from_str(&doc).unwrap();
        process_data(&mut data)?;

        if let Some(text) = data.get("text") {
            if Some(text).unwrap().as_str().unwrap().trim().is_empty() {
                fully_skipped += 1
            } else {
                output_data.extend(serde_json::to_vec(&data).unwrap());
                output_data.extend(b"\n");
            }
        } else {
            continue;
        }
    }

    let output_data = io::compress_data(output_data, &output_file);
    if fully_skipped < count {
        if is_oss(&output_file) {
            let (output_bucket, output_key) = split_oss_path(output_file);
            let client = oss::get_bucket(output_bucket);
            let mut headers = HashMap::new();
            headers.insert("content-type", "text/plain");
            let data: &[u8] = &output_data;
            let _ = client
                .put_object(data, output_key.clone(), headers, None)
                .await;
        } else {
            let mut output_file = OpenOptions::new()
                .read(false)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&output_file)?;
            output_file.write_all(&output_data)?;
        }
    }

    match pbar_option {
        Some(pbar) => {
            let pb = pbar.lock().unwrap();
            pb.inc(1);
        }
        None => (),
    }

    println!(
        "filtering all files in {:?} seconds",
        start_time.elapsed().as_secs()
    );
    Ok(())
}

fn process_data(data: &mut Value) -> Result<bool, Error> {
    let mut filters: Vec<Box<dyn filter::Filter>> = Vec::new();
    // TODO 在这里注册更多的 Filter Trait
    filters.push(Box::new(filter::CacheTokenFilter {
        lang: "en".to_string(),
    }));
    filters.push(Box::new(filter::GopherRepetitionFilter {
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
    }));
    filters.push(Box::new(filter::FinewebQualityFilter {
        line_punct_thr: 0.12,
        short_line_length: 30,
        short_line_thr: 0.67,
        char_duplicates_ratio: 0.1,
        new_line_ratio: 0.3,
    }));
    filters.push(Box::new(filter::UncacheTokenFilter {}));

    for f in filters {
        let start_time = TokioInstant::now();
        if let Ok(false) = f.filter(data) {
            return Ok(false);
        }

        println!(
            "{:?} filtering all files in {:?} seconds",
            f.name(),
            start_time.elapsed().as_secs()
        );

        util::clear_key(data, util::TEXT_KEY);
    }
    Ok(true)
}

/*==============================================================
=                         MAIN BLOCK                           =
==============================================================*/

fn main() -> Result<()> {
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
