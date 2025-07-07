use anyhow::{anyhow, Error, Result};
use async_compression::tokio::bufread::GzipDecoder as asyncGZ;
use async_compression::tokio::bufread::ZstdDecoder as asyncZstd;
use clap::Parser;
use flate2::read::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use io::expand_dirs;
use oss::{get_reader_from_oss, is_oss};
use std::fs::{create_dir_all, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Cursor, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::available_parallelism;
use std::time::Instant;
use threadpool::ThreadPool;
use vtext::tokenize::{Tokenizer, VTextTokenizerParams};
use zstd::stream::read::Decoder as ZstDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

pub mod io;
pub mod oss;

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

    for input_file in input_files {
        let output_file = get_output_filename(&input_file, output);
        let pbar_option: Option<Arc<Mutex<ProgressBar>>> = if no_progress_bar {
            None
        } else {
            Some(pbar.clone())
        };

        threadpool.execute(move || {
            println!("Worker thread id: {:?}", std::thread::current().id());
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
                Ok(_) => {
                    println!("success");
                }
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
    println!("INPUT: {input_file:?}, OUTPUT: {output_file:?}");

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

    // TODO process docs

    match pbar_option {
        Some(pbar) => {
            let pb = pbar.lock().unwrap();
            pb.inc(1);
        }
        None => (),
    }
    Ok(())
}

fn split_words(text: &str, lang: &str) -> Result<Vec<String>, Error> {
    let tok = VTextTokenizerParams::default().lang(lang).build()?;

    let tokens: Vec<String> = tok.tokenize(text).map(|s| s.to_string()).collect();

    println!("tokens: {:?}", tokens);
    Ok(tokens)
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

    println!("Main thread id: {:?}", std::thread::current().id());
    let _ = process_files(args.input, &args.output, &threads, false);
    Ok(())
}
