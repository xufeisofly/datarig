use anyhow::{anyhow, Error, Result};
use clap::Parser;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::{ProgressBar, ProgressStyle};
use io::expand_dirs;
use oss::split_oss_path;
use oss::{get_reader_from_oss, is_oss};
use oss_rust_sdk::async_object::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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

pub static TERMINAL_PUNCTUATION: [&str; 159] = [
    "᪩",
    "？",
    "⁈",
    "𑩂",
    "．",
    "꩞",
    "𑅃",
    "﹗",
    "𑂾",
    "\u{1B7D}",
    "፧",
    "𑅂",
    "꡶",
    "꘎",
    "⁉",
    "࠾",
    "᪨",
    "𑊩",
    "𑱂",
    "᱿",
    "𖩮",
    "᥅",
    "\u{11F43}",
    "\u{11F44}",
    "﹒",
    "𑈹",
    "𑈸",
    "።",
    "܂",
    "؞",
    "꛳",
    "\u{10F88}",
    "𑗍",
    "𐩖",
    "𑙂",
    "\u{061D}",
    "꩟",
    "᠉",
    "\u{1B7E}",
    "𑗗",
    "᰼",
    "𑻸",
    "？",
    "𑪜",
    "꧉",
    "𑗉",
    "𐽙",
    "𖫵",
    "𖬷",
    "܀",
    "꓿",
    "᜵",
    "𑗏",
    "𑁇",
    "𑗓",
    "𑥄",
    "៖",
    "𑥆",
    "𑗑",
    "𑗒",
    "꯫",
    "۔",
    "𐩗",
    "\u{10F86}",
    "꡷",
    "\u{2E54}",
    "｡",
    "៕",
    "߹",
    "⸮",
    ".",
    "𑇅",
    "࠹",
    "𛲟",
    "꫰",
    "꤯",
    "𐽗",
    "᭞",
    "𑜼",
    "፨",
    "𑃁",
    "꣏",
    "𑇟",
    "𖬸",
    "𑪛",
    "𑜾",
    "࠷",
    "𝪈",
    "?",
    "𑃀",
    "𑗃",
    "！",
    "։",
    "꣎",
    "॥",
    "𑗖",
    "᭛",
    "᠃",
    "!",
    "၊",
    "𖺘",
    "⁇",
    "𑗌",
    "𑑋",
    "𖭄",
    "᭟",
    "𑅁",
    "𑙁",
    "⸼",
    "꩝",
    "𑗋",
    "。",
    "꧈",
    "꫱",
    "𑜽",
    "𐽖",
    "𑂿",
    "᙮",
    "។",
    "꛷",
    "\u{10F89}",
    "៚",
    "᥄",
    "𑗕",
    "𑗎",
    "᪪",
    "᭚",
    "࠽",
    "𑇞",
    "𑗊",
    "𐽘",
    "\u{2E53}",
    "𑗔",
    "𖩯",
    "𑇍",
    "𑻷",
    "𐽕",
    "𑩃",
    "।",
    "𑗂",
    "𑇆",
    "𑁈",
    "။",
    "᱾",
    "𑱁",
    "꘏",
    "܁",
    "᜶",
    "‼",
    "𑈻",
    "‽",
    "᪫",
    "﹖",
    "𑑌",
    "𑈼",
    "\u{10F87}",
    "𑗐",
    "៙",
    "᰻",
];

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

    let output_data = compress_data(output_data, &output_file);
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
    Ok(())
}

fn clear_text_key(data: &mut Value) {
    if let Value::Object(ref mut map) = data {
        map.remove("text");
    }
}

fn process_data(data: &mut Value) -> Result<bool, Error> {
    if let Ok(false) = fineweb_quality_filter(data, 0.12, 30, 0.67, 0.1, 0.3) {
        clear_text_key(data);
    }
    Ok(true)
}

fn fineweb_quality_filter(
    data: &mut Value,
    line_punct_thr: f64,
    short_line_length: usize,
    short_line_thr: f64,
    char_duplicates_ratio: f64,
    new_line_ratio: f64,
) -> Result<bool, Error> {
    let text = data["text"].as_str().unwrap();
    let lines: Vec<&str> = text
        .split("\n")
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();
    if lines.len() == 0 {
        return Ok(false);
    }
    let stop_chars = TERMINAL_PUNCTUATION;
    let total = lines.len();
    if total == 0 {
        return Ok(false);
    }
    let count = lines
        .iter()
        .filter(|l| stop_chars.iter().any(|ch| l.ends_with(ch)))
        .count();

    if (count as f64 / total as f64) < line_punct_thr {
        return Ok(false);
    }

    if (lines
        .iter()
        .filter(|l| l.len() <= short_line_length)
        .count() as f64
        / total as f64)
        > short_line_thr
    {
        return Ok(false);
    }

    let (_, dup_chars) = find_duplicates(&lines);
    if (dup_chars as f64 / text.replace("\n", "").len() as f64) > char_duplicates_ratio {
        return Ok(false);
    }

    let result = split_words(text, "en", false, true);
    match result {
        Ok(tokens) => {
            if text.matches('\n').count() as f64 / tokens.len() as f64 > new_line_ratio {
                return Ok(false);
            }
        }
        Err(e) => {
            println!("split words failed: {}", e);
            return Err(e);
        }
    }

    Ok(true)
}

fn find_duplicates(x: &[&str]) -> (usize, usize) {
    let mut unique_x = HashSet::new();
    let mut duplicate_elements = 0;
    let mut duplicate_chars = 0;

    for &element in x {
        if unique_x.contains(element) {
            duplicate_elements += 1;
            duplicate_chars += element.len();
        } else {
            unique_x.insert(element);
        }
    }

    (duplicate_elements, duplicate_chars)
}

fn split_words(
    text: &str,
    lang: &str,
    ignore_punctuation: bool,
    ignore_whitespace: bool,
) -> Result<Vec<String>, Error> {
    let tok = VTextTokenizerParams::default().lang(lang).build()?;
    let mut tokens: Vec<String> = tok.tokenize(text).map(|s| s.to_string()).collect();

    if ignore_whitespace {
        tokens = tokens.iter().map(|w| w.trim().to_string()).collect();
    }
    if ignore_punctuation {
        tokens = tokens
            .into_iter()
            .filter(|w| {
                w.chars()
                    .next()
                    .map(|c| c.is_alphanumeric() || (!ignore_whitespace && c.is_whitespace()))
                    .unwrap_or(false)
            })
            .collect();
    }

    Ok(tokens)
}

fn compress_data(data: Vec<u8>, filename: &PathBuf) -> Vec<u8> {
    // 安全获取扩展名，防止无扩展名文件导致的崩溃
    let output_data = match filename.extension().and_then(|ext| ext.to_str()) {
        Some("gz") => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&data).unwrap();
            encoder.finish().unwrap()
        }
        Some("zstd") | Some("zst") => {
            let mut encoder = ZstdEncoder::new(Vec::new(), 0).unwrap();
            encoder.write_all(&data).unwrap();
            encoder.finish().unwrap()
        }
        _ => data,
    };
    output_data
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
