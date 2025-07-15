use crate::oss::is_oss;
use crate::oss::{count_oss_dirsize, expand_oss_dir, get_reader_from_oss, write_cursor_to_oss};
use anyhow::anyhow;
use anyhow::Error;
use color_eyre::eyre::Result;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use glob::glob;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

const VALID_EXTS: &[&str] = &[".jsonl", ".jsonl.gz", ".jsonl.zstd", ".jsonl.zst"];

/*======================================================================
=                              Listing files                           =
======================================================================*/

pub(crate) fn expand_dirs(
    paths: Vec<PathBuf>,
    manual_ext: Option<&[&str]>,
) -> Result<Vec<PathBuf>, Error> {
    // For local directories -> does a glob over each directory to get all files with given extension
    let exts = if !manual_ext.is_none() {
        manual_ext.unwrap()
    } else {
        VALID_EXTS
    };
    let mut files: Vec<PathBuf> = Vec::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for path in paths {
        if is_oss(path.clone()) {
            runtime.block_on(async {
                let oss_paths = expand_oss_dir(&path, exts).await.unwrap();
                files.extend(oss_paths);
            });
        } else if path.is_dir() {
            let path_str = path
                .to_str()
                .ok_or_else(|| anyhow!("invalid path '{}'", path.to_string_lossy()))?;
            for ext in exts {
                let pattern = format!("{}/**/*{}", path_str, ext);
                for entry in glob(&pattern).expect("Failed to read glob pattern") {
                    if let Ok(path) = entry {
                        files.push(path)
                    }
                }
            }
        } else {
            files.push(path.clone());
        }
    }
    Ok(files)
}

#[allow(dead_code)]
pub(crate) fn count_dirsize(dirname: &PathBuf) -> Result<usize, Error> {
    if is_oss(dirname) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(count_oss_dirsize(dirname));
        Ok(result?)
    } else {
        count_local_dirsize(dirname)
    }
}

#[allow(dead_code)]
fn count_local_dirsize(path: &PathBuf) -> Result<usize, Error> {
    let mut total_size: usize = 0;
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap().path();
        if entry.is_file() {
            total_size += fs::metadata(&entry).unwrap().len() as usize;
        } else if entry.is_dir() {
            total_size += count_local_dirsize(&entry).unwrap();
        }
    }
    Ok(total_size as usize)
}

/*====================================================================
=                           Reading files                            =
====================================================================*/

#[allow(dead_code)]
pub(crate) fn read_pathbuf_to_mem(
    input_file: &PathBuf,
) -> Result<BufReader<Cursor<Vec<u8>>>, Error> {
    // Generic method to read local or s3 file into memory
    let reader = if is_oss(input_file) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        match rt.block_on(get_reader_from_oss(input_file, Some(5))) {
            Ok(result) => result,
            Err(err) => {
                eprintln!("Error! {:?}", err);
                return Err(err.into());
            }
        }
    } else {
        let contents =
            read_local_file_into_memory(input_file).expect("Failed to read contents into memory");
        BufReader::new(contents)
    };
    Ok(reader)
}

#[allow(dead_code)]
fn read_local_file_into_memory(input_file: &PathBuf) -> Result<Cursor<Vec<u8>>, Error> {
    // Takes a local file (must be local!) and reads it into a Cursor of bytes
    let mut file = File::open(input_file).expect("Failed to open file");

    let mut contents = Vec::new();
    let ext = input_file
        .extension()
        .unwrap()
        .to_string_lossy()
        .to_lowercase();
    if ext == "gz" {
        // Gzip case
        let mut decoder = MultiGzDecoder::new(file);
        decoder
            .read_to_end(&mut contents)
            .expect("Failed to read local gz file");
    } else if ext == "zstd" || ext == "zst" {
        // Zstd case
        let mut decoder = ZstdDecoder::new(file).unwrap();
        decoder
            .read_to_end(&mut contents)
            .expect("Failed to read local zstd file");
    } else {
        file.read_to_end(&mut contents)
            .expect("Failed to read local file");

        // No compression case
    }
    Ok(Cursor::new(contents))
}

/*====================================================================
=                          Writing files                             =
====================================================================*/
#[allow(dead_code)]
pub(crate) fn write_mem_to_pathbuf(contents: &[u8], output_file: &PathBuf) -> Result<(), Error> {
    let compressed_data = compress_data(contents.to_vec(), output_file);
    if is_oss(output_file) {
        let cursor = Cursor::new(compressed_data);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        match rt.block_on(write_cursor_to_oss(&output_file, cursor)) {
            Ok(result) => result,
            Err(err) => {
                eprintln!("Error! {:?}", err);
                return Err(err.into());
            }
        };
    } else {
        if let Some(parent) = Path::new(output_file).parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = File::create(output_file)
            .expect(format!("Unable to create output file {:?}", output_file).as_str());
        file.write_all(&compressed_data)
            .expect(format!("Unable to write to {:?}", output_file).as_str());
    }
    Ok(())
}

pub(crate) fn compress_data(data: Vec<u8>, filename: &PathBuf) -> Vec<u8> {
    // Given a filename with an extension, compresses a bytestream accordingly
    // {zst, zstd} -> zstandard, {gz} -> gzip, anything else -> nothing
    let output_data = match filename.extension().unwrap().to_str() {
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
