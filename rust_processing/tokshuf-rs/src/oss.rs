use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder as asyncGZ;
use async_compression::tokio::bufread::ZstdDecoder as asyncZstd;
use oss_rust_sdk::async_object::*;
use oss_rust_sdk::errors::Error as OSSError;
use oss_rust_sdk::oss::OSS;
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::io::{BufReader, Cursor};
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::process;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader as tBufReader;
use tokio::time::sleep;
use tokio::time::{sleep, Duration};

/// 获取指定 bucket 的 OSS 实例，若已存在则直接复用
pub fn get_bucket(bucket_name: String) -> OSS<'static> {
    let access_id = env::var("OSS_ACCESS_KEY_ID").unwrap();
    let access_secret = env::var("OSS_ACCESS_KEY_SECRET").unwrap();
    let endpoint = "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com/";
    OSS::new(access_id, access_secret, endpoint.into(), bucket_name)
}

/// 从 oss://bucket_name/path 格式的 URL 中解析出 bucket_name 和 path
fn split_file_path(url: &str) -> Result<(String, String), String> {
    if !url.starts_with("oss://") {
        return Err("Invalid OSS URL".into());
    }
    let rest = &url[6..]; // 去除 "oss://"
    let parts: Vec<&str> = rest.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err("Invalid OSS URL, missing path".into());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// 获取本机局域网 IP 地址（不返回 127.0.0.1）
fn get_local_ip() -> String {
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(local_addr) = socket.local_addr() {
                return local_addr.ip().to_string();
            }
        }
    }
    "127.0.0.1".to_string()
}

/// 根据本机 IP 和进程号生成唯一标识
pub fn get_worker_key() -> String {
    format!("{}_{}", get_local_ip(), process::id())
}

pub(crate) fn is_oss<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref()
        .to_str()
        .map_or(false, |s| s.starts_with("oss://"))
}

pub(crate) fn split_oss_path<P: AsRef<Path>>(path: P) -> (String, String) {
    // Splits s3_uri into (bucket, key)
    let path_str = path.as_ref().to_str().expect("Invalid path");

    let path_without_scheme = path_str
        .strip_prefix("oss://")
        .expect("Path must start with 'oss://'");

    let slash_index = path_without_scheme
        .find('/')
        .expect("Path must contain a slash after the bucket name");

    let bucket = &path_without_scheme[..slash_index];
    let key = &path_without_scheme[slash_index + 1..];
    (bucket.to_string(), key.to_string())
}

pub(crate) async fn write_cursor_to_oss(
    oss_uri: &PathBuf,
    cursor: Cursor<Vec<u8>>,
) -> Result<(), OSSError> {
    let (oss_bucket, oss_key) = split_oss_path(oss_uri);
    let client = get_bucket(oss_bucket);
    let bytes: &[u8] = &cursor.into_inner();
    let mut headers = HashMap::new();
    headers.insert("content-type", "text/plain");
    let response = client.put_object(bytes, oss_key, headers, None).await?;

    Ok(response)
}

async fn list_all_oss_objects(bucket: &str, prefix: &str) -> Result<Vec<String>> {
    let client = oss::get_bucket(bucket.to_string());
    let mut all_keys = Vec::new();
    let mut marker: Option<String> = None;
    let mut is_truncated = true;

    while is_truncated {
        let mut params = HashMap::new();
        params.insert("prefix", Some(prefix));
        params.insert("max-keys", Some("1000"));

        // 如果有marker，添加到参数中
        if let Some(mark) = &marker {
            params.insert("marker", Some(mark));
        }

        let result = client.list_object(None, params).await?;

        // 检查是否还有更多结果
        is_truncated = result.is_truncated();

        // 获取最后一个对象的key作为下一页的marker
        let contents = result.contents();
        marker = if is_truncated && !contents.is_empty() {
            Some(contents.last().unwrap().key().to_string())
        } else {
            None
        };

        for object in contents {
            all_keys.push(object.key().to_string());
        }
    }

    Ok(all_keys)
}

pub(crate) async fn expand_oss_dir(
    oss_uri: &PathBuf,
    valid_exts: &[&str],
) -> Result<Vec<PathBuf>, OSSError> {
    let mut oss_files: Vec<PathBuf> = Vec::new();
    let (bucket, prefix) = split_oss_path(oss_uri);

    // 调用封装的函数
    let all_keys = list_all_oss_objects(&bucket, &prefix).await?;

    for key in all_keys {
        if !(key.ends_with(".jsonl.gz")
            || key.ends_with(".jsonl")
            || key.ends_with(".jsonl.zstd")
            || key.ends_with(".tsv")
            || key.ends_with(".jsonl.zst"))
        {
            continue;
        }
        let mut oss_file = PathBuf::from("oss://");
        oss_file.push(bucket.clone());
        oss_file.push(&key);
        oss_files.push(oss_file);
    }

    Ok(oss_files)
}
