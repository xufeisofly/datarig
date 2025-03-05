use once_cell::sync::Lazy;
use oss_rust_sdk::oss::OSS;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
};

/// 全局缓存：key是bucket名称，value是OSS实例（用Arc包裹）。
/// 一旦创建，会一直存在，重复请求相同的 bucket 名会直接复用。
static OSS_CACHE: Lazy<Mutex<HashMap<String, Arc<OSS<'static>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// 获取指定 bucket 的 OSS 实例，若已存在则直接复用
pub fn get_bucket(bucket_name: String) -> Arc<OSS<'static>> {
    // 上锁访问全局缓存
    let mut cache = OSS_CACHE.lock().unwrap();

    // 如果已经创建过，则直接返回已有实例的 Arc 克隆
    if let Some(oss_instance) = cache.get(&bucket_name) {
        return Arc::clone(oss_instance);
    }

    // 否则，新建并插入到缓存中
    let access_id = env::var("OSS_ACCESS_KEY_ID").unwrap();
    let access_secret = env::var("OSS_ACCESS_KEY_SECRET").unwrap();
    println!("access_id: {access_id}, access_secret: {access_secret}");

    let endpoint = "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com/";
    let new_oss = Arc::new(OSS::new(
        access_id,
        access_secret,
        endpoint.into(),
        bucket_name.clone(),
    ));

    cache.insert(bucket_name, Arc::clone(&new_oss));
    new_oss
}
