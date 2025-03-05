use oss_rust_sdk::oss::OSS;
use std::env;

/// 获取指定 bucket 的 OSS 实例，若已存在则直接复用
pub fn get_bucket(bucket_name: String) -> OSS<'static> {
    // 否则，新建并插入到缓存中
    let access_id = env::var("OSS_ACCESS_KEY_ID").unwrap();
    let access_secret = env::var("OSS_ACCESS_KEY_SECRET").unwrap();
    println!("access_id: {access_id}, access_secret: {access_secret}");

    let endpoint = "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com/";
    let new_oss = OSS::new(access_id, access_secret, endpoint.into(), bucket_name);
    new_oss
}
