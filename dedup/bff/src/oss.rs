use oss_rust_sdk::oss::OSS;
use std::env;

pub fn get_bucket(bucket_name: String) -> OSS<'static> {
    let access_id = env::var("OSS_ACCESS_KEY_ID").unwrap();
    let access_secret = env::var("OSS_ACCESS_KEY_SECRET").unwrap();
    println!("access_id: {access_id}, access_secret: {access_secret}");
    let endpoint = "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com/".into();
    let oss_instance = OSS::new(access_id, access_secret, endpoint, bucket_name);
    return oss_instance;
}
