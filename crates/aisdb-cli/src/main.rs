use std::env;
use std::io;
use std::path::{Path, PathBuf};
use rusoto_core::Region;
use aisdb_s3::S3Downloader;
use num_cpus;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 7 {
        eprintln!("Usage: {} <config_path> <profile> <region> <bucket> <prefix> <dir>", args[0]);
        return Ok(());
    }

    let config_path = Path::new(&args[1]);
    let profile = &args[2];
    let region_str = args[3].clone();
    let region = Region::Custom {
        name: region_str.clone(),
        endpoint: format!("https://s3.{}.amazonaws.com", region_str),
    };
    let bucket = args[4].clone();
    let prefix = args[5].clone();
    let dir_name = PathBuf::from(&args[6]);
    let num_threads = num_cpus::get();

    let downloader = S3Downloader::new_with_config(
        config_path,
        profile,
        region,
        bucket,
        prefix,
        dir_name,
        num_threads,
    )?;

    downloader.process_files().await?;

    Ok(())
} 