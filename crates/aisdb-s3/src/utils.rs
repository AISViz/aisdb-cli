use std::fs::File;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use md5::{Md5, Digest};
use rusoto_s3::{S3Client, S3, GetObjectRequest};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use std::env;

pub fn calculate_md5(file: &mut File) -> io::Result<String> {
    file.seek(SeekFrom::Start(0))?;
    let mut hasher = Md5::new();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    hasher.update(&buffer);
    let hash = hasher.finalize();
    Ok(BASE64.encode(hash))
}

pub async fn download_and_check(
    client: Arc<S3Client>,
    bucket: String,
    key: String,
    etag: String,
) -> io::Result<Option<(PathBuf, bool)>> {
    let temp_file = NamedTempFile::new()?;
    let mut file = temp_file.as_file().try_clone()?;
    
    let get_req = GetObjectRequest {
        bucket: bucket.clone(),
        key: key.clone(),
        if_none_match: Some(etag.clone()),
        ..Default::default()
    };

    match client.get_object(get_req).await {
        Ok(result) => {
            let stream = result.body.unwrap();
            let mut bytes = Vec::new();
            stream.into_async_read().read_to_end(&mut bytes)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            file.write_all(&bytes)?;
            
            let md5 = calculate_md5(&mut file)?;
            let clean_etag = etag.trim_matches('"');
            
            if md5 == clean_etag {
                let file_name = Path::new(&key).file_name()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid file name"))?;
                let temp_path = env::temp_dir().join(file_name);
                temp_file.persist(&temp_path)?;
                Ok(Some((temp_path, true)))
            } else {
                Ok(Some((temp_file.into_temp_path().to_path_buf(), false)))
            }
        }
        Err(e) => {
            if e.to_string().contains("304 Not Modified") {
                Ok(None)
            } else {
                Err(io::Error::new(io::ErrorKind::Other, e))
            }
        }
    }
} 