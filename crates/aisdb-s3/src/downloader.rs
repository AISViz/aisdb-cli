use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use rusoto_core::Region;
use rusoto_s3::{S3Client, S3, ListObjectsRequest, DeleteObjectRequest};

use crate::client::create_s3_client_with_config;
use crate::utils::download_and_check;
use crate::zip::fast_unzip;

pub struct S3Downloader {
    pub(crate) client: Arc<S3Client>,
    pub(crate) bucket: String,
    prefix: String,
    dir_name: PathBuf,
    num_threads: usize,
}

impl S3Downloader {
    pub fn new_with_config(
        config_path: &Path,
        profile: &str,
        region: Region,
        bucket: String,
        prefix: String,
        dir_name: PathBuf,
        num_threads: usize,
    ) -> io::Result<Self> {
        let client = Arc::new(create_s3_client_with_config(config_path, profile, region)?);
        
        Ok(Self {
            client,
            bucket,
            prefix,
            dir_name,
            num_threads,
        })
    }

    pub async fn process_files(&self) -> io::Result<()> {
        if !self.dir_name.exists() {
            fs::create_dir_all(&self.dir_name)?;
        }

        let list_req = ListObjectsRequest {
            bucket: self.bucket.clone(),
            prefix: Some(self.prefix.clone()),
            ..Default::default()
        };

        let result = self.client.list_objects(list_req).await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            
        let objects = match result.contents {
            Some(objects) => objects,
            None => return Ok(()),
        };

        let mut downloads = Vec::new();
        for obj in objects {
            let key = obj.key.unwrap();
            let etag = obj.e_tag.unwrap();
            
            if let Ok(Some((path, checksum_matched))) = download_and_check(
                self.client.clone(),
                self.bucket.clone(),
                key.clone(),
                etag,
            ).await {
                if checksum_matched {
                    let delete_req = DeleteObjectRequest {
                        bucket: self.bucket.clone(),
                        key: key,
                        ..Default::default()
                    };
                    
                    if let Err(e) = self.client.delete_object(delete_req).await {
                        eprintln!("Failed to delete object: {}", e);
                    }
                }
                downloads.push(path);
            }
        }

        if !downloads.is_empty() {
            fast_unzip(downloads, self.dir_name.clone(), self.num_threads)?;
        }

        Ok(())
    }
} 