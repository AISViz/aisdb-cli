use std::io;
use std::path::Path;
use rusoto_core::{Region, HttpClient};
use rusoto_credential::ProfileProvider;
use rusoto_s3::S3Client;

pub fn create_s3_client_with_config(config_path: &Path, profile: &str, region: Region) -> io::Result<S3Client> {
    let provider = ProfileProvider::with_configuration(
        config_path,
        profile,
    );
    
    let dispatcher = HttpClient::new()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        
    Ok(S3Client::new_with(
        dispatcher,
        provider,
        region
    ))
} 