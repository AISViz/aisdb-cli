use std::io;
use std::path::PathBuf;
use rusoto_core::Region;
use rusoto_s3::{S3, ListObjectsRequest};
use tempfile::TempDir;
use tempfile::NamedTempFile;
use std::io::Write;
use ::zip::write::ZipWriter;
use std::sync::Arc;

use crate::*;

fn get_config_path() -> io::Result<PathBuf> {
    let workspace_root = PathBuf::from("../../boto.cfg");
    if workspace_root.exists() {
        println!("Using config file from workspace root: {:?}", workspace_root);
        return Ok(workspace_root);
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "Could not find boto.cfg in any location",
    ))
}

#[tokio::test]
async fn test_s3_connection() -> io::Result<()> {
    let config_path = get_config_path()?;
    let profile = "meridian";
    let region = Region::Custom {
        name: "us-east-1".to_string(),
        endpoint: "https://s3.wasabisys.com".to_string(),
    };

    let client = create_s3_client_with_config(&config_path, profile, region)?;
    
    let list_req = ListObjectsRequest {
        bucket: "ee-meridian-exchange".to_string(),
        prefix: Some("".to_string()),
        ..Default::default()
    };

    match client.list_objects(list_req).await {
        Ok(_) => {
            println!("Successfully connected to S3");
            Ok(())
        }
        Err(e) => {
            eprintln!("Failed to connect to S3: {}", e);
            Err(io::Error::new(io::ErrorKind::Other, e.to_string()))
        }
    }
}


#[tokio::test]
async fn test_s3_downloader_creation() -> io::Result<()> {
    let config_path = get_config_path()?;
    let profile = "meridian";
    let region = Region::Custom {
        name: "us-east-1".to_string(),
        endpoint: "https://s3.wasabisys.com".to_string(),
    };
    let bucket = "ee-meridian-exchange".to_string();
    
    let temp_dir = TempDir::new()?;
    S3Downloader::new_with_config(
        &config_path,
        profile,
        region,
        bucket,
        "test/".to_string(),
        temp_dir.path().to_path_buf(),
        num_cpus::get(),
    )?;

    Ok(())
}

#[test]
fn test_calculate_md5() -> io::Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    let test_data = b"Hello, World!";
    temp_file.write_all(test_data)?;
    
    let mut file = temp_file.as_file_mut();
    let md5 = utils::calculate_md5(&mut file)?;
    let expected_md5 = "ZajifYh5KDgxtmS9i38K1A==";
    
    assert_eq!(md5, expected_md5);
    Ok(())
}

#[test]
fn test_zip_operations() -> io::Result<()> {
    // Create temporary directory
    let temp_dir = TempDir::new()?;
    let zip_path = temp_dir.path().join("test.zip");
    
    // Create test ZIP file
    {
        let zip_file = std::fs::File::create(&zip_path)?;
        let mut zip = ZipWriter::new(zip_file);
        
        // Add some test files
        zip.start_file("test1.txt", Default::default())?;
        zip.write_all(b"Test content 1")?;
        
        zip.start_file("test2.txt", Default::default())?;
        zip.write_all(b"Test content 2")?;
        
        zip.finish()?;
    }
    
    // Create extraction target directory
    let extract_dir = temp_dir.path().join("extract");
    std::fs::create_dir(&extract_dir)?;
    
    // Test single file extraction
    zip::fast_unzip_single(&zip_path, &extract_dir)?;
    
    // Verify files were extracted correctly
    assert!(extract_dir.join("test1.txt").exists());
    assert!(extract_dir.join("test2.txt").exists());
    
    // Test file content
    let content1 = std::fs::read_to_string(extract_dir.join("test1.txt"))?;
    assert_eq!(content1, "Test content 1");
    
    Ok(())
}

#[tokio::test]
async fn test_download_and_check() -> io::Result<()> {
    let config_path = get_config_path()?;
    let profile = "meridian";
    let region = Region::Custom {
        name: "us-east-1".to_string(),
        endpoint: "https://s3.wasabisys.com".to_string(),
    };

    let client = Arc::new(create_s3_client_with_config(&config_path, profile, region)?);
    let bucket = "ee-meridian-exchange".to_string();
    let key = "test/test.txt".to_string();  // 使用一个已知存在的文件
    let etag = "\"d41d8cd98f00b204e9800998ecf8427e\"".to_string();  // 空文件的 MD5

    if let Ok(Some((path, checksum_matched))) = utils::download_and_check(
        client,
        bucket,
        key,
        etag,
    ).await {
        assert!(path.exists());
        assert!(checksum_matched);
    }

    Ok(())
}

#[test]
fn test_parallel_zip_extraction() -> io::Result<()> {
    // Create multiple test ZIP files
    let temp_dir = TempDir::new()?;
    let mut zip_files = Vec::new();
    
    for i in 0..3 {
        let zip_path = temp_dir.path().join(format!("test{}.zip", i));
        let zip_file = std::fs::File::create(&zip_path)?;
        let mut zip = ZipWriter::new(zip_file);
        
        zip.start_file(format!("file{}.txt", i), Default::default())?;
        zip.write_all(format!("Content {}", i).as_bytes())?;
        zip.finish()?;
        
        zip_files.push(zip_path);
    }
    
    // Create extraction target directory
    let extract_dir = temp_dir.path().join("extract");
    std::fs::create_dir(&extract_dir)?;
    
    // Test parallel extraction
    zip::fast_unzip(zip_files, extract_dir.clone(), num_cpus::get())?;
    
    // Verify all files were extracted correctly
    for i in 0..3 {
        let file_path = extract_dir.join(format!("file{}.txt", i));
        assert!(file_path.exists());
        let content = std::fs::read_to_string(file_path)?;
        assert_eq!(content, format!("Content {}", i));
    }
    
    Ok(())
}