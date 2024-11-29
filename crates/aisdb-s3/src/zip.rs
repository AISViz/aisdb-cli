use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use rayon::prelude::*;
use zip::ZipArchive;

pub fn fast_unzip_single(zip_path: &Path, dir_name: &Path) -> io::Result<()> {
    let existing: HashSet<OsString> = fs::read_dir(dir_name)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();
    
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    
    let contents: HashSet<String> = {
        let mut names = HashSet::new();
        for i in 0..archive.len() {
            if let Ok(file) = archive.by_index(i) {
                names.insert(file.name().to_string());
            }
        }
        names
    };
    
    let needs_extraction = contents.iter()
        .any(|name| !existing.contains(&OsString::from(name.as_str())));
        
    if needs_extraction {
        archive.extract(dir_name)?;
    }
    
    Ok(())
}

pub fn fast_unzip(zip_files: Vec<PathBuf>, dir_name: PathBuf, num_threads: usize) -> io::Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .unwrap();
        
    zip_files.par_iter()
        .try_for_each(|zip_path| fast_unzip_single(zip_path, &dir_name))
} 