use std::{os::unix::fs::MetadataExt, path::PathBuf, time::UNIX_EPOCH};

use chrono::{DateTime, Utc};

const BYTES_TO_GB: u64 = 1024 * 1024 * 1024;

pub fn clean_hyperliquid_fs_data(
    hl_data_dir: PathBuf,
    max_age_hours: usize,
    max_size_gb: usize
) -> eyre::Result<()> {
    let mut files = Vec::new();
    recursive_files(&hl_data_dir, &mut files)?;

    files.sort_by_key(|file_with_meta| -1 * file_with_meta.size_gb as i64);

    for file in files {
        tracing::info!(
            "({} GB) {}  -  {}",
            file.size_gb,
            file.path.display(),
            file.last_touched.to_rfc2822()
        );
    }

    Ok(())
}

fn recursive_files(dir_path: &PathBuf, files: &mut Vec<FileWithMeta>) -> eyre::Result<()> {
    if !dir_path.exists() {
        return Err(eyre::eyre!("path does not exist: {}", dir_path.display()));
    }

    if !dir_path.is_dir() {
        return Err(eyre::eyre!("path is not a directory: {}", dir_path.display()));
    }

    for entry in std::fs::read_dir(dir_path)? {
        let path = entry?.path();

        if path.is_dir() {
            recursive_files(&path, files)?;
        } else if path.is_file() {
            files.push(FileWithMeta::new(&path)?);
        }
    }

    Ok(())
}

struct FileWithMeta {
    path:         PathBuf,
    last_touched: DateTime<Utc>,
    size_gb:      u64
}

impl FileWithMeta {
    fn new(path: &PathBuf) -> eyre::Result<Self> {
        if !path.exists() {
            return Err(eyre::eyre!("path does not exist: {}", path.display()));
        }

        if !path.is_file() {
            return Err(eyre::eyre!("path is not a file: {}", path.display()));
        }

        let meta = std::fs::metadata(path)?;

        let modified = meta.modified()?.duration_since(UNIX_EPOCH)?.as_secs();
        let accessed = meta.accessed()?.duration_since(UNIX_EPOCH)?.as_secs();
        let size_gb = meta.size() / BYTES_TO_GB;

        Ok(Self {
            path: path.clone(),
            last_touched: DateTime::from_timestamp(std::cmp::min(accessed, modified) as i64, 0)
                .unwrap(),
            size_gb
        })
    }
}
