use std::{os::unix::fs::MetadataExt, path::PathBuf, time::UNIX_EPOCH};

use chrono::{DateTime, TimeDelta, Utc};

const BYTES_TO_MB: u64 = 1024 * 1024;

pub fn clean_hyperliquid_fs_data(
    hl_data_dir: &PathBuf,
    max_age_hours: usize,
    min_size_mb: u64
) -> eyre::Result<()> {
    tracing::info!(data_dir=?hl_data_dir.display(), max_age_hours,min_size_mb, "cleaning hyperliquid filesystem");
    let mut files = Vec::new();
    let max_age = Utc::now()
        .checked_sub_signed(TimeDelta::hours(max_age_hours as i64))
        .unwrap();
    recursive_files(hl_data_dir, max_age, min_size_mb, &mut files)?;

    files.sort_by_key(|file_with_meta| -1 * file_with_meta.size_mb as i64);

    for file in &files {
        tracing::debug!(path=?file.path.display(),size_mb=file.size_mb,last_touched=?file.last_touched.to_rfc2822(), "deleting file");
        std::fs::remove_file(&file.path)?;
    }

    tracing::info!(data_dir=?hl_data_dir.display(), max_age_hours,min_size_mb, "successfully removed {} files", files.len());

    Ok(())
}

fn recursive_files(
    dir_path: &PathBuf,
    max_age: DateTime<Utc>,
    min_size_mb: u64,
    files: &mut Vec<FileWithMeta>
) -> eyre::Result<()> {
    if !dir_path.exists() {
        return Err(eyre::eyre!("path does not exist: {}", dir_path.display()));
    }

    if !dir_path.is_dir() {
        return Err(eyre::eyre!("path is not a directory: {}", dir_path.display()));
    }

    for entry in std::fs::read_dir(dir_path)? {
        let path = entry?.path();

        if path.is_dir() {
            recursive_files(&path, max_age, min_size_mb, files)?;
        } else if path.is_file() {
            let file_with_meta = FileWithMeta::new(&path)?;
            if file_with_meta.size_mb >= min_size_mb
                && file_with_meta.last_touched.timestamp() <= max_age.timestamp()
            {
                files.push(file_with_meta);
            }
        }
    }

    Ok(())
}

struct FileWithMeta {
    path:         PathBuf,
    last_touched: DateTime<Utc>,
    size_mb:      u64
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
        let size_mb = meta.size() / BYTES_TO_MB;

        Ok(Self {
            path: path.clone(),
            last_touched: DateTime::from_timestamp(std::cmp::min(accessed, modified) as i64, 0)
                .unwrap(),
            size_mb
        })
    }
}
