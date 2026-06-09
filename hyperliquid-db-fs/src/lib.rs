use std::{path::PathBuf, time::UNIX_EPOCH};

use chrono::{DateTime, Utc};

pub fn clean_hyperliquid_fs_data(hl_data_dir: PathBuf, max_age_hours: usize) -> eyre::Result<()> {
    let mut files = Vec::new();
    recursive_files(&hl_data_dir, &mut files)?;

    files.sort_by_key(|file_with_meta| -1 * file_with_meta.last_touched.timestamp());

    for file in files {
        println!("{}  -  {}", file.path.display(), file.last_touched.to_rfc2822());
    }

    Ok(())
}

fn recursive_files(dir_path: &PathBuf, files: &mut Vec<FileWithMeta>) -> eyre::Result<()> {
    if !dir_path.exists() {
        return Err(eyre::eyre!("path does not exist: {}", dir_path.display()));
    }

    Ok(())
}

struct FileWithMeta {
    path:         PathBuf,
    last_touched: DateTime<Utc>
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

        Ok(Self {
            path:         path.clone(),
            last_touched: DateTime::from_timestamp(std::cmp::min(accessed, modified) as i64, 0)
                .unwrap()
        })
    }
}
