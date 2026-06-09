use std::path::Path;

use hyperliquid_db_fs::clean_hyperliquid_fs_data;
use tracing::Level;

pub mod utils;

const DEFAULT_DATA_DIR: &str = "/root/hl/data";
const DEFAULT_MAX_AGE_HOURS: u64 = 24;
const DEFAULT_MIN_SIZE_MB: u64 = 1000;

pub fn run() -> eyre::Result<()> {
    utils::init_logging(Level::DEBUG);

    let hl_data_dir = std::env::var("HL_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
    let max_age_hours = std::env::var("MAX_AGE_HOURS")
        .unwrap_or_else(|_| DEFAULT_MAX_AGE_HOURS.to_string())
        .parse()?;
    let min_size_mb = std::env::var("MIN_SIZE_MB")
        .unwrap_or_else(|_| DEFAULT_MIN_SIZE_MB.to_string())
        .parse()?;

    clean_hyperliquid_fs_data(&Path::new(&hl_data_dir).to_path_buf(), max_age_hours, min_size_mb)?;

    Ok(())
}
