use std::path::Path;

use hyperliquid_db_fs::clean_hyperliquid_fs_data;
use tracing::Level;

pub mod utils;

const DEFAULT_DATA_DIR: &str = "/root/hl/data";

pub fn run() -> eyre::Result<()> {
    utils::init_logging(Level::DEBUG);

    let hl_data_dir = std::env::var("HL_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());

    clean_hyperliquid_fs_data(Path::new(&hl_data_dir).to_path_buf(), 0)?;

    Ok(())
}
