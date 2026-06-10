pub mod builder;
pub(crate) mod cli;
mod ws_manager;
use std::path::Path;

use hyperliquid_db_fs::run_hyperliquid_fs_cleaner;
pub use ws_manager::WsHandler;
mod data_client;
pub mod play_bin;
pub mod route;
pub mod types;

pub use data_client::HyperliquidDataClient;
use hyperliquid_db_core::types::HyperliquidDataKind;
use tracing::Level;

use crate::builder::HyperliquidWebsocketBuilder;

pub mod utils;

const DEFAULT_DATA_DIR: &str = "/root/hl/data";
const DEFAULT_MAX_AGE_HOURS: u64 = 24;
const DEFAULT_MIN_SIZE_MB: u64 = 1000;
const DEFAULT_FS_CLEAN_INTERVAL_HRS: u64 = 12;

const DEFAULT_RPC_ADDR: &str = "127.0.0.1:3000";

pub async fn run_server() -> eyre::Result<()> {
    crate::utils::init_logging(Level::DEBUG);

    let clean_fs = std::env::var("CLEAN_FS")
        .unwrap_or_else(|_| true.to_string())
        .parse()?;
    if clean_fs {
        tracing::info!("running hyperliquid file cleaner");
        run_file_cleaner()?;
    }

    let app = HyperliquidWebsocketBuilder::new(HyperliquidDataKind::all()).build()?;

    let listener = tokio::net::TcpListener::bind(DEFAULT_RPC_ADDR).await?;
    tracing::info!(addr = DEFAULT_RPC_ADDR, "running rpc server");
    axum::serve(listener, app).await?;

    Ok(())
}

fn run_file_cleaner() -> eyre::Result<()> {
    let hl_data_dir = std::env::var("HL_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
    let hl_data_path = Path::new(&hl_data_dir).to_path_buf();

    let max_age_hours = std::env::var("MAX_AGE_HOURS")
        .unwrap_or_else(|_| DEFAULT_MAX_AGE_HOURS.to_string())
        .parse()?;
    let min_size_mb = std::env::var("MIN_SIZE_MB")
        .unwrap_or_else(|_| DEFAULT_MIN_SIZE_MB.to_string())
        .parse()?;

    let fs_clean_interval = std::env::var("FS_CLEAN_INTERVAL_HRS")
        .unwrap_or_else(|_| DEFAULT_FS_CLEAN_INTERVAL_HRS.to_string())
        .parse()?;

    std::thread::spawn(move || {
        loop {
            if let Err(e) =
                run_hyperliquid_fs_cleaner(&hl_data_path, max_age_hours, min_size_mb, None)
            {
                tracing::error!(?e, "error cleaning filesystem - retrying in 1 minute");
                std::thread::sleep(std::time::Duration::from_mins(1));
            } else {
                std::thread::sleep(std::time::Duration::from_hours(fs_clean_interval));
            }
        }
    });

    Ok(())
}
