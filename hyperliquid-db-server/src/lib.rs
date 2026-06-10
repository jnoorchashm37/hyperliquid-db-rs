pub mod builder;
pub(crate) mod cli;
mod ws_manager;

use clap::Parser;
use hyperliquid_db_fs::{HyperliquidDataFsConfig, run_hyperliquid_fs_cleaner};
pub use ws_manager::WsHandler;
mod data_client;
pub mod play_bin;
pub mod route;
pub mod types;

pub use data_client::HyperliquidDataClient;

use crate::{builder::HyperliquidWebsocketBuilder, cli::HyperliquidDataRpcCli};

pub mod utils;

pub async fn run_server() -> eyre::Result<()> {
    let cli = HyperliquidDataRpcCli::parse();
    crate::utils::init_logging(cli.log_level.into());

    if cli.clean_fs {
        tracing::info!("running hyperliquid file cleaner");
        run_file_cleaner(cli.fs_cleaner_config());
    }

    let app = HyperliquidWebsocketBuilder::new(cli.hyperliquid_data_kinds()).build()?;

    let listener = tokio::net::TcpListener::bind(cli.rpc_addr).await?;
    tracing::info!(addr = ?cli.rpc_addr, "running rpc server");
    axum::serve(listener, app).await?;

    Ok(())
}

fn run_file_cleaner(config: HyperliquidDataFsConfig) {
    std::thread::spawn(move || {
        run_hyperliquid_fs_cleaner(config).unwrap();
    });
}
