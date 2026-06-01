pub mod builder;
mod ws_manager;
pub use ws_manager::*;
mod data_client;
pub mod route;

pub use data_client::*;
use hyperliquid_db_core::types::HyperliquidDataKind;
use tracing::Level;

use crate::builder::HyperliquidWebsocketBuilder;

pub mod utils;

const DEFAULT_RPC_ADDR: &str = "127.0.0.1:3000";

pub async fn run_server() -> eyre::Result<()> {
    crate::utils::init_logging(Level::DEBUG);

    let app = HyperliquidWebsocketBuilder::new(HyperliquidDataKind::all()).build()?;

    let listener = tokio::net::TcpListener::bind(DEFAULT_RPC_ADDR).await?;
    tracing::info!(addr = DEFAULT_RPC_ADDR, "running rpc server");
    axum::serve(listener, app).await?;

    Ok(())
}
