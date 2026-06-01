use std::sync::mpsc;

pub mod types;

mod data_manager;
pub use data_manager::*;
pub mod utils;

use tracing::Level;

pub mod processors;

use crate::{fs_handlers::DirectoryWatcher, hl_fs::HyperliquidDirKind};

pub mod fs_handlers;
pub mod hl_fs;

pub const HYPERLIQUID_DATA_DIR: &str = "/var/lib/hyperliquid/hl/data";

pub fn run_stream() -> eyre::Result<()> {
    crate::utils::init_logging(Level::DEBUG);

    let (out_tx, out_rx) = mpsc::channel();

    tracing::info!("initializing watcher");
    DirectoryWatcher::spawn(HyperliquidDirKind::NodeFills, out_tx)?;
    tracing::info!("initialized watcher");

    // let mut deriver = TradeDeriver::new();
    // tracing::info!("created TradeDeriver watcher");
    // loop {
    //     let data = out_rx.recv()??;

    //     let out = match data.name {
    //         HyperliquidDirKind::NodeFills => deriver.handle_raw_data(data)?,
    //         _ => unreachable!()
    //     };
    //     tracing::info!("{out:?}");
    // }

    Ok(())
}
