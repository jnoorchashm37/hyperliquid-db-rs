use std::sync::mpsc;

use hyperliquid_db_core::{fs_handlers::DirectoryWatcher, hl_fs::HyperliquidDirKind};
use tracing::Level;

pub mod utils;

pub fn run_server() -> eyre::Result<()> {
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
