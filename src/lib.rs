use std::sync::mpsc;

pub mod constructed_data;
pub mod utils;

use crate::{
    constructed_data::{HyperliquidDataDeriver, TradeDeriver},
    fs_handlers::DirectoryWatcher,
    hl_fs::HyperliquidDataDirKind
};

pub mod fs_handlers;
pub mod hl_fs;

pub const HYPERLIQUID_DATA_DIR: &str = "/var/lib/hyperliquid/hl/data";

pub fn run_stream() -> eyre::Result<()> {
    crate::utils::init_logging();
    let (out_tx, out_rx) = mpsc::channel();

    tracing::info!("initializing watcher");
    let watcher = DirectoryWatcher::new(HyperliquidDataDirKind::NodeFills, out_tx)?;
    tracing::info!("initialized watcher");
    watcher.run();

    let mut deriver = TradeDeriver::new();
    loop {
        let data = out_rx.recv()??;

        let out = match data.name {
            HyperliquidDataDirKind::NodeFills => deriver.handle_raw_data(data)?,
            _ => unreachable!()
        };
        tracing::info!("{out:?}");
    }
}
