use std::{collections::HashMap, sync::mpsc};

use eyre::WrapErr;
pub mod constructed_data;

use crate::{
    constructed_data::{HyperliquidDataDeriver, TradeDeriver},
    fs_handlers::{directory_watcher::DirectoryWatcher, types::FsOutData},
    hl_fs::{HyperliquidDataDirKind, schemas::NodeFillsRow}
};

pub mod fs_handlers;
pub mod hl_fs;

pub const HYPERLIQUID_DATA_DIR: &str = "/var/lib/hyperliquid/hl/data";

pub fn run_stream() -> eyre::Result<()> {
    let (out_tx, out_rx) = mpsc::channel();

    println!("initializing watcher");
    let watcher = DirectoryWatcher::new(HyperliquidDataDirKind::NodeFills, out_tx)?;
    println!("initialized watcher");
    watcher.run();

    let mut deriver = TradeDeriver::new();
    loop {
        let data = out_rx.recv()??;

        match data.name {
            HyperliquidDataDirKind::NodeFills => deriver.handle_raw_data(data)?,
            _ => unreachable!()
        };
    }
}
