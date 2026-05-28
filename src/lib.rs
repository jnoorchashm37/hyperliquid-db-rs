use std::{
    path::{Path, PathBuf},
    sync::mpsc
};

use crate::{
    fs_handlers::{directory_watcher::DirectoryWatcher, types::FsOutData},
    hl_fs::{HyperliquidDataDirKind, schemas::NodeFillsStreamingRow}
};

pub mod fs_handlers;
pub mod hl_fs;

pub const HYPERLIQUID_DATA_DIR: &str = "/var/lib/hyperliquid/hl/data";

pub fn run_stream() -> eyre::Result<()> {
    let (out_tx, out_rx) = mpsc::channel();
    println!("initializing watcher");
    let watcher = DirectoryWatcher::new(HyperliquidDataDirKind::NodeFillsStreaming, out_tx)?;
    println!("initialized watcher");
    watcher.run();

    loop {
        handle_incoming(out_rx.recv()??)?;
    }

    Ok(())
}

fn handle_incoming(data: FsOutData) -> eyre::Result<()> {
    let value = match data.name {
        HyperliquidDataDirKind::NodeFillsStreaming => {
            serde_json::from_slice::<NodeFillsStreamingRow>(&data.bytes)?
        }
        _ => unreachable!()
    };

    println!("{value:?}");

    Ok(())
}
