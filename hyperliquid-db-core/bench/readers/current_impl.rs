use std::{path::Path, sync::mpsc, thread};

use hyperliquid_db::{
    fs_handlers::{DirectoryWatcher, types::FsOutData},
    hl_fs::HyperliquidDirKind
};

pub fn spawn_file_reader(
    name: HyperliquidDirKind,
    dir_path: &Path
) -> eyre::Result<mpsc::Receiver<eyre::Result<FsOutData>>> {
    let (tx, rx) = mpsc::channel();

    let watcher = DirectoryWatcher::new(name, tx)?;

    thread::spawn(move || {
        watcher.run();
    });

    Ok(rx)
}
