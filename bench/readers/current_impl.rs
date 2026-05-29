use std::{path::Path, sync::mpsc, thread};

use hyperliquid_db::{
    fs_handlers::{directory_watcher::DirectoryWatcher, types::FsOutData},
    hl_fs::HyperliquidDataDirKind
};

pub fn spawn_file_reader(
    name: HyperliquidDataDirKind,
    dir_path: &Path
) -> eyre::Result<mpsc::Receiver<eyre::Result<FsOutData>>> {
    let (tx, rx) = mpsc::channel();

    let watcher = DirectoryWatcher::new(name, tx)?;

    thread::spawn(move || {
        watcher.run();
    });

    Ok(rx)
}
