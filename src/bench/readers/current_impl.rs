use std::{path::Path, sync::mpsc, thread};

use hyperliquid_db::fs_watchers::{
    directory::{DirectoryWatcher, OutData},
    types::HyperliquidDataDirKind,
};

pub fn spawn_file_reader(
    name: HyperliquidDataDirKind,
    dir_path: &Path,
) -> eyre::Result<mpsc::Receiver<OutData>> {
    let (tx, rx) = mpsc::channel();

    let watcher = DirectoryWatcher::new(name, &dir_path.to_path_buf(), tx)?;

    thread::spawn(move || {
        if let Err(err) = watcher.run_safe() {
            eprintln!("current file reader exited: {err:?}");
        }
    });

    Ok(rx)
}
