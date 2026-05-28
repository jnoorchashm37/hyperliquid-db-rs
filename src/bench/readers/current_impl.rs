use std::{
    path::{Path, PathBuf},
    sync::mpsc
};

pub fn spawn_file_reader(
    name: HyperliquidDataDirKind,
    dir_path: &PathBuf
) -> mpsc::Reciever<OutData> {
    let (tx, rx) = mpsc::channel();

    let watcher =
        DirectoryWatcher::new(HyperliquidDataDirKind::ReplicaCmds, &directory, tx).unwrap();

    std::thread::spawn(move || {
        watcher.run_safe().unwrap();
    });

    rx
}
