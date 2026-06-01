use std::sync::mpsc;

use crate::hl_fs::{HyperliquidDirDataWithMeta, HyperliquidDirKind};

pub struct DirectoryWatcher {
    out_tx: mpsc::Sender<eyre::Result<HyperliquidDirDataWithMeta>>
}

impl DirectoryWatcher {
    pub fn spawn(
        _name: HyperliquidDirKind,
        out_tx: mpsc::Sender<eyre::Result<HyperliquidDirDataWithMeta>>
    ) -> eyre::Result<()> {
        let watcher = Self { out_tx };
        watcher.run();

        Ok(())
    }

    pub fn run(self) {
        std::thread::spawn(move || {
            self.out_tx
                .send(Err(eyre::eyre!("cannot run directory watcher in stub mode")))
                .unwrap();
        });
    }
}
