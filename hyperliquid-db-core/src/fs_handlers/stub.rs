use std::sync::mpsc;

use crate::hl_fs::{HyperliquidDirData, HyperliquidDirKind};

pub struct DirectoryWatcher {
    out_tx: mpsc::Sender<eyre::Result<HyperliquidDirData>>
}

impl DirectoryWatcher {
    pub fn spawn(
        _name: HyperliquidDirKind,
        out_tx: mpsc::Sender<eyre::Result<HyperliquidDirData>>
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
