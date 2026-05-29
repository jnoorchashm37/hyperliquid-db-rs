use std::sync::mpsc;

use crate::{fs_handlers::types::FsOutData, hl_fs::HyperliquidDataDirKind};

pub struct DirectoryWatcher {
    out_tx: mpsc::Sender<eyre::Result<FsOutData>>
}

impl DirectoryWatcher {
    pub fn new(
        _name: HyperliquidDataDirKind,
        out_tx: mpsc::Sender<eyre::Result<FsOutData>>
    ) -> eyre::Result<Self> {
        Ok(Self { out_tx })
    }

    pub fn run(self) {
        self.out_tx
            .send(Err(eyre::eyre!("cannot run directory watcher in stub mode")))
            .unwrap();
    }
}
