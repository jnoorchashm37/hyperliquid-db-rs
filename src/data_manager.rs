use std::{
    collections::HashMap,
    sync::{Arc, mpsc}
};

use itertools::Itertools;
use tokio::sync::broadcast;

use crate::{
    fs_handlers::DirectoryWatcher,
    hl_fs::{HyperliquidDirData, HyperliquidDirKind},
    types::HyperliquidDataKind
};

pub struct HyperliquidDataManager {
    raw_data_rx:      mpsc::Receiver<eyre::Result<HyperliquidDirData>>,
    subscription_txs:
        HashMap<HyperliquidDataKind, broadcast::Sender<Arc<eyre::Result<HyperliquidDirData>>>>,
    kind_map:         HashMap<HyperliquidDirKind, Vec<HyperliquidDataKind>>
}

impl HyperliquidDataManager {
    pub fn new(data_kinds: &[HyperliquidDataKind]) -> eyre::Result<Self> {
        tracing::info!("initializing data manager");

        let kind_map = data_kinds
            .iter()
            .flat_map(|data_kind| {
                data_kind
                    .required_dirs()
                    .into_iter()
                    .map(|dir_kind| (dir_kind, *data_kind))
            })
            .into_group_map();

        if kind_map.is_empty() {
            return Err(eyre::eyre!("dir_kinds cannot be empty"));
        }

        let (raw_data_tx, raw_data_rx) = mpsc::channel();

        kind_map.keys().try_for_each(|kind| {
            tracing::info!(?kind, "initializing directory watcher");

            DirectoryWatcher::spawn(*kind, raw_data_tx.clone())?;

            tracing::debug!(?kind, "initialized directory watcher");
            eyre::Ok(())
        })?;

        let subscription_txs = data_kinds
            .iter()
            .map(|data_kind| (*data_kind, broadcast::channel(10000).0))
            .collect();

        tracing::debug!("initialized data manager");

        Ok(Self { raw_data_rx, subscription_txs, kind_map })
    }

    pub fn subscribe(
        &mut self,
        data_kind: HyperliquidDataKind
    ) -> eyre::Result<broadcast::Receiver<Arc<eyre::Result<HyperliquidDirData>>>> {
        let tx = self.subscription_txs.get(&data_kind).ok_or_else(|| {
            eyre::eyre!("data dir {data_kind:?} should have a subscription registered")
        })?;
        Ok(tx.subscribe())
    }

    pub fn run(mut self) {
        assert!(!self.subscription_txs.is_empty());
        tracing::info!("running data manager");
        std::thread::spawn(move || {
            if let Err(error) = self.run_safe() {
                tracing::error!("error running data manager: {error:?}");
                self.send_kill(error);
            } else {
                let error = eyre::eyre!("data manager ended prematurely");
                tracing::error!(?error);
                self.send_kill(error);
            }
        });
    }

    fn run_safe(&mut self) -> eyre::Result<()> {
        loop {
            let data = self.raw_data_rx.recv()??;
            self.send_data(data)?;
        }
    }

    fn send_data(&mut self, data: HyperliquidDirData) -> eyre::Result<()> {
        let dir_kind = data.kind();
        let data = Arc::new(Ok(data));

        let data_kinds = self
            .kind_map
            .get(&dir_kind)
            .ok_or_else(|| eyre::eyre!("data dir {dir_kind} should have data kinds registered"))?;

        data_kinds.iter().try_for_each(|data_kind| {
            self.subscription_txs
                .get(data_kind)
                .ok_or_else(|| {
                    eyre::eyre!("data dir {data_kind:?} should have a subscription registered")
                })?
                .send(data.clone())?;
            eyre::Ok(())
        })?;

        Ok(())
    }

    fn send_kill(&self, error: eyre::ErrReport) {
        let data = Arc::new(Err(eyre::eyre!("{error:?}")));
        self.subscription_txs.iter().for_each(|(_, tx)| {
            tx.send(data.clone()).unwrap();
        });
    }
}
