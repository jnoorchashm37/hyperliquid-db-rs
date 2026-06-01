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

pub struct HyperliquidDirDataManager {
    raw_data_rx:  mpsc::Receiver<eyre::Result<HyperliquidDirData>>,
    subscription: DirDataSubscriptionChannel
}

impl HyperliquidDirDataManager {
    pub fn new(
        data_kinds: &[HyperliquidDataKind],
        subscription_kind: DirDataSubscriptionChannelKind
    ) -> eyre::Result<Self> {
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

        let subscription = match subscription_kind {
            DirDataSubscriptionChannelKind::Unified => {
                DirDataSubscriptionChannel::Unified(broadcast::channel(10000).0)
            }
            DirDataSubscriptionChannelKind::DataSpecific => {
                let subscription_txs = data_kinds
                    .iter()
                    .map(|data_kind| (*data_kind, broadcast::channel(10000).0))
                    .collect();
                DirDataSubscriptionChannel::DataSpecific { subscription_txs, kind_map }
            }
        };

        tracing::debug!("initialized data manager");

        Ok(Self { raw_data_rx, subscription })
    }

    pub fn subscribe(
        &mut self,
        data_kind: HyperliquidDataKind
    ) -> eyre::Result<broadcast::Receiver<Arc<eyre::Result<HyperliquidDirData>>>> {
        self.subscription.subscribe(data_kind)
    }

    pub fn run(mut self) {
        assert!(!self.subscription.is_empty());
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
        self.subscription.send_data(data)
    }

    fn send_kill(&self, error: eyre::ErrReport) {
        self.subscription.send_kill(error)
    }
}

enum DirDataSubscriptionChannel {
    Unified(broadcast::Sender<Arc<eyre::Result<HyperliquidDirData>>>),
    DataSpecific {
        subscription_txs:
            HashMap<HyperliquidDataKind, broadcast::Sender<Arc<eyre::Result<HyperliquidDirData>>>>,
        kind_map:         HashMap<HyperliquidDirKind, Vec<HyperliquidDataKind>>
    }
}

impl DirDataSubscriptionChannel {
    fn subscribe(
        &self,
        data_kind: HyperliquidDataKind
    ) -> eyre::Result<broadcast::Receiver<Arc<eyre::Result<HyperliquidDirData>>>> {
        match self {
            DirDataSubscriptionChannel::Unified(sender) => Ok(sender.subscribe()),
            DirDataSubscriptionChannel::DataSpecific { subscription_txs, .. } => {
                let tx = subscription_txs.get(&data_kind).ok_or_else(|| {
                    eyre::eyre!("data dir {data_kind:?} should have a subscription registered")
                })?;
                Ok(tx.subscribe())
            }
        }
    }

    fn send_data(&mut self, data: HyperliquidDirData) -> eyre::Result<()> {
        let dir_kind = data.kind();
        let data = Arc::new(Ok(data));

        let (subscription_txs, kind_map) = match self {
            DirDataSubscriptionChannel::Unified(sender) => {
                sender.send(data)?;
                return Ok(())
            }
            DirDataSubscriptionChannel::DataSpecific { subscription_txs, kind_map } => {
                (subscription_txs, kind_map)
            }
        };

        let data_kinds = kind_map
            .get(&dir_kind)
            .ok_or_else(|| eyre::eyre!("data dir {dir_kind} should have data kinds registered"))?;

        data_kinds.iter().try_for_each(|data_kind| {
            subscription_txs
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
        match self {
            DirDataSubscriptionChannel::Unified(sender) => {
                sender.send(data).unwrap();
            }
            DirDataSubscriptionChannel::DataSpecific { subscription_txs, .. } => {
                subscription_txs.iter().for_each(|(_, tx)| {
                    tx.send(data.clone()).unwrap();
                });
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            DirDataSubscriptionChannel::Unified(_) => false,
            DirDataSubscriptionChannel::DataSpecific { subscription_txs, .. } => {
                subscription_txs.is_empty()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DirDataSubscriptionChannelKind {
    Unified,
    DataSpecific
}
