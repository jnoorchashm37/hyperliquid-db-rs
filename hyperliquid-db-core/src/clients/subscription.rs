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

pub enum DirDataSubscriptionChannel {
    Unified(broadcast::Sender<Arc<eyre::Result<HyperliquidDirData>>>),
    DataSpecific {
        subscription_txs:
            HashMap<HyperliquidDataKind, broadcast::Sender<Arc<eyre::Result<HyperliquidDirData>>>>,
        kind_map:         HashMap<HyperliquidDirKind, Vec<HyperliquidDataKind>>
    }
}

impl DirDataSubscriptionChannel {
    pub fn subscribe(
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

    pub fn send_data(&mut self, data: HyperliquidDirData) -> eyre::Result<()> {
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

    pub fn send_kill(&self, error: eyre::ErrReport) {
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

    pub fn is_empty(&self) -> bool {
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
