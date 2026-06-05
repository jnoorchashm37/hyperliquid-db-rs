use std::sync::{Arc, mpsc};

use itertools::Itertools;
use tokio::sync::broadcast;

use crate::{
    fs_handlers::DirectoryWatcher,
    hl_fs::HyperliquidDirDataWithMeta,
    processors::{
        HyperliquidDataProcessorHandle, HyperliquidDataProcessorKind, OrderBookDeriver,
        TradeDeriver
    },
    types::{HyperliquidData, HyperliquidDataKind}
};

pub struct HyperliquidDataManager {
    raw_data_rx:    mpsc::Receiver<eyre::Result<HyperliquidDirDataWithMeta>>,
    processors:     Vec<Box<dyn HyperliquidDataProcessorHandle>>,
    parsed_data_tx: broadcast::Sender<Arc<eyre::Result<HyperliquidData>>>
}

impl HyperliquidDataManager {
    pub fn spawn(
        data_kinds: &[HyperliquidDataKind]
    ) -> eyre::Result<broadcast::Sender<Arc<eyre::Result<HyperliquidData>>>> {
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

        let processors = data_kinds
            .iter()
            .map(HyperliquidDataKind::processor_kind)
            .unique()
            .map(kind_to_processor)
            .collect();

        tracing::debug!("spawned data manager");

        let parsed_data_tx = broadcast::channel(10000).0;
        let this = Self { raw_data_rx, parsed_data_tx: parsed_data_tx.clone(), processors };
        this.run();

        Ok(parsed_data_tx)
    }

    fn run(mut self) {
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

    fn send_data(&mut self, data: HyperliquidDirDataWithMeta) -> eyre::Result<()> {
        self.processors.iter_mut().try_for_each(|processor| {
            match processor.handle_data(&data) {
                Ok(results) => results.into_iter().try_for_each(|processed_data| {
                    self.parsed_data_tx.send(Arc::new(Ok(processed_data)))?;
                    eyre::Ok(())
                })?,
                Err(error) => {
                    self.parsed_data_tx.send(Arc::new(Err(error)))?;
                }
            }

            eyre::Ok(())
        })?;

        Ok(())
    }

    fn send_kill(&self, error: eyre::ErrReport) {
        self.parsed_data_tx.send(Arc::new(Err(error))).unwrap();
    }
}

fn kind_to_processor(
    kind: HyperliquidDataProcessorKind
) -> Box<dyn HyperliquidDataProcessorHandle> {
    match kind {
        HyperliquidDataProcessorKind::Trades => Box::new(TradeDeriver::new()),
        HyperliquidDataProcessorKind::Orderbook => Box::new(OrderBookDeriver::new())
    }
}
