use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex}
};

use eyre::{Context, ContextCompat};
use serde::{Deserialize, Serialize};

use crate::{
    processors::l4_orderbook::{
        FETCH_SNAPSHOT_SLEEP_TIME_SEC,
        types::{Coin, InnerL4Order, Snapshot, Snapshots}
    },
    types::L4Order,
    utils::unix_timestamp
};

#[derive(Clone)]
pub struct StateSnapshotFetcher {
    snapshot:   Arc<Mutex<Option<StateSnapshot>>>,
    auto_fetch: bool
}

impl StateSnapshotFetcher {
    pub fn new() -> Self {
        let this = Self { snapshot: Arc::new(Mutex::new(None)), auto_fetch: true };
        this.fetch_new();

        this
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        Self { snapshot: Arc::new(Mutex::new(None)), auto_fetch: false }
    }

    #[cfg(test)]
    pub fn with_snapshot(snapshot: StateSnapshot) -> Self {
        Self { snapshot: Arc::new(Mutex::new(Some(snapshot))), auto_fetch: false }
    }

    #[allow(unused)]
    pub fn read<T>(&self, f: impl FnOnce(&Option<StateSnapshot>) -> T) -> eyre::Result<T> {
        let lock = self.snapshot.try_lock().map_err(|e| eyre::eyre!("{e:?}"))?;
        let val = f(&lock);
        drop(lock);
        Ok(val)
    }

    pub fn write<T>(&self, f: impl FnOnce(&mut Option<StateSnapshot>) -> T) -> eyre::Result<T> {
        let mut lock = self.snapshot.try_lock().map_err(|e| eyre::eyre!("{e:?}"))?;
        let val = f(&mut lock);
        drop(lock);
        Ok(val)
    }

    #[allow(unused)]
    pub fn is_snapshot_set(&self) -> eyre::Result<bool> {
        self.read(|val| val.is_some())
    }

    pub fn fetch_new(&self) {
        if !self.auto_fetch {
            return;
        }

        let this = self.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(FETCH_SNAPSHOT_SLEEP_TIME_SEC));
            let snapshot_out_path = Self::process_rmp_file().unwrap();
            let (height, snapshots) = Self::load_snapshots_from_file::<
                InnerL4Order,
                (String, L4Order)
            >(&snapshot_out_path)
            .unwrap();
            // sleep to let some updates build up.
            std::thread::sleep(std::time::Duration::from_secs(1));

            this.write(|val| *val = Some(StateSnapshot { height, snapshots }))
                .unwrap();
        });
    }

    fn process_rmp_file() -> eyre::Result<PathBuf> {
        let dir_path = std::env::home_dir()
            .wrap_err("Could not find home directory")?
            .join("hyperliquid-db-rs-state-snapshots");
        if !dir_path.exists() {
            std::fs::create_dir_all(&dir_path)
                .wrap_err(format!("could not create directory: {dir_path:?}"))?;
        }

        let output_path = dir_path.join(&format!("{}.json", unix_timestamp().as_secs()));

        let payload = serde_json::json!({
            "type": "fileSnapshot",
            "request": {
                "type": "l4Snapshots",
                "includeUsers": true,
                "includeTriggerOrders": false
            },
            "outPath": output_path,
            "includeHeightInOutput": true
        });

        let client = reqwest::blocking::Client::new();
        client
            .post("http://localhost:3001/info")
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()?
            .error_for_status()?;

        Ok(output_path)
    }

    fn load_snapshots_from_file<O, R>(path: &PathBuf) -> eyre::Result<(u64, Snapshots<O>)>
    where
        O: TryFrom<R, Error = eyre::ErrReport>,
        R: Serialize + for<'a> Deserialize<'a>
    {
        #[allow(clippy::type_complexity)]
        let file_contents = std::fs::read_to_string(path)?;
        let (height, snapshot): (u64, Vec<(String, [Vec<R>; 2])>) =
            serde_json::from_str(&file_contents)?;
        Ok((
            height,
            Snapshots::new(
                snapshot
                    .into_iter()
                    .map(|(coin, [bids, asks])| {
                        let bids: Vec<O> = bids
                            .into_iter()
                            .map(O::try_from)
                            .collect::<eyre::Result<Vec<O>>>()?;
                        let asks: Vec<O> = asks
                            .into_iter()
                            .map(O::try_from)
                            .collect::<eyre::Result<Vec<O>>>()?;
                        Ok((Coin::new(&coin), Snapshot::new([bids, asks])))
                    })
                    .collect::<eyre::Result<HashMap<Coin, Snapshot<O>>>>()?
            )
        ))
    }
}

impl Default for StateSnapshotFetcher {
    fn default() -> Self {
        Self::new()
    }
}

pub struct StateSnapshot {
    pub height:    u64,
    pub snapshots: Snapshots<InnerL4Order>
}
