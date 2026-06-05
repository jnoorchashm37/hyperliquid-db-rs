use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex}
};

use eyre::{Context, ContextCompat};
use serde::{Deserialize, Serialize};

use crate::{
    processors::orderbook::{
        FETCH_SNAPSHOT_SLEEP_TIME_SEC,
        types::{Coin, InnerL4Order, Snapshot, Snapshots}
    },
    types::L4Order,
    utils::unix_timestamp
};

type RawSnapshotPayload<R> = (u64, Vec<(String, [Vec<R>; 2])>);

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
            println!(
                "[orderbook snapshot] waiting {FETCH_SNAPSHOT_SLEEP_TIME_SEC}s before requesting \
                 fresh l4 snapshot"
            );
            std::thread::sleep(std::time::Duration::from_secs(FETCH_SNAPSHOT_SLEEP_TIME_SEC));
            let snapshot_out_path = match Self::process_rmp_file() {
                Ok(snapshot_out_path) => snapshot_out_path,
                Err(error) => {
                    println!("[orderbook snapshot] failed to request snapshot: {error:?}");
                    return;
                }
            };
            println!("[orderbook snapshot] loading snapshot file {snapshot_out_path:?}");
            let (height, snapshots) =
                match Self::load_snapshots_from_file::<InnerL4Order, (String, L4Order)>(
                    &snapshot_out_path
                ) {
                    Ok((height, snapshots)) => (height, snapshots),
                    Err(error) => {
                        println!(
                            "[orderbook snapshot] failed to load snapshot file \
                             {snapshot_out_path:?}: {error:?}"
                        );
                        return;
                    }
                };
            println!("[orderbook snapshot] loaded snapshot height={height}");
            // sleep to let some updates build up.
            std::thread::sleep(std::time::Duration::from_secs(1));

            if let Err(error) = this.write(|val| *val = Some(StateSnapshot { height, snapshots })) {
                println!("[orderbook snapshot] failed to publish snapshot: {error:?}");
                return;
            }
            println!("[orderbook snapshot] snapshot published height={height}");
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

        let output_path = dir_path.join(format!("{}.json", unix_timestamp().as_secs()));
        println!(
            "[orderbook snapshot] requesting l4Snapshots from http://localhost:3001/info \
             out_path={output_path:?}"
        );

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
        println!("[orderbook snapshot] snapshot request accepted");

        Ok(output_path)
    }

    fn load_snapshots_from_file<O, R>(path: &PathBuf) -> eyre::Result<(u64, Snapshots<O>)>
    where
        O: TryFrom<R, Error = eyre::ErrReport>,
        R: Serialize + for<'a> Deserialize<'a>
    {
        let file_contents = std::fs::read_to_string(path)?;
        let (height, snapshot): RawSnapshotPayload<R> = serde_json::from_str(&file_contents)?;
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn loads_snapshot_orders_with_string_or_number_decimals() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot.json");
        let payload = json!([
            101,
            [
                [
                    "BTC",
                    [
                        [
                            [
                                "0xuser",
                                {
                                    "coin": "BTC",
                                    "side": "B",
                                    "limitPx": "0.60045",
                                    "sz": "1.25",
                                    "oid": 1,
                                    "timestamp": 123,
                                    "triggerCondition": "N/A",
                                    "isTrigger": false,
                                    "triggerPx": "0.0",
                                    "isPositionTpsl": false,
                                    "reduceOnly": false,
                                    "orderType": "Limit",
                                    "tif": "Gtc",
                                    "cloid": null
                                }
                            ]
                        ],
                        [
                            [
                                "0xuser",
                                {
                                    "coin": "BTC",
                                    "side": "A",
                                    "limitPx": 0.70045,
                                    "sz": 2.5,
                                    "oid": 2,
                                    "timestamp": 124,
                                    "triggerCondition": "N/A",
                                    "isTrigger": false,
                                    "triggerPx": 0.0,
                                    "isPositionTpsl": false,
                                    "reduceOnly": false,
                                    "orderType": "Limit",
                                    "tif": "Gtc",
                                    "cloid": null
                                }
                            ]
                        ]
                    ]
                ]
            ]
        ]);
        std::fs::write(&path, payload.to_string()).unwrap();

        let (height, snapshots) = StateSnapshotFetcher::load_snapshots_from_file::<
            InnerL4Order,
            (String, L4Order)
        >(&path)
        .unwrap();

        let mut snapshots = snapshots.value();
        let snapshot = snapshots.remove(&Coin::new("BTC")).unwrap();
        let [bids, asks] = snapshot.into_levels();

        assert_eq!(height, 101);
        assert!((bids[0].limit_px.to_f64() - 0.60045).abs() < f64::EPSILON);
        assert!((bids[0].sz.to_f64() - 1.25).abs() < f64::EPSILON);
        assert!((asks[0].limit_px.to_f64() - 0.70045).abs() < f64::EPSILON);
        assert!((asks[0].sz.to_f64() - 2.5).abs() < f64::EPSILON);
    }
}
