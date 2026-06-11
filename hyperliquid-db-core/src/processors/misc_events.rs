use crate::{
    hl_fs::{HyperliquidDirData, HyperliquidDirDataWithMeta},
    processors::HyperliquidDataProcessorHandle,
    types::{HyperliquidData, HyperliquidDataWithMeta, MiscEvent, ParsedDataPipelineMeta},
    utils::unix_timestamp
};

#[derive(Default)]
pub struct MiscEventsDeriver {}

impl MiscEventsDeriver {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataProcessorHandle for MiscEventsDeriver {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Vec<HyperliquidData>> {
        let processing_data_at_ns = unix_timestamp().as_nanos();

        let rows = match &data.data {
            HyperliquidDirData::MiscEvents(items) => items,
            _ => return Ok(Vec::new())
        };

        let mut events = Vec::new();
        for row in rows {
            let time = row.block_time_unix_ms()?;
            for event in row.events.iter().cloned() {
                events.push(MiscEvent::from_event(time, row.block_number, event));
            }
        }

        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut pipeline_meta = ParsedDataPipelineMeta::default();
        pipeline_meta.modify_with_fs_data(&data.pipeline_meta);
        pipeline_meta.processing_data_at_ns = processing_data_at_ns;
        pipeline_meta.processed_data_at_ns = unix_timestamp().as_nanos();

        Ok(vec![HyperliquidData::MiscEvents(
            events
                .into_iter()
                .map(|event| HyperliquidDataWithMeta {
                    data:          event,
                    pipeline_meta: pipeline_meta.clone()
                })
                .collect()
        )])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::MiscEventsDeriver;
    use crate::{
        fs_handlers::types::FsOutData,
        hl_fs::{
            HyperliquidDirData, HyperliquidDirDataWithMeta, HyperliquidDirKind,
            schemas::{MiscEventsRows, NodeFillsRow}
        },
        processors::HyperliquidDataProcessorHandle,
        types::{HyperliquidData, MiscEventInner, MiscEventLedgerDelta}
    };

    #[test]
    fn derives_misc_events_from_rows() {
        let mut deriver = MiscEventsDeriver::new();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::MiscEvents(vec![events_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::MiscEvents)
            })
            .unwrap();

        assert_eq!(out.len(), 1);

        let HyperliquidData::MiscEvents(events) = &out[0] else { unreachable!() };
        assert_eq!(events.len(), 3);

        let ledger_update = &events[0].data;
        assert_eq!(ledger_update.time, 1_781_139_602_896);
        assert_eq!(ledger_update.height, 1030773868);
        assert_eq!(
            ledger_update.hash,
            "0x5a44d83dc07d168a5bbe043d705c6c02055b00235b70355cfe0d83907f70f074"
        );

        match &ledger_update.inner {
            MiscEventInner::LedgerUpdate { users, delta } => {
                assert_eq!(users.len(), 2);
                assert_eq!(users[0], "0x8b1c0722a978599bb254c2bdd75992fa605073f6");

                match delta {
                    MiscEventLedgerDelta::Send { user, token, amount, nonce, .. } => {
                        assert_eq!(user, "0x8b1c0722a978599bb254c2bdd75992fa605073f6");
                        assert_eq!(token, "USDC");
                        assert!((amount - 500.008953).abs() < f64::EPSILON);
                        assert_eq!(*nonce, 1781139594371);
                    }
                    _ => panic!("expected send ledger delta")
                }
            }
            _ => panic!("expected ledger update inner event")
        }

        match &events[1].data.inner {
            MiscEventInner::CDeposit { user, amount } => {
                assert_eq!(user, "0x163014fed389c392a0a214d2c4a28a73ec804741");
                assert!((amount - 0.00982529).abs() < f64::EPSILON);
            }
            _ => panic!("expected c deposit inner event")
        }

        match &events[2].data.inner {
            MiscEventInner::Funding { deltas } => {
                assert_eq!(deltas.len(), 2);
                assert_eq!(deltas[0].user, "0x00000000000dfd47d200dc26e4e5c79b7e6de984");
                assert_eq!(deltas[0].coin, "ZEC");
                assert!((deltas[0].funding_amount - 9.330246).abs() < f64::EPSILON);
                assert!((deltas[1].szi - -2744.84).abs() < f64::EPSILON);
            }
            _ => panic!("expected funding inner event")
        }
    }

    #[test]
    fn ignores_other_dir_data() {
        let mut deriver = MiscEventsDeriver::new();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeFills(vec![NodeFillsRow {
                    local_time:   "2026-06-11T11:00:00.710597964".to_string(),
                    block_time:   "2026-06-11T11:00:00.103069128".to_string(),
                    block_number: 1,
                    events:       Vec::new()
                }]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeFills)
            })
            .unwrap();

        assert!(out.is_empty());
    }

    #[test]
    fn skips_rows_without_events() {
        let mut deriver = MiscEventsDeriver::new();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::MiscEvents(vec![empty_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::MiscEvents)
            })
            .unwrap();

        assert!(out.is_empty());
    }

    fn events_row() -> MiscEventsRows {
        serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T01:00:03.104726053",
                "block_time":"2026-06-11T01:00:02.896899363",
                "block_number":1030773868,
                "events":[
                    {
                        "time":"2026-06-11T01:00:02.896899363",
                        "hash":"0x5a44d83dc07d168a5bbe043d705c6c02055b00235b70355cfe0d83907f70f074",
                        "inner":{"LedgerUpdate":{
                            "users":["0x8b1c0722a978599bb254c2bdd75992fa605073f6","0x2000000000000000000000000000000000000000"],
                            "delta":{
                                "type":"send",
                                "user":"0x8b1c0722a978599bb254c2bdd75992fa605073f6",
                                "destination":"0x2000000000000000000000000000000000000000",
                                "sourceDex":"spot",
                                "destinationDex":"spot",
                                "token":"USDC",
                                "amount":"500.008953",
                                "usdcValue":"500.008953",
                                "fee":"0.0",
                                "nativeTokenFee":"0.00002",
                                "nonce":1781139594371,
                                "feeToken":""
                            }
                        }}
                    },
                    {
                        "time":"2026-06-11T01:00:02.896899363",
                        "hash":"0x7fe9ff9e443d22a38163043d7061460207350083df30417523b2aaf10330fc8e",
                        "inner":{"CDeposit":{
                            "user":"0x163014fed389c392a0a214d2c4a28a73ec804741",
                            "amount":"0.00982529"
                        }}
                    },
                    {
                        "time":"2026-06-11T01:00:02.896899363",
                        "hash":"0x0000000000000000000000000000000000000000000000000000000000000000",
                        "inner":{"Funding":{"deltas":[
                            {"user":"0x00000000000dfd47d200dc26e4e5c79b7e6de984","coin":"ZEC","funding_amount":"9.330246","szi":"280.0","funding_rate":"-0.0000810249"},
                            {"user":"0x00000000002f3d928030df28cb15b27b56888b9b","coin":"HYPE","funding_amount":"4.506878","szi":"-2744.84","funding_rate":"0.0000308526"}
                        ]}}
                    }
                ]
            }"#
        )
        .unwrap()
    }

    fn empty_row() -> MiscEventsRows {
        serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T01:00:03.104726053",
                "block_time":"2026-06-11T01:00:02.896899363",
                "block_number":1030773868,
                "events":[]
            }"#
        )
        .unwrap()
    }

    fn fs_data(kind: HyperliquidDirKind) -> Arc<FsOutData> {
        Arc::new(FsOutData {
            name: kind,
            bytes: vec![],
            path: String::new(),
            chunk_len: 0,
            notification_received_at_ns: 0,
            pipeline: Default::default()
        })
    }
}
