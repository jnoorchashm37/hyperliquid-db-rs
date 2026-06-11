use crate::{
    hl_fs::{HyperliquidDirData, HyperliquidDirDataWithMeta},
    processors::HyperliquidDataProcessorHandle,
    types::{HyperliquidData, HyperliquidDataWithMeta, ParsedDataPipelineMeta, ReplicaCmd},
    utils::unix_timestamp
};

#[derive(Default)]
pub struct ReplicaCmdsDeriver {}

impl ReplicaCmdsDeriver {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataProcessorHandle for ReplicaCmdsDeriver {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Vec<HyperliquidData>> {
        let processing_data_at_ns = unix_timestamp().as_nanos();

        let rows = match &data.data {
            HyperliquidDirData::ReplicaCmds(items) => items,
            _ => return Ok(Vec::new())
        };

        let mut cmds = Vec::new();
        for row in rows {
            let time = row.block_time_unix_ms()?;
            cmds.extend(ReplicaCmd::from_row(time, row.clone()));
        }

        if cmds.is_empty() {
            return Ok(Vec::new());
        }

        let mut pipeline_meta = ParsedDataPipelineMeta::default();
        pipeline_meta.modify_with_fs_data(&data.pipeline_meta);
        pipeline_meta.processing_data_at_ns = processing_data_at_ns;
        pipeline_meta.processed_data_at_ns = unix_timestamp().as_nanos();

        Ok(vec![HyperliquidData::ReplicaCmds(
            cmds.into_iter()
                .map(|cmd| HyperliquidDataWithMeta {
                    data:          cmd,
                    pipeline_meta: pipeline_meta.clone()
                })
                .collect()
        )])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::ReplicaCmdsDeriver;
    use crate::{
        fs_handlers::types::FsOutData,
        hl_fs::{
            HyperliquidDirData, HyperliquidDirDataWithMeta, HyperliquidDirKind,
            schemas::{NodeFillsRow, ReplicaCmdsRows}
        },
        processors::HyperliquidDataProcessorHandle,
        types::{
            HyperliquidData, ReplicaCmdAction, ReplicaCmdGrouping, ReplicaCmdOrderStatus,
            ReplicaCmdOrderType, ReplicaCmdRes, ReplicaCmdResponse
        }
    };

    #[test]
    fn derives_replica_cmds_from_rows() {
        let mut deriver = ReplicaCmdsDeriver::new();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::ReplicaCmds(vec![cmds_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::ReplicaCmds)
            })
            .unwrap();

        assert_eq!(out.len(), 1);

        let HyperliquidData::ReplicaCmds(cmds) = &out[0] else { unreachable!() };
        assert_eq!(cmds.len(), 2);

        let order_cmd = &cmds[0].data;
        assert_eq!(order_cmd.time, 1_781_198_868_584);
        assert_eq!(order_cmd.round, 1326518535);
        assert_eq!(order_cmd.parent_round, 1326518534);
        assert_eq!(order_cmd.proposer, "0x5ac99df645f3414876c816caa18b2d234024b487");
        assert_eq!(
            order_cmd.hash,
            "0xd8a0c8434c44bf6bc8f619a523091d0727ef29acba5f89078702167175b1935e"
        );
        assert_eq!(order_cmd.broadcaster, "0xc9ba414780c1b75e9806e3469300993fd2bfa883");
        assert_eq!(order_cmd.broadcaster_nonce, 1781198798932);
        assert_eq!(order_cmd.signature.v, 28);
        assert_eq!(order_cmd.nonce, 1781198704899);
        assert_eq!(order_cmd.vault_address, None);
        assert_eq!(order_cmd.user.as_deref(), Some("0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00"));

        match &order_cmd.action {
            ReplicaCmdAction::Order { orders, grouping, builder } => {
                assert_eq!(orders.len(), 1);
                assert_eq!(orders[0].a, 110049);
                assert!(!orders[0].b);
                assert!((orders[0].p - 91.545).abs() < f64::EPSILON);
                assert_eq!(orders[0].t, ReplicaCmdOrderType::Limit { tif: "Ioc".to_string() });
                assert_eq!(grouping, &ReplicaCmdGrouping::Named("na".to_string()));
                assert!(builder.is_none());
            }
            _ => panic!("expected order action")
        }

        match order_cmd.res.as_ref().unwrap() {
            ReplicaCmdRes::Ok { response: ReplicaCmdResponse::Order { data } } => {
                assert_eq!(data.statuses.len(), 1);
                match &data.statuses[0] {
                    ReplicaCmdOrderStatus::Resting { oid, cloid } => {
                        assert_eq!(*oid, 465972521426);
                        assert_eq!(cloid.as_deref(), Some("0x19eb7b922ece43e00000000000000000"));
                    }
                    _ => panic!("expected resting status")
                }
            }
            _ => panic!("expected ok order response")
        }

        let cancel_cmd = &cmds[1].data;
        assert_eq!(
            cancel_cmd.vault_address.as_deref(),
            Some("0x4bcc092a156905788bf2a1a59e5968623957210d")
        );
        assert_eq!(cancel_cmd.expires_after, Some(1781198899999));
        assert_eq!(cancel_cmd.user, None);

        match &cancel_cmd.action {
            ReplicaCmdAction::Cancel { cancels } => {
                assert_eq!(cancels.len(), 1);
                assert_eq!(cancels[0].a, 5);
                assert_eq!(cancels[0].o, 465988686977);
            }
            _ => panic!("expected cancel action")
        }

        match cancel_cmd.res.as_ref().unwrap() {
            ReplicaCmdRes::Err { response } => {
                assert_eq!(
                    response,
                    "User or API Wallet 0xab9c34e99e77ec2974e76c56fde158a8ea1637be does not exist."
                );
            }
            _ => panic!("expected err res")
        }
    }

    #[test]
    fn ignores_other_dir_data() {
        let mut deriver = ReplicaCmdsDeriver::new();

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
    fn skips_rows_without_cmds() {
        let mut deriver = ReplicaCmdsDeriver::new();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::ReplicaCmds(vec![empty_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::ReplicaCmds)
            })
            .unwrap();

        assert!(out.is_empty());
    }

    fn cmds_row() -> ReplicaCmdsRows {
        serde_json::from_str(
            r#"{
                "abci_block":{
                    "time":"2026-06-11T17:27:48.584957873",
                    "signed_action_bundles":[
                        ["0xd8a0c8434c44bf6bc8f619a523091d0727ef29acba5f89078702167175b1935e",{
                            "signed_actions":[
                                {
                                    "signature":{"r":"0xb6fd0a84550f94c90ff19c33f51880849e94005cc69e498948cbe5a9cc12ba7","s":"0x6555258e4de8749a2151ca834ea67fa9bf44891012b71e51f55c49d2e5d94c48","v":28},
                                    "action":{"type":"order","orders":[{"a":110049,"b":false,"p":"91.545","s":"4.37","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0xbbd0a82e4ab3713bc3607bb9f369a91f"}],"grouping":"na"},
                                    "nonce":1781198704899
                                },
                                {
                                    "signature":{"r":"0x75ab253011428dcc6cee7778799c9ce45628dc37c900e0320aaed482721da4d8","s":"0x184f6b20599ad176c004e4c084febfa0274892212e5c13a043901c27cadd9b88","v":27},
                                    "vaultAddress":"0x4bcc092a156905788bf2a1a59e5968623957210d",
                                    "expiresAfter":1781198899999,
                                    "action":{"type":"cancel","cancels":[{"a":5,"o":465988686977}]},
                                    "nonce":1781198704900
                                }
                            ],
                            "broadcaster":"0xc9ba414780c1b75e9806e3469300993fd2bfa883",
                            "broadcaster_nonce":1781198798932
                        }]
                    ],
                    "round":1326518535,
                    "parent_round":1326518534,
                    "hardfork":{"version":90,"round":1319594786},
                    "proposer":"0x5ac99df645f3414876c816caa18b2d234024b487"
                },
                "resps":{"Full":[
                    ["0xd8a0c8434c44bf6bc8f619a523091d0727ef29acba5f89078702167175b1935e",[
                        {"user":"0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00","res":{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":465972521426,"cloid":"0x19eb7b922ece43e00000000000000000"}}]}}}},
                        {"user":null,"res":{"status":"err","response":"User or API Wallet 0xab9c34e99e77ec2974e76c56fde158a8ea1637be does not exist."}}
                    ]]
                ]}
            }"#
        )
        .unwrap()
    }

    fn empty_row() -> ReplicaCmdsRows {
        serde_json::from_str(
            r#"{
                "abci_block":{
                    "time":"2026-06-11T17:27:48.584957873",
                    "signed_action_bundles":[],
                    "round":1326518535,
                    "parent_round":1326518534,
                    "hardfork":{"version":90,"round":1319594786},
                    "proposer":"0x5ac99df645f3414876c816caa18b2d234024b487"
                },
                "resps":{"Full":[]}
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
