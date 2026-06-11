use crate::{
    hl_fs::{HyperliquidDirData, HyperliquidDirDataWithMeta},
    processors::HyperliquidDataProcessorHandle,
    types::{Hip3OracleUpdate, HyperliquidData, HyperliquidDataWithMeta, ParsedDataPipelineMeta},
    utils::unix_timestamp
};

#[derive(Default)]
pub struct Hip3OracleUpdatesDeriver {}

impl Hip3OracleUpdatesDeriver {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataProcessorHandle for Hip3OracleUpdatesDeriver {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Vec<HyperliquidData>> {
        let processing_data_at_ns = unix_timestamp().as_nanos();

        let rows = match &data.data {
            HyperliquidDirData::Hip3OracleUpdates(items) => items,
            _ => return Ok(Vec::new())
        };

        let mut updates = Vec::new();
        for row in rows {
            let time = row.block_time_unix_ms()?;
            for event in row.events.iter().cloned() {
                updates.push(Hip3OracleUpdate::from_event(time, row.block_number, event));
            }
        }

        if updates.is_empty() {
            return Ok(Vec::new());
        }

        let mut pipeline_meta = ParsedDataPipelineMeta::default();
        pipeline_meta.modify_with_fs_data(&data.pipeline_meta);
        pipeline_meta.processing_data_at_ns = processing_data_at_ns;
        pipeline_meta.processed_data_at_ns = unix_timestamp().as_nanos();

        Ok(vec![HyperliquidData::Hip3OracleUpdates(
            updates
                .into_iter()
                .map(|update| HyperliquidDataWithMeta {
                    data:          update,
                    pipeline_meta: pipeline_meta.clone()
                })
                .collect()
        )])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::Hip3OracleUpdatesDeriver;
    use crate::{
        fs_handlers::types::FsOutData,
        hl_fs::{
            HyperliquidDirData, HyperliquidDirDataWithMeta, HyperliquidDirKind,
            schemas::{Hip3OracleUpdatesRows, NodeFillsRow}
        },
        processors::HyperliquidDataProcessorHandle,
        types::{Hip3OracleUpdateClass, HyperliquidData}
    };

    #[test]
    fn derives_oracle_updates_from_rows() {
        let mut deriver = Hip3OracleUpdatesDeriver::new();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::Hip3OracleUpdates(vec![update_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::Hip3OracleUpdates)
            })
            .unwrap();

        assert_eq!(out.len(), 1);

        let HyperliquidData::Hip3OracleUpdates(updates) = &out[0] else { unreachable!() };
        assert_eq!(updates.len(), 1);

        let update = &updates[0].data;
        assert_eq!(update.time, 1_781_175_600_103);
        assert_eq!(update.height, 1031299617);
        assert_eq!(update.update_class, Hip3OracleUpdateClass::Deployer);

        assert_eq!(update.mark_px_inputs.len(), 2);
        assert_eq!(update.mark_px_inputs[0].coin, "flx:BTC");
        assert!((update.mark_px_inputs[0].px - 90608.6).abs() < f64::EPSILON);
        assert!((update.spot_px_inputs[1].px - 158.33).abs() < f64::EPSILON);

        let mark_px = &update.oracle_pxs.coin_to_mark_px[0];
        assert_eq!(mark_px.coin, "flx:BTC");
        assert!((mark_px.px - 90608.6).abs() < f64::EPSILON);
        assert_eq!(mark_px.last_update_time_unix_ms().unwrap(), 1_781_175_600_103);
        assert!((mark_px.daily_px - 90608.6).abs() < f64::EPSILON);

        let oracle_px = &update.oracle_pxs.coin_to_oracle_px[0];
        assert!((oracle_px.px - 63171.0).abs() < f64::EPSILON);
        assert!((oracle_px.daily_px - 61521.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ignores_other_dir_data() {
        let mut deriver = Hip3OracleUpdatesDeriver::new();

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
        let mut deriver = Hip3OracleUpdatesDeriver::new();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::Hip3OracleUpdates(vec![empty_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::Hip3OracleUpdates)
            })
            .unwrap();

        assert!(out.is_empty());
    }

    fn update_row() -> Hip3OracleUpdatesRows {
        serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T11:00:00.710597964",
                "block_time":"2026-06-11T11:00:00.103069128",
                "block_number":1031299617,
                "events":[{
                    "update_class":"Deployer",
                    "mark_px_inputs":[["flx:BTC","90608.6"],["flx:COIN","158.42"]],
                    "spot_px_inputs":[["flx:BTC","63171.0"],["flx:COIN","158.33"]],
                    "external_perp_px_inputs":[["flx:BTC","63138.0"],["flx:COIN","154.02"]],
                    "oracle_pxs":{
                        "coin_to_mark_px":[["flx:BTC",{"px":"90608.6","last_update_time":"2026-06-11T11:00:00.103069128","daily_px":"90608.6"}]],
                        "coin_to_oracle_px":[["flx:BTC",{"px":"63171.0","last_update_time":"2026-06-11T11:00:00.103069128","daily_px":"61521.0"}]],
                        "coin_to_external_perp_px":[["flx:BTC",{"px":"63138.0","last_update_time":"2026-06-11T11:00:00.103069128","daily_px":"61485.0"}]]
                    }
                }]
            }"#
        )
        .unwrap()
    }

    fn empty_row() -> Hip3OracleUpdatesRows {
        serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T11:00:00.710597964",
                "block_time":"2026-06-11T11:00:00.103069128",
                "block_number":1031299617,
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
