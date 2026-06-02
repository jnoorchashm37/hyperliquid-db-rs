use std::{collections::HashMap, sync::Arc};

use crate::{
    fs_handlers::types::FsOutData,
    hl_fs::{
        HyperliquidDirData, HyperliquidDirDataWithMeta,
        schemas::{NodeFillsFill, NodeFillsSide}
    },
    processors::HyperliquidDataProcessorHandle,
    types::{
        HyperliquidData, HyperliquidDataWithMeta, ParsedDataPipelineMeta, PendingTrade, Trade
    },
    utils::unix_timestamp
};

#[derive(Default)]
pub struct TradeDeriver {
    pending_fills: HashMap<u64, (PendingTrade, ParsedDataPipelineMeta)>
}

impl TradeDeriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pending_fill_count(&self) -> usize {
        self.pending_fills.len()
    }

    fn new_fill(
        &mut self,
        fill: NodeFillsFill,
        fs_data: Arc<FsOutData>,
        processing_data_at_ns: u128
    ) -> eyre::Result<Option<(Trade, ParsedDataPipelineMeta)>> {
        let tid = fill.tid;
        let (pending_trade, pipeline_meta) = self.pending_fills.entry(tid).or_default();
        pipeline_meta.modify_with_fs_data(&fs_data);

        match fill.side {
            NodeFillsSide::A => pending_trade.ask = Some(fill),
            NodeFillsSide::B => pending_trade.bid = Some(fill)
        }

        if !pending_trade.is_complete() {
            return Ok(None);
        }

        let (pending_trade, mut pipeline_meta) = self
            .pending_fills
            .remove(&tid)
            .expect("pending trade exists");
        let trade = pending_trade.into_trade()?;
        tracing::debug!(?trade, "found new trade");

        pipeline_meta.processing_data_at_ns = processing_data_at_ns;
        pipeline_meta.processed_data_at_ns = unix_timestamp().as_nanos();

        Ok(Some((trade, pipeline_meta)))
    }
}

impl HyperliquidDataProcessorHandle for TradeDeriver {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Option<HyperliquidData>> {
        let processing_data_at_ns = unix_timestamp().as_nanos();

        let fills = match &data.data {
            HyperliquidDirData::NodeFills(items) => items.clone(),
            _ => return Ok(None)
        };

        let mut trades = Vec::new();
        for fill in fills.iter().flat_map(|fill| fill.events.clone()) {
            if let Some((trade, pipeline_meta)) =
                self.new_fill(fill, data.pipeline_meta.clone(), processing_data_at_ns)?
            {
                trades.push(HyperliquidDataWithMeta { data: trade, pipeline_meta });
            }
        }

        if trades.is_empty() { Ok(None) } else { Ok(Some(HyperliquidData::Trades(trades))) }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::TradeDeriver;
    use crate::{
        fs_handlers::types::FsOutData,
        hl_fs::{
            HyperliquidDirData, HyperliquidDirDataWithMeta, HyperliquidDirKind,
            schemas::{NodeFillsFill, NodeFillsRow, NodeFillsSide}
        },
        processors::HyperliquidDataProcessorHandle,
        types::{PendingTrade, TradeSide}
    };

    #[test]
    fn derives_order_book_server_trade_shape() {
        let trade = PendingTrade { ask: Some(ask_fill(false)), bid: Some(bid_fill(true)) }
            .into_trade()
            .unwrap();

        assert_eq!(
            serde_json::to_string(&trade).unwrap(),
            r#"{"coin":"BTC","side":"B","px":"106296.0","sz":"0.00017","hash":"0xhash","time":1751430933565,"tid":293353986402527,"users":["0xbuyer","0xseller"]}"#
        );
    }

    #[test]
    fn matches_fills_across_updates() {
        let mut deriver = TradeDeriver::new();

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeFills(vec![row(vec![ask_fill(false)])]),
                pipeline_meta: Arc::new(FsOutData {
                    name: HyperliquidDirKind::NodeFills,
                    bytes: vec![],
                    path: String::new(),
                    chunk_len: 0,
                    notification_received_at_ns: 0,
                    pipeline: Default::default()
                })
            })
            .unwrap();
        assert_eq!(deriver.pending_fill_count(), 1);

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeFills(vec![row(vec![bid_fill(true)])]),
                pipeline_meta: Arc::new(FsOutData {
                    name: HyperliquidDirKind::NodeFills,
                    bytes: vec![],
                    path: String::new(),
                    chunk_len: 0,
                    notification_received_at_ns: 0,
                    pipeline: Default::default()
                })
            })
            .unwrap();
        assert_eq!(deriver.pending_fill_count(), 0);
    }

    #[test]
    fn ask_taker_produces_ask_side_trade() {
        let trade = PendingTrade { ask: Some(ask_fill(true)), bid: Some(bid_fill(false)) }
            .into_trade()
            .unwrap();

        assert_eq!(trade.side, TradeSide::Ask);
    }

    fn row(events: Vec<NodeFillsFill>) -> NodeFillsRow {
        NodeFillsRow {
            local_time: "2025-06-24T02:56:36.172847427".to_string(),
            block_time: "2025-06-24T02:56:36.172847427".to_string(),
            block_number: 1,
            events
        }
    }

    fn ask_fill(crossed: bool) -> NodeFillsFill {
        fill("0xseller", NodeFillsSide::A, crossed)
    }

    fn bid_fill(crossed: bool) -> NodeFillsFill {
        fill("0xbuyer", NodeFillsSide::B, crossed)
    }

    fn fill(user: &str, side: NodeFillsSide, crossed: bool) -> NodeFillsFill {
        NodeFillsFill {
            user: user.to_string(),
            coin: "BTC".to_string(),
            px: 106296.0,
            sz: 0.00017,
            side,
            time: 1751430933565,
            start_position: 0.0,
            dir: "Open Long".to_string(),
            closed_pnl: 0.0,
            hash: "0xhash".to_string(),
            oid: 1,
            crossed,
            fee: 0.0,
            builder_fee: None,
            tid: 293353986402527,
            cloid: None,
            fee_token: "USDC".to_string(),
            builder: None,
            twap_id: None,
            deployer_fee: None
        }
    }
}
