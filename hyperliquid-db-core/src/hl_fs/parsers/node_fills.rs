use crate::hl_fs::{HyperliquidDirData, parsers::HyperliquidDataParser, schemas::NodeFillsRow};

#[derive(Default)]
pub struct NodeFillsParser {
    line_buffer: Vec<u8>
}

impl NodeFillsParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataParser for NodeFillsParser {
    type ParsedType = NodeFillsRow;

    fn line_buffer(&mut self) -> &mut Vec<u8> {
        &mut self.line_buffer
    }

    fn parse_raw_type(data: &[u8]) -> eyre::Result<Self::ParsedType> {
        Ok(serde_json::from_slice::<Self::ParsedType>(data)?)
    }
}

impl From<Vec<NodeFillsRow>> for HyperliquidDirData {
    fn from(value: Vec<NodeFillsRow>) -> Self {
        Self::NodeFills(value)
    }
}

#[cfg(test)]
mod tests {

    use super::NodeFillsParser;
    use crate::{
        fs_handlers::types::FsOutData,
        hl_fs::{HyperliquidDirData, HyperliquidDirKind, parsers::HyperliquidDataParser},
        utils::unix_timestamp
    };

    #[test]
    fn buffers_partial_lines_across_chunks() {
        let mut deriver = NodeFillsParser::new();
        let row = concat!(
            r#"{"local_time":"2025-06-24T02:56:36.172847427","block_time":"2025-06-24T02:56:36.172847427","block_number":1,"events":["#,
            r#"["0xseller",{"coin":"BTC","px":"106296.0","sz":"0.00017","side":"A","time":1751430933565,"startPosition":"0.0","dir":"Open Short","closedPnl":"0.0","hash":"0xhash","oid":1,"crossed":true,"fee":"0.0","builderFee":null,"tid":293353986402527,"cloid":null,"feeToken":"USDC","builder":null,"twapId":null,"deployerFee":null}],"#,
            r#"["0xbuyer",{"coin":"BTC","px":"106296.0","sz":"0.00017","side":"B","time":1751430933565,"startPosition":"0.0","dir":"Open Long","closedPnl":"0.0","hash":"0xhash","oid":2,"crossed":false,"fee":"0.0","builderFee":null,"tid":293353986402527,"cloid":null,"feeToken":"USDC","builder":null,"twapId":null,"deployerFee":null}]"#,
            r#"]}"#,
            "\n"
        );
        let split_idx = row.len() / 2;

        let first_chunk = deriver
            .handle_raw_data(fs_data(row.as_bytes()[..split_idx].to_vec()))
            .unwrap();

        let first_chunk = match first_chunk.data {
            HyperliquidDirData::NodeFills(node_fills_rows) => node_fills_rows,
            _ => unreachable!()
        };
        assert!(first_chunk.is_empty());

        let second_chunk = deriver
            .handle_raw_data(fs_data(row.as_bytes()[split_idx..].to_vec()))
            .unwrap();
        let second_chunk = match second_chunk.data {
            HyperliquidDirData::NodeFills(node_fills_rows) => node_fills_rows,
            _ => unreachable!()
        };
        assert_eq!(second_chunk.len(), 1);
        assert_eq!(second_chunk[0].events[0].tid, 293353986402527);
        assert_eq!(deriver.line_buffer.len(), 0);
    }

    fn fs_data(bytes: Vec<u8>) -> FsOutData {
        let chunk_len = bytes.len();
        FsOutData {
            name: HyperliquidDirKind::NodeFills,
            bytes,
            path: "test-node-fills".to_string(),
            chunk_len,
            notification_received_at_ns: unix_timestamp().as_nanos(),
            pipeline: Default::default()
        }
    }
}
