use crate::hl_fs::{
    parsers::HyperliquidDataParser, schemas::NodeRawBookDiffsRows, HyperliquidDirData
};

#[derive(Default)]
pub struct NodeRawBookDiffsParser {
    line_buffer: Vec<u8>
}

impl NodeRawBookDiffsParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataParser for NodeRawBookDiffsParser {
    type ParsedType = NodeRawBookDiffsRows;

    fn line_buffer(&mut self) -> &mut Vec<u8> {
        &mut self.line_buffer
    }

    fn parse_raw_type(data: &[u8]) -> eyre::Result<Self::ParsedType> {
        Ok(serde_json::from_slice::<Self::ParsedType>(data)?)
    }
}

impl From<Vec<NodeRawBookDiffsRows>> for HyperliquidDirData {
    fn from(value: Vec<NodeRawBookDiffsRows>) -> Self {
        Self::NodeRawBookDiffs(value)
    }
}
