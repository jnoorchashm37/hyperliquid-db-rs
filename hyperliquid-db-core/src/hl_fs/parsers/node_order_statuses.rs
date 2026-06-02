use crate::hl_fs::{
    parsers::HyperliquidDataParser, schemas::NodeOrderStatusesRows, HyperliquidDirData
};

#[derive(Default)]
pub struct NodeOrderStatusesParser {
    line_buffer: Vec<u8>
}

impl NodeOrderStatusesParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataParser for NodeOrderStatusesParser {
    type ParsedType = NodeOrderStatusesRows;

    fn line_buffer(&mut self) -> &mut Vec<u8> {
        &mut self.line_buffer
    }

    fn parse_raw_type(data: &[u8]) -> eyre::Result<Self::ParsedType> {
        Ok(serde_json::from_slice::<Self::ParsedType>(data)?)
    }
}

impl From<Vec<NodeOrderStatusesRows>> for HyperliquidDirData {
    fn from(value: Vec<NodeOrderStatusesRows>) -> Self {
        Self::NodeOrderStatuses(value)
    }
}
