use crate::hl_fs::{
    HyperliquidDirData, parsers::HyperliquidDataParser, schemas::Hip3OracleUpdatesRows
};

#[derive(Default)]
pub struct Hip3OracleUpdatesParser {
    line_buffer: Vec<u8>
}

impl Hip3OracleUpdatesParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataParser for Hip3OracleUpdatesParser {
    type ParsedType = Hip3OracleUpdatesRows;

    fn line_buffer(&mut self) -> &mut Vec<u8> {
        &mut self.line_buffer
    }

    fn parse_raw_type(data: &[u8]) -> eyre::Result<Self::ParsedType> {
        Ok(serde_json::from_slice::<Self::ParsedType>(data)?)
    }
}

impl From<Vec<Hip3OracleUpdatesRows>> for HyperliquidDirData {
    fn from(value: Vec<Hip3OracleUpdatesRows>) -> Self {
        Self::Hip3OracleUpdates(value)
    }
}
