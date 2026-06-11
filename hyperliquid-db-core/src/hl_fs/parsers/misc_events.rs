use crate::hl_fs::{HyperliquidDirData, parsers::HyperliquidDataParser, schemas::MiscEventsRows};

#[derive(Default)]
pub struct MiscEventsParser {
    line_buffer: Vec<u8>
}

impl MiscEventsParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataParser for MiscEventsParser {
    type ParsedType = MiscEventsRows;

    fn line_buffer(&mut self) -> &mut Vec<u8> {
        &mut self.line_buffer
    }

    fn parse_raw_type(data: &[u8]) -> eyre::Result<Self::ParsedType> {
        Ok(serde_json::from_slice::<Self::ParsedType>(data)?)
    }
}

impl From<Vec<MiscEventsRows>> for HyperliquidDirData {
    fn from(value: Vec<MiscEventsRows>) -> Self {
        Self::MiscEvents(value)
    }
}
