use eyre::Context;

mod node_fills;
pub use node_fills::NodeFillsParser;

use crate::{fs_handlers::types::FsOutData, hl_fs::HyperliquidDirData};

pub trait HyperliquidDataParser: Default
where
    HyperliquidDirData: From<Vec<Self::ParsedType>>
{
    type ParsedType;

    fn handle_raw_data(&mut self, data: FsOutData) -> eyre::Result<HyperliquidDirData> {
        let path = data.path;
        let mut buffer = std::mem::take(self.line_buffer());
        buffer.extend_from_slice(&data.bytes);

        let mut out_data = Vec::new();
        let mut line_start = 0;
        let mut consumed_len = 0;
        let mut result = Ok(());
        for newline_idx in buffer
            .iter()
            .enumerate()
            .filter_map(|(idx, byte)| (*byte == b'\n').then_some(idx))
        {
            let line = &buffer[line_start..newline_idx];
            if !line
                .strip_suffix(b"\r")
                .unwrap_or(line)
                .iter()
                .all(|byte| byte.is_ascii_whitespace())
            {
                let parsed_value = match Self::parse_raw_type(line).wrap_err_with(|| {
                    format!("failed to parse node_fills_streaming row from {path}")
                }) {
                    Ok(parsed_value) => parsed_value,
                    Err(err) => {
                        result = Err(err);
                        break;
                    }
                };

                out_data.push(parsed_value);
            }
            line_start = newline_idx + 1;
            consumed_len = line_start;
        }

        if consumed_len > 0 {
            buffer.drain(..consumed_len);
        }

        *self.line_buffer() = buffer;
        result.map(|_| out_data.into())
    }

    fn line_buffer(&mut self) -> &mut Vec<u8>;

    fn parse_raw_type(data: &[u8]) -> eyre::Result<Self::ParsedType>;
}
