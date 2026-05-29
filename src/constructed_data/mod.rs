mod trades;
use eyre::Context;
use serde::de::DeserializeOwned;
pub use trades::*;

use crate::fs_handlers::types::FsOutData;

pub mod types;

pub trait HyperliquidDataDeriver {
    type RawType;
    type ParsedType;

    fn handle_raw_data(&mut self, data: FsOutData) -> eyre::Result<Vec<Self::ParsedType>> {
        let path = data.path;
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&data.bytes);

        let mut out_data = Vec::new();
        let mut line_start = 0;
        let mut consumed_len = 0;
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
                let parsed_value = Self::parse_raw_type(line).wrap_err_with(|| {
                    format!("failed to parse node_fills_streaming row from {path}")
                })?;
                // println!("{value:?}");
                let all_values = self.construct_data(parsed_value)?;
                out_data.extend(all_values);
            }
            line_start = newline_idx + 1;
            consumed_len = line_start;
        }

        if consumed_len > 0 {
            buffer.drain(..consumed_len);
        }

        Ok(out_data)
    }

    fn parse_raw_type(data: &[u8]) -> eyre::Result<Self::RawType>;

    fn construct_data(&mut self, data: Self::RawType) -> eyre::Result<Vec<Self::ParsedType>>;
}
