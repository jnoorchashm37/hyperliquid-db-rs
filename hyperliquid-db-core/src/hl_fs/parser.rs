use std::sync::Arc;

use eyre::Context;
use serde::de::DeserializeOwned;

use crate::{
    fs_handlers::types::FsOutData,
    hl_fs::{HyperliquidDirData, HyperliquidDirDataWithMeta}
};

#[derive(Default)]
pub struct HyperliquidDataParser {
    line_buffer: Vec<u8>
}

impl HyperliquidDataParser {
    pub fn new() -> Self {
        Self::default()
    }

    fn line_buffer(&mut self) -> &mut Vec<u8> {
        &mut self.line_buffer
    }

    fn parse_raw_type<T: DeserializeOwned>(data: &[u8]) -> eyre::Result<T> {
        Ok(serde_json::from_slice::<T>(data)?)
    }

    pub fn handle_raw_data<T>(
        &mut self,
        data: FsOutData
    ) -> eyre::Result<HyperliquidDirDataWithMeta>
    where
        T: DeserializeOwned,
        HyperliquidDirData: From<Vec<T>>
    {
        let arced_data = Arc::new(data.clone());
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
                let parsed_value = match Self::parse_raw_type::<T>(line).wrap_err_with(|| {
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
        result.map(|_| HyperliquidDirDataWithMeta {
            data:          out_data.into(),
            pipeline_meta: arced_data.clone()
        })
    }
}
