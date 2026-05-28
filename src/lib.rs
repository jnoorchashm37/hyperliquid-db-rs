use std::{collections::HashMap, sync::mpsc};

use eyre::WrapErr;

use crate::{
    fs_handlers::{directory_watcher::DirectoryWatcher, types::FsOutData},
    hl_fs::{HyperliquidDataDirKind, schemas::NodeFillsRow}
};

pub mod fs_handlers;
pub mod hl_fs;

pub const HYPERLIQUID_DATA_DIR: &str = "/var/lib/hyperliquid/hl/data";

pub fn run_stream() -> eyre::Result<()> {
    let (out_tx, out_rx) = mpsc::channel();
    let mut parse_buffers = HashMap::new();

    println!("initializing watcher");
    let watcher = DirectoryWatcher::new(HyperliquidDataDirKind::NodeFills, out_tx)?;
    println!("initialized watcher");
    watcher.run();

    loop {
        handle_incoming(out_rx.recv()??, &mut parse_buffers)?;
    }
}

fn handle_incoming(
    data: FsOutData,
    parse_buffers: &mut HashMap<String, Vec<u8>>
) -> eyre::Result<()> {
    match data.name {
        HyperliquidDataDirKind::NodeFills => {
            handle_node_fills_streaming(data, parse_buffers)?;
        }
        _ => unreachable!()
    };

    Ok(())
}

fn handle_node_fills_streaming(
    data: FsOutData,
    parse_buffers: &mut HashMap<String, Vec<u8>>
) -> eyre::Result<()> {
    let path = data.path;
    let buffer = parse_buffers.entry(path.clone()).or_default();
    buffer.extend_from_slice(&data.bytes);

    let mut line_start = 0;
    let mut consumed_len = 0;
    for newline_idx in buffer
        .iter()
        .enumerate()
        .filter_map(|(idx, byte)| (*byte == b'\n').then_some(idx))
    {
        parse_node_fills_streaming_row(&path, &buffer[line_start..newline_idx])?;
        line_start = newline_idx + 1;
        consumed_len = line_start;
    }

    if consumed_len > 0 {
        buffer.drain(..consumed_len);
    }

    Ok(())
}

fn parse_node_fills_streaming_row(path: &str, line: &[u8]) -> eyre::Result<()> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(());
    }

    let value = serde_json::from_slice::<NodeFillsRow>(line)
        .wrap_err_with(|| format!("failed to parse node_fills_streaming row from {path}"))?;
    println!("{value:?}");

    Ok(())
}
