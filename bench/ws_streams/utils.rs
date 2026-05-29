use std::{
    net::TcpStream,
    sync::mpsc::{self, Receiver},
    time::Duration
};

use hyperliquid_db::{
    fs_handlers::{directory_watcher::DirectoryWatcher, types::FsOutData},
    hl_fs::HyperliquidDataDirKind
};
use tungstenite::{WebSocket, connect, stream::MaybeTlsStream};

pub const HL_WEBSOCKET_ENDPOINT: &str = "wss://api.hyperliquid.xyz/ws";

pub type HlWebSocket = WebSocket<MaybeTlsStream<TcpStream>>;

pub fn spawn_hl_websocket(endpoint: &str) -> eyre::Result<HlWebSocket> {
    let url = format!("{HL_WEBSOCKET_ENDPOINT}/{endpoint}");
    let (socket, _response) = connect(url)?;
    Ok(socket)
}

pub fn spawn_hl_watcher() -> eyre::Result<Receiver<eyre::Result<FsOutData>>> {
    let (out_tx, out_rx) = mpsc::channel();
    let watcher = DirectoryWatcher::new(HyperliquidDataDirKind::NodeFills, out_tx)?;
    watcher.run();
    Ok(out_rx)
}

pub fn timestamp_utc() -> Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
}
