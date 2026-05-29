use std::{
    io,
    net::TcpStream,
    sync::mpsc::{self, Receiver},
    time::Duration
};

use hyperliquid_db::{
    fs_handlers::{DirectoryWatcher, types::FsOutData},
    hl_fs::HyperliquidDataDirKind
};
use serde_json::json;
use tungstenite::{Message, WebSocket, connect, stream::MaybeTlsStream};

pub const HL_WEBSOCKET_ENDPOINT: &str = "wss://api.hyperliquid.xyz/ws";

pub type HlWebSocket = WebSocket<MaybeTlsStream<TcpStream>>;

pub fn spawn_hl_websocket() -> eyre::Result<HlWebSocket> {
    let (socket, _response) = connect(HL_WEBSOCKET_ENDPOINT)?;
    Ok(socket)
}

pub fn spawn_hl_trades_websocket(coin: &str) -> eyre::Result<HlWebSocket> {
    let mut socket = spawn_hl_websocket()?;
    let subscription = json!({
        "method": "subscribe",
        "subscription": {
            "type": "trades",
            "coin": coin
        }
    });
    socket.send(Message::Text(subscription.to_string().into()))?;

    Ok(socket)
}

pub fn set_hl_websocket_read_timeout(
    socket: &mut HlWebSocket,
    timeout: Option<Duration>
) -> io::Result<()> {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream.set_read_timeout(timeout),
        MaybeTlsStream::NativeTls(stream) => stream.get_ref().set_read_timeout(timeout),
        _ => Ok(())
    }
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
