use std::{io, net::TcpStream, path::Path, sync::mpsc::Receiver, time::Duration};

use hyperliquid_db_core::{
    HYPERLIQUID_DATA_DIR, fs_handlers::types::FsOutData, hl_fs::HyperliquidDirKind
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
    crate::raw_fs_reader::spawn_file_reader(
        HyperliquidDirKind::NodeFills,
        Path::new(HYPERLIQUID_DATA_DIR)
    )
}
