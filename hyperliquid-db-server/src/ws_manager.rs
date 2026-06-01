use std::sync::Arc;

use axum::extract::ws::WebSocket;
use hyperliquid_db_core::types::{HyperliquidData, HyperliquidDataKind};
use tokio::sync::broadcast::Sender;

use crate::{
    HyperliquidDataClient,
    route::{ErrorMessage, RoutedMessage}
};

#[derive(Clone)]
pub struct WsHandler {
    channel:   &'static str,
    data_kind: HyperliquidDataKind,
    data_tx:   Sender<Arc<eyre::Result<HyperliquidData>>>
}

impl WsHandler {
    pub fn new(
        channel: &'static str,
        data_kind: HyperliquidDataKind,
        data_tx: Sender<Arc<eyre::Result<HyperliquidData>>>
    ) -> Self {
        Self { channel, data_kind, data_tx }
    }

    pub async fn run(mut self, mut websocket: WebSocket) {
        let mut client = HyperliquidDataClient::new(self.data_kind, self.data_tx.subscribe());
        loop {
            match client.recv().await {
                Ok(Some(data)) => {
                    if self.send_data(&mut websocket, data).await.is_err() {
                        return;
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    self.send_error(&mut websocket, error).await;
                    return;
                }
            }
        }
    }

    async fn send_data(
        &mut self,
        websocket: &mut WebSocket,
        data: HyperliquidData
    ) -> Result<(), axum::Error> {
        let message = RoutedMessage { channel: self.channel, data };

        websocket.send(message.serialize_message()).await
    }

    async fn send_error(&mut self, websocket: &mut WebSocket, error: eyre::ErrReport) {
        tracing::error!(?error, "subscribeTrades websocket error");

        let message = ErrorMessage { error: error.to_string() };

        let _ = websocket.send(message.serialize_error_message()).await;
    }
}
