use axum::{Router, extract::WebSocketUpgrade, response::IntoResponse, routing::get};
use hyperliquid_db_core::{HyperliquidDataManager, types::HyperliquidDataKind};

use crate::WsHandler;

pub struct HyperliquidWebsocketBuilder {
    data_kinds: Vec<HyperliquidDataKind>
}

impl HyperliquidWebsocketBuilder {
    pub fn new(data_kinds: Vec<HyperliquidDataKind>) -> Self {
        Self { data_kinds }
    }

    pub fn build(self) -> eyre::Result<Router> {
        let data_rx = HyperliquidDataManager::spawn(&self.data_kinds)?;

        let mut app = Router::new();

        for data_kind in self.data_kinds {
            let name = channel_name(data_kind);
            let ws_handler = WsHandler::new(name, data_kind, data_rx.clone());

            app = app.route(
                route_name(data_kind),
                get(|ws_upgrade| subscribe_route(ws_upgrade, ws_handler))
            );
        }

        Ok(app)
    }
}

async fn subscribe_route(ws: WebSocketUpgrade, handler: WsHandler) -> impl IntoResponse {
    ws.on_upgrade(|socket| handler.run(socket))
}

fn channel_name(kind: HyperliquidDataKind) -> &'static str {
    match kind {
        HyperliquidDataKind::Trades => "trades"
    }
}

fn route_name(kind: HyperliquidDataKind) -> &'static str {
    match kind {
        HyperliquidDataKind::Trades => "/subscribeTrades"
    }
}
