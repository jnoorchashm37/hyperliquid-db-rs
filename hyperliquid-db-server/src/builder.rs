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
        let data_tx = HyperliquidDataManager::spawn(&self.data_kinds)?;

        let mut app = Router::new();

        for data_kind in self.data_kinds {
            let name = channel_name(data_kind);
            let ws_handler = WsHandler::new(name, data_kind, data_tx.clone());

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
        HyperliquidDataKind::Trades => "trades",
        HyperliquidDataKind::L4Book => "l4Book",
        HyperliquidDataKind::L2Book => "l2Book",
        HyperliquidDataKind::Hip3OracleUpdates => "hip3OracleUpdates",
        HyperliquidDataKind::MiscEvents => "miscEvents"
    }
}

fn route_name(kind: HyperliquidDataKind) -> &'static str {
    match kind {
        HyperliquidDataKind::Trades => "/subscribeTrades",
        HyperliquidDataKind::L4Book => "/subscribeL4Book",
        HyperliquidDataKind::L2Book => "/subscribeL2Book",
        HyperliquidDataKind::Hip3OracleUpdates => "/subscribeHip3OracleUpdates",
        HyperliquidDataKind::MiscEvents => "/subscribeMiscEvents"
    }
}
