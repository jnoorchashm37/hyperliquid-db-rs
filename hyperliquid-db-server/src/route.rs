use axum::extract::ws::Message;
use serde::Serialize;

#[derive(Serialize)]
pub struct RoutedMessage<D: Serialize> {
    pub channel: &'static str,
    pub data:    D
}

impl<D: Serialize> RoutedMessage<D> {
    pub fn serialize_message(&self) -> Message {
        let text = match serde_json::to_string(self) {
            Ok(text) => text,
            Err(error) => {
                tracing::error!(?error, "failed to serialize trades");
                return Message::Close(None);
            }
        };
        Message::Text(text.into())
    }
}

#[derive(Serialize)]
pub struct ErrorMessage {
    pub error: String
}

impl ErrorMessage {
    pub fn serialize_error_message(&self) -> Message {
        if let Ok(text) = serde_json::to_string(self) {
            return Message::Text(text.into());
        }
        Message::Close(None)
    }
}
