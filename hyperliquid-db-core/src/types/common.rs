use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "A")]
    Ask,
    #[serde(rename = "B")]
    Bid
}
