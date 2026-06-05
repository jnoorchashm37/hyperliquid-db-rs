use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Hash)]
pub struct RpcTrade {
    pub coin:  String,
    pub side:  String,
    pub px:    String,
    pub sz:    String,
    pub hash:  String,
    pub time:  u64,
    pub tid:   u64,
    pub users: [String; 2]
}
