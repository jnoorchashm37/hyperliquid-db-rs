use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Hash)]
pub struct L2Book {
    pub coin:   String,
    pub levels: [Vec<L2BookLevel>; 2],
    pub time:   u64
}

impl L2Book {
    pub fn bids(&self) -> &[L2BookLevel] {
        &self.levels[0]
    }

    pub fn asks(&self) -> &[L2BookLevel] {
        &self.levels[1]
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Hash)]
pub struct L2BookLevel {
    pub px: String,
    pub sz: String,
    pub n:  u64
}
