use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr
};

use crate::HYPERLIQUID_DATA_DIR;

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub enum HyperliquidDataDirKind {
    ReplicaCmds,
    NodeSlowBlockTimes,
    NodeFillsStreaming
}

impl HyperliquidDataDirKind {
    pub fn dir_path(&self) -> PathBuf {
        Path::new(HYPERLIQUID_DATA_DIR).join(self.to_string())
    }
}

impl FromStr for HyperliquidDataDirKind {
    type Err = eyre::ErrReport;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "replica_cmds" => Ok(Self::ReplicaCmds),
            "node_slow_block_times" => Ok(Self::NodeSlowBlockTimes),
            "node_fills_streaming" => Ok(Self::NodeFillsStreaming),
            _ => Err(eyre::eyre!("invalid `HyperliquidDataDirKind`: {s}"))
        }
    }
}

impl fmt::Display for HyperliquidDataDirKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            HyperliquidDataDirKind::ReplicaCmds => "replica_cmds",
            HyperliquidDataDirKind::NodeSlowBlockTimes => "node_slow_block_times",
            HyperliquidDataDirKind::NodeFillsStreaming => "node_fills_streaming"
        };

        fmt::Display::fmt(s, f)
    }
}

impl fmt::Debug for HyperliquidDataDirKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
