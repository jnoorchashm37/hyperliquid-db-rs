use std::{fmt, net::SocketAddr, path::PathBuf};

use clap::{Parser, ValueEnum};
use hyperliquid_db_core::types::HyperliquidDataKind;
use hyperliquid_db_fs::HyperliquidDataFsConfig;
use tracing::Level;

const DEFAULT_DATA_DIR: &str = "/root/hl/data";
const DEFAULT_MAX_AGE_HOURS: u64 = 24;
const DEFAULT_FS_CLEAN_INTERVAL_HRS: u64 = 12;

const DEFAULT_RPC_ADDR: &str = "127.0.0.1:3737";

#[derive(Debug, Parser)]
#[command(name = "hyperliquid-db-server", version)]
pub struct HyperliquidDataRpcCli {
    #[arg(default_value = DEFAULT_RPC_ADDR)]
    pub rpc_addr: SocketAddr,

    #[arg(long = "data-kind", value_delimiter = ',')]
    pub data_kinds: Vec<CliDataKind>,

    #[arg(default_value_t = true)]
    pub clean_fs: bool,

    #[arg(short, default_value = DEFAULT_DATA_DIR, requires = "clean_fs")]
    pub data_dir: PathBuf,

    #[arg(long, default_value_t = DEFAULT_MAX_AGE_HOURS, requires = "clean_fs", value_parser = clap::value_parser!(u64).range(DEFAULT_MAX_AGE_HOURS..))]
    pub max_age_hours: u64,

    #[arg(long, requires = "clean_fs")]
    pub min_size_mb: Option<u64>,

    #[arg(long = "fs-clean-interval-hours", default_value_t = DEFAULT_FS_CLEAN_INTERVAL_HRS, requires = "clean_fs")]
    pub fs_clean_interval_hours: u64,

    #[arg(long, value_enum, default_value_t = LogLevel::Debug)]
    pub log_level: LogLevel
}

impl HyperliquidDataRpcCli {
    pub fn fs_cleaner_config(&self) -> HyperliquidDataFsConfig {
        HyperliquidDataFsConfig::new(self.data_dir.clone(), self.max_age_hours, self.min_size_mb)
            .with_loop(self.fs_clean_interval_hours, true)
    }

    pub fn hyperliquid_data_kinds(&self) -> Vec<HyperliquidDataKind> {
        if self.data_kinds.is_empty() {
            return HyperliquidDataKind::all();
        }

        self.data_kinds
            .iter()
            .copied()
            .map(HyperliquidDataKind::from)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliDataKind {
    Trades,
    L4Book,
    L2Book,
    Hip3OracleUpdates,
    MiscEvents
}

impl From<CliDataKind> for HyperliquidDataKind {
    fn from(value: CliDataKind) -> Self {
        match value {
            CliDataKind::Trades => Self::Trades,
            CliDataKind::L4Book => Self::L4Book,
            CliDataKind::L2Book => Self::L2Book,
            CliDataKind::Hip3OracleUpdates => Self::Hip3OracleUpdates,
            CliDataKind::MiscEvents => Self::MiscEvents
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace
}

impl From<LogLevel> for Level {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Error => Self::ERROR,
            LogLevel::Warn => Self::WARN,
            LogLevel::Info => Self::INFO,
            LogLevel::Debug => Self::DEBUG,
            LogLevel::Trace => Self::TRACE
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace"
        };

        f.write_str(value)
    }
}
