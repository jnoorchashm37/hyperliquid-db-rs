use std::{fmt, path::PathBuf};

use clap::{Parser, ValueEnum};
use hyperliquid_db_fs::HyperliquidDataFsConfig;
use tracing::Level;

const DEFAULT_DATA_DIR: &str = "/root/hl/data";
const DEFAULT_MAX_AGE_HOURS: usize = 24;
const DEFAULT_MIN_SIZE_MB: u64 = 1000;

#[derive(Debug, Parser)]
#[command(name = "hyperliquid-db-fs")]
pub struct HyperliquidDataFsCli {
    #[arg(short, default_value = DEFAULT_DATA_DIR)]
    pub data_dir: PathBuf,

    /// only considers files last touched before `now - max_age_hours`
    #[arg(short = 'a', long, default_value_t = DEFAULT_MAX_AGE_HOURS)]
    pub max_age_hours: usize,

    /// only considers files above this size
    #[arg(short = 's', long, default_value_t = DEFAULT_MIN_SIZE_MB)]
    pub min_size_mb: u64,

    #[arg(short = 'l', long)]
    pub loop_sleep_interval_hours: Option<u64>,

    #[arg(long, requires = "loop_sleep_interval_hours", default_value_t = false)]
    pub infallible_loop: bool,

    #[arg(long, value_enum, default_value_t = LogLevel::Debug)]
    pub log_level: LogLevel
}

impl HyperliquidDataFsCli {
    pub fn into_config(self) -> HyperliquidDataFsConfig {
        super::utils::init_logging(self.log_level.into());
        let config =
            HyperliquidDataFsConfig::new(self.data_dir, self.max_age_hours, self.min_size_mb);

        if let Some(loop_sleep_interval_hours) = self.loop_sleep_interval_hours {
            config.with_loop(loop_sleep_interval_hours, self.infallible_loop)
        } else {
            config
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
