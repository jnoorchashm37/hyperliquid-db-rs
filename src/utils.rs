use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing_subscriber::EnvFilter;

pub const NS_PER_MS: u128 = 1_000_000;
pub const NS_PER_SEC: u128 = 1_000_000_000;

pub fn unix_timestamp() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
}

/// Installs a compact tracing subscriber for command-line progress logs.
pub fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("hyperliquid_db=debug"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .without_time()
        .with_level(false)
        .with_target(true)
        .with_writer(std::io::stderr)
        .init();
}
