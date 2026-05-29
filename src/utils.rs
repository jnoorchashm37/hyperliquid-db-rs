use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub const NS_PER_MS: u128 = 1_000_000;
pub const NS_PER_SEC: u128 = 1_000_000_000;

pub fn unix_timestamp() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
}

pub fn init_logging(level: Level) {
    let format = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_target(true);

    let filter = tracing_subscriber::filter::Targets::new().with_target("hyperliquid_db", level);

    let _ = tracing_subscriber::registry()
        .with(format)
        .with(filter)
        .try_init()
        .ok();
}
