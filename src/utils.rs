use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const NS_PER_MS: u128 = 1_000_000;
pub const NS_PER_SEC: u128 = 1_000_000_000;

pub fn unix_timestamp() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
}
