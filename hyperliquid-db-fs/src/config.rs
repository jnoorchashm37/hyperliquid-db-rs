use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct HyperliquidDataFsConfig {
    pub hl_data_dir:   PathBuf,
    pub max_age_hours: u64,
    pub min_size_mb:   Option<u64>,
    pub loop_config:   Option<HyperliquidDataLoopConfig>
}

impl HyperliquidDataFsConfig {
    pub fn new(hl_data_dir: PathBuf, max_age_hours: u64, min_size_mb: Option<u64>) -> Self {
        Self { hl_data_dir, max_age_hours, min_size_mb, loop_config: None }
    }

    pub fn with_loop(mut self, loop_sleep_interval_hrs: u64, infallible_loop: bool) -> Self {
        self.loop_config =
            Some(HyperliquidDataLoopConfig { loop_sleep_interval_hrs, infallible_loop });
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HyperliquidDataLoopConfig {
    pub loop_sleep_interval_hrs: u64,
    pub infallible_loop:         bool
}
