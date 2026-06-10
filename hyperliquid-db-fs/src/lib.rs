mod config;
pub use config::HyperliquidDataFsConfig;
pub(crate) mod fs;

use crate::fs::clean_hyperliquid_fs_data;

/// if `loop_sleep_interval_hrs` is None, no loop
pub fn run_hyperliquid_fs_cleaner(config: HyperliquidDataFsConfig) -> eyre::Result<()> {
    tracing::info!(
        data_dir=?config.hl_data_dir.display(),
        max_age_hours=config.max_age_hours,
        min_size_mb=config.min_size_mb,
        loop_config=?config.loop_config,
        "running hyperliquid filesystem cleaner"
    );

    if let Some(loop_cfg) = config.loop_config {
        loop {
            if let Err(error) = clean_hyperliquid_fs_data(
                &config.hl_data_dir,
                config.max_age_hours,
                config.min_size_mb
            ) {
                tracing::error!(?error, "error cleaning filesystem - retrying in 1 minute");
                if !loop_cfg.infallible_loop {
                    return Err(error);
                }
                std::thread::sleep(std::time::Duration::from_mins(1));
            } else {
                std::thread::sleep(std::time::Duration::from_hours(
                    loop_cfg.loop_sleep_interval_hrs
                ));
            }
        }
    } else {
        clean_hyperliquid_fs_data(&config.hl_data_dir, config.max_age_hours, config.min_size_mb)
            .inspect_err(|error| tracing::error!(?error, "error cleaning filesystem"))?;
    }

    Ok(())
}
