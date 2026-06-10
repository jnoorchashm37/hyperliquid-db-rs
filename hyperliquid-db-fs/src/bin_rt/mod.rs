use clap::Parser;
use hyperliquid_db_fs::run_hyperliquid_fs_cleaner;

use crate::bin_rt::cli::HyperliquidDataFsCli;

pub mod cli;
pub mod utils;

pub fn run() -> eyre::Result<()> {
    let config = HyperliquidDataFsCli::parse().into_config();

    run_hyperliquid_fs_cleaner(config)?;

    Ok(())
}
