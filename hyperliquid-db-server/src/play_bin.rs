use hyperliquid_db_core::{HyperliquidDataManager, types::HyperliquidDataKind};

pub fn run_play_bin() -> eyre::Result<()> {
    let mut data_rx = HyperliquidDataManager::spawn(&[HyperliquidDataKind::L4Book])?;

    loop {
        match data_rx.blocking_recv()?.as_ref() {
            Ok(v) => println!("{v:?}"),
            Err(e) => return Err(eyre::eyre!("{e:?}"))
        }
    }
}
