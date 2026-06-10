#[cfg(not(feature = "example"))]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> eyre::Result<()> {
    hyperliquid_db_server::run_server().await
}

#[cfg(feature = "example")]
fn main() -> eyre::Result<()> {
    hyperliquid_db_server::play_bin::run_play_bin()
}
