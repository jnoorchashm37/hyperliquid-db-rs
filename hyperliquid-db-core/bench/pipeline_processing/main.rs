#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("ws_speed compares against the inotify-based current_impl reader.");
    eprintln!("Run this benchmark on Linux with: cargo bench --bench ws_speed");
}

// #[cfg(target_os = "linux")]
mod trades;
// #[cfg(target_os = "linux")]
mod utils;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(err) = trades::run_trades_ws_bench() {
        eprintln!("{err:?}");
        std::process::exit(1);
    }
}
