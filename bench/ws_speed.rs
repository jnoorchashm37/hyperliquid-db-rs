// #[cfg(not(target_os = "linux"))]
// fn main() {
//     eprintln!("ws_speed compares against the inotify-based current_impl
// reader.");     eprintln!("Run this benchmark on Linux with: cargo bench
// --bench ws_speed"); }

// #[cfg(target_os = "linux")]
mod ws_streams;

// #[cfg(target_os = "linux")]
fn main() {
    ws_streams::run_trades_ws_bench();
}
