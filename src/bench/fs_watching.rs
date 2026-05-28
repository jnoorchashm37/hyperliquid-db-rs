#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("fs_watching compares against the inotify-based current_impl reader.");
    eprintln!("Run this benchmark on Linux with: cargo bench --bench fs_watching");
}

mod readers;

#[cfg(target_os = "linux")]
fn main() {
    readers::run_fs_readers_bench();
}
