#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("fs_watching compares against the inotify-based current_impl reader.");
    eprintln!("Run this benchmark on Linux with: cargo bench --bench fs_watching");
}

#[cfg(target_os = "linux")]
mod fs_watching;
#[cfg(target_os = "linux")]
mod raw_fs_reader;

#[cfg(target_os = "linux")]
fn main() {
    fs_watching::run_fs_readers_bench();
}
