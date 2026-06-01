// #[cfg(target_os = "linux")]
pub mod directory_watcher;
// #[cfg(target_os = "linux")]
pub use directory_watcher::DirectoryWatcher;

mod file;
// #[cfg(not(target_os = "linux"))]
// mod stub;
// #[cfg(not(target_os = "linux"))]
// pub use stub::DirectoryWatcher;
pub mod types;
