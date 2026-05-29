use std::{
    collections::HashMap,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    sync::mpsc
};

use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};

use crate::{
    fs_handlers::types::{
        ActiveDirectory, FileTailState, FsOutData, FsPipelineTimestamps, unix_timestamp_ns
    },
    hl_fs::HyperliquidDataDirKind
};

const NS_PER_MS: u128 = 1_000_000;

pub struct DirectoryWatcher {
    directory:  ActiveDirectory,
    notifier:   Inotify,
    watch_dirs: HashMap<WatchDescriptor, PathBuf>,
    out_tx:     mpsc::Sender<eyre::Result<FsOutData>>
}

impl DirectoryWatcher {
    pub fn new(
        name: HyperliquidDataDirKind,
        out_tx: mpsc::Sender<eyre::Result<FsOutData>>
    ) -> eyre::Result<Self> {
        let directory = ActiveDirectory::new(name)?;
        let notifier = Inotify::init()?;
        let mut watcher = Self { directory, notifier, watch_dirs: HashMap::new(), out_tx };
        watcher.add_directory_watches()?;

        Ok(watcher)
    }

    pub fn run(mut self) {
        std::thread::spawn(move || {
            if let Err(error) = self.run_safe() {
                eprintln!("error running filesystem watcher: {error:?}");
                self.out_tx.send(Err(error)).unwrap();
            }
        });
    }

    fn run_safe(&mut self) -> eyre::Result<()> {
        let mut event_buf = [0_u8; 16 * 1024];

        loop {
            let events = self.notifier.read_events_blocking(&mut event_buf)?;
            let notification_batch_received_at_ns = unix_timestamp_ns();
            for event in events {
                if event.mask.contains(EventMask::Q_OVERFLOW) {
                    // Production code: full rescan here.
                    return Err(eyre::eyre!(
                        "inotify queue overflow; rescan directory and reconcile offsets"
                    ));
                }
                if event.mask.contains(EventMask::IGNORED) {
                    self.watch_dirs.remove(&event.wd);
                    continue;
                }

                let Some(path) = self.event_path(&event.wd, event.name) else {
                    continue;
                };

                if event.mask.contains(EventMask::ISDIR) {
                    if event.mask.contains(EventMask::CREATE)
                        || event.mask.contains(EventMask::MOVED_TO)
                    {
                        self.add_directory_watches_recursive(&path)?;
                        self.drain_new_files_recursive(&path, notification_batch_received_at_ns)?;
                    }
                    continue;
                }

                self.drain_file(&path, notification_batch_received_at_ns)?;
            }
        }
    }

    fn event_path(&self, wd: &WatchDescriptor, name: Option<&OsStr>) -> Option<PathBuf> {
        let dir_path = self.watch_dirs.get(wd)?;
        Some(match name {
            Some(name) => dir_path.join(name),
            None => dir_path.clone()
        })
    }

    fn add_directory_watches(&mut self) -> eyre::Result<()> {
        let dir_path = self.directory.dir_path.clone();
        self.add_directory_watches_recursive(&dir_path)
    }

    fn add_directory_watches_recursive(&mut self, dir_path: &Path) -> eyre::Result<()> {
        if !dir_path.is_dir() {
            return Ok(());
        }

        self.watch_directory(dir_path)?;

        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                self.add_directory_watches_recursive(&entry.path())?;
            }
        }

        Ok(())
    }

    fn watch_directory(&mut self, dir_path: &Path) -> eyre::Result<()> {
        let wd = self.notifier.watches().add(dir_path, Self::watch_mask())?;
        self.watch_dirs.insert(wd, dir_path.to_path_buf());

        Ok(())
    }

    fn watch_mask() -> WatchMask {
        WatchMask::CREATE
            | WatchMask::MODIFY
            | WatchMask::CLOSE_WRITE
            | WatchMask::MOVED_TO
            | WatchMask::DELETE_SELF
            | WatchMask::MOVE_SELF
    }

    fn drain_new_files_recursive(
        &mut self,
        dir_path: &Path,
        notification_batch_received_at_ns: u128
    ) -> eyre::Result<()> {
        if !dir_path.is_dir() {
            return Ok(());
        }

        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;

            if file_type.is_dir() {
                self.drain_new_files_recursive(&path, notification_batch_received_at_ns)?;
            } else if file_type.is_file() {
                self.drain_file(&path, notification_batch_received_at_ns)?;
            }
        }

        Ok(())
    }

    fn drain_file(
        &mut self,
        path: &Path,
        notification_batch_received_at_ns: u128
    ) -> eyre::Result<()> {
        let drain_file_started_at_ns = unix_timestamp_ns();
        if !path.is_file() {
            return Ok(());
        }

        let path = path.to_path_buf();
        if !self.directory.file_states.contains_key(&path) {
            self.directory
                .file_states
                .insert(path.clone(), FileTailState::new(&path, false)?);
        }

        if let Some(state) = self.directory.file_states.get_mut(&path) {
            let out_tx = self.out_tx.clone();
            let name = self.directory.name;
            let path = path.display().to_string();
            let drain_new_bytes_started_at_ns = unix_timestamp_ns();
            let mut chunks = Vec::new();

            state.drain_new_bytes(|chunk| {
                chunks.push(PendingFsChunk {
                    bytes:                 chunk.to_vec(),
                    chunk_len:             chunk.len(),
                    file_bytes_read_at_ns: unix_timestamp_ns()
                });
                Ok(())
            })?;
            let drain_new_bytes_finished_at_ns = unix_timestamp_ns();
            let drain_file_finished_at_ns = unix_timestamp_ns();

            for chunk in chunks {
                let channel_send_started_at_ns = unix_timestamp_ns();
                out_tx.send(Ok(FsOutData {
                    name,
                    bytes: chunk.bytes,
                    path: path.clone(),
                    chunk_len: chunk.chunk_len,
                    notification_received_at_ms: notification_batch_received_at_ns / NS_PER_MS,
                    pipeline: FsPipelineTimestamps {
                        notification_batch_received_at_ns,
                        drain_file_started_at_ns,
                        drain_new_bytes_started_at_ns,
                        file_bytes_read_at_ns: chunk.file_bytes_read_at_ns,
                        drain_new_bytes_finished_at_ns,
                        drain_file_finished_at_ns,
                        channel_send_started_at_ns
                    }
                }))?;
            }
        }

        Ok(())
    }
}

struct PendingFsChunk {
    bytes:                 Vec<u8>,
    chunk_len:             usize,
    file_bytes_read_at_ns: u128
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directory_watcher() {
        let (tx, rx) = mpsc::channel();

        let watcher = DirectoryWatcher::new(HyperliquidDataDirKind::NodeFills, tx).unwrap();
        watcher.run();

        loop {
            let t = rx.recv().unwrap().unwrap();
            println!("{t:?}");
        }
    }
}
