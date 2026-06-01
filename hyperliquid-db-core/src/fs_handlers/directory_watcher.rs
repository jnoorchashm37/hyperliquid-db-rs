use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::mpsc
};

use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};

use crate::{
    fs_handlers::types::{ActiveDirectory, FileTailState, FsOutData, FsPipelineTimestamps},
    hl_fs::{
        HyperliquidDirData, HyperliquidDirKind,
        parsers::{HyperliquidDataParser, NodeFillsParser}
    },
    utils::unix_timestamp
};

pub struct DirectoryWatcher {
    name:       HyperliquidDirKind,
    directory:  ActiveDirectory,
    notifier:   Inotify,
    watch_dirs: HashMap<WatchDescriptor, PathBuf>,
    out_tx:     mpsc::Sender<eyre::Result<HyperliquidDirData>>
}

impl DirectoryWatcher {
    pub fn spawn(
        name: HyperliquidDirKind,
        out_tx: mpsc::Sender<eyre::Result<HyperliquidDirData>>
    ) -> eyre::Result<()> {
        let directory = ActiveDirectory::new(name)?;
        let notifier = Inotify::init()?;
        let mut watcher = Self { name, directory, notifier, watch_dirs: HashMap::new(), out_tx };
        watcher.add_directory_watches_recursive(&watcher.directory.dir_path.clone())?;
        watcher.run();

        Ok(watcher)
    }

    pub fn run(mut self) {
        std::thread::spawn(move || {
            let result = match self.name {
                HyperliquidDirKind::NodeFills => self.run_safe::<NodeFillsParser>()
            };
            if let Err(error) = result {
                tracing::error!("error running filesystem watcher: {error:?}");
                self.out_tx.send(Err(error)).unwrap();
            } else {
                let error = eyre::eyre!("filesystem watcher ended prematurely");
                tracing::error!(?error);
                self.out_tx.send(Err(error)).unwrap();
            }
        });
    }

    fn run_safe<P: HyperliquidDataParser>(&mut self) -> eyre::Result<()> {
        let mut parser = P::default();

        let mut event_buf = [0_u8; 16 * 1024];

        loop {
            let events = self.notifier.read_events_blocking(&mut event_buf)?;
            let notification_batch_received_at_ns = unix_timestamp().as_nanos();
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

                let Some(path) = self
                    .watch_dirs
                    .get(&event.wd)
                    .map(|dir_path| match event.name {
                        Some(name) => dir_path.join(name),
                        None => dir_path.clone()
                    })
                else {
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

                self.drain_file(&mut parser, &path, notification_batch_received_at_ns)?;
            }
        }
    }

    fn add_directory_watches_recursive(&mut self, dir_path: &Path) -> eyre::Result<()> {
        if !dir_path.is_dir() {
            return Ok(());
        }

        let wd = self.notifier.watches().add(dir_path, Self::watch_mask())?;
        self.watch_dirs.insert(wd, dir_path.to_path_buf());

        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                self.add_directory_watches_recursive(&entry.path())?;
            }
        }

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
        parser: &mut impl HyperliquidDataParser,
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
                self.drain_file(parser, &path, notification_batch_received_at_ns)?;
            }
        }

        Ok(())
    }

    fn drain_file(
        &mut self,
        parser: &mut impl HyperliquidDataParser,
        path: &Path,
        notification_batch_received_at_ns: u128
    ) -> eyre::Result<()> {
        let drain_file_started_at_ns = unix_timestamp().as_nanos();
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
            let drain_new_bytes_started_at_ns = unix_timestamp().as_nanos();
            let mut chunks = Vec::new();

            state.drain_new_bytes(|chunk| {
                chunks.push(PendingFsChunk {
                    bytes:                 chunk.to_vec(),
                    chunk_len:             chunk.len(),
                    file_bytes_read_at_ns: unix_timestamp().as_nanos()
                });
                Ok(())
            })?;
            let drain_new_bytes_finished_at_ns = unix_timestamp().as_nanos();
            let drain_file_finished_at_ns = unix_timestamp().as_nanos();

            for chunk in chunks {
                let channel_send_started_at_ns = unix_timestamp().as_nanos();
                let raw_data = FsOutData {
                    name,
                    bytes: chunk.bytes,
                    path: path.clone(),
                    chunk_len: chunk.chunk_len,
                    notification_received_at_ns: notification_batch_received_at_ns,
                    pipeline: FsPipelineTimestamps {
                        notification_batch_received_at_ns,
                        drain_file_started_at_ns,
                        drain_new_bytes_started_at_ns,
                        file_bytes_read_at_ns: chunk.file_bytes_read_at_ns,
                        drain_new_bytes_finished_at_ns,
                        drain_file_finished_at_ns,
                        channel_send_started_at_ns
                    }
                };

                let data = parser.handle_raw_data(raw_data)?;

                out_tx.send(Ok(data))?;
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

        let watcher = DirectoryWatcher::new(HyperliquidDirKind::NodeFills, tx).unwrap();
        watcher.run();

        loop {
            let t = rx.recv().unwrap().unwrap();
            println!("{t:?}");
        }
    }
}
