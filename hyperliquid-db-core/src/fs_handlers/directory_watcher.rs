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
        HyperliquidDirData, HyperliquidDirDataWithMeta, HyperliquidDirKind,
        parsers::{
            HyperliquidDataParser, NodeFillsParser, NodeOrderStatusesParser, NodeRawBookDiffsParser
        }
    },
    utils::unix_timestamp
};

pub struct DirectoryWatcher {
    name:       HyperliquidDirKind,
    directory:  ActiveDirectory,
    notifier:   Inotify,
    watch_dirs: HashMap<WatchDescriptor, PathBuf>,
    out_tx:     mpsc::Sender<eyre::Result<HyperliquidDirDataWithMeta>>
}

impl DirectoryWatcher {
    pub fn spawn(
        name: HyperliquidDirKind,
        out_tx: mpsc::Sender<eyre::Result<HyperliquidDirDataWithMeta>>
    ) -> eyre::Result<()> {
        let directory = ActiveDirectory::new(name)?;
        tracing::info!(
            name = ?name,
            path = ?directory.dir_path,
            existing_files = directory.file_states.len(),
            "initializing directory watcher"
        );
        let notifier = Inotify::init()?;
        let mut watcher = Self { name, directory, notifier, watch_dirs: HashMap::new(), out_tx };
        watcher.add_directory_watches_recursive(&watcher.directory.dir_path.clone())?;
        tracing::info!(
            name = ?name,
            directories = watcher.watch_dirs.len(),
            "watching directories"
        );
        watcher.run();

        Ok(())
    }

    pub fn run(mut self) {
        std::thread::spawn(move || {
            let result = match self.name {
                HyperliquidDirKind::NodeFills => self.run_safe::<NodeFillsParser>(),
                HyperliquidDirKind::NodeOrderStatuses => self.run_safe::<NodeOrderStatusesParser>(),
                HyperliquidDirKind::NodeRawBookDiffs => self.run_safe::<NodeRawBookDiffsParser>(),
                HyperliquidDirKind::Hip3OracleUpdates => self.run_safe::<Hip3OracleUpdatesParser>()
            };
            if let Err(error) = result {
                tracing::error!("error running filesystem watcher: {error:?}");
                let _ = self.out_tx.send(Err(error));
            } else {
                let error = eyre::eyre!("filesystem watcher ended prematurely");
                tracing::error!(?error);
                let _ = self.out_tx.send(Err(error));
            }
        });
    }

    fn run_safe<P>(&mut self) -> eyre::Result<()>
    where
        P: HyperliquidDataParser,
        HyperliquidDirData: From<Vec<P::ParsedType>>
    {
        let mut parser = P::default();

        let mut event_buf = [0_u8; 16 * 1024];
        tracing::info!(name = ?self.name, "waiting for filesystem events");

        loop {
            let events = self.notifier.read_events_blocking(&mut event_buf)?;
            let notification_batch_received_at_ns = unix_timestamp().as_nanos();
            tracing::trace!(
                name = ?self.name,
                notification_batch_received_at_ns,
                "received filesystem event batch"
            );
            for event in events {
                tracing::trace!(
                    name = ?self.name,
                    mask = ?event.mask,
                    watch_descriptor = ?event.wd,
                    event_name = ?event.name,
                    "processing filesystem event"
                );
                if event.mask.contains(EventMask::Q_OVERFLOW) {
                    // Production code: full rescan here.
                    return Err(eyre::eyre!(
                        "inotify queue overflow; rescan directory and reconcile offsets"
                    ));
                }
                if event.mask.contains(EventMask::IGNORED) {
                    self.watch_dirs.remove(&event.wd);
                    tracing::trace!(
                        name = ?self.name,
                        watch_descriptor = ?event.wd,
                        "removed ignored watch descriptor"
                    );
                    continue;
                }

                let Some(path) =
                    self.watch_dirs
                        .get(&event.wd)
                        .map(|dir_path: &PathBuf| match event.name {
                            Some(name) => dir_path.join(name),
                            None => dir_path.clone()
                        })
                else {
                    tracing::trace!(
                        name = ?self.name,
                        watch_descriptor = ?event.wd,
                        "skipping event for unknown watch descriptor"
                    );
                    continue;
                };

                if event.mask.contains(EventMask::DELETE)
                    || event.mask.contains(EventMask::MOVED_FROM)
                {
                    if event.mask.contains(EventMask::ISDIR) {
                        self.remove_directory_file_states(&path);
                    } else {
                        self.remove_file_state(&path);
                    }
                    continue;
                }

                if event.mask.contains(EventMask::ISDIR) {
                    if event.mask.contains(EventMask::CREATE)
                        || event.mask.contains(EventMask::MOVED_TO)
                    {
                        tracing::debug!(name = ?self.name, path = ?path, "watching new directory");
                        self.add_directory_watches_recursive(&path)?;
                        self.drain_new_files_recursive(
                            &mut parser,
                            &path,
                            notification_batch_received_at_ns
                        )?;
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
        tracing::trace!(name = ?self.name, path = ?dir_path, "added directory watch");

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
            | WatchMask::MOVED_FROM
            | WatchMask::DELETE
            | WatchMask::DELETE_SELF
            | WatchMask::MOVE_SELF
    }

    fn remove_file_state(&mut self, path: &Path) {
        if self.directory.file_states.remove(path).is_some() {
            tracing::debug!(name = ?self.name, path = ?path, "stopped tracking deleted file");
        } else {
            tracing::trace!(name = ?self.name, path = ?path, "deleted file was not tracked");
        }
    }

    fn remove_directory_file_states(&mut self, dir_path: &Path) {
        let tracked_files_before = self.directory.file_states.len();
        self.directory
            .file_states
            .retain(|path, _| !path.starts_with(dir_path));
        let removed_files = tracked_files_before - self.directory.file_states.len();

        if removed_files > 0 {
            tracing::debug!(
                name = ?self.name,
                path = ?dir_path,
                removed_files,
                "stopped tracking files under deleted directory"
            );
        } else {
            tracing::trace!(
                name = ?self.name,
                path = ?dir_path,
                "deleted directory had no tracked files"
            );
        }
    }

    fn drain_new_files_recursive<P>(
        &mut self,
        parser: &mut P,
        dir_path: &Path,
        notification_batch_received_at_ns: u128
    ) -> eyre::Result<()>
    where
        P: HyperliquidDataParser,
        HyperliquidDirData: From<Vec<P::ParsedType>>
    {
        if !dir_path.is_dir() {
            return Ok(());
        }

        tracing::trace!(name = ?self.name, path = ?dir_path, "draining new files recursively");

        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;

            if file_type.is_dir() {
                self.drain_new_files_recursive(parser, &path, notification_batch_received_at_ns)?;
            } else if file_type.is_file() {
                self.drain_file(parser, &path, notification_batch_received_at_ns)?;
            }
        }

        Ok(())
    }

    fn drain_file<P>(
        &mut self,
        parser: &mut P,
        path: &Path,
        notification_batch_received_at_ns: u128
    ) -> eyre::Result<()>
    where
        P: HyperliquidDataParser,
        HyperliquidDirData: From<Vec<P::ParsedType>>
    {
        tracing::trace!(name = ?self.name, path = ?path, "draining file");
        let drain_file_started_at_ns = unix_timestamp().as_nanos();
        if !path.is_file() {
            tracing::trace!(name = ?self.name, path = ?path, "skipping non-file path");
            return Ok(());
        }

        let path = path.to_path_buf();
        if !self.directory.file_states.contains_key(&path) {
            tracing::debug!(name = ?self.name, path = ?path, "tracking new file");
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

            if !chunks.is_empty() {
                let total_bytes: usize = chunks.iter().map(|chunk| chunk.chunk_len).sum();
                tracing::trace!(
                    name = ?name,
                    path = %path,
                    chunks = chunks.len(),
                    total_bytes,
                    "drained new file bytes"
                );
            } else {
                tracing::trace!(name = ?name, path = %path, "no new file bytes drained");
            }

            for chunk in chunks {
                let channel_send_started_at_ns = unix_timestamp().as_nanos();
                tracing::trace!(
                    name = ?name,
                    path = %path,
                    chunk_len = chunk.chunk_len,
                    notification_batch_received_at_ns,
                    drain_file_started_at_ns,
                    drain_new_bytes_started_at_ns,
                    file_bytes_read_at_ns = chunk.file_bytes_read_at_ns,
                    drain_new_bytes_finished_at_ns,
                    drain_file_finished_at_ns,
                    channel_send_started_at_ns,
                    "parsing filesystem chunk"
                );
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
    use std::sync::mpsc;

    use super::DirectoryWatcher;
    use crate::hl_fs::HyperliquidDirKind;

    #[test]
    fn test_directory_watcher() {
        let (tx, rx) = mpsc::channel();

        DirectoryWatcher::spawn(HyperliquidDirKind::NodeFills, tx).unwrap();

        loop {
            let t = rx.recv().unwrap().unwrap();
            println!("{t:?}");
        }
    }
}
