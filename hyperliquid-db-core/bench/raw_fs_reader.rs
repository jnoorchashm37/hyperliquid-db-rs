use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::mpsc,
    thread
};

use hyperliquid_db::{
    fs_handlers::types::{FileTailState, FsOutData, FsPipelineTimestamps},
    hl_fs::HyperliquidDirKind,
    utils::unix_timestamp
};
use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};

pub fn spawn_file_reader(
    name: HyperliquidDirKind,
    base_dir: &Path
) -> eyre::Result<mpsc::Receiver<eyre::Result<FsOutData>>> {
    let directory = RawActiveDirectory::new(name, base_dir)?;
    let notifier = Inotify::init()?;
    let (out_tx, out_rx) = mpsc::channel();

    let mut reader =
        RawDirectoryWatcher { name, directory, notifier, watch_dirs: HashMap::new(), out_tx };
    reader.add_directory_watches_recursive(&reader.directory.dir_path.clone())?;

    thread::spawn(move || {
        if let Err(err) = reader.run() {
            let _ = reader.out_tx.send(Err(err));
        }
    });

    Ok(out_rx)
}

struct RawDirectoryWatcher {
    name:       HyperliquidDirKind,
    directory:  RawActiveDirectory,
    notifier:   Inotify,
    watch_dirs: HashMap<WatchDescriptor, PathBuf>,
    out_tx:     mpsc::Sender<eyre::Result<FsOutData>>
}

impl RawDirectoryWatcher {
    fn run(&mut self) -> eyre::Result<()> {
        let mut event_buf = [0_u8; 16 * 1024];

        loop {
            let events = self.notifier.read_events_blocking(&mut event_buf)?;
            let notification_batch_received_at_ns = unix_timestamp().as_nanos();
            for event in events {
                if event.mask.contains(EventMask::Q_OVERFLOW) {
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

                self.drain_file(&path, notification_batch_received_at_ns)?;
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
                self.out_tx.send(Ok(FsOutData {
                    name: self.name,
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
                }))?;
            }
        }

        Ok(())
    }
}

struct RawActiveDirectory {
    dir_path:    PathBuf,
    file_states: HashMap<PathBuf, FileTailState>
}

impl RawActiveDirectory {
    fn new(name: HyperliquidDirKind, base_dir: &Path) -> eyre::Result<Self> {
        let dir_path = base_dir.join(name.to_string()).canonicalize()?;
        let mut file_states = HashMap::new();
        collect_file_states(&dir_path, true, &mut file_states)?;

        Ok(Self { dir_path, file_states })
    }
}

struct PendingFsChunk {
    bytes:                 Vec<u8>,
    chunk_len:             usize,
    file_bytes_read_at_ns: u128
}

fn collect_file_states(
    dir_path: &Path,
    start_at_end: bool,
    file_states: &mut HashMap<PathBuf, FileTailState>
) -> eyre::Result<()> {
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;

        if file_type.is_dir() {
            collect_file_states(&path, start_at_end, file_states)?;
        } else if file_type.is_file() {
            file_states
                .entry(path.clone())
                .or_insert(FileTailState::new(&path, start_at_end)?);
        }
    }

    Ok(())
}
