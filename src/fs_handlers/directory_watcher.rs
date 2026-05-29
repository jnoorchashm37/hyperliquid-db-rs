use std::{
    collections::HashMap,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    sync::mpsc,
    time::Instant
};

use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};

use crate::{
    fs_handlers::types::{ActiveDirectory, FileTailState, FsOutData},
    hl_fs::HyperliquidDataDirKind
};

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
            let notification_received_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
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
                        self.drain_new_files_recursive(&path, notification_received_at_ms)?;
                    }
                    continue;
                }

                self.drain_file(&path, notification_received_at_ms)?;
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
        notification_received_at_ms: u128
    ) -> eyre::Result<()> {
        if !dir_path.is_dir() {
            return Ok(());
        }

        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;

            if file_type.is_dir() {
                self.drain_new_files_recursive(&path, notification_received_at_ms)?;
            } else if file_type.is_file() {
                self.drain_file(&path, notification_received_at_ms)?;
            }
        }

        Ok(())
    }

    fn drain_file(&mut self, path: &Path, notification_received_at_ms: u128) -> eyre::Result<()> {
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

            state.drain_new_bytes(|chunk| {
                out_tx.send(Ok(FsOutData {
                    name,
                    bytes: chunk.to_vec(),
                    path: path.clone(),
                    chunk_len: chunk.len(),
                    notification_received_at_ms
                }))?;
                Ok(())
            })?;
        }

        Ok(())
    }
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
