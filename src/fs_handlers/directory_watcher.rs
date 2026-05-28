use std::{path::PathBuf, sync::mpsc, time::Instant};

use inotify::{EventMask, Inotify, WatchMask};

use crate::{
    fs_handlers::types::{ActiveDirectory, FileTailState, FsOutData},
    hl_fs::HyperliquidDataDirKind
};

pub struct DirectoryWatcher {
    directory: ActiveDirectory,
    notifier:  Inotify,
    out_tx:    mpsc::Sender<eyre::Result<FsOutData>>
}

impl DirectoryWatcher {
    pub fn new(
        name: HyperliquidDataDirKind,
        out_tx: mpsc::Sender<eyre::Result<FsOutData>>
    ) -> eyre::Result<Self> {
        let directory = ActiveDirectory::new(name)?;

        let notifier = Inotify::init()?;
        notifier.watches().add(
            name.dir_path(),
            WatchMask::CREATE | WatchMask::MODIFY | WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO
        )?;

        Ok(Self { directory, notifier, out_tx })
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
            let notification_received_at = Instant::now();
            for event in events {
                if event.mask.contains(EventMask::Q_OVERFLOW) {
                    // Production code: full rescan here.
                    return Err(eyre::eyre!(
                        "inotify queue overflow; rescan directory and reconcile offsets"
                    ));
                }

                let Some(name) = event.name else {
                    continue;
                };

                let path = self.directory.dir_path.join(name);
                if !path.is_file() {
                    continue;
                }

                if !self.directory.file_states.contains_key(&path) {
                    self.directory
                        .file_states
                        .insert(path.clone(), FileTailState::new(&path, false)?);
                }

                if let Some(state) = self.directory.file_states.get_mut(&path) {
                    state.drain_new_bytes(|chunk| {
                        self.out_tx.send(Ok(FsOutData {
                            name: self.directory.name,
                            bytes: chunk.to_vec(),
                            path: path.display().to_string(),
                            chunk_len: chunk.len(),
                            notification_received_at
                        }))?;
                        Ok(())
                    })?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn test_directory_watcher() {
        let (tx, rx) = mpsc::channel();

        let watcher = DirectoryWatcher::new(HyperliquidDataDirKind::ReplicaCmds, tx).unwrap();
        watcher.run();

        loop {
            let t = rx.recv().unwrap().unwrap();
            println!("{t:?}");
        }
    }
}
