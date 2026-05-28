use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::Instant
};

use hyperliquid_db::fs_watchers::types::{FsOutData, HyperliquidDataDirKind};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

pub fn spawn_file_reader(
    _name: HyperliquidDataDirKind,
    dir_path: &Path
) -> eyre::Result<mpsc::Receiver<eyre::Result<FsOutData>>> {
    let directory = dir_path.canonicalize()?;
    let (out_tx, out_rx) = mpsc::channel();
    let (fs_event_tx, fs_event_rx) = mpsc::channel();

    let mut watcher = notify::recommended_watcher(move |res| {
        let _ = fs_event_tx.send(TimedFsEvent {
            event:                    res,
            notification_received_at: Instant::now()
        });
    })?;
    watcher.watch(&directory, RecursiveMode::Recursive)?;

    let mut reader = ExistingFsReader { current_file: None, current_path: None, out_tx };

    thread::spawn(move || {
        if let Err(err) = reader.run(fs_event_rx, watcher) {
            eprintln!("existing file reader exited: {err:?}");
        }
    });

    Ok(out_rx)
}

struct ExistingFsReader {
    current_file: Option<File>,
    current_path: Option<PathBuf>,
    out_tx:       mpsc::Sender<eyre::Result<FsOutData>>
}

struct TimedFsEvent {
    event:                    notify::Result<Event>,
    notification_received_at: Instant
}

impl ExistingFsReader {
    fn run(
        &mut self,
        fs_event_rx: mpsc::Receiver<TimedFsEvent>,
        _watcher: RecommendedWatcher
    ) -> eyre::Result<()> {
        while let Ok(timed_event) = fs_event_rx.recv() {
            let event = timed_event.event?;
            let notification_received_at = timed_event.notification_received_at;

            if event.kind.is_create() {
                for path in event.paths {
                    self.on_file_creation(path, notification_received_at)?;
                }
            } else if event.kind.is_modify() {
                for path in event.paths {
                    self.on_file_modification(path, notification_received_at)?;
                }
            }
        }

        Err(eyre::eyre!("notify file event channel closed"))
    }

    fn on_file_creation(
        &mut self,
        new_file: PathBuf,
        notification_received_at: Instant
    ) -> eyre::Result<()> {
        if !new_file.is_file() {
            return Ok(());
        }

        self.flush_current_file(notification_received_at)?;
        self.current_file = Some(File::open(&new_file)?);
        self.current_path = Some(new_file);

        Ok(())
    }

    fn on_file_modification(
        &mut self,
        new_file: PathBuf,
        notification_received_at: Instant
    ) -> eyre::Result<()> {
        if !new_file.is_file() {
            return Ok(());
        }

        if self.current_file.is_some() {
            self.flush_current_file(notification_received_at)
        } else {
            let mut file = File::open(&new_file)?;
            file.seek(SeekFrom::End(0))?;
            self.current_file = Some(file);
            self.current_path = Some(new_file);
            Ok(())
        }
    }

    fn flush_current_file(&mut self, notification_received_at: Instant) -> eyre::Result<()> {
        let Some(file) = self.current_file.as_mut() else {
            return Ok(());
        };

        let mut data = String::new();
        file.read_to_string(&mut data)?;
        if data.is_empty() {
            return Ok(());
        }

        let path = self
            .current_path
            .as_ref()
            .map_or_else(String::new, |path| path.display().to_string());
        let chunk_len = data.len();

        self.out_tx.send(Ok(FsOutData {
            name: HyperliquidDataDirKind::NodeSlowBlockTimes,
            bytes: data.into_bytes(),
            path,
            chunk_len,
            notification_received_at
        }))?;

        Ok(())
    }
}
