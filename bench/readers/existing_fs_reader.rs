use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::mpsc,
    thread
};

use hyperliquid_db::{
    fs_handlers::types::{FsOutData, FsPipelineTimestamps},
    hl_fs::HyperliquidDirKind,
    utils::unix_timestamp
};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

pub fn spawn_file_reader(
    name: HyperliquidDirKind,
    dir_path: &Path
) -> eyre::Result<mpsc::Receiver<eyre::Result<FsOutData>>> {
    let directory = dir_path.join(name.to_string()).canonicalize()?;
    let (out_tx, out_rx) = mpsc::channel();
    let (fs_event_tx, fs_event_rx) = mpsc::channel();

    let mut watcher = notify::recommended_watcher(move |res| {
        let _ = fs_event_tx.send(TimedFsEvent {
            event: res,
            notification_batch_received_at_ns: unix_timestamp().as_nanos()
        });
    })?;
    watcher.watch(&directory, RecursiveMode::Recursive)?;

    let mut reader = ExistingFsReader { name, current_file: None, current_path: None, out_tx };

    thread::spawn(move || {
        if let Err(err) = reader.run(fs_event_rx, watcher) {
            eprintln!("existing file reader exited: {err:?}");
        }
    });

    Ok(out_rx)
}

struct ExistingFsReader {
    name:         HyperliquidDirKind,
    current_file: Option<File>,
    current_path: Option<PathBuf>,
    out_tx:       mpsc::Sender<eyre::Result<FsOutData>>
}

struct TimedFsEvent {
    event: notify::Result<Event>,
    notification_batch_received_at_ns: u128
}

impl ExistingFsReader {
    fn run(
        &mut self,
        fs_event_rx: mpsc::Receiver<TimedFsEvent>,
        _watcher: RecommendedWatcher
    ) -> eyre::Result<()> {
        while let Ok(timed_event) = fs_event_rx.recv() {
            let event = timed_event.event?;
            let notification_batch_received_at_ns = timed_event.notification_batch_received_at_ns;

            if event.kind.is_create() {
                for path in event.paths {
                    self.on_file_creation(path, notification_batch_received_at_ns)?;
                }
            } else if event.kind.is_modify() {
                for path in event.paths {
                    self.on_file_modification(path, notification_batch_received_at_ns)?;
                }
            }
        }

        Err(eyre::eyre!("notify file event channel closed"))
    }

    fn on_file_creation(
        &mut self,
        new_file: PathBuf,
        notification_batch_received_at_ns: u128
    ) -> eyre::Result<()> {
        if !new_file.is_file() {
            return Ok(());
        }

        self.flush_current_file(notification_batch_received_at_ns)?;
        self.current_file = Some(File::open(&new_file)?);
        self.current_path = Some(new_file);

        Ok(())
    }

    fn on_file_modification(
        &mut self,
        new_file: PathBuf,
        notification_batch_received_at_ns: u128
    ) -> eyre::Result<()> {
        if !new_file.is_file() {
            return Ok(());
        }

        if self.current_file.is_some() {
            self.flush_current_file(notification_batch_received_at_ns)
        } else {
            let mut file = File::open(&new_file)?;
            file.seek(SeekFrom::End(0))?;
            self.current_file = Some(file);
            self.current_path = Some(new_file);
            Ok(())
        }
    }

    fn flush_current_file(&mut self, notification_batch_received_at_ns: u128) -> eyre::Result<()> {
        let drain_file_started_at_ns = unix_timestamp().as_nanos();
        let Some(file) = self.current_file.as_mut() else {
            return Ok(());
        };

        let mut data = String::new();
        let drain_new_bytes_started_at_ns = unix_timestamp().as_nanos();
        file.read_to_string(&mut data)?;
        let file_bytes_read_at_ns = unix_timestamp().as_nanos();
        let drain_new_bytes_finished_at_ns = file_bytes_read_at_ns;
        if data.is_empty() {
            return Ok(());
        }

        let path = self
            .current_path
            .as_ref()
            .map_or_else(String::new, |path| path.display().to_string());
        let chunk_len = data.len();
        let drain_file_finished_at_ns = unix_timestamp().as_nanos();
        let channel_send_started_at_ns = unix_timestamp().as_nanos();

        self.out_tx.send(Ok(FsOutData {
            name: self.name,
            bytes: data.into_bytes(),
            path,
            chunk_len,
            notification_received_at_ns: notification_batch_received_at_ns,
            pipeline: FsPipelineTimestamps {
                notification_batch_received_at_ns,
                drain_file_started_at_ns,
                drain_new_bytes_started_at_ns,
                file_bytes_read_at_ns,
                drain_new_bytes_finished_at_ns,
                drain_file_finished_at_ns,
                channel_send_started_at_ns
            }
        }))?;

        Ok(())
    }
}
