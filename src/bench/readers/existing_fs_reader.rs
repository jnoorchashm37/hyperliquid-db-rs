use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
};

use hyperliquid_db::fs_watchers::{directory::OutData, types::HyperliquidDataDirKind};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

pub fn spawn_file_reader(
    _name: HyperliquidDataDirKind,
    dir_path: &Path,
) -> eyre::Result<mpsc::Receiver<OutData>> {
    let directory = dir_path.canonicalize()?;
    let (out_tx, out_rx) = mpsc::channel();
    let (fs_event_tx, fs_event_rx) = mpsc::channel();

    let mut watcher = notify::recommended_watcher(move |res| {
        let _ = fs_event_tx.send(res);
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
    out_tx: mpsc::Sender<OutData>,
}

impl ExistingFsReader {
    fn run(
        &mut self,
        fs_event_rx: mpsc::Receiver<notify::Result<Event>>,
        _watcher: RecommendedWatcher,
    ) -> eyre::Result<()> {
        while let Ok(event) = fs_event_rx.recv() {
            let event = event?;

            if event.kind.is_create() {
                for path in event.paths {
                    self.on_file_creation(path)?;
                }
            } else if event.kind.is_modify() {
                for path in event.paths {
                    self.on_file_modification(path)?;
                }
            }
        }

        Err(eyre::eyre!("notify file event channel closed"))
    }

    fn on_file_creation(&mut self, new_file: PathBuf) -> eyre::Result<()> {
        if !new_file.is_file() {
            return Ok(());
        }

        self.flush_current_file()?;
        self.current_file = Some(File::open(&new_file)?);
        self.current_path = Some(new_file);

        Ok(())
    }

    fn on_file_modification(&mut self, new_file: PathBuf) -> eyre::Result<()> {
        if !new_file.is_file() {
            return Ok(());
        }

        if self.current_file.is_some() {
            self.flush_current_file()
        } else {
            let mut file = File::open(&new_file)?;
            file.seek(SeekFrom::End(0))?;
            self.current_file = Some(file);
            self.current_path = Some(new_file);
            Ok(())
        }
    }

    fn flush_current_file(&mut self) -> eyre::Result<()> {
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

        self.out_tx
            .send(OutData { bytes: data.into_bytes(), path, chunk_len })?;

        Ok(())
    }
}
