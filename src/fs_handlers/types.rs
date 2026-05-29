use std::{
    collections::HashMap,
    fs,
    os::unix::fs::FileExt,
    path::{Path, PathBuf}
};

use crate::hl_fs::HyperliquidDataDirKind;

pub struct ActiveDirectory {
    pub name:        HyperliquidDataDirKind,
    pub dir_path:    PathBuf,
    pub file_states: HashMap<PathBuf, FileTailState>
}

impl ActiveDirectory {
    pub fn new(name: HyperliquidDataDirKind) -> eyre::Result<Self> {
        let dir_path = name.dir_path();
        let mut file_states: HashMap<PathBuf, FileTailState> = HashMap::new();

        collect_file_states(&dir_path, true, &mut file_states)?;

        Ok(Self { name, dir_path, file_states })
    }
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

#[derive(Debug)]
pub struct FileTailState {
    pub file:   fs::File,
    pub offset: u64
}

impl FileTailState {
    pub fn new(path: &PathBuf, start_at_end: bool) -> eyre::Result<Self> {
        let file = fs::File::open(path)?;
        let offset = if start_at_end { file.metadata()?.len() } else { 0 };

        Ok(Self { file, offset })
    }

    pub fn drain_new_bytes(
        &mut self,
        mut on_chunk: impl FnMut(&[u8]) -> eyre::Result<()>
    ) -> eyre::Result<()> {
        let mut buf = [0_u8; 64 * 1024];

        loop {
            let n = self.file.read_at(&mut buf, self.offset)?;
            if n == 0 {
                break; // EOF for now
            }
            on_chunk(&buf[..n])?;
            self.offset += n as u64;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FsOutData {
    pub name: HyperliquidDataDirKind,
    pub bytes: Vec<u8>,
    pub path: String,
    pub chunk_len: usize,
    pub notification_received_at_ns: u128,
    pub pipeline: FsPipelineTimestamps
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FsPipelineTimestamps {
    pub notification_batch_received_at_ns: u128,
    pub drain_file_started_at_ns:          u128,
    pub drain_new_bytes_started_at_ns:     u128,
    pub file_bytes_read_at_ns:             u128,
    pub drain_new_bytes_finished_at_ns:    u128,
    pub drain_file_finished_at_ns:         u128,
    pub channel_send_started_at_ns:        u128
}
