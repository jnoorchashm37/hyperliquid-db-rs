use std::{collections::HashMap, fmt, fs, os::unix::fs::FileExt, path::PathBuf, time::Instant};

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

        for entry in fs::read_dir(&dir_path)? {
            let path = entry?.path();
            if path.is_file() {
                file_states.insert(path.clone(), FileTailState::new(&path, true)?);
            }
        }

        Ok(Self { name, dir_path, file_states })
    }
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
    pub name:                     HyperliquidDataDirKind,
    pub bytes:                    Vec<u8>,
    pub path:                     String,
    pub chunk_len:                usize,
    pub notification_received_at: Instant
}
