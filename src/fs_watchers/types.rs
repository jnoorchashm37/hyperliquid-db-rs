use std::{collections::HashMap, fmt, fs, os::unix::fs::FileExt, path::PathBuf, str::FromStr};

pub struct ActiveDirectory {
    pub name:        HyperliquidDataDirKind,
    pub dir_path:    PathBuf,
    pub file_states: HashMap<PathBuf, FileTailState>
}

impl ActiveDirectory {
    pub fn new(name: HyperliquidDataDirKind, dir_path: &PathBuf) -> eyre::Result<Self> {
        let mut file_states: HashMap<PathBuf, FileTailState> = HashMap::new();

        for entry in fs::read_dir(dir_path)? {
            let path = entry?.path();
            if path.is_file() {
                file_states.insert(path.clone(), FileTailState::new(&path, true)?);
            }
        }

        Ok(Self { name, dir_path: dir_path.clone(), file_states })
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub enum HyperliquidDataDirKind {
    ReplicaCmds,
    NodeSlowBlockTimes
}

impl FromStr for HyperliquidDataDirKind {
    type Err = eyre::ErrReport;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "replica_cmds" => Ok(Self::ReplicaCmds),
            "node_slow_block_times" => Ok(Self::NodeSlowBlockTimes),
            _ => Err(eyre::eyre!("invalid `HyperliquidDataDirKind`: {s}"))
        }
    }
}

impl fmt::Display for HyperliquidDataDirKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            HyperliquidDataDirKind::ReplicaCmds => "replica_cmds",
            HyperliquidDataDirKind::NodeSlowBlockTimes => "node_slow_block_times"
        };

        fmt::Display::fmt(s, f)
    }
}

impl fmt::Debug for HyperliquidDataDirKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
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
