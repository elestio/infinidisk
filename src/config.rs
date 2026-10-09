use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub local_dir: PathBuf,
    /// file:///absolute/directory or s3://bucket/prefix
    pub store: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub listen: SocketAddr,
    pub checkpoint_seconds: u64,
    pub memory_cache_mib: usize,
    pub disk_cache_mib: u64,
    pub hot_wal_mib: u64,
    /// Conservative accounting cap for the current in-memory page index.
    pub max_index_mib: usize,
    pub read_extent_kib: u64,
    pub max_pending_mib: u64,
    pub segment_mib: u64,
    pub max_inflight: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            local_dir: "/var/lib/infinidisk2/volume".into(),
            store: "file:///var/lib/infinidisk2/objects".into(),
            endpoint: None,
            region: "us-east-1".into(),
            listen: "127.0.0.1:11900".parse().unwrap(),
            checkpoint_seconds: 5,
            memory_cache_mib: 128,
            disk_cache_mib: 2048,
            hot_wal_mib: 1024,
            max_index_mib: 1024,
            read_extent_kib: 64,
            max_pending_mib: 8192,
            segment_mib: 16,
            max_inflight: 128,
        }
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let c: Self = toml::from_str(&std::fs::read_to_string(path).context("read config")?)?;
        ensure!(c.local_dir.is_absolute(), "local_dir must be absolute");
        ensure!(
            c.listen.ip().is_loopback(),
            "NBD is unauthenticated: listen must be loopback"
        );
        ensure!(
            c.checkpoint_seconds > 0 && c.segment_mib > 0 && c.max_inflight > 0,
            "zero configuration limit"
        );
        ensure!(c.memory_cache_mib <= 16384, "memory cache too large");
        ensure!(
            [16, 64, 256].contains(&c.read_extent_kib),
            "read_extent_kib must be 16, 64 or 256"
        );
        ensure!(
            c.max_pending_mib >= c.segment_mib * 2,
            "pending limit must hold at least two segments"
        );
        ensure!(
            c.segment_mib <= 64
                && c.max_pending_mib <= 1024 * 1024
                && c.disk_cache_mib <= 1024 * 1024
                && c.hot_wal_mib <= 1024 * 1024,
            "invalid disk/segment limit"
        );
        ensure!(
            c.max_index_mib > 0 && c.max_index_mib <= 65536,
            "invalid index limit"
        );
        Ok(c)
    }
}
