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
    /// Choose bounded physical read groups from the requested pages' locality.
    pub adaptive_reads: bool,
    pub max_pending_mib: u64,
    pub segment_mib: u64,
    pub max_inflight: usize,
    /// Persist data and retrieval metadata, without unrelated inode timestamps.
    /// false retains the original fsync path for measured comparisons.
    pub sync_data_only: bool,
    /// Reserve WAL blocks without changing EOF. Experimental; measured separately.
    pub wal_preallocate: bool,
    /// Submit each WAL header and payload through one vectored write.
    pub wal_writev: bool,
    /// Experimental logical-page SSD cache, within disk_cache_mib.
    pub logical_cache: bool,
    /// Experimental commit records in the WAL: one durability barrier.
    pub wal_commit_records: bool,
    /// Experimental fully initialized fixed-capacity WAL segments.
    pub wal_fixed_size: bool,
    /// Maximum additional delay to merge concurrent flush requests.
    pub flush_batch_us: u64,
    /// Experimental disposable cache fills outside the write acknowledgement path.
    pub async_cache: bool,
    /// Bound the cache worker's queued and executing payloads.
    pub cache_queue_mib: usize,
    /// Batch local reads per request and partition the logical cache.
    pub fast_local_reads: bool,
    /// Prepare fixed WAL files before rotation and snapshot checkpoint metadata.
    pub checkpoint_pipeline: bool,
    /// Avoid resynchronizing immutable files already covered by a durable frontier.
    pub selective_sync: bool,
    /// Experimental persistent ublk workers and shared completion notification.
    pub ublk_fast_path: bool,
    /// New-volume format: recover ONLY a complete S3 generation after restart.
    /// FLUSH/FUA order writes but do not promise per-transaction persistence.
    pub generation_mode: bool,
    /// Stop accepting writes when the oldest unpublished generation is this old.
    pub generation_max_lag_seconds: u64,
    /// Rebuildable SSD index with a bounded resident shard cache.
    pub paged_index: bool,
    /// Publish only final page versions at each checkpoint.
    pub compact_checkpoints: bool,
    /// Experimental IDWAL002 aligned records (old readers reject this format).
    pub aligned_wal: bool,
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
            adaptive_reads: false,
            max_pending_mib: 8192,
            segment_mib: 16,
            max_inflight: 128,
            sync_data_only: true,
            wal_preallocate: false,
            wal_writev: true,
            logical_cache: false,
            wal_commit_records: false,
            wal_fixed_size: false,
            flush_batch_us: 0,
            async_cache: false,
            cache_queue_mib: 16,
            fast_local_reads: false,
            checkpoint_pipeline: false,
            selective_sync: false,
            ublk_fast_path: false,
            generation_mode: false,
            generation_max_lag_seconds: 30,
            paged_index: false,
            compact_checkpoints: false,
            aligned_wal: false,
        }
    }
}
impl Config {
    /// Explicit settings for newly generated configs. Deserializing an older
    /// config retains historical defaults for omitted fields.
    pub fn recommended() -> Self {
        Self {
            disk_cache_mib: 4096,
            hot_wal_mib: 64,
            max_index_mib: 128,
            max_pending_mib: 1024,
            segment_mib: 32,
            logical_cache: true,
            wal_commit_records: true,
            wal_fixed_size: true,
            async_cache: true,
            fast_local_reads: true,
            checkpoint_pipeline: true,
            selective_sync: true,
            ublk_fast_path: true,
            paged_index: true,
            adaptive_reads: true,
            ..Self::default()
        }
    }

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
        ensure!(
            c.flush_batch_us <= 5000,
            "flush batching delay exceeds 5 ms"
        );
        ensure!(c.memory_cache_mib <= 16384, "memory cache too large");
        ensure!(
            !c.generation_mode
                || (c.generation_max_lag_seconds >= c.checkpoint_seconds
                    && c.generation_max_lag_seconds <= 3600),
            "invalid generation lag bound"
        );
        ensure!(
            (1..=128).contains(&c.cache_queue_mib),
            "invalid cache queue budget"
        );
        ensure!(
            [16, 64, 256].contains(&c.read_extent_kib),
            "read_extent_kib must be 16, 64 or 256"
        );
        ensure!(
            !c.adaptive_reads || (c.logical_cache && c.fast_local_reads),
            "adaptive_reads requires logical_cache and fast_local_reads"
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
        if c.checkpoint_pipeline {
            ensure!(
                c.wal_fixed_size,
                "checkpoint_pipeline requires wal_fixed_size"
            );
        }
        if c.wal_fixed_size {
            let units = if c.checkpoint_pipeline { 5 } else { 2 };
            ensure!(
                c.max_pending_mib * 1024 * 1024 >= crate::wal_pool::capacity(&c) * units,
                "pending WAL budget must hold two initialized segments plus any preparation pool"
            );
        }
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_wal_budget_is_validated_using_physical_capacity() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("config.toml");
        let mut config = Config {
            local_dir: directory.path().join("volume"),
            wal_fixed_size: true,
            segment_mib: 16,
            max_pending_mib: 32,
            ..Config::default()
        };
        for (pipeline, budget, valid) in [
            (false, 32, false),
            (false, 49, true),
            (true, 49, false),
            (true, 121, true),
        ] {
            config.checkpoint_pipeline = pipeline;
            config.max_pending_mib = budget;
            std::fs::write(&path, toml::to_string(&config)?)?;
            assert_eq!(
                Config::load(&path).is_ok(),
                valid,
                "pipeline={pipeline}, budget={budget}"
            );
        }
        Ok(())
    }
}
