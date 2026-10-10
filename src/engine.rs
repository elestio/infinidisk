use crate::{
    cache::{DiskCache, EXTENT as READ_EXTENT},
    config::Config,
    index::PageIndex,
    store::Store,
    wal::{self, MAX_IO, PAGE, Ref, Segment, Watermark},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use fs2::FileExt as _;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::UpdateVersion;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs::{File, OpenOptions},
    os::unix::fs::FileExt as UnixFileExt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::{Mutex, MutexGuard, Notify};
use uuid::Uuid;

const SHARD_PAGES: u64 = 4096;
const MAX_HEAD_BYTES: u64 = 64 * 1024 * 1024;
const SMALL_READ: u64 = 16 * 1024;
const LARGE_READ: u64 = 256 * 1024;
#[cfg(test)]
#[path = "../tests/support/adaptive_reads.rs"]
mod adaptive_tests;
#[cfg(test)]
#[path = "../tests/support/index_objects.rs"]
mod index_object_tests;
type ScrubGroups = BTreeMap<(Uuid, u64), (u64, Vec<(u64, u32)>)>;
type ReadMiss = (u64, Option<Ref>, Option<Arc<File>>);

struct ReadGroup {
    segment: Uuid,
    start: u64,
    extent: u64,
    pages: Vec<(u64, Ref)>,
}

struct FetchedGroup {
    pages: Vec<(u64, Bytes)>,
    // Hold admission through CRC, cache fill and copying into the caller's buffer.
    _permit: Option<crate::download::Permit>,
}

/// Offline warm owns each physical range until all of its logical pages land
/// in the SSD cache. These buffers never depend on the online extent LRU.
struct WarmGroup {
    pages: Vec<(u64, Ref)>,
    local: Option<Arc<File>>,
}
#[derive(Default)]
struct WarmRanges {
    active: AtomicU64,
    peak: AtomicU64,
}
struct WarmRangeGuard<'a>(&'a WarmRanges);
impl WarmRanges {
    fn start(&self) -> WarmRangeGuard<'_> {
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(active, Ordering::Relaxed);
        WarmRangeGuard(self)
    }
}
impl Drop for WarmRangeGuard<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}
struct WarmResult {
    pages: usize,
    range_peak: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Identity {
    pub volume: Uuid,
    pub writer: Uuid,
    pub size: u64,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Shard {
    key: String,
    hash: String,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Head {
    pub format: u32,
    pub volume: Uuid,
    pub size: u64,
    pub writer: Option<Uuid>,
    pub generation: u64,
    pub seq: u64,
    pub shards: BTreeMap<u64, Shard>,
}
struct Remote {
    head: Head,
    version: UpdateVersion,
}
struct State {
    seq: u64,
    durable: u64,
    index: PageIndex,
    dirty: BTreeSet<u64>,
    active: Segment,
    sealed: Vec<Segment>,
    local: HashMap<Uuid, Arc<File>>,
    sealed_lengths: HashMap<Uuid, u64>,
    pending: u64,
    resident: VecDeque<Segment>,
    resident_bytes: u64,
}
impl State {
    fn reference(&mut self, page: u64) -> Result<Option<Ref>> {
        // A sealed local WAL is still the only authority until its object has
        // been uploaded. Checkpoint publication installs bounded remote refs.
        self.index.get(&page)
    }
}
#[derive(Serialize)]
pub struct Status {
    pub volume: Uuid,
    pub size: u64,
    pub sequence: u64,
    pub local_durable_sequence: u64,
    pub remote_sequence: u64,
    pub remote_generation: u64,
    pub pending_bytes: u64,
    pub allocated_pages: usize,
    pub poisoned: bool,
    pub uptime_seconds: u64,
    pub hot_wal_bytes: u64,
    pub flush_calls: u64,
    pub flush_groups: u64,
    pub flush_wait_ns: u64,
    pub wal_sync_ns: u64,
    pub watermark_sync_ns: u64,
    pub logical_cache_hits: u64,
    pub remote_gets: u64,
    pub remote_bytes: u64,
    pub range_cache_bytes: usize,
    pub downloads: crate::download::Stats,
    pub adaptive_small_gets: u64,
    pub adaptive_large_gets: u64,
    pub checkpoint_wal_bytes: u64,
    pub uploaded_segment_bytes: u64,
    pub index: crate::index::IndexStats,
    pub index_object_cache: crate::index_objects::Stats,
    pub wal_pool_bytes: u64,
    pub cache_queue_bytes: usize,
    pub cache_fills_skipped: u64,
    pub cache_fill_errors: u64,
    pub durability_mode: &'static str,
    pub unpublished_age_ms: u64,
}
#[derive(Default)]
struct FlushMetrics {
    calls: AtomicU64,
    groups: AtomicU64,
    wait_ns: AtomicU64,
    wal_ns: AtomicU64,
    watermark_ns: AtomicU64,
    page_hits: AtomicU64,
    remote_gets: AtomicU64,
    remote_bytes: AtomicU64,
    adaptive_small_gets: AtomicU64,
    adaptive_large_gets: AtomicU64,
    checkpoint_wal_bytes: AtomicU64,
    uploaded_segment_bytes: AtomicU64,
}
pub struct Engine {
    pub config: Config,
    pub identity: Identity,
    pub store: Store,
    state: Mutex<State>,
    watermark: Arc<std::sync::Mutex<Watermark>>,
    remote: Mutex<Remote>,
    flush_lock: Mutex<()>,
    checkpoint_lock: Mutex<()>,
    cache: std::sync::Mutex<crate::read_cache::ReadCache>,
    fetch_locks: Vec<Mutex<()>>,
    downloads: crate::download::Downloads,
    disk_cache: DiskCache,
    index_objects: Arc<crate::index_objects::IndexObjects>,
    page_cache: Option<Arc<crate::page_cache::PageCache>>,
    cache_writer: Option<crate::page_cache::CacheWriter>,
    wal_pool: Option<crate::wal_pool::WalPool>,
    space_available: Notify,
    poisoned: AtomicBool,
    _lock: File,
    started: Instant,
    flush_metrics: FlushMetrics,
    unpublished_since_ns: AtomicU64,
}
fn encode(h: &Head) -> Result<Bytes> {
    ensure!(
        bincode::serialized_size(h)? <= MAX_HEAD_BYTES - 40,
        "remote HEAD exceeds the supported metadata size; publication refused"
    );
    let payload = bincode::serialize(h)?;
    let mut b = Vec::with_capacity(payload.len() + 40);
    b.extend_from_slice(b"IDHEAD01");
    b.extend_from_slice(&Sha256::digest(&payload));
    b.extend(payload);
    Ok(b.into())
}
fn decode(b: &[u8]) -> Result<Head> {
    ensure!(
        b.len() >= 40 && b.len() as u64 <= MAX_HEAD_BYTES && &b[..8] == b"IDHEAD01",
        "invalid remote HEAD"
    );
    ensure!(
        Sha256::digest(&b[40..]).as_slice() == &b[8..40],
        "HEAD checksum mismatch"
    );
    let h: Head = bincode::deserialize(&b[40..])?;
    ensure!(
        (h.format == 1 || h.format == 2) && h.size > 0 && h.size.is_multiple_of(PAGE as u64),
        "unsupported/invalid volume format"
    );
    Ok(h)
}
fn local_lock(c: &Config) -> Result<File> {
    std::fs::create_dir_all(&c.local_dir)?;
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(c.local_dir.join("LOCK"))?;
    f.try_lock_exclusive()
        .context("local directory is already in use")?;
    Ok(f)
}
impl Engine {
    pub async fn init(c: &Config, size: u64) -> Result<Identity> {
        ensure!(
            size >= PAGE as u64 && size.is_multiple_of(PAGE as u64),
            "size must be a positive multiple of 4096"
        );
        let _lock = local_lock(c)?;
        ensure!(
            !c.local_dir.join("identity.json").exists(),
            "local volume already exists"
        );
        ensure!(
            !c.local_dir.join("wal").exists() && !c.local_dir.join("durable").exists(),
            "directory contains an incomplete volume; inspect it before retrying"
        );
        let store = Store::new(c)?;
        ensure!(
            store.head().await?.is_none(),
            "remote volume already exists; use adopt, never init"
        );
        let i = Identity {
            volume: Uuid::new_v4(),
            writer: Uuid::new_v4(),
            size,
        };
        let h = Head {
            format: if c.generation_mode { 2 } else { 1 },
            volume: i.volume,
            size,
            writer: Some(i.writer),
            generation: 0,
            seq: 0,
            shards: BTreeMap::new(),
        };
        store
            .cas(encode(&h)?, None)
            .await
            .context("create remote volume atomically")?;
        std::fs::create_dir(c.local_dir.join("wal"))?;
        Watermark::create(&c.local_dir.join("durable"))?;
        wal::atomic(
            &c.local_dir.join("identity.json"),
            &serde_json::to_vec_pretty(&i)?,
        )?;
        Ok(i)
    }
    /// Recovery on a new host requires explicit fencing of the old writer first.
    pub async fn adopt(c: &Config, takeover: bool) -> Result<Identity> {
        let _lock = local_lock(c)?;
        ensure!(
            !c.local_dir.join("identity.json").exists()
                && !c.local_dir.join("wal").exists()
                && !c.local_dir.join("durable").exists(),
            "adopt requires an empty local volume directory"
        );
        let store = Store::new(c)?;
        let (b, v) = store.head().await?.context("no remote volume")?;
        let mut h = decode(&b)?;
        ensure!(
            (h.format == 2) == c.generation_mode,
            "generation mode must match the volume format"
        );
        ensure!(
            h.writer != Some(Uuid::nil()),
            "remote volume is under GC maintenance; resume gc --apply on its original host"
        );
        ensure!(
            h.writer.is_none() || takeover,
            "remote writer exists; fence its host, then pass --takeover"
        );
        // Validate the complete remote index before changing ownership.
        Self::load_index(&store, &h, c, None).await?;
        let i = Identity {
            volume: h.volume,
            writer: Uuid::new_v4(),
            size: h.size,
        };
        h.writer = Some(i.writer);
        h.generation += 1;
        store.cas(encode(&h)?, Some(v)).await?;
        std::fs::create_dir(c.local_dir.join("wal"))?;
        Watermark::create(&c.local_dir.join("durable"))?.persist(h.seq)?;
        wal::atomic(
            &c.local_dir.join("identity.json"),
            &serde_json::to_vec_pretty(&i)?,
        )?;
        Ok(i)
    }
    async fn load_index(
        store: &Store,
        h: &Head,
        c: &Config,
        cache: Option<&Arc<crate::index_objects::IndexObjects>>,
    ) -> Result<PageIndex> {
        let mut parts = stream::iter(h.shards.iter().map(|(&id, s)| async move {
            let cached = if let Some(cache) = cache {
                cache.get(h.volume, id, &s.key, &s.hash).await
            } else {
                None
            };
            let from_cache = cached.is_some();
            let b = if let Some(bytes) = cached {
                bytes
            } else {
                let bytes = store.get(&s.key).await?;
                if let Some(cache) = cache {
                    cache.remote_read(bytes.len());
                }
                bytes
            };
            ensure!(
                b.len() <= 1024 * 1024 && hex::encode(Sha256::digest(&b)) == s.hash,
                "index shard checksum/size mismatch"
            );
            let map: BTreeMap<u64, Ref> = bincode::deserialize(&b)?;
            for (&p, r) in &map {
                ensure!(
                    p < h.size / PAGE as u64
                        && p / SHARD_PAGES == id
                        && r.offset >= 64
                        && r.offset
                            .checked_add(PAGE as u64)
                            .is_some_and(|end| end <= r.segment_len),
                    "invalid index reference"
                );
            }
            if !from_cache && let Some(cache) = cache {
                cache.put(h.volume, id, &s.key, &s.hash, b).await;
            }
            Ok::<_, anyhow::Error>((id, map))
        }))
        .buffer_unordered(16);
        let mut index = if c.paged_index {
            PageIndex::paged(
                &c.local_dir.join("index-scratch"),
                c.max_index_mib * 1024 * 1024,
            )?
        } else {
            PageIndex::memory(c.max_index_mib * 1024 * 1024)
        };
        while let Some((id, part)) = parts.try_next().await? {
            index.replace_shard(id, part)?;
        }
        Ok(index)
    }
    pub async fn open(c: Config) -> Result<Arc<Self>> {
        let lock = local_lock(&c)?;
        let identity: Identity = serde_json::from_slice(
            &std::fs::read(c.local_dir.join("identity.json")).context("volume not initialized")?,
        )?;
        let store = Store::new(&c)?;
        let (b, version) = store.head().await?.context("remote HEAD is missing")?;
        let head = decode(&b)?;
        ensure!(
            (head.format == 2) == c.generation_mode,
            "generation mode must match the volume format; changing durability in place is forbidden"
        );
        ensure!(
            head.volume == identity.volume && head.size == identity.size,
            "local/remote identity mismatch"
        );
        ensure!(
            head.writer == Some(identity.writer),
            "writer has been fenced; refusing to serve old local state"
        );
        let index_objects = crate::index_objects::IndexObjects::open(
            c.local_dir.join("remote-index-cache"),
            c.remote_index_cache_mib * 1024 * 1024,
        )
        .await;
        let mut index = Self::load_index(&store, &head, &c, Some(&index_objects)).await?;
        let mut paths: Vec<_> = std::fs::read_dir(c.local_dir.join("wal"))?
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<_>>()?;
        ensure!(
            paths
                .iter()
                .all(|p| p.extension().is_some_and(|e| e == "wal")),
            "unexpected file in WAL directory"
        );
        paths.sort();
        if c.generation_mode {
            // The verified HEAD is the ONLY recovery root for this format.
            // Never replay a subset of a later generation, even if it looks valid.
            for path in &paths {
                std::fs::remove_file(path)?;
            }
            wal::sync_dir(&c.local_dir.join("wal"))?;
            paths.clear();
            tracing::warn!(
                sequence = head.seq,
                "generation mode: recovered complete remote generation; applications must restart"
            );
        }
        let watermark_path = c.local_dir.join("durable");
        let wm = if c.generation_mode {
            // This local marker is not a recovery authority for format 2.
            // A torn or missing marker must not prevent verified HEAD recovery.
            match std::fs::remove_file(&watermark_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            Watermark::create(&watermark_path)?
        } else {
            Watermark::open(&watermark_path)?
        };
        let mut seq = head.seq;
        let mut committed = if c.generation_mode {
            head.seq
        } else {
            head.seq.max(wm.seq)
        };
        let mut sealed = Vec::new();
        let mut local = HashMap::new();
        let mut dirty = BTreeSet::new();
        let mut pending = 0;
        let mut resident = VecDeque::new();
        let mut resident_bytes = 0;
        for (n, path) in paths.iter().enumerate() {
            let first_seq: u64 = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.split_once('-'))
                .context("invalid WAL filename")?
                .0
                .parse()?;
            let recovered = Segment::open(
                path.clone(),
                identity.volume,
                identity.size,
                n + 1 == paths.len(),
            );
            let (s, records) = match recovered {
                Ok(r) => r,
                Err(err) if first_seq <= head.seq => {
                    // Every published checkpoint seals whole segments. A segment starting
                    // at/before HEAD is completely committed remotely and is only a cache.
                    tracing::warn!(error=%err,path=%path.display(),"discarding damaged committed WAL cache");
                    std::fs::remove_file(path)?;
                    continue;
                }
                Err(err) => return Err(err),
            };
            if c.wal_preallocate {
                s.release_reservation(c.segment_mib * 1024 * 1024)?;
            }
            committed = committed.max(s.commit_seq);
            for row in records {
                if row.seq <= head.seq {
                    continue;
                }
                ensure!(
                    row.seq == seq + 1,
                    "missing WAL sequence: expected {}, found {}",
                    seq + 1,
                    row.seq
                );
                if row.zero_count > 0 {
                    dirty.extend(index.remove_range(row.first..row.first + row.zero_count)?);
                }
                for (j, (r, zero)) in row.pages.into_iter().enumerate() {
                    let p = row.first + j as u64;
                    if zero {
                        index.remove(&p)?;
                    } else {
                        index.insert(p, r)?;
                    }
                    dirty.insert(p / SHARD_PAGES);
                }
                seq = row.seq;
            }
            local.insert(s.id, Arc::new(s.file.try_clone()?));
            if s.last_seq <= head.seq {
                resident_bytes += s.storage_bytes();
                resident.push_back(s);
            } else {
                pending += s.storage_bytes();
                sealed.push(s);
            }
        }
        ensure!(
            seq >= committed,
            "acknowledged durable writes are missing: WAL {}, watermark {}",
            seq,
            committed
        );
        while resident_bytes > c.hot_wal_mib * 1024 * 1024 {
            let Some(segment) = resident.pop_front() else {
                break;
            };
            match std::fs::remove_file(&segment.path) {
                Ok(()) => {
                    resident_bytes -= segment.storage_bytes();
                    local.remove(&segment.id);
                }
                Err(err) => {
                    tracing::warn!(error=%err,"committed WAL cache eviction failed");
                    resident.push_front(segment);
                    break;
                }
            }
        }
        let mut active = Segment::create_with_format(
            &c.local_dir.join("wal"),
            identity.volume,
            seq + 1,
            if c.wal_preallocate {
                c.segment_mib * 1024 * 1024
            } else {
                0
            },
            c.wal_writev,
            crate::wal_pool::format(&c),
        )?;
        if c.wal_fixed_size {
            active.initialize_capacity(crate::wal_pool::capacity(&c))?;
        }
        local.insert(active.id, Arc::new(active.file.try_clone()?));
        let cache = crate::read_cache::ReadCache::new(c.memory_cache_mib * 1024 * 1024);
        let disk_cache = DiskCache::open(
            c.local_dir.join("cache"),
            if c.logical_cache {
                0
            } else {
                c.disk_cache_mib * 1024 * 1024
            },
            c.read_extent_kib * 1024,
        )?;
        let page_cache = if c.logical_cache {
            Some(Arc::new(crate::page_cache::PageCache::open_partitioned(
                &c.local_dir.join("logical-cache"),
                identity.volume,
                c.disk_cache_mib * 1024 * 1024,
                if c.fast_local_reads { 16 } else { 1 },
            )?))
        } else {
            None
        };
        let cache_writer = if c.async_cache {
            page_cache
                .as_ref()
                .map(|cache| {
                    crate::page_cache::CacheWriter::start(
                        cache.clone(),
                        c.cache_queue_mib * 1024 * 1024,
                    )
                })
                .transpose()?
        } else {
            None
        };
        let sealed_lengths = sealed
            .iter()
            .chain(resident.iter())
            .map(|s| (s.id, s.len))
            .collect();
        let wal_pool = if c.checkpoint_pipeline && c.wal_fixed_size {
            Some(crate::wal_pool::WalPool::start(&c)?)
        } else {
            None
        };
        let downloads =
            crate::download::Downloads::new(c.download_budget_mib, c.download_max_requests);
        Ok(Arc::new(Self {
            config: c,
            identity,
            store,
            state: Mutex::new(State {
                seq,
                durable: committed,
                index,
                dirty,
                active,
                sealed,
                local,
                sealed_lengths,
                pending,
                resident,
                resident_bytes,
            }),
            watermark: Arc::new(std::sync::Mutex::new(wm)),
            remote: Mutex::new(Remote { head, version }),
            flush_lock: Mutex::new(()),
            checkpoint_lock: Mutex::new(()),
            cache: std::sync::Mutex::new(cache),
            fetch_locks: (0..256).map(|_| Mutex::new(())).collect(),
            downloads,
            disk_cache,
            index_objects,
            page_cache,
            cache_writer,
            wal_pool,
            space_available: Notify::new(),
            poisoned: AtomicBool::new(false),
            _lock: lock,
            started: Instant::now(),
            flush_metrics: FlushMetrics::default(),
            unpublished_since_ns: AtomicU64::new(0),
        }))
    }
    fn healthy(&self) -> Result<()> {
        ensure!(
            !self.poisoned.load(Ordering::Acquire),
            "volume failed closed; inspect logs before restarting"
        );
        Ok(())
    }
    fn fail_closed(&self) {
        self.poisoned.store(true, Ordering::Release);
        self.space_available.notify_waiters();
    }
    fn mark_unpublished(&self) {
        let now = (self.started.elapsed().as_nanos() as u64).max(1);
        let _ =
            self.unpublished_since_ns
                .compare_exchange(0, now, Ordering::AcqRel, Ordering::Acquire);
    }
    fn unpublished_age_ns(&self) -> u64 {
        let start = self.unpublished_since_ns.load(Ordering::Acquire);
        if start == 0 {
            0
        } else {
            (self.started.elapsed().as_nanos() as u64).saturating_sub(start)
        }
    }
    async fn writable_state(&self, additional: u64) -> Result<MutexGuard<'_, State>> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(50);
        loop {
            let notification = self.space_available.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            let s = self.state.lock().await;
            self.healthy()?;
            let required = if s.active.capacity > 0 {
                // Reserve room for a new initialized file during rotation/checkpoint.
                s.pending + s.active.storage_bytes() + s.active.capacity
            } else {
                s.pending + s.active.len + additional
            };
            let lag_ok = !self.config.generation_mode
                || self.unpublished_age_ns()
                    < self.config.generation_max_lag_seconds * 1_000_000_000;
            let pool_reserve = self.wal_pool.as_ref().map(|p| p.reserve_bytes).unwrap_or(0);
            if required + pool_reserve <= self.config.max_pending_mib * 1024 * 1024 && lag_ok {
                return Ok(s);
            }
            drop(s);
            tokio::time::timeout_at(deadline, notification)
                .await
                .context("pending WAL remained full for 50 seconds; remote checkpoint required")?;
        }
    }
    fn bounds(&self, offset: u64, len: usize) -> Result<()> {
        ensure!(
            len <= MAX_IO
                && offset
                    .checked_add(len as u64)
                    .is_some_and(|e| e <= self.identity.size),
            "I/O outside volume or above 8 MiB"
        );
        Ok(())
    }
    async fn page(&self, page: u64, r: Option<Ref>, local: Option<Arc<File>>) -> Result<Bytes> {
        self.page_with_fill(page, r, local, false).await
    }
    async fn page_with_fill(
        &self,
        page: u64,
        r: Option<Ref>,
        local: Option<Arc<File>>,
        fill_must_succeed: bool,
    ) -> Result<Bytes> {
        let version = r.clone();
        if let (Some(cache), Some(version)) = (&self.page_cache, &version) {
            let cache = cache.clone();
            let version = version.clone();
            if let Some(b) = tokio::task::spawn_blocking(move || cache.get(page, &version)).await? {
                self.flush_metrics.page_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(b);
            }
        }
        let b = self.extent_page(r, local).await?;
        if let (Some(cache), Some(version)) = (&self.page_cache, version) {
            if let Some(writer) = &self.cache_writer {
                // Extent slices may retain up to 260 KiB for a 4 KiB page.
                // Own exactly the charged payload before queueing the fill.
                writer.enqueue(page, vec![(page, version)], Bytes::copy_from_slice(&b));
                return Ok(b);
            }
            let cache = cache.clone();
            let data = b.clone();
            if let Err(err) =
                tokio::task::spawn_blocking(move || cache.put(page, &version, &data)).await?
            {
                if fill_must_succeed {
                    return Err(err.context("offline cache warm fill failed"));
                }
                tracing::warn!(error=%err,"disposable page cache fill failed");
            }
        }
        Ok(b)
    }
    async fn extent_page(&self, r: Option<Ref>, local: Option<Arc<File>>) -> Result<Bytes> {
        let Some(r) = r else {
            return Ok(Bytes::from_static(&[0; PAGE]));
        };
        if let Some(f) = local {
            match Segment::read(&f, &r) {
                Ok(b) => return Ok(b.into()),
                Err(err) if r.segment_len == 0 => {
                    self.fail_closed();
                    return Err(err);
                }
                Err(err) => {
                    tracing::warn!(error=%err,segment=%r.segment,"sealed local page damaged; attempting verified remote copy")
                }
            }
        }
        if r.segment_len == 0 {
            self.fail_closed();
            anyhow::bail!("unpublished local page is missing");
        }
        let extent = self.config.read_extent_kib * 1024;
        let start = r.offset / extent * extent;
        let k = (r.segment, start);
        let offset = (r.offset - start) as usize;
        let valid = |b: &Bytes| {
            offset + PAGE <= b.len() && crc32fast::hash(&b[offset..offset + PAGE]) == r.crc
        };
        let cached = self.cache.lock().unwrap().get(&k).cloned();
        if let Some(b) = cached.filter(valid) {
            return Ok(b.slice(offset..offset + PAGE));
        }
        let stripe = ((r.segment.as_u128() as u64 ^ (start / extent)) % 256) as usize;
        let _guard = self.fetch_locks[stripe].lock().await;
        let cached = self.cache.lock().unwrap().get(&k).cloned();
        if let Some(b) = cached.filter(valid) {
            return Ok(b.slice(offset..offset + PAGE));
        }
        self.cache.lock().unwrap().pop(&k);
        if let Some(b) = self.disk_cache.get(k).await.filter(valid) {
            self.cache.lock().unwrap().put(k, b.clone());
            return Ok(b.slice(offset..offset + PAGE));
        }
        self.disk_cache.remove(k);
        let end = (start + extent + PAGE as u64).min(r.segment_len);
        let _permit = self.downloads.acquire((end - start) as usize).await?;
        let b = self
            .store
            .range(&format!("segments/{}", r.segment), start..end)
            .await?;
        self.flush_metrics
            .remote_gets
            .fetch_add(1, Ordering::Relaxed);
        self.flush_metrics
            .remote_bytes
            .fetch_add(b.len() as u64, Ordering::Relaxed);
        ensure!(valid(&b), "remote page checksum mismatch");
        self.disk_cache.put(k, &b).await;
        self.cache.lock().unwrap().put(k, b.clone());
        // Do not keep a full downloaded range alive in callers after admission
        // ends. The separate RAM LRU may retain its own budgeted copy.
        Ok(Bytes::copy_from_slice(&b[offset..offset + PAGE]))
    }
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.healthy()?;
        self.bounds(offset, len)?;
        if len == 0 {
            return Ok(Vec::new());
        }
        if self.config.fast_local_reads {
            return self.read_buffer(offset, len, vec![0; len]).await;
        }
        let first = offset / PAGE as u64;
        let end = (offset + len as u64).div_ceil(PAGE as u64);
        let refs: Vec<_> = {
            let mut s = self.state.lock().await;
            (first..end)
                .map(|p| {
                    let r = s.reference(p)?;
                    let f = r.as_ref().and_then(|r| s.local.get(&r.segment)).cloned();
                    Ok::<_, anyhow::Error>((p, r, f))
                })
                .collect::<Result<Vec<_>>>()
                .inspect_err(|_| self.fail_closed())?
        };
        let pages: Vec<Bytes> = stream::iter(refs.into_iter().map(|(p, r, f)| self.page(p, r, f)))
            .buffered(32)
            .try_collect()
            .await?;
        let mut output = Vec::with_capacity(pages.len() * PAGE);
        for p in pages {
            output.extend_from_slice(&p);
        }
        let start = (offset % PAGE as u64) as usize;
        Ok(output[start..start + len].to_vec())
    }

    /// Adapt to physical locality, not merely the size of a logical request.
    /// Sixteen requested pages in a 256 KiB region amortize a large GET. Sparse
    /// regions remain 16 KiB, including fragmented large logical reads.
    fn read_groups(misses: Vec<ReadMiss>) -> Result<Vec<ReadGroup>> {
        let mut regions = BTreeMap::<(Uuid, u64), Vec<(u64, Ref)>>::new();
        for (page, reference, _) in misses {
            let r = reference.context("missing reference in remote read")?;
            ensure!(r.segment_len > 0, "unpublished local page is missing");
            regions
                .entry((r.segment, r.offset / LARGE_READ * LARGE_READ))
                .or_default()
                .push((page, r));
        }
        let mut groups = Vec::new();
        for ((segment, start), pages) in regions {
            if pages.len() >= 16 {
                groups.push(ReadGroup {
                    segment,
                    start,
                    extent: LARGE_READ,
                    pages,
                });
            } else {
                let mut small = BTreeMap::<u64, Vec<(u64, Ref)>>::new();
                for (page, r) in pages {
                    small
                        .entry(r.offset / SMALL_READ * SMALL_READ)
                        .or_default()
                        .push((page, r));
                }
                groups.extend(small.into_iter().map(|(start, pages)| ReadGroup {
                    segment,
                    start,
                    extent: SMALL_READ,
                    pages,
                }));
            }
        }
        Ok(groups)
    }

    async fn fetch_group(&self, group: ReadGroup) -> Result<FetchedGroup> {
        let segment_len = group.pages[0].1.segment_len;
        ensure!(
            group
                .pages
                .iter()
                .all(|(_, r)| r.segment == group.segment && r.segment_len == segment_len),
            "inconsistent remote read group"
        );
        let valid = |start: u64, data: &Bytes| {
            group.pages.iter().all(|(_, r)| {
                r.offset
                    .checked_sub(start)
                    .and_then(|n| usize::try_from(n).ok())
                    .and_then(|n| data.get(n..n.saturating_add(PAGE)))
                    .is_some_and(|page| crc32fast::hash(page) == r.crc)
            })
        };
        // Both sizes share a lock for their containing 256 KiB region. A small
        // read can reuse a preceding large fetch without another network call.
        let large_start = group.start / LARGE_READ * LARGE_READ;
        let stripe = ((group.segment.as_u128() as u64 ^ (large_start / LARGE_READ)) % 256) as usize;
        let guard = self.fetch_locks[stripe].lock().await;
        let mut cached = None;
        {
            let mut cache = self.cache.lock().unwrap();
            for start in [large_start, group.start] {
                if let Some(bytes) = cache.get(&(group.segment, start)).cloned()
                    && valid(start, &bytes)
                {
                    cached = Some((start, bytes));
                    break;
                }
            }
        }
        let mut permit = None;
        let (start, data) = if let Some(cached) = cached {
            cached
        } else {
            let end = group
                .start
                .saturating_add(group.extent + PAGE as u64)
                .min(segment_len);
            permit = self.downloads.acquire((end - group.start) as usize).await?;
            let bytes = self
                .store
                .range(&format!("segments/{}", group.segment), group.start..end)
                .await?;
            self.flush_metrics
                .remote_gets
                .fetch_add(1, Ordering::Relaxed);
            self.flush_metrics
                .remote_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            if group.extent == LARGE_READ {
                self.flush_metrics
                    .adaptive_large_gets
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.flush_metrics
                    .adaptive_small_gets
                    .fetch_add(1, Ordering::Relaxed);
            }
            ensure!(
                valid(group.start, &bytes),
                "remote page checksum mismatch in adaptive read"
            );
            self.cache
                .lock()
                .unwrap()
                .put((group.segment, group.start), bytes.clone());
            (group.start, bytes)
        };
        drop(guard);
        // Validate every requested page before returning or caching any of them.
        // The owned range survives even with a disabled RAM cache.
        let pages: Vec<_> = group
            .pages
            .iter()
            .map(|(page, r)| {
                let offset = (r.offset - start) as usize;
                (*page, data.slice(offset..offset + PAGE))
            })
            .collect();
        if let Some(writer) = &self.cache_writer {
            let mut packed = Vec::with_capacity(pages.len() * PAGE);
            for (_, bytes) in &pages {
                packed.extend_from_slice(bytes);
            }
            writer.enqueue_packed(group.pages, packed.into());
        } else if let Some(cache) = &self.page_cache {
            let cache = cache.clone();
            let versions = group.pages;
            let payload = pages.clone();
            let result = tokio::task::spawn_blocking(move || {
                for ((page, version), (_, bytes)) in versions.iter().zip(&payload) {
                    cache.put(*page, version, bytes)?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?;
            if let Err(error) = result {
                tracing::warn!(%error, "disposable grouped cache fill failed");
            }
        }
        Ok(FetchedGroup {
            pages,
            _permit: permit,
        })
    }
    /// Owned buffer crosses the local I/O worker once, including ublk buffers.
    /// Only partial edge pages need a scratch page; full pages go to the caller.
    pub async fn read_buffer<B: AsMut<[u8]> + Send + 'static>(
        &self,
        offset: u64,
        len: usize,
        mut output: B,
    ) -> Result<B> {
        self.healthy()?;
        self.bounds(offset, len)?;
        ensure!(output.as_mut().len() >= len, "read output buffer too short");
        if len == 0 {
            return Ok(output);
        }
        let first = offset / PAGE as u64;
        let end = (offset + len as u64).div_ceil(PAGE as u64);
        let refs: Vec<_> = {
            let mut s = self.state.lock().await;
            (first..end)
                .map(|p| {
                    let r = s.reference(p)?;
                    let file = r.as_ref().and_then(|r| s.local.get(&r.segment)).cloned();
                    Ok::<_, anyhow::Error>((p, r, file))
                })
                .collect::<Result<Vec<_>>>()
                .inspect_err(|_| self.fail_closed())?
        };
        let cache = self.page_cache.clone();
        let (mut output, misses, hits) = tokio::task::spawn_blocking(move || {
            let mut misses = Vec::new();
            let mut hits = 0_u64;
            let mut scratch = [0; PAGE];
            for (p, r, file) in refs {
                let page_start = p * PAGE as u64;
                let begin = page_start.max(offset);
                let finish = (page_start + PAGE as u64).min(offset + len as u64);
                let dest_start = (begin - offset) as usize;
                let count = (finish - begin) as usize;
                let partial = count != PAGE;
                let dest = if partial {
                    &mut scratch[..]
                } else {
                    &mut output.as_mut()[dest_start..dest_start + PAGE]
                };
                let found = if let Some(version) = &r {
                    if cache
                        .as_ref()
                        .is_some_and(|cache| cache.get_into(p, version, dest))
                    {
                        hits += 1;
                        true
                    } else if let Some(file) = &file {
                        if file.read_exact_at(dest, version.offset).is_ok()
                            && crc32fast::hash(dest) == version.crc
                        {
                            true
                        } else if version.segment_len == 0 {
                            anyhow::bail!("unpublished local page damaged");
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    dest.fill(0);
                    true
                };
                if found && partial {
                    let source = (begin - page_start) as usize;
                    output.as_mut()[dest_start..dest_start + count]
                        .copy_from_slice(&scratch[source..source + count]);
                }
                if !found {
                    misses.push((p, r, file));
                }
            }
            Ok::<_, anyhow::Error>((output, misses, hits))
        })
        .await
        .inspect_err(|_| self.fail_closed())?
        .inspect_err(|_| self.fail_closed())?;
        self.flush_metrics
            .page_hits
            .fetch_add(hits, Ordering::Relaxed);
        if self.config.adaptive_reads {
            let groups = Self::read_groups(misses).inspect_err(|_| self.fail_closed())?;
            let mut fetched = stream::iter(groups.into_iter().map(|group| self.fetch_group(group)))
                .buffer_unordered(8);
            while let Some(group) = fetched.try_next().await? {
                for (p, bytes) in group.pages {
                    let start = (p * PAGE as u64).max(offset);
                    let end = ((p + 1) * PAGE as u64).min(offset + len as u64);
                    let source = (start - p * PAGE as u64) as usize;
                    output.as_mut()[(start - offset) as usize..(end - offset) as usize]
                        .copy_from_slice(&bytes[source..source + (end - start) as usize]);
                }
            }
            return Ok(output);
        }
        let mut fetched = stream::iter(misses.into_iter().map(|(p, r, f)| async move {
            Ok::<_, anyhow::Error>((p, self.page(p, r, f).await?))
        }))
        .buffer_unordered(32);
        while let Some((p, bytes)) = fetched.try_next().await? {
            let start = (p * PAGE as u64).max(offset);
            let end = ((p + 1) * PAGE as u64).min(offset + len as u64);
            let source = (start - p * PAGE as u64) as usize;
            output.as_mut()[(start - offset) as usize..(end - offset) as usize]
                .copy_from_slice(&bytes[source..source + (end - start) as usize]);
        }
        Ok(output)
    }
    pub async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.healthy()?;
        self.bounds(offset, data.len())?;
        if data.is_empty() {
            return Ok(());
        }
        let first = offset / PAGE as u64;
        let end = (offset + data.len() as u64).div_ceil(PAGE as u64);
        let mut s = self
            .writable_state(
                (end - first) * PAGE as u64
                    + crate::wal_pool::format(&self.config).record_overhead() as u64,
            )
            .await?;
        let mut pages = vec![0; (end - first) as usize * PAGE];
        let start = (offset % PAGE as u64) as usize;
        if start != 0 || !data.len().is_multiple_of(PAGE) {
            for p in [first, end - 1] {
                let r = s.reference(p).inspect_err(|_| self.fail_closed())?;
                let f = r.as_ref().and_then(|r| s.local.get(&r.segment)).cloned();
                let b = self.page(p, r, f).await?;
                let dest = (p - first) as usize * PAGE;
                pages[dest..dest + PAGE].copy_from_slice(&b);
            }
        }
        pages[start..start + data.len()].copy_from_slice(data);
        let mut added = 0;
        for (j, b) in pages.as_chunks::<PAGE>().0.iter().enumerate() {
            if !b.iter().all(|v| *v == 0)
                && !s
                    .index
                    .contains_key(&(first + j as u64))
                    .inspect_err(|_| self.fail_closed())?
            {
                added += 1;
            }
        }
        s.index.check_additional_pages(added)?;
        let seq = s.seq.checked_add(1).context("sequence exhausted")?;
        let refs = match s.active.append(seq, first, &pages) {
            Ok(r) => r,
            Err(e) => {
                self.fail_closed();
                return Err(e.context("WAL append failed; volume poisoned"));
            }
        };
        for (j, (r, zero)) in refs.into_iter().enumerate() {
            let p = first + j as u64;
            if zero {
                s.index.remove(&p).inspect_err(|_| self.fail_closed())?;
            } else {
                s.index.insert(p, r).inspect_err(|_| self.fail_closed())?;
            }
            s.dirty.insert(p / SHARD_PAGES);
        }
        s.seq = seq;
        self.mark_unpublished();
        let mut fills = Vec::new();
        if self.page_cache.is_some() {
            for p in first..end {
                if let Some(r) = s.index.get(&p).inspect_err(|_| self.fail_closed())? {
                    fills.push((p, r));
                }
            }
        }
        if s.active.len >= self.config.segment_mib * 1024 * 1024 {
            self.rotate(&mut s)?;
        }
        drop(s);
        if let Some(writer) = &self.cache_writer {
            writer.enqueue(first, fills, pages.into());
            return Ok(());
        }
        if let Some(cache) = &self.page_cache {
            let cache = cache.clone();
            let result = tokio::task::spawn_blocking(move || {
                for (p, r) in fills {
                    let start = (p - first) as usize * PAGE;
                    cache.put(p, &r, &pages[start..start + PAGE])?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?;
            if let Err(err) = result {
                tracing::warn!(error=%err,"disposable write cache fill failed");
            }
        }
        Ok(())
    }
    fn rotate(&self, s: &mut State) -> Result<()> {
        let prepared = self.wal_pool.as_ref().and_then(|pool| pool.take());
        let next = if let Some(prepared) = prepared {
            prepared
                .activate(
                    &self.config.local_dir.join("wal"),
                    self.identity.volume,
                    s.seq + 1,
                )
                .inspect_err(|_| self.fail_closed())?
        } else {
            let mut next = Segment::create_with_format(
                &self.config.local_dir.join("wal"),
                self.identity.volume,
                s.seq + 1,
                if self.config.wal_preallocate {
                    self.config.segment_mib * 1024 * 1024
                } else {
                    0
                },
                self.config.wal_writev,
                crate::wal_pool::format(&self.config),
            )
            .inspect_err(|_| self.fail_closed())?;
            if self.config.wal_fixed_size {
                next.initialize_capacity(crate::wal_pool::capacity(&self.config))
                    .inspect_err(|_| self.fail_closed())?;
            }
            next
        };
        s.local.insert(next.id, Arc::new(next.file.try_clone()?));
        let old = std::mem::replace(&mut s.active, next);
        s.sealed_lengths.insert(old.id, old.len);
        if self.config.wal_preallocate {
            old.release_reservation(self.config.segment_mib * 1024 * 1024)
                .inspect_err(|_| self.fail_closed())?;
        }
        s.pending += old.storage_bytes();
        s.sealed.push(old);
        Ok(())
    }
    pub async fn zero(&self, offset: u64, len: u64) -> Result<()> {
        self.healthy()?;
        ensure!(
            offset
                .checked_add(len)
                .is_some_and(|n| n <= self.identity.size),
            "zero range outside volume"
        );
        if len == 0 {
            return Ok(());
        }
        let end = offset + len;
        let first = offset.div_ceil(PAGE as u64);
        let last = end / PAGE as u64;
        if first >= last {
            // At most two pages for a range with no complete interior page.
            self.write(offset, &vec![0; len as usize]).await?;
            return Ok(());
        }
        if !offset.is_multiple_of(PAGE as u64) {
            self.write(offset, &vec![0; (first * PAGE as u64 - offset) as usize])
                .await?;
        }
        if !end.is_multiple_of(PAGE as u64) {
            self.write(
                last * PAGE as u64,
                &vec![0; (end - last * PAGE as u64) as usize],
            )
            .await?;
        }
        let mut s = self
            .writable_state(crate::wal_pool::format(&self.config).record_overhead() as u64)
            .await?;
        let seq = s.seq.checked_add(1).context("sequence exhausted")?;
        if let Err(err) = s.active.zero(seq, first, last - first) {
            self.fail_closed();
            return Err(err);
        }
        let changed = s
            .index
            .remove_range(first..last)
            .inspect_err(|_| self.fail_closed())?;
        s.dirty.extend(changed);
        s.seq = seq;
        self.mark_unpublished();
        if s.active.len >= self.config.segment_mib * 1024 * 1024 {
            self.rotate(&mut s)?;
        }
        Ok(())
    }
    pub async fn flush(&self) -> Result<()> {
        if self.config.generation_mode {
            // Taking State orders the barrier after completed writes. A later
            // checkpoint captures an entire prefix; no disk durability is claimed.
            drop(self.writable_state(0).await?);
            return Ok(());
        }
        self.flush_durable().await
    }
    async fn flush_durable(&self) -> Result<()> {
        self.healthy()?;
        self.flush_metrics.calls.fetch_add(1, Ordering::Relaxed);
        let target = self.state.lock().await.seq;
        let wait = Instant::now();
        let _guard = self.flush_lock.lock().await;
        // A previous barrier may have failed while this request was queued.
        self.healthy()?;
        self.flush_metrics
            .wait_ns
            .fetch_add(wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if self.state.lock().await.durable >= target {
            return Ok(());
        }
        // Concurrent connections share one fsync group and one durability frontier.
        if self.config.flush_batch_us > 0 {
            tokio::time::sleep(std::time::Duration::from_micros(self.config.flush_batch_us)).await;
        } else {
            tokio::task::yield_now().await;
        }
        let (seq, files) = {
            let mut s = self.state.lock().await;
            let seq = s.seq;
            if self.config.wal_commit_records {
                s.active.commit(seq).inspect_err(|_| self.fail_closed())?;
            }
            (
                seq,
                s.sealed
                    .iter()
                    .filter(|segment| !self.config.selective_sync || segment.last_seq > s.durable)
                    .map(|s| s.file.try_clone())
                    .chain(std::iter::once(s.active.file.try_clone()))
                    .collect::<std::io::Result<Vec<_>>>()?,
            )
        };
        let wm = self.watermark.clone();
        let data_only = self.config.sync_data_only;
        let commit_records = self.config.wal_commit_records;
        let r: Result<(u64, u64)> = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            for f in files {
                if data_only {
                    f.sync_data()?;
                } else {
                    f.sync_all()?;
                }
            }
            let wal_ns = start.elapsed().as_nanos() as u64;
            let start = Instant::now();
            let mut wm = wm.lock().unwrap();
            if commit_records { /* the validated commit marker is in the synchronized WAL */
            } else if data_only {
                wm.persist_data(seq)?;
            } else {
                wm.persist(seq)?;
            }
            Ok((wal_ns, start.elapsed().as_nanos() as u64))
        })
        .await
        .inspect_err(|_| self.fail_closed())?;
        let (wal_ns, watermark_ns) = r
            .inspect_err(|_| self.fail_closed())
            .context("local durability failed")?;
        self.flush_metrics.groups.fetch_add(1, Ordering::Relaxed);
        self.flush_metrics
            .wal_ns
            .fetch_add(wal_ns, Ordering::Relaxed);
        self.flush_metrics
            .watermark_ns
            .fetch_add(watermark_ns, Ordering::Relaxed);
        self.state.lock().await.durable = seq;
        Ok(())
    }
    pub async fn checkpoint(&self) -> Result<()> {
        self.healthy()?;
        let _guard = self.checkpoint_lock.lock().await;
        self.flush_durable().await?;
        let capture_ns = (self.started.elapsed().as_nanos() as u64).max(1);
        let (seq, segments, snapshots, dirty, lengths, sync_files, sources) = {
            let mut s = self.state.lock().await;
            if s.dirty.is_empty() && s.sealed.is_empty() && s.active.is_empty() {
                return Ok(());
            }
            if !s.active.is_empty() {
                self.rotate(&mut s)?;
            }
            let sync_files = s
                .sealed
                .iter()
                .filter(|segment| !self.config.selective_sync || segment.last_seq > s.durable)
                .map(|f| f.file.try_clone())
                .collect::<std::io::Result<Vec<_>>>()?;
            let lengths = s.sealed_lengths.clone();
            let dirty = std::mem::take(&mut s.dirty);
            let shards = s
                .index
                .snapshot(dirty.iter().copied())
                .inspect_err(|_| self.fail_closed())?;
            (
                s.seq,
                s.sealed
                    .iter()
                    .map(|s| (s.id, s.path.clone(), s.len, s.last_seq, s.capacity))
                    .collect::<Vec<_>>(),
                shards,
                dirty,
                lengths,
                sync_files,
                Arc::new(
                    s.sealed
                        .iter()
                        .map(|segment| (segment.id, s.local[&segment.id].clone()))
                        .collect::<HashMap<_, _>>(),
                ),
            )
        };
        let result = async {
            // Sealed files cannot be changed. All disk barriers and serialization
            // run outside State; new writes use the next segment.
            tokio::task::spawn_blocking(move || {
                for file in sync_files {file.sync_all()?;}
                Ok::<_, anyhow::Error>(())
            }).await.inspect_err(|_| self.fail_closed())?.inspect_err(|_| self.fail_closed())?;
            let uploads =
                segments
                    .clone()
                    .into_iter()
                    .map(|(id, path, len, last_seq, capacity)| {
                        let store = self.store.clone();
                        async move {
                            use tokio::io::AsyncReadExt;
                            let mut file = tokio::fs::File::open(path)
                                .await
                                .inspect_err(|_| self.fail_closed())?;
                            let physical = file
                                .metadata()
                                .await
                                .inspect_err(|_| self.fail_closed())?
                                .len();
                            if physical != if capacity > 0 { capacity } else { len } {
                                self.fail_closed();
                                anyhow::bail!("sealed WAL physical length changed");
                            }
                            let mut b = vec![0; len as usize];
                            file.read_exact(&mut b)
                                .await
                                .inspect_err(|_| self.fail_closed())?;
                            let volume = self.identity.volume;
                            let size = self.identity.size;
                            let checked = tokio::task::spawn_blocking(move || {
                                ensure!(b.len() as u64 == len, "sealed WAL length changed");
                                wal::validate_upload(&b, volume, id, last_seq, size)?;
                                Ok::<_, anyhow::Error>(b)
                            })
                            .await
                            .inspect_err(|_| self.fail_closed())?;
                            let b = checked.inspect_err(|_| self.fail_closed())?;
                            self.flush_metrics.checkpoint_wal_bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
                            if self.config.compact_checkpoints {Ok(())} else {
                                let len = b.len();
                                store.immutable(&format!("segments/{id}"), b.into()).await?;
                                self.flush_metrics.uploaded_segment_bytes.fetch_add(len as u64, Ordering::Relaxed);
                                Ok(())
                            }
                        }
                    });
            stream::iter(uploads)
                .buffer_unordered(4)
                .try_collect::<Vec<_>>()
                .await?;
            let lengths = Arc::new(lengths);
            let uploaded: Vec<(u64, Option<Shard>)> = stream::iter(snapshots.into_iter().map(|(id, snapshot)| {
                let store = self.store.clone();
                let lengths = lengths.clone();
                let sources = sources.clone();
                async move {
                    let compact = self.config.compact_checkpoints;
                    let volume = self.identity.volume;
                    let size = self.identity.size;
                    let (map, replacements, packed) = tokio::task::spawn_blocking(move || {
                        let mut map = (*snapshot.load()?).clone();
                        let mut replacements = Vec::new();
                        for (&page, r) in &mut map {
                            if let Some(len) = lengths.get(&r.segment)
                                && r.segment_len != *len {
                                    let old = r.clone();
                                    r.segment_len = *len;
                                    replacements.push((page, old, r.clone()));
                            }
                            ensure!(r.segment_len > 0, "checkpoint contains an unsealed reference");
                        }
                        let mut pages = Vec::new();
                        if compact {
                            for (&page, r) in &map {
                                if let Some(file) = sources.get(&r.segment) {pages.push((page, Segment::read(file, r)?));}
                            }
                        }
                        let packed = if pages.is_empty() {None} else {
                            let (segment, bytes, refs) = wal::pack_pages(volume, &pages, size)?;
                            for (p, new) in refs {
                                let old = map.insert(p, new.clone()).context("compact reference missing")?;
                                replacements.push((p, old, new));
                            }
                            Some((segment, bytes))
                        };
                        Ok::<_, anyhow::Error>((map, replacements, packed))
                    }).await.inspect_err(|_| self.fail_closed())?.inspect_err(|_| self.fail_closed())?;
                    if map.is_empty() {
                        return Ok((id, None));
                    }
                    if let Some((segment, bytes)) = packed {
                        let len = bytes.len();
                        store.immutable(&format!("segments/{segment}"), bytes.into()).await?;
                        self.flush_metrics.uploaded_segment_bytes.fetch_add(len as u64, Ordering::Relaxed);
                    }
                    // At most four serialized shards/packed objects are live. We
                    // never collect all dirty payloads in memory before uploading.
                    let b = bincode::serialize(&map)?;
                    let hash = hex::encode(Sha256::digest(&b));
                    let key = format!("indexes/{id}/{}", Uuid::new_v4());
                    let b = Bytes::from(b);
                    store.immutable(&key, b.clone()).await?;
                    self.index_objects.put(volume, id, &key, &hash, b).await;
                    if !replacements.is_empty() {
                        let mut state = self.state.lock().await;
                        for (p, old, new) in &replacements {
                            if state.index.get(p).inspect_err(|_| self.fail_closed())?.is_some_and(|r| (r.segment, r.offset, r.crc) == (old.segment, old.offset, old.crc)) {
                                state.index.insert(*p, new.clone()).inspect_err(|_| self.fail_closed())?;
                            }
                        }
                        drop(state);
                        if let Some(cache) = self.page_cache.clone() {
                            tokio::task::spawn_blocking(move || {
                                for (p, old, new) in replacements {if let Err(err) = cache.rekey(p, &old, &new) {tracing::warn!(error=%err, "disposable cache rekey failed");}}
                            }).await?;
                        }
                    }
                    Ok::<_, anyhow::Error>((id, Some(Shard { key, hash })))
                }
            })).buffer_unordered(4).try_collect().await?;
            drop(sources);
            let mut remote = self.remote.lock().await;
            let mut h = remote.head.clone();
            for (id, shard) in uploaded {
                if let Some(shard) = shard {h.shards.insert(id, shard);} else {h.shards.remove(&id);}
            }
            h.seq = seq;
            h.generation += 1;
            let version = match self
                .store
                .cas(encode(&h)?, Some(remote.version.clone()))
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    // A failed transport may have committed HEAD. Reconcile the exact candidate.
                    if let Ok(Some((b, v))) = self.store.head().await {
                        if b == encode(&h)? {
                            v
                        } else {
                            if b != encode(&remote.head)? {
                                self.fail_closed();
                            }
                            return Err(e.context("remote HEAD CAS failed"));
                        }
                    } else {
                        return Err(e.context("remote HEAD CAS outcome unknown; WAL retained"));
                    }
                }
            };
            remote.head = h;
            remote.version = version;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(e) = result {
            self.state.lock().await.dirty.extend(dirty);
            return Err(e);
        }
        let mut s = self.state.lock().await;
        self.unpublished_since_ns
            .store(if s.seq > seq { capture_ns } else { 0 }, Ordering::Release);
        let mut retained = Vec::new();
        let sealed = std::mem::take(&mut s.sealed);
        for segment in sealed {
            if segment.last_seq <= seq {
                // Every captured live reference now carries its sealed length.
                // Concurrent overwrites point at later segments; this side map
                // must not grow with the lifetime number of WAL rotations.
                s.sealed_lengths.remove(&segment.id);
                // Keep recent writes on SSD after publication instead of making their
                // first read pay an S3 GET. This cache is bounded and disposable.
                s.pending -= segment.storage_bytes();
                s.resident_bytes += segment.storage_bytes();
                s.resident.push_back(segment);
            } else {
                retained.push(segment);
            }
        }
        s.sealed = retained;
        self.space_available.notify_waiters();
        let mut evicted = Vec::new();
        while s.resident_bytes > self.config.hot_wal_mib * 1024 * 1024 {
            let Some(segment) = s.resident.pop_front() else {
                break;
            };
            let readers = s
                .local
                .remove(&segment.id)
                .context("resident segment missing file")?;
            s.resident_bytes -= segment.storage_bytes();
            evicted.push((segment, readers));
        }
        drop(s);
        for (segment, readers) in evicted {
            let (segment, readers) = if let Some(pool) = &self.wal_pool
                && segment.storage_bytes() <= pool.unit_bytes
            {
                match wal::RetiredSegment::try_retire(segment, readers, seq, &pool.directory)
                    .inspect_err(|_| self.fail_closed())?
                {
                    wal::Retirement::Ready(retired) => {
                        pool.offer(retired);
                        continue;
                    }
                    // Unlink published files still held by readers instead of
                    // retaining an over-budget cache until another checkpoint.
                    // Their open descriptors stay valid, but are never reused.
                    wal::Retirement::Busy { segment, readers } => (segment, readers),
                }
            } else {
                (segment, readers)
            };
            if let Err(err) = std::fs::remove_file(&segment.path) {
                tracing::warn!(error=%err, "local WAL cache eviction failed");
                let mut s = self.state.lock().await;
                s.resident_bytes += segment.storage_bytes();
                s.local.insert(segment.id, readers);
                s.resident.push_back(segment);
            }
        }
        wal::sync_dir(&self.config.local_dir.join("wal"))?;
        Ok(())
    }
    pub async fn status(&self) -> Status {
        let s = self.state.lock().await;
        let r = self.remote.lock().await;
        let (cache_queue_bytes, cache_fills_skipped, cache_fill_errors) = self
            .cache_writer
            .as_ref()
            .map(|w| w.status())
            .unwrap_or_default();
        Status {
            volume: self.identity.volume,
            size: self.identity.size,
            sequence: s.seq,
            local_durable_sequence: s.durable,
            remote_sequence: r.head.seq,
            remote_generation: r.head.generation,
            pending_bytes: s.pending + s.active.storage_bytes(),
            allocated_pages: s.index.len(),
            poisoned: self.poisoned.load(Ordering::Acquire),
            uptime_seconds: self.started.elapsed().as_secs(),
            hot_wal_bytes: s.resident_bytes,
            flush_calls: self.flush_metrics.calls.load(Ordering::Relaxed),
            flush_groups: self.flush_metrics.groups.load(Ordering::Relaxed),
            flush_wait_ns: self.flush_metrics.wait_ns.load(Ordering::Relaxed),
            wal_sync_ns: self.flush_metrics.wal_ns.load(Ordering::Relaxed),
            watermark_sync_ns: self.flush_metrics.watermark_ns.load(Ordering::Relaxed),
            logical_cache_hits: self.flush_metrics.page_hits.load(Ordering::Relaxed),
            remote_gets: self.flush_metrics.remote_gets.load(Ordering::Relaxed),
            remote_bytes: self.flush_metrics.remote_bytes.load(Ordering::Relaxed),
            range_cache_bytes: self.cache.lock().unwrap().bytes(),
            downloads: self.downloads.status(),
            adaptive_small_gets: self
                .flush_metrics
                .adaptive_small_gets
                .load(Ordering::Relaxed),
            adaptive_large_gets: self
                .flush_metrics
                .adaptive_large_gets
                .load(Ordering::Relaxed),
            checkpoint_wal_bytes: self
                .flush_metrics
                .checkpoint_wal_bytes
                .load(Ordering::Relaxed),
            uploaded_segment_bytes: self
                .flush_metrics
                .uploaded_segment_bytes
                .load(Ordering::Relaxed),
            index: s.index.stats(),
            index_object_cache: self.index_objects.status(),
            wal_pool_bytes: self.wal_pool.as_ref().map(|p| p.bytes()).unwrap_or(0),
            cache_queue_bytes,
            cache_fills_skipped,
            cache_fill_errors,
            durability_mode: if self.config.generation_mode {
                "remote-generation-rollback"
            } else {
                "local-fsync"
            },
            unpublished_age_ms: self.unpublished_age_ns() / 1_000_000,
        }
    }
    /// Offline only: opening takes the exclusive volume lock.
    pub async fn warm(c: Config) -> Result<usize> {
        Self::warm_with_concurrency(c, 128).await
    }
    /// Concurrency limits physical groups, including their cache probes/fills.
    pub async fn warm_with_concurrency(mut c: Config, concurrency: usize) -> Result<usize> {
        ensure!(
            (1..=128).contains(&concurrency),
            "warm concurrency must be between 1 and 128"
        );
        ensure!(c.logical_cache, "warm requires logical_cache=true");
        // An explicit offline warm must finish every fill. The online queue may
        // skip a disposable fill under pressure, leaving a successful warm cold.
        c.async_cache = false;
        let e = Self::open(c).await?;
        Ok(e.warm_cache(concurrency).await?.pages)
    }
    async fn warm_cache(self: &Arc<Self>, concurrency: usize) -> Result<WarmResult> {
        ensure!(
            (1..=128).contains(&concurrency),
            "warm concurrency must be between 1 and 128"
        );
        ensure!(
            !self.config.async_cache,
            "offline warm requires synchronous fills"
        );
        let mut refs: Vec<_> = {
            let mut s = self.state.lock().await;
            s.index.entries().inspect_err(|_| self.fail_closed())?
        };
        let count = refs.len();
        ensure!(
            (count as u64) * 4136 <= self.config.disk_cache_mib * 1024 * 1024,
            "allocated pages do not fit the configured logical cache"
        );
        self.page_cache
            .as_ref()
            .context("logical cache unavailable for warm")?
            .ensure_capacity_for(refs.iter().map(|(page, _)| *page))?;
        let extent = self.config.read_extent_kib * 1024;
        ensure!(
            [16, 64, 256].contains(&self.config.read_extent_kib),
            "invalid warm extent size"
        );
        refs.sort_unstable_by_key(|(_, reference)| (reference.segment, reference.offset));
        let mut groups: Vec<WarmGroup> = Vec::new();
        {
            let s = self.state.lock().await;
            for (page, reference) in refs {
                let key = (reference.segment, reference.offset / extent);
                let same = groups.last().is_some_and(|group| {
                    let previous = &group.pages[0].1;
                    (previous.segment, previous.offset / extent) == key
                });
                if !same {
                    groups.push(WarmGroup {
                        local: s.local.get(&reference.segment).cloned(),
                        pages: Vec::new(),
                    });
                }
                groups.last_mut().unwrap().pages.push((page, reference));
            }
        }
        let group_count = groups.len();
        let ranges = WarmRanges::default();
        let mut remaining = groups.into_iter();
        let mut pending = futures::stream::FuturesUnordered::new();
        for group in remaining.by_ref().take(concurrency) {
            pending.push(self.warm_group(group, &ranges));
        }
        let mut completed = 0;
        let mut failure = None;
        let cadence = std::time::Duration::from_secs(5);
        let mut progress = tokio::time::interval_at(tokio::time::Instant::now() + cadence, cadence);
        progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !pending.is_empty() {
            let result = tokio::select! {
                result = pending.next() => result.unwrap(),
                _ = progress.tick() => {
                    tracing::info!(pages_completed = completed, pages_total = count,
                        remote_gets = self.flush_metrics.remote_gets.load(Ordering::Relaxed),
                        remote_bytes = self.flush_metrics.remote_bytes.load(Ordering::Relaxed),
                        range_inflight = ranges.active.load(Ordering::Relaxed),
                        max_range_inflight = ranges.peak.load(Ordering::Relaxed),
                        "offline cache warm progress");
                    continue;
                }
            };
            match result {
                Ok(pages) => completed += pages,
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
            // On error, drain already-started fills before releasing the
            // volume lock. A blocking cache put must not outlive this open.
            if failure.is_none()
                && let Some(group) = remaining.next()
            {
                pending.push(self.warm_group(group, &ranges));
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        let result = WarmResult {
            pages: count,
            range_peak: ranges.peak.load(Ordering::Relaxed),
        };
        tracing::info!(
            pages = count,
            groups = group_count,
            concurrency,
            remote_gets = self.flush_metrics.remote_gets.load(Ordering::Relaxed),
            remote_bytes = self.flush_metrics.remote_bytes.load(Ordering::Relaxed),
            max_range_inflight = result.range_peak,
            "offline cache warm completed"
        );
        Ok(result)
    }
    async fn warm_group(self: &Arc<Self>, group: WarmGroup, ranges: &WarmRanges) -> Result<usize> {
        let count = group.pages.len();
        if let Some(file) = group.local {
            // Retain authoritative local-WAL validation and fail_closed. A
            // published damaged copy may use the existing verified fallback.
            for (page, reference) in group.pages {
                self.page_with_fill(page, Some(reference), Some(file.clone()), true)
                    .await?;
            }
            return Ok(count);
        }
        if group
            .pages
            .iter()
            .any(|(_, reference)| reference.segment_len == 0)
        {
            self.fail_closed();
            anyhow::bail!("unpublished local page is missing");
        }
        let owner = self.clone();
        let missing = tokio::task::spawn_blocking(move || {
            // Keep the engine/LOCK alive even if the caller cancels its future.
            let cache = owner.page_cache.as_ref().unwrap();
            group
                .pages
                .into_iter()
                .filter(|(page, reference)| {
                    if cache.get(*page, reference).is_some() {
                        owner
                            .flush_metrics
                            .page_hits
                            .fetch_add(1, Ordering::Relaxed);
                        false
                    } else {
                        true
                    }
                })
                .collect::<Vec<_>>()
        })
        .await?;
        if missing.is_empty() {
            return Ok(count);
        }
        let first = &missing[0].1;
        let extent = self.config.read_extent_kib * 1024;
        let start = first.offset / extent * extent;
        let end = start
            .saturating_add(extent + PAGE as u64)
            .min(first.segment_len);
        ensure!(
            missing.iter().all(|(_, r)| r.segment == first.segment
                && r.segment_len == first.segment_len
                && r.offset / extent * extent == start),
            "inconsistent offline warm group"
        );
        let active = ranges.start();
        let downloaded = self
            .store
            .range(&format!("segments/{}", first.segment), start..end)
            .await;
        drop(active);
        let bytes = downloaded?;
        self.flush_metrics
            .remote_gets
            .fetch_add(1, Ordering::Relaxed);
        self.flush_metrics
            .remote_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        // Check every required page before putting any member of this range.
        // The owned range survives through the entire blocking fill; RAM=0 is
        // therefore just as efficient as a large online extent cache.
        for (_, reference) in &missing {
            let offset = (reference.offset - start) as usize;
            ensure!(
                offset
                    .checked_add(PAGE)
                    .is_some_and(|end| end <= bytes.len())
                    && crc32fast::hash(&bytes[offset..offset + PAGE]) == reference.crc,
                "remote page checksum mismatch during offline warm"
            );
        }
        let owner = self.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let cache = owner.page_cache.as_ref().unwrap();
            for (page, reference) in missing {
                let offset = (reference.offset - start) as usize;
                cache
                    .put(page, &reference, &bytes[offset..offset + PAGE])
                    .context("offline cache warm fill failed")?;
            }
            Ok(())
        })
        .await??;
        Ok(count)
    }
    /// Offline layout experiment. Old remote objects remain available; CAS is last.
    pub async fn compact(c: Config) -> Result<usize> {
        let e = Self::open(c).await?;
        e.checkpoint().await?;
        let refs: Vec<_> = {
            let mut s = e.state.lock().await;
            s.index
                .entries()?
                .into_iter()
                .map(|(p, r)| {
                    Ok((
                        p,
                        s.reference(p)?.unwrap(),
                        s.local.get(&r.segment).cloned(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?
        };
        let count = refs.len();
        let mut index = BTreeMap::new();
        let dir = e
            .config
            .local_dir
            .join(format!("compact-{}", Uuid::new_v4()));
        std::fs::create_dir(&dir)?;
        let result = async {
            let mut segment = Segment::create(&dir, e.identity.volume, 1)?;
            let mut record_seq = 0;
            for chunk in refs.chunks(256) {
                let pages = stream::iter(
                    chunk
                        .iter()
                        .map(|(p, r, f)| e.page(*p, Some(r.clone()), f.clone())),
                )
                .buffered(32)
                .try_collect::<Vec<_>>()
                .await?;
                for ((p, _, _), b) in chunk.iter().zip(pages) {
                    record_seq += 1;
                    let r = segment.append(record_seq, *p, &b)?.remove(0).0;
                    index.insert(*p, r);
                }
                if segment.len >= e.config.segment_mib * 1024 * 1024 {
                    let b = std::fs::read(&segment.path)?;
                    wal::validate_upload(
                        &b,
                        e.identity.volume,
                        segment.id,
                        segment.last_seq,
                        e.identity.size,
                    )?;
                    for r in index.values_mut().filter(|r| r.segment == segment.id) {
                        r.segment_len = segment.len;
                    }
                    e.store
                        .immutable(&format!("segments/{}", segment.id), b.into())
                        .await?;
                    std::fs::remove_file(&segment.path)?;
                    segment = Segment::create(&dir, e.identity.volume, record_seq + 1)?;
                }
            }
            if segment.len > 64 {
                let b = std::fs::read(&segment.path)?;
                wal::validate_upload(
                    &b,
                    e.identity.volume,
                    segment.id,
                    segment.last_seq,
                    e.identity.size,
                )?;
                for r in index.values_mut().filter(|r| r.segment == segment.id) {
                    r.segment_len = segment.len;
                }
                e.store
                    .immutable(&format!("segments/{}", segment.id), b.into())
                    .await?;
            }
            let mut remote = e.remote.lock().await;
            let mut h = remote.head.clone();
            h.shards.clear();
            for (id, rows) in &index.into_iter().fold(
                BTreeMap::<u64, BTreeMap<u64, Ref>>::new(),
                |mut a, (p, r)| {
                    a.entry(p / SHARD_PAGES).or_default().insert(p, r);
                    a
                },
            ) {
                let b = bincode::serialize(rows)?;
                let hash = hex::encode(Sha256::digest(&b));
                let key = format!("indexes/{id}/{}", Uuid::new_v4());
                let b = Bytes::from(b);
                e.store.immutable(&key, b.clone()).await?;
                e.index_objects
                    .put(e.identity.volume, *id, &key, &hash, b)
                    .await;
                h.shards.insert(*id, Shard { key, hash });
            }
            h.generation += 1;
            e.store
                .cas(encode(&h)?, Some(remote.version.clone()))
                .await?;
            remote.head = h;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        std::fs::remove_dir_all(&dir)?;
        result?;
        Ok(count)
    }
    pub async fn inspect(c: &Config) -> Result<Head> {
        let store = Store::new(c)?;
        decode(&store.head().await?.context("no volume")?.0)
    }
    /// Offline mark-and-sweep. HEAD is atomically fenced with a reserved writer ID
    /// for the entire deletion phase. An interrupted sweep stays fenced until resumed.
    pub async fn gc(c: &Config, apply: bool, min_age_seconds: u64) -> Result<GcReport> {
        use object_store::ObjectStoreExt;
        let _lock = local_lock(c)?;
        let identity: Identity =
            serde_json::from_slice(&std::fs::read(c.local_dir.join("identity.json"))?)?;
        let store = Store::new(c)?;
        let (bytes, mut version) = store.head().await?.context("no volume")?;
        let mut h = decode(&bytes)?;
        ensure!(h.volume == identity.volume, "GC volume identity mismatch");
        let token_path = c.local_dir.join("gc-token.json");
        if h.writer == Some(Uuid::nil()) {
            let token: GcToken = serde_json::from_slice(
                &std::fs::read(&token_path)
                    .context("maintenance token missing; do not clear the remote fence blindly")?,
            )?;
            ensure!(
                apply
                    && token.volume == identity.volume
                    && token.writer == identity.writer
                    && token.generation == h.generation,
                "GC fence belongs to another host or generation"
            );
        } else {
            ensure!(
                h.writer == Some(identity.writer),
                "GC requires the current owner on a stopped volume"
            );
        }
        let mut index = Self::load_index(&store, &h, c, None).await?;
        let mut live: BTreeSet<object_store::path::Path> =
            h.shards.values().map(|s| store.path(&s.key)).collect();
        for (_, r) in index.entries()? {
            live.insert(store.path(&format!("segments/{}", r.segment)));
        }
        if apply && h.writer != Some(Uuid::nil()) {
            let token = GcToken {
                volume: identity.volume,
                writer: identity.writer,
                generation: h.generation + 1,
            };
            wal::atomic(&token_path, &serde_json::to_vec(&token)?)?;
            h.writer = Some(Uuid::nil());
            h.generation += 1;
            version = store
                .cas(encode(&h)?, Some(version))
                .await
                .context("enter offline GC fence")?;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let mut report = GcReport {
            apply,
            candidate_objects: 0,
            candidate_bytes: 0,
            deleted_objects: 0,
        };
        for tree in ["segments", "indexes"] {
            let prefix = store.path(tree);
            let boundary = format!("{prefix}/");
            let mut objects = store.inner.list(Some(&prefix));
            while let Some(object) = objects.try_next().await? {
                if !object.location.as_ref().starts_with(&boundary)
                    || live.contains(&object.location)
                {
                    continue;
                }
                let age = now.saturating_sub(object.last_modified.timestamp());
                if age < 0 || (age as u64) < min_age_seconds {
                    continue;
                }
                report.candidate_objects += 1;
                report.candidate_bytes += object.size;
                if apply {
                    store.inner.delete(&object.location).await?;
                    report.deleted_objects += 1;
                }
            }
        }
        if apply {
            h.writer = Some(identity.writer);
            h.generation += 1;
            store
                .cas(encode(&h)?, Some(version))
                .await
                .context("leave GC fence; if this fails, resume gc --apply")?;
            std::fs::remove_file(token_path)?;
            wal::sync_dir(&c.local_dir)?;
        }
        Ok(report)
    }
    pub async fn verify_remote(c: &Config) -> Result<(u64, usize)> {
        let store = Store::new(c)?;
        let h = decode(&store.head().await?.context("no volume")?.0)?;
        let mut index = Self::load_index(&store, &h, c, None).await?;
        let n = index.len();
        let mut groups = ScrubGroups::new();
        for (_, r) in index.entries()? {
            let start = r.offset / READ_EXTENT * READ_EXTENT;
            let group = groups
                .entry((r.segment, start))
                .or_insert((r.segment_len, Vec::new()));
            ensure!(group.0 == r.segment_len, "inconsistent segment lengths");
            group.1.push((r.offset, r.crc));
        }
        stream::iter(groups.into_iter().map(|((segment, start), (len, pages))| {
            let store = store.clone();
            async move {
                let b = store
                    .range(
                        &format!("segments/{segment}"),
                        start..(start + READ_EXTENT + PAGE as u64).min(len),
                    )
                    .await?;
                for (offset, crc) in pages {
                    let pos = (offset - start) as usize;
                    ensure!(
                        pos + PAGE <= b.len() && crc32fast::hash(&b[pos..pos + PAGE]) == crc,
                        "remote page checksum mismatch"
                    );
                }
                Ok::<_, anyhow::Error>(())
            }
        }))
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await?;
        Ok((h.seq, n))
    }
}

#[derive(Serialize, Deserialize)]
struct GcToken {
    volume: Uuid,
    writer: Uuid,
    generation: u64,
}
#[derive(Serialize)]
pub struct GcReport {
    pub apply: bool,
    pub candidate_objects: u64,
    pub candidate_bytes: u64,
    pub deleted_objects: u64,
}

#[cfg(test)]
#[path = "../tests/support/astra_warm.rs"]
mod warm_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn head_writer_never_publishes_a_root_rejected_by_its_reader_size_limit() -> Result<()> {
        let mut head = Head {
            format: 1,
            volume: Uuid::new_v4(),
            size: PAGE as u64,
            writer: Some(Uuid::new_v4()),
            generation: 0,
            seq: 0,
            shards: BTreeMap::new(),
        };
        let original = encode(&head)?;
        assert_eq!(decode(&original)?.volume, head.volume);
        head.shards.insert(
            0,
            Shard {
                key: "x".repeat(MAX_HEAD_BYTES as usize),
                hash: "0".repeat(64),
            },
        );
        assert!(
            encode(&head)
                .unwrap_err()
                .to_string()
                .contains("metadata size")
        );
        Ok(())
    }
    fn config(t: &tempfile::TempDir) -> Config {
        Config {
            local_dir: t.path().join("local"),
            store: format!("file://{}", t.path().join("remote").display()),
            segment_mib: 1,
            hot_wal_mib: 0,
            ..Config::default()
        }
    }
    #[tokio::test]
    async fn local_and_remote_recovery_partial_writes_and_trim() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![5; PAGE * 2]).await?;
        e.write(7, b"hello").await?;
        e.write(PAGE as u64, &vec![0; PAGE]).await?;
        e.flush().await?;
        let expected = e.read(0, PAGE * 2).await?;
        drop(e);
        let e = Engine::open(c.clone()).await?;
        assert_eq!(e.read(0, PAGE * 2).await?, expected);
        e.checkpoint().await?;
        assert_eq!(e.read(0, PAGE * 2).await?, expected);
        drop(e);
        let mut other = c.clone();
        other.local_dir = t.path().join("adopted");
        assert!(Engine::adopt(&other, false).await.is_err());
        Engine::adopt(&other, true).await?;
        let e = Engine::open(other).await?;
        assert_eq!(e.read(0, PAGE * 2).await?, expected);
        assert!(Engine::open(c).await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn experimental_cache_compaction_and_single_barrier_recover() -> Result<()> {
        let t = tempfile::tempdir()?;
        let mut c = config(&t);
        c.logical_cache = true;
        c.disk_cache_mib = 1;
        c.wal_commit_records = true;
        c.wal_fixed_size = true;
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        for n in 0..20 {
            e.write(0, &vec![n; PAGE * 8]).await?;
            e.flush().await?;
        }
        assert_eq!(Watermark::open(&c.local_dir.join("durable"))?.seq, 0);
        let seq = e.status().await.local_durable_sequence;
        drop(e);
        let e = Engine::open(c.clone()).await?;
        assert_eq!(e.status().await.local_durable_sequence, seq);
        assert_eq!(e.read(0, PAGE * 8).await?, vec![19; PAGE * 8]);
        e.zero(PAGE as u64, PAGE as u64).await?;
        e.flush().await?;
        let expected = e.read(0, PAGE * 8).await?;
        e.checkpoint().await?;
        drop(e);
        assert_eq!(Engine::compact(c.clone()).await?, 7);
        assert_eq!(Engine::verify_remote(&c).await?.1, 7);
        std::fs::remove_dir_all(c.local_dir.join("logical-cache"))?;
        assert_eq!(Engine::warm(c.clone()).await?, 7);
        let e = Engine::open(c.clone()).await?;
        assert_eq!(e.read(0, PAGE * 8).await?, expected);
        drop(e);
        let mut restored = c;
        restored.local_dir = t.path().join("fresh");
        Engine::adopt(&restored, true).await?;
        let e = Engine::open(restored).await?;
        assert_eq!(e.read(0, PAGE * 8).await?, expected);
        Ok(())
    }
    #[tokio::test]
    async fn single_barrier_rotation_requires_all_prefix_records() -> Result<()> {
        let t = tempfile::tempdir()?;
        let mut c = config(&t);
        c.wal_commit_records = true;
        Engine::init(&c, 4 * 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![3; 1024 * 1024]).await?;
        e.write(1024 * 1024, &vec![4; PAGE]).await?;
        e.flush().await?;
        drop(e);
        let paths: Vec<_> = std::fs::read_dir(c.local_dir.join("wal"))?
            .map(|e| e.unwrap().path())
            .collect();
        let oldest = paths.iter().min().unwrap();
        std::fs::remove_file(oldest)?;
        assert!(Engine::open(c).await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn compaction_upload_failure_preserves_the_previous_remote_root() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![7; PAGE * 2]).await?;
        e.checkpoint().await?;
        // Prime a verified cache so the failure is at immutable PUT, not source GET.
        assert_eq!(e.read(0, PAGE * 2).await?, vec![7; PAGE * 2]);
        drop(e);
        let before = encode(&Engine::inspect(&c).await?)?;
        let segments = t.path().join("remote/segments");
        let backup = t.path().join("remote/segments-backup");
        std::fs::rename(&segments, &backup)?;
        std::fs::write(&segments, b"block uploads")?;
        assert!(Engine::compact(c.clone()).await.is_err());
        assert_eq!(encode(&Engine::inspect(&c).await?)?, before);
        std::fs::remove_file(&segments)?;
        std::fs::rename(&backup, &segments)?;
        assert_eq!(Engine::verify_remote(&c).await?.1, 2);
        let e = Engine::open(c).await?;
        assert_eq!(e.read(0, PAGE * 2).await?, vec![7; PAGE * 2]);
        Ok(())
    }
    #[tokio::test]
    async fn fixed_wal_accounts_physical_capacity_and_backpressures_before_rotation() -> Result<()>
    {
        let t = tempfile::tempdir()?;
        let mut c = config(&t);
        c.wal_fixed_size = true;
        c.max_pending_mib = 20;
        Engine::init(&c, 4 * 1024 * 1024).await?;
        let e = Engine::open(c).await?;
        e.write(0, &vec![7; 1024 * 1024]).await?;
        assert!(e.status().await.pending_bytes > 18 * 1024 * 1024);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                e.write(1024 * 1024, &vec![8; PAGE])
            )
            .await
            .is_err()
        );
        e.checkpoint().await?;
        e.write(1024 * 1024, &vec![8; PAGE]).await?;
        e.flush().await?;
        assert_eq!(e.read(1024 * 1024, PAGE).await?, vec![8; PAGE]);
        Ok(())
    }
    #[tokio::test]
    async fn corrupted_pending_wal_never_replaces_the_last_remote_checkpoint() -> Result<()> {
        use std::os::unix::fs::FileExt;
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![7; PAGE]).await?;
        e.checkpoint().await?;
        let good_remote_seq = e.status().await.remote_sequence;
        e.write(0, &vec![9; PAGE]).await?;
        e.flush().await?;
        {
            let mut s = e.state.lock().await;
            let r = s.index.get(&0)?.unwrap();
            s.active.file.write_all_at(&[77], r.offset + 11)?;
        }
        ensure!(
            e.checkpoint().await.is_err(),
            "corrupted local WAL was published"
        );
        assert!(e.status().await.poisoned);
        assert_eq!(e.status().await.remote_sequence, good_remote_seq);
        drop(e);
        let mut recovered = c;
        recovered.local_dir = t.path().join("remote-recovery");
        Engine::adopt(&recovered, true).await?;
        let e = Engine::open(recovered).await?;
        assert_eq!(e.read(0, PAGE).await?, vec![7; PAGE]);
        Ok(())
    }
    #[tokio::test]
    async fn queued_barrier_rejects_acknowledgement_after_volume_failure() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c).await?;
        e.write(0, &vec![7; PAGE]).await?;
        let guard = e.flush_lock.lock().await;
        let queued = e.clone();
        let task = tokio::spawn(async move { queued.flush().await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while e.flush_metrics.calls.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        // Models failure of an earlier barrier while another client is queued.
        e.fail_closed();
        drop(guard);
        ensure!(
            task.await?.is_err(),
            "failed volume acknowledged a queued barrier"
        );
        assert_eq!(e.status().await.local_durable_sequence, 0);
        Ok(())
    }
    #[tokio::test]
    async fn model_and_concurrent_flush() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        let mut model = vec![0; 1024 * 1024];
        let mut seed = 1234567u64;
        for n in 0..300 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let off = (seed as usize % (model.len() - 8192)) as u64;
            let len = (seed as usize >> 20) % 8192 + 1;
            let b = vec![(n % 255) as u8; len];
            e.write(off, &b).await?;
            model[off as usize..off as usize + len].copy_from_slice(&b);
            if n % 23 == 0 {
                e.checkpoint().await?;
            }
        }
        let (a, b) = tokio::join!(e.flush(), e.flush());
        a?;
        b?;
        assert_eq!(e.read(0, model.len()).await?, model);
        e.checkpoint().await?;
        drop(e);
        let e = Engine::open(c).await?;
        assert_eq!(e.read(0, model.len()).await?, model);
        Ok(())
    }
    #[tokio::test]
    async fn missing_acknowledged_wal_fails_closed() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![1; PAGE]).await?;
        e.flush().await?;
        drop(e);
        for p in std::fs::read_dir(c.local_dir.join("wal"))? {
            std::fs::remove_file(p?.path())?;
        }
        assert!(Engine::open(c).await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn failed_checkpoint_keeps_wal_and_retries() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![42; PAGE]).await?;
        e.flush().await?;
        let head = t.path().join("remote/HEAD");
        let saved = t.path().join("saved-HEAD");
        std::fs::rename(&head, &saved)?;
        assert!(e.checkpoint().await.is_err());
        assert_eq!(e.read(0, PAGE).await?, vec![42; PAGE]);
        std::fs::rename(saved, head)?;
        e.checkpoint().await?;
        drop(e);
        let e = Engine::open(c).await?;
        assert_eq!(e.read(0, PAGE).await?, vec![42; PAGE]);
        Ok(())
    }
    #[tokio::test]
    async fn remote_corruption_is_never_returned_as_data() -> Result<()> {
        use std::os::unix::fs::FileExt;
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![23; PAGE]).await?;
        e.checkpoint().await?;
        drop(e);
        let segment = std::fs::read_dir(t.path().join("remote/segments"))?
            .next()
            .unwrap()?
            .path();
        OpenOptions::new()
            .write(true)
            .open(segment)?
            .write_all_at(&[99], 100)?;
        let e = Engine::open(c.clone()).await?;
        assert!(e.read(0, PAGE).await.is_err());
        assert!(Engine::verify_remote(&c).await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn corrupted_disposable_cache_is_rebuilt_from_verified_remote_data() -> Result<()> {
        use std::os::unix::fs::FileExt;
        let t = tempfile::tempdir()?;
        let mut c = config(&t);
        c.hot_wal_mib = 16;
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![3; PAGE * 2]).await?;
        e.checkpoint().await?;
        let path = std::fs::read_dir(c.local_dir.join("wal"))?
            .filter_map(|p| p.ok())
            .map(|p| p.path())
            .find(|p| std::fs::metadata(p).unwrap().len() > 64)
            .unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)?
            .write_all_at(&[99], 100)?;
        assert_eq!(e.read(0, PAGE).await?, vec![3; PAGE]);
        drop(e);
        let e = Engine::open(c.clone()).await?;
        assert_eq!(e.read(0, PAGE * 2).await?, vec![3; PAGE * 2]);
        drop(e);
        // Corrupt a different page in a cached extent. The first valid page must
        // not cause its unchecked neighbor to be returned or treated as authoritative.
        let cache = std::fs::read_dir(c.local_dir.join("cache"))?
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().is_some_and(|x| x == "cache"))
            .unwrap()
            .path();
        OpenOptions::new()
            .write(true)
            .open(cache)?
            .write_all_at(&[99], 64 + 32 + PAGE as u64 + 5)?;
        let e = Engine::open(c).await?;
        assert_eq!(e.read(0, PAGE).await?, vec![3; PAGE]);
        assert_eq!(e.read(PAGE as u64, PAGE).await?, vec![3; PAGE]);
        Ok(())
    }
    #[tokio::test]
    async fn offline_gc_keeps_live_data_and_resumes_a_crashed_maintenance_fence() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(&t);
        Engine::init(&c, 1024 * 1024).await?;
        let e = Engine::open(c.clone()).await?;
        e.write(0, &vec![1; PAGE]).await?;
        e.checkpoint().await?;
        e.write(0, &vec![2; PAGE]).await?;
        e.checkpoint().await?;
        assert!(Engine::gc(&c, false, 0).await.is_err());
        drop(e);
        let report = Engine::gc(&c, false, 0).await?;
        assert!(report.candidate_objects >= 2);
        let report = Engine::gc(&c, true, 0).await?;
        assert!(report.deleted_objects >= 2);
        let e = Engine::open(c.clone()).await?;
        assert_eq!(e.read(0, PAGE).await?, vec![2; PAGE]);
        let id = e.identity.clone();
        drop(e);
        let store = Store::new(&c)?;
        let (b, v) = store.head().await?.unwrap();
        let mut h = decode(&b)?;
        h.writer = Some(Uuid::nil());
        h.generation += 1;
        let token = GcToken {
            volume: id.volume,
            writer: id.writer,
            generation: h.generation,
        };
        wal::atomic(
            &c.local_dir.join("gc-token.json"),
            &serde_json::to_vec(&token)?,
        )?;
        store.cas(encode(&h)?, Some(v)).await?;
        assert!(Engine::open(c.clone()).await.is_err());
        let mut other = c.clone();
        other.local_dir = t.path().join("other");
        assert!(Engine::adopt(&other, true).await.is_err());
        Engine::gc(&c, true, 0).await?;
        let e = Engine::open(c).await?;
        assert_eq!(e.read(0, PAGE).await?, vec![2; PAGE]);
        Ok(())
    }
    #[tokio::test]
    async fn full_pending_wal_waits_for_checkpoint_instead_of_acknowledging_missing_data()
    -> Result<()> {
        let t = tempfile::tempdir()?;
        let mut c = config(&t);
        c.max_pending_mib = 3;
        Engine::init(&c, 8 * 1024 * 1024).await?;
        let e = Engine::open(c).await?;
        e.write(0, &vec![1; 2 * 1024 * 1024]).await?;
        let engine = e.clone();
        let task = tokio::spawn(async move {
            engine
                .write(2 * 1024 * 1024, &vec![2; 2 * 1024 * 1024])
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!task.is_finished());
        e.checkpoint().await?;
        tokio::time::timeout(std::time::Duration::from_secs(2), task).await???;
        assert_eq!(e.read(2 * 1024 * 1024, PAGE).await?, vec![2; PAGE]);
        Ok(())
    }
}
