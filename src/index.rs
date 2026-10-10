//! The page index is rebuildable metadata, never a local recovery authority.
//!
//! Sharding bounds copy-on-write at checkpoint time. The paged backend keeps a
//! fixed number of shards resident and spills checksummed shards to scratch SSD.
//! Every open starts a fresh session: HEAD and the WAL must populate the index.
//! There are deliberately no fsyncs here; an unavailable/corrupt shard is an
//! error, never an empty page. The engine must fail closed on that error.
use crate::wal::Ref;
use anyhow::{Context, Result, ensure};
use bincode::Options;
use lru::LruCache;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    num::NonZeroUsize,
    ops::Range,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use uuid::Uuid;

pub const SHARD_PAGES: u64 = 4096;
pub const ENTRY_BUDGET_BYTES: usize = 128;
/// Conservative page-map accounting. The shard directory and LRU nodes are
/// reported separately; this is not a hard cap on the process allocator/RSS.
pub const SHARD_BUDGET_BYTES: usize = SHARD_PAGES as usize * ENTRY_BUDGET_BYTES;
const HEADER_BYTES: usize = 80;
const MAX_PAYLOAD_BYTES: usize = SHARD_BUDGET_BYTES;
const MAGIC: &[u8; 8] = b"IDIDX001";
const SHARD_METADATA_BUDGET_BYTES: usize = 128;

pub type PageMap = BTreeMap<u64, Ref>;

#[derive(Clone, Debug, Default, Serialize)]
pub struct IndexStats {
    pub paged: bool,
    pub allocated_pages: usize,
    pub shards: usize,
    pub resident_shards: usize,
    pub resident_pages: usize,
    pub resident_budget_bytes: usize,
    pub configured_budget_bytes: usize,
    pub directory_budget_bytes: usize,
    /// Versions held by the one permitted checkpoint generation. A resident
    /// version can still be shared with the active index; do not equate this
    /// conservative allowance with incremental allocated memory.
    pub snapshot_budget_bytes: usize,
    pub snapshot_in_flight: bool,
    pub cache_reads: u64,
    pub cache_writes: u64,
    pub evictions: u64,
}

/// A checkpoint owns one generation. Refusing a second simultaneous generation
/// prevents unlimited copy-on-write versions from escaping the resident bound.
#[derive(Default)]
struct SnapshotState {
    in_flight: AtomicBool,
    resident_bytes: AtomicUsize,
}

struct SnapshotGeneration {
    state: Arc<SnapshotState>,
}

impl Drop for SnapshotGeneration {
    fn drop(&mut self) {
        self.state.resident_bytes.store(0, Ordering::Release);
        self.state.in_flight.store(false, Ordering::Release);
    }
}

/// Only the directory created by this instance is removed. Previous sessions
/// are neither trusted nor reused, even if their checksums are valid.
struct ScratchDirectory {
    path: PathBuf,
    session: Uuid,
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct SnapshotFile {
    path: PathBuf,
    directory: Arc<ScratchDirectory>,
    id: u64,
    count: usize,
}

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Clone)]
enum SnapshotData {
    Memory(Arc<PageMap>),
    Disk(Arc<SnapshotFile>),
}

/// Capturing snapshots never serializes resident shards. Nonresident snapshots
/// are immutable hard links; cache replacements always use rename, never an
/// in-place write. Load these outside the engine state lock, with bounded
/// parallelism, then release each loaded Arc after serialization/upload.
#[derive(Clone)]
pub struct ShardSnapshot {
    data: SnapshotData,
    _generation: Arc<SnapshotGeneration>,
}

impl ShardSnapshot {
    pub fn load(&self) -> Result<Arc<PageMap>> {
        match &self.data {
            SnapshotData::Memory(map) => Ok(Arc::clone(map)),
            SnapshotData::Disk(file) => Ok(Arc::new(read_shard_file(
                &file.path,
                file.directory.session,
                file.id,
                file.count,
            )?)),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        matches!(self.data, SnapshotData::Disk(_))
    }
}

struct MemoryIndex {
    shards: BTreeMap<u64, Arc<PageMap>>,
    max_bytes: usize,
}

struct ResidentShard {
    map: Arc<PageMap>,
    dirty: bool,
}

struct PagedIndex {
    directory: Arc<ScratchDirectory>,
    /// One count per nonempty shard, not one key/reference per page.
    counts: BTreeMap<u64, usize>,
    resident: LruCache<u64, ResidentShard>,
    max_bytes: usize,
    reads: u64,
    writes: u64,
    evictions: u64,
}

enum Backend {
    Memory(MemoryIndex),
    Paged(PagedIndex),
}

pub struct PageIndex {
    backend: Backend,
    len: usize,
    snapshots: Arc<SnapshotState>,
}

impl PageIndex {
    /// Legacy memory budgeting with a sharded/COW representation. No allocation
    /// is made for empty logical pages.
    pub fn memory(max_bytes: usize) -> Self {
        Self {
            backend: Backend::Memory(MemoryIndex {
                shards: BTreeMap::new(),
                max_bytes,
            }),
            len: 0,
            snapshots: Arc::default(),
        }
    }

    /// Create an empty, disposable index. The caller must reconstruct it from
    /// its verified remote HEAD plus local WAL; existing scratch is ignored.
    /// max_bytes budgets resident page maps, with a minimum of one 512 KiB shard.
    pub fn paged(cache_root: &Path, max_bytes: usize) -> Result<Self> {
        let capacity = NonZeroUsize::new(max_bytes / SHARD_BUDGET_BYTES)
            .context("paged index needs at least 512 KiB of resident index budget")?;
        std::fs::create_dir_all(cache_root).context("create index scratch root")?;
        let session = Uuid::new_v4();
        let path = cache_root.join(format!("session-{session}"));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .context("create fresh index scratch session")?;
        Ok(Self {
            backend: Backend::Paged(PagedIndex {
                directory: Arc::new(ScratchDirectory { path, session }),
                counts: BTreeMap::new(),
                resident: LruCache::new(capacity),
                max_bytes,
                reads: 0,
                writes: 0,
                evictions: 0,
            }),
            len: 0,
            snapshots: Arc::default(),
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn is_paged(&self) -> bool {
        matches!(self.backend, Backend::Paged(_))
    }

    /// Fail before mutating the WAL if this operation would exceed the memory
    /// backend's allocation limit. Paged mode limits residency, not volume size.
    pub fn check_additional_pages(&self, additional: usize) -> Result<()> {
        let next = self
            .len
            .checked_add(additional)
            .context("index size overflow")?;
        if let Backend::Memory(index) = &self.backend {
            ensure!(
                next <= index.max_bytes / ENTRY_BUDGET_BYTES,
                "page index exceeds its memory budget"
            );
        }
        Ok(())
    }

    pub fn get(&mut self, page: &u64) -> Result<Option<Ref>> {
        let id = *page / SHARD_PAGES;
        Ok(match &mut self.backend {
            Backend::Memory(index) => index.shards.get(&id).and_then(|m| m.get(page)).cloned(),
            Backend::Paged(index) => {
                if !index.counts.contains_key(&id) {
                    return Ok(None);
                }
                index.ensure_resident(id)?;
                index
                    .resident
                    .get(&id)
                    .and_then(|s| s.map.get(page))
                    .cloned()
            }
        })
    }

    pub fn contains_key(&mut self, page: &u64) -> Result<bool> {
        Ok(self.get(page)?.is_some())
    }

    pub fn insert(&mut self, page: u64, reference: Ref) -> Result<Option<Ref>> {
        let id = page / SHARD_PAGES;
        let old = self.get(&page)?;
        self.check_additional_pages(usize::from(old.is_none()))?;
        match &mut self.backend {
            Backend::Memory(index) => {
                let shard = index.shards.entry(id).or_default();
                Arc::make_mut(shard).insert(page, reference);
            }
            Backend::Paged(index) => {
                index.ensure_resident(id)?;
                let shard = index
                    .resident
                    .get_mut(&id)
                    .context("missing resident index shard")?;
                Arc::make_mut(&mut shard.map).insert(page, reference);
                shard.dirty = true;
                index.counts.insert(id, shard.map.len());
            }
        }
        if old.is_none() {
            self.len += 1;
        }
        Ok(old)
    }

    pub fn remove(&mut self, page: &u64) -> Result<Option<Ref>> {
        let id = *page / SHARD_PAGES;
        let Some(old) = self.get(page)? else {
            return Ok(None);
        };
        match &mut self.backend {
            Backend::Memory(index) => {
                let shard = index
                    .shards
                    .get_mut(&id)
                    .context("missing memory index shard")?;
                Arc::make_mut(shard).remove(page);
                if shard.is_empty() {
                    index.shards.remove(&id);
                }
            }
            Backend::Paged(index) => {
                let shard = index
                    .resident
                    .get_mut(&id)
                    .context("missing resident index shard")?;
                Arc::make_mut(&mut shard.map).remove(page);
                shard.dirty = true;
                if shard.map.is_empty() {
                    index.counts.remove(&id);
                    index.resident.pop(&id);
                    // An obsolete file is unreachable without its in-memory
                    // count; failure to unlink it cannot revive a logical page.
                    let _ = std::fs::remove_file(index.shard_path(id));
                } else {
                    index.counts.insert(id, shard.map.len());
                }
            }
        }
        self.len -= 1;
        Ok(Some(old))
    }

    pub fn range(&mut self, pages: Range<u64>) -> Result<Vec<(u64, Ref)>> {
        if pages.start >= pages.end {
            return Ok(Vec::new());
        }
        let ids = self.shard_ids(pages.start / SHARD_PAGES..=(pages.end - 1) / SHARD_PAGES);
        let mut rows = Vec::new();
        for id in ids {
            let map = self.load_shard(id)?;
            rows.extend(map.range(pages.clone()).map(|(&p, r)| (p, r.clone())));
        }
        Ok(rows)
    }

    /// Remove a potentially volume-sized range without collecting its pages.
    /// Only one shard is inspected/mutated at a time. Returned dirty ids cost
    /// one entry per changed shard; snapshots retain their prior COW versions.
    pub fn remove_range(&mut self, pages: Range<u64>) -> Result<Vec<u64>> {
        if pages.start >= pages.end {
            return Ok(Vec::new());
        }
        let ids = self.shard_ids(pages.start / SHARD_PAGES..=(pages.end - 1) / SHARD_PAGES);
        let mut changed = Vec::new();
        for id in ids {
            let removed = match &mut self.backend {
                Backend::Memory(index) => {
                    let shard = index
                        .shards
                        .get_mut(&id)
                        .context("missing memory index shard")?;
                    if shard.range(pages.clone()).next().is_none() {
                        continue;
                    }
                    let before = shard.len();
                    Arc::make_mut(shard).retain(|page, _| !pages.contains(page));
                    let removed = before - shard.len();
                    if shard.is_empty() {
                        index.shards.remove(&id);
                    }
                    removed
                }
                Backend::Paged(index) => {
                    // Validate cold shards even for a full-shard TRIM. Runtime
                    // cache corruption must still surface as an error.
                    index.ensure_resident(id)?;
                    let shard = index
                        .resident
                        .get_mut(&id)
                        .context("missing resident index shard")?;
                    if shard.map.range(pages.clone()).next().is_none() {
                        continue;
                    }
                    let before = shard.map.len();
                    Arc::make_mut(&mut shard.map).retain(|page, _| !pages.contains(page));
                    let removed = before - shard.map.len();
                    if shard.map.is_empty() {
                        index.counts.remove(&id);
                        index.resident.pop(&id);
                        let _ = std::fs::remove_file(index.shard_path(id));
                    } else {
                        shard.dirty = true;
                        index.counts.insert(id, shard.map.len());
                    }
                    removed
                }
            };
            self.len -= removed;
            changed.push(id);
        }
        Ok(changed)
    }

    /// Offline operations only. This deliberately materializes every entry;
    /// hot paths and checkpointing must use individual shards/snapshots instead.
    pub fn entries(&mut self) -> Result<Vec<(u64, Ref)>> {
        let mut rows = Vec::with_capacity(self.len);
        for id in self.shard_ids(..) {
            rows.extend(self.load_shard(id)?.iter().map(|(&p, r)| (p, r.clone())));
        }
        Ok(rows)
    }

    /// Inspect a single shard. Callers should release the returned Arc promptly;
    /// retaining arbitrary versions themselves is outside the resident budget.
    pub fn load_shard(&mut self, id: u64) -> Result<Arc<PageMap>> {
        match &mut self.backend {
            Backend::Memory(index) => Ok(index.shards.get(&id).cloned().unwrap_or_default()),
            Backend::Paged(index) => {
                if !index.counts.contains_key(&id) {
                    return Ok(Arc::default());
                }
                index.ensure_resident(id)?;
                Ok(Arc::clone(
                    &index
                        .resident
                        .get(&id)
                        .context("missing resident index shard")?
                        .map,
                ))
            }
        }
    }

    /// Import one verified remote shard at startup. Page ownership is checked
    /// here; validating segment bounds and the remote hash is the engine's job.
    pub fn replace_shard(&mut self, id: u64, map: PageMap) -> Result<()> {
        validate_pages(id, &map)?;
        let old_count = match &self.backend {
            Backend::Memory(index) => index.shards.get(&id).map_or(0, |m| m.len()),
            Backend::Paged(index) => index.counts.get(&id).copied().unwrap_or(0),
        };
        let next_len = (self.len - old_count)
            .checked_add(map.len())
            .context("index size overflow")?;
        if let Backend::Memory(index) = &self.backend {
            ensure!(
                next_len <= index.max_bytes / ENTRY_BUDGET_BYTES,
                "page index exceeds its memory budget"
            );
        }
        match &mut self.backend {
            Backend::Memory(index) => {
                if map.is_empty() {
                    index.shards.remove(&id);
                } else {
                    index.shards.insert(id, Arc::new(map));
                }
            }
            Backend::Paged(index) => {
                if map.is_empty() {
                    index.counts.remove(&id);
                    index.resident.pop(&id);
                    let _ = std::fs::remove_file(index.shard_path(id));
                } else {
                    if !index.resident.contains(&id) {
                        index.make_room()?;
                    }
                    index.counts.insert(id, map.len());
                    index.resident.put(
                        id,
                        ResidentShard {
                            map: Arc::new(map),
                            dirty: true,
                        },
                    );
                }
            }
        }
        self.len = next_len;
        Ok(())
    }

    /// Capture a stable generation while the engine holds its state lock.
    /// Resident shards share immutable Arcs; evicted shards only need a hard
    /// link. Serialization and disk reads happen when the caller later loads
    /// these handles outside that lock. Only one live generation is permitted.
    pub fn snapshot<I>(&self, ids: I) -> Result<Vec<(u64, ShardSnapshot)>>
    where
        I: IntoIterator<Item = u64>,
    {
        ensure!(
            self.snapshots
                .in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "an index snapshot generation is already in flight"
        );
        let generation = Arc::new(SnapshotGeneration {
            state: Arc::clone(&self.snapshots),
        });
        let mut result = Vec::new();
        // De-duplicate shard ids without introducing per-page metadata.
        let ids: std::collections::BTreeSet<_> = ids.into_iter().collect();
        let mut resident_bytes = 0usize;
        for id in ids {
            let data = match &self.backend {
                Backend::Memory(index) => {
                    let map = index.shards.get(&id).cloned().unwrap_or_default();
                    resident_bytes =
                        resident_bytes.saturating_add(map.len().saturating_mul(ENTRY_BUDGET_BYTES));
                    SnapshotData::Memory(map)
                }
                Backend::Paged(index) => {
                    if let Some(shard) = index.resident.peek(&id) {
                        resident_bytes = resident_bytes.saturating_add(SHARD_BUDGET_BYTES);
                        SnapshotData::Memory(Arc::clone(&shard.map))
                    } else if let Some(&count) = index.counts.get(&id) {
                        let path = index
                            .directory
                            .path
                            .join(format!("{id:016x}-{}.snapshot", Uuid::new_v4()));
                        std::fs::hard_link(index.shard_path(id), &path)
                            .context("capture immutable index shard")?;
                        SnapshotData::Disk(Arc::new(SnapshotFile {
                            path,
                            directory: Arc::clone(&index.directory),
                            id,
                            count,
                        }))
                    } else {
                        SnapshotData::Memory(Arc::default())
                    }
                }
            };
            result.push((
                id,
                ShardSnapshot {
                    data,
                    _generation: Arc::clone(&generation),
                },
            ));
        }
        self.snapshots
            .resident_bytes
            .store(resident_bytes, Ordering::Release);
        Ok(result)
    }

    pub fn stats(&self) -> IndexStats {
        let mut stats = IndexStats {
            allocated_pages: self.len,
            snapshot_budget_bytes: self.snapshots.resident_bytes.load(Ordering::Acquire),
            snapshot_in_flight: self.snapshots.in_flight.load(Ordering::Acquire),
            ..Default::default()
        };
        match &self.backend {
            Backend::Memory(index) => {
                stats.shards = index.shards.len();
                stats.resident_shards = stats.shards;
                stats.resident_pages = self.len;
                stats.resident_budget_bytes = self.len.saturating_mul(ENTRY_BUDGET_BYTES);
                stats.configured_budget_bytes = index.max_bytes;
            }
            Backend::Paged(index) => {
                stats.paged = true;
                stats.shards = index.counts.len();
                stats.resident_shards = index.resident.len();
                stats.resident_pages = index.resident.iter().map(|(_, s)| s.map.len()).sum();
                stats.resident_budget_bytes =
                    stats.resident_shards.saturating_mul(SHARD_BUDGET_BYTES);
                stats.configured_budget_bytes = index.max_bytes;
                stats.cache_reads = index.reads;
                stats.cache_writes = index.writes;
                stats.evictions = index.evictions;
            }
        }
        stats.directory_budget_bytes = stats.shards.saturating_mul(SHARD_METADATA_BUDGET_BYTES);
        stats
    }

    fn shard_ids<R>(&self, range: R) -> Vec<u64>
    where
        R: std::ops::RangeBounds<u64>,
    {
        match &self.backend {
            Backend::Memory(index) => index.shards.range(range).map(|(&id, _)| id).collect(),
            Backend::Paged(index) => index.counts.range(range).map(|(&id, _)| id).collect(),
        }
    }
}

impl PagedIndex {
    fn shard_path(&self, id: u64) -> PathBuf {
        self.directory.path.join(format!("{id:016x}.idx"))
    }

    fn make_room(&mut self) -> Result<()> {
        if self.resident.len() < self.resident.cap().get() {
            return Ok(());
        }
        if let Some((&id, shard)) = self.resident.peek_lru() {
            if shard.dirty {
                write_shard_file(&self.shard_path(id), self.directory.session, id, &shard.map)?;
                self.writes += 1;
            }
            // Retain the only current copy if write/rename fails. A caller can
            // poison the engine without a silently missing index entry.
            self.resident.pop_lru();
            self.evictions += 1;
        }
        Ok(())
    }

    fn ensure_resident(&mut self, id: u64) -> Result<()> {
        if self.resident.contains(&id) {
            return Ok(());
        }
        // A temporary decoded shard adds at most one shard to resident memory.
        // Decode before evicting, so a corrupt requested shard preserves the
        // currently resident set and never becomes an implicit empty shard.
        let map = if let Some(&count) = self.counts.get(&id) {
            let map = read_shard_file(&self.shard_path(id), self.directory.session, id, count)?;
            self.reads += 1;
            map
        } else {
            BTreeMap::new()
        };
        self.make_room()?;
        self.resident.put(
            id,
            ResidentShard {
                map: Arc::new(map),
                dirty: false,
            },
        );
        Ok(())
    }
}

fn validate_pages(id: u64, map: &PageMap) -> Result<()> {
    ensure!(
        map.len() <= SHARD_PAGES as usize,
        "too many pages in index shard"
    );
    ensure!(
        map.keys().all(|p| *p / SHARD_PAGES == id),
        "page belongs to another index shard"
    );
    Ok(())
}

fn write_shard_file(path: &Path, session: Uuid, id: u64, map: &PageMap) -> Result<()> {
    validate_pages(id, map)?;
    let payload = bincode::serialize(map).context("encode index scratch shard")?;
    ensure!(
        payload.len() <= MAX_PAYLOAD_BYTES,
        "oversized index scratch shard"
    );
    let mut header = [0u8; HEADER_BYTES];
    header[..8].copy_from_slice(MAGIC);
    header[8..24].copy_from_slice(session.as_bytes());
    header[24..32].copy_from_slice(&id.to_le_bytes());
    header[32..40].copy_from_slice(&(map.len() as u64).to_le_bytes());
    header[40..48].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    header[48..80].copy_from_slice(&Sha256::digest(&payload));
    let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    let outcome = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&header)?;
        file.write_all(&payload)?;
        drop(file);
        // No fsync: this is an expendable cache. Rename keeps existing hard-link
        // snapshots immutable, including when a shard is evicted repeatedly.
        std::fs::rename(&temporary, path).context("replace index scratch shard")?;
        Ok(())
    })();
    if outcome.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    outcome
}

fn read_shard_file(path: &Path, session: Uuid, id: u64, count: usize) -> Result<PageMap> {
    let mut file = File::open(path).with_context(|| format!("read index scratch shard {id}"))?;
    let len = file.metadata()?.len();
    ensure!(
        (HEADER_BYTES as u64..=(HEADER_BYTES + MAX_PAYLOAD_BYTES) as u64).contains(&len),
        "invalid index scratch size"
    );
    let mut header = [0u8; HEADER_BYTES];
    file.read_exact(&mut header)?;
    ensure!(
        &header[..8] == MAGIC && &header[8..24] == session.as_bytes(),
        "invalid index scratch identity"
    );
    ensure!(
        u64::from_le_bytes(header[24..32].try_into()?) == id,
        "wrong index scratch shard"
    );
    ensure!(
        count <= SHARD_PAGES as usize
            && u64::from_le_bytes(header[32..40].try_into()?) == count as u64,
        "index scratch page count mismatch"
    );
    ensure!(
        u64::from_le_bytes(header[40..48].try_into()?) == len - HEADER_BYTES as u64,
        "index scratch payload size mismatch"
    );
    let mut payload = vec![0u8; len as usize - HEADER_BYTES];
    file.read_exact(&mut payload)?;
    ensure!(
        Sha256::digest(&payload).as_slice() == &header[48..80],
        "index scratch checksum mismatch"
    );
    let map: PageMap = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_PAYLOAD_BYTES as u64)
        .reject_trailing_bytes()
        .deserialize(&payload)
        .context("decode index scratch shard")?;
    ensure!(
        map.len() == count,
        "decoded index scratch page count mismatch"
    );
    validate_pages(id, &map)?;
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(n: u64) -> Ref {
        Ref {
            segment: Uuid::from_u128(n as u128 + 1),
            offset: 64 + n * 4096,
            crc: n as u32,
            segment_len: 0,
        }
    }

    fn disk_path(index: &PageIndex, id: u64) -> PathBuf {
        let Backend::Paged(index) = &index.backend else {
            panic!("expected paged backend")
        };
        index.shard_path(id)
    }

    #[test]
    fn eviction_round_trip_preserves_sparse_updates_and_bounds_resident_maps() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut index = PageIndex::paged(temporary.path(), 2 * SHARD_BUDGET_BYTES)?;
        let mut expected = BTreeMap::new();
        for shard in 0..19 {
            for offset in [0, 7, SHARD_PAGES - 1] {
                let page = shard * SHARD_PAGES + offset;
                let value = reference(page);
                index.insert(page, value.clone())?;
                expected.insert(page, value);
                assert!(index.stats().resident_budget_bytes <= 2 * SHARD_BUDGET_BYTES);
                assert!(index.stats().resident_pages <= 6);
            }
        }
        assert_eq!(index.len(), expected.len());
        assert_eq!(index.stats().shards, 19);
        assert!(index.stats().evictions > 0);
        for (&page, value) in expected.iter().rev() {
            assert_eq!(index.get(&page)?.as_ref(), Some(value));
        }
        let selected = (SHARD_PAGES - 1)..(2 * SHARD_PAGES + 8);
        assert_eq!(
            index.range(selected.clone())?,
            expected
                .range(selected)
                .map(|(&p, r)| (p, r.clone()))
                .collect::<Vec<_>>()
        );
        for page in [7, SHARD_PAGES, 18 * SHARD_PAGES + 7] {
            assert_eq!(index.remove(&page)?, expected.remove(&page));
        }
        assert_eq!(index.entries()?, expected.into_iter().collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn checkpoint_captures_resident_and_spilled_versions_without_loading_every_shard() -> Result<()>
    {
        let temporary = tempfile::tempdir()?;
        let mut index = PageIndex::paged(temporary.path(), SHARD_BUDGET_BYTES)?;
        for id in 0..8 {
            index.insert(id * SHARD_PAGES, reference(id))?;
        }
        let reads = index.stats().cache_reads;
        let snapshots = index.snapshot(0..8)?;
        assert_eq!(index.stats().cache_reads, reads);
        assert_eq!(index.stats().resident_shards, 1);
        assert_eq!(index.stats().snapshot_budget_bytes, SHARD_BUDGET_BYTES);
        assert_eq!(snapshots.iter().filter(|(_, s)| s.is_on_disk()).count(), 7);
        assert!(index.snapshot([0]).is_err());
        for id in 0..8 {
            index.insert(id * SHARD_PAGES, reference(100 + id))?;
        }
        // Deleting a live shard must not delete its captured hard-link content.
        index.remove(&0)?;
        for (id, snapshot) in &snapshots {
            assert_eq!(
                snapshot.load()?.get(&(id * SHARD_PAGES)),
                Some(&reference(*id))
            );
        }
        drop(snapshots);
        assert!(!index.stats().snapshot_in_flight);
        assert_eq!(index.stats().snapshot_budget_bytes, 0);
        assert!(index.snapshot([0])?[0].1.load()?.is_empty());
        Ok(())
    }

    #[test]
    fn dense_large_trim_keeps_one_resident_shard_and_preserves_snapshots() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut index = PageIndex::paged(temporary.path(), SHARD_BUDGET_BYTES)?;
        const SHARDS: u64 = 16;
        // 256 MiB of fully allocated logical pages, but only 512 KiB of index
        // residency. A range() of these 65,536 refs would exceed that budget.
        for id in 0..SHARDS {
            let map = (id * SHARD_PAGES..(id + 1) * SHARD_PAGES)
                .map(|page| (page, reference(page)))
                .collect();
            index.replace_shard(id, map)?;
            assert!(index.stats().resident_budget_bytes <= SHARD_BUDGET_BYTES);
        }
        let snapshots = index.snapshot(0..SHARDS)?;
        let trimmed = SHARD_PAGES / 2..(SHARDS - 1) * SHARD_PAGES + SHARD_PAGES / 2;
        assert_eq!(
            index.remove_range(trimmed.clone())?,
            (0..SHARDS).collect::<Vec<_>>()
        );
        assert_eq!(index.len(), SHARD_PAGES as usize);
        assert_eq!(index.stats().shards, 2);
        assert!(index.stats().resident_budget_bytes <= SHARD_BUDGET_BYTES);
        assert_eq!(index.stats().snapshot_budget_bytes, SHARD_BUDGET_BYTES);
        for page in 0..SHARDS * SHARD_PAGES {
            assert_eq!(
                index.get(&page)?,
                (!trimmed.contains(&page)).then(|| reference(page))
            );
        }
        for (id, snapshot) in snapshots {
            let map = snapshot.load()?;
            assert_eq!(map.len(), SHARD_PAGES as usize);
            assert_eq!(
                map.get(&(id * SHARD_PAGES)),
                Some(&reference(id * SHARD_PAGES))
            );
        }
        assert_eq!(index.remove_range(trimmed)?, Vec::<u64>::new());
        assert_eq!(
            index.remove_range(0..SHARDS * SHARD_PAGES)?,
            vec![0, SHARDS - 1]
        );
        assert!(index.is_empty());
        Ok(())
    }

    #[test]
    fn memory_range_removal_handles_holes_and_keeps_snapshot_versions() -> Result<()> {
        let mut index = PageIndex::memory(3 * ENTRY_BUDGET_BYTES);
        for page in [0, 9, SHARD_PAGES] {
            index.insert(page, reference(page))?;
        }
        let snapshots = index.snapshot([0, 1])?;
        assert!(index.remove_range(1..9)?.is_empty());
        assert!(index.remove_range(9..9)?.is_empty());
        assert_eq!(index.remove_range(9..SHARD_PAGES + 1)?, vec![0, 1]);
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(&0)?, Some(reference(0)));
        assert_eq!(snapshots[0].1.load()?.len(), 2);
        assert_eq!(snapshots[1].1.load()?.len(), 1);
        Ok(())
    }

    #[test]
    fn scratch_is_never_reopened_as_authoritative_state() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut first = PageIndex::paged(temporary.path(), SHARD_BUDGET_BYTES)?;
        first.insert(0, reference(1))?;
        first.insert(SHARD_PAGES, reference(2))?;
        assert!(disk_path(&first, 0).exists());
        let mut second = PageIndex::paged(temporary.path(), SHARD_BUDGET_BYTES)?;
        assert!(second.is_empty());
        assert!(second.get(&0)?.is_none());
        assert_ne!(disk_path(&first, 0), disk_path(&second, 0));
        assert_eq!(first.get(&0)?, Some(reference(1)));
        Ok(())
    }

    #[test]
    fn corrupt_or_missing_evicted_shard_fails_closed() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut index = PageIndex::paged(temporary.path(), SHARD_BUDGET_BYTES)?;
        index.insert(0, reference(1))?;
        index.insert(SHARD_PAGES, reference(2))?;
        let path = disk_path(&index, 0);
        let original = std::fs::read(&path)?;
        let mut corrupt = original.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&path, &corrupt)?;
        assert!(index.get(&0).unwrap_err().to_string().contains("checksum"));
        assert!(index.remove_range(0..2 * SHARD_PAGES).is_err());
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(&SHARD_PAGES)?, Some(reference(2)));
        std::fs::write(&path, &original)?;
        std::fs::remove_file(&path)?;
        assert!(index.get(&0).is_err());
        assert_eq!(index.len(), 2);
        Ok(())
    }

    #[test]
    fn wrong_session_shard_and_count_are_rejected() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("shard");
        let session = Uuid::new_v4();
        let map = BTreeMap::from([(SHARD_PAGES, reference(1))]);
        write_shard_file(&path, session, 1, &map)?;
        assert!(read_shard_file(&path, Uuid::new_v4(), 1, 1).is_err());
        assert!(read_shard_file(&path, session, 0, 1).is_err());
        assert!(read_shard_file(&path, session, 1, 2).is_err());
        assert!(write_shard_file(&path, session, 0, &map).is_err());
        Ok(())
    }

    #[test]
    fn failed_spill_preserves_the_only_current_copy() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut index = PageIndex::paged(temporary.path(), SHARD_BUDGET_BYTES)?;
        index.insert(0, reference(1))?;
        // A directory at the destination forces rename failure on every host,
        // including root, unlike a permission-based error injection.
        std::fs::create_dir(disk_path(&index, 0))?;
        assert!(index.insert(SHARD_PAGES, reference(2)).is_err());
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(&0)?, Some(reference(1)));
        assert_eq!(index.get(&SHARD_PAGES)?, None);
        assert_eq!(index.stats().evictions, 0);
        Ok(())
    }

    #[test]
    fn memory_budget_import_and_copy_on_write_are_preserved() -> Result<()> {
        let mut index = PageIndex::memory(2 * ENTRY_BUDGET_BYTES);
        index.replace_shard(0, BTreeMap::from([(0, reference(0)), (1, reference(1))]))?;
        assert!(index.insert(2, reference(2)).is_err());
        assert!(
            index
                .replace_shard(1, BTreeMap::from([(0, reference(0))]))
                .is_err()
        );
        let snapshots = index.snapshot([0, 1])?;
        index.insert(0, reference(100))?;
        index.remove(&1)?;
        index.insert(SHARD_PAGES, reference(200))?;
        assert_eq!(snapshots[0].1.load()?.get(&0), Some(&reference(0)));
        assert_eq!(snapshots[0].1.load()?.get(&1), Some(&reference(1)));
        assert!(snapshots[1].1.load()?.is_empty());
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(&0)?, Some(reference(100)));
        Ok(())
    }

    #[test]
    fn paged_replacement_removal_and_snapshot_cleanup_are_consistent() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut index = PageIndex::paged(temporary.path(), SHARD_BUDGET_BYTES)?;
        index.replace_shard(0, BTreeMap::from([(0, reference(0)), (7, reference(7))]))?;
        index.replace_shard(1, BTreeMap::from([(SHARD_PAGES, reference(1))]))?;
        let snapshot = index.snapshot([0])?;
        let session_path = disk_path(&index, 0).parent().unwrap().to_owned();
        index.replace_shard(0, BTreeMap::from([(2, reference(2))]))?;
        assert_eq!(index.len(), 2);
        index.replace_shard(1, BTreeMap::new())?;
        assert_eq!(index.len(), 1);
        assert!(index.get(&SHARD_PAGES)?.is_none());
        drop(index);
        assert!(session_path.exists());
        assert_eq!(snapshot[0].1.load()?.len(), 2);
        drop(snapshot);
        assert!(!session_path.exists());
        Ok(())
    }

    #[test]
    fn full_shard_payload_fits_the_bounded_decoder() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut index = PageIndex::paged(temporary.path(), SHARD_BUDGET_BYTES)?;
        let map: PageMap = (0..SHARD_PAGES).map(|p| (p, reference(p))).collect();
        index.replace_shard(0, map.clone())?;
        index.insert(SHARD_PAGES, reference(SHARD_PAGES))?;
        assert_eq!(*index.load_shard(0)?, map);
        assert_eq!(index.len(), SHARD_PAGES as usize + 1);
        Ok(())
    }
}
