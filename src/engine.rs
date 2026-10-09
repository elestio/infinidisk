use crate::{
    cache::{DiskCache, EXTENT as READ_EXTENT},
    config::Config,
    store::Store,
    wal::{self, MAX_IO, PAGE, Ref, Segment, Watermark},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use fs2::FileExt as _;
use futures::{StreamExt, TryStreamExt, stream};
use lru::LruCache;
use object_store::UpdateVersion;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs::{File, OpenOptions},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::{Mutex, MutexGuard, Notify};
use uuid::Uuid;

const SHARD_PAGES: u64 = 4096;
type ScrubGroups = BTreeMap<(Uuid, u64), (u64, Vec<(u64, u32)>)>;
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
    index: BTreeMap<u64, Ref>,
    dirty: BTreeSet<u64>,
    active: Segment,
    sealed: Vec<Segment>,
    local: HashMap<Uuid, Arc<File>>,
    pending: u64,
    resident: VecDeque<Segment>,
    resident_bytes: u64,
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
    cache: std::sync::Mutex<LruCache<(Uuid, u64), Bytes>>,
    fetch_locks: Vec<Mutex<()>>,
    disk_cache: DiskCache,
    space_available: Notify,
    poisoned: AtomicBool,
    _lock: File,
    started: Instant,
}
fn encode(h: &Head) -> Result<Bytes> {
    let payload = bincode::serialize(h)?;
    let mut b = Vec::with_capacity(payload.len() + 40);
    b.extend_from_slice(b"IDHEAD01");
    b.extend_from_slice(&Sha256::digest(&payload));
    b.extend(payload);
    Ok(b.into())
}
fn decode(b: &[u8]) -> Result<Head> {
    ensure!(
        b.len() >= 40 && b.len() <= 64 * 1024 * 1024 && &b[..8] == b"IDHEAD01",
        "invalid remote HEAD"
    );
    ensure!(
        Sha256::digest(&b[40..]).as_slice() == &b[8..40],
        "HEAD checksum mismatch"
    );
    let h: Head = bincode::deserialize(&b[40..])?;
    ensure!(
        h.format == 1 && h.size > 0 && h.size.is_multiple_of(PAGE as u64),
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
            format: 1,
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
            h.writer != Some(Uuid::nil()),
            "remote volume is under GC maintenance; resume gc --apply on its original host"
        );
        ensure!(
            h.writer.is_none() || takeover,
            "remote writer exists; fence its host, then pass --takeover"
        );
        // Validate the complete remote index before changing ownership.
        Self::load_index(&store, &h, c.max_index_mib).await?;
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
    async fn load_index(store: &Store, h: &Head, max_mib: usize) -> Result<BTreeMap<u64, Ref>> {
        let mut parts = stream::iter(h.shards.iter().map(|(&id, s)| async move {
            let b = store.get(&s.key).await?;
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
            Ok::<_, anyhow::Error>(map)
        }))
        .buffer_unordered(16);
        let mut index = BTreeMap::new();
        while let Some(part) = parts.try_next().await? {
            ensure!(
                index.len() + part.len() <= max_mib * 1024 * 1024 / 128,
                "page index exceeds max_index_mib; increase it or migrate to a larger-memory host"
            );
            index.extend(part);
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
            head.volume == identity.volume && head.size == identity.size,
            "local/remote identity mismatch"
        );
        ensure!(
            head.writer == Some(identity.writer),
            "writer has been fenced; refusing to serve old local state"
        );
        let mut index = Self::load_index(&store, &head, c.max_index_mib).await?;
        let wm = Watermark::open(&c.local_dir.join("durable"))?;
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
        let mut seq = head.seq;
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
                    let removed: Vec<_> = index
                        .range(row.first..row.first + row.zero_count)
                        .map(|(p, _)| *p)
                        .collect();
                    for p in removed {
                        index.remove(&p);
                        dirty.insert(p / SHARD_PAGES);
                    }
                }
                for (j, (r, zero)) in row.pages.into_iter().enumerate() {
                    let p = row.first + j as u64;
                    if zero {
                        index.remove(&p);
                    } else {
                        index.insert(p, r);
                    }
                    dirty.insert(p / SHARD_PAGES);
                }
                seq = row.seq;
                ensure!(
                    index.len() <= c.max_index_mib * 1024 * 1024 / 128,
                    "recovered page index exceeds max_index_mib"
                );
            }
            local.insert(s.id, Arc::new(s.file.try_clone()?));
            if s.last_seq <= head.seq {
                resident_bytes += s.len;
                resident.push_back(s);
            } else {
                pending += s.len;
                sealed.push(s);
            }
        }
        ensure!(
            seq >= wm.seq,
            "acknowledged durable writes are missing: WAL {}, watermark {}",
            seq,
            wm.seq
        );
        while resident_bytes > c.hot_wal_mib * 1024 * 1024 {
            let Some(segment) = resident.pop_front() else {
                break;
            };
            match std::fs::remove_file(&segment.path) {
                Ok(()) => {
                    resident_bytes -= segment.len;
                    local.remove(&segment.id);
                }
                Err(err) => {
                    tracing::warn!(error=%err,"committed WAL cache eviction failed");
                    resident.push_front(segment);
                    break;
                }
            }
        }
        let active = Segment::create(&c.local_dir.join("wal"), identity.volume, seq + 1)?;
        local.insert(active.id, Arc::new(active.file.try_clone()?));
        let cache = LruCache::new(
            NonZeroUsize::new(
                (c.memory_cache_mib * 1024 * 1024 / (c.read_extent_kib as usize * 1024 + PAGE))
                    .max(1),
            )
            .unwrap(),
        );
        let disk_cache = DiskCache::open(
            c.local_dir.join("cache"),
            c.disk_cache_mib * 1024 * 1024,
            c.read_extent_kib * 1024,
        )?;
        Ok(Arc::new(Self {
            config: c,
            identity,
            store,
            state: Mutex::new(State {
                seq,
                durable: wm.seq,
                index,
                dirty,
                active,
                sealed,
                local,
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
            disk_cache,
            space_available: Notify::new(),
            poisoned: AtomicBool::new(false),
            _lock: lock,
            started: Instant::now(),
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
    async fn writable_state(&self, additional: u64) -> Result<MutexGuard<'_, State>> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(50);
        loop {
            let notification = self.space_available.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            let s = self.state.lock().await;
            self.healthy()?;
            if s.pending + s.active.len + additional <= self.config.max_pending_mib * 1024 * 1024 {
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
    async fn page(&self, r: Option<Ref>, local: Option<Arc<File>>) -> Result<Bytes> {
        let Some(r) = r else {
            return Ok(Bytes::from_static(&[0; PAGE]));
        };
        if let Some(f) = local {
            match Segment::read(&f, &r) {
                Ok(b) => return Ok(b.into()),
                Err(err) if r.segment_len == 0 => return Err(err),
                Err(err) => {
                    tracing::warn!(error=%err,segment=%r.segment,"sealed local page damaged; attempting verified remote copy")
                }
            }
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
        let b = self
            .store
            .range(&format!("segments/{}", r.segment), start..end)
            .await?;
        ensure!(valid(&b), "remote page checksum mismatch");
        self.disk_cache.put(k, &b).await;
        self.cache.lock().unwrap().put(k, b.clone());
        Ok(b.slice(offset..offset + PAGE))
    }
    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.healthy()?;
        self.bounds(offset, len)?;
        if len == 0 {
            return Ok(Vec::new());
        }
        let first = offset / PAGE as u64;
        let end = (offset + len as u64).div_ceil(PAGE as u64);
        let refs: Vec<_> = {
            let s = self.state.lock().await;
            (first..end)
                .map(|p| {
                    let r = s.index.get(&p).cloned();
                    let f = r.as_ref().and_then(|r| s.local.get(&r.segment)).cloned();
                    (r, f)
                })
                .collect()
        };
        let pages: Vec<Bytes> = stream::iter(refs.into_iter().map(|(r, f)| self.page(r, f)))
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
    pub async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.healthy()?;
        self.bounds(offset, data.len())?;
        if data.is_empty() {
            return Ok(());
        }
        let first = offset / PAGE as u64;
        let end = (offset + data.len() as u64).div_ceil(PAGE as u64);
        let mut s = self
            .writable_state((end - first) * PAGE as u64 + 32)
            .await?;
        let mut pages = vec![0; (end - first) as usize * PAGE];
        let start = (offset % PAGE as u64) as usize;
        if start != 0 || !data.len().is_multiple_of(PAGE) {
            for p in [first, end - 1] {
                let r = s.index.get(&p).cloned();
                let f = r.as_ref().and_then(|r| s.local.get(&r.segment)).cloned();
                let b = self.page(r, f).await?;
                let dest = (p - first) as usize * PAGE;
                pages[dest..dest + PAGE].copy_from_slice(&b);
            }
        }
        pages[start..start + data.len()].copy_from_slice(data);
        let added = pages
            .as_chunks::<PAGE>()
            .0
            .iter()
            .enumerate()
            .filter(|(j, b)| {
                !b.iter().all(|v| *v == 0) && !s.index.contains_key(&(first + *j as u64))
            })
            .count();
        ensure!(
            s.index.len() + added <= self.config.max_index_mib * 1024 * 1024 / 128,
            "page index limit reached; increase max_index_mib"
        );
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
                s.index.remove(&p);
            } else {
                s.index.insert(p, r);
            }
            s.dirty.insert(p / SHARD_PAGES);
        }
        s.seq = seq;
        if s.active.len >= self.config.segment_mib * 1024 * 1024 {
            self.rotate(&mut s)?;
        }
        Ok(())
    }
    fn rotate(&self, s: &mut State) -> Result<()> {
        let next = Segment::create(
            &self.config.local_dir.join("wal"),
            self.identity.volume,
            s.seq + 1,
        )
        .inspect_err(|_| self.fail_closed())?;
        s.local.insert(next.id, Arc::new(next.file.try_clone()?));
        let old = std::mem::replace(&mut s.active, next);
        s.pending += old.len;
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
        let mut s = self.writable_state(32).await?;
        let seq = s.seq.checked_add(1).context("sequence exhausted")?;
        if let Err(err) = s.active.zero(seq, first, last - first) {
            self.fail_closed();
            return Err(err);
        }
        let keys: Vec<_> = s.index.range(first..last).map(|(p, _)| *p).collect();
        for p in keys {
            s.index.remove(&p);
            s.dirty.insert(p / SHARD_PAGES);
        }
        s.seq = seq;
        Ok(())
    }
    pub async fn flush(&self) -> Result<()> {
        self.healthy()?;
        let target = self.state.lock().await.seq;
        let _guard = self.flush_lock.lock().await;
        if self.state.lock().await.durable >= target {
            return Ok(());
        }
        // Concurrent connections share one fsync group and one durability frontier.
        tokio::task::yield_now().await;
        let (seq, files) = {
            let s = self.state.lock().await;
            (
                s.seq,
                s.sealed
                    .iter()
                    .map(|s| s.file.try_clone())
                    .chain(std::iter::once(s.active.file.try_clone()))
                    .collect::<std::io::Result<Vec<_>>>()?,
            )
        };
        let wm = self.watermark.clone();
        let r: Result<()> = tokio::task::spawn_blocking(move || {
            for f in files {
                f.sync_all()?;
            }
            wm.lock().unwrap().persist(seq)?;
            Ok(())
        })
        .await?;
        if let Err(e) = r {
            self.fail_closed();
            return Err(e.context("local durability failed"));
        }
        self.state.lock().await.durable = seq;
        Ok(())
    }
    pub async fn checkpoint(&self) -> Result<()> {
        self.healthy()?;
        let _guard = self.checkpoint_lock.lock().await;
        self.flush().await?;
        let (seq, segments, shards, dirty) = {
            let mut s = self.state.lock().await;
            if s.dirty.is_empty() && s.sealed.is_empty() {
                return Ok(());
            }
            if s.active.len > 64 {
                self.rotate(&mut s)?;
            }
            // Seal files before uploading. New writes continue in a different segment.
            for f in &s.sealed {
                f.file.sync_all()?;
            }
            let lengths: HashMap<Uuid, u64> = s.sealed.iter().map(|f| (f.id, f.len)).collect();
            for r in s.index.values_mut() {
                if let Some(len) = lengths.get(&r.segment) {
                    r.segment_len = *len;
                }
            }
            let dirty = std::mem::take(&mut s.dirty);
            let mut shards = Vec::new();
            for id in &dirty {
                let map: BTreeMap<u64, Ref> = s
                    .index
                    .range(id * SHARD_PAGES..(id + 1) * SHARD_PAGES)
                    .map(|(p, r)| (*p, r.clone()))
                    .collect();
                shards.push((*id, bincode::serialize(&map)?));
            }
            (
                s.seq,
                s.sealed
                    .iter()
                    .map(|s| (s.id, s.path.clone()))
                    .collect::<Vec<_>>(),
                shards,
                dirty,
            )
        };
        let result = async {
            let uploads = segments.clone().into_iter().map(|(id, path)| {
                let store = self.store.clone();
                async move {
                    let b = tokio::fs::read(path).await?;
                    store.immutable(&format!("segments/{id}"), b.into()).await
                }
            });
            stream::iter(uploads)
                .buffer_unordered(4)
                .try_collect::<Vec<_>>()
                .await?;
            let mut remote = self.remote.lock().await;
            let mut h = remote.head.clone();
            let uploaded: Vec<(u64, Shard)> = stream::iter(shards.into_iter().map(|(id, b)| {
                let store = self.store.clone();
                async move {
                    let hash = hex::encode(Sha256::digest(&b));
                    let key = format!("indexes/{id}/{}", Uuid::new_v4());
                    store.immutable(&key, b.into()).await?;
                    Ok::<_, anyhow::Error>((id, Shard { key, hash }))
                }
            }))
            .buffer_unordered(4)
            .try_collect()
            .await?;
            for (id, shard) in uploaded {
                h.shards.insert(id, shard);
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
        let mut retained = Vec::new();
        let sealed = std::mem::take(&mut s.sealed);
        for segment in sealed {
            if segment.last_seq <= seq {
                // Keep recent writes on SSD after publication instead of making their
                // first read pay an S3 GET. This cache is bounded and disposable.
                s.pending -= segment.len;
                s.resident_bytes += segment.len;
                s.resident.push_back(segment);
            } else {
                retained.push(segment);
            }
        }
        s.sealed = retained;
        self.space_available.notify_waiters();
        while s.resident_bytes > self.config.hot_wal_mib * 1024 * 1024 {
            let Some(segment) = s.resident.pop_front() else {
                break;
            };
            match std::fs::remove_file(&segment.path) {
                Ok(()) => {
                    s.resident_bytes -= segment.len;
                    s.local.remove(&segment.id);
                }
                Err(e) => {
                    tracing::warn!(error=%e,"local WAL eviction failed");
                    s.resident.push_front(segment);
                    break;
                }
            }
        }
        wal::sync_dir(&self.config.local_dir.join("wal"))?;
        Ok(())
    }
    pub async fn status(&self) -> Status {
        let s = self.state.lock().await;
        let r = self.remote.lock().await;
        Status {
            volume: self.identity.volume,
            size: self.identity.size,
            sequence: s.seq,
            local_durable_sequence: s.durable,
            remote_sequence: r.head.seq,
            remote_generation: r.head.generation,
            pending_bytes: s.pending + s.active.len,
            allocated_pages: s.index.len(),
            poisoned: self.poisoned.load(Ordering::Acquire),
            uptime_seconds: self.started.elapsed().as_secs(),
            hot_wal_bytes: s.resident_bytes,
        }
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
        let index = Self::load_index(&store, &h, c.max_index_mib).await?;
        let mut live: BTreeSet<object_store::path::Path> =
            h.shards.values().map(|s| store.path(&s.key)).collect();
        for r in index.into_values() {
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
        let index = Self::load_index(&store, &h, c.max_index_mib).await?;
        let n = index.len();
        let mut groups = ScrubGroups::new();
        for r in index.into_values() {
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
mod tests {
    use super::*;
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
