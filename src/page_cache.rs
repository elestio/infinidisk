//! Disposable packed cache of current logical pages. Metadata is not durability.
//! A torn cache update is a miss: both the version and payload CRC must match.
use crate::wal::{PAGE, Ref};
use anyhow::Result;
use bytes::Bytes;
use lru::LruCache;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    os::unix::fs::FileExt,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
};
use uuid::Uuid;

const META: usize = 40;
struct Entry {
    slot: u64,
    segment: Uuid,
    offset: u64,
    crc: u32,
}
struct State {
    entries: LruCache<u64, Entry>,
    free: Vec<u64>,
}
pub struct PageCache {
    data: File,
    meta: File,
    state: Vec<Mutex<State>>,
}
impl PageCache {
    pub fn open(dir: &Path, volume: Uuid, bytes: u64) -> Result<Self> {
        Self::open_partitioned(dir, volume, bytes, 1)
    }
    pub fn open_partitioned(
        dir: &Path,
        volume: Uuid,
        bytes: u64,
        partitions: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let cap = bytes / (PAGE + META) as u64;
        let partitions = partitions.clamp(1, 16).min(cap.max(1) as usize);
        let marker = format!("{volume}:{cap}:{partitions}");
        let reset = std::fs::read_to_string(dir.join("identity"))
            .ok()
            .as_deref()
            != Some(&marker);
        let data = OpenOptions::new()
            .create(true)
            .truncate(reset)
            .read(true)
            .write(true)
            .open(dir.join("pages"))?;
        let mut meta = OpenOptions::new()
            .create(true)
            .truncate(reset)
            .read(true)
            .write(true)
            .open(dir.join("versions"))?;
        data.set_len(cap * PAGE as u64)?;
        meta.set_len(cap * META as u64)?;
        std::fs::write(dir.join("identity"), marker)?;
        let mut states: Vec<_> = (0..partitions)
            .map(|_| State {
                entries: LruCache::unbounded(),
                free: Vec::new(),
            })
            .collect();
        let mut scan = std::io::BufReader::new(&mut meta);
        for slot in 0..cap {
            let state = &mut states[slot as usize % partitions];
            let mut b = [0; META];
            scan.read_exact(&mut b)?;
            if crc32fast::hash(&b[..36]) != u32::from_le_bytes(b[36..40].try_into()?) {
                state.free.push(slot);
                continue;
            }
            let page = u64::from_le_bytes(b[..8].try_into()?);
            if page as usize % partitions != slot as usize % partitions {
                state.free.push(slot);
                continue;
            }
            let entry = Entry {
                slot,
                segment: Uuid::from_slice(&b[8..24])?,
                offset: u64::from_le_bytes(b[24..32].try_into()?),
                crc: u32::from_le_bytes(b[32..36].try_into()?),
            };
            if let Some(old) = state.entries.put(page, entry) {
                state.free.push(old.slot);
            }
        }
        meta.seek(SeekFrom::Start(0))?;
        Ok(Self {
            data,
            meta,
            state: states.into_iter().map(Mutex::new).collect(),
        })
    }
    pub fn get(&self, page: u64, expected: &Ref) -> Option<Bytes> {
        let mut b = vec![0; PAGE];
        self.get_into(page, expected, &mut b).then(|| b.into())
    }
    /// Offline warm promises to retain all requested pages. Total free bytes
    /// alone are insufficient when addresses are skewed toward one partition.
    pub fn ensure_capacity_for(&self, pages: impl Iterator<Item = u64>) -> Result<()> {
        let mut needed = vec![0_usize; self.state.len()];
        for page in pages {
            needed[page as usize % self.state.len()] += 1;
        }
        for (partition, needed) in self.state.iter().zip(needed) {
            let partition = partition.lock().unwrap();
            anyhow::ensure!(
                needed <= partition.free.len() + partition.entries.len(),
                "allocated pages do not fit one logical cache partition; increase disk_cache_mib"
            );
        }
        Ok(())
    }
    pub fn get_into(&self, page: u64, expected: &Ref, b: &mut [u8]) -> bool {
        if b.len() != PAGE {
            return false;
        }
        let mut s = self.state[page as usize % self.state.len()].lock().unwrap();
        let Some(e) = s.entries.get(&page) else {
            return false;
        };
        if (e.segment, e.offset, e.crc) != (expected.segment, expected.offset, expected.crc) {
            return false;
        }
        if self.data.read_exact_at(b, e.slot * PAGE as u64).is_ok()
            && crc32fast::hash(b) == expected.crc
        {
            return true;
        }
        let e = s.entries.pop(&page).unwrap();
        s.free.push(e.slot);
        false
    }
    pub fn put(&self, page: u64, version: &Ref, b: &[u8]) -> Result<()> {
        if b.len() != PAGE || crc32fast::hash(b) != version.crc {
            return Ok(());
        }
        let mut s = self.state[page as usize % self.state.len()].lock().unwrap();
        if s.entries.peek(&page).is_some_and(|e| {
            (e.segment, e.offset, e.crc) == (version.segment, version.offset, version.crc)
        }) {
            return Ok(());
        }
        let slot = if let Some(e) = s.entries.pop(&page) {
            e.slot
        } else if let Some(slot) = s.free.pop() {
            slot
        } else if let Some((_, e)) = s.entries.pop_lru() {
            e.slot
        } else {
            return Ok(());
        };
        let mut meta = [0; META];
        meta[..8].copy_from_slice(&page.to_le_bytes());
        meta[8..24].copy_from_slice(version.segment.as_bytes());
        meta[24..32].copy_from_slice(&version.offset.to_le_bytes());
        meta[32..36].copy_from_slice(&version.crc.to_le_bytes());
        let crc = crc32fast::hash(&meta[..36]);
        meta[36..40].copy_from_slice(&crc.to_le_bytes());
        let result = self
            .data
            .write_all_at(b, slot * PAGE as u64)
            .and_then(|_| self.meta.write_all_at(&meta, slot * META as u64));
        if result.is_err() {
            s.free.push(slot);
        } else {
            s.entries.put(
                page,
                Entry {
                    slot,
                    segment: version.segment,
                    offset: version.offset,
                    crc: version.crc,
                },
            );
        }
        result?;
        Ok(())
    }
    /// Preserve cached payload when compaction changes only its immutable address.
    pub fn rekey(&self, page: u64, old: &Ref, new: &Ref) -> Result<()> {
        if old.crc != new.crc {
            anyhow::bail!("cache rekey changes payload checksum");
        }
        if (old.segment, old.offset) == (new.segment, new.offset) {
            // Sealing changes segment_len, which the disposable cache does not
            // store. Avoid a metadata write for every page at each checkpoint.
            return Ok(());
        }
        let mut s = self.state[page as usize % self.state.len()].lock().unwrap();
        let Some(e) = s.entries.peek_mut(&page) else {
            return Ok(());
        };
        if (e.segment, e.offset, e.crc) != (old.segment, old.offset, old.crc) {
            return Ok(());
        }
        let mut meta = [0; META];
        meta[..8].copy_from_slice(&page.to_le_bytes());
        meta[8..24].copy_from_slice(new.segment.as_bytes());
        meta[24..32].copy_from_slice(&new.offset.to_le_bytes());
        meta[32..36].copy_from_slice(&new.crc.to_le_bytes());
        let crc = crc32fast::hash(&meta[..36]);
        meta[36..].copy_from_slice(&crc.to_le_bytes());
        self.meta.write_all_at(&meta, e.slot * META as u64)?;
        e.segment = new.segment;
        e.offset = new.offset;
        Ok(())
    }
}

struct Fill {
    first: u64,
    versions: Vec<(u64, Ref)>,
    data: Bytes,
    charge: usize,
    counters: Arc<WriterCounters>,
}
impl Drop for Fill {
    fn drop(&mut self) {
        self.counters.bytes.fetch_sub(self.charge, Ordering::AcqRel);
    }
}
#[derive(Default)]
struct WriterCounters {
    bytes: AtomicUsize,
    skipped: AtomicU64,
    errors: AtomicU64,
}
/// The queue is disposable. Dropping a fill never drops the authoritative WAL.
pub struct CacheWriter {
    send: Option<mpsc::SyncSender<Fill>>,
    thread: Option<std::thread::JoinHandle<()>>,
    counters: Arc<WriterCounters>,
    limit: usize,
}
impl CacheWriter {
    pub fn start(cache: Arc<PageCache>, limit: usize) -> std::io::Result<Self> {
        let (send, receive) = mpsc::sync_channel::<Fill>(256);
        let counters = Arc::new(WriterCounters::default());
        let thread = std::thread::Builder::new()
            .name("cache-fill".into())
            .spawn(move || {
                while let Ok(first) = receive.recv() {
                    let mut batch = vec![first];
                    batch.extend(receive.try_iter().take(63));
                    // Coalesce versions within a bounded batch. A racing older version
                    // may cause a later miss; expected-version checks forbid stale data.
                    let mut latest = std::collections::HashMap::new();
                    for (i, fill) in batch.iter().enumerate() {
                        for (p, r) in &fill.versions {
                            latest.insert(*p, (i, r));
                        }
                    }
                    for (p, (i, r)) in latest {
                        let fill = &batch[i];
                        let start = (p - fill.first) as usize * PAGE;
                        if cache.put(p, r, &fill.data[start..start + PAGE]).is_err() {
                            fill.counters.errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })?;
        Ok(Self {
            send: Some(send),
            thread: Some(thread),
            counters,
            limit,
        })
    }
    pub fn enqueue(&self, first: u64, versions: Vec<(u64, Ref)>, data: Bytes) {
        let charge = data.len() + versions.len() * (std::mem::size_of::<(u64, Ref)>() + 64);
        if self
            .counters
            .bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(charge).filter(|n| *n <= self.limit)
            })
            .is_err()
        {
            self.counters.skipped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let fill = Fill {
            first,
            versions,
            data,
            charge,
            counters: self.counters.clone(),
        };
        if self.send.as_ref().unwrap().try_send(fill).is_err() {
            self.counters.skipped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn status(&self) -> (usize, u64, u64) {
        (
            self.counters.bytes.load(Ordering::Relaxed),
            self.counters.skipped.load(Ordering::Relaxed),
            self.counters.errors.load(Ordering::Relaxed),
        )
    }
}
impl Drop for CacheWriter {
    fn drop(&mut self) {
        self.send.take();
        // Join before Engine releases its volume lock: a new opener must not race
        // writes from an old cache worker, especially after cache repartitioning.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn versions_eviction_reopen_and_torn_payload() -> Result<()> {
        let d = tempfile::tempdir()?;
        let volume = Uuid::new_v4();
        let b = vec![7; PAGE];
        let mut r = Ref {
            segment: Uuid::new_v4(),
            offset: 96,
            crc: crc32fast::hash(&b),
            segment_len: 8192,
        };
        let c = PageCache::open(d.path(), volume, (PAGE + META) as u64 * 2)?;
        c.put(0, &r, &b)?;
        c.put(1, &r, &b)?;
        assert_eq!(c.get(0, &r).unwrap().as_ref(), b);
        c.put(2, &r, &b)?;
        assert!(c.get(1, &r).is_none());
        r.offset += PAGE as u64;
        assert!(c.get(0, &r).is_none());
        c.put(0, &r, &b)?;
        drop(c);
        let c = PageCache::open(d.path(), volume, (PAGE + META) as u64 * 2)?;
        assert!(c.get(0, &r).is_some());
        let slot = c.state[0].lock().unwrap().entries.peek(&0).unwrap().slot;
        c.data.write_all_at(&[9], slot * PAGE as u64)?;
        assert!(c.get(0, &r).is_none());
        drop(c);
        let c = PageCache::open(d.path(), Uuid::new_v4(), (PAGE + META) as u64 * 2)?;
        assert!(c.get(2, &r).is_none());
        Ok(())
    }
}
