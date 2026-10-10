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
    sync::Mutex,
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
    state: Mutex<State>,
}
impl PageCache {
    pub fn open(dir: &Path, volume: Uuid, bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let cap = bytes / (PAGE + META) as u64;
        let marker = format!("{volume}:{cap}");
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
        let mut entries = LruCache::unbounded();
        let mut free = Vec::new();
        let mut scan = std::io::BufReader::new(&mut meta);
        for slot in 0..cap {
            let mut b = [0; META];
            scan.read_exact(&mut b)?;
            if crc32fast::hash(&b[..36]) != u32::from_le_bytes(b[36..40].try_into()?) {
                free.push(slot);
                continue;
            }
            let page = u64::from_le_bytes(b[..8].try_into()?);
            let entry = Entry {
                slot,
                segment: Uuid::from_slice(&b[8..24])?,
                offset: u64::from_le_bytes(b[24..32].try_into()?),
                crc: u32::from_le_bytes(b[32..36].try_into()?),
            };
            if let Some(old) = entries.put(page, entry) {
                free.push(old.slot);
            }
        }
        meta.seek(SeekFrom::Start(0))?;
        Ok(Self {
            data,
            meta,
            state: Mutex::new(State { entries, free }),
        })
    }
    pub fn get(&self, page: u64, expected: &Ref) -> Option<Bytes> {
        let mut s = self.state.lock().unwrap();
        let e = s.entries.get(&page)?;
        if (e.segment, e.offset, e.crc) != (expected.segment, expected.offset, expected.crc) {
            return None;
        }
        let mut b = vec![0; PAGE];
        if self
            .data
            .read_exact_at(&mut b, e.slot * PAGE as u64)
            .is_ok()
            && crc32fast::hash(&b) == expected.crc
        {
            return Some(b.into());
        }
        let e = s.entries.pop(&page)?;
        s.free.push(e.slot);
        None
    }
    pub fn put(&self, page: u64, version: &Ref, b: &[u8]) -> Result<()> {
        if b.len() != PAGE || crc32fast::hash(b) != version.crc {
            return Ok(());
        }
        let mut s = self.state.lock().unwrap();
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
        let slot = c.state.lock().unwrap().entries.peek(&0).unwrap().slot;
        c.data.write_all_at(&[9], slot * PAGE as u64)?;
        assert!(c.get(0, &r).is_none());
        drop(c);
        let c = PageCache::open(d.path(), Uuid::new_v4(), (PAGE + META) as u64 * 2)?;
        assert!(c.get(2, &r).is_none());
        Ok(())
    }
}
