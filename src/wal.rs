//! Append-only, checksummed 4 KiB page records. A torn final record is recoverable;
//! a complete record with a bad checksum is corruption and fails closed.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub const PAGE: usize = 4096;
pub const MAX_IO: usize = 8 * 1024 * 1024;
const HEADER: usize = 64;
const RECORD: usize = 32;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ref {
    pub segment: Uuid,
    pub offset: u64,
    pub crc: u32,
    pub segment_len: u64,
}
pub struct Segment {
    pub id: Uuid,
    pub path: PathBuf,
    pub file: File,
    pub len: u64,
    pub last_seq: u64,
}
pub struct Recovered {
    pub seq: u64,
    pub first: u64,
    pub pages: Vec<(Ref, bool)>,
    pub zero_count: u64,
}

pub fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
pub fn atomic(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    f.write_all(data)?;
    f.sync_all()?;
    std::fs::rename(tmp, path)?;
    sync_dir(path.parent().context("no parent")?)
}
impl Segment {
    pub fn create(dir: &Path, volume: Uuid, next: u64) -> Result<Self> {
        let id = Uuid::new_v4();
        let path = dir.join(format!("{next:020}-{id}.wal"));
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut h = [0u8; HEADER];
        h[..8].copy_from_slice(b"IDWAL001");
        h[8..24].copy_from_slice(id.as_bytes());
        h[24..40].copy_from_slice(volume.as_bytes());
        let crc = crc32fast::hash(&h[..60]);
        h[60..].copy_from_slice(&crc.to_le_bytes());
        file.write_all(&h)?;
        file.sync_all()?;
        sync_dir(dir)?;
        Ok(Self {
            id,
            path,
            file,
            len: HEADER as u64,
            last_seq: next - 1,
        })
    }
    pub fn open(
        path: PathBuf,
        volume: Uuid,
        size: u64,
        allow_tail: bool,
    ) -> Result<(Self, Vec<Recovered>)> {
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut h = [0u8; HEADER];
        file.read_exact(&mut h).context("truncated WAL header")?;
        ensure!(
            &h[..8] == b"IDWAL001"
                && crc32fast::hash(&h[..60]) == u32::from_le_bytes(h[60..].try_into()?),
            "invalid WAL header"
        );
        ensure!(
            &h[24..40] == volume.as_bytes(),
            "WAL belongs to another volume"
        );
        let id = Uuid::from_slice(&h[8..24])?;
        ensure!(
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(&format!("{id}.wal"))),
            "WAL filename identity mismatch"
        );
        let end = file.metadata()?.len();
        let mut pos = HEADER as u64;
        let mut rows = Vec::new();
        let mut last = 0;
        while pos < end {
            if end - pos < RECORD as u64 {
                ensure!(allow_tail, "truncated nonfinal WAL record");
                break;
            }
            let mut r = [0u8; RECORD];
            file.read_exact(&mut r)?;
            let zero = &r[..4] == b"ZER1";
            ensure!(
                zero || &r[..4] == b"WRT1",
                "invalid WAL record magic at {pos}"
            );
            let len = u32::from_le_bytes(r[4..8].try_into()?) as usize;
            let seq = u64::from_le_bytes(r[8..16].try_into()?);
            let first = u64::from_le_bytes(r[16..24].try_into()?);
            let count = u32::from_le_bytes(r[24..28].try_into()?) as usize;
            ensure!(
                count > 0
                    && ((zero && len == 0)
                        || (!zero && len == count * PAGE && len <= MAX_IO + PAGE)),
                "invalid WAL record length"
            );
            ensure!(
                first
                    .checked_add(count as u64)
                    .is_some_and(|n| n <= size / PAGE as u64),
                "WAL record outside volume"
            );
            ensure!(seq > last, "unordered WAL sequence");
            if end - pos - (RECORD as u64) < len as u64 {
                ensure!(allow_tail, "truncated nonfinal WAL payload");
                break;
            }
            let mut payload = vec![0; len];
            file.read_exact(&mut payload)?;
            let mut hash = crc32fast::Hasher::new();
            hash.update(&r[..28]);
            hash.update(&payload);
            ensure!(
                hash.finalize() == u32::from_le_bytes(r[28..32].try_into()?),
                "WAL checksum mismatch at {pos}"
            );
            let pages = payload
                .as_chunks::<PAGE>()
                .0
                .iter()
                .enumerate()
                .map(|(i, b)| {
                    (
                        Ref {
                            segment: id,
                            offset: pos + RECORD as u64 + (i * PAGE) as u64,
                            crc: crc32fast::hash(b),
                            segment_len: 0,
                        },
                        b.iter().all(|x| *x == 0),
                    )
                })
                .collect();
            rows.push(Recovered {
                seq,
                first,
                pages,
                zero_count: if zero { count as u64 } else { 0 },
            });
            last = seq;
            pos += (RECORD + len) as u64;
        }
        if pos != end {
            file.set_len(pos)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok((
            Self {
                id,
                path,
                file,
                len: pos,
                last_seq: last,
            },
            rows,
        ))
    }
    pub fn append(&mut self, seq: u64, first: u64, data: &[u8]) -> Result<Vec<(Ref, bool)>> {
        ensure!(
            !data.is_empty() && data.len().is_multiple_of(PAGE) && data.len() <= MAX_IO + PAGE,
            "invalid append size"
        );
        let mut h = [0u8; RECORD];
        h[..4].copy_from_slice(b"WRT1");
        h[4..8].copy_from_slice(&(data.len() as u32).to_le_bytes());
        h[8..16].copy_from_slice(&seq.to_le_bytes());
        h[16..24].copy_from_slice(&first.to_le_bytes());
        h[24..28].copy_from_slice(&((data.len() / PAGE) as u32).to_le_bytes());
        let mut crc = crc32fast::Hasher::new();
        crc.update(&h[..28]);
        crc.update(data);
        h[28..32].copy_from_slice(&crc.finalize().to_le_bytes());
        // The engine poisons the volume if either write fails: never append after a partial record.
        self.file.write_all(&h)?;
        self.file.write_all(data)?;
        let refs = data
            .as_chunks::<PAGE>()
            .0
            .iter()
            .enumerate()
            .map(|(i, b)| {
                (
                    Ref {
                        segment: self.id,
                        offset: self.len + RECORD as u64 + (i * PAGE) as u64,
                        crc: crc32fast::hash(b),
                        segment_len: 0,
                    },
                    b.iter().all(|x| *x == 0),
                )
            })
            .collect();
        self.len += (RECORD + data.len()) as u64;
        self.last_seq = seq;
        Ok(refs)
    }
    pub fn read(file: &File, r: &Ref) -> Result<Vec<u8>> {
        let mut b = vec![0; PAGE];
        file.read_exact_at(&mut b, r.offset)?;
        ensure!(crc32fast::hash(&b) == r.crc, "local page checksum mismatch");
        Ok(b)
    }
    pub fn zero(&mut self, seq: u64, first: u64, count: u64) -> Result<()> {
        ensure!(
            count > 0 && count <= u32::MAX as u64,
            "zero range too large"
        );
        let mut h = [0; RECORD];
        h[..4].copy_from_slice(b"ZER1");
        h[8..16].copy_from_slice(&seq.to_le_bytes());
        h[16..24].copy_from_slice(&first.to_le_bytes());
        h[24..28].copy_from_slice(&(count as u32).to_le_bytes());
        let crc = crc32fast::hash(&h[..28]);
        h[28..].copy_from_slice(&crc.to_le_bytes());
        self.file.write_all(&h)?;
        self.len += RECORD as u64;
        self.last_seq = seq;
        Ok(())
    }
}

pub struct Watermark {
    file: File,
    pub seq: u64,
    next_slot: u64,
}
impl Watermark {
    pub fn create(path: &Path) -> Result<Self> {
        let f = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        f.set_len(32)?;
        let mut w = Self {
            file: f,
            seq: 0,
            next_slot: 0,
        };
        w.persist(0)?;
        sync_dir(path.parent().unwrap())?;
        Ok(w)
    }
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut b = [0; 32];
        file.read_exact_at(&mut b, 0)?;
        let valid: Vec<(u64, u64)> = b
            .as_chunks::<16>()
            .0
            .iter()
            .enumerate()
            .filter_map(|(slot, s)| {
                if &s[..4] == b"IDWM"
                    && crc32fast::hash(&s[..12]) == u32::from_le_bytes(s[12..].try_into().ok()?)
                {
                    Some((u64::from_le_bytes(s[4..12].try_into().ok()?), slot as u64))
                } else {
                    None
                }
            })
            .collect();
        let Some((seq, slot)) = valid.into_iter().max() else {
            bail!("no valid durability watermark")
        };
        Ok(Self {
            file,
            seq,
            next_slot: slot ^ 1,
        })
    }
    pub fn persist(&mut self, seq: u64) -> Result<()> {
        // Alternate independently checksummed slots. Generation increments even for repeated seq.
        let slot = self.next_slot;
        let mut b = [0; 16];
        b[..4].copy_from_slice(b"IDWM");
        b[4..12].copy_from_slice(&seq.to_le_bytes());
        let crc = crc32fast::hash(&b[..12]);
        b[12..].copy_from_slice(&crc.to_le_bytes());
        self.file.write_all_at(&b, slot * 16)?;
        self.file.sync_all()?;
        self.seq = seq;
        self.next_slot ^= 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tail_is_recovered_but_complete_corruption_is_rejected() -> Result<()> {
        let t = tempfile::tempdir()?;
        let v = Uuid::new_v4();
        let mut s = Segment::create(t.path(), v, 1)?;
        s.append(1, 0, &vec![7; PAGE])?;
        s.file.sync_all()?;
        s.file.write_all(b"WRT")?;
        let p = s.path.clone();
        drop(s);
        let (s, r) = Segment::open(p.clone(), v, 4096 * 4, true)?;
        assert_eq!(r.len(), 1);
        s.file
            .write_all_at(&[9], HEADER as u64 + RECORD as u64 + 11)?;
        drop(s);
        assert!(Segment::open(p, v, 4096 * 4, true).is_err());
        Ok(())
    }
    #[test]
    fn watermark_survives_one_torn_slot() -> Result<()> {
        let t = tempfile::tempdir()?;
        let p = t.path().join("durable");
        let mut w = Watermark::create(&p)?;
        w.persist(1)?;
        w.persist(2)?;
        w.file.write_all_at(&[0; 16], 0)?;
        drop(w);
        assert!(Watermark::open(&p)?.seq >= 1);
        Ok(())
    }
}
