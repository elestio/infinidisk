//! Append-only, checksummed 4 KiB page records. A torn final record is recoverable;
//! a complete record with a bad checksum is corruption and fails closed.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{self, IoSlice, Read, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
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
    vectored: bool,
}
pub struct Recovered {
    pub seq: u64,
    pub first: u64,
    pub pages: Vec<(Ref, bool)>,
    pub zero_count: u64,
}

/// Validate exactly the immutable bytes that will be uploaded. A damaged local
/// journal must never replace the last good remote checkpoint.
pub fn validate_upload(
    data: &[u8],
    volume: Uuid,
    id: Uuid,
    last_seq: u64,
    size: u64,
) -> Result<()> {
    let h = data.get(..HEADER).context("truncated sealed WAL header")?;
    ensure!(
        &h[..8] == b"IDWAL001"
            && &h[8..24] == id.as_bytes()
            && &h[24..40] == volume.as_bytes()
            && crc32fast::hash(&h[..60]) == u32::from_le_bytes(h[60..64].try_into()?),
        "invalid sealed WAL identity/header"
    );
    let mut pos = HEADER;
    let mut previous = 0;
    while pos < data.len() {
        let r = data
            .get(pos..pos + RECORD)
            .context("truncated sealed WAL record")?;
        let zero = &r[..4] == b"ZER1";
        ensure!(
            zero || &r[..4] == b"WRT1",
            "invalid sealed WAL record magic"
        );
        let len = u32::from_le_bytes(r[4..8].try_into()?) as usize;
        let seq = u64::from_le_bytes(r[8..16].try_into()?);
        let first = u64::from_le_bytes(r[16..24].try_into()?);
        let count = u32::from_le_bytes(r[24..28].try_into()?) as usize;
        ensure!(
            count > 0
                && ((zero && len == 0)
                    || (!zero && count.checked_mul(PAGE) == Some(len) && len <= MAX_IO + PAGE)),
            "invalid sealed WAL record length"
        );
        ensure!(
            seq > previous
                && first
                    .checked_add(count as u64)
                    .is_some_and(|end| end <= size / PAGE as u64),
            "invalid sealed WAL sequence/range"
        );
        let end = (pos + RECORD)
            .checked_add(len)
            .context("sealed WAL length overflow")?;
        let payload = data
            .get(pos + RECORD..end)
            .context("truncated sealed WAL payload")?;
        let mut hash = crc32fast::Hasher::new();
        hash.update(&r[..28]);
        hash.update(payload);
        ensure!(
            hash.finalize() == u32::from_le_bytes(r[28..32].try_into()?),
            "sealed WAL checksum mismatch at {pos}"
        );
        previous = seq;
        pos = end;
    }
    ensure!(
        data.len() == HEADER || previous == last_seq,
        "sealed WAL frontier mismatch"
    );
    Ok(())
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
// writev can stop inside either slice or be interrupted. Never append another
// record until both slices are written, and poison the engine on any other error.
fn write_record<W: Write>(writer: &mut W, header: &[u8], data: &[u8]) -> io::Result<()> {
    let mut slices = [IoSlice::new(header), IoSlice::new(data)];
    let mut remaining = &mut slices[..];
    while !remaining.is_empty() {
        match writer.write_vectored(remaining) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => IoSlice::advance_slices(&mut remaining, n),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
fn allocate(file: &File, mode: i32, offset: u64, len: u64) -> Result<()> {
    let offset = i64::try_from(offset)?;
    let len = i64::try_from(len)?;
    loop {
        // SAFETY: live file descriptor, checked nonnegative offsets, no pointers.
        if unsafe { libc::fallocate(file.as_raw_fd(), mode, offset, len) } == 0 {
            return Ok(());
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e.into());
        }
    }
}
impl Segment {
    pub fn create(dir: &Path, volume: Uuid, next: u64) -> Result<Self> {
        Self::create_with_options(dir, volume, next, 0, true)
    }
    pub fn create_with_options(
        dir: &Path,
        volume: Uuid,
        next: u64,
        reserve: u64,
        vectored: bool,
    ) -> Result<Self> {
        let id = Uuid::new_v4();
        let path = dir.join(format!("{next:020}-{id}.wal"));
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        if reserve > 0 {
            allocate(&file, libc::FALLOC_FL_KEEP_SIZE, 0, reserve).context("reserve WAL blocks")?;
        }
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
            vectored,
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
                vectored: true,
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
        if self.vectored {
            write_record(&mut self.file, &h, data)?;
        } else {
            self.file.write_all(&h)?;
            self.file.write_all(data)?;
        }
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
    /// Release reserved blocks strictly beyond EOF before retaining/uploading a sealed segment.
    pub fn release_reservation(&self, reserve: u64) -> Result<()> {
        let start = self.len.div_ceil(PAGE as u64) * PAGE as u64;
        if reserve > start {
            allocate(
                &self.file,
                libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_PUNCH_HOLE,
                start,
                reserve - start,
            )
            .context("release unused WAL reservation")?;
        }
        Ok(())
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
        self.persist_using(seq, false)
    }
    pub fn persist_data(&mut self, seq: u64) -> Result<()> {
        self.persist_using(seq, true)
    }
    fn persist_using(&mut self, seq: u64, data_only: bool) -> Result<()> {
        // Alternate independently checksummed slots. Generation increments even for repeated seq.
        let slot = self.next_slot;
        let mut b = [0; 16];
        b[..4].copy_from_slice(b"IDWM");
        b[4..12].copy_from_slice(&seq.to_le_bytes());
        let crc = crc32fast::hash(&b[..12]);
        b[12..].copy_from_slice(&crc.to_le_bytes());
        self.file.write_all_at(&b, slot * 16)?;
        if data_only {
            self.file.sync_data()?;
        } else {
            self.file.sync_all()?;
        }
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
    fn vectored_record_handles_short_writes_interruptions_and_zero_progress() -> Result<()> {
        struct Short {
            bytes: Vec<u8>,
            interrupt: bool,
        }
        impl Write for Short {
            fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                if self.interrupt {
                    self.interrupt = false;
                    return Err(io::ErrorKind::Interrupted.into());
                }
                let n = b.len().min(3);
                self.bytes.extend_from_slice(&b[..n]);
                Ok(n)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut short = Short {
            bytes: Vec::new(),
            interrupt: true,
        };
        write_record(&mut short, b"header!", b"payload!")?;
        assert_eq!(short.bytes, b"header!payload!");
        let mut empty = &mut [][..];
        assert_eq!(
            write_record(&mut empty, b"h", b"p").unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        Ok(())
    }
    #[test]
    fn reservation_preserves_eof_recovery_and_releases_unused_blocks() -> Result<()> {
        use std::os::unix::fs::MetadataExt;
        let t = tempfile::tempdir()?;
        let v = Uuid::new_v4();
        let reserve = 1024 * 1024;
        let mut s = Segment::create_with_options(t.path(), v, 1, reserve, true)?;
        assert_eq!(s.file.metadata()?.len(), HEADER as u64);
        assert!(s.file.metadata()?.blocks() * 512 >= reserve);
        s.append(1, 0, &vec![7; PAGE])?;
        s.file.sync_data()?;
        s.release_reservation(reserve)?;
        assert!(s.file.metadata()?.blocks() * 512 <= s.len.div_ceil(PAGE as u64) * PAGE as u64);
        let path = s.path.clone();
        let len = s.len;
        drop(s);
        let (s, records) = Segment::open(path, v, PAGE as u64 * 4, false)?;
        assert_eq!(s.len, len);
        assert_eq!(records.len(), 1);
        assert_eq!(
            Segment::read(&s.file, &records[0].pages[0].0)?,
            vec![7; PAGE]
        );
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
