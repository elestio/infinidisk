//! Append-only, checksummed 4 KiB page records. A torn final record is recoverable;
//! a complete record with a bad checksum is corruption and fails closed.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fs::{File, OpenOptions},
    io::{self, IoSlice, Read, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileExt, MetadataExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

pub const PAGE: usize = 4096;
pub const MAX_IO: usize = 8 * 1024 * 1024;
const HEADER: usize = 64;
const RECORD: usize = 32;
const MAX_CAPACITY: u64 = 80 * 1024 * 1024;

/// IDWAL002 aligns the file header, record headers, and payload pages to 4 KiB.
/// Padding must be zero and is validated during recovery and before publication.
/// The format is opt-in: existing IDWAL001 journals continue to be readable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Format {
    #[default]
    Legacy,
    Aligned,
}
impl Format {
    pub const fn header_len(self) -> usize {
        match self {
            Self::Legacy => HEADER,
            Self::Aligned => PAGE,
        }
    }
    pub const fn record_overhead(self) -> usize {
        match self {
            Self::Legacy => RECORD,
            Self::Aligned => PAGE,
        }
    }
    fn magic(self) -> &'static [u8; 8] {
        match self {
            Self::Legacy => b"IDWAL001",
            Self::Aligned => b"IDWAL002",
        }
    }
    fn from_header(h: &[u8]) -> Result<Self> {
        match h.get(..8) {
            Some(b"IDWAL001") => Ok(Self::Legacy),
            Some(b"IDWAL002") => Ok(Self::Aligned),
            _ => bail!("invalid WAL format"),
        }
    }
    fn valid_capacity(self, capacity: u64) -> bool {
        capacity == 0
            || (capacity >= self.header_len() as u64
                && capacity <= MAX_CAPACITY
                && (self == Self::Legacy || capacity.is_multiple_of(PAGE as u64)))
    }
}
fn header(format: Format, id: Uuid, volume: Uuid, next: u64, capacity: u64) -> Vec<u8> {
    let mut h = vec![0; format.header_len()];
    h[..8].copy_from_slice(format.magic());
    h[8..24].copy_from_slice(id.as_bytes());
    h[24..40].copy_from_slice(volume.as_bytes());
    h[40..48].copy_from_slice(&next.to_le_bytes());
    h[48..56].copy_from_slice(&capacity.to_le_bytes());
    let crc = crc32fast::hash(&h[..60]);
    h[60..64].copy_from_slice(&crc.to_le_bytes());
    h
}

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
    pub commit_seq: u64,
    pub capacity: u64,
    pub format: Format,
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
    let format = Format::from_header(h)?;
    ensure!(
        &h[8..24] == id.as_bytes()
            && &h[24..40] == volume.as_bytes()
            && crc32fast::hash(&h[..60]) == u32::from_le_bytes(h[60..64].try_into()?),
        "invalid sealed WAL identity/header"
    );
    ensure!(
        data.get(HEADER..format.header_len())
            .is_some_and(|padding| padding.iter().all(|b| *b == 0)),
        "invalid sealed WAL header padding"
    );
    let first_seq = u64::from_le_bytes(h[40..48].try_into()?);
    ensure!(first_seq > 0, "invalid sealed WAL first sequence");
    let capacity = u64::from_le_bytes(h[48..56].try_into()?);
    ensure!(
        format.valid_capacity(capacity) && (capacity == 0 || data.len() as u64 <= capacity),
        "invalid sealed WAL capacity"
    );
    let overhead = format.record_overhead();
    let mut pos = format.header_len();
    let mut previous = first_seq - 1;
    while pos < data.len() {
        let r = data
            .get(pos..pos + overhead)
            .context("truncated sealed WAL record")?;
        ensure!(
            r[RECORD..].iter().all(|b| *b == 0),
            "invalid sealed WAL record padding"
        );
        let commit = &r[..4] == b"CMT1";
        let zero = &r[..4] == b"ZER1";
        ensure!(
            commit || zero || &r[..4] == b"WRT1",
            "invalid sealed WAL record magic"
        );
        let len = u32::from_le_bytes(r[4..8].try_into()?) as usize;
        let seq = u64::from_le_bytes(r[8..16].try_into()?);
        let first = u64::from_le_bytes(r[16..24].try_into()?);
        let count = u32::from_le_bytes(r[24..28].try_into()?) as usize;
        if commit {
            ensure!(
                len == 0 && count == 0 && first == 0 && seq == previous,
                "invalid sealed commit marker"
            );
            ensure!(
                crc32fast::hash(&r[..28]) == u32::from_le_bytes(r[28..32].try_into()?),
                "sealed commit CRC mismatch"
            );
            pos += overhead;
            continue;
        }
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
        let end = (pos + overhead)
            .checked_add(len)
            .context("sealed WAL length overflow")?;
        let payload = data
            .get(pos + overhead..end)
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
    ensure!(previous == last_seq, "sealed WAL frontier mismatch");
    Ok(())
}
/// Build an immutable remote segment from the latest pages of a checkpoint.
/// This container is NOT a local recovery journal: its record sequences are
/// internal, starting at one, independent of the engine's durability frontier.
///
/// Inputs must be strictly increasing logical page numbers. Contiguous pages
/// share a WRT1 record up to MAX_IO bytes; noncontiguous pages start a new record.
/// The caller must verify all source WALs before replacing their references.
/// Every result is validated with the normal sealed-WAL parser before return.
/// There are no filesystem writes or durability barriers on this path.
pub fn pack_pages(
    volume: Uuid,
    pages: &[(u64, Vec<u8>)],
    volume_size: u64,
) -> Result<(Uuid, Vec<u8>, BTreeMap<u64, Ref>)> {
    ensure!(!pages.is_empty(), "cannot pack an empty page set");
    ensure!(
        volume_size > 0 && volume_size.is_multiple_of(PAGE as u64),
        "invalid pack volume size"
    );
    let maximum_length = pages
        .len()
        .checked_mul(PAGE + RECORD)
        .and_then(|length| length.checked_add(HEADER))
        .context("packed WAL size overflow")?;
    ensure!(
        maximum_length <= MAX_CAPACITY as usize,
        "packed WAL exceeds maximum segment size"
    );
    let mut previous_page = None;
    for (page, bytes) in pages {
        ensure!(
            bytes.len() == PAGE && *page < volume_size / PAGE as u64,
            "invalid packed page size/range"
        );
        ensure!(
            previous_page.is_none_or(|previous| previous < *page),
            "packed pages must be sorted and unique"
        );
        previous_page = Some(*page);
    }
    let id = Uuid::new_v4();
    let mut bytes = Vec::with_capacity(maximum_length);
    bytes.extend_from_slice(&header(Format::Legacy, id, volume, 1, 0));
    let mut refs = BTreeMap::new();
    let mut start = 0;
    let mut seq = 0u64;
    while start < pages.len() {
        let mut end = start + 1;
        while end < pages.len()
            && end - start < MAX_IO / PAGE
            && pages[end].0 == pages[end - 1].0 + 1
        {
            end += 1;
        }
        seq += 1;
        let count = end - start;
        let mut record = [0u8; RECORD];
        record[..4].copy_from_slice(b"WRT1");
        record[4..8].copy_from_slice(&((count * PAGE) as u32).to_le_bytes());
        record[8..16].copy_from_slice(&seq.to_le_bytes());
        record[16..24].copy_from_slice(&pages[start].0.to_le_bytes());
        record[24..28].copy_from_slice(&(count as u32).to_le_bytes());
        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&record[..28]);
        for (_, data) in &pages[start..end] {
            checksum.update(data);
        }
        record[28..32].copy_from_slice(&checksum.finalize().to_le_bytes());
        bytes.extend_from_slice(&record);
        for (page, data) in &pages[start..end] {
            refs.insert(
                *page,
                Ref {
                    segment: id,
                    offset: bytes.len() as u64,
                    crc: crc32fast::hash(data),
                    segment_len: 0,
                },
            );
            bytes.extend_from_slice(data);
        }
        start = end;
    }
    let length = bytes.len() as u64;
    for reference in refs.values_mut() {
        reference.segment_len = length;
    }
    validate_upload(&bytes, volume, id, seq, volume_size)?;
    Ok((id, bytes, refs))
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
        Self::create_with_format(dir, volume, next, reserve, vectored, Format::Legacy)
    }
    pub fn create_with_format(
        dir: &Path,
        volume: Uuid,
        next: u64,
        reserve: u64,
        vectored: bool,
        format: Format,
    ) -> Result<Self> {
        ensure!(next > 0, "invalid WAL first sequence");
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
        file.write_all(&header(format, id, volume, next, 0))?;
        file.sync_all()?;
        sync_dir(dir)?;
        Ok(Self {
            id,
            path,
            file,
            len: format.header_len() as u64,
            last_seq: next - 1,
            commit_seq: 0,
            capacity: 0,
            format,
            vectored,
        })
    }
    pub fn header_len(&self) -> usize {
        self.format.header_len()
    }
    pub fn record_overhead(&self) -> usize {
        self.format.record_overhead()
    }
    pub fn is_empty(&self) -> bool {
        self.len == self.header_len() as u64
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
        let format = Format::from_header(&h)?;
        ensure!(
            crc32fast::hash(&h[..60]) == u32::from_le_bytes(h[60..].try_into()?),
            "invalid WAL header"
        );
        ensure!(
            &h[24..40] == volume.as_bytes(),
            "WAL belongs to another volume"
        );
        let id = Uuid::from_slice(&h[8..24])?;
        let first_seq = u64::from_le_bytes(h[40..48].try_into()?);
        ensure!(first_seq > 0, "invalid WAL first sequence");
        let mut padding = vec![0; format.header_len() - HEADER];
        file.read_exact(&mut padding)
            .context("truncated WAL header padding")?;
        ensure!(
            padding.iter().all(|b| *b == 0),
            "invalid WAL header padding"
        );
        ensure!(
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n == format!("{first_seq:020}-{id}.wal")),
            "WAL filename identity mismatch"
        );
        let end = file.metadata()?.len();
        let capacity = u64::from_le_bytes(h[48..56].try_into()?);
        ensure!(
            format.valid_capacity(capacity) && (capacity == 0 || end <= capacity),
            "invalid fixed WAL capacity"
        );
        let overhead = format.record_overhead();
        let mut pos = format.header_len() as u64;
        let mut rows = Vec::new();
        let mut last = first_seq - 1;
        let mut commit_seq = 0;
        let mut record_header = [0u8; PAGE];
        while pos < end {
            if end - pos < overhead as u64 {
                ensure!(allow_tail, "truncated nonfinal WAL record");
                break;
            }
            let r = &mut record_header[..overhead];
            file.read_exact(r)?;
            if capacity > 0 && r.iter().all(|b| *b == 0) {
                // Zero padding is allowed only when all remaining bytes are zero.
                // A hole in front of a later record/commit is corruption.
                let mut remaining = end - pos - overhead as u64;
                let mut b = [0; 65536];
                while remaining > 0 {
                    let n = (remaining as usize).min(b.len());
                    file.read_exact(&mut b[..n])?;
                    ensure!(
                        b[..n].iter().all(|v| *v == 0),
                        "nonzero data after fixed WAL padding"
                    );
                    remaining -= n as u64;
                }
                break;
            }
            ensure!(
                r[RECORD..].iter().all(|b| *b == 0),
                "invalid WAL record padding"
            );
            let commit = &r[..4] == b"CMT1";
            let zero = &r[..4] == b"ZER1";
            ensure!(
                commit || zero || &r[..4] == b"WRT1",
                "invalid WAL record magic at {pos}"
            );
            let len = u32::from_le_bytes(r[4..8].try_into()?) as usize;
            let seq = u64::from_le_bytes(r[8..16].try_into()?);
            let first = u64::from_le_bytes(r[16..24].try_into()?);
            let count = u32::from_le_bytes(r[24..28].try_into()?) as usize;
            if commit {
                ensure!(
                    len == 0 && count == 0 && first == 0 && seq == last,
                    "invalid WAL commit marker"
                );
                ensure!(
                    crc32fast::hash(&r[..28]) == u32::from_le_bytes(r[28..32].try_into()?),
                    "WAL commit CRC mismatch"
                );
                commit_seq = seq;
                pos += overhead as u64;
                continue;
            }
            ensure!(
                count > 0
                    && ((zero && len == 0)
                        || (!zero && count.checked_mul(PAGE) == Some(len) && len <= MAX_IO + PAGE)),
                "invalid WAL record length"
            );
            ensure!(
                first
                    .checked_add(count as u64)
                    .is_some_and(|n| n <= size / PAGE as u64),
                "WAL record outside volume"
            );
            ensure!(seq > last, "unordered WAL sequence");
            if end - pos - (overhead as u64) < len as u64 {
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
                            offset: pos + overhead as u64 + (i * PAGE) as u64,
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
            pos += (overhead + len) as u64;
        }
        if capacity == 0 && pos != end {
            file.set_len(pos)?;
            file.sync_all()?;
        }
        if capacity > 0 && end != capacity {
            // Only creation interrupted before the first record may have a short file.
            // A truncated file containing records is not a recoverable cache tail.
            ensure!(rows.is_empty(), "truncated fixed WAL containing records");
            file.set_len(capacity)?;
            file.sync_data()?;
        }
        file.seek(SeekFrom::Start(pos))?;
        Ok((
            Self {
                id,
                path,
                file,
                len: pos,
                last_seq: last,
                commit_seq,
                capacity,
                format,
                vectored: true,
            },
            rows,
        ))
    }
    pub fn storage_bytes(&self) -> u64 {
        self.len.max(self.capacity)
    }
    pub fn initialize_capacity(&mut self, capacity: u64) -> Result<()> {
        ensure!(
            self.is_empty() && capacity > 0 && self.format.valid_capacity(capacity),
            "invalid WAL initialization"
        );
        let mut h = [0; HEADER];
        self.file.read_exact_at(&mut h, 0)?;
        h[48..56].copy_from_slice(&capacity.to_le_bytes());
        let crc = crc32fast::hash(&h[..60]);
        h[60..64].copy_from_slice(&crc.to_le_bytes());
        self.file.write_all_at(&h, 0)?;
        self.file.set_len(capacity)?;
        let zeros = [0; 65536];
        let mut off = self.header_len() as u64;
        while off < capacity {
            let n = ((capacity - off) as usize).min(zeros.len());
            self.file.write_all_at(&zeros[..n], off)?;
            off += n as u64;
        }
        self.file.sync_data()?;
        self.file.seek(SeekFrom::Start(self.header_len() as u64))?;
        self.capacity = capacity;
        Ok(())
    }
    pub fn commit(&mut self, seq: u64) -> Result<()> {
        ensure!(
            self.capacity == 0 || self.len + self.record_overhead() as u64 <= self.capacity,
            "fixed WAL commit exceeds capacity"
        );
        ensure!(seq == self.last_seq, "commit does not match the WAL tail");
        let mut record_header = [0u8; PAGE];
        let h = &mut record_header[..self.record_overhead()];
        h[..4].copy_from_slice(b"CMT1");
        h[8..16].copy_from_slice(&seq.to_le_bytes());
        let crc = crc32fast::hash(&h[..28]);
        h[28..32].copy_from_slice(&crc.to_le_bytes());
        self.file.write_all(h)?;
        self.len += self.record_overhead() as u64;
        self.commit_seq = seq;
        Ok(())
    }
    pub fn append(&mut self, seq: u64, first: u64, data: &[u8]) -> Result<Vec<(Ref, bool)>> {
        ensure!(seq > self.last_seq, "unordered WAL append sequence");
        ensure!(
            !data.is_empty() && data.len().is_multiple_of(PAGE) && data.len() <= MAX_IO + PAGE,
            "invalid append size"
        );
        ensure!(
            self.capacity == 0
                || self.len + (self.record_overhead() + data.len()) as u64 <= self.capacity,
            "fixed WAL append exceeds capacity"
        );
        let mut record_header = [0u8; PAGE];
        let h = &mut record_header[..self.record_overhead()];
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
            write_record(&mut self.file, h, data)?;
        } else {
            self.file.write_all(h)?;
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
                        offset: self.len + self.record_overhead() as u64 + (i * PAGE) as u64,
                        crc: crc32fast::hash(b),
                        segment_len: 0,
                    },
                    b.iter().all(|x| *x == 0),
                )
            })
            .collect();
        self.len += (self.record_overhead() + data.len()) as u64;
        self.last_seq = seq;
        Ok(refs)
    }
    /// Release allocation strictly beyond physical EOF. Linux ext4 may ignore
    /// PUNCH_HOLE entirely beyond i_size; ftruncate to the *unchanged* EOF removes
    /// those unwritten extents. Fixed-capacity initialized extents stay intact.
    pub fn release_reservation(&self, reserve: u64) -> Result<()> {
        let end = self.storage_bytes();
        if reserve > end.div_ceil(PAGE as u64) * PAGE as u64 {
            ensure!(
                self.file.metadata()?.len() == end,
                "WAL physical length changed"
            );
            self.file
                .set_len(end)
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
        ensure!(seq > self.last_seq, "unordered WAL zero sequence");
        ensure!(
            count > 0 && count <= u32::MAX as u64,
            "zero range too large"
        );
        ensure!(
            self.capacity == 0 || self.len + self.record_overhead() as u64 <= self.capacity,
            "fixed WAL zero exceeds capacity"
        );
        let mut record_header = [0u8; PAGE];
        let h = &mut record_header[..self.record_overhead()];
        h[..4].copy_from_slice(b"ZER1");
        h[8..16].copy_from_slice(&seq.to_le_bytes());
        h[16..24].copy_from_slice(&first.to_le_bytes());
        h[24..28].copy_from_slice(&(count as u32).to_le_bytes());
        let crc = crc32fast::hash(&h[..28]);
        h[28..32].copy_from_slice(&crc.to_le_bytes());
        self.file.write_all(h)?;
        self.len += self.record_overhead() as u64;
        self.last_seq = seq;
        Ok(())
    }
}

/// A file whose expensive allocation and initialization completed before the
/// engine takes its write-state lock. Keep these in a sibling directory, never
/// in the WAL directory: recovery accepts only fully activated `*.wal` files.
///
/// Preparation deliberately writes zeros instead of making sparse holes. That
/// preserves initialized extents for the fixed-capacity sync-data fast path.
/// Files left behind by an interrupted preparation are disposable. They must be
/// unlinked, not adopted or zeroed in-place without going through `try_retire`.
pub struct PreparedSegment {
    path: PathBuf,
    file: File,
    capacity: u64,
    reserve: u64,
    format: Format,
    vectored: bool,
}

/// Publication and reader exclusion are checked before a retired inode may be
/// modified. The engine must remove the Arc from its lookup map first. Existing
/// reads retain Arc leases; a busy result transfers ownership back unchanged.
pub enum Retirement {
    Ready(RetiredSegment),
    Busy {
        segment: Segment,
        readers: Arc<File>,
    },
}

pub struct RetiredSegment {
    path: PathBuf,
    file: File,
}

// Linux renameat2 prevents even an accidental collision from replacing a WAL.
// Both paths must be on the same filesystem. A failure leaves the source intact.
fn rename_new(source: &Path, destination: &Path) -> Result<()> {
    let source = CString::new(source.as_os_str().as_bytes())?;
    let destination = CString::new(destination.as_os_str().as_bytes())?;
    // SAFETY: valid NUL-terminated paths; AT_FDCWD resolves absolute/relative
    // paths exactly as the standard filesystem operations used elsewhere.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

fn prepare_storage(file: &File, reserve: u64, capacity: u64, format: Format) -> Result<()> {
    ensure!(
        format.valid_capacity(capacity),
        "invalid prepared WAL capacity"
    );
    ensure!(reserve <= MAX_CAPACITY, "invalid prepared WAL reservation");
    let length = capacity.max(format.header_len() as u64);
    // Fixed-capacity reuse keeps the inode and extents; it never truncates to
    // zero and reallocates them. The old WAL name is already durably removed.
    file.set_len(length)?;
    if reserve > length {
        allocate(file, libc::FALLOC_FL_KEEP_SIZE, 0, reserve)?;
    }
    let zeros = [0; 65536];
    let mut offset = 0;
    while offset < length {
        let count = ((length - offset) as usize).min(zeros.len());
        file.write_all_at(&zeros[..count], offset)?;
        offset += count as u64;
    }
    file.sync_all()?;
    Ok(())
}

impl PreparedSegment {
    /// Blocking work. Call from a bounded preparation worker outside write locks.
    pub fn prepare(
        pool_dir: &Path,
        reserve: u64,
        capacity: u64,
        vectored: bool,
        format: Format,
    ) -> Result<Self> {
        ensure!(
            format.valid_capacity(capacity),
            "invalid prepared WAL capacity"
        );
        std::fs::create_dir_all(pool_dir)?;
        let path = pool_dir.join(format!("{}.prepared", Uuid::new_v4()));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        if let Err(error) = prepare_storage(&file, reserve, capacity, format) {
            let _ = std::fs::remove_file(&path);
            return Err(error.context("prepare WAL storage"));
        }
        Ok(Self {
            path,
            file,
            capacity,
            reserve,
            format,
            vectored,
        })
    }

    pub fn storage_bytes(&self) -> u64 {
        self.capacity
            .max(self.reserve)
            .max(self.format.header_len() as u64)
    }

    /// Dispose of an unused prepared inode when a bounded queue is full or stops.
    pub fn discard(self) -> Result<()> {
        std::fs::remove_file(&self.path)?;
        sync_dir(self.path.parent().context("prepared WAL has no parent")?)
    }

    /// Bind the prepared bytes to a fresh segment UUID and sequence, persist the
    /// header, then publish its directory entry. Successful return means both
    /// file and name are durable. No payload initialization happens on this path.
    ///
    /// This still requires a file barrier and directory barriers: acknowledging
    /// writes before those complete could lose the active segment after a crash.
    pub fn activate(mut self, wal_dir: &Path, volume: Uuid, next: u64) -> Result<Segment> {
        ensure!(next > 0, "invalid WAL first sequence");
        let id = Uuid::new_v4();
        let path = wal_dir.join(format!("{next:020}-{id}.wal"));
        self.file
            .write_all_at(&header(self.format, id, volume, next, self.capacity), 0)?;
        self.file.sync_all()?;
        rename_new(&self.path, &path).context("activate prepared WAL")?;
        sync_dir(wal_dir)?;
        sync_dir(self.path.parent().context("prepared WAL has no parent")?)?;
        self.file
            .seek(SeekFrom::Start(self.format.header_len() as u64))?;
        Ok(Segment {
            id,
            path,
            file: self.file,
            len: self.format.header_len() as u64,
            last_seq: next - 1,
            commit_seq: 0,
            capacity: self.capacity,
            format: self.format,
            vectored: self.vectored,
        })
    }
}

impl RetiredSegment {
    pub fn storage_bytes(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Drop a safely retired inode instead of preparing it for another turn.
    pub fn discard(self) -> Result<()> {
        std::fs::remove_file(&self.path)?;
        sync_dir(self.path.parent().context("retired WAL has no parent")?)
    }

    /// Only a published segment with a matching, exclusive Arc read lease may
    /// enter the pool. Reader Files must never be independently cloned out of
    /// this Arc: every concurrent reader must retain an Arc until its I/O ends.
    /// A Busy result performs no filesystem mutation and retains both handles.
    pub fn try_retire(
        segment: Segment,
        readers: Arc<File>,
        published_seq: u64,
        pool_dir: &Path,
    ) -> Result<Retirement> {
        ensure!(
            segment.last_seq <= published_seq,
            "cannot recycle an unpublished WAL"
        );
        let metadata = segment.file.metadata()?;
        let reader_metadata = readers.metadata()?;
        let path_metadata = std::fs::symlink_metadata(&segment.path)?;
        ensure!(
            path_metadata.file_type().is_file()
                && path_metadata.dev() == metadata.dev()
                && path_metadata.ino() == metadata.ino(),
            "WAL pathname no longer belongs to the retired inode"
        );
        ensure!(
            metadata.dev() == reader_metadata.dev() && metadata.ino() == reader_metadata.ino(),
            "WAL read lease belongs to another inode"
        );
        ensure!(
            metadata.nlink() == 1,
            "cannot recycle a WAL with multiple links"
        );
        let reader_file = match Arc::try_unwrap(readers) {
            Ok(file) => file,
            Err(readers) => return Ok(Retirement::Busy { segment, readers }),
        };
        drop(reader_file);
        std::fs::create_dir_all(pool_dir)?;
        let path = pool_dir.join(format!("{}.retired", Uuid::new_v4()));
        rename_new(&segment.path, &path).context("retire published WAL")?;
        // This deletion must be durable BEFORE zeroing the inode. Otherwise a
        // crash could resurrect the old WAL filename pointing at new bytes.
        sync_dir(segment.path.parent().context("retired WAL has no parent")?)?;
        sync_dir(pool_dir)?;
        Ok(Retirement::Ready(Self {
            path,
            file: segment.file,
        }))
    }

    /// Erase the retired generation fully, then reuse its initialized extents.
    /// This is the expensive step; perform it outside all engine write locks.
    pub fn prepare(
        self,
        reserve: u64,
        capacity: u64,
        vectored: bool,
        format: Format,
    ) -> Result<PreparedSegment> {
        if let Err(error) = prepare_storage(&self.file, reserve, capacity, format) {
            return match self.discard() {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("retired WAL cleanup also failed: {cleanup}")))
                }
            };
        }
        Ok(PreparedSegment {
            path: self.path,
            file: self.file,
            capacity,
            reserve,
            format,
            vectored,
        })
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
    fn packed_checkpoint_pages_roundtrip_with_complete_references() -> Result<()> {
        let t = tempfile::tempdir()?;
        let volume = Uuid::new_v4();
        let input = vec![
            (1, vec![21; PAGE]),
            (2, vec![22; PAGE]),
            (7, vec![27; PAGE]),
        ];
        let (id, bytes, refs) = pack_pages(volume, &input, (16 * PAGE) as u64)?;
        // Two contiguous pages share one header, the distant page gets another.
        assert_eq!(bytes.len(), HEADER + RECORD * 2 + PAGE * 3);
        validate_upload(&bytes, volume, id, 2, (16 * PAGE) as u64)?;
        let path = t.path().join(format!("{:020}-{id}.wal", 1));
        std::fs::write(&path, &bytes)?;
        let (segment, rows) = Segment::open(path, volume, (16 * PAGE) as u64, false)?;
        assert_eq!(rows.len(), 2);
        assert_eq!(segment.last_seq, 2);
        assert_eq!(rows[0].first, 1);
        assert_eq!(rows[0].pages.len(), 2);
        assert_eq!(rows[1].first, 7);
        for (page, data) in &input {
            let reference = &refs[page];
            assert_eq!(reference.segment, id);
            assert_eq!(reference.segment_len, bytes.len() as u64);
            assert_eq!(Segment::read(&segment.file, reference)?, *data);
        }
        Ok(())
    }

    #[test]
    fn packed_checkpoint_splits_maximum_records_and_rejects_invalid_inputs() -> Result<()> {
        let volume = Uuid::new_v4();
        let volume_size = (4096 * PAGE) as u64;
        let pages: Vec<_> = (0..=MAX_IO / PAGE)
            .map(|page| (page as u64, vec![page as u8; PAGE]))
            .collect();
        let (id, bytes, refs) = pack_pages(volume, &pages, volume_size)?;
        assert_eq!(bytes.len(), HEADER + RECORD * 2 + pages.len() * PAGE);
        assert_eq!(refs.len(), pages.len());
        validate_upload(&bytes, volume, id, 2, volume_size)?;
        for invalid in [
            vec![],
            vec![(1, vec![0; PAGE - 1])],
            vec![(4096, vec![0; PAGE])],
            vec![(2, vec![0; PAGE]), (1, vec![0; PAGE])],
            vec![(1, vec![0; PAGE]), (1, vec![0; PAGE])],
        ] {
            assert!(pack_pages(volume, &invalid, volume_size).is_err());
        }
        Ok(())
    }

    #[test]
    fn aligned_wal_roundtrip_preserves_pages_zero_ranges_and_barriers() -> Result<()> {
        for fixed in [false, true] {
            let t = tempfile::tempdir()?;
            let volume = Uuid::new_v4();
            let mut segment =
                Segment::create_with_format(t.path(), volume, 17, 0, true, Format::Aligned)?;
            if fixed {
                segment.initialize_capacity(1024 * 1024)?;
            }
            let mut data = vec![29; PAGE * 2];
            data[PAGE..].fill(0);
            let refs = segment.append(17, 1, &data)?;
            assert_eq!(refs[0].0.offset, (2 * PAGE) as u64);
            assert!(
                refs.iter()
                    .all(|(r, _)| r.offset.is_multiple_of(PAGE as u64))
            );
            assert!(!refs[0].1);
            assert!(refs[1].1);
            segment.commit(17)?;
            segment.zero(18, 5, 3)?;
            segment.commit(18)?;
            segment.file.sync_all()?;
            let len = segment.len;
            let path = segment.path.clone();
            let bytes = std::fs::read(&path)?;
            validate_upload(
                &bytes[..len as usize],
                volume,
                segment.id,
                18,
                (16 * PAGE) as u64,
            )?;
            drop(segment);
            let (segment, recovered) = Segment::open(path, volume, (16 * PAGE) as u64, false)?;
            assert_eq!(segment.format, Format::Aligned);
            assert_eq!(segment.last_seq, 18);
            assert_eq!(segment.commit_seq, 18);
            assert_eq!(segment.len, len);
            assert_eq!(recovered.len(), 2);
            assert_eq!(recovered[0].seq, 17);
            assert_eq!(recovered[0].pages, refs);
            assert_eq!(recovered[1].zero_count, 3);
            assert_eq!(Segment::read(&segment.file, &refs[0].0)?, vec![29; PAGE]);
        }
        Ok(())
    }

    #[test]
    fn aligned_padding_and_data_corruption_fail_closed_locally_and_remotely() -> Result<()> {
        let t = tempfile::tempdir()?;
        let volume = Uuid::new_v4();
        let mut segment =
            Segment::create_with_format(t.path(), volume, 1, 0, true, Format::Aligned)?;
        segment.append(1, 0, &vec![73; PAGE])?;
        segment.commit(1)?;
        segment.file.sync_all()?;
        let original = std::fs::read(&segment.path)?;
        for offset in [
            HEADER + 9,
            PAGE + RECORD + 13,
            2 * PAGE + 19,
            3 * PAGE + RECORD + 27,
        ] {
            let mut bytes = original.clone();
            bytes[offset] ^= 0xff;
            segment.file.write_all_at(&bytes, 0)?;
            assert!(
                Segment::open(segment.path.clone(), volume, (4 * PAGE) as u64, true).is_err(),
                "offset={offset}"
            );
            assert!(
                validate_upload(&bytes, volume, segment.id, 1, (4 * PAGE) as u64).is_err(),
                "offset={offset}"
            );
        }
        segment.file.write_all_at(&original, 0)?;
        assert!(Segment::open(segment.path.clone(), volume, (4 * PAGE) as u64, false).is_ok());
        Ok(())
    }

    #[test]
    fn aligned_torn_final_record_is_discarded_but_nonfinal_tail_is_rejected() -> Result<()> {
        for format in [Format::Legacy, Format::Aligned] {
            for partial_payload in [false, true] {
                let t = tempfile::tempdir()?;
                let volume = Uuid::new_v4();
                let mut segment =
                    Segment::create_with_format(t.path(), volume, 1, 0, true, format)?;
                segment.append(1, 0, &vec![3; PAGE])?;
                segment.commit(1)?;
                let durable_end = segment.len;
                segment.append(2, 1, &vec![4; PAGE])?;
                let suffix = if partial_payload {
                    format.record_overhead() + PAGE / 2
                } else {
                    11
                };
                segment.file.set_len(durable_end + suffix as u64)?;
                segment.file.sync_all()?;
                let path = segment.path.clone();
                assert!(Segment::open(path.clone(), volume, (4 * PAGE) as u64, false).is_err());
                drop(segment);
                let (segment, rows) = Segment::open(path, volume, (4 * PAGE) as u64, true)?;
                assert_eq!(segment.len, durable_end);
                assert_eq!(segment.file.metadata()?.len(), durable_end);
                assert_eq!(segment.commit_seq, 1);
                assert_eq!(rows.len(), 1);
            }
        }
        Ok(())
    }

    #[test]
    fn aligned_fixed_wal_rejects_gap_before_a_later_commit() -> Result<()> {
        let t = tempfile::tempdir()?;
        let volume = Uuid::new_v4();
        let mut segment =
            Segment::create_with_format(t.path(), volume, 1, 0, true, Format::Aligned)?;
        segment.initialize_capacity(1024 * 1024)?;
        segment.append(1, 0, &vec![61; PAGE])?;
        let first_commit = segment.len;
        segment.commit(1)?;
        segment.append(2, 1, &vec![62; PAGE])?;
        segment.commit(2)?;
        segment.file.write_all_at(&[0; PAGE], first_commit)?;
        assert!(Segment::open(segment.path.clone(), volume, (4 * PAGE) as u64, true).is_err());
        Ok(())
    }

    #[test]
    fn aligned_capacity_and_sequences_are_checked_before_mutating_the_wal() -> Result<()> {
        let t = tempfile::tempdir()?;
        let volume = Uuid::new_v4();
        assert!(
            Segment::create_with_format(t.path(), volume, 0, 0, true, Format::Aligned).is_err()
        );
        let mut segment =
            Segment::create_with_format(t.path(), volume, 1, 0, true, Format::Aligned)?;
        assert!(segment.initialize_capacity((4 * PAGE + 1) as u64).is_err());
        segment.initialize_capacity((4 * PAGE) as u64)?;
        segment.append(1, 0, &vec![49; PAGE])?;
        let after_append = std::fs::read(&segment.path)?;
        assert!(segment.append(1, 1, &vec![50; PAGE]).is_err());
        assert!(segment.zero(1, 1, 1).is_err());
        assert_eq!(std::fs::read(&segment.path)?, after_append);
        segment.commit(1)?;
        assert_eq!(segment.len, segment.capacity);
        let original = std::fs::read(&segment.path)?;
        assert!(segment.append(2, 1, &vec![50; PAGE]).is_err());
        assert!(segment.zero(2, 1, 1).is_err());
        assert!(segment.commit(1).is_err());
        assert_eq!(std::fs::read(&segment.path)?, original);
        assert_eq!(segment.last_seq, 1);
        Ok(())
    }

    #[test]
    fn recycled_wal_waits_for_readers_and_cannot_resurrect_old_pages() -> Result<()> {
        let t = tempfile::tempdir()?;
        let wal_dir = t.path().join("wal");
        let pool_dir = t.path().join("wal-pool");
        std::fs::create_dir(&wal_dir)?;
        let volume = Uuid::new_v4();
        let capacity = 1024 * 1024;
        let prepared = PreparedSegment::prepare(&pool_dir, 0, capacity, true, Format::Legacy)?;
        assert_eq!(std::fs::read_dir(&wal_dir)?.count(), 0);
        let mut segment = prepared.activate(&wal_dir, volume, 1)?;
        let old_id = segment.id;
        let old_path = segment.path.clone();
        let inode = segment.file.metadata()?.ino();
        let refs = segment.append(1, 0, &vec![217; PAGE * 3])?;
        segment.commit(1)?;
        segment.file.sync_all()?;
        let readers = Arc::new(segment.file.try_clone()?);
        let outstanding_read = readers.clone();
        let Retirement::Busy { segment, readers } =
            RetiredSegment::try_retire(segment, readers, 1, &pool_dir)?
        else {
            panic!("a live reader must prevent recycling");
        };
        assert!(old_path.exists());
        assert_eq!(
            Segment::read(&outstanding_read, &refs[0].0)?,
            vec![217; PAGE]
        );
        drop(outstanding_read);
        let Retirement::Ready(retired) =
            RetiredSegment::try_retire(segment, readers, 1, &pool_dir)?
        else {
            panic!("an exclusive published segment should be reusable");
        };
        assert!(!old_path.exists());
        // The old name is already durably gone before any old bytes are erased.
        assert_eq!(std::fs::read_dir(&wal_dir)?.count(), 0);
        let prepared = retired.prepare(0, capacity, true, Format::Aligned)?;
        let mut segment = prepared.activate(&wal_dir, volume, 2)?;
        assert_ne!(segment.id, old_id);
        assert_eq!(segment.file.metadata()?.ino(), inode);
        assert!(segment.is_empty());
        let bytes = std::fs::read(&segment.path)?;
        assert!(bytes[PAGE..].iter().all(|b| *b == 0));
        let (_, recovered) = Segment::open(segment.path.clone(), volume, (8 * PAGE) as u64, true)?;
        assert!(recovered.is_empty());
        segment.append(2, 4, &vec![91; PAGE])?;
        segment.commit(2)?;
        segment.file.sync_all()?;
        let (segment, recovered) =
            Segment::open(segment.path.clone(), volume, (8 * PAGE) as u64, true)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].first, 4);
        assert_eq!(recovered[0].seq, 2);
        assert_eq!(segment.commit_seq, 2);
        Ok(())
    }

    #[test]
    fn unused_prepared_and_retired_segments_can_be_discarded_without_orphans() -> Result<()> {
        let t = tempfile::tempdir()?;
        let pool = t.path().join("pool");
        let wal = t.path().join("wal");
        std::fs::create_dir(&wal)?;
        let prepared = PreparedSegment::prepare(&pool, 0, 1024 * 1024, true, Format::Aligned)?;
        assert_eq!(std::fs::read_dir(&pool)?.count(), 1);
        prepared.discard()?;
        assert_eq!(std::fs::read_dir(&pool)?.count(), 0);
        let volume = Uuid::new_v4();
        let segment = PreparedSegment::prepare(&pool, 0, 1024 * 1024, true, Format::Aligned)?
            .activate(&wal, volume, 1)?;
        let readers = Arc::new(segment.file.try_clone()?);
        let Retirement::Ready(retired) = RetiredSegment::try_retire(segment, readers, 0, &pool)?
        else {
            panic!("empty segment should be reusable");
        };
        retired.discard()?;
        assert_eq!(std::fs::read_dir(&pool)?.count(), 0);
        assert_eq!(std::fs::read_dir(&wal)?.count(), 0);
        Ok(())
    }

    #[test]
    fn recycling_rejects_unpublished_wrong_inode_and_hardlinked_segments() -> Result<()> {
        for invalid in ["unpublished", "wrong-inode", "hardlink"] {
            let t = tempfile::tempdir()?;
            let pool = t.path().join("pool");
            let mut segment = Segment::create(t.path(), Uuid::new_v4(), 1)?;
            segment.append(1, 0, &vec![77; PAGE])?;
            let path = segment.path.clone();
            let bytes = std::fs::read(&path)?;
            let reader = if invalid == "wrong-inode" {
                File::create(t.path().join("unrelated"))?
            } else {
                segment.file.try_clone()?
            };
            if invalid == "hardlink" {
                std::fs::hard_link(&path, t.path().join("extra-link"))?;
            }
            let frontier = if invalid == "unpublished" { 0 } else { 1 };
            assert!(
                RetiredSegment::try_retire(segment, Arc::new(reader), frontier, &pool).is_err()
            );
            assert_eq!(std::fs::read(path)?, bytes);
        }
        Ok(())
    }

    #[test]
    fn fixed_wal_reservation_release_preserves_initialized_capacity() -> Result<()> {
        let t = tempfile::tempdir()?;
        let mut segment =
            Segment::create_with_options(t.path(), Uuid::new_v4(), 1, 2 * 1024 * 1024, true)?;
        segment.initialize_capacity(1024 * 1024)?;
        segment.append(1, 0, &vec![39; PAGE])?;
        segment.file.sync_all()?;
        segment.release_reservation(2 * 1024 * 1024)?;
        assert_eq!(segment.file.metadata()?.len(), 1024 * 1024);
        assert_eq!(segment.file.metadata()?.blocks() * 512, 1024 * 1024);
        let bytes = std::fs::read(&segment.path)?;
        assert!(bytes[segment.len as usize..].iter().all(|b| *b == 0));
        Ok(())
    }

    #[test]
    fn initialized_wal_keeps_physical_eof_and_rejects_a_hole_before_commit() -> Result<()> {
        let t = tempfile::tempdir()?;
        let v = Uuid::new_v4();
        let mut s = Segment::create(t.path(), v, 1)?;
        s.initialize_capacity(1024 * 1024)?;
        s.append(1, 0, &vec![7; PAGE])?;
        s.commit(1)?;
        s.append(2, 1, &vec![8; PAGE])?;
        s.commit(2)?;
        s.file.sync_data()?;
        assert_eq!(s.file.metadata()?.len(), 1024 * 1024);
        let logical = s.len;
        let path = s.path.clone();
        let bytes = std::fs::read(&path)?;
        validate_upload(&bytes[..logical as usize], v, s.id, 2, PAGE as u64 * 4)?;
        drop(s);
        let (mut s, rows) = Segment::open(path.clone(), v, PAGE as u64 * 4, true)?;
        assert_eq!(rows.len(), 2);
        assert_eq!(s.commit_seq, 2);
        assert_eq!(s.len, logical);
        s.append(3, 2, &vec![9; PAGE])?;
        s.commit(3)?;
        assert_eq!(s.file.metadata()?.len(), 1024 * 1024);
        s.file
            .write_all_at(&[0; RECORD], HEADER as u64 + (RECORD + PAGE) as u64)?;
        assert!(Segment::open(path, v, PAGE as u64 * 4, true).is_err());
        Ok(())
    }
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
