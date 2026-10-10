//! Bounded, disposable preparation queue. Only published WALs may be recycled.
use crate::{
    config::Config,
    wal::{self, Format, PreparedSegment, RetiredSegment},
};
use anyhow::{Result, ensure};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::Duration,
};

pub fn format(c: &Config) -> Format {
    if c.aligned_wal {
        Format::Aligned
    } else {
        Format::Legacy
    }
}
pub fn capacity(c: &Config) -> u64 {
    if !c.wal_fixed_size {
        return 0;
    }
    let bytes = c.segment_mib * 1024 * 1024
        + wal::MAX_IO as u64
        + wal::PAGE as u64
        + 2 * format(c).record_overhead() as u64
        + format(c).header_len() as u64;
    if c.aligned_wal {
        bytes.div_ceil(wal::PAGE as u64) * wal::PAGE as u64
    } else {
        bytes
    }
}

struct Budget {
    used: AtomicU64,
    limit: u64,
}
struct Reservation {
    budget: Arc<Budget>,
    bytes: u64,
    release: bool,
}
impl Budget {
    fn reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(bytes)?;
            if next > self.limit {
                return None;
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => used = current,
            }
        }
        Some(Reservation {
            budget: self.clone(),
            bytes,
            release: true,
        })
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if self.release {
            self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

trait Disposable {
    fn discard(self) -> Result<()>;
}
impl Disposable for PreparedSegment {
    fn discard(self) -> Result<()> {
        PreparedSegment::discard(self)
    }
}
impl Disposable for RetiredSegment {
    fn discard(self) -> Result<()> {
        RetiredSegment::discard(self)
    }
}
/// Channel destruction, failed sends, and thread shutdown all release the file
/// and its reservation together. A failed unlink keeps its budget charged.
struct Pooled<T: Disposable> {
    value: Option<T>,
    reservation: Option<Reservation>,
}
impl<T: Disposable> Pooled<T> {
    fn new(value: T, reservation: Reservation) -> Self {
        Self {
            value: Some(value),
            reservation: Some(reservation),
        }
    }
    fn into_parts(mut self) -> (T, Reservation) {
        (self.value.take().unwrap(), self.reservation.take().unwrap())
    }
    fn into_value(mut self) -> T {
        self.value.take().unwrap()
    }
}
impl<T: Disposable> Drop for Pooled<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take()
            && let Err(error) = value.discard()
        {
            // Keep a conservative charge for a file we could not remove.
            if let Some(reservation) = self.reservation.as_mut() {
                reservation.release = false;
            }
            tracing::warn!(%error, "failed to discard disposable WAL pool file");
        }
    }
}

struct Settings {
    directory: PathBuf,
    capacity: u64,
    reserve: u64,
    format: Format,
    vectored: bool,
    unit_bytes: u64,
}

fn worker(
    settings: Settings,
    send_ready: mpsc::SyncSender<Pooled<PreparedSegment>>,
    receive_retired: mpsc::Receiver<Pooled<RetiredSegment>>,
    budget: Arc<Budget>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Acquire) {
        let retired = receive_retired.try_recv().ok();
        let prepared = if let Some(retired) = retired {
            let (old, reservation) = retired.into_parts();
            old.prepare(
                settings.reserve,
                settings.capacity,
                settings.vectored,
                settings.format,
            )
            .map(|value| Pooled::new(value, reservation))
        } else if let Some(reservation) = budget.reserve(settings.unit_bytes) {
            PreparedSegment::prepare(
                &settings.directory,
                settings.reserve,
                settings.capacity,
                settings.vectored,
                settings.format,
            )
            .map(|value| Pooled::new(value, reservation))
        } else {
            // A concurrent offer/discard may momentarily own the last unit.
            // This wait is rare; a full ready queue blocks send without polling.
            match receive_retired.recv_timeout(Duration::from_millis(20)) {
                Ok(retired) => {
                    let (old, reservation) = retired.into_parts();
                    old.prepare(
                        settings.reserve,
                        settings.capacity,
                        settings.vectored,
                        settings.format,
                    )
                    .map(|value| Pooled::new(value, reservation))
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                // The engine may still rotate through its checked synchronous
                // path. Stop rather than retrying allocation in an error loop.
                tracing::warn!(%error, "WAL preparation stopped");
                break;
            }
        };
        if stop.load(Ordering::Acquire) {
            drop(prepared);
            break;
        }
        if let Err(error) = send_ready.send(prepared) {
            drop(error.0);
            break;
        }
    }
    // Dropping the receiver drains queued Pooled values and deletes their files.
    // Any subsequent offer observes Disconnected and disposes of its own inode.
}

pub struct WalPool {
    ready: Mutex<Option<mpsc::Receiver<Pooled<PreparedSegment>>>>,
    retired: Option<mpsc::SyncSender<Pooled<RetiredSegment>>>,
    thread: Option<std::thread::JoinHandle<()>>,
    budget: Arc<Budget>,
    stop: Arc<AtomicBool>,
    pub reserve_bytes: u64,
    pub unit_bytes: u64,
    pub directory: PathBuf,
}
impl WalPool {
    pub fn start(c: &Config) -> Result<Self> {
        ensure!(
            c.wal_fixed_size && (1..=64).contains(&c.segment_mib),
            "WAL preparation pool requires fixed segments of 1..64 MiB"
        );
        let capacity = capacity(c);
        let reserve = if c.wal_preallocate {
            c.segment_mib * 1024 * 1024
        } else {
            0
        };
        let format = format(c);
        let unit_bytes = capacity.max(reserve).max(format.header_len() as u64);
        Self::start_with_settings(Settings {
            directory: c.local_dir.join("wal-pool"),
            capacity,
            reserve,
            format,
            vectored: c.wal_writev,
            unit_bytes,
        })
    }
    fn start_with_settings(settings: Settings) -> Result<Self> {
        let directory = settings.directory.clone();
        std::fs::create_dir_all(&directory)?;
        // Pool entries are never recovery authority. Validate the whole listing
        // before deleting anything, so an unexpected entry fails predictably.
        let entries = std::fs::read_dir(&directory)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        ensure!(
            entries.iter().all(|path| path
                .extension()
                .is_some_and(|extension| extension == "prepared" || extension == "retired")),
            "unexpected WAL pool entry"
        );
        for path in entries {
            std::fs::remove_file(path)?;
        }
        wal::sync_dir(&directory)?;
        // One queued + one held by the blocking producer = two prepared files.
        // One additional retired file may wait for reuse. Every in-flight file
        // holds one budget reservation, including concurrent rejected offers.
        let (send_ready, ready) = mpsc::sync_channel(1);
        let (retired, receive_retired) = mpsc::sync_channel(1);
        let unit_bytes = settings.unit_bytes;
        let reserve_bytes = unit_bytes
            .checked_mul(3)
            .ok_or_else(|| anyhow::anyhow!("WAL pool budget overflow"))?;
        let budget = Arc::new(Budget {
            used: AtomicU64::new(0),
            limit: reserve_bytes,
        });
        let stop = Arc::new(AtomicBool::new(false));
        let worker_budget = budget.clone();
        let worker_stop = stop.clone();
        let thread = std::thread::Builder::new()
            .name("wal-prepare".into())
            .spawn(move || {
                worker(
                    settings,
                    send_ready,
                    receive_retired,
                    worker_budget,
                    worker_stop,
                )
            })?;
        Ok(Self {
            ready: Mutex::new(Some(ready)),
            retired: Some(retired),
            thread: Some(thread),
            budget,
            stop,
            reserve_bytes,
            unit_bytes,
            directory,
        })
    }
    pub fn take(&self) -> Option<PreparedSegment> {
        let prepared = self
            .ready
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()?
            .try_recv()
            .ok()?;
        // Returned storage is now charged to the engine's active/resident WALs.
        Some(prepared.into_value())
    }
    pub fn offer(&self, old: RetiredSegment) {
        // A config change may make recovered old files larger than this pool's
        // unit. Discard those instead of undercounting their physical footprint.
        if !old
            .storage_bytes()
            .is_ok_and(|bytes| bytes <= self.unit_bytes)
        {
            if let Err(error) = old.discard() {
                tracing::warn!(%error, "oversized WAL pool discard failed");
            }
            return;
        }
        let Some(reservation) = self.budget.reserve(self.unit_bytes) else {
            if let Err(error) = old.discard() {
                tracing::warn!(%error, "full WAL pool discard failed");
            }
            return;
        };
        let queued = Pooled::new(old, reservation);
        if let Some(retired) = &self.retired {
            match retired.try_send(queued) {
                Ok(()) => (),
                Err(mpsc::TrySendError::Full(value) | mpsc::TrySendError::Disconnected(value)) => {
                    drop(value)
                }
            }
        }
        // If stopping, queued is dropped here with its file and reservation.
    }
    pub fn bytes(&self) -> u64 {
        self.budget.used.load(Ordering::Acquire)
    }
}
impl Drop for WalPool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.retired.take();
        // Closing the receiver releases queued files and unblocks a producer
        // waiting in send. Join only after both channel endpoints are closed.
        let ready = self
            .ready
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        drop(ready);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{PAGE, Retirement, Segment};
    use std::{collections::BTreeSet, os::unix::fs::MetadataExt, time::Instant};

    fn config(local: &std::path::Path) -> Config {
        Config {
            local_dir: local.to_owned(),
            segment_mib: 1,
            wal_fixed_size: true,
            checkpoint_pipeline: true,
            aligned_wal: true,
            ..Config::default()
        }
    }
    fn wait_for(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "WAL pool did not make progress");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    fn take(pool: &WalPool) -> PreparedSegment {
        let mut ready = None;
        wait_for(|| {
            ready = pool.take();
            ready.is_some()
        });
        ready.unwrap()
    }
    fn retire(segment: Segment, pool: &WalPool, frontier: u64) -> RetiredSegment {
        let readers = Arc::new(segment.file.try_clone().unwrap());
        match RetiredSegment::try_retire(segment, readers, frontier, &pool.directory).unwrap() {
            Retirement::Ready(retired) => retired,
            Retirement::Busy { .. } => panic!("test WAL has no concurrent reader"),
        }
    }
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn pool_is_send_sync_and_shutdown_joins_with_a_full_ready_queue() -> Result<()> {
        assert_send_sync::<WalPool>();
        let t = tempfile::tempdir()?;
        let pool = WalPool::start(&config(t.path()))?;
        let budget = pool.budget.clone();
        let directory = pool.directory.clone();
        wait_for(|| {
            pool.bytes() == pool.unit_bytes * 2
                && std::fs::read_dir(&directory).unwrap().count() == 2
        });
        // One entry is queued and the producer is holding another during send.
        let start = Instant::now();
        drop(pool);
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read_dir(directory)?.count(), 0);
        Ok(())
    }

    #[test]
    fn recycle_reuses_the_inode_and_keeps_disk_and_accounting_bounded() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(t.path());
        let wal_dir = t.path().join("wal");
        std::fs::create_dir(&wal_dir)?;
        let volume = uuid::Uuid::new_v4();
        let pool = WalPool::start(&c)?;
        let budget = pool.budget.clone();
        let directory = pool.directory.clone();
        let mut active = take(&pool).activate(&wal_dir, volume, 1)?;
        active.append(1, 0, &vec![97; PAGE])?;
        active.commit(1)?;
        active.file.sync_all()?;
        let recycled_inode = active.file.metadata()?.ino();
        let old_id = active.id;
        wait_for(|| pool.bytes() == pool.unit_bytes * 2);
        pool.offer(retire(active, &pool, 1));
        assert!(pool.bytes() <= pool.reserve_bytes);
        let mut observed = BTreeSet::new();
        let mut reused = None;
        // Two prepared entries can precede the retired file in the FIFO path.
        for next in 2..=5 {
            let prepared = take(&pool);
            let segment = prepared.activate(&wal_dir, volume, next)?;
            let inode = segment.file.metadata()?.ino();
            observed.insert(inode);
            if inode == recycled_inode {
                assert_ne!(segment.id, old_id);
                let (_, rows) =
                    Segment::open(segment.path.clone(), volume, (8 * PAGE) as u64, true)?;
                assert!(rows.is_empty());
                reused = Some(segment);
                break;
            }
            assert!(pool.bytes() <= pool.reserve_bytes);
            assert!(std::fs::read_dir(&directory)?.count() <= 3);
            // These files have no data; their empty generation is published.
            retire(segment, &pool, next - 1).discard()?;
        }
        let reused = reused.expect("retired inode must reach the prepared queue");
        retire(reused, &pool, 10).discard()?;
        assert!(observed.len() <= 3);
        drop(pool);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read_dir(directory)?.count(), 0);
        assert_eq!(std::fs::read_dir(wal_dir)?.count(), 0);
        Ok(())
    }

    #[test]
    fn rejected_retired_offers_leave_no_orphan_and_no_counter_growth() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(t.path());
        let wal_dir = t.path().join("wal");
        std::fs::create_dir(&wal_dir)?;
        let pool = WalPool::start(&c)?;
        wait_for(|| pool.bytes() == pool.unit_bytes * 2);
        for seq in 1..=8 {
            let segment = Segment::create(&wal_dir, uuid::Uuid::new_v4(), seq)?;
            pool.offer(retire(segment, &pool, seq - 1));
            assert!(pool.bytes() <= pool.reserve_bytes);
            assert!(std::fs::read_dir(&pool.directory)?.count() <= 3);
        }
        let directory = pool.directory.clone();
        let budget = pool.budget.clone();
        drop(pool);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read_dir(directory)?.count(), 0);
        assert_eq!(std::fs::read_dir(wal_dir)?.count(), 0);
        Ok(())
    }

    #[test]
    fn failed_preparation_stops_and_releases_its_budget_for_checked_fallback() -> Result<()> {
        let t = tempfile::tempdir()?;
        // Exercise the real worker error path with a rejected aligned capacity.
        // Public start builds validated settings; I/O errors take this same path.
        let pool = WalPool::start_with_settings(Settings {
            directory: t.path().join("wal-pool"),
            capacity: 123,
            reserve: 0,
            format: Format::Aligned,
            vectored: true,
            unit_bytes: (PAGE * 2) as u64,
        })?;
        wait_for(|| pool.thread.as_ref().unwrap().is_finished());
        assert_eq!(pool.bytes(), 0);
        assert!(pool.take().is_none());
        assert_eq!(std::fs::read_dir(&pool.directory)?.count(), 0);
        // A stopped pool does not prevent ordinary checked segment creation.
        let volume = uuid::Uuid::new_v4();
        let mut fallback = Segment::create(t.path(), volume, 1)?;
        fallback.append(1, 0, &vec![9; PAGE])?;
        fallback.file.sync_all()?;
        let (_, rows) = Segment::open(fallback.path.clone(), volume, (2 * PAGE) as u64, false)?;
        assert_eq!(rows.len(), 1);
        // A later offer to the disconnected receiver is deleted synchronously.
        pool.offer(retire(fallback, &pool, 1));
        assert_eq!(pool.bytes(), 0);
        assert_eq!(std::fs::read_dir(&pool.directory)?.count(), 0);
        Ok(())
    }

    #[test]
    fn failed_recycled_preparation_discards_the_old_inode() -> Result<()> {
        let t = tempfile::tempdir()?;
        let directory = t.path().join("wal-pool");
        let mut segment = Segment::create(t.path(), uuid::Uuid::new_v4(), 1)?;
        segment.append(1, 0, &vec![51; PAGE])?;
        let readers = Arc::new(segment.file.try_clone()?);
        let Retirement::Ready(retired) =
            RetiredSegment::try_retire(segment, readers, 1, &directory)?
        else {
            panic!("no concurrent readers");
        };
        assert_eq!(std::fs::read_dir(&directory)?.count(), 1);
        assert!(retired.prepare(0, 123, true, Format::Aligned).is_err());
        assert_eq!(std::fs::read_dir(directory)?.count(), 0);
        Ok(())
    }

    #[test]
    fn startup_cleans_stale_pool_entries_but_rejects_unexpected_files() -> Result<()> {
        let t = tempfile::tempdir()?;
        let c = config(t.path());
        let directory = t.path().join("wal-pool");
        std::fs::create_dir(&directory)?;
        std::fs::write(directory.join("stale.prepared"), b"unused")?;
        std::fs::write(directory.join("stale.retired"), b"published")?;
        std::fs::write(directory.join("unexpected.wal"), b"do not remove")?;
        assert!(WalPool::start(&c).is_err());
        assert_eq!(std::fs::read_dir(&directory)?.count(), 3);
        std::fs::remove_file(directory.join("unexpected.wal"))?;
        let pool = WalPool::start(&c)?;
        assert!(!directory.join("stale.prepared").exists());
        assert!(!directory.join("stale.retired").exists());
        drop(pool);
        assert_eq!(std::fs::read_dir(directory)?.count(), 0);
        Ok(())
    }
}
