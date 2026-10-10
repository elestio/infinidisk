//! Admission of online data ranges, shared by every connection of one volume.
//! The budget covers reserved range payloads, not total process RSS or SDK buffers.
use anyhow::{Result, ensure};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const UNIT: usize = 4096;
const SMALL: usize = 20 * 1024;
const MAX_RANGE: usize = 260 * 1024;

#[derive(Clone, Default, Serialize)]
pub struct Stats {
    pub enabled: bool,
    pub budget_bytes: usize,
    pub small_reserve_bytes: usize,
    pub max_requests: usize,
    pub waiting: usize,
    pub reserved_bytes: usize,
    pub peak_reserved_bytes: usize,
    pub active: usize,
    pub peak_active: usize,
    pub admissions: u64,
    pub small_reserve_admissions: u64,
    pub wait_ns: u64,
}

pub struct Downloads {
    shared: Arc<Semaphore>,
    small: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    stats: Arc<Mutex<Stats>>,
    enabled: bool,
}

// Counters and semaphore permits are released together, including cancellation.
struct Waiting(Arc<Mutex<Stats>>);
impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.lock().unwrap().waiting -= 1;
    }
}
struct Reservation {
    _permit: OwnedSemaphorePermit,
    bytes: usize,
    stats: Arc<Mutex<Stats>>,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.stats.lock().unwrap().reserved_bytes -= self.bytes;
    }
}
pub struct Permit {
    _payload: Reservation,
    _request: OwnedSemaphorePermit,
    stats: Arc<Mutex<Stats>>,
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.stats.lock().unwrap().active -= 1;
    }
}

impl Downloads {
    pub fn new(mib: usize, requests: usize) -> Self {
        let budget = mib * 1024 * 1024;
        // One eighth (up to 1 MiB) lets small reads bypass a queued large GET.
        // The remaining FIFO prevents an endless small stream starving large IO.
        let reserve = (budget / 8).min(1024 * 1024);
        Self {
            shared: Arc::new(Semaphore::new((budget - reserve) / UNIT)),
            small: Arc::new(Semaphore::new(reserve / UNIT)),
            requests: Arc::new(Semaphore::new(requests)),
            stats: Arc::new(Mutex::new(Stats {
                enabled: mib > 0,
                budget_bytes: budget,
                small_reserve_bytes: reserve,
                max_requests: requests,
                ..Stats::default()
            })),
            enabled: mib > 0,
        }
    }

    pub async fn acquire(&self, bytes: usize) -> Result<Option<Permit>> {
        if !self.enabled {
            return Ok(None);
        }
        ensure!(
            bytes > 0 && bytes <= MAX_RANGE,
            "invalid online download size"
        );
        let units = bytes.div_ceil(UNIT) as u32;
        let start = Instant::now();
        self.stats.lock().unwrap().waiting += 1;
        let _waiting = Waiting(self.stats.clone());
        let (payload, reserved_small) = if bytes <= SMALL {
            // Do not pay for the reserve unless shared capacity is unavailable.
            if let Ok(permit) = self.shared.clone().try_acquire_many_owned(units) {
                (permit, false)
            } else {
                tokio::select! {
                    p = self.shared.clone().acquire_many_owned(units) => (p?, false),
                    p = self.small.clone().acquire_many_owned(units) => (p?, true),
                }
            }
        } else {
            (self.shared.clone().acquire_many_owned(units).await?, false)
        };
        let charged = units as usize * UNIT;
        {
            let mut s = self.stats.lock().unwrap();
            s.reserved_bytes += charged;
            s.peak_reserved_bytes = s.peak_reserved_bytes.max(s.reserved_bytes);
        }
        let payload = Reservation {
            _permit: payload,
            bytes: charged,
            stats: self.stats.clone(),
        };
        // Always acquire bytes before request slots. A queued large transfer
        // cannot consume every slot while a small read uses its reserved bytes.
        let request = self.requests.clone().acquire_owned().await?;
        {
            let mut s = self.stats.lock().unwrap();
            s.active += 1;
            s.peak_active = s.peak_active.max(s.active);
            s.admissions += 1;
            s.small_reserve_admissions += u64::from(reserved_small);
            s.wait_ns = s.wait_ns.saturating_add(start.elapsed().as_nanos() as u64);
        }
        Ok(Some(Permit {
            _payload: payload,
            _request: request,
            stats: self.stats.clone(),
        }))
    }

    pub fn status(&self) -> Stats {
        self.stats.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Poll;

    #[tokio::test]
    async fn small_read_bypasses_large_waiter_and_cancellation_returns_budget() -> Result<()> {
        let downloads = Downloads::new(1, 64);
        let a = downloads.acquire(MAX_RANGE).await?;
        let b = downloads.acquire(MAX_RANGE).await?;
        let c = downloads.acquire(MAX_RANGE).await?;
        let mut queued = Box::pin(downloads.acquire(MAX_RANGE));
        assert!(matches!(futures::poll!(&mut queued), Poll::Pending));
        // The shared FIFO has a large waiter, despite 116 KiB still available.
        let small = downloads.acquire(SMALL).await?;
        assert_eq!(downloads.status().small_reserve_admissions, 1);
        assert_eq!(downloads.status().waiting, 1);
        drop(queued);
        drop((a, b, c, small));
        let s = downloads.status();
        assert_eq!((s.active, s.waiting, s.reserved_bytes), (0, 0, 0));
        assert!(s.peak_reserved_bytes <= s.budget_bytes);
        assert_eq!(downloads.shared.available_permits(), 224);
        assert_eq!(downloads.small.available_permits(), 32);
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_while_waiting_for_request_slot_releases_reserved_bytes() -> Result<()> {
        let downloads = Downloads::new(1, 1);
        let first = downloads.acquire(MAX_RANGE).await?;
        let mut queued = Box::pin(downloads.acquire(MAX_RANGE));
        assert!(matches!(futures::poll!(&mut queued), Poll::Pending));
        assert_eq!(downloads.status().reserved_bytes, 2 * MAX_RANGE);
        drop(queued);
        assert_eq!(downloads.status().reserved_bytes, MAX_RANGE);
        drop(first);
        let next = downloads.acquire(MAX_RANGE).await?;
        drop(next);
        let s = downloads.status();
        assert_eq!((s.active, s.waiting, s.reserved_bytes), (0, 0, 0));
        assert_eq!(s.peak_active, 1);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_even_without_a_cache_and_on_task_abort() -> Result<()> {
        let downloads = Arc::new(Downloads::new(1, 2));
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let owner = downloads.clone();
        let task = tokio::spawn(async move {
            let _permit = owner.acquire(MAX_RANGE).await.unwrap();
            sender.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        receiver.await?;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let s = downloads.status();
        assert_eq!((s.active, s.waiting, s.reserved_bytes), (0, 0, 0));
        assert!(downloads.acquire(MAX_RANGE + 1).await.is_err());
        assert!(Downloads::new(0, 64).acquire(MAX_RANGE).await?.is_none());
        Ok(())
    }
}
