//! Experimental ublk transport. FLUSH and FUA retain the engine's contract.
//!
//! `ublk_fast_path` transfers each tag's reusable buffer to a persistent engine
//! worker and coalesces cross-runtime wakeups per queue. This is not kernel
//! zero-copy: ublk's regular copy transport is deliberately retained.
use crate::engine::Engine;
use anyhow::{Result, ensure};
use futures::FutureExt;
use libublk::{BufDesc, UblkFlags, ctrl::UblkCtrlBuilder, helpers::IoBuf, io::UblkDev};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::{
    future::poll_fn,
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd},
    panic::AssertUnwindSafe,
    task::{Poll, Waker},
};
use tokio::sync::{Semaphore, mpsc};

const QUEUE_DEPTH: u16 = 32;
const BUFFER_BYTES: usize = 1024 * 1024;

/// Errors must stop the whole target: the SDK otherwise waits forever for the
/// sibling FETCH tasks after one tag exits. The control syscall runs outside
/// the queue thread, which must remain available to reap aborted commands.
struct TargetFailure {
    first: Mutex<Option<String>>,
    notify: std::sync::mpsc::SyncSender<bool>,
    wake: Mutex<Vec<FailureWake>>,
}
type FailureWake = Box<dyn Fn(&str) + Send + Sync>;
impl TargetFailure {
    fn new<F>(stop: F) -> std::io::Result<(Arc<Self>, std::thread::JoinHandle<()>)>
    where
        F: FnOnce() + Send + 'static,
    {
        let (notify, receive) = std::sync::mpsc::sync_channel(1);
        let state = Arc::new(Self {
            first: Mutex::new(None),
            notify,
            wake: Mutex::new(Vec::new()),
        });
        let controller = std::thread::Builder::new()
            .name("ublk-stop".into())
            .spawn(move || {
                if receive.recv() == Ok(true) {
                    stop();
                }
            })?;
        Ok((state, controller))
    }
    fn report(&self, error: String) {
        let publish = {
            let mut first = self.first.lock().unwrap_or_else(|e| e.into_inner());
            if first.is_some() {
                false
            } else {
                *first = Some(error.clone());
                true
            }
        };
        if publish {
            tracing::error!(error=%error, "stopping ublk after transport failure");
            // STOP_DEV only aborts commands armed in the kernel. A tag waiting
            // on a dead worker has no such command: wake its userspace wait and
            // interrupt io_uring_enter before asking the kernel to stop.
            for wake in self.wake.lock().unwrap_or_else(|e| e.into_inner()).iter() {
                wake(&error);
            }
            // Exactly one failure is published. Normal shutdown sends false
            // only after all queue handlers and accepted workers have ended.
            let _ = self.notify.try_send(true);
        }
    }
    fn register_wake(&self, wake: impl Fn(&str) + Send + Sync + 'static) {
        let mut registered = self.wake.lock().unwrap_or_else(|e| e.into_inner());
        registered.push(Box::new(wake));
        // Registration may race the first error. Register before reading its
        // state so either the publisher or this path wakes the new waiter.
        if let Err(error) = self.result() {
            registered.last().unwrap()(&error.to_string());
        }
    }
    fn finish(&self) {
        if self.result().is_ok() {
            let _ = self.notify.try_send(false);
        }
    }
    fn result(&self) -> Result<()> {
        match &*self.first.lock().unwrap_or_else(|e| e.into_inner()) {
            Some(error) => Err(anyhow::anyhow!(error.clone())),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
struct WorkerDrain {
    active: Mutex<usize>,
    idle: Condvar,
}
impl WorkerDrain {
    fn start(self: &Arc<Self>, failure: Arc<TargetFailure>) -> WorkerGuard {
        *self.active.lock().unwrap() += 1;
        WorkerGuard {
            drain: self.clone(),
            failure,
        }
    }
    fn wait(&self) {
        let active = self.active.lock().unwrap();
        drop(self.idle.wait_while(active, |count| *count != 0).unwrap());
    }
}
struct WorkerGuard {
    drain: Arc<WorkerDrain>,
    failure: Arc<TargetFailure>,
}
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.failure.report("ublk engine worker panicked".into());
        }
        let mut active = self.drain.active.lock().unwrap();
        *active -= 1;
        if *active == 0 {
            self.drain.idle.notify_all();
        }
    }
}

#[derive(Debug, PartialEq)]
struct RequestFields {
    op: u32,
    fua: bool,
    offset: u64,
    len: usize,
}
fn decode_request(
    op_flags: u32,
    start_sector: u64,
    nr_sectors: u32,
    capacity: usize,
) -> std::result::Result<RequestFields, i32> {
    let op = op_flags & 0xff;
    let (offset, len) = match op {
        // FLUSH has no data address. In particular the kernel may use a sector
        // sentinel that overflows byte conversion; neither field is meaningful.
        libublk::sys::UBLK_IO_OP_FLUSH => (0, 0),
        libublk::sys::UBLK_IO_OP_READ | libublk::sys::UBLK_IO_OP_WRITE => {
            let offset = start_sector.checked_mul(512).ok_or(-libc::EINVAL)?;
            let len = (nr_sectors as usize)
                .checked_mul(512)
                .filter(|len| *len <= capacity)
                .ok_or(-libc::EINVAL)?;
            (offset, len)
        }
        _ => return Err(-libc::EOPNOTSUPP),
    };
    Ok(RequestFields {
        op,
        fua: op_flags & libublk::sys::UBLK_IO_F_FUA != 0,
        offset,
        len,
    })
}

// A Tokio cross-thread wake cannot interrupt libublk's io_uring_enter.
// Complete the reply before signalling an eventfd polled on that ring.
struct CompletionSignal(std::fs::File);
impl CompletionSignal {
    fn new() -> std::io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self(unsafe { std::fs::File::from_raw_fd(fd) }))
    }
    fn notify(&self) -> std::io::Result<()> {
        match (&self.0).write_all(&1_u64.to_ne_bytes()) {
            // A saturated eventfd is already readable, so this wake is covered.
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            result => result,
        }
    }
    fn consume(&self) -> std::io::Result<u64> {
        let mut value = [0; 8];
        match (&self.0).read_exact(&mut value) {
            Ok(()) => Ok(u64::from_ne_bytes(value)),
            // A completion consumed directly by its task can leave a redundant
            // readiness notification. Treat that as an empty batch.
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e),
        }
    }
}

/// Ownership moves; the aligned allocation keeps the same address on success.
struct RequestBuffer(IoBuf<u8>);
impl RequestBuffer {
    fn new(len: usize) -> Self {
        let mut buffer = IoBuf::new(len);
        // IoBuf::new leaves memory uninitialized. Initialize before exposing a
        // Rust slice or registering it, including tags first used for READ.
        buffer.zero_buf();
        Self(buffer)
    }
}
impl AsMut<[u8]> for RequestBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut_slice()
    }
}

struct EngineRequest {
    op: u32,
    fua: bool,
    offset: u64,
    len: usize,
    buffer: RequestBuffer,
}
struct EngineCompletion {
    buffer: RequestBuffer,
    result: Result<i32>,
}
#[derive(Default)]
struct CompletionSlot {
    result: Option<EngineCompletion>,
    waker: Option<Waker>,
}
struct InboxState {
    slots: Vec<CompletionSlot>,
    // One bit per tag, so completion-before-wait and immediate tag reuse can
    // never grow an auxiliary completion queue beyond the device depth.
    pending: u32,
    failure: Option<String>,
}
struct CompletionInbox {
    signal: CompletionSignal,
    state: Mutex<InboxState>,
    stopping: AtomicBool,
}
impl CompletionInbox {
    fn new(depth: u16) -> std::io::Result<Self> {
        assert!((1..=QUEUE_DEPTH).contains(&depth));
        Ok(Self {
            signal: CompletionSignal::new()?,
            state: Mutex::new(InboxState {
                slots: (0..depth).map(|_| CompletionSlot::default()).collect(),
                pending: 0,
                failure: None,
            }),
            stopping: AtomicBool::new(false),
        })
    }
    fn complete(&self, tag: u16, completion: EngineCompletion) -> std::io::Result<()> {
        let (notify, waker) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.failure.is_some() {
                return Ok(());
            }
            if state.slots[tag as usize].result.is_some() {
                return Err(std::io::Error::other("duplicate ublk tag completion"));
            }
            state.slots[tag as usize].result = Some(completion);
            let notify = state.pending == 0;
            state.pending |= 1 << tag;
            (notify, state.slots[tag as usize].waker.take())
        };
        // Schedule the tag from the engine runtime as well as waking the ring.
        // LocalSet and io_uring have distinct park/wake paths: the eventfd is
        // still necessary to interrupt io_uring_enter on an otherwise idle queue.
        if let Some(waker) = waker {
            waker.wake();
        }
        // Publishing precedes the wake. A producer arriving after the consumer
        // clears `pending` starts a new batch and must signal again. A producer
        // arriving before that clear is included in the current batch.
        if notify {
            self.signal.notify()?;
        }
        Ok(())
    }
    fn wake_ready(&self) {
        let mut wake: [Option<Waker>; QUEUE_DEPTH as usize] = std::array::from_fn(|_| None);
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let pending = std::mem::take(&mut state.pending);
            for (tag, slot) in state.slots.iter_mut().enumerate() {
                if pending & (1 << tag) != 0 {
                    wake[tag] = slot.waker.take();
                }
            }
        }
        for waker in wake.into_iter().flatten() {
            waker.wake();
        }
    }
    fn fail(&self, error: String) {
        let mut wake: [Option<Waker>; QUEUE_DEPTH as usize] = std::array::from_fn(|_| None);
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.failure = Some(error);
            for (tag, slot) in state.slots.iter_mut().enumerate() {
                wake[tag] = slot.waker.take();
            }
        }
        for waker in wake.into_iter().flatten() {
            waker.wake();
        }
    }
    async fn receive(&self, tag: u16) -> Result<EngineCompletion> {
        poll_fn(|cx| {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(completion) = state.slots[tag as usize].result.take() {
                state.slots[tag as usize].waker = None;
                return Poll::Ready(Ok(completion));
            }
            if let Some(error) = &state.failure {
                return Poll::Ready(Err(anyhow::anyhow!(error.clone())));
            }
            let waker = &mut state.slots[tag as usize].waker;
            if !waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                *waker = Some(cx.waker().clone());
            }
            Poll::Pending
        })
        .await
    }
    fn stop_pump(&self) -> std::io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        self.signal.notify()
    }
    async fn pump(self: Arc<Self>) -> Result<()> {
        while !self.stopping.load(Ordering::Acquire) {
            let event = libublk::ops::poll_add(
                libublk::ops::TgtFd::Raw(self.signal.0.as_raw_fd()),
                libc::POLLIN as u32,
            )?
            .await;
            ensure!(event >= 0, "ublk completion poll failed: {event}");
            ensure!(
                event & i32::from(libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) == 0,
                "ublk completion eventfd failed: {event}"
            );
            self.signal.consume()?;
            self.wake_ready();
        }
        Ok(())
    }
}

async fn execute(engine: &Engine, request: EngineRequest) -> EngineCompletion {
    let EngineRequest {
        op,
        fua,
        offset,
        len,
        buffer,
    } = request;
    let capacity = buffer.0.len();
    let mut buffer = Some(buffer);
    let result = AssertUnwindSafe(async {
        ensure!(len <= capacity, "ublk request exceeds buffer");
        match op {
            libublk::sys::UBLK_IO_OP_READ => {
                let output = engine
                    .read_buffer(offset, len, buffer.take().unwrap())
                    .await?;
                buffer = Some(output);
            }
            libublk::sys::UBLK_IO_OP_WRITE => {
                engine
                    .write(offset, &buffer.as_ref().unwrap().0.as_slice()[..len])
                    .await?;
                if fua {
                    engine.flush().await?;
                }
            }
            libublk::sys::UBLK_IO_OP_FLUSH => engine.flush().await?,
            _ => anyhow::bail!("unsupported ublk op {op}"),
        }
        Ok(if op == libublk::sys::UBLK_IO_OP_FLUSH {
            0
        } else {
            len as i32
        })
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("engine request panicked")));
    // A failed owned READ may have dropped its allocation. No kernel command
    // is armed between FETCH's completion and COMMIT, so replacement is safe
    // with the current copy transport. MLOCK/registered/zero-copy buffers would
    // require preserving the allocation on every error and must not be enabled
    // without changing this contract.
    let buffer = buffer.unwrap_or_else(|| RequestBuffer::new(capacity));
    EngineCompletion { buffer, result }
}

// Local to a queue thread. The last tag must join the auxiliary POLL_ADD task
// while the runtime and its op slab still exist. Dropping/cancelling that task
// at runtime destruction leaves an orphan CQE and can panic in TLS destructors.
struct QueuePump {
    tags: std::cell::Cell<u16>,
    task: std::cell::RefCell<Option<libublk::executor::TaskHandle>>,
}
impl QueuePump {
    async fn finish_tag(&self, inbox: &CompletionInbox, qid: u16) {
        self.tags.set(self.tags.get() - 1);
        if self.tags.get() == 0 {
            if let Err(error) = inbox.stop_pump() {
                tracing::error!(qid, error=%error, "cannot stop ublk completion pump; exiting without final checkpoint");
                std::process::exit(1);
            }
            let task = self.task.borrow_mut().take();
            if let Some(task) = task {
                task.await;
            }
            tracing::trace!(qid, "ublk completion pump drained");
        }
    }
}

fn run_fast_queue(
    engine: Arc<Engine>,
    handle: tokio::runtime::Handle,
    qid: u16,
    dev: &Arc<UblkDev>,
    limit: Arc<Semaphore>,
    failure: Arc<TargetFailure>,
) -> Result<()> {
    let inbox = Arc::new(CompletionInbox::new(dev.dev_info.queue_depth)?);
    let failed_inbox = inbox.clone();
    failure.register_wake(move |error| {
        failed_inbox.fail(error.into());
        if let Err(error) = failed_inbox.signal.notify() {
            tracing::error!(qid, error=%error, "ublk failed-inbox wake failed");
        }
    });
    let drain = Arc::new(WorkerDrain::default());
    let task_inbox = inbox.clone();
    let task_drain = drain.clone();
    let task_failure = failure.clone();
    let pump = std::rc::Rc::new(QueuePump {
        tags: std::cell::Cell::new(dev.dev_info.queue_depth),
        task: std::cell::RefCell::new(None),
    });
    let result = libublk::UblkRuntime::run_io_tasks(dev, qid, move |q, tag| {
        let engine = engine.clone();
        let handle = handle.clone();
        let inbox = task_inbox.clone();
        let drain = task_drain.clone();
        let failure = task_failure.clone();
        let limit = limit.clone();
        let pump = pump.clone();
        async move {
            let mut phase = "setup";
            let result = AssertUnwindSafe(async {
                if tag == 0 {
                    let pump_inbox = inbox.clone();
                    let pump_failure = failure.clone();
                    *pump.task.borrow_mut() = Some(libublk::executor::spawn_local(async move {
                        let ended = AssertUnwindSafe(pump_inbox.pump()).catch_unwind().await;
                        if !matches!(ended, Ok(Ok(()))) {
                            pump_failure.report(format!("ublk queue {qid} completion pump failed: {ended:?}"));
                        }
                    }));
                }
                let (requests, mut pending) = mpsc::channel::<EngineRequest>(1);
                let completions = inbox.clone();
                let worker_failure = failure.clone();
                let worker = drain.start(failure.clone());
                handle.spawn(async move {
                    let _worker = worker;
                    while let Some(request) = pending.recv().await {
                        // One limit covers every queue, not an allowance per tag.
                        let permit = limit.acquire().await.expect("ublk limit stays open");
                        tracing::trace!(qid, tag, op=request.op, len=request.len, "ublk engine started");
                        let completion = execute(&engine, request).await;
                        tracing::trace!(qid, tag, result=?completion.result, "ublk engine completed");
                        drop(permit);
                        if let Err(error) = completions.complete(tag, completion) {
                            worker_failure.report(format!("ublk queue {qid} tag {tag} notification failed: {error}"));
                        }
                    }
                });
                let mut buffer = RequestBuffer::new(q.dev().dev_info.max_io_buf_bytes as usize);
                phase = "initial_fetch";
                let fetched = q
                    .submit_io_prep_cmd(tag, BufDesc::Slice(buffer.0.as_slice()), 0, Some(&buffer.0))
                    .await?;
                if fetched < 0 {
                    return Err(libublk::UblkError::OtherError(fetched));
                }
                loop {
                    phase = "decode_request";
                    let iod = q.get_iod(tag);
                    tracing::trace!(qid, tag, phase, op_flags=iod.op_flags, start_sector=iod.start_sector, nr_sectors=iod.nr_sectors, "ublk descriptor");
                    let res = match decode_request(iod.op_flags, iod.start_sector, iod.nr_sectors, buffer.0.len()) {
                        Ok(fields) => {
                            let request = EngineRequest {
                                op: fields.op,
                                fua: fields.fua,
                                offset: fields.offset,
                                len: fields.len,
                                buffer,
                            };
                            tracing::trace!(qid, tag, op=request.op, len=request.len, fua=request.fua, "ublk request queued");
                            // A full channel violates one-request-per-tag.
                            phase = "send_to_engine";
                            requests.try_send(request)
                                .map_err(|_| libublk::UblkError::OtherError(-libc::EIO))?;
                            phase = "receive_from_engine";
                            let completion = inbox.receive(tag).await
                                .map_err(|_| libublk::UblkError::OtherError(-libc::EIO))?;
                            tracing::trace!(qid, tag, result=?completion.result, "ublk completion received");
                            buffer = completion.buffer;
                            match completion.result {
                                Ok(res) => res,
                                Err(error) => {
                                    tracing::error!(qid, tag, error=%error, "ublk request failed");
                                    -libc::EIO
                                }
                            }
                        }
                        Err(errno) => {
                            // An invalid/unsupported request is completed with
                            // errno; abandoning its tag would hang the block I/O.
                            tracing::warn!(qid, tag, op_flags=iod.op_flags, start_sector=iod.start_sector, nr_sectors=iod.nr_sectors, errno, "ublk request rejected");
                            errno
                        }
                    };
                    phase = "commit_and_fetch";
                    tracing::trace!(qid, tag, result=res, "ublk commit submitted");
                    let fetched = q
                        .submit_io_commit_cmd(tag, BufDesc::Slice(buffer.0.as_slice()), res)
                        .await?;
                    if fetched < 0 {
                        return Err(libublk::UblkError::OtherError(fetched));
                    }
                }
            }).catch_unwind().await
                .unwrap_or(Err(libublk::UblkError::OtherError(-libc::EIO)));
            if let Err(error) = &result
                && !matches!(error, libublk::UblkError::QueueIsDown)
            {
                tracing::error!(qid, tag, phase, error=?error, "ublk tag task stopped");
                failure.report(format!(
                    "ublk queue {qid} tag {tag} stopped at {phase}: {error:?}"
                ));
            }
            pump.finish_tag(&inbox, qid).await;
            result
        }
    });
    inbox.fail("ublk queue shut down".into());
    // Both successful and failed queue shutdowns drain already accepted work
    // before main can publish its final checkpoint. Workers run on outer Tokio.
    drain.wait();
    if let Err(error) = &result {
        failure.report(format!("ublk queue {qid} failed: {error:?}"));
    }
    result.map_err(Into::into)
}

fn run_legacy_queue(
    engine: Arc<Engine>,
    handle: tokio::runtime::Handle,
    qid: u16,
    dev: &Arc<UblkDev>,
    failure: Arc<TargetFailure>,
) -> Result<()> {
    let drain = Arc::new(WorkerDrain::default());
    let task_drain = drain.clone();
    let task_failure = failure.clone();
    let result = libublk::UblkRuntime::run_io_tasks(dev, qid, move |q, tag| {
        let engine = engine.clone();
        let handle = handle.clone();
        let drain = task_drain.clone();
        let failure = task_failure.clone();
        async move {
            let result = AssertUnwindSafe(async {
                let signal =
                    Arc::new(CompletionSignal::new().map_err(libublk::UblkError::IOError)?);
                let failed_signal = signal.clone();
                failure.register_wake(move |_| {
                    if let Err(error) = failed_signal.notify() {
                        tracing::error!(qid, tag, error=%error, "ublk failed-reply wake failed");
                    }
                });
                let mut buf = IoBuf::<u8>::new(q.dev().dev_info.max_io_buf_bytes as usize);
                buf.zero_buf();
                let fetched = q
                    .submit_io_prep_cmd(tag, BufDesc::Slice(buf.as_slice()), 0, Some(&buf))
                    .await?;
                if fetched < 0 {
                    return Err(libublk::UblkError::OtherError(fetched));
                }
                loop {
                    let iod = q.get_iod(tag);
                    let res = match decode_request(
                        iod.op_flags,
                        iod.start_sector,
                        iod.nr_sectors,
                        buf.len(),
                    ) {
                        Err(errno) => errno,
                        Ok(RequestFields {
                            op,
                            fua,
                            offset,
                            len,
                        }) => {
                            let e = engine.clone();
                            let input = if op == libublk::sys::UBLK_IO_OP_WRITE {
                                buf.as_slice()[..len].to_vec()
                            } else {
                                Vec::new()
                            };
                            let (reply, receive) = tokio::sync::oneshot::channel();
                            let wake = signal.clone();
                            let worker_failure = failure.clone();
                            let worker = drain.start(failure.clone());
                            handle.spawn(async move {
                                let _worker = worker;
                                let result = AssertUnwindSafe(async move {
                                    let b = match op {
                                        libublk::sys::UBLK_IO_OP_READ => {
                                            e.read(offset, len).await?
                                        }
                                        libublk::sys::UBLK_IO_OP_WRITE => {
                                            e.write(offset, &input).await?;
                                            if fua {
                                                e.flush().await?;
                                            }
                                            Vec::new()
                                        }
                                        libublk::sys::UBLK_IO_OP_FLUSH => {
                                            e.flush().await?;
                                            Vec::new()
                                        }
                                        _ => anyhow::bail!("unsupported ublk op {op}"),
                                    };
                                    Ok::<_, anyhow::Error>(b)
                                })
                                .catch_unwind()
                                .await
                                .unwrap_or_else(|_| {
                                    Err(anyhow::anyhow!("engine request panicked"))
                                });
                                let _ = reply.send(result);
                                if let Err(error) = wake.notify() {
                                    worker_failure.report(format!(
                                        "ublk queue {qid} tag {tag} notification failed: {error}"
                                    ));
                                }
                            });
                            let event = libublk::ops::poll_add(
                                libublk::ops::TgtFd::Raw(signal.0.as_raw_fd()),
                                libc::POLLIN as u32,
                            )?
                            .await;
                            if event < 0 {
                                return Err(libublk::UblkError::OtherError(event));
                            }
                            signal.consume().map_err(libublk::UblkError::IOError)?;
                            failure
                                .result()
                                .map_err(|_| libublk::UblkError::OtherError(-libc::EIO))?;
                            match receive.await {
                                Ok(Ok(b)) => {
                                    if op == libublk::sys::UBLK_IO_OP_READ {
                                        buf.as_mut_slice()[..b.len()].copy_from_slice(&b);
                                    }
                                    if op == libublk::sys::UBLK_IO_OP_FLUSH {
                                        0
                                    } else {
                                        len as i32
                                    }
                                }
                                other => {
                                    tracing::error!(qid, tag, error=?other, "ublk request failed");
                                    -libc::EIO
                                }
                            }
                        }
                    };
                    let fetched = q
                        .submit_io_commit_cmd(tag, BufDesc::Slice(buf.as_slice()), res)
                        .await?;
                    if fetched < 0 {
                        return Err(libublk::UblkError::OtherError(fetched));
                    }
                }
            })
            .catch_unwind()
            .await
            .unwrap_or(Err(libublk::UblkError::OtherError(-libc::EIO)));
            if let Err(error) = &result
                && !matches!(error, libublk::UblkError::QueueIsDown)
            {
                failure.report(format!("ublk queue {qid} tag {tag} stopped: {error:?}"));
            }
            result
        }
    });
    drain.wait();
    if let Err(error) = &result {
        failure.report(format!("ublk queue {qid} failed: {error:?}"));
    }
    result.map_err(Into::into)
}

pub fn delete(id: i32) -> Result<()> {
    ensure!(id >= 0, "explicit device id required");
    let ctrl = libublk::ctrl::UblkCtrl::new_simple(id)?;
    ensure!(
        ctrl.get_target_type_from_json()? == "infinidisk2-experimental",
        "device is not an InfiniDisk2 experimental target"
    );
    use std::os::unix::fs::OpenOptionsExt;
    let disk = format!("/dev/ublkb{id}");
    let _claim = if ctrl.dev_info().state as u32 == libublk::sys::UBLK_S_DEV_DEAD {
        // Without USER_RECOVERY the kernel removes the block disk after server
        // death, but leaves the owned control target for explicit deletion.
        ensure!(
            !std::path::Path::new(&disk).exists()
                && !std::path::Path::new(&format!("/sys/class/block/ublkb{id}")).exists(),
            "dead ublk target still has a block disk"
        );
        let mounts = std::fs::read_to_string("/proc/self/mountinfo")?;
        ensure!(
            !mounts.lines().any(|line| line
                .split_once(" - ")
                .and_then(|(_, tail)| tail.split_whitespace().nth(1))
                == Some(disk.as_str())),
            "dead ublk disk is still mounted"
        );
        None
    } else {
        Some(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_EXCL)
                .open(&disk)?,
        )
    };
    // Synchronous deletion can wait for this exclusive block-device claim.
    // The async control request stops queues; dropping the claim lets removal finish.
    ctrl.del_dev_async()?;
    Ok(())
}
pub fn serve(
    engine: Arc<Engine>,
    handle: tokio::runtime::Handle,
    id: i32,
    queues: u16,
) -> Result<()> {
    ensure!(
        id >= 0 && (1..=8).contains(&queues),
        "invalid ublk id/queue count"
    );
    ensure!(
        !std::path::Path::new(&format!("/dev/ublkc{id}")).exists()
            && !std::path::Path::new(&format!("/sys/class/block/ublkb{id}")).exists(),
        "ublk device id is already in use"
    );
    let fast = engine.config.ublk_fast_path;
    let limit = Arc::new(Semaphore::new(engine.config.max_inflight));
    let depth = if fast {
        // At most max_inflight slots, except that every configured queue needs
        // one slot when max_inflight < queues. Execution still obeys the shared
        // semaphore. The fixed transport memory is at most 8 * 32 * 1 MiB.
        (engine.config.max_inflight / usize::from(queues)).clamp(1, QUEUE_DEPTH as usize) as u16
    } else {
        QUEUE_DEPTH
    };
    let ctrl = UblkCtrlBuilder::default()
        .name("infinidisk2-experimental")
        .id(id)
        .nr_queues(queues)
        .depth(depth)
        .io_buf_bytes(BUFFER_BYTES as u32)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()?;
    let size = engine.identity.size;
    let owner = std::process::id() as i32;
    let (failure, controller) = TargetFailure::new(move || {
        let result: Result<()> = (|| {
            let target = libublk::ctrl::UblkCtrl::new_simple(id)?;
            ensure!(
                target.dev_info().ublksrv_pid == owner,
                "ublk device owner changed; refuse to stop it"
            );
            target.kill_dev()?;
            Ok(())
        })();
        if let Err(error) = result {
            // Returning here can leave the remaining FETCH tasks parked forever.
            // Last resort is a recoverable process crash, not a claimed drain.
            tracing::error!(id, owner, error=%error, "ublk emergency stop failed; exiting without final checkpoint");
            std::process::exit(1);
        }
    })?;
    let queue_failure = failure.clone();
    let target_result = ctrl.run_target(
        move |dev: &mut UblkDev| {
            dev.tgt.dev_size = size;
            if fast {
                // One POLL_ADD accompanies the tags; retain headroom for its
                // rearm without changing kernel depth or buffer/inflight quotas.
                dev.tgt.sq_depth = depth * 2;
                dev.tgt.cq_depth = depth * 2;
            }
            dev.tgt.params = libublk::sys::ublk_params {
                types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
                basic: libublk::sys::ublk_param_basic {
                    attrs: libublk::sys::UBLK_ATTR_VOLATILE_CACHE | libublk::sys::UBLK_ATTR_FUA,
                    logical_bs_shift: 9,
                    physical_bs_shift: 12,
                    io_opt_shift: 12,
                    io_min_shift: 9,
                    max_sectors: dev.dev_info.max_io_buf_bytes >> 9,
                    dev_sectors: size >> 9,
                    ..Default::default()
                },
                ..Default::default()
            };
            Ok(())
        },
        move |qid, dev| {
            let result = if fast {
                run_fast_queue(
                    engine.clone(),
                    handle.clone(),
                    qid,
                    dev,
                    limit.clone(),
                    queue_failure.clone(),
                )
            } else {
                run_legacy_queue(
                    engine.clone(),
                    handle.clone(),
                    qid,
                    dev,
                    queue_failure.clone(),
                )
            };
            if let Err(error) = result {
                queue_failure.report(format!("ublk queue {qid} stopped: {error:#}"));
            }
        },
        move |ctrl| {
            ctrl.dump();
        },
    );
    if let Err(error) = &target_result {
        failure.report(format!("ublk target failed: {error:?}"));
    }
    failure.finish();
    controller
        .join()
        .map_err(|_| anyhow::anyhow!("ublk controller panicked"))?;
    failure.result()?;
    target_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        pin::pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake},
    };

    #[derive(Default)]
    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn completion(value: u8) -> EngineCompletion {
        let mut buffer = RequestBuffer::new(4096);
        buffer.as_mut().fill(value);
        EngineCompletion {
            buffer,
            result: Ok(4096),
        }
    }

    #[test]
    fn flush_descriptor_does_not_decode_unused_sector_or_length() {
        let fields =
            decode_request(libublk::sys::UBLK_IO_OP_FLUSH, u64::MAX, u32::MAX, 4096).unwrap();
        assert_eq!(fields.offset, 0);
        assert_eq!(fields.len, 0);
        assert_eq!(fields.op, libublk::sys::UBLK_IO_OP_FLUSH);
        let fields = decode_request(
            libublk::sys::UBLK_IO_OP_WRITE | libublk::sys::UBLK_IO_F_FUA,
            8,
            8,
            4096,
        )
        .unwrap();
        assert_eq!((fields.offset, fields.len, fields.fua), (4096, 4096, true));
        assert_eq!(
            decode_request(libublk::sys::UBLK_IO_OP_READ, u64::MAX, 8, 4096),
            Err(-libc::EINVAL)
        );
        assert_eq!(
            decode_request(libublk::sys::UBLK_IO_OP_WRITE, 0, 9, 4096),
            Err(-libc::EINVAL)
        );
        assert_eq!(
            decode_request(0xff, u64::MAX, u32::MAX, 4096),
            Err(-libc::EOPNOTSUPP)
        );
    }

    #[test]
    fn first_transport_error_stops_once_and_is_returned() {
        let stops = Arc::new(AtomicUsize::new(0));
        let stopped = stops.clone();
        let caller = std::thread::current().id();
        let (failure, controller) = TargetFailure::new(move || {
            assert_ne!(std::thread::current().id(), caller);
            stopped.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
        failure.report("first failure".into());
        std::thread::scope(|scope| {
            for _ in 0..32 {
                let failure = failure.clone();
                scope.spawn(move || failure.report("later failure".into()));
            }
        });
        failure.finish();
        controller.join().unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        assert_eq!(failure.result().unwrap_err().to_string(), "first failure");
    }

    #[test]
    fn normal_queue_shutdown_drains_workers_without_stopping_device_again() {
        let (failure, controller) = TargetFailure::new(|| panic!("unexpected stop")).unwrap();
        let drain = Arc::new(WorkerDrain::default());
        let first = drain.start(failure.clone());
        let second = drain.start(failure.clone());
        let waiting = drain.clone();
        let (done, receive) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            waiting.wait();
            done.send(()).unwrap();
        });
        drop(first);
        assert!(receive.try_recv().is_err());
        assert_eq!(*drain.active.lock().unwrap(), 1);
        drop(second);
        receive
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        thread.join().unwrap();
        failure.finish();
        controller.join().unwrap();
        failure.result().unwrap();
    }

    #[test]
    fn dead_worker_fails_userspace_wait_before_stopping_kernel_queue() {
        let inbox = Arc::new(CompletionInbox::new(1).unwrap());
        let stopped_inbox = inbox.clone();
        let (failure, controller) = TargetFailure::new(move || {
            // A mocked STOP callback verifies ordering, not a kernel STOP test.
            assert!(stopped_inbox.state.lock().unwrap().failure.is_some());
        })
        .unwrap();
        let failed_inbox = inbox.clone();
        failure.register_wake(move |error| {
            failed_inbox.fail(error.into());
            failed_inbox.signal.notify().unwrap();
        });
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut receive = pin!(inbox.receive(0));
        assert!(receive.as_mut().poll(&mut cx).is_pending());
        let drain = Arc::new(WorkerDrain::default());
        let guard = drain.start(failure.clone());
        assert!(
            std::thread::spawn(move || {
                let _guard = guard;
                panic!("injected worker death before complete");
            })
            .join()
            .is_err()
        );
        drain.wait();
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(inbox.signal.consume().unwrap(), 1);
        assert!(matches!(
            receive.as_mut().poll(&mut cx),
            Poll::Ready(Err(_))
        ));
        let late_wake = Arc::new(AtomicUsize::new(0));
        let observed = late_wake.clone();
        failure.register_wake(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(late_wake.load(Ordering::SeqCst), 1);
        failure.finish();
        controller.join().unwrap();
        assert_eq!(
            failure.result().unwrap_err().to_string(),
            "ublk engine worker panicked"
        );
    }

    #[test]
    fn queue_completion_batch_keeps_buffers_and_signals_once() {
        let inbox = CompletionInbox::new(QUEUE_DEPTH).unwrap();
        let mut pointers = Vec::new();
        for tag in 0..QUEUE_DEPTH {
            let completion = completion(tag as u8);
            pointers.push(completion.buffer.0.as_ptr());
            inbox.complete(tag, completion).unwrap();
        }
        assert_eq!(inbox.signal.consume().unwrap(), 1);
        inbox.wake_ready();
        for tag in 0..QUEUE_DEPTH {
            let completion = futures::executor::block_on(inbox.receive(tag)).unwrap();
            assert_eq!(completion.result.unwrap(), 4096);
            assert_eq!(completion.buffer.0.as_ptr(), pointers[tag as usize]);
            assert!(
                completion
                    .buffer
                    .0
                    .as_slice()
                    .iter()
                    .all(|b| *b == tag as u8)
            );
        }
        assert_eq!(inbox.signal.consume().unwrap(), 0);
    }

    #[test]
    fn completion_wakes_waiter_and_rearms_the_next_batch() {
        let inbox = CompletionInbox::new(2).unwrap();
        let count = Arc::new(WakeCount::default());
        let waker = Waker::from(count.clone());
        let mut cx = Context::from_waker(&waker);
        let mut receive = pin!(inbox.receive(0));
        assert!(receive.as_mut().poll(&mut cx).is_pending());
        inbox.complete(0, completion(11)).unwrap();
        // Engine completion schedules the waiter before the ring pump runs.
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        assert_eq!(inbox.signal.consume().unwrap(), 1);
        // A completion arriving between eventfd consumption and draining the
        // inbox belongs to that same batch and needs no second eventfd write.
        inbox.complete(1, completion(22)).unwrap();
        inbox.wake_ready();
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        assert!(receive.as_mut().poll(&mut cx).is_ready());
        assert_eq!(
            futures::executor::block_on(inbox.receive(1))
                .unwrap()
                .result
                .unwrap(),
            4096
        );
        assert_eq!(inbox.signal.consume().unwrap(), 0);
        inbox.complete(0, completion(33)).unwrap();
        assert_eq!(inbox.signal.consume().unwrap(), 1);
        inbox.wake_ready();
        assert_eq!(
            futures::executor::block_on(inbox.receive(0))
                .unwrap()
                .buffer
                .0
                .as_slice()[0],
            33
        );
    }

    #[test]
    fn early_completion_and_tag_reuse_do_not_grow_or_lose_a_batch() {
        let inbox = CompletionInbox::new(1).unwrap();
        for value in 0..100_u8 {
            inbox.complete(0, completion(value)).unwrap();
            let result = futures::executor::block_on(inbox.receive(0)).unwrap();
            assert_eq!(result.buffer.0.as_slice()[0], value);
        }
        // The pump never ran; the same bit represents all early completions.
        assert_eq!(inbox.state.lock().unwrap().pending, 1);
        assert_eq!(inbox.signal.consume().unwrap(), 1);
        inbox.wake_ready();
        assert_eq!(inbox.state.lock().unwrap().pending, 0);
        inbox.complete(0, completion(101)).unwrap();
        assert_eq!(inbox.signal.consume().unwrap(), 1);
        inbox.wake_ready();
        assert_eq!(
            futures::executor::block_on(inbox.receive(0))
                .unwrap()
                .buffer
                .0
                .as_slice()[0],
            101
        );
    }

    #[test]
    fn cross_thread_completion_repeatedly_survives_rearm_races() {
        let inbox = Arc::new(CompletionInbox::new(1).unwrap());
        let producer = inbox.clone();
        let (ack, received) = std::sync::mpsc::sync_channel(0);
        let thread = std::thread::spawn(move || {
            for generation in 0..1000 {
                producer
                    .complete(0, completion((generation % 251) as u8))
                    .unwrap();
                received.recv().unwrap();
            }
        });
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for generation in 0..1000 {
            let mut receive = pin!(inbox.receive(0));
            let result = loop {
                if let Poll::Ready(result) = receive.as_mut().poll(&mut cx) {
                    break result.unwrap();
                }
                let mut poll = libc::pollfd {
                    fd: inbox.signal.0.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // Same readiness discipline as the queue's io_uring POLL_ADD,
                // without needing /dev/ublk-control to test the wake protocol.
                let ready = unsafe { libc::poll(&mut poll, 1, 1000) };
                assert!(
                    ready > 0,
                    "lost cross-thread completion wake at {generation}"
                );
                inbox.signal.consume().unwrap();
                inbox.wake_ready();
            };
            assert_eq!(result.buffer.0.as_slice()[0], (generation % 251) as u8);
            ack.send(()).unwrap();
        }
        thread.join().unwrap();
    }

    /// Exercise the actual LocalSet/io_uring scheduler, not a manually polled
    /// future. Run explicitly under an external watchdog on a Linux test host.
    #[test]
    #[ignore = "requires Linux io_uring; run with an external 30-second timeout"]
    fn queue_runtime_delivers_cross_runtime_completions() -> Result<()> {
        fn exercise() -> Result<()> {
            use libublk::io_uring::IoUring;
            use std::cell::RefCell;
            use std::time::Duration;

            libublk::ublk_init_task_ring(|cell| {
                let ring = IoUring::builder()
                    .setup_cqsize(32)
                    .setup_coop_taskrun()
                    .build(32)?;
                cell.set(RefCell::new(ring))
                    .map_err(|_| libublk::UblkError::OtherError(-libc::EEXIST))
            })?;
            let engine_runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            let runtime = libublk::UblkRuntime::new()?;
            let inbox = Arc::new(CompletionInbox::new(QUEUE_DEPTH)?);
            let delivered = Arc::new(AtomicUsize::new(0));
            let mut workers = Vec::new();
            runtime.block_on(async {
                let pump_inbox = inbox.clone();
                let pump = std::rc::Rc::new(QueuePump {
                    tags: std::cell::Cell::new(QUEUE_DEPTH),
                    task: RefCell::new(Some(libublk::executor::spawn_local(async move {
                        pump_inbox.pump().await.unwrap();
                    }))),
                });
                let mut tasks = Vec::new();
                for tag in 0..QUEUE_DEPTH {
                    let (send, mut receive) = mpsc::channel::<u8>(1);
                    let complete = inbox.clone();
                    workers.push(engine_runtime.spawn(async move {
                        while let Some(value) = receive.recv().await {
                            if value % 7 == 0 {
                                tokio::time::sleep(Duration::from_micros(100)).await;
                            }
                            complete.complete(tag, completion(value)).unwrap();
                        }
                    }));
                    let inbox = inbox.clone();
                    let delivered = delivered.clone();
                    let pump = pump.clone();
                    tasks.push(libublk::executor::spawn_local(async move {
                        for generation in 0..512 {
                            let value = ((generation + usize::from(tag)) % 251) as u8;
                            send.try_send(value).unwrap();
                            // Alternate completion-before-wait and pending waiters,
                            // with real ring CQEs in place of kernel FETCH/COMMIT.
                            if generation % 2 == 0 {
                                assert_eq!(libublk::ops::nop().unwrap().await, 0);
                            }
                            let completed = inbox.receive(tag).await.unwrap();
                            assert_eq!(completed.buffer.0.as_slice()[0], value);
                            assert_eq!(libublk::ops::nop().unwrap().await, 0);
                            delivered.fetch_add(1, Ordering::Relaxed);
                        }
                        pump.finish_tag(&inbox, 0).await;
                    }));
                }
                for task in tasks {
                    task.await;
                }
                assert_eq!(pump.tags.get(), 0);
                assert!(
                    !libublk::reactor::has_pending_ops(),
                    "pump or tag left an orphan ring operation"
                );
            });
            engine_runtime.block_on(async {
                for worker in workers {
                    worker.await.unwrap();
                }
            });
            // TaskHandle deliberately erases panics: this also detects any tag
            // that exited early instead of counting that exit as successful work.
            assert_eq!(
                delivered.load(Ordering::Relaxed),
                512 * usize::from(QUEUE_DEPTH)
            );
            Ok(())
        }
        // New OS threads exercise TLS teardown, not only future completion.
        for _ in 0..3 {
            std::thread::spawn(exercise)
                .join()
                .map_err(|_| anyhow::anyhow!("ublk runtime thread panicked during teardown"))??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn owned_requests_preserve_fua_flush_and_read_buffer_on_success() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let config = crate::config::Config {
            local_dir: temp.path().join("local"),
            store: format!("file://{}", temp.path().join("objects").display()),
            fast_local_reads: true,
            ..Default::default()
        };
        Engine::init(&config, 1024 * 1024).await?;
        let engine = Engine::open(config.clone()).await?;
        let mut buffer = RequestBuffer::new(16384);
        buffer.as_mut().fill(17);
        let pointer = buffer.0.as_ptr();
        let written = execute(
            &engine,
            EngineRequest {
                op: libublk::sys::UBLK_IO_OP_WRITE,
                fua: true,
                offset: 0,
                len: 16384,
                buffer,
            },
        )
        .await;
        assert_eq!(written.result?, 16384);
        assert_eq!(written.buffer.0.as_ptr(), pointer);
        assert_eq!(engine.status().await.local_durable_sequence, 1);
        let mut buffer = written.buffer;
        buffer.as_mut().fill(23);
        let written = execute(
            &engine,
            EngineRequest {
                op: libublk::sys::UBLK_IO_OP_WRITE,
                fua: false,
                offset: 16384,
                len: 4096,
                buffer,
            },
        )
        .await;
        assert_eq!(written.result?, 4096);
        assert_eq!(engine.status().await.local_durable_sequence, 1);
        let flushed = execute(
            &engine,
            EngineRequest {
                op: libublk::sys::UBLK_IO_OP_FLUSH,
                fua: false,
                offset: 0,
                len: 0,
                buffer: written.buffer,
            },
        )
        .await;
        assert_eq!(flushed.result?, 0);
        assert_eq!(engine.status().await.local_durable_sequence, 2);
        let read = execute(
            &engine,
            EngineRequest {
                op: libublk::sys::UBLK_IO_OP_READ,
                fua: false,
                offset: 512,
                len: 8192,
                buffer: flushed.buffer,
            },
        )
        .await;
        assert_eq!(read.result?, 8192);
        assert_eq!(read.buffer.0.as_ptr(), pointer);
        assert_eq!(&read.buffer.0.as_slice()[..8192], &[17; 8192]);
        // A failing owned read loses no worker/tag: its replacement allocation
        // can be used by the next valid request, without exposing stale bytes.
        let failed = execute(
            &engine,
            EngineRequest {
                op: libublk::sys::UBLK_IO_OP_READ,
                fua: false,
                offset: 2 * 1024 * 1024,
                len: 4096,
                buffer: read.buffer,
            },
        )
        .await;
        assert!(failed.result.is_err());
        assert_eq!(failed.buffer.0.len(), 16384);
        let read = execute(
            &engine,
            EngineRequest {
                op: libublk::sys::UBLK_IO_OP_READ,
                fua: false,
                offset: 16384,
                len: 4096,
                buffer: failed.buffer,
            },
        )
        .await;
        assert_eq!(read.result?, 4096);
        assert_eq!(&read.buffer.0.as_slice()[..4096], &[23; 4096]);
        drop(engine);
        let reopened = Engine::open(config).await?;
        assert_eq!(reopened.read(0, 16384).await?, vec![17; 16384]);
        assert_eq!(reopened.read(16384, 4096).await?, vec![23; 4096]);
        Ok(())
    }
}
