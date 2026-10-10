//! Experimental ublk transport. Copies are retained; FLUSH and FUA are honored.
use crate::engine::Engine;
use anyhow::{Result, ensure};
use futures::FutureExt;
use libublk::{BufDesc, UblkFlags, ctrl::UblkCtrlBuilder, helpers::IoBuf, io::UblkDev};
use std::sync::Arc;
use std::{
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd},
    panic::AssertUnwindSafe,
};

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
        (&self.0).write_all(&1_u64.to_ne_bytes())
    }
    fn consume(&self) -> std::io::Result<()> {
        let mut value = [0; 8];
        (&self.0).read_exact(&mut value)
    }
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
    let ctrl = UblkCtrlBuilder::default()
        .name("infinidisk2-experimental")
        .id(id)
        .nr_queues(queues)
        .depth(32)
        .io_buf_bytes(1024 * 1024)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()?;
    let size = engine.identity.size;
    ctrl.run_target(
        move |dev: &mut UblkDev| {
            dev.tgt.dev_size = size;
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
            let engine = engine.clone();
            let handle = handle.clone();
            let result = libublk::UblkRuntime::run_io_tasks(dev, qid, move |q, tag| {
                let engine = engine.clone();
                let handle = handle.clone();
                async move {
                    let signal =
                        Arc::new(CompletionSignal::new().map_err(libublk::UblkError::IOError)?);
                    let mut buf = IoBuf::<u8>::new(q.dev().dev_info.max_io_buf_bytes as usize);
                    let fetched = q
                        .submit_io_prep_cmd(tag, BufDesc::Slice(buf.as_slice()), 0, Some(&buf))
                        .await?;
                    if fetched < 0 {
                        return Err(libublk::UblkError::OtherError(fetched));
                    }
                    loop {
                        let iod = q.get_iod(tag);
                        let op = iod.op_flags & 0xff;
                        let fua = iod.op_flags & libublk::sys::UBLK_IO_F_FUA != 0;
                        let offset = iod.start_sector << 9;
                        let len = (iod.nr_sectors as usize) << 9;
                        let e = engine.clone();
                        let input = if op == libublk::sys::UBLK_IO_OP_WRITE {
                            buf.as_slice()[..len].to_vec()
                        } else {
                            Vec::new()
                        };
                        let (reply, receive) = tokio::sync::oneshot::channel();
                        let wake = signal.clone();
                        handle.spawn(async move {
                            let result = AssertUnwindSafe(async move {
                                let b = match op {
                                    libublk::sys::UBLK_IO_OP_READ => e.read(offset, len).await?,
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
                            .unwrap_or_else(|_| Err(anyhow::anyhow!("engine request panicked")));
                            let _ = reply.send(result);
                            if let Err(err) = wake.notify() {
                                tracing::error!(error=%err,"ublk completion notification failed");
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
                        let result = receive.await;
                        let res = match result {
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
                                tracing::error!(error=?other,"ublk request failed");
                                -libc::EIO
                            }
                        };
                        let fetched = q
                            .submit_io_commit_cmd(tag, BufDesc::Slice(buf.as_slice()), res)
                            .await?;
                        if fetched < 0 {
                            return Err(libublk::UblkError::OtherError(fetched));
                        }
                    }
                }
            });
            if let Err(err) = result {
                tracing::error!(error=%err,"ublk queue stopped");
            }
        },
        move |ctrl| {
            ctrl.dump();
        },
    )?;
    Ok(())
}
