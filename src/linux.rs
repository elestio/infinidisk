//! Native Linux NBD attachment. The command stays in NBD_DO_IT until detached.
//! No shell daemon, ZeroFS process, or nbd-client dependency is involved.
use crate::config::Config;
use anyhow::{Context, Result, ensure};
use fs2::FileExt as _;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::TcpStream,
    os::fd::AsRawFd,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    time::Duration,
};
const SET_SOCK: libc::c_ulong = 0xab00;
const SET_BLKSIZE: libc::c_ulong = 0xab01;
const DO_IT: libc::c_ulong = 0xab03;
const CLEAR_SOCK: libc::c_ulong = 0xab04;
const SET_SIZE_BLOCKS: libc::c_ulong = 0xab07;
const DISCONNECT: libc::c_ulong = 0xab08;
const SET_TIMEOUT: libc::c_ulong = 0xab09;
const SET_FLAGS: libc::c_ulong = 0xab0a;
fn ioctl(f: &File, cmd: libc::c_ulong, arg: libc::c_ulong) -> Result<()> {
    // SAFETY: Linux NBD ioctls here take scalar unsigned-long arguments, no pointers.
    if unsafe { libc::ioctl(f.as_raw_fd(), cmd, arg) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
fn validate(d: &Path) -> Result<String> {
    let n = d
        .file_name()
        .and_then(|n| n.to_str())
        .context("invalid device")?;
    ensure!(
        d.parent() == Some(Path::new("/dev"))
            && n.strip_prefix("nbd")
                .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())),
        "device must be /dev/nbd<number>"
    );
    Ok(n.into())
}
fn negotiate(c: &Config) -> Result<(TcpStream, u64, u16)> {
    let mut s = TcpStream::connect_timeout(&c.listen, Duration::from_secs(5))?;
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(Duration::from_secs(10)))?;
    s.set_write_timeout(Some(Duration::from_secs(10)))?;
    let mut h = [0; 18];
    s.read_exact(&mut h)?;
    ensure!(
        u64::from_be_bytes(h[..8].try_into()?) == 0x4e42444d41474943
            && u64::from_be_bytes(h[8..16].try_into()?) == 0x49484156454f5054
            && u16::from_be_bytes(h[16..].try_into()?) & 3 == 3,
        "unsupported NBD handshake"
    );
    s.write_all(&3u32.to_be_bytes())?;
    s.write_all(&0x49484156454f5054u64.to_be_bytes())?;
    s.write_all(&1u32.to_be_bytes())?;
    s.write_all(&11u32.to_be_bytes())?;
    s.write_all(b"infinidisk2")?;
    let mut export = [0; 10];
    s.read_exact(&mut export)?;
    let size = u64::from_be_bytes(export[..8].try_into()?);
    let flags = u16::from_be_bytes(export[8..].try_into()?);
    ensure!(
        size > 0 && size % 4096 == 0 && flags & 256 != 0,
        "invalid export or no multi-connection support"
    );
    s.set_read_timeout(None)?;
    s.set_write_timeout(None)?;
    Ok((s, size, flags))
}
pub fn attach(c: &Config, d: &Path, connections: u8) -> Result<()> {
    ensure!(
        connections > 0 && connections <= 32,
        "connections must be 1..32"
    );
    let n = validate(d)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(format!("/run/lock/infinidisk2-{n}"))?;
    lock.try_lock_exclusive()
        .context("NBD device already reserved by InfiniDisk2")?;
    ensure!(
        !Path::new("/sys/class/block").join(&n).join("pid").exists(),
        "NBD device already attached"
    );
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(d)
        .context("load Linux nbd module first")?;
    let mut sockets = Vec::new();
    let mut shape = None;
    for _ in 0..connections {
        let (s, size, flags) = negotiate(c)?;
        if let Some(old) = shape {
            ensure!(
                old == (size, flags),
                "export changed during multi-connection negotiation"
            );
        } else {
            shape = Some((size, flags));
        }
        sockets.push(s);
    }
    let (size, flags) = shape.unwrap();
    // First successful SET_SOCK marks ownership; never clear someone else's device.
    ioctl(&f, SET_SOCK, sockets[0].as_raw_fd() as libc::c_ulong)?;
    let result = (|| {
        for s in sockets.iter().skip(1) {
            ioctl(&f, SET_SOCK, s.as_raw_fd() as libc::c_ulong)?;
        }
        ioctl(&f, SET_BLKSIZE, 4096)?;
        ioctl(&f, SET_SIZE_BLOCKS, (size / 4096) as libc::c_ulong)?;
        ioctl(&f, SET_TIMEOUT, 60)?;
        ioctl(&f, SET_FLAGS, flags as libc::c_ulong)?;
        tracing::info!(device=%d.display(),connections,size,"entering Linux NBD_DO_IT; keep this process running");
        ioctl(&f, DO_IT, 0)
    })();
    let _ = ioctl(&f, CLEAR_SOCK, 0);
    result
}
pub fn detach(d: &Path) -> Result<()> {
    let n = validate(d)?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo")?;
    let dev = std::fs::read_to_string(Path::new("/sys/class/block").join(&n).join("dev"))?;
    ensure!(
        !mounts
            .lines()
            .any(|line| line.split_whitespace().nth(2) == Some(dev.trim())),
        "device is mounted; unmount first"
    );
    let holders = Path::new("/sys/class/block").join(&n).join("holders");
    ensure!(
        std::fs::read_dir(holders)?.next().is_none(),
        "device has block-device holders"
    );
    // Kernel exclusive block-device claiming also catches mounts in other namespaces
    // and mounted child partitions, and closes the mount-versus-detach race.
    let f = OpenOptions::new().read(true).write(true).custom_flags(libc::O_EXCL).open(d)
        .context("device is still claimed (filesystem, partition, namespace or mapper); unmount all consumers first")?;
    ioctl(&f, DISCONNECT, 0)
}
