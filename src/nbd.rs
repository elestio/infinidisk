//! Fixed-newstyle NBD, simple replies, shared cache and global FLUSH/FUA.
use crate::{engine::Engine, wal::MAX_IO};
use anyhow::{Result, bail, ensure};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, watch},
    task::JoinSet,
};
const NBD_MAGIC: u64 = 0x4e42444d41474943;
const IHAVEOPT: u64 = 0x49484156454f5054;
const OPT_REPLY: u64 = 0x3e889045565a9;
const REQUEST: u32 = 0x25609513;
const REPLY: u32 = 0x67446698;
const EXPORT_FLAGS: u16 = 1 | 4 | 8 | 32 | 64 | 256;
async fn option_reply(s: &mut TcpStream, opt: u32, kind: u32, payload: &[u8]) -> Result<()> {
    s.write_u64(OPT_REPLY).await?;
    s.write_u32(opt).await?;
    s.write_u32(kind).await?;
    s.write_u32(payload.len() as u32).await?;
    s.write_all(payload).await?;
    Ok(())
}
async fn handshake(s: &mut TcpStream, e: &Engine) -> Result<bool> {
    s.set_nodelay(true)?;
    s.write_u64(NBD_MAGIC).await?;
    s.write_u64(IHAVEOPT).await?;
    s.write_u16(3).await?;
    let client = s.read_u32().await?;
    ensure!(client & !3 == 0 && client & 1 != 0, "invalid client flags");
    loop {
        ensure!(s.read_u64().await? == IHAVEOPT, "invalid option magic");
        let opt = s.read_u32().await?;
        let len = s.read_u32().await? as usize;
        ensure!(len <= 65536, "option payload too large");
        let mut payload = vec![0; len];
        s.read_exact(&mut payload).await?;
        match opt {
            1 => {
                if !payload.is_empty() && payload != b"infinidisk2" {
                    bail!("unknown export");
                }
                s.write_u64(e.identity.size).await?;
                s.write_u16(EXPORT_FLAGS).await?;
                if client & 2 == 0 {
                    s.write_all(&[0; 124]).await?;
                }
                return Ok(true);
            }
            2 => {
                option_reply(s, opt, 1, &[]).await?;
                return Ok(false);
            }
            3 => {
                let mut b = Vec::new();
                b.extend_from_slice(&11u32.to_be_bytes());
                b.extend_from_slice(b"infinidisk2");
                option_reply(s, opt, 2, &b).await?;
                option_reply(s, opt, 1, &[]).await?;
            }
            6 | 7 => {
                let valid = if len >= 6 {
                    let name_len = u32::from_be_bytes(payload[..4].try_into()?) as usize;
                    if name_len.checked_add(6).is_some_and(|n| n <= len) {
                        let n = 4 + name_len;
                        let count = u16::from_be_bytes(payload[n..n + 2].try_into()?) as usize;
                        (name_len == 0 || &payload[4..n] == b"infinidisk2")
                            && n + 2 + 2 * count == len
                    } else {
                        false
                    }
                } else {
                    false
                };
                if !valid {
                    option_reply(s, opt, 0x80000003, &[]).await?;
                    continue;
                }
                let mut info = Vec::new();
                info.extend_from_slice(&0u16.to_be_bytes());
                info.extend_from_slice(&e.identity.size.to_be_bytes());
                info.extend_from_slice(&EXPORT_FLAGS.to_be_bytes());
                option_reply(s, opt, 3, &info).await?;
                let mut sizes = Vec::new();
                sizes.extend_from_slice(&3u16.to_be_bytes());
                for n in [512u32, 4096, MAX_IO as u32] {
                    sizes.extend_from_slice(&n.to_be_bytes());
                }
                option_reply(s, opt, 3, &sizes).await?;
                option_reply(s, opt, 1, &[]).await?;
                if opt == 7 {
                    return Ok(true);
                }
            }
            _ => option_reply(s, opt, 0x80000001, &[]).await?,
        }
    }
}
struct Response {
    handle: u64,
    errno: u32,
    data: Vec<u8>,
    _memory: tokio::sync::OwnedSemaphorePermit,
}
async fn connection(
    mut socket: TcpStream,
    e: Arc<Engine>,
    limit: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let proceed = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        handshake(&mut socket, &e),
    )
    .await??;
    if !proceed {
        return Ok(());
    }
    let (mut reader, mut writer) = socket.into_split();
    let (tx, mut rx) = mpsc::channel::<Response>(32);
    let replies = tokio::spawn(async move {
        while let Some(r) = rx.recv().await {
            writer.write_u32(REPLY).await?;
            writer.write_u32(r.errno).await?;
            writer.write_u64(r.handle).await?;
            if r.errno == 0 {
                writer.write_all(&r.data).await?;
            }
        }
        Ok::<_, std::io::Error>(())
    });
    let mut tasks = JoinSet::new();
    let mut error = None;
    loop {
        let magic = tokio::select! {
            _=shutdown.changed()=>break,
            result=reader.read_u32()=> match result {Ok(m)=>m,Err(err)=> { if err.kind()!=std::io::ErrorKind::UnexpectedEof {error=Some(err.into());} break;}}
        };
        if magic != REQUEST {
            error = Some(anyhow::anyhow!("invalid request magic"));
            break;
        }
        let header = async {
            let flags = reader.read_u16().await?;
            let command = reader.read_u16().await?;
            let handle = reader.read_u64().await?;
            let offset = reader.read_u64().await?;
            let len = reader.read_u32().await?;
            Ok::<_, std::io::Error>((flags, command, handle, offset, len))
        }
        .await;
        let (flags, command, handle, offset, len) = match header {
            Ok(h) => h,
            Err(err) => {
                error = Some(err.into());
                break;
            }
        };
        if command == 2 {
            break;
        }
        if len as usize > MAX_IO && command != 4 && command != 6 {
            error = Some(anyhow::anyhow!("request exceeds advertised maximum"));
            break;
        }
        let permit = limit.clone().acquire_owned().await?;
        let allocation = if command == 0 || command == 1 {
            len.max(4096)
        } else {
            4096
        };
        let mem = memory
            .clone()
            .acquire_many_owned(allocation.div_ceil(4096))
            .await?;
        let mut payload = if command == 1 {
            vec![0; len as usize]
        } else {
            Vec::new()
        };
        if command == 1
            && let Err(err) = reader.read_exact(&mut payload).await
        {
            error = Some(err.into());
            break;
        }
        let engine = e.clone();
        let tx = tx.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let valid_flags = match command {
                1 | 6 => flags & !1 == 0,
                0 | 3 | 4 => flags == 0,
                _ => false,
            };
            let result: Result<Vec<u8>> = async {
                ensure!(valid_flags, "unsupported command flags");
                ensure!(
                    command == 3 || (offset % 512 == 0 && len % 512 == 0),
                    "unaligned NBD request"
                );
                match command {
                    0 => engine.read(offset, len as usize).await,
                    1 => {
                        engine.write(offset, &payload).await?;
                        if flags & 1 != 0 {
                            engine.flush().await?;
                        }
                        Ok(Vec::new())
                    }
                    3 => {
                        ensure!(offset == 0 && len == 0, "invalid FLUSH");
                        engine.flush().await?;
                        Ok(Vec::new())
                    }
                    4 | 6 => {
                        engine.zero(offset, len as u64).await?;
                        if flags & 1 != 0 {
                            engine.flush().await?;
                        }
                        Ok(Vec::new())
                    }
                    _ => bail!("unsupported NBD command"),
                }
            }
            .await;
            let r = match result {
                Ok(data) => Response {
                    handle,
                    errno: 0,
                    data,
                    _memory: mem,
                },
                Err(err) => {
                    tracing::error!(command,offset,len,error=%err,"NBD request failed");
                    Response {
                        handle,
                        errno: if !valid_flags {
                            libc::EOPNOTSUPP as u32
                        } else {
                            libc::EIO as u32
                        },
                        data: Vec::new(),
                        _memory: mem,
                    }
                }
            };
            let _ = tx.send(r).await;
        });
        while let Some(result) = tasks.try_join_next() {
            if let Err(err) = result {
                tracing::error!(error=%err,"request task failed");
            }
        }
    }
    // Once accepted, writes must finish even if a client disconnects.
    while tasks.join_next().await.is_some() {}
    drop(tx);
    let _ = replies.await;
    if let Some(err) = error {
        return Err(err);
    }
    Ok(())
}
pub async fn serve(e: Arc<Engine>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let listener = TcpListener::bind(e.config.listen).await?;
    let limit = Arc::new(Semaphore::new(e.config.max_inflight));
    let memory = Arc::new(Semaphore::new(128 * 1024 * 1024 / 4096));
    let mut connections = JoinSet::new();
    tracing::info!(listen=%e.config.listen,size=e.identity.size,"NBD listening");
    loop {
        tokio::select! {
            _=shutdown.changed()=>break,
            r=listener.accept()=> { let (s,peer)=r?; let (engine,l,m,rx)=(e.clone(),limit.clone(),memory.clone(),shutdown.clone());
                connections.spawn(async move {if let Err(err)=connection(s,engine,l,m,rx).await {tracing::warn!(%peer,error=%err,"NBD connection ended");}});
            }
            _=connections.join_next(),if !connections.is_empty()=>{}
        }
    }
    drop(listener);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .map_err(|_| anyhow::anyhow!("client connections did not drain in 30 seconds; WAL retained"))?;
    Ok(())
}
