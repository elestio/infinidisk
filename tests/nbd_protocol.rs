use anyhow::Result;
use infinidisk2::{config::Config, engine::Engine, nbd};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
};
async fn connect(c: &Config) -> Result<TcpStream> {
    let mut socket = None;
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(c.listen).await {
            socket = Some(s);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let mut s = socket.ok_or_else(|| anyhow::anyhow!("server did not listen"))?;
    assert_eq!(s.read_u64().await?, 0x4e42444d41474943);
    assert_eq!(s.read_u64().await?, 0x49484156454f5054);
    assert_eq!(s.read_u16().await?, 3);
    s.write_u32(3).await?;
    s.write_u64(0x49484156454f5054).await?;
    s.write_u32(1).await?;
    s.write_u32(0).await?;
    assert_eq!(s.read_u64().await?, c_size());
    assert_eq!(s.read_u16().await? & 256, 256);
    Ok(s)
}
fn c_size() -> u64 {
    1024 * 1024 * 1024
}
async fn request(
    s: &mut TcpStream,
    cmd: u16,
    flags: u16,
    handle: u64,
    offset: u64,
    len: u32,
    payload: &[u8],
) -> Result<(u32, Vec<u8>)> {
    s.write_u32(0x25609513).await?;
    s.write_u16(flags).await?;
    s.write_u16(cmd).await?;
    s.write_u64(handle).await?;
    s.write_u64(offset).await?;
    s.write_u32(len).await?;
    s.write_all(payload).await?;
    assert_eq!(s.read_u32().await?, 0x67446698);
    let errno = s.read_u32().await?;
    assert_eq!(s.read_u64().await?, handle);
    let mut data = vec![
        0;
        if cmd == 0 && errno == 0 {
            len as usize
        } else {
            0
        }
    ];
    s.read_exact(&mut data).await?;
    Ok((errno, data))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_connections_share_flush_fua_and_large_trim() -> Result<()> {
    let t = tempfile::tempdir()?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let listen = listener.local_addr()?;
    drop(listener);
    let c = Config {
        local_dir: t.path().join("local"),
        store: format!("file://{}", t.path().join("remote").display()),
        listen,
        ..Config::default()
    };
    Engine::init(&c, c_size()).await?;
    let e = Engine::open(c.clone()).await?;
    let (tx, rx) = watch::channel(false);
    let engine = e.clone();
    let task = tokio::spawn(async move { nbd::serve(engine, rx).await });
    let mut a = connect(&c).await?;
    let mut b = connect(&c).await?;
    assert_eq!(
        request(&mut a, 1, 0, 1, 0, 4096, &vec![9; 4096]).await?.0,
        0
    );
    assert_eq!(
        request(&mut b, 1, 0, 2, 4096, 4096, &vec![8; 4096])
            .await?
            .0,
        0
    );
    assert_eq!(request(&mut a, 3, 0, 3, 0, 0, &[]).await?.0, 0);
    assert_eq!(e.status().await.local_durable_sequence, 2);
    assert_eq!(
        request(&mut b, 1, 1, 4, 8192, 4096, &vec![7; 4096])
            .await?
            .0,
        0
    );
    assert_eq!(e.status().await.local_durable_sequence, 3);
    assert_eq!(
        request(&mut a, 0, 0, 5, 8192, 4096, &[]).await?.1,
        vec![7; 4096]
    );
    // Large TRIM records are constant-size, not gigabytes of zero WAL payload.
    assert_eq!(
        request(&mut b, 4, 0, 6, 0, 512 * 1024 * 1024, &[]).await?.0,
        0
    );
    assert_eq!(request(&mut a, 3, 0, 7, 0, 0, &[]).await?.0, 0);
    assert!(e.status().await.pending_bytes < 20000);
    assert_eq!(
        request(&mut a, 0, 0, 8, 0, 12288, &[]).await?.1,
        vec![0; 12288]
    );
    drop(a);
    drop(b);
    tx.send(true)?;
    task.await??;
    drop(e);
    let e = Engine::open(c).await?;
    assert_eq!(e.read(0, 12288).await?, vec![0; 12288]);
    Ok(())
}
