use anyhow::{Context, Result};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orderly_shutdown_publishes_a_generation_after_its_lag_limit() -> Result<()> {
    use std::{process::Stdio, time::Duration};

    let t = tempfile::tempdir()?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let listen = listener.local_addr()?;
    drop(listener);
    let remote = t.path().join("remote");
    let c = Config {
        local_dir: t.path().join("local"),
        store: format!("file://{}", remote.display()),
        listen,
        generation_mode: true,
        checkpoint_seconds: 2,
        generation_max_lag_seconds: 2,
        ..Config::default()
    };
    Engine::init(&c, c_size()).await?;
    let config = t.path().join("config.toml");
    std::fs::write(&config, toml::to_string(&c)?)?;
    // Block object publication without changing HEAD or relying on permissions
    // (the tests also run as root). Opening this prefix as a directory fails.
    let obstruction = remote.join("segments");
    std::fs::write(&obstruction, "temporarily unavailable object prefix")?;
    let log_path = t.path().join("server.log");
    let log = std::fs::File::create(&log_path)?;
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_infinidisk"))
        .arg("-c")
        .arg(&config)
        .arg("serve")
        .env("RUST_LOG", "infinidisk=info,infinidisk2=info")
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true)
        .spawn()?;
    let mut socket = connect(&c).await?;
    let payload = [0x63; 4096];
    assert_eq!(request(&mut socket, 1, 0, 1, 0, 4096, &payload).await?.0, 0);
    drop(socket);
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert_eq!(Engine::inspect(&c).await?.seq, 0);
    assert!(std::fs::read_to_string(&log_path)?.contains("S3 checkpoint failed"));
    std::fs::remove_file(&obstruction)?;
    // Shutdown stops the periodic publisher. The final publication must not
    // first wait on the lag admission barrier, which only publication can clear.
    let pid = child.id().context("server exited before SIGTERM")? as libc::pid_t;
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let exit = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .context("shutdown waited on generation backpressure instead of publishing")??;
    assert!(exit.success(), "{}", std::fs::read_to_string(&log_path)?);
    assert_eq!(Engine::inspect(&c).await?.seq, 1);
    let reopened = Engine::open(c).await?;
    assert_eq!(reopened.read(0, payload.len()).await?, payload);
    Ok(())
}
