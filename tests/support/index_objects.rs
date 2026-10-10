use super::*;

async fn fixture() -> Result<(tempfile::TempDir, Config)> {
    let directory = tempfile::tempdir()?;
    let config = Config {
        local_dir: directory.path().join("local"),
        store: format!("file://{}", directory.path().join("remote").display()),
        memory_cache_mib: 0,
        disk_cache_mib: 0,
        hot_wal_mib: 0,
        max_index_mib: 1,
        remote_index_cache_mib: 1,
        segment_mib: 1,
        max_pending_mib: 128,
        ..Config::recommended()
    };
    Engine::init(&config, 64 * 1024 * 1024).await?;
    let engine = Engine::open(config.clone()).await?;
    engine.write(0, &[1; PAGE]).await?;
    engine.write(SHARD_PAGES * PAGE as u64, &[2; PAGE]).await?;
    engine.checkpoint().await?;
    assert_eq!(engine.status().await.index_object_cache.entries, 2);
    drop(engine);
    Ok((directory, config))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_objects_warm_restart_keeps_head_authority_and_replays_later_wal() -> Result<()> {
    let (_directory, config) = fixture().await?;
    let engine = Engine::open(config.clone()).await?;
    let status = engine.status().await.index_object_cache;
    assert_eq!((status.hits, status.remote_gets), (2, 0));
    engine.write(0, &[3; PAGE]).await?;
    engine.flush().await?;
    drop(engine); // durable WAL remains newer than cached remote index
    let recovered = Engine::open(config.clone()).await?;
    assert_eq!(recovered.read(0, PAGE).await?, vec![3; PAGE]);
    assert_eq!(recovered.status().await.index_object_cache.hits, 2);
    drop(recovered);
    let mut other = config.clone();
    other.local_dir = config.local_dir.with_file_name("other-writer");
    Engine::adopt(&other, true).await?;
    assert!(
        Engine::open(config).await.is_err(),
        "cached indexes cannot bypass writer fencing"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_objects_new_head_ignores_old_object_and_fetches_only_changed_shard() -> Result<()> {
    let (_directory, config) = fixture().await?;
    let cache = config.local_dir.join("remote-index-cache");
    let saved = std::fs::read_dir(&cache)?
        .map(|e| {
            let path = e?.path();
            Ok((path.clone(), std::fs::read(path)?))
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let engine = Engine::open(config.clone()).await?;
    engine.write(0, &[4; PAGE]).await?;
    engine.checkpoint().await?;
    drop(engine);
    // Simulate losing cache fills from the latest checkpoint, without rolling
    // back either HEAD or the WAL. Only older immutable copies remain.
    std::fs::remove_dir_all(&cache)?;
    std::fs::create_dir(&cache)?;
    for (path, bytes) in saved {
        std::fs::write(path, bytes)?;
    }
    let engine = Engine::open(config).await?;
    let status = engine.status().await.index_object_cache;
    assert_eq!((status.hits, status.remote_gets), (1, 1));
    assert_eq!(engine.read(0, PAGE).await?, vec![4; PAGE]);
    assert_eq!(
        engine.read(SHARD_PAGES * PAGE as u64, PAGE).await?,
        vec![2; PAGE]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_objects_corrupt_local_copy_falls_back_but_authority_corruption_fails() -> Result<()>
{
    let (directory, config) = fixture().await?;
    let files = std::fs::read_dir(config.local_dir.join("remote-index-cache"))?
        .map(|x| x.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut damaged = std::fs::read(&files[0])?;
    damaged[0] ^= 1;
    std::fs::write(&files[0], damaged)?;
    let engine = Engine::open(config.clone()).await?;
    let status = engine.status().await.index_object_cache;
    assert_eq!(
        (status.hits, status.corruptions, status.remote_gets),
        (1, 1, 1)
    );
    let head = Engine::inspect(&config).await?;
    drop(engine);
    for shard in head.shards.values() {
        std::fs::write(
            directory.path().join("remote").join(&shard.key),
            b"bad authoritative index",
        )?;
    }
    // A valid local copy still matches HEAD. Offline validation must actually
    // check S3 and cannot mask remote damage using this disposable cache.
    assert!(
        Engine::load_index(&Store::new(&config)?, &head, &config, None)
            .await
            .is_err()
    );
    for path in files {
        std::fs::write(path, b"bad local copy too")?;
    }
    assert!(Engine::open(config).await.is_err());
    Ok(())
}
