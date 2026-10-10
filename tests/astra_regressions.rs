use anyhow::{Context, Result};
use infinidisk2::{config::Config, engine::Engine, wal::PAGE};
use std::time::Duration;

#[tokio::test]
async fn empty_trim_checkpoint_clears_generation_lag_before_next_write() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let config = Config {
        local_dir: fixture.path().join("volume"),
        store: format!("file://{}", fixture.path().join("objects").display()),
        generation_mode: true,
        generation_max_lag_seconds: 1,
        checkpoint_seconds: 1,
        memory_cache_mib: 0,
        disk_cache_mib: 0,
        hot_wal_mib: 0,
        ..Config::default()
    };
    Engine::init(&config, 16 * 1024 * 1024).await?;
    let engine = Engine::open(config).await?;

    // A filesystem may discard an already-empty range before its first write.
    // This creates a WAL sequence without making an index shard dirty.
    engine.zero(0, PAGE as u64).await?;
    let trim_sequence = engine.status().await.sequence;
    assert_eq!(trim_sequence, 1);
    engine.checkpoint().await?;
    let published = engine.status().await;
    assert_eq!(published.allocated_pages, 0);
    assert_eq!(published.remote_sequence, trim_sequence);
    assert_eq!(published.unpublished_age_ms, 0);

    // Waiting longer than the lag limit must not strand the next real write.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let payload = [0x5a; PAGE];
    tokio::time::timeout(Duration::from_secs(1), engine.write(0, &payload))
        .await
        .context("published empty TRIM left generation backpressure blocked")??;
    engine.checkpoint().await?;
    let published = engine.status().await;
    assert_eq!(published.remote_sequence, published.sequence);
    assert_eq!(engine.read(0, PAGE).await?, payload);
    Ok(())
}

#[tokio::test]
async fn trimmed_sparse_regions_leave_no_empty_shards_in_published_head() -> Result<()> {
    const REGIONS: u64 = 12;
    const REGION_BYTES: u64 = 4096 * PAGE as u64;
    let fixture = tempfile::tempdir()?;
    let config = Config {
        local_dir: fixture.path().join("volume"),
        store: format!("file://{}", fixture.path().join("objects").display()),
        memory_cache_mib: 0,
        disk_cache_mib: 0,
        hot_wal_mib: 0,
        ..Config::default()
    };
    Engine::init(&config, REGIONS * REGION_BYTES).await?;
    let engine = Engine::open(config.clone()).await?;
    for region in 0..REGIONS {
        let offset = region * REGION_BYTES;
        engine.write(offset, &[region as u8 + 1; PAGE]).await?;
        engine.checkpoint().await?;
        let written = Engine::inspect(&config).await?;
        assert_eq!(written.shards.len(), 1);
        assert!(written.shards.contains_key(&region));

        engine.zero(offset, PAGE as u64).await?;
        engine.checkpoint().await?;
        let trimmed = Engine::inspect(&config).await?;
        assert!(
            trimmed.shards.is_empty(),
            "empty region {region} remained in HEAD"
        );
        assert_eq!(trimmed.seq, 2 * (region + 1));
        let status = engine.status().await;
        assert_eq!(status.allocated_pages, 0);
        assert_eq!(status.remote_sequence, status.sequence);
    }
    drop(engine);

    let reopened = Engine::open(config).await?;
    assert_eq!(reopened.status().await.remote_sequence, 2 * REGIONS);
    for region in 0..REGIONS {
        assert_eq!(reopened.read(region * REGION_BYTES, PAGE).await?, [0; PAGE]);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_warm_retains_fragmented_pages_with_online_async_fills_enabled() -> Result<()> {
    const PAGES: u64 = 1024;
    let fixture = tempfile::tempdir()?;
    let remote = fixture.path().join("objects");
    let config = Config {
        local_dir: fixture.path().join("volume"),
        store: format!("file://{}", remote.display()),
        memory_cache_mib: 1,
        disk_cache_mib: 8,
        hot_wal_mib: 0,
        segment_mib: 1,
        logical_cache: true,
        async_cache: true,
        cache_queue_mib: 1,
        fast_local_reads: true,
        ..Config::default()
    };
    Engine::init(&config, 8 * 1024 * 1024).await?;
    let engine = Engine::open(config.clone()).await?;
    for physical in 0..PAGES {
        let logical = physical * 73 % PAGES;
        let value = (logical % 251 + 1) as u8;
        engine.write(logical * PAGE as u64, &[value; PAGE]).await?;
    }
    engine.checkpoint().await?;
    drop(engine);
    // Eliminate all prior logical cache fills; hot WAL and extent SSD are off.
    std::fs::remove_dir_all(config.local_dir.join("logical-cache"))?;
    assert_eq!(Engine::warm(config.clone()).await?, PAGES as usize);
    std::fs::rename(remote.join("segments"), remote.join("segments-unavailable"))?;
    let engine = Engine::open(config).await?;
    for logical in (0..PAGES).rev() {
        let value = (logical % 251 + 1) as u8;
        assert_eq!(
            engine.read(logical * PAGE as u64, PAGE).await?,
            [value; PAGE]
        );
    }
    assert_eq!(engine.status().await.logical_cache_hits, PAGES);
    Ok(())
}

#[tokio::test]
async fn offline_warm_refuses_a_working_set_that_overflows_one_cache_partition() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let config = Config {
        local_dir: fixture.path().join("volume"),
        store: format!("file://{}", fixture.path().join("objects").display()),
        disk_cache_mib: 1,
        hot_wal_mib: 0,
        logical_cache: true,
        fast_local_reads: true,
        ..Config::default()
    };
    Engine::init(&config, 8 * 1024 * 1024).await?;
    let engine = Engine::open(config.clone()).await?;
    // 17 pages fit the total 1 MiB budget, but not a single sixteenth of it.
    for page in 0..17 {
        engine.write(page * 16 * PAGE as u64, &[0x37; PAGE]).await?;
    }
    engine.checkpoint().await?;
    drop(engine);
    assert!(
        Engine::warm(config)
            .await
            .unwrap_err()
            .to_string()
            .contains("one logical cache partition")
    );
    Ok(())
}
