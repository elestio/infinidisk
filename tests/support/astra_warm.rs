//! Private offline-warm tests: file-backed objects and real SSD cache files.
use super::*;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::time::Duration;

fn config(t: &tempfile::TempDir, extent: u64) -> Config {
    Config {
        local_dir: t.path().join("volume"),
        store: format!("file://{}", t.path().join("objects").display()),
        memory_cache_mib: 0,
        disk_cache_mib: 8,
        hot_wal_mib: 0,
        segment_mib: 1,
        logical_cache: true,
        fast_local_reads: true,
        read_extent_kib: extent,
        ..Config::default()
    }
}

async fn published(c: &Config, pages: u64) -> Result<Arc<Engine>> {
    Engine::init(c, 8 * 1024 * 1024).await?;
    let engine = Engine::open(c.clone()).await?;
    for physical in 0..pages {
        let logical = physical * 73 % pages;
        engine
            .write(logical * PAGE as u64, &[value(logical); PAGE])
            .await?;
    }
    engine.checkpoint().await?;
    drop(engine);
    std::fs::remove_dir_all(c.local_dir.join("logical-cache"))?;
    Engine::open(c.clone()).await
}

fn value(page: u64) -> u8 {
    (page % 251 + 1) as u8
}

fn throttle(engine: &mut Arc<Engine>, per_call: Duration, per_byte: Duration) {
    let owner = Arc::get_mut(engine).unwrap();
    // ThrottledStore only accepts streams; preserve the real file backend via
    // the SDK's stream adapter instead of its unimplemented File payload arm.
    let stream = object_store::chunked::ChunkedStore::new(owner.store.inner.clone(), 1024 * 1024);
    owner.store.inner = Arc::new(ThrottledStore::new(
        stream,
        ThrottleConfig {
            wait_get_per_call: per_call,
            wait_get_per_byte: per_byte,
            ..ThrottleConfig::default()
        },
    ));
}

async fn groups(engine: &Engine) -> Result<Vec<Vec<(u64, Ref)>>> {
    let mut groups = BTreeMap::new();
    for (page, reference) in engine.state.lock().await.index.entries()? {
        groups
            .entry((
                reference.segment,
                reference.offset / (engine.config.read_extent_kib * 1024),
            ))
            .or_insert_with(Vec::new)
            .push((page, reference));
    }
    Ok(groups.into_values().collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fragmented_ram_zero_downloads_each_missing_range_once_and_retains_every_page() -> Result<()>
{
    for extent in [64, 256] {
        let fixture = tempfile::tempdir()?;
        let c = config(&fixture, extent);
        let mut engine = published(&c, 1024).await?;
        let groups = groups(&engine).await?;
        let head = std::fs::read(fixture.path().join("objects/HEAD"))?;
        let cache = engine.page_cache.as_ref().unwrap();
        // One entire range is already present; another is only partly cached.
        for (page, reference) in &groups[0] {
            cache.put(*page, reference, &[value(*page); PAGE])?;
        }
        let (page, reference) = &groups[1][0];
        cache.put(*page, reference, &[value(*page); PAGE])?;
        throttle(&mut engine, Duration::from_millis(15), Duration::ZERO);
        let warmed = engine.warm_cache(8).await?;
        assert_eq!(warmed.pages, 1024);
        assert_eq!(
            warmed.range_peak, 8,
            "concurrency must apply to distinct GETs"
        );
        let before_second = engine.status().await;
        assert_eq!(before_second.remote_gets, groups.len() as u64 - 1);
        let expected_bytes: u64 = groups
            .iter()
            .skip(1)
            .map(|group| {
                let r = &group[0].1;
                let start = r.offset / (extent * 1024) * (extent * 1024);
                (start + extent * 1024 + PAGE as u64).min(r.segment_len) - start
            })
            .sum();
        assert_eq!(before_second.remote_bytes, expected_bytes);
        assert!(
            engine.cache.lock().unwrap().is_empty(),
            "warm must not use the tiny extent LRU"
        );
        std::fs::rename(
            fixture.path().join("objects/segments"),
            fixture.path().join("objects/offline-segments"),
        )?;
        let retained = engine.warm_cache(128).await?;
        assert_eq!(retained.pages, 1024);
        assert_eq!(retained.range_peak, 0);
        assert_eq!(engine.status().await.remote_gets, before_second.remote_gets);
        for page in (0..1024).rev() {
            assert_eq!(
                engine.read(page * PAGE as u64, PAGE).await?,
                [value(page); PAGE]
            );
        }
        assert_eq!(std::fs::read(fixture.path().join("objects/HEAD"))?, head);
    }
    Ok(())
}

#[tokio::test]
async fn range_concurrency_is_bounded_and_invalid_limits_fail_before_opening() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let c = config(&fixture, 64);
    for invalid in [0, 129, usize::MAX] {
        assert!(
            Engine::warm_with_concurrency(c.clone(), invalid)
                .await
                .is_err()
        );
        assert!(!c.local_dir.exists());
    }
    for limit in [1, 4, 128] {
        let fixture = tempfile::tempdir()?;
        let c = config(&fixture, 64);
        let mut engine = published(&c, 128).await?;
        let group_count = groups(&engine).await?.len();
        throttle(&mut engine, Duration::from_millis(30), Duration::ZERO);
        let result = engine.warm_cache(limit).await?;
        assert_eq!(result.range_peak as usize, limit.min(group_count));
        assert_eq!(engine.status().await.remote_gets as usize, group_count);
    }
    Ok(())
}

#[tokio::test]
async fn corrupt_range_never_partially_fills_valid_pages_from_that_group() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let c = config(&fixture, 64);
    let engine = published(&c, 32).await?;
    let mut pages = groups(&engine).await?.remove(0);
    pages.sort_unstable_by_key(|(_, reference)| reference.offset);
    let last = &pages.last().unwrap().1;
    let file = OpenOptions::new().write(true).open(
        fixture
            .path()
            .join("objects/segments")
            .join(last.segment.to_string()),
    )?;
    file.write_all_at(&[0xff], last.offset)?;
    let result = engine
        .warm_group(
            WarmGroup {
                pages: pages.clone(),
                local: None,
            },
            &WarmRanges::default(),
        )
        .await;
    assert!(result.unwrap_err().to_string().contains("checksum"));
    assert_eq!(engine.status().await.remote_gets, 1);
    for (page, reference) in pages {
        assert!(
            engine
                .page_cache
                .as_ref()
                .unwrap()
                .get(page, &reference)
                .is_none()
        );
    }
    drop(engine);
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(c.local_dir.join("LOCK"))?;
    lock.try_lock_exclusive()?;
    Ok(())
}

#[tokio::test]
async fn unpublished_missing_or_corrupt_local_pages_fail_closed_without_remote_reads() -> Result<()>
{
    for missing in [false, true] {
        let fixture = tempfile::tempdir()?;
        let c = config(&fixture, 64);
        Engine::init(&c, 8 * 1024 * 1024).await?;
        let mut engine = Engine::open(c.clone()).await?;
        engine.write(0, &[0x6a; PAGE]).await?;
        let (reference, file) = {
            let mut s = engine.state.lock().await;
            let r = s.reference(0)?.unwrap();
            assert_eq!(r.segment_len, 0);
            let f = if missing {
                s.local.remove(&r.segment).unwrap()
            } else {
                s.local[&r.segment].clone()
            };
            (r, f)
        };
        if !missing {
            file.write_all_at(&[0xff], reference.offset)?;
        }
        let cold = crate::page_cache::PageCache::open_partitioned(
            &fixture.path().join("cold-cache"),
            engine.identity.volume,
            c.disk_cache_mib * 1024 * 1024,
            16,
        )?;
        Arc::get_mut(&mut engine).unwrap().page_cache = Some(Arc::new(cold));
        let error = engine
            .warm_cache(8)
            .await
            .err()
            .context("corruption accepted")?;
        assert!(
            error
                .to_string()
                .contains(if missing { "unpublished" } else { "checksum" })
        );
        assert!(engine.status().await.poisoned);
        assert_eq!(engine.status().await.remote_gets, 0);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_cache_fill_drains_other_started_ranges_before_releasing_volume_lock() -> Result<()>
{
    use std::os::fd::{AsRawFd, FromRawFd};
    let fixture = tempfile::tempdir()?;
    let c = config(&fixture, 64);
    let mut engine = published(&c, 17).await?;
    assert_eq!(groups(&engine).await?.len(), 2);
    let dir = fixture.path().join("sealed-cache");
    std::fs::create_dir(&dir)?;
    // A sealed memfd gives a deterministic real write error without filling
    // the host disk, changing process limits, or modifying production cache code.
    let fd = unsafe {
        libc::memfd_create(
            c"warm-fill-failure".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    ensure!(fd >= 0, "memfd_create: {}", std::io::Error::last_os_error());
    let data = unsafe { File::from_raw_fd(fd) };
    std::os::unix::fs::symlink(
        format!("/proc/self/fd/{}", data.as_raw_fd()),
        dir.join("pages"),
    )?;
    let cache = crate::page_cache::PageCache::open_partitioned(
        &dir,
        engine.identity.volume,
        c.disk_cache_mib * 1024 * 1024,
        16,
    )?;
    ensure!(
        unsafe { libc::fcntl(data.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_WRITE) } == 0,
        "seal cache: {}",
        std::io::Error::last_os_error()
    );
    Arc::get_mut(&mut engine).unwrap().page_cache = Some(Arc::new(cache));
    // The short tail fails its put well before the first full range returns.
    throttle(&mut engine, Duration::ZERO, Duration::from_micros(8));
    let started = Instant::now();
    let task = tokio::spawn(async move { engine.warm_cache(2).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(c.local_dir.join("LOCK"))?;
    assert!(
        lock.try_lock_exclusive().is_err(),
        "LOCK released while a range still runs"
    );
    let error = tokio::time::timeout(Duration::from_secs(3), task)
        .await??
        .err()
        .context("cache write failure ignored")?;
    assert!(error.to_string().contains("warm fill failed"));
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "started range was cancelled instead of drained"
    );
    lock.try_lock_exclusive()?;
    Ok(())
}
