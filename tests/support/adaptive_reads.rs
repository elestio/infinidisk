use super::*;

async fn remote_fixture(memory: usize, disk: u64) -> Result<(tempfile::TempDir, Config, Vec<u8>)> {
    let directory = tempfile::tempdir()?;
    let c = Config {
        local_dir: directory.path().join("writer"),
        store: format!("file://{}", directory.path().join("objects").display()),
        memory_cache_mib: memory,
        disk_cache_mib: disk,
        hot_wal_mib: 0,
        max_index_mib: 1,
        max_pending_mib: 128,
        segment_mib: 1,
        checkpoint_pipeline: false,
        async_cache: false,
        ..Config::recommended()
    };
    let mut bytes = vec![0; 512 * 1024];
    for (i, page) in bytes.chunks_mut(PAGE).enumerate() {
        page.fill((i % 251 + 1) as u8);
    }
    Engine::init(&c, 4 * 1024 * 1024).await?;
    let e = Engine::open(c.clone()).await?;
    e.write(0, &bytes).await?;
    e.checkpoint().await?;
    drop(e);
    let mut fresh = c;
    fresh.local_dir = directory.path().join("reader");
    Engine::adopt(&fresh, true).await?;
    Ok((directory, fresh, bytes))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adaptive_dense_read_owns_ranges_with_zero_ram_and_retains_verified_pages() -> Result<()> {
    let (_directory, c, expected) = remote_fixture(0, 8).await?;
    let e = Engine::open(c).await?;
    assert_eq!(
        e.read(3, expected.len() - 10).await?,
        expected[3..expected.len() - 7]
    );
    let status = e.status().await;
    assert_eq!(status.range_cache_bytes, 0);
    assert!(status.adaptive_large_gets >= 2);
    assert!(
        status.remote_gets <= 4,
        "each physical group should download once"
    );
    assert_eq!(
        e.read(3, expected.len() - 10).await?,
        expected[3..expected.len() - 7]
    );
    assert_eq!(e.status().await.remote_gets, status.remote_gets);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adaptive_random_uses_small_ranges_and_reuses_verified_large_ranges() -> Result<()> {
    let (_directory, c, expected) = remote_fixture(1, 0).await?;
    let e = Engine::open(c).await?;
    assert_eq!(e.read(0, PAGE).await?, expected[..PAGE]);
    let first = e.status().await;
    assert_eq!(first.adaptive_small_gets, 1);
    assert!(first.remote_bytes <= SMALL_READ + PAGE as u64);
    assert_eq!(e.read(0, 128 * 1024).await?, expected[..128 * 1024]);
    let before = e.status().await;
    assert_eq!(before.adaptive_large_gets, 1);
    assert_eq!(
        e.read(27 * PAGE as u64, PAGE).await?,
        expected[27 * PAGE..28 * PAGE]
    );
    assert_eq!(e.status().await.remote_gets, before.remote_gets);
    // A corrupt disposable range must be replaced from the remote authority.
    let r = e.state.lock().await.reference(27)?.unwrap();
    let start = r.offset / LARGE_READ * LARGE_READ;
    {
        let mut cache = e.cache.lock().unwrap();
        let mut bad = cache.get(&(r.segment, start)).unwrap().to_vec();
        bad[(r.offset - start) as usize] ^= 1;
        cache.put((r.segment, start), bad.into());
    }
    assert_eq!(
        e.read(27 * PAGE as u64, PAGE).await?,
        expected[27 * PAGE..28 * PAGE]
    );
    assert_eq!(e.status().await.remote_gets, before.remote_gets + 1);
    assert!(e.status().await.range_cache_bytes <= 1024 * 1024);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adaptive_group_checks_all_pages_before_installing_any_payload() -> Result<()> {
    let (directory, c, _) = remote_fixture(1, 8).await?;
    let e = Engine::open(c).await?;
    let r = e.state.lock().await.reference(27)?.unwrap();
    let path = directory
        .path()
        .join("objects/segments")
        .join(r.segment.to_string());
    OpenOptions::new()
        .write(true)
        .open(path)?
        .write_all_at(&[0], r.offset)?;
    assert!(e.read(0, 128 * 1024).await.is_err());
    let first = e.state.lock().await.reference(0)?.unwrap();
    assert!(e.page_cache.as_ref().unwrap().get(0, &first).is_none());
    assert_eq!(e.status().await.range_cache_bytes, 0);
    Ok(())
}

#[test]
fn adaptive_grouping_keeps_fragmented_reads_small() -> Result<()> {
    let segment = Uuid::new_v4();
    let refs = (0..32)
        .map(|i| {
            (
                i,
                Some(Ref {
                    segment,
                    offset: i * LARGE_READ,
                    segment_len: 33 * LARGE_READ,
                    crc: 0,
                }),
                None,
            )
        })
        .collect();
    let groups = Engine::read_groups(refs)?;
    assert_eq!(groups.len(), 32);
    assert!(groups.iter().all(|group| group.extent == SMALL_READ));
    Ok(())
}
