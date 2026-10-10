//! End-to-end engine checks for the experimental paths. These use the real
//! local object-store backend, WAL files, disposable caches and paged index.
//! Process restart here is close/reopen; external SIGKILL/power-loss campaigns
//! remain separate from these deterministic consistency checks.
use anyhow::{Context, Result, ensure};
use infinidisk2::{config::Config, engine::Engine, index::SHARD_PAGES, wal::PAGE};
use std::{fs::OpenOptions, os::unix::fs::FileExt, sync::Arc, time::Duration};
use tokio::{sync::Barrier, task::JoinSet};

const MIB: u64 = 1024 * 1024;
const SHARD_BYTES: u64 = SHARD_PAGES * PAGE as u64;

fn experimental_config(directory: &tempfile::TempDir) -> Config {
    Config {
        local_dir: directory.path().join("local"),
        store: format!("file://{}", directory.path().join("remote").display()),
        memory_cache_mib: 0,
        disk_cache_mib: 1,
        hot_wal_mib: 0,
        max_index_mib: 1,
        segment_mib: 1,
        max_pending_mib: 128,
        logical_cache: true,
        async_cache: true,
        cache_queue_mib: 1,
        fast_local_reads: true,
        selective_sync: true,
        paged_index: true,
        wal_commit_records: true,
        ..Config::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_trim_with_low_resident_index_replays_wal_and_publishes_exact_shards() -> Result<()> {
    const SHARDS: u64 = 16;
    const PAGES_PER_SHARD: usize = 513;
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.logical_cache = false;
    config.async_cache = false;
    config.disk_cache_mib = 0;
    Engine::init(&config, SHARDS * SHARD_BYTES).await?;
    let engine = Engine::open(config.clone()).await?;
    for shard in 0..SHARDS {
        engine
            .write(
                shard * SHARD_BYTES,
                &vec![shard as u8 + 1; PAGES_PER_SHARD * PAGE],
            )
            .await?;
    }
    let allocated = engine.status().await;
    assert_eq!(allocated.allocated_pages, SHARDS as usize * PAGES_PER_SHARD);
    assert_eq!(allocated.index.resident_shards, 2);
    assert!(allocated.index.evictions > 0);
    assert!(allocated.index.resident_budget_bytes <= MIB as usize);
    engine.checkpoint().await?;
    let baseline = Engine::inspect(&config).await?;
    assert_eq!(baseline.shards.len(), SHARDS as usize);

    // One large TRIM, with partially retained first/last shards and fourteen
    // whole shards removed. All individual writes remain below MAX_IO.
    let first = 257 * PAGE as u64;
    let last = (SHARDS - 1) * SHARD_BYTES + 256 * PAGE as u64;
    engine.zero(first, last - first).await?;
    engine.flush().await?;
    let trimmed = engine.status().await;
    assert_eq!(trimmed.allocated_pages, 514);
    assert_eq!(trimmed.remote_sequence, baseline.seq);
    assert_eq!(trimmed.sequence, baseline.seq + 1);
    assert!(trimmed.index.resident_budget_bytes <= MIB as usize);
    drop(engine);

    // HEAD still describes all sixteen shards. Only WAL replay can recover the
    // acknowledged TRIM and reconstruct its exact dirty-shard set.
    let engine = Engine::open(config.clone()).await?;
    assert_eq!(engine.status().await.allocated_pages, 514);
    assert!(engine.status().await.index.resident_budget_bytes <= MIB as usize);
    async fn verify(engine: &Engine, first: u64, last: u64) -> Result<()> {
        for shard in 0..SHARDS {
            let offset = shard * SHARD_BYTES;
            let bytes = engine.read(offset, PAGES_PER_SHARD * PAGE).await?;
            for (page, payload) in bytes.as_chunks::<PAGE>().0.iter().enumerate() {
                let value = if (first..last).contains(&(offset + page as u64 * PAGE as u64)) {
                    0
                } else {
                    shard as u8 + 1
                };
                ensure!(
                    payload.iter().all(|&byte| byte == value),
                    "TRIM changed a wrong page"
                );
            }
        }
        Ok(())
    }
    verify(&engine, first, last).await?;
    engine.checkpoint().await?;
    let published = Engine::inspect(&config).await?;
    assert_eq!(published.seq, baseline.seq + 1);
    assert_eq!(
        published.shards.keys().copied().collect::<Vec<_>>(),
        vec![0, SHARDS - 1]
    );
    drop(engine);

    let mut adopted = config;
    adopted.local_dir = directory.path().join("adopted-trim");
    Engine::adopt(&adopted, true).await?;
    let engine = Engine::open(adopted).await?;
    assert_eq!(engine.status().await.allocated_pages, 514);
    verify(&engine, first, last).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_fill_saturation_is_disposable_and_local_fsync_still_recovers() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = experimental_config(&directory);
    Engine::init(&config, 32 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    // A single 2 MiB fill cannot fit a 1 MiB queue, independently of scheduling.
    // Skipping it must not skip the authoritative write or its durability.
    let mut expected = vec![0x53; 2 * MIB as usize];
    engine.write(0, &expected).await?;
    expected[PAGE - 7..PAGE + 13].copy_from_slice(&[0x29; 20]);
    engine.write((PAGE - 7) as u64, &[0x29; 20]).await?;
    let status = engine.status().await;
    assert!(status.cache_fills_skipped >= 1);
    assert!(status.cache_queue_bytes <= MIB as usize);
    assert_eq!(status.cache_fill_errors, 0);
    assert_eq!(status.durability_mode, "local-fsync");
    assert_eq!(engine.read(0, expected.len()).await?, expected);
    engine.flush().await?;
    let durable = engine.status().await.local_durable_sequence;
    assert!(durable >= 2);
    drop(engine);

    let engine = Engine::open(config.clone()).await?;
    assert_eq!(engine.status().await.local_durable_sequence, durable);
    assert_eq!(engine.read(0, expected.len()).await?, expected);
    engine.checkpoint().await?;
    assert_eq!(
        Engine::verify_remote(&config).await?.1,
        expected.len() / PAGE
    );
    assert_eq!(
        engine.read(3, expected.len() - 6).await?,
        expected[3..expected.len() - 3]
    );
    drop(engine);

    let mut fresh = config;
    fresh.local_dir = directory.path().join("adopted");
    Engine::adopt(&fresh, true).await?;
    let engine = Engine::open(fresh).await?;
    assert_eq!(engine.read(0, expected.len()).await?, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_versions_and_paged_checkpoints_do_not_mix_request_contents() -> Result<()> {
    concurrent_checkpoint_scenario(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_packed_checkpoints_and_prepared_aligned_wal_preserve_versions() -> Result<()> {
    concurrent_checkpoint_scenario(true).await
}

async fn concurrent_checkpoint_scenario(complete: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    if complete {
        config.compact_checkpoints = true;
        config.checkpoint_pipeline = true;
        config.wal_fixed_size = true;
        config.aligned_wal = true;
    }
    Engine::init(&config, 128 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    // Each request straddles two index shards. Three independent writers exceed
    // the resident two-shard index cap and force SSD spills during checkpoints.
    let starts: Vec<_> = (0..3)
        .map(|slot| (slot * 2 + 1) * SHARD_BYTES - PAGE as u64)
        .collect();
    for &offset in &starts {
        engine.write(offset, &vec![1; PAGE * 2]).await?;
    }
    let barrier = Arc::new(Barrier::new(8));
    let mut tasks = JoinSet::new();
    for (slot, &offset) in starts.iter().enumerate() {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            for iteration in 1..=24 {
                let value = 2 + slot as u8 * 64 + iteration;
                let data = vec![value; PAGE * 2];
                engine.write(offset, &data).await?;
                ensure!(
                    engine.read(offset, data.len()).await? == data,
                    "read after write returned another version"
                );
                if iteration % 4 == 0 {
                    engine.flush().await?;
                }
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    for (slot, &offset) in starts.iter().enumerate() {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            for _ in 0..100 {
                let data = engine.read(offset + 7, PAGE * 2 - 14).await?;
                let value = data[0];
                ensure!(
                    data.iter().all(|&b| b == value),
                    "one read mixed different write versions"
                );
                ensure!(
                    value == 1 || (3 + slot as u8 * 64..=26 + slot as u8 * 64).contains(&value),
                    "cache returned a page from another address"
                );
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    let checkpoint_engine = Arc::clone(&engine);
    let checkpoint_barrier = Arc::clone(&barrier);
    tasks.spawn(async move {
        checkpoint_barrier.wait().await;
        for _ in 0..16 {
            checkpoint_engine.checkpoint().await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    });
    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("concurrent engine test timed out")??;
    engine.flush().await?;
    engine.checkpoint().await?;
    assert!(!engine.status().await.poisoned);
    assert_eq!(engine.status().await.cache_fill_errors, 0);
    assert_eq!(Engine::verify_remote(&config).await?.1, 6);
    drop(engine);

    let engine = Engine::open(config.clone()).await?;
    for (slot, &offset) in starts.iter().enumerate() {
        assert_eq!(
            engine.read(offset, PAGE * 2).await?,
            vec![26 + slot as u8 * 64; PAGE * 2]
        );
    }
    drop(engine);
    let mut fresh = config;
    fresh.local_dir = directory.path().join("adopted");
    Engine::adopt(&fresh, true).await?;
    let engine = Engine::open(fresh).await?;
    for (slot, &offset) in starts.iter().enumerate() {
        assert_eq!(
            engine.read(offset, PAGE * 2).await?,
            vec![26 + slot as u8 * 64; PAGE * 2]
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_large_volume_recovers_many_shards_with_one_mib_index_budget() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = experimental_config(&directory);
    // Sparse writes exercise a large address space without allocating its data.
    let size = 2_u64 * 1024 * 1024 * MIB;
    Engine::init(&config, size).await?;
    let engine = Engine::open(config.clone()).await?;
    let offsets: Vec<_> = (0..48)
        .map(|i| i * 1024 * SHARD_BYTES + 17 * PAGE as u64)
        .collect();
    for (i, &offset) in offsets.iter().enumerate() {
        engine.write(offset, &vec![i as u8 + 1; PAGE]).await?;
    }
    engine.flush().await?;
    drop(engine);
    let engine = Engine::open(config.clone()).await?;
    for (i, &offset) in offsets.iter().enumerate().rev() {
        assert_eq!(engine.read(offset, PAGE).await?, vec![i as u8 + 1; PAGE]);
    }
    engine.checkpoint().await?;
    assert_eq!(Engine::inspect(&config).await?.shards.len(), offsets.len());
    assert_eq!(Engine::verify_remote(&config).await?.1, offsets.len());
    drop(engine);
    let mut fresh = config;
    fresh.local_dir = directory.path().join("adopted");
    Engine::adopt(&fresh, true).await?;
    let engine = Engine::open(fresh).await?;
    for (i, &offset) in offsets.iter().enumerate() {
        assert_eq!(engine.read(offset, PAGE).await?, vec![i as u8 + 1; PAGE]);
        assert_eq!(
            engine.read(offset + PAGE as u64, PAGE).await?,
            vec![0; PAGE]
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paged_index_allocates_beyond_the_legacy_one_mib_page_limit() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = experimental_config(&directory);
    Engine::init(&config, 64 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    let chunk = vec![0x39; 4 * MIB as usize];
    for i in 0..8 {
        engine.write(i * 4 * MIB, &chunk).await?;
    }
    engine.write(32 * MIB, &vec![0x47; PAGE]).await?;
    let allocated = 32 * MIB as usize / PAGE + 1;
    assert_eq!(engine.status().await.allocated_pages, allocated);
    assert!(allocated > config.max_index_mib * MIB as usize / 128);
    engine.flush().await?;
    drop(engine);

    let engine = Engine::open(config.clone()).await?;
    assert_eq!(engine.status().await.allocated_pages, allocated);
    for offset in [
        0,
        SHARD_BYTES - PAGE as u64,
        SHARD_BYTES,
        32 * MIB - PAGE as u64,
    ] {
        assert_eq!(engine.read(offset, PAGE).await?, vec![0x39; PAGE]);
    }
    assert_eq!(engine.read(32 * MIB, PAGE).await?, vec![0x47; PAGE]);
    engine.checkpoint().await?;
    assert_eq!(Engine::verify_remote(&config).await?.1, allocated);
    drop(engine);

    let mut fresh = config;
    fresh.local_dir = directory.path().join("adopted");
    Engine::adopt(&fresh, true).await?;
    let engine = Engine::open(fresh).await?;
    assert_eq!(engine.status().await.allocated_pages, allocated);
    assert_eq!(engine.read(0, PAGE).await?, vec![0x39; PAGE]);
    assert_eq!(engine.read(32 * MIB, PAGE).await?, vec![0x47; PAGE]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_corruption_poisons_runtime_but_is_discarded_on_wal_reconstruction() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = experimental_config(&directory);
    Engine::init(&config, 128 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    for id in 0..6 {
        engine
            .write(id * SHARD_BYTES, &vec![id as u8 + 1; PAGE])
            .await?;
    }
    engine.flush().await?;
    let session = std::fs::read_dir(config.local_dir.join("index-scratch"))?
        .next()
        .context("no scratch session")??
        .path();
    let shard = session.join("0000000000000000.idx");
    let mut bytes = std::fs::read(&shard)?;
    *bytes.last_mut().context("empty scratch shard")? ^= 1;
    std::fs::write(&shard, bytes)?;
    assert!(engine.read(0, PAGE).await.is_err());
    assert!(engine.status().await.poisoned);
    assert!(
        engine
            .write(6 * SHARD_BYTES, &vec![77; PAGE])
            .await
            .is_err()
    );
    assert!(engine.flush().await.is_err());
    drop(engine);

    let engine = Engine::open(config).await?;
    assert!(!engine.status().await.poisoned);
    for id in 0..6 {
        assert_eq!(
            engine.read(id * SHARD_BYTES, PAGE).await?,
            vec![id as u8 + 1; PAGE]
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_logical_cache_payload_falls_back_to_the_durable_reference() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    // Synchronous fill makes the corruption injection deterministic. The read
    // path is the same partitioned cache used by the asynchronous writer.
    config.async_cache = false;
    Engine::init(&config, 32 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    let expected = vec![0x48; PAGE * 16];
    engine.write(0, &expected).await?;
    engine.flush().await?;
    drop(engine);
    let pages = OpenOptions::new()
        .write(true)
        .open(config.local_dir.join("logical-cache/pages"))?;
    let size = pages.metadata()?.len() as usize;
    pages.write_all_at(&vec![0xa7; size], 0)?;
    drop(pages);

    let engine = Engine::open(config).await?;
    assert_eq!(engine.read(0, expected.len()).await?, expected);
    assert!(!engine.status().await.poisoned);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_restart_uses_one_complete_head_and_discards_later_flushed_writes() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.generation_mode = true;
    Engine::init(&config, 128 * MIB).await?;
    assert_eq!(Engine::inspect(&config).await?.format, 2);
    let engine = Engine::open(config.clone()).await?;
    let offsets = [0, 2 * SHARD_BYTES, 5 * SHARD_BYTES];
    for (i, &offset) in offsets.iter().enumerate() {
        engine.write(offset, &vec![i as u8 + 11; PAGE * 2]).await?;
    }
    engine.checkpoint().await?;
    let published = Engine::inspect(&config).await?.seq;
    let durable_before = engine.status().await.local_durable_sequence;
    engine.write(0, &vec![99; PAGE * 2]).await?;
    engine.zero(offsets[1], PAGE as u64).await?;
    engine
        .write(offsets[2] + PAGE as u64, &vec![88; PAGE])
        .await?;
    engine.flush().await?;
    let status = engine.status().await;
    assert_eq!(status.durability_mode, "remote-generation-rollback");
    assert_eq!(status.remote_sequence, published);
    assert_eq!(status.local_durable_sequence, durable_before);
    assert!(status.sequence > published);
    assert_eq!(engine.read(0, PAGE).await?, vec![99; PAGE]);
    drop(engine);

    let engine = Engine::open(config.clone()).await?;
    assert_eq!(engine.status().await.sequence, published);
    for (i, &offset) in offsets.iter().enumerate() {
        assert_eq!(
            engine.read(offset, PAGE * 2).await?,
            vec![i as u8 + 11; PAGE * 2]
        );
    }
    // Even an entirely unusable unpublished local WAL cannot be selected as a
    // partially newer generation. Only the verified remote HEAD is a root.
    engine.write(offsets[1], &vec![44; PAGE * 2]).await?;
    engine.flush().await?;
    drop(engine);
    for file in std::fs::read_dir(config.local_dir.join("wal"))? {
        std::fs::write(file?.path(), b"torn unpublished generation")?;
    }
    let engine = Engine::open(config.clone()).await?;
    assert_eq!(engine.status().await.sequence, published);
    for (i, &offset) in offsets.iter().enumerate() {
        assert_eq!(
            engine.read(offset, PAGE * 2).await?,
            vec![i as u8 + 11; PAGE * 2]
        );
    }
    drop(engine);

    let mut fresh = config;
    fresh.local_dir = directory.path().join("adopted");
    Engine::adopt(&fresh, true).await?;
    let engine = Engine::open(fresh).await?;
    for (i, &offset) in offsets.iter().enumerate() {
        assert_eq!(
            engine.read(offset, PAGE * 2).await?,
            vec![i as u8 + 11; PAGE * 2]
        );
    }
    Ok(())
}

#[tokio::test]
async fn durability_mode_cannot_be_changed_by_reopening_or_adopting_a_volume() -> Result<()> {
    for generation_mode in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut config = experimental_config(&directory);
        config.generation_mode = generation_mode;
        Engine::init(&config, 32 * MIB).await?;
        let before = Engine::inspect(&config).await?;
        let mut wrong = config.clone();
        wrong.generation_mode = !generation_mode;
        assert!(Engine::open(wrong.clone()).await.is_err());
        wrong.local_dir = directory.path().join("wrong-adoption");
        assert!(Engine::adopt(&wrong, true).await.is_err());
        let after = Engine::inspect(&config).await?;
        assert_eq!(before.writer, after.writer);
        assert_eq!(before.generation, after.generation);
        assert_eq!(before.format, after.format);
        let engine = Engine::open(config).await?;
        assert!(!engine.status().await.poisoned);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_lag_backpressure_waits_for_successful_publication() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.generation_mode = true;
    config.checkpoint_seconds = 1;
    config.generation_max_lag_seconds = 1;
    Engine::init(&config, 32 * MIB).await?;
    let engine = Engine::open(config).await?;
    engine.write(0, &vec![1; PAGE]).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while engine.status().await.unpublished_age_ms < 1_020 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("unpublished generation age did not advance")?;
    let data = vec![2; PAGE];
    let blocked = engine.write(PAGE as u64, &data);
    tokio::pin!(blocked);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut blocked)
            .await
            .is_err()
    );
    assert_eq!(engine.status().await.sequence, 1);
    engine.checkpoint().await?;
    tokio::time::timeout(Duration::from_secs(5), blocked)
        .await
        .context("publication did not wake blocked write")??;
    assert_eq!(engine.status().await.sequence, 2);
    assert_eq!(engine.status().await.remote_sequence, 1);
    assert_eq!(engine.read(PAGE as u64, PAGE).await?, data);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn packed_checkpoint_cas_failure_keeps_the_old_root_and_can_retry() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.compact_checkpoints = true;
    config.checkpoint_pipeline = true;
    config.wal_fixed_size = true;
    config.aligned_wal = true;
    Engine::init(&config, 128 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    for id in 0..6 {
        engine
            .write(id * SHARD_BYTES, &vec![10 + id as u8; PAGE])
            .await?;
    }
    engine.checkpoint().await?;
    let head_path = directory.path().join("remote/HEAD");
    let original_head = std::fs::read(&head_path)?;
    let old_sequence = Engine::inspect(&config).await?.seq;
    for id in 0..6 {
        engine
            .write(id * SHARD_BYTES, &vec![30 + id as u8; PAGE])
            .await?;
    }
    engine.flush().await?;
    let new_sequence = engine.status().await.sequence;

    // Deterministic failure at CAS, after immutable uploads and live-reference
    // remapping. A directory cannot be opened as the writable HEAD lock file,
    // even by root; the existing published HEAD remains readable throughout.
    let lock = directory.path().join("remote/.HEAD.lock");
    let saved_lock = directory.path().join("remote/.HEAD.lock.saved");
    std::fs::rename(&lock, &saved_lock)?;
    std::fs::create_dir(&lock)?;
    assert!(engine.checkpoint().await.is_err());
    assert_eq!(std::fs::read(&head_path)?, original_head);
    assert_eq!(engine.status().await.remote_sequence, old_sequence);
    assert!(!engine.status().await.poisoned);
    assert!(engine.status().await.pending_bytes > 0);
    for id in 0..6 {
        assert_eq!(
            engine.read(id * SHARD_BYTES, PAGE).await?,
            vec![30 + id as u8; PAGE]
        );
    }
    std::fs::remove_dir(&lock)?;
    std::fs::rename(&saved_lock, &lock)?;
    engine.checkpoint().await?;
    assert_eq!(engine.status().await.remote_sequence, new_sequence);
    assert_eq!(Engine::verify_remote(&config).await?.1, 6);
    drop(engine);

    let mut adopted = config;
    adopted.local_dir = directory.path().join("adopted");
    Engine::adopt(&adopted, true).await?;
    let engine = Engine::open(adopted).await?;
    for id in 0..6 {
        assert_eq!(
            engine.read(id * SHARD_BYTES, PAGE).await?,
            vec![30 + id as u8; PAGE]
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn packed_checkpoint_fencing_conflict_preserves_the_new_writer_root() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.compact_checkpoints = true;
    Engine::init(&config, 32 * MIB).await?;
    let old = Engine::open(config.clone()).await?;
    old.write(0, &vec![3; PAGE]).await?;
    old.checkpoint().await?;
    old.write(0, &vec![9; PAGE]).await?;
    old.flush().await?;
    let mut adopted = config.clone();
    adopted.local_dir = directory.path().join("new-writer");
    Engine::adopt(&adopted, true).await?;
    let fenced = Engine::inspect(&adopted).await?;
    assert!(old.checkpoint().await.is_err());
    assert!(old.status().await.poisoned);
    assert!(old.write(PAGE as u64, &vec![7; PAGE]).await.is_err());
    let unchanged = Engine::inspect(&adopted).await?;
    assert_eq!(unchanged.writer, fenced.writer);
    assert_eq!(unchanged.seq, fenced.seq);
    assert_eq!(unchanged.generation, fenced.generation);
    drop(old);
    let current = Engine::open(adopted).await?;
    assert_eq!(current.read(0, PAGE).await?, vec![3; PAGE]);
    current.write(PAGE as u64, &vec![4; PAGE]).await?;
    current.checkpoint().await?;
    assert_eq!(
        current.read(0, PAGE * 2).await?,
        [vec![3; PAGE], vec![4; PAGE]].concat()
    );
    Ok(())
}

async fn measure_repeated_page_publication(compact: bool) -> Result<(u64, u64)> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.compact_checkpoints = compact;
    Engine::init(&config, 32 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    for iteration in 1..=40 {
        engine.write(0, &vec![iteration; PAGE * 32]).await?;
    }
    engine.checkpoint().await?;
    let first = engine.status().await;
    assert_eq!(engine.read(0, PAGE * 32).await?, vec![40; PAGE * 32]);
    for iteration in 101..=120 {
        engine.write(0, &vec![iteration; PAGE * 32]).await?;
    }
    engine.checkpoint().await?;
    let final_status = engine.status().await;
    assert!(final_status.checkpoint_wal_bytes > first.checkpoint_wal_bytes);
    let second_upload = final_status.uploaded_segment_bytes - first.uploaded_segment_bytes;
    if compact {
        assert!(second_upload < (PAGE * 32 * 2) as u64);
    } else {
        assert!(second_upload >= (PAGE * 32 * 20) as u64);
    }
    assert_eq!(engine.read(0, PAGE * 32).await?, vec![120; PAGE * 32]);
    assert_eq!(Engine::verify_remote(&config).await?.1, 32);
    let objects_bytes = std::fs::read_dir(directory.path().join("remote/segments"))?
        .try_fold(0u64, |total, entry| {
            Ok::<_, std::io::Error>(total + entry?.metadata()?.len())
        })?;
    assert_eq!(objects_bytes, final_status.uploaded_segment_bytes);
    Ok((final_status.checkpoint_wal_bytes, objects_bytes))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compact_checkpoints_publish_only_final_versions_across_repeated_generations() -> Result<()>
{
    let (raw_wal, raw_objects) = measure_repeated_page_publication(false).await?;
    let (packed_wal, packed_objects) = measure_repeated_page_publication(true).await?;
    assert!(raw_wal >= (PAGE * 32 * 60) as u64);
    assert!(packed_wal >= (PAGE * 32 * 60) as u64);
    assert!(
        packed_objects * 10 < raw_objects,
        "compaction did not remove the repeated page history"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_cannot_recover_from_unpublished_wal_when_head_is_corrupt() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.generation_mode = true;
    config.compact_checkpoints = true;
    Engine::init(&config, 32 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    engine.write(0, &vec![3; PAGE]).await?;
    engine.checkpoint().await?;
    let published = engine.status().await.remote_sequence;
    engine.write(0, &vec![9; PAGE]).await?;
    engine.flush().await?;
    drop(engine);

    let wal: Vec<_> = std::fs::read_dir(config.local_dir.join("wal"))?
        .map(|entry| {
            let path = entry?.path();
            Ok::<_, std::io::Error>((path.clone(), std::fs::read(path)?))
        })
        .collect::<std::io::Result<_>>()?;
    assert!(!wal.is_empty());
    let head_path = directory.path().join("remote/HEAD");
    let valid = std::fs::read(&head_path)?;
    let mut corrupt = valid.clone();
    corrupt[8] ^= 1; // The checksum must be checked before local WAL removal.
    std::fs::write(&head_path, &corrupt)?;
    assert!(Engine::open(config.clone()).await.is_err());
    let mut attempted_adoption = config.clone();
    attempted_adoption.local_dir = directory.path().join("rejected-adoption");
    assert!(Engine::adopt(&attempted_adoption, true).await.is_err());
    assert_eq!(std::fs::read(&head_path)?, corrupt);
    for (path, bytes) in wal {
        assert_eq!(std::fs::read(path)?, bytes);
    }
    std::fs::write(&head_path, valid)?;
    let engine = Engine::open(config).await?;
    assert_eq!(engine.status().await.sequence, published);
    assert_eq!(engine.read(0, PAGE).await?, vec![3; PAGE]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detecting_a_corrupt_unpublished_page_stops_further_durable_writes() -> Result<()> {
    unpublished_corruption_scenario(false, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detecting_a_corrupt_sealed_unpublished_page_stops_further_durable_writes() -> Result<()> {
    unpublished_corruption_scenario(true, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_read_detecting_a_corrupt_active_wal_stops_further_durable_writes() -> Result<()> {
    unpublished_corruption_scenario(false, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_read_detecting_a_corrupt_sealed_wal_stops_further_durable_writes() -> Result<()> {
    unpublished_corruption_scenario(true, false).await
}

async fn unpublished_corruption_scenario(sealed: bool, fast: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.logical_cache = false;
    config.disk_cache_mib = 0;
    config.fast_local_reads = fast;
    Engine::init(&config, 32 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    engine
        .write(0, &vec![0x34; if sealed { MIB as usize } else { PAGE }])
        .await?;
    engine.flush().await?;
    let wal = std::fs::read_dir(config.local_dir.join("wal"))?
        .map(|entry| {
            let entry = entry?;
            Ok::<_, std::io::Error>((entry.metadata()?.len(), entry.path()))
        })
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .max_by_key(|(len, _)| *len)
        .context("missing unpublished WAL")?
        .1;
    // This fixture uses IDWAL001: 64-byte segment + 32-byte record header.
    OpenOptions::new()
        .write(true)
        .open(wal)?
        .write_all_at(&[0x59], 64 + 32 + 17)?;
    assert!(engine.read(0, PAGE).await.is_err());
    assert!(
        engine.status().await.poisoned,
        "detected authoritative WAL corruption left the engine writable"
    );
    assert!(engine.write(PAGE as u64, &vec![0x71; PAGE]).await.is_err());
    assert!(engine.flush().await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_published_wal_cache_falls_back_without_poisoning_a_healthy_volume() -> Result<()> {
    for fast in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut config = experimental_config(&directory);
        config.logical_cache = false;
        config.disk_cache_mib = 0;
        config.hot_wal_mib = 4;
        config.fast_local_reads = fast;
        Engine::init(&config, 32 * MIB).await?;
        let engine = Engine::open(config.clone()).await?;
        engine.write(0, &vec![0x34; PAGE]).await?;
        engine.checkpoint().await?;
        let wal = std::fs::read_dir(config.local_dir.join("wal"))?
            .map(|entry| {
                let entry = entry?;
                Ok::<_, std::io::Error>((entry.metadata()?.len(), entry.path()))
            })
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .max_by_key(|(len, _)| *len)
            .context("missing published WAL cache")?
            .1;
        OpenOptions::new()
            .write(true)
            .open(wal)?
            .write_all_at(&[0x59], 64 + 32 + 17)?;
        assert_eq!(engine.read(0, PAGE).await?, vec![0x34; PAGE]);
        assert!(!engine.status().await.poisoned);
        assert!(engine.status().await.remote_gets > 0);
        engine.write(PAGE as u64, &vec![0x71; PAGE]).await?;
        engine.flush().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_ignores_broken_or_missing_local_watermark_but_durable_mode_rejects_it()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = experimental_config(&directory);
    config.generation_mode = true;
    Engine::init(&config, 32 * MIB).await?;
    let engine = Engine::open(config.clone()).await?;
    engine.write(0, &vec![3; PAGE]).await?;
    engine.checkpoint().await?;
    let published = engine.status().await.remote_sequence;
    engine.write(0, &vec![9; PAGE]).await?;
    engine.flush().await?;
    drop(engine);

    std::fs::write(config.local_dir.join("durable"), [0xa5; 32])?;
    let engine = Engine::open(config.clone()).await?;
    assert_eq!(engine.status().await.sequence, published);
    assert_eq!(engine.read(0, PAGE).await?, vec![3; PAGE]);
    drop(engine);
    std::fs::remove_file(config.local_dir.join("durable"))?;
    let engine = Engine::open(config).await?;
    assert_eq!(engine.status().await.sequence, published);
    assert_eq!(engine.read(0, PAGE).await?, vec![3; PAGE]);
    drop(engine);

    let directory = tempfile::tempdir()?;
    let mut durable_config = experimental_config(&directory);
    durable_config.wal_commit_records = false;
    Engine::init(&durable_config, 32 * MIB).await?;
    let engine = Engine::open(durable_config.clone()).await?;
    engine.write(0, &vec![7; PAGE]).await?;
    engine.flush().await?;
    drop(engine);
    std::fs::write(durable_config.local_dir.join("durable"), [0xa5; 32])?;
    assert!(Engine::open(durable_config.clone()).await.is_err());
    std::fs::remove_file(durable_config.local_dir.join("durable"))?;
    assert!(Engine::open(durable_config).await.is_err());
    Ok(())
}
