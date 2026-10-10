//! Disposable cache of immutable *remote* index objects, distinct from the
//! mutable paged-index scratch. HEAD and WAL remain the recovery authorities.
use bytes::Bytes;
use lru::LruCache;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use uuid::Uuid;

pub(crate) const MAX_OBJECT: u64 = 1024 * 1024;
const MAX_ENTRIES: usize = 16_384;

#[derive(Default)]
struct State {
    entries: Option<LruCache<String, u64>>,
    bytes: u64,
}

#[derive(Default, Debug, Serialize)]
pub struct Stats {
    pub enabled: bool,
    pub budget_bytes: u64,
    pub bytes: u64,
    pub entries: u64,
    pub hits: u64,
    pub misses: u64,
    pub corruptions: u64,
    pub errors: u64,
    pub remote_gets: u64,
    pub remote_bytes: u64,
}

pub(crate) struct IndexObjects {
    directory: PathBuf,
    budget: u64,
    state: Mutex<State>,
    enabled: AtomicBool,
    bytes: AtomicU64,
    entries: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    corruptions: AtomicU64,
    errors: AtomicU64,
    remote_gets: AtomicU64,
    remote_bytes: AtomicU64,
}

impl IndexObjects {
    pub async fn open(directory: PathBuf, budget: u64) -> Arc<Self> {
        let cache = Arc::new(Self {
            directory,
            budget,
            state: Mutex::new(State::default()),
            enabled: AtomicBool::new(budget > 0),
            bytes: AtomicU64::new(0),
            entries: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            corruptions: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            remote_gets: AtomicU64::new(0),
            remote_bytes: AtomicU64::new(0),
        });
        if budget > 0 {
            let worker = cache.clone();
            match tokio::task::spawn_blocking(move || worker.scan()).await {
                Ok(Ok(())) => {}
                error => cache.disable(format!("initialize: {error:?}")),
            }
        }
        cache
    }

    fn key(volume: Uuid, shard: u64, object: &str, hash: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(b"InfiniDisk2 remote index cache v1\0");
        digest.update(volume.as_bytes());
        digest.update(shard.to_le_bytes());
        digest.update((object.len() as u64).to_le_bytes());
        digest.update(object.as_bytes());
        digest.update(hash.as_bytes());
        hex::encode(digest.finalize())
    }

    fn path(&self, key: &str) -> PathBuf {
        self.directory.join(format!("{key}.idx"))
    }

    fn disable(&self, error: String) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.enabled.store(false, Ordering::Relaxed);
        tracing::warn!(%error, "remote index cache unavailable; use verified S3 objects");
    }

    fn publish_size(&self, state: &State) {
        self.bytes.store(state.bytes, Ordering::Relaxed);
        self.entries.store(
            state.entries.as_ref().map_or(0, |x| x.len()) as u64,
            Ordering::Relaxed,
        );
    }

    fn remove(&self, state: &mut State, key: &str) -> std::io::Result<()> {
        match std::fs::remove_file(self.path(key)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if let Some(length) = state.entries.as_mut().unwrap().pop(key) {
            state.bytes -= length;
        }
        self.publish_size(state);
        Ok(())
    }

    fn room(&self, state: &mut State, length: u64) -> std::io::Result<()> {
        while state.bytes > self.budget - length
            || state.entries.as_ref().unwrap().len() >= MAX_ENTRIES
        {
            let key = state
                .entries
                .as_ref()
                .unwrap()
                .peek_lru()
                .unwrap()
                .0
                .clone();
            self.remove(state, &key)?;
        }
        Ok(())
    }

    fn scan(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.directory)?;
        let mut state = self.state.lock().unwrap();
        state.entries = Some(LruCache::unbounded());
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(id) = name.strip_suffix(".tmp") {
                if Uuid::parse_str(id).is_ok() {
                    std::fs::remove_file(entry.path())?;
                }
                continue;
            }
            let Some(key) = name.strip_suffix(".idx") else {
                continue;
            };
            if key.len() != 64 || !key.bytes().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }
            let metadata = entry.path().symlink_metadata()?;
            let length = metadata.len();
            if !metadata.is_file() || length == 0 || length > MAX_OBJECT || length > self.budget {
                std::fs::remove_file(entry.path())?;
                continue;
            }
            self.room(&mut state, length)?;
            state.entries.as_mut().unwrap().put(key.into(), length);
            state.bytes += length;
        }
        self.publish_size(&state);
        Ok(())
    }

    pub async fn get(
        self: &Arc<Self>,
        volume: Uuid,
        shard: u64,
        object: &str,
        hash: &str,
    ) -> Option<Bytes> {
        if !self.enabled.load(Ordering::Relaxed) {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let worker = self.clone();
        let key = Self::key(volume, shard, object, hash);
        let expected = hash.to_owned();
        let result = tokio::task::spawn_blocking(move || {
            let mut state = worker.state.lock().unwrap();
            state.entries.as_mut()?.get(&key)?;
            let data = (|| {
                let file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(worker.path(&key))?;
                let mut bytes = Vec::new();
                file.take(MAX_OBJECT + 1).read_to_end(&mut bytes)?;
                Ok::<_, std::io::Error>(bytes)
            })();
            if let Ok(bytes) = data {
                if bytes.len() as u64 <= MAX_OBJECT
                    && hex::encode(Sha256::digest(&bytes)) == expected
                {
                    return Some(Bytes::from(bytes));
                }
                worker.corruptions.fetch_add(1, Ordering::Relaxed);
            }
            if let Err(error) = worker.remove(&mut state, &key) {
                worker.disable(error.to_string());
            }
            None
        })
        .await;
        let value = match result {
            Ok(value) => value,
            Err(error) => {
                self.disable(error.to_string());
                None
            }
        };
        if value.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        value
    }

    /// Writes are disposable: no fsync and never an application durability barrier.
    /// Serialize fills off the async workers; one temporary object at most.
    pub async fn put(
        self: &Arc<Self>,
        volume: Uuid,
        shard: u64,
        object: &str,
        hash: &str,
        bytes: Bytes,
    ) {
        if !self.enabled.load(Ordering::Relaxed)
            || bytes.is_empty()
            || bytes.len() as u64 > MAX_OBJECT
            || bytes.len() as u64 > self.budget
        {
            return;
        }
        let worker = self.clone();
        let key = Self::key(volume, shard, object, hash);
        let expected = hash.to_owned();
        let result = tokio::task::spawn_blocking(move || {
            let mut state = worker.state.lock().unwrap();
            if !worker.enabled.load(Ordering::Relaxed) {
                return Ok(());
            }
            if hex::encode(Sha256::digest(&bytes)) != expected {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "index cache fill checksum mismatch",
                ));
            }
            worker.remove(&mut state, &key)?;
            worker.room(&mut state, bytes.len() as u64)?;
            let temporary = worker.directory.join(format!("{}.tmp", Uuid::new_v4()));
            let written = (|| {
                let mut file = File::create_new(&temporary)?;
                file.write_all(&bytes)?;
                drop(file);
                std::fs::rename(&temporary, worker.path(&key))
            })();
            if written.is_err() {
                let _ = std::fs::remove_file(&temporary);
            }
            written?;
            state.entries.as_mut().unwrap().put(key, bytes.len() as u64);
            state.bytes += bytes.len() as u64;
            worker.publish_size(&state);
            Ok::<_, std::io::Error>(())
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            error => self.disable(format!("fill: {error:?}")),
        }
    }

    pub fn remote_read(&self, bytes: usize) {
        self.remote_gets.fetch_add(1, Ordering::Relaxed);
        self.remote_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn status(&self) -> Stats {
        Stats {
            enabled: self.enabled.load(Ordering::Relaxed),
            budget_bytes: self.budget,
            bytes: self.bytes.load(Ordering::Relaxed),
            entries: self.entries.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            corruptions: self.corruptions.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            remote_gets: self.remote_gets.load(Ordering::Relaxed),
            remote_bytes: self.remote_bytes.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn index_objects_cache_budget_identity_and_unavailable_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache");
        let volume = Uuid::new_v4();
        let bytes = Bytes::from_static(b"abcdefgh");
        let hash = hex::encode(Sha256::digest(&bytes));
        let cache = IndexObjects::open(path.clone(), 8).await;
        cache
            .put(volume, 1, "indexes/1/a", &hash, bytes.clone())
            .await;
        assert_eq!(
            cache.get(volume, 1, "indexes/1/a", &hash).await,
            Some(bytes.clone())
        );
        assert!(
            cache
                .get(Uuid::new_v4(), 1, "indexes/1/a", &hash)
                .await
                .is_none()
        );
        assert!(cache.get(volume, 2, "indexes/1/a", &hash).await.is_none());
        cache
            .put(volume, 2, "indexes/2/b", &hash, bytes.clone())
            .await;
        assert!(cache.get(volume, 1, "indexes/1/a", &hash).await.is_none());
        assert_eq!(cache.status().bytes, 8);
        drop(cache);
        std::fs::write(path.join(format!("{}.tmp", Uuid::new_v4())), b"torn").unwrap();
        let cache = IndexObjects::open(path.clone(), 8).await;
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 1);
        assert_eq!(
            cache.get(volume, 2, "indexes/2/b", &hash).await,
            Some(bytes.clone())
        );
        drop(cache);
        let disabled = IndexObjects::open(path, 0).await;
        assert!(
            disabled
                .get(volume, 2, "indexes/2/b", &hash)
                .await
                .is_none()
        );
        let blocked = dir.path().join("not-a-directory");
        std::fs::write(&blocked, b"occupied").unwrap();
        let unavailable = IndexObjects::open(blocked, 8).await;
        assert!(!unavailable.status().enabled);
        assert_eq!(unavailable.status().errors, 1);
        unavailable.put(volume, 1, "x", &hash, bytes).await;
        assert!(unavailable.get(volume, 1, "x", &hash).await.is_none());
    }
}
