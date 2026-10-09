//! Disposable bounded SSD extent cache. It never establishes durability.
use anyhow::Result;
use lru::LruCache;
use std::{path::PathBuf, sync::Mutex};
use uuid::Uuid;
pub const EXTENT: u64 = 256 * 1024;
pub type Key = (Uuid, u64);
struct Entry {
    path: PathBuf,
    len: u64,
}
struct State {
    entries: LruCache<Key, Entry>,
    bytes: u64,
}
pub struct DiskCache {
    dir: PathBuf,
    max: u64,
    state: Mutex<State>,
}
impl DiskCache {
    pub fn open(dir: PathBuf, max: u64, extent: u64) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let marker = dir.join("extent-size");
        if std::fs::read_to_string(&marker).ok().as_deref() != Some(&extent.to_string()) {
            for f in std::fs::read_dir(&dir)? {
                let f = f?;
                if f.path().extension().is_some_and(|e| e == "cache") {
                    std::fs::remove_file(f.path())?;
                }
            }
            std::fs::write(&marker, extent.to_string())?;
        }
        let cache = Self {
            dir: dir.clone(),
            max,
            state: Mutex::new(State {
                entries: LruCache::unbounded(),
                bytes: 0,
            }),
        };
        for file in std::fs::read_dir(&dir)? {
            let file = file?;
            let name = file.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(base) = name.strip_suffix(".cache") else {
                continue;
            };
            let Some((id, start)) = base.split_once('.') else {
                continue;
            };
            if let (Ok(id), Ok(start)) = (Uuid::parse_str(id), start.parse::<u64>()) {
                let len = file.metadata()?.len();
                if len > 0 && len <= extent + 4096 && start % extent == 0 {
                    let mut s = cache.state.lock().unwrap();
                    s.bytes += len;
                    s.entries.put(
                        (id, start),
                        Entry {
                            path: file.path(),
                            len,
                        },
                    );
                } else {
                    let _ = std::fs::remove_file(file.path());
                }
            }
        }
        cache.evict();
        Ok(cache)
    }
    fn evict(&self) {
        let mut s = self.state.lock().unwrap();
        while s.bytes > self.max {
            if let Some((_, e)) = s.entries.pop_lru() {
                s.bytes -= e.len;
                let _ = std::fs::remove_file(e.path);
            } else {
                break;
            }
        }
    }
    pub async fn get(&self, k: Key) -> Option<bytes::Bytes> {
        if self.max == 0 {
            return None;
        }
        let path = self
            .state
            .lock()
            .unwrap()
            .entries
            .get(&k)
            .map(|e| e.path.clone())?;
        match tokio::fs::read(path).await {
            Ok(b) => Some(b.into()),
            Err(_) => {
                self.remove(k);
                None
            }
        }
    }
    pub fn remove(&self, k: Key) {
        let mut s = self.state.lock().unwrap();
        if let Some(e) = s.entries.pop(&k) {
            s.bytes -= e.len;
            let _ = std::fs::remove_file(e.path);
        }
    }
    pub async fn put(&self, k: Key, b: &[u8]) {
        if self.max == 0 || b.len() as u64 > self.max {
            return;
        }
        let path = self.dir.join(format!("{}.{}.cache", k.0, k.1));
        let temp = self.dir.join(format!("{}.tmp", Uuid::new_v4()));
        let result = async {
            tokio::fs::write(&temp, b).await?;
            tokio::fs::rename(&temp, &path).await?;
            Ok::<_, std::io::Error>(())
        }
        .await;
        if let Err(err) = result {
            let _ = tokio::fs::remove_file(&temp).await;
            tracing::warn!(error=%err,"SSD cache write failed; cache is disposable");
            return;
        }
        let mut s = self.state.lock().unwrap();
        let len = b.len() as u64;
        if let Some(old) = s.entries.put(k, Entry { path, len }) {
            s.bytes -= old.len;
        }
        s.bytes += len;
        drop(s);
        self.evict();
    }
}
