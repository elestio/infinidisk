//! Variable-size, disposable range cache. The budget counts owned payload bytes.
use bytes::Bytes;
use lru::LruCache;
use uuid::Uuid;

pub(crate) type Key = (Uuid, u64);

pub(crate) struct ReadCache {
    entries: LruCache<Key, Bytes>,
    bytes: usize,
    limit: usize,
}

impl ReadCache {
    pub fn new(limit: usize) -> Self {
        Self {
            entries: LruCache::unbounded(),
            bytes: 0,
            limit,
        }
    }

    pub fn get(&mut self, key: &Key) -> Option<&Bytes> {
        self.entries.get(key)
    }

    pub fn pop(&mut self, key: &Key) {
        if let Some(old) = self.entries.pop(key) {
            self.bytes -= old.len();
        }
    }

    pub fn put(&mut self, key: Key, data: Bytes) {
        if data.is_empty() || data.len() > self.limit {
            return;
        }
        self.pop(&key);
        while self.bytes > self.limit - data.len() {
            if let Some((_, old)) = self.entries.pop_lru() {
                self.bytes -= old.len();
            } else {
                break;
            }
        }
        self.bytes += data.len();
        self.entries.put(key, data);
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_sizes_replacement_eviction_and_zero_budget() {
        let id = Uuid::nil();
        let mut cache = ReadCache::new(32);
        cache.put((id, 0), Bytes::from(vec![1; 16]));
        cache.put((id, 16), Bytes::from(vec![2; 16]));
        cache.put((id, 0), Bytes::from(vec![3; 24]));
        assert_eq!(cache.bytes(), 24);
        assert!(cache.get(&(id, 16)).is_none());
        cache.put((id, 32), Bytes::from(vec![4; 64]));
        assert_eq!(cache.bytes(), 24);
        cache.pop(&(id, 0));
        assert_eq!(cache.bytes(), 0);
        let mut disabled = ReadCache::new(0);
        disabled.put((id, 0), Bytes::from_static(b"x"));
        assert_eq!(disabled.bytes(), 0);
    }
}
