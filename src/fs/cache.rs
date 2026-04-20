use crate::Hash;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

pub const CACHE_SIZE_BYTES: usize = 8 * 1024 * 1024 * 1024;

pub struct ChunkCache {
    order: VecDeque<Hash>,
    map: HashMap<Hash, Arc<Vec<u8>>>,
    size_bytes: usize,
}

impl Default for ChunkCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkCache {
    pub fn new() -> Self {
        Self {
            order: VecDeque::new(),
            map: HashMap::new(),
            size_bytes: 0,
        }
    }

    pub fn get(&mut self, hash: &[u8; 32]) -> Option<Arc<Vec<u8>>> {
        if self.map.contains_key(hash) {
            self.order.retain(|h| h != hash);
            self.order.push_back(*hash);
            return self.map.get(hash).cloned();
        }
        None
    }

    fn evict_one(&mut self) -> bool {
        while let Some(old_hash) = self.order.pop_front() {
            if let Some(data) = self.map.remove(&old_hash) {
                self.size_bytes -= data.len();
                return true;
            }
        }
        false
    }

    pub fn put(&mut self, hash: Hash, data: Vec<u8>) {
        let data_size = data.len();

        if let Some(old) = self.map.remove(&hash) {
            self.size_bytes -= old.len();
            self.order.retain(|h| *h != hash);
        }

        while self.size_bytes + data_size > CACHE_SIZE_BYTES {
            if !self.evict_one() {
                break;
            }
        }

        let data = Arc::new(data);
        self.order.push_back(hash);
        self.map.insert(hash, Arc::clone(&data));
        self.size_bytes += data_size;
    }

    pub fn clear(&mut self) {
        self.order.clear();
        self.map.clear();
        self.size_bytes = 0;
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.map.contains_key(hash)
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

pub struct ChunkCacheManager {
    cache: ChunkCache,
}

impl ChunkCacheManager {
    pub fn new() -> Self {
        Self {
            cache: ChunkCache::new(),
        }
    }

    pub fn get(&mut self, hash: &[u8; 32]) -> Option<Arc<Vec<u8>>> {
        self.cache.get(hash)
    }

    pub fn put(&mut self, hash: Hash, data: Vec<u8>) {
        self.cache.put(hash, data);
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.cache.contains(hash)
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }

    pub fn size_bytes(&self) -> usize {
        self.cache.size_bytes()
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for ChunkCacheManager {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedChunkCache = Arc<RwLock<ChunkCacheManager>>;

pub fn create_chunk_cache() -> SharedChunkCache {
    Arc::new(RwLock::new(ChunkCacheManager::new()))
}