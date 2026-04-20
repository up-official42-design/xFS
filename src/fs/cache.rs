use crate::Hash;
use std::collections::{HashMap, VecDeque};

pub const CACHE_SIZE_BYTES: usize = 8 * 1024 * 1024 * 1024; // 8 GB

pub struct ChunkCache {
    order: VecDeque<Hash>,
    map: HashMap<Hash, Vec<u8>>,
    positions: HashMap<Hash, u64>,
    current_gen: u64,
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
            positions: HashMap::new(),
            current_gen: 0,
            size_bytes: 0,
        }
    }

    pub fn get(&mut self, hash: &[u8; 32]) -> Option<Vec<u8>> {
        if self.positions.get(hash).copied().is_some() {
            self.positions.insert(*hash, self.current_gen);
            self.current_gen = self.current_gen.wrapping_add(1);
            return self.map.get(hash).cloned();
        }
        None
    }

    fn evict_one(&mut self) -> bool {
        while let Some(old_hash) = self.order.pop_front() {
            if self.positions.contains_key(&old_hash) {
                if let Some(old_data) = self.map.remove(&old_hash) {
                    self.size_bytes -= old_data.len();
                }
                self.positions.remove(&old_hash);
                return true;
            }
        }
        false
    }

    pub fn put(&mut self, hash: Hash, data: Vec<u8>) {
        let data_size = data.len();

        if let Some(old) = self.map.remove(&hash) {
            self.size_bytes -= old.len();
        }

        while self.size_bytes + data_size > CACHE_SIZE_BYTES {
            if !self.evict_one() {
                break;
            }
        }

        self.positions.insert(hash, self.current_gen);
        self.current_gen = self.current_gen.wrapping_add(1);
        self.order.push_back(hash);

        self.map.insert(hash, data);
        self.size_bytes += data_size;
    }

    pub fn clear(&mut self) {
        self.order.clear();
        self.map.clear();
        self.positions.clear();
        self.current_gen = 0;
        self.size_bytes = 0;
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.map.contains_key(hash)
    }
}
