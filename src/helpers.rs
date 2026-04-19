use crate::{MetaStore, Hash, Node};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};
use std::thread;
use std::time::Duration;

pub const CACHE_SIZE_BYTES: usize = 8 * 1024 * 1024 * 1024; // 8 GB
pub const DECOMPRESSED_CHUNK_SIZE: usize = 512 * 1024; // 512 KB decompressed

#[derive(Default)]
pub struct ChunkCache {
    cache: VecDeque<(Hash, Vec<u8>)>,
    size_bytes: usize,
}

impl ChunkCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&mut self, hash: &[u8; 32]) -> Option<Vec<u8>> {
        if let Some(pos) = self.cache.iter().position(|(h, _)| *h == *hash) {
            let (_, data) = self.cache.remove(pos).unwrap();
            return Some(data);
        }
        None
    }

    pub fn put(&mut self, hash: Hash, data: Vec<u8>) {
        let data_size = data.len();
        while self.size_bytes + data_size > CACHE_SIZE_BYTES {
            if let Some((_, removed)) = self.cache.pop_front() {
                self.size_bytes -= removed.len();
            } else {
                break;
            }
        }
        self.cache.push_back((hash, data));
        self.size_bytes += data_size;
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.size_bytes = 0;
    }
}

static DECOMPRESSED_CHUNK_CACHE: OnceLock<RwLock<ChunkCache>> = OnceLock::new();

pub fn get_chunk_cache() -> &'static RwLock<ChunkCache> {
    DECOMPRESSED_CHUNK_CACHE.get_or_init(|| RwLock::new(ChunkCache::new()))
}

pub fn is_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

pub const CHUNK_SIZE: usize = 512 * 1024; // 512 KB

pub fn file_setup() {
    if !Path::new("/etc/xfs").is_dir() {
        if Path::new("/etc/xfs").is_file() {
            fs::remove_file(Path::new("/etc/xfs")).expect("Failed to remove file");
        }
        fs::create_dir_all("/etc/xfs").expect("Failed to create dir");
    }

    if !Path::new("/etc/xfs/meta.json").is_file() {
        if Path::new("/etc/xfs/meta.json").is_dir() {
            fs::remove_dir_all("/etc/xfs/meta.json").expect("Failed to remove dir");
        }
        let file = fs::File::create("/etc/xfs/meta.json").expect("Failed to create metastore file");
        file.write_at(b"{}", 0)
            .expect("Failed to write meta.json file");
    }

    // Chunk storage directory - stored in /etc/xfs/store
    if !Path::new("/etc/xfs/store").is_dir() {
        if Path::new("/etc/xfs/store").is_file() {
            fs::remove_file("/etc/xfs/store").expect("Failed to remove file");
        }
        fs::create_dir_all("/etc/xfs/store").expect("Failed to create chunks dir");
    }
}

/// Convert hash bytes to hex string
fn hash_to_hex(hash: &[u8; 32]) -> String {
    hex::encode(hash)
}

/// Get the full path for a chunk file
pub fn get_chunk_path(hash: &[u8; 32]) -> std::path::PathBuf {
    let hex = hash_to_hex(hash);
    // Use sharding: first 2 chars as subdir for better filesystem performance
    let subdir = &hex[..2];
    std::path::PathBuf::from(format!("/etc/xfs/store/{}/{}", subdir, hex))
}

/// Ensure the chunk directory exists
fn ensure_chunk_dir(hash: &[u8; 32]) {
    let hex = hash_to_hex(hash);
    let subdir = format!("/etc/xfs/store/{}", &hex[..2]);
    let _ = fs::create_dir_all(&subdir);
}

/// Store a chunk on disk, compressed with zstd
pub fn store_chunk(hash: &[u8; 32], data: &[u8]) -> Result<(), std::io::Error> {
    ensure_chunk_dir(hash);
    let path = get_chunk_path(hash);

    // Compress with zstd
    let compressed = zstd::encode_all(data, 3)?; // level 3 for balanced speed/compression

    fs::write(path, compressed)?;
    Ok(())
}

/// Load a chunk from cache or disk and decompress
pub fn load_chunk(hash: &[u8; 32]) -> Result<Vec<u8>, std::io::Error> {
    let mut cache = get_chunk_cache().write().unwrap();
    if let Some(data) = cache.get(hash) {
        return Ok(data);
    }
    drop(cache);

    let path = get_chunk_path(hash);
    let compressed = fs::read(path)?;

    let data = zstd::decode_all(&compressed[..])?;

    let mut cache = get_chunk_cache().write().unwrap();
    cache.put(*hash, data.clone());

    Ok(data)
}

/// Check if a chunk exists on disk
pub fn chunk_exists(hash: &[u8; 32]) -> bool {
    get_chunk_path(hash).exists()
}

pub fn collect_used_chunks(metastore: &MetaStore) -> std::collections::HashSet<Hash> {
    let mut used = std::collections::HashSet::new();
    for node in metastore.structure.values() {
        match node {
            Node::File(inode) => {
                for chunk in &inode.chunks {
                    used.insert(*chunk);
                }
            }
            Node::Directory { .. } => {}
            Node::Symlink { .. } => {}
        }
    }
    used
}

pub fn collect_stored_chunks() -> std::collections::HashSet<Hash> {
    let mut stored = std::collections::HashSet::new();
    let store_path = Path::new("/etc/xfs/store");
    if let Ok(entries) = fs::read_dir(store_path) {
        for entry in entries.flatten() {
            let subdir_path = entry.path();
            if subdir_path.is_dir() {
                if let Ok(subdir_entries) = fs::read_dir(&subdir_path) {
                    for file in subdir_entries.flatten() {
                        let path = file.path();
                        if path.is_file() {
                            if let Some(name) = path.file_name() {
                                let name_str = match name.to_str() {
                                    Some(s) => s,
                                    None => continue,
                                };
                                let hex = match hex::decode(name_str) {
                                    Ok(h) => h,
                                    Err(_) => continue,
                                };
                                if hex.len() == 32 {
                                    let mut hash = [0u8; 32];
                                    hash.copy_from_slice(&hex);
                                    stored.insert(hash);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    stored
}

pub fn cleanup_unused_chunks(metastore: &MetaStore) {
    let used = collect_used_chunks(metastore);
    let stored = collect_stored_chunks();

    for hash in stored.difference(&used) {
        let path = get_chunk_path(hash);
        if path.exists() {
            if let Err(e) = fs::remove_file(&path) {
                eprintln!("Failed to remove unused chunk {:?}: {}", hash, e);
            }
        }
    }
}

pub static METASTORE_FOR_GC: OnceLock<Arc<RwLock<MetaStore>>> = OnceLock::new();

pub fn set_metastore_for_gc(metastore: Arc<RwLock<MetaStore>>) {
    let _ = METASTORE_FOR_GC.set(metastore);
}

pub fn start_gc_thread() {
    let _ = thread::spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(60));
            if let Some(metastore) = METASTORE_FOR_GC.get() {
                let metastore = metastore.clone();
                let store = metastore.read().unwrap();
                cleanup_unused_chunks(&store);
            }
        }
    });
}

pub fn load_metastore() -> Arc<RwLock<MetaStore>> {
    let data = fs::read_to_string("/etc/xfs/meta.json").expect("Failed to read metastore file");
    match serde_json::from_str::<MetaStore>(&data) {
        Ok(metastore) => Arc::new(RwLock::new(metastore)),
        Err(_) => Arc::new(RwLock::new(MetaStore {
            structure: HashMap::new(),
            chunks: HashMap::new(),
        })),
    }
}
#[derive(Debug)]
pub enum MetaStoreSaveError {
    IO(std::io::Error),
    SerializationError(serde_json::Error),
    LockPoisonError,
}

pub fn save_metastore(metastore: Arc<RwLock<MetaStore>>) -> Result<(), MetaStoreSaveError> {
    let Ok(data) = (*metastore).read() else {
        return Err(MetaStoreSaveError::LockPoisonError);
    };

    let json =
        serde_json::to_string(&*data).map_err(MetaStoreSaveError::SerializationError)?;

    fs::write("/etc/xfs/meta.json", json.as_bytes()).map_err(MetaStoreSaveError::IO)?;

    Ok(())
}
