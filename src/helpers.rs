use crate::{Chunk, Hash, MetaStore, Node};
pub use crate::fs::cache::{create_chunk_cache, SharedChunkCache, ChunkCacheManager, CACHE_SIZE_BYTES};
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};
use std::thread;
use std::time::Duration;

const ZERO_HASH: Hash = [0u8; 32];

pub const DECOMPRESSED_CHUNK_SIZE: usize = 512 * 1024;

static CHUNK_CACHE: OnceLock<SharedChunkCache> = OnceLock::new();

pub fn get_chunk_cache() -> &'static SharedChunkCache {
    CHUNK_CACHE.get_or_init(|| Arc::new(RwLock::new(ChunkCacheManager::new())))
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

    // Restrict /etc/xfs to root only using native fs operations
    if let Err(e) = fs::set_permissions("/etc/xfs", std::fs::Permissions::from_mode(0o700)) {
        eprintln!("Warning: Failed to chmod /etc/xfs: {}", e);
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

    // Mountpoint directory - must be world-accessible
    if !Path::new("/mnt/xfs").is_dir() {
        if Path::new("/mnt/xfs").exists() {
            fs::remove_file("/mnt/xfs").expect("Failed to remove file");
        }
        fs::create_dir_all("/mnt/xfs").expect("Failed to create mountpoint dir");
    }
    if let Err(e) = fs::set_permissions("/mnt/xfs", std::fs::Permissions::from_mode(0o755)) {
        eprintln!("Warning: Failed to chmod /mnt/xfs: {}", e);
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
        return Ok(data.as_ref().to_vec());
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

/// Increment reference count for a chunk
pub fn inc_chunk_ref(metastore: &mut MetaStore, hash: Hash) {
    metastore
        .chunks
        .entry(hash)
        .and_modify(|c| c.nlink += 1)
        .or_insert(Chunk { hash, nlink: 1 });
}

/// Decrement reference count for a chunk, returns true if chunk is no longer referenced
pub fn dec_chunk_ref(metastore: &mut MetaStore, hash: &Hash) -> bool {
    if let Some(chunk) = metastore.chunks.get_mut(hash) {
        chunk.nlink = chunk.nlink.saturating_sub(1);
        if chunk.nlink == 0 {
            metastore.chunks.remove(hash);
            return true;
        }
    }
    false
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
            if subdir_path.is_dir()
                && let Ok(subdir_entries) = fs::read_dir(&subdir_path)
            {
                for file in subdir_entries.flatten() {
                    let path = file.path();
                    if path.is_file()
                        && let Some(name) = path.file_name()
                    {
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
    stored
}

pub fn cleanup_unused_chunks(metastore: &MetaStore) {
    let stored = collect_stored_chunks();

    for hash in &stored {
        let should_delete = match metastore.chunks.get(hash) {
            Some(chunk) => chunk.nlink == 0,
            None => true,
        };

        if should_delete {
            let path = get_chunk_path(hash);
            if path.exists()
                && let Err(e) = fs::remove_file(&path)
            {
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

pub fn rebuild_chunk_nlink(metastore: &mut MetaStore) {
    let mut hashes_to_inc: Vec<Hash> = Vec::new();
    for node in metastore.structure.values() {
        if let Node::File(inode) = node {
            for hash in &inode.chunks {
                if *hash != ZERO_HASH {
                    hashes_to_inc.push(*hash);
                }
            }
        }
    }
    for hash in hashes_to_inc {
        inc_chunk_ref(metastore, hash);
    }
}

pub fn load_metastore() -> Arc<RwLock<MetaStore>> {
    let data = fs::read_to_string("/etc/xfs/meta.json").expect("Failed to read metastore file");
    match serde_json::from_str::<MetaStore>(&data) {
        Ok(mut metastore) => {
            // Ensure next_inode is at least max existing inode + 1
            let max_inode = metastore.structure.keys().max().copied().unwrap_or(0);
            if metastore.next_inode <= max_inode {
                metastore.next_inode = max_inode + 1;
            }
            rebuild_chunk_nlink(&mut metastore);
            Arc::new(RwLock::new(metastore))
        }
        Err(_) => Arc::new(RwLock::new(MetaStore {
            structure: HashMap::new(),
            chunks: HashMap::new(),
            next_inode: 2, // Start from 2, reserving 1 for root
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

    let json = serde_json::to_string(&*data).map_err(MetaStoreSaveError::SerializationError)?;

    fs::write("/etc/xfs/meta.json", json.as_bytes()).map_err(MetaStoreSaveError::IO)?;

    Ok(())
}
