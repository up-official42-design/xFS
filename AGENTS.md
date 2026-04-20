# AGENTS.md — xFS

A content-addressed FUSE filesystem written in Rust.

## Quick Start

```bash
cargo build --release
sudo cargo run
```

## Architecture

- **Entrypoint:** `src/main.rs` — root check, metastore init, genesis setup, FUSE mount
- **Filesystem:** `src/fs/mod.rs` — implements `fuser::Filesystem` trait (v0.17.0)
- **Helpers:** `src/helpers.rs` — persistence, chunk storage, caching, GC thread

## Runtime Requirements

- **Must run as root** — exits with code 1 if `geteuid() != 0`
- **Mountpoint:** `/mnt/xfs` (created automatically with 755 permissions)
- **Metadata:** `/etc/xfs/meta.json` (JSON-serialized metastore)
- **Chunk storage:** `/etc/xfs/store/<2hex>/<32hex>` (zstd-compressed, sharded by first 2 hex chars)
- **Cache:** 8GB LRU decompressed chunk cache in memory

## Fuser 0.17.0 API Notes

- Inode numbers are `INodeNo` (wrapper), access raw via `ino.0`
- Methods take `&self` — state changes require `Arc<RwLock<MetaStore>>` interior mutability
- `getattr` receives `Option<u64>` file handle

## Key Types

```rust
pub type InodeId = u64;
pub type Hash = [u8; 32];

pub struct MetaStore {
    pub structure: HashMap<InodeId, Node>,
    pub chunks: HashMap<Hash, Chunk>,
    pub next_inode: u64,
}

pub enum Node {
    File(Inode),
    Directory { inode: Inode, entries: HashMap<String, InodeId> },
    Symlink { inode: Inode, target: String },
}
```

## Development

- `cargo check` / `cargo clippy` — validation
- Release build recommended (FUSE is performance-sensitive)
- Metastore auto-saves on graceful exit; manual save via `save_metastore()`
- Background GC thread runs every 60s (cleanup unused chunks)
- Genesis creates root (inode 1) and `/home` directory on first run

## Dependencies

- `fuser = "0.17.0"` — FUSE bindings
- `serde`, `serde_json` — serialization
- `sha2` — content hashing
- `zstd` — compression (level 3)
- `chrono` — timestamps
- `libc`, `errno`, `hex` — low-level