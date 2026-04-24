# xFS

A content-addressed FUSE filesystem written in Rust.

## Quick Start

```bash
cargo build --release
sudo cargo run
```

## Requirements

- Must run as root
- Mountpoint: `/mnt/xfs`
- Metadata: `/etc/xfs/meta.json`
- Chunk storage: `/etc/xfs/store/`

## Architecture

- **Entrypoint:** `src/main.rs` — root check, metastore init, genesis setup, FUSE mount
- **Filesystem:** `src/fs/mod.rs` — implements `fuser::Filesystem` trait
- **Helpers:** `src/helpers.rs` — persistence, chunk storage, caching, GC thread

## Development

```bash
cargo check
cargo clippy
```

## Dependencies

- `fuser` — FUSE bindings
- `serde` — serialization
- `sha2` — content hashing
- `zstd` — compression