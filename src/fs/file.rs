use super::TTL;
use super::attr::make_attr;
use crate::helpers::{CHUNK_SIZE, dec_chunk_ref, inc_chunk_ref, load_chunk, store_chunk};
use crate::utils::{get_valid_name, now_ts};
use crate::{Hash, Inode, MetaStore, Node};
use fuser::{Errno, INodeNo, LockOwner, OpenFlags, ReplyData, ReplyEntry, Request};
use sha2::{Digest, Sha256};
use std::sync::{Arc, RwLock};

const ZERO_HASH: Hash = [0u8; 32];

#[allow(clippy::too_many_arguments)]
pub fn handle_read(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    ino: INodeNo,
    _fh: fuser::FileHandle,
    offset: u64,
    size: u32,
    _flags: OpenFlags,
    _lock: Option<LockOwner>,
    reply: ReplyData,
) {
    let store = state.read().unwrap();

    if let Some(Node::File(inode)) = store.structure.get(&ino.0) {
        if offset >= inode.size {
            reply.data(&[]);
            return;
        }

        let bytes_to_read = (size as u64).min(inode.size - offset);
        let mut data = Vec::with_capacity(bytes_to_read as usize);
        let mut current_offset = offset;
        let end_offset = offset + bytes_to_read;

        while current_offset < end_offset {
            let chunk_idx = (current_offset as usize) / CHUNK_SIZE;
            let chunk_offset = (current_offset as usize) % CHUNK_SIZE;
            let bytes_from_chunk =
                (CHUNK_SIZE - chunk_offset).min((end_offset - current_offset) as usize);

            if chunk_idx < inode.chunks.len() {
                let hash = inode.chunks[chunk_idx];

                if hash == ZERO_HASH {
                    data.extend(vec![0u8; bytes_from_chunk]);
                } else {
                    match load_chunk(&hash) {
                        Ok(chunk_data) => {
                            let available = chunk_data.len().saturating_sub(chunk_offset);
                            let to_copy = bytes_from_chunk.min(available);

                            if to_copy > 0 {
                                data.extend_from_slice(
                                    &chunk_data[chunk_offset..chunk_offset + to_copy],
                                );
                            }
                            if to_copy < bytes_from_chunk {
                                data.extend(vec![0u8; bytes_from_chunk - to_copy]);
                            }
                        }
                        Err(_) => {
                            reply.error(Errno::EIO);
                            return;
                        }
                    }
                }
            } else {
                data.extend(vec![0u8; bytes_from_chunk]);
            }

            current_offset += bytes_from_chunk as u64;
        }

        reply.data(&data);
    } else {
        reply.error(Errno::ENOENT);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn handle_write(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    ino: INodeNo,
    _fh: fuser::FileHandle,
    offset: u64,
    data: &[u8],
    _write_flags: fuser::WriteFlags,
    _flags: OpenFlags,
    _lock: Option<LockOwner>,
    reply: fuser::ReplyWrite,
) {
    // Phase 1: Read existing metadata (brief read lock)
    let (first_chunk_idx, last_chunk_idx, write_end, chunks_clone) = {
        let store = state.read().unwrap();
        let write_end = offset + data.len() as u64;
        let first_chunk_idx = (offset as usize) / CHUNK_SIZE;
        let last_chunk_idx = (write_end as usize - 1) / CHUNK_SIZE;

        let inode = match store.structure.get(&ino.0) {
            Some(Node::File(inode)) => inode,
            _ => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        (first_chunk_idx, last_chunk_idx, write_end, inode.chunks.clone())
    };

    // Phase 2: Build new chunk data (lock-free)
    let mut chunk_preps: Vec<(usize, Vec<u8>, Hash)> = Vec::new();

    for chunk_idx in first_chunk_idx..=last_chunk_idx {
        let chunk_offset = chunk_idx * CHUNK_SIZE;
        let chunk_end = chunk_offset + CHUNK_SIZE;

        let write_start_in_chunk = (offset as usize)
            .saturating_sub(chunk_offset)
            .min(CHUNK_SIZE);
        let write_end_in_chunk = ((write_end as usize).min(chunk_end)).saturating_sub(chunk_offset);

        let mut new_chunk_data = vec![0u8; CHUNK_SIZE];
        let mut is_zero_chunk = true;

        let old_hash = if chunk_idx < chunks_clone.len() {
            chunks_clone[chunk_idx]
        } else {
            ZERO_HASH
        };

        let has_old_chunk = old_hash != ZERO_HASH && load_chunk(&old_hash).is_ok();

        if has_old_chunk && let Ok(existing_data) = load_chunk(&old_hash) {
            let existing_len = existing_data.len().min(CHUNK_SIZE);
            new_chunk_data[..existing_len].copy_from_slice(&existing_data[..existing_len]);
        }

        if write_start_in_chunk < write_end_in_chunk {
            let data_start = (chunk_offset + write_start_in_chunk).saturating_sub(offset as usize);
            let data_end = data_start + (write_end_in_chunk - write_start_in_chunk);
            if data_end <= data.len() {
                new_chunk_data[write_start_in_chunk..write_end_in_chunk]
                    .copy_from_slice(&data[data_start..data_end]);
                is_zero_chunk = false;
            }
        }

        if is_zero_chunk || new_chunk_data.iter().all(|b| *b == 0) {
            chunk_preps.push((chunk_idx, vec![], ZERO_HASH));
            continue;
        }

        // Hash and store chunk outside of any locks
        let mut hasher = Sha256::new();
        hasher.update(&new_chunk_data);
        let new_hash: Hash = hasher.finalize().into();

        if let Err(e) = store_chunk(&new_hash, &new_chunk_data) {
            eprintln!("Error storing chunk: {:?}", e);
            reply.error(Errno::EIO);
            return;
        }

        chunk_preps.push((chunk_idx, new_chunk_data, new_hash));
    }

    // Phase 3: Update metadata (write lock)
    let mut store = state.write().unwrap();
    let now = now_ts();

    // Update chunk references
    for (chunk_idx, _, new_hash) in &chunk_preps {
        let old_hash = if *chunk_idx < chunks_clone.len() {
            chunks_clone[*chunk_idx]
        } else {
            ZERO_HASH
        };

        if old_hash != ZERO_HASH && old_hash != *new_hash {
            dec_chunk_ref(&mut store, &old_hash);
        }
        if *new_hash != ZERO_HASH && old_hash != *new_hash {
            inc_chunk_ref(&mut store, *new_hash);
        }
    }

    if let Some(Node::File(inode)) = store.structure.get_mut(&ino.0) {
        if inode.chunks.len() <= last_chunk_idx {
            inode.chunks.resize(last_chunk_idx + 1, ZERO_HASH);
        }
        for (idx, _, hash) in chunk_preps {
            inode.chunks[idx] = hash;
        }
        if write_end > inode.size {
            inode.size = write_end;
        }
        inode.modified_at = now;
        inode.accessed_at = now;
    }

    reply.written(data.len() as u32);
}

#[allow(clippy::too_many_arguments)]
pub fn handle_create(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    parent: INodeNo,
    name: &OsStr,
    mode: u32,
    umask: u32,
    _flags: i32,
    reply: fuser::ReplyCreate,
) {
    let Some(name_str) = get_valid_name(name) else {
        reply.error(Errno::EINVAL);
        return;
    };

    let mut store = state.write().unwrap();
    let now = now_ts();

    if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0) {
        if entries.contains_key(&name_str) {
            reply.error(Errno::EEXIST);
            return;
        }
    } else {
        reply.error(Errno::ENOENT);
        return;
    }

    let new_ino = store.next_inode;
    store.next_inode += 1;

    let perm = if mode == 0 { 0o644 & !umask } else { mode & !umask };

    let inode = Node::File(Inode {
        size: 0,
        nlink: 1,
        permissions: perm,
        uid: _req.uid(),
        gid: _req.gid(),
        created_at: now,
        modified_at: now,
        accessed_at: now,
        chunks: Vec::new(),
    });

    store.structure.insert(new_ino, inode);

    if let Some(Node::Directory {
        entries,
        inode: parent_inode,
    }) = store.structure.get_mut(&parent.0)
    {
        entries.insert(name_str, new_ino);
        parent_inode.modified_at = now;
        parent_inode.accessed_at = now;
    }

    if let Some(node) = store.structure.get(&new_ino) {
        reply.created(
            &TTL,
            &make_attr(INodeNo(new_ino), node),
            fuser::Generation(0),
            fuser::FileHandle(0),
            fuser::FopenFlags::empty(),
        );
    } else {
        reply.error(Errno::EIO);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn handle_mknod(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    parent: INodeNo,
    name: &OsStr,
    mode: u32,
    umask: u32,
    _rdev: u32,
    reply: ReplyEntry,
) {
    let Some(name_str) = get_valid_name(name) else {
        reply.error(Errno::EINVAL);
        return;
    };

    let mut store = state.write().unwrap();
    let now = now_ts();

    if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0) {
        if entries.contains_key(&name_str) {
            reply.error(Errno::EEXIST);
            return;
        }
    } else {
        reply.error(Errno::ENOENT);
        return;
    }

    let new_ino = store.next_inode;
    store.next_inode += 1;

    let perm = if mode == 0 { 0o644 & !umask } else { mode & !umask };

    let inode = Node::File(Inode {
        size: 0,
        nlink: 1,
        permissions: perm,
        uid: _req.uid(),
        gid: _req.gid(),
        created_at: now,
        modified_at: now,
        accessed_at: now,
        chunks: Vec::new(),
    });

    store.structure.insert(new_ino, inode);

    if let Some(Node::Directory {
        entries,
        inode: parent_inode,
    }) = store.structure.get_mut(&parent.0)
    {
        entries.insert(name_str, new_ino);
        parent_inode.modified_at = now;
        parent_inode.accessed_at = now;
    }

    if let Some(node) = store.structure.get(&new_ino) {
        reply.entry(&TTL, &make_attr(INodeNo(new_ino), node), fuser::Generation(0));
    } else {
        reply.error(Errno::EIO);
    }
}
