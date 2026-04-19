use crate::{MetaStore, Node, Inode};
use crate::helpers::{CHUNK_SIZE, store_chunk, load_chunk};
use fuser::{
    FileAttr, FileHandle, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, Request, INodeNo, Errno, LockOwner, OpenFlags,
};
use std::sync::{Arc, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use std::ffi::OsStr;
use sha2::{Sha256, Digest};

pub struct XFS {
    pub state: Arc<RwLock<MetaStore>>,
}

const TTL: Duration = Duration::from_secs(1);

impl XFS {
    // Note: ino is now INodeNo, not u64
    fn make_attr(&self, ino: INodeNo, node: &Node) -> FileAttr {
        let i = match node {
            Node::File(inode) => inode,
            Node::Directory { inode, .. } => inode,
            Node::Symlink { inode, .. } => inode,
        };

        FileAttr {
            ino,
            size: i.size,
            blocks: i.size.div_ceil(512),
            atime: UNIX_EPOCH + Duration::from_secs(i.accessed_at),
            mtime: UNIX_EPOCH + Duration::from_secs(i.modified_at),
            ctime: UNIX_EPOCH + Duration::from_secs(i.created_at),
            crtime: UNIX_EPOCH + Duration::from_secs(i.created_at),
            kind: match node {
                Node::File(_) => FileType::RegularFile,
                Node::Directory { .. } => FileType::Directory,
                Node::Symlink { .. } => FileType::Symlink,
            },
            perm: i.permissions as u16,
            nlink: i.nlink,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }
}

impl Filesystem for XFS {
    // 0.17.0 uses &self (immutable) and adds an optional FileHandle
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let store = self.state.read().unwrap();
        if let Some(node) = store.structure.get(&ino.0) { // ino.0 extracts the u64
            reply.attr(&TTL, &self.make_attr(ino, node));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().unwrap_or("");
        let store = self.state.read().unwrap();

        if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0)
            && let Some(&ino_u64) = entries.get(name_str)
            && let Some(node) = store.structure.get(&ino_u64)
        {
            reply.entry(&TTL, &self.make_attr(INodeNo(ino_u64), node), fuser::Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn readdir(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let store = self.state.read().unwrap();
        if let Some(Node::Directory { entries, .. }) = store.structure.get(&ino.0) {
            if offset == 0 {
                let _ = reply.add(ino, 0, FileType::Directory, ".");
                let _ = reply.add(ino, 1, FileType::Directory, "..");
            }

            for (i, (name, &child_ino)) in entries.iter().enumerate().skip(offset as usize) {
                let node = store.structure.get(&child_ino);
                let kind = match node {
                    Some(Node::File(_)) => FileType::RegularFile,
                    Some(Node::Symlink { .. }) => FileType::Symlink,
                    Some(Node::Directory { .. }) => FileType::Directory,
                    _ => FileType::Directory,
                };

                // Note: reply.add takes INodeNo and offset is u64
                if reply.add(INodeNo(child_ino), (i + 2) as u64, kind, name) {
                    break;
                }
            }
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn read(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock: Option<LockOwner>, reply: ReplyData) {
        let store = self.state.read().unwrap();

        if let Some(Node::File(inode)) = store.structure.get(&ino.0) {
            // If reading beyond file size, return empty
            if offset >= inode.size {
                reply.data(&[]);
                return;
            }

            // Calculate which chunk to start from
            let mut data = Vec::new();
            let mut current_offset = offset;
            let mut remaining_size = size as usize;
            let mut chunk_index = (offset as usize) / CHUNK_SIZE;

            while remaining_size > 0 && chunk_index < inode.chunks.len() {
                let hash = &inode.chunks[chunk_index];

                // Load and decompress chunk
                match load_chunk(hash) {
                    Ok(chunk_data) => {
                        let chunk_offset = offset as usize % CHUNK_SIZE;
                        let bytes_available = chunk_data.len().saturating_sub(chunk_offset);
                        let bytes_to_copy = remaining_size.min(bytes_available);

                        if bytes_to_copy > 0 {
                            data.extend_from_slice(&chunk_data[chunk_offset..chunk_offset + bytes_to_copy]);
                            remaining_size -= bytes_to_copy;
                        }

                        chunk_index += 1;
                        current_offset = (current_offset / CHUNK_SIZE as u64 + 1) * CHUNK_SIZE as u64;
                    }
                    Err(e) => {
                        eprintln!("Error loading chunk: {:?}", e);
                        reply.error(Errno::EIO);
                        return;
                    }
                }
            }

            reply.data(&data);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn write(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, data: &[u8], _write_flags: fuser::WriteFlags, _flags: OpenFlags, _lock: Option<LockOwner>, reply: fuser::ReplyWrite) {
        let mut store = self.state.write().unwrap();

        if let Some(Node::File(inode)) = store.structure.get_mut(&ino.0) {
            let mut written = 0;
            let current_offset = offset as usize;
            let first_chunk_index = current_offset / CHUNK_SIZE;

            let mut chunk_updates: Vec<([u8; 32], usize)> = Vec::new();

            // First pass: store chunks and collect updates
            for chunk_idx in 0.. {
                let chunk_start = chunk_idx * CHUNK_SIZE;
                let chunk_end = ((chunk_idx + 1) * CHUNK_SIZE).min(current_offset + data.len());

                if chunk_start >= current_offset + data.len() {
                    break;
                }

                let data_start = chunk_start.saturating_sub(current_offset);
                let data_end = if chunk_end > current_offset {
                    (chunk_end - current_offset).min(data.len())
                } else {
                    0
                };

                if data_start >= data_end {
                    continue;
                }

                let chunk_data = &data[data_start..data_end];

                // Hash the chunk data
                let mut hasher = Sha256::new();
                hasher.update(chunk_data);
                let hash: [u8; 32] = hasher.finalize().into();

                // Store compressed chunk
                if let Err(e) = store_chunk(&hash, chunk_data) {
                    eprintln!("Error storing chunk: {:?}", e);
                    reply.error(Errno::EIO);
                    return;
                }

                chunk_updates.push((hash, first_chunk_index + chunk_idx));
                written += chunk_data.len();
            }

            // Second pass: update inode and chunk tracking
            for (hash, chunk_number) in chunk_updates {
                if chunk_number < inode.chunks.len() {
                    inode.chunks[chunk_number] = hash;
                } else {
                    inode.chunks.push(hash);
                }
            }

            // Update file size
            let new_size = offset + data.len() as u64;
            inode.size = inode.size.max(new_size);

            // Update modified timestamp
            inode.modified_at = chrono::Utc::now().timestamp() as u64;

            reply.written(written as u32);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn create(&self, _req: &Request, parent: INodeNo, name: &OsStr, _mode: u32, _umask: u32, _flags: i32, reply: fuser::ReplyCreate) {
        let name_str = name.to_str().unwrap_or("");
        let mut store = self.state.write().unwrap();

        if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0)
            && entries.contains_key(name_str)
        {
            reply.error(Errno::EEXIST);
            return;
        }

        let next_inode = store.structure.keys().max().unwrap_or(&0) + 1;
        let now = chrono::Utc::now().timestamp() as u64;

        let inode = Node::File(Inode {
            size: 0,
            nlink: 1,
            permissions: 0o644,
            created_at: now,
            modified_at: now,
            accessed_at: now,
            chunks: Vec::new(),
        });

        store.structure.insert(next_inode, inode);

        if let Some(Node::Directory { entries, .. }) = store.structure.get_mut(&parent.0) {
            entries.insert(name_str.to_string(), next_inode);
        }

        if let Some(node) = store.structure.get(&next_inode) {
            reply.created(&TTL, &self.make_attr(INodeNo(next_inode), node), fuser::Generation(0), FileHandle(0), fuser::FopenFlags::empty());
        } else {
            reply.error(Errno::EIO);
        }
    }

    fn mknod(&self, _req: &Request, parent: INodeNo, name: &OsStr, _mode: u32, _umask: u32, _rdev: u32, reply: ReplyEntry) {
        let name_str = name.to_str().unwrap_or("");
        let mut store = self.state.write().unwrap();

        if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0)
            && entries.contains_key(name_str)
        {
            reply.error(Errno::EEXIST);
            return;
        }

        let next_inode = store.structure.keys().max().unwrap_or(&0) + 1;
        let now = chrono::Utc::now().timestamp() as u64;

        let inode = Node::File(Inode {
            size: 0,
            nlink: 1,
            permissions: 0o644,
            created_at: now,
            modified_at: now,
            accessed_at: now,
            chunks: Vec::new(),
        });

        store.structure.insert(next_inode, inode);

        if let Some(Node::Directory { entries, .. }) = store.structure.get_mut(&parent.0) {
            entries.insert(name_str.to_string(), next_inode);
        }

        if let Some(node) = store.structure.get(&next_inode) {
            reply.entry(&TTL, &self.make_attr(INodeNo(next_inode), node), fuser::Generation(0));
        } else {
            reply.error(Errno::EIO);
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let store = self.state.read().unwrap();
        if let Some(Node::Symlink { target, .. }) = store.structure.get(&ino.0) {
            reply.data(target.as_bytes());
        } else {
            reply.error(Errno::EINVAL);
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap_or("");
        let mut store = self.state.write().unwrap();

        let ino = if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0) {
            if let Some(&ino) = entries.get(name_str) {
                ino
            } else {
                reply.error(Errno::ENOENT);
                return;
            }
        } else {
            reply.error(Errno::ENOENT);
            return;
        };

        store.structure.remove(&ino);

        if let Some(Node::Directory { entries, .. }) = store.structure.get_mut(&parent.0) {
            entries.remove(name_str);
        }

        reply.ok();
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap_or("");
        let mut store = self.state.write().unwrap();

        let ino = if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0) {
            if let Some(&ino) = entries.get(name_str) {
                ino
            } else {
                reply.error(Errno::ENOENT);
                return;
            }
        } else {
            reply.error(Errno::ENOENT);
            return;
        };

        if let Some(Node::Directory { entries, .. }) = store.structure.get(&ino) {
            if !entries.is_empty() {
                reply.error(Errno::ENOTEMPTY);
                return;
            }
        } else {
            reply.error(Errno::ENOTDIR);
            return;
        }

        store.structure.remove(&ino);

        if let Some(Node::Directory { entries, .. }) = store.structure.get_mut(&parent.0) {
            entries.remove(name_str);
        }

        reply.ok();
    }
}
