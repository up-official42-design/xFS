use super::TTL;
use super::attr::make_attr;
use crate::utils::{get_valid_name, now_ts};
use crate::{Inode, MetaStore, Node};
use fuser::{Errno, FileType, INodeNo, ReplyDirectory, ReplyEmpty, ReplyEntry, Request};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::{Arc, RwLock};

pub fn handle_lookup(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    parent: INodeNo,
    name: &OsStr,
    reply: fuser::ReplyEntry,
) {
    let Some(name_str) = get_valid_name(name) else {
        reply.error(Errno::EINVAL);
        return;
    };
    let store = state.read().unwrap();

    if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0) {
        if let Some(&ino_u64) = entries.get(&name_str) {
            if let Some(node) = store.structure.get(&ino_u64) {
                reply.entry(
                    &TTL,
                    &make_attr(INodeNo(ino_u64), node),
                    fuser::Generation(0),
                );
            } else {
                reply.error(Errno::EIO);
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    } else {
        reply.error(Errno::ENOENT);
    }
}

pub fn handle_readdir(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    ino: INodeNo,
    _fh: fuser::FileHandle,
    offset: u64,
    mut reply: ReplyDirectory,
) {
    let store = state.read().unwrap();
    if let Some(Node::Directory { entries, .. }) = store.structure.get(&ino.0) {
        // Use stable ordering by sorting entries
        let mut sorted_entries: Vec<_> = entries.iter().collect();
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut offset_idx = offset.saturating_sub(1) as usize;

        // .
        if offset == 0 || offset_idx < 1 {
            if reply.add(INodeNo(ino.0), 1, FileType::Directory, ".") {
                return reply.ok();
            }
        }

        // ..
        if offset <= 2 {
            if offset_idx < 2 {
                offset_idx = 2;
            }
            let parent_ino = entries.get("..").copied().unwrap_or(1);
            if reply.add(INodeNo(parent_ino), 2, FileType::Directory, "..") {
                return reply.ok();
            }
        }

        for (name, &child_ino) in sorted_entries {
            if name == "." || name == ".." {
                continue;
            }
            let entry_offset = (offset_idx + 1) as u64;
            if entry_offset < offset {
                offset_idx += 1;
                continue;
            }
            let node = store.structure.get(&child_ino);
            let kind = match node {
                Some(Node::File(_)) => FileType::RegularFile,
                Some(Node::Symlink { .. }) => FileType::Symlink,
                Some(Node::Directory { .. }) => FileType::Directory,
                _ => FileType::RegularFile,
            };
            if reply.add(INodeNo(child_ino), entry_offset + 1, kind, name) {
                return reply.ok();
            }
            offset_idx += 1;
        }
        reply.ok();
    } else {
        reply.error(Errno::ENOENT);
    }
}

pub fn handle_mkdir(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    parent: INodeNo,
    name: &OsStr,
    mode: u32,
    umask: u32,
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

    let mut entries = HashMap::new();
    entries.insert(".".to_string(), new_ino);
    entries.insert("..".to_string(), parent.0);

    let perm = if mode == 0 { 0o755 & !umask } else { mode & !umask };

    let inode = Node::Directory {
        inode: Inode {
            size: 0,
            nlink: 2,
            permissions: perm,
            uid: _req.uid(),
            gid: _req.gid(),
            created_at: now,
            modified_at: now,
            accessed_at: now,
            chunks: Vec::new(),
        },
        entries,
    };

    store.structure.insert(new_ino, inode);

    if let Some(Node::Directory {
        entries,
        inode: parent_inode,
    }) = store.structure.get_mut(&parent.0)
    {
        entries.insert(name_str, new_ino);
        parent_inode.nlink += 1;
        parent_inode.modified_at = now;
        parent_inode.accessed_at = now;
    }

    if let Some(node) = store.structure.get(&new_ino) {
        reply.entry(
            &TTL,
            &make_attr(INodeNo(new_ino), node),
            fuser::Generation(0),
        );
    } else {
        reply.error(Errno::EIO);
    }
}

pub fn handle_rmdir(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    parent: INodeNo,
    name: &OsStr,
    reply: ReplyEmpty,
) {
    let Some(name_str) = get_valid_name(name) else {
        reply.error(Errno::EINVAL);
        return;
    };

    let mut store = state.write().unwrap();
    let now = now_ts();

    let ino = if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0) {
        if let Some(&ino) = entries.get(&name_str) {
            ino
        } else {
            reply.error(Errno::ENOENT);
            return;
        }
    } else {
        reply.error(Errno::ENOENT);
        return;
    };

    let is_empty = if let Some(Node::Directory { entries, .. }) = store.structure.get(&ino) {
        let non_special: Vec<_> = entries.keys().filter(|k| *k != "." && *k != "..").collect();
        non_special.is_empty()
    } else {
        reply.error(Errno::ENOTDIR);
        return;
    };

    if !is_empty {
        reply.error(Errno::ENOTEMPTY);
        return;
    }

    store.structure.remove(&ino);

    if let Some(Node::Directory {
        entries,
        inode: parent_inode,
    }) = store.structure.get_mut(&parent.0)
    {
        entries.remove(&name_str);
        parent_inode.nlink = parent_inode.nlink.checked_sub(1).unwrap_or_else(|| {
            eprintln!("Warning: directory nlink underflow");
            0
        });
        parent_inode.modified_at = now;
        parent_inode.accessed_at = now;
        parent_inode.created_at = now; // ctime update for directory change
    }

    reply.ok();
}
