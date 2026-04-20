use super::TTL;
use super::attr::make_attr;
use crate::{Inode, MetaStore, Node};
use fuser::{Errno, FileType, INodeNo, ReplyDirectory, ReplyEmpty, ReplyEntry, Request};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::{Arc, RwLock};

fn get_valid_name(name: &OsStr) -> Option<String> {
    name.to_str()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn now_ts() -> u64 {
    chrono::Utc::now().timestamp() as u64
}

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

    if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0)
        && let Some(&ino_u64) = entries.get(&name_str)
        && let Some(node) = store.structure.get(&ino_u64)
    {
        reply.entry(
            &TTL,
            &make_attr(INodeNo(ino_u64), node),
            fuser::Generation(0),
        );
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
        let mut all_entries: Vec<(&str, u64, FileType)> = Vec::new();

        all_entries.push((".", ino.0, FileType::Directory));
        if let Some(parent_ino) = entries.get("..") {
            all_entries.push(("..", *parent_ino, FileType::Directory));
        } else {
            all_entries.push(("..", 1, FileType::Directory));
        }

        for (name, &child_ino) in entries.iter() {
            if name == "." || name == ".." {
                continue;
            }
            let node = store.structure.get(&child_ino);
            let kind = match node {
                Some(Node::File(_)) => FileType::RegularFile,
                Some(Node::Symlink { .. }) => FileType::Symlink,
                Some(Node::Directory { .. }) => FileType::Directory,
                _ => FileType::RegularFile,
            };
            all_entries.push((name, child_ino, kind));
        }

        for (i, (name, child_ino, kind)) in all_entries.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*child_ino), (i + 1) as u64, *kind, name) {
                break;
            }
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

    // Default directories: 755, apply umask if mode provided, otherwise use 755
    let perm = if mode == 0 {
        0o755 & !umask
    } else {
        (mode & !umask) | 0o111
    };

    // Auto-create user home folder: if under /home, use directory name as uid
    // This assumes /home has inode 2 (the second entry we created in genesis)
    let home_uid = if parent.0 == 2 { 0 } else { _req.uid() };
    let home_gid = if parent.0 == 2 { 0 } else { _req.gid() };

    let inode = Node::Directory {
        inode: Inode {
            size: 0,
            nlink: 2,
            permissions: perm,
            uid: home_uid,
            gid: home_gid,
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
        parent_inode.nlink = parent_inode.nlink.saturating_sub(1);
        parent_inode.modified_at = now;
        parent_inode.accessed_at = now;
    }

    reply.ok();
}
