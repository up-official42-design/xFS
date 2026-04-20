use crate::helpers::{cleanup_unused_chunks, dec_chunk_ref};
use crate::{MetaStore, Node};
use fuser::{Errno, INodeNo, RenameFlags, ReplyData, ReplyEmpty, Request};
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

pub fn handle_unlink(
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

    let (ino, is_file) =
        if let Some(Node::Directory { entries, .. }) = store.structure.get(&parent.0) {
            if let Some(&ino) = entries.get(&name_str) {
                let is_file = matches!(store.structure.get(&ino), Some(Node::File(_)));
                (ino, is_file)
            } else {
                reply.error(Errno::ENOENT);
                return;
            }
        } else {
            reply.error(Errno::ENOENT);
            return;
        };

    if !is_file {
        reply.error(Errno::EISDIR);
        return;
    }

    let chunk_refs = if let Some(Node::File(inode)) = store.structure.get(&ino) {
        if inode.nlink == 1 {
            Some(inode.chunks.clone())
        } else {
            None
        }
    } else {
        None
    };

    let needs_removal = if let Some(Node::File(inode)) = store.structure.get_mut(&ino) {
        inode.nlink = inode.nlink.saturating_sub(1);
        if inode.nlink == 0 {
            true
        } else {
            inode.created_at = now;
            false
        }
    } else {
        false
    };

    if needs_removal {
        if let Some(chunks) = chunk_refs {
            for hash in &chunks {
                if *hash != [0u8; 32] {
                    dec_chunk_ref(&mut store, hash);
                }
            }
        }
        store.structure.remove(&ino);
    }

    if let Some(Node::Directory {
        entries,
        inode: parent_inode,
    }) = store.structure.get_mut(&parent.0)
    {
        entries.remove(&name_str);
        parent_inode.modified_at = now;
        parent_inode.accessed_at = now;
    }

    cleanup_unused_chunks(&store);
    reply.ok();
}

#[allow(clippy::too_many_arguments)]
pub fn handle_rename(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    parent: INodeNo,
    name: &OsStr,
    newparent: INodeNo,
    newname: &OsStr,
    _flags: RenameFlags,
    reply: ReplyEmpty,
) {
    let Some(name_str) = get_valid_name(name) else {
        reply.error(Errno::EINVAL);
        return;
    };
    let Some(newname_str) = get_valid_name(newname) else {
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

    let replace_info: Option<(u64, bool, Vec<[u8; 32]>)> = {
        if let Some(Node::Directory { entries, .. }) = store.structure.get(&newparent.0) {
            if let Some(&existing_ino) = entries.get(&newname_str) {
                if existing_ino != ino {
                    match store.structure.get(&existing_ino) {
                        Some(Node::Directory {
                            entries: existing_entries,
                            ..
                        }) => {
                            let non_special: Vec<_> = existing_entries
                                .keys()
                                .filter(|k| *k != "." && *k != "..")
                                .collect();
                            if non_special.is_empty() {
                                Some((existing_ino, false, Vec::new()))
                            } else {
                                None
                            }
                        }
                        Some(Node::File(inode)) => {
                            let hashes = if inode.nlink > 1 {
                                None
                            } else {
                                Some(inode.chunks.clone())
                            };
                            Some((existing_ino, true, hashes.unwrap_or_default()))
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    };

    let mut target_ino_to_remove: Option<u64> = None;
    let mut target_np_nlink_decr = false;

    if let Some((existing_ino, is_file, chunks)) = &replace_info {
        if *is_file {
            let hashes: Vec<[u8; 32]> = chunks.to_vec();
            for hash in hashes {
                if hash != [0u8; 32] {
                    dec_chunk_ref(&mut store, &hash);
                }
            }
            target_ino_to_remove = Some(*existing_ino);
        } else {
            target_ino_to_remove = Some(*existing_ino);
            target_np_nlink_decr = true;
        }
    }

    if let Some(target_ino) = target_ino_to_remove {
        store.structure.remove(&target_ino);
        if target_np_nlink_decr
            && let Some(Node::Directory {
                inode: np_inode, ..
            }) = store.structure.get_mut(&newparent.0)
        {
            np_inode.nlink = np_inode.nlink.saturating_sub(1);
        }
    }

    if let Some(Node::Directory {
        entries,
        inode: parent_inode,
    }) = store.structure.get_mut(&parent.0)
    {
        entries.remove(&name_str);
        parent_inode.modified_at = now;
        parent_inode.accessed_at = now;
    }

    if let Some(Node::Directory {
        entries,
        inode: parent_inode,
    }) = store.structure.get_mut(&newparent.0)
    {
        entries.insert(newname_str.clone(), ino);
        parent_inode.modified_at = now;
        parent_inode.accessed_at = now;
    }

    match store.structure.get_mut(&ino) {
        Some(Node::File(inode)) => {
            inode.created_at = now;
        }
        Some(Node::Directory { inode, entries }) => {
            inode.created_at = now;
            if parent.0 != newparent.0 {
                entries.insert("..".to_string(), newparent.0);
                if let Some(Node::Directory {
                    inode: old_parent_inode,
                    ..
                }) = store.structure.get_mut(&parent.0)
                {
                    old_parent_inode.nlink = old_parent_inode.nlink.saturating_sub(1);
                }
                if let Some(Node::Directory {
                    inode: new_parent_inode,
                    ..
                }) = store.structure.get_mut(&newparent.0)
                {
                    new_parent_inode.nlink = new_parent_inode.nlink.saturating_sub(1);
                }
            }
        }
        _ => {}
    }

    cleanup_unused_chunks(&store);
    reply.ok();
}

pub fn handle_readlink(
    state: &Arc<RwLock<MetaStore>>,
    _req: &Request,
    ino: INodeNo,
    reply: ReplyData,
) {
    let store = state.read().unwrap();
    if let Some(Node::Symlink { target, .. }) = store.structure.get(&ino.0) {
        reply.data(target.as_bytes());
    } else {
        reply.error(Errno::EINVAL);
    }
}
