use crate::helpers::dec_chunk_ref;
use crate::utils::{get_valid_name, now_ts};
use crate::{MetaStore, Node};
use fuser::{Errno, INodeNo, RenameFlags, ReplyData, ReplyEmpty, Request};
use std::sync::{Arc, RwLock};

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
            inode.modified_at = now;
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
    flags: RenameFlags,
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

    // Check if newparent is a descendant of ino (would create a loop)
    if let Some(Node::Directory { .. }) = store.structure.get(&ino) {
        let mut check = newparent.0;
        loop {
            if check == ino {
                reply.error(Errno::EINVAL);
                return;
            }
            if let Some(Node::Directory { entries, .. }) = store.structure.get(&check) {
                if let Some(&parent_ino) = entries.get("..") {
                    if parent_ino == check {
                        break; // reached root
                    }
                    check = parent_ino;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    // Check if target exists and handle rename flags
    let target_exists = store.structure.get(&newparent.0)
        .and_then(|node| match node {
            Node::Directory { entries, .. } => entries.get(&newname_str).copied(),
            _ => None,
        })
        .is_some();

    if flags.contains(RenameFlags::NOREPLACE) && target_exists {
        reply.error(Errno::EEXIST);
        return;
    }

    // RENAME_EXCHANGE is not supported yet
    if flags.contains(RenameFlags::EXCHANGE) {
        reply.error(Errno::ENOSYS);
        return;
    }

    let replace_info: Option<(u64, bool, u32)> = {
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
                                Some((existing_ino, false, 0))
                            } else {
                                None
                            }
                        }
                        Some(Node::File(inode)) => {
                            Some((existing_ino, true, inode.nlink))
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

    if let Some((existing_ino, is_file, nlink)) = &replace_info {
        if *is_file {
            // Only decrement chunk refs and remove from structure if this is the last hardlink
            if *nlink == 1 {
                if let Some(Node::File(inode)) = store.structure.get(existing_ino) {
                    for hash in &inode.chunks {
                        if *hash != [0u8; 32] {
                            dec_chunk_ref(&mut store, hash);
                        }
                    }
                }
                target_ino_to_remove = Some(*existing_ino);
            } else {
                // Just decrement the nlink count, don't remove or touch chunks
                if let Some(Node::File(inode)) = store.structure.get_mut(existing_ino) {
                    inode.nlink -= 1;
                }
            }
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

    // Handle same-directory rename to avoid double mutable borrow
    if parent.0 == newparent.0 {
        if let Some(Node::Directory {
            entries,
            inode: parent_inode,
        }) = store.structure.get_mut(&parent.0)
        {
            entries.remove(&name_str);
            entries.insert(newname_str.clone(), ino);
            parent_inode.modified_at = now;
            parent_inode.accessed_at = now;
        }
    } else {
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
    }

    match store.structure.get_mut(&ino) {
        Some(Node::File(inode)) => {
            inode.modified_at = now;
        }
        Some(Node::Directory { inode, entries }) => {
            inode.modified_at = now;
            entries.insert("..".to_string(), newparent.0);
        }
        _ => {}
    }

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

pub fn handle_symlink(
    state: &Arc<RwLock<MetaStore>>,
    req: &Request,
    parent: INodeNo,
    link_name: &std::ffi::OsStr,
    target: &std::path::Path,
    reply: fuser::ReplyEntry,
) {
    let Some(name_str) = get_valid_name(link_name) else {
        reply.error(Errno::EINVAL);
        return;
    };

    // Use lossy conversion for non-UTF8 symlink targets (better than empty string)
    let target_str = target.to_string_lossy().into_owned();

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

    let inode = Node::Symlink {
        inode: crate::Inode {
            size: target_str.len() as u64,
            nlink: 1,
            permissions: 0o777,
            uid: req.uid(),
            gid: req.gid(),
            created_at: now,
            modified_at: now,
            accessed_at: now,
            chunks: Vec::new(),
        },
        target: target_str,
    };

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
        use super::attr::make_attr;
        reply.entry(
            &super::TTL,
            &make_attr(fuser::INodeNo(new_ino), node),
            fuser::Generation(0),
        );
    } else {
        reply.error(Errno::EIO);
    }
}
