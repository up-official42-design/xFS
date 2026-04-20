use crate::Node;
use fuser::{FileAttr, FileType, INodeNo};
use std::time::{Duration, UNIX_EPOCH};

pub fn make_attr(ino: INodeNo, node: &Node) -> FileAttr {
    let i = match node {
        Node::File(inode) => inode,
        Node::Directory { inode, .. } => inode,
        Node::Symlink { inode, .. } => inode,
    };

    // For files, count actual allocated chunks instead of size/512
    // This correctly handles sparse files
    let blocks = match node {
        Node::File(inode) => {
            let non_zero_chunks = inode
                .chunks
                .iter()
                .filter(|&&hash| hash != [0u8; 32])
                .count();
            (non_zero_chunks * 512 * 1024 / 512) as u64
        }
        _ => i.size.div_ceil(512),
    };

    FileAttr {
        ino,
        size: i.size,
        blocks,
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
        uid: i.uid,
        gid: i.gid,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}
