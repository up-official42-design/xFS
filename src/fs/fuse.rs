use crate::MetaStore;
use fuser::{
    FileAttr, FileHandle, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, Request, INodeNo, Errno, LockOwner, OpenFlags, TimeOrNow, BsdFileFlags,
    RenameFlags, KernelConfig, AccessFlags,
};
use std::sync::{Arc, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use std::ffi::OsStr;
use crate::Node;
use super::dir::{handle_lookup, handle_readdir, handle_mkdir, handle_rmdir};
use super::file::{handle_read, handle_write, handle_create, handle_mknod};
use super::link::{handle_unlink, handle_rename};

pub const TTL: Duration = Duration::from_secs(1);

pub struct XFS {
    pub state: Arc<RwLock<MetaStore>>,
}

impl XFS {
    pub fn new(state: Arc<RwLock<MetaStore>>) -> Self {
        Self { state }
    }

    pub fn make_attr(&self, ino: INodeNo, node: &Node) -> FileAttr {
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
            uid: i.uid,
            gid: i.gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }
}

impl Filesystem for XFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        handle_lookup(&self.state, req, parent, name, reply);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let store = self.state.read().unwrap();
        if let Some(node) = store.structure.get(&ino.0) {
            reply.attr(&TTL, &self.make_attr(ino, node));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let mut store = self.state.write().unwrap();

        if let Some(node) = store.structure.get_mut(&ino.0) {
            if let Some(new_size) = size && let Node::File(inode) = node {
                inode.size = new_size;
            }

            if let Some(mtime_val) = mtime {
                let now = chrono::Utc::now().timestamp();
                let mtime_secs: u64 = match mtime_val {
                    TimeOrNow::SpecificTime(t) => t
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(now),
                    TimeOrNow::Now => now,
                } as u64;
                match node {
                    Node::File(inode) => inode.modified_at = mtime_secs,
                    Node::Directory { inode, .. } => inode.modified_at = mtime_secs,
                    Node::Symlink { inode, .. } => inode.modified_at = mtime_secs,
                }
            }

            if let Some(mode_val) = mode {
                match node {
                    Node::File(inode) => inode.permissions = mode_val,
                    Node::Directory { inode, .. } => inode.permissions = mode_val,
                    Node::Symlink { inode, .. } => inode.permissions = mode_val,
                }
            }

            if let Some(uid_val) = uid {
                match node {
                    Node::File(inode) => inode.uid = uid_val,
                    Node::Directory { inode, .. } => inode.uid = uid_val,
                    Node::Symlink { inode, .. } => inode.uid = uid_val,
                }
            }

            if let Some(gid_val) = gid {
                match node {
                    Node::File(inode) => inode.gid = gid_val,
                    Node::Directory { inode, .. } => inode.gid = gid_val,
                    Node::Symlink { inode, .. } => inode.gid = gid_val,
                }
            }

            reply.attr(&TTL, &self.make_attr(ino, node));
        } else {
            reply.error(Errno::ENOENT);
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

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        handle_mknod(&self.state, req, parent, name, mode, umask, rdev, reply);
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        handle_mkdir(&self.state, req, parent, name, mode, umask, reply);
    }

    fn unlink(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        handle_unlink(&self.state, req, parent, name, reply);
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        handle_rmdir(&self.state, req, parent, name, reply);
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        handle_rename(&self.state, req, parent, name, newparent, newname, flags, reply);
    }

    fn read(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock: Option<LockOwner>,
        reply: ReplyData,
    ) {
        handle_read(&self.state, req, ino, fh, offset, size, flags, lock, reply);
    }

    fn write(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        write_flags: fuser::WriteFlags,
        flags: OpenFlags,
        lock: Option<LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        handle_write(&self.state, req, ino, fh, offset, data, write_flags, flags, lock, reply);
    }

    fn readdir(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        handle_readdir(&self.state, req, ino, fh, offset, reply);
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        handle_create(&self.state, req, parent, name, mode, umask, flags, reply);
    }

    fn access(&self, req: &Request, ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        let store = self.state.read().unwrap();
        if let Some(node) = store.structure.get(&ino.0) {
            let inode = match node {
                Node::File(inode) => inode,
                Node::Directory { inode, .. } => inode,
                Node::Symlink { inode, .. } => inode,
            };

            let perm = inode.permissions;
            let uid = inode.uid;
            let gid = inode.gid;

            let is_root = req.uid() == 0;
            let user_matches = req.uid() == uid;
            let group_matches = req.gid() == gid;

            let mode_bits = if is_root {
                0o7
            } else if user_matches {
                (perm >> 6) & 0o7
            } else if group_matches {
                (perm >> 3) & 0o7
            } else {
                perm & 0o7
            };

            let has_perms = ((mask & AccessFlags::R_OK).is_empty() || (mode_bits & 0o4) != 0)
                && ((mask & AccessFlags::W_OK).is_empty() || (mode_bits & 0o2) != 0)
                && ((mask & AccessFlags::X_OK).is_empty() || (mode_bits & 0o1) != 0);

            if has_perms {
                reply.ok();
            } else {
                reply.error(Errno::EACCES);
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    }
}