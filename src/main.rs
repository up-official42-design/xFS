pub mod fs;
pub mod helpers;
pub mod utils;

use crate::helpers::{
    cleanup_unused_chunks, file_setup, is_root, load_metastore, save_metastore,
    set_metastore_for_gc, start_gc_thread,
};
use fuser::{Config, MountOption, SessionACL};
use std::collections::HashMap;
use std::process::Command;

pub type InodeId = u64;
pub type Hash = [u8; 32];

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct Inode {
    pub size: u64,
    pub nlink: u32,
    pub permissions: u32,
    pub uid: u32,
    pub gid: u32,
    pub created_at: u64,
    pub modified_at: u64,
    pub accessed_at: u64,
    pub chunks: Vec<Hash>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub enum Node {
    File(Inode),
    Directory {
        inode: Inode,
        entries: HashMap<String, InodeId>,
    },
    Symlink {
        inode: Inode,
        target: String,
    },
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Chunk {
    pub hash: Hash,
    pub nlink: u32,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct MetaStore {
    pub structure: HashMap<InodeId, Node>,
    pub chunks: HashMap<Hash, Chunk>,
    pub next_inode: u64,
}

fn cleanup_mount_point(path: &str) {
    // We don't really care if this fails (e.g., if the path isn't mounted),
    // so we just fire and forget.
    let _ = Command::new("sudo")
        .arg("umount")
        .arg("-l")
        .arg(path)
        .status();
}

fn main() {
    if !is_root() {
        eprintln!("Error: xFS requires root privileges for mounting.");
        std::process::exit(1);
    }

    file_setup();
    println!("--- Welcome to xFS ---");

    let metastore = load_metastore();

    // Cleanup unused chunks on startup (only if filesystem was previously initialized)
    {
        let store = metastore.read().unwrap();
        if store.next_inode > 1 {
            cleanup_unused_chunks(&store);
        }
    }

    // Spawn GC thread
    set_metastore_for_gc(metastore.clone());
    start_gc_thread();

    // Genesis: Ensure Inode 1 exists (Root) and /home folder
    let needs_save = {
        let mut lock = metastore.write().unwrap();
        let now = chrono::Utc::now().timestamp() as u64;

        // Root directory (inode 1) with 755 for everyone to read
        let created_root = !lock.structure.contains_key(&1);
        if created_root {
            println!("Genesis: Creating root directory (Inode 1)...");
            let mut entries = HashMap::new();
            entries.insert(".".to_string(), 1);
            entries.insert("..".to_string(), 1);

            // /home directory
            let home_inode = lock.next_inode;
            lock.next_inode += 1;
            let mut home_entries = HashMap::new();
            home_entries.insert(".".to_string(), home_inode);
            home_entries.insert("..".to_string(), 1);
            lock.structure.insert(home_inode, Node::Directory {
                inode: Inode {
                    size: 0,
                    nlink: 2,
                    permissions: 0o755,
                    uid: 0,
                    gid: 0,
                    created_at: now,
                    modified_at: now,
                    accessed_at: now,
                    chunks: Vec::new(),
                },
                entries: home_entries,
            });
            entries.insert("home".to_string(), home_inode);

            lock.structure.insert(1, Node::Directory {
                inode: Inode {
                    size: 0,
                    nlink: 2,
                    permissions: 0o755,
                    uid: 0,
                    gid: 0,
                    created_at: now,
                    modified_at: now,
                    accessed_at: now,
                    chunks: Vec::new(),
                },
                entries,
            });
        }

        // Ensure next_inode is at least 2
        if lock.next_inode <= 1 {
            lock.next_inode = 2;
        }
        created_root
    };

    if needs_save {
        save_metastore(metastore.clone()).expect("Failed to save initial metastore");
    }

    let mountpoint = "/mnt/xfs"; // Mount at /mnt/xfs

    let options = vec![
        MountOption::CUSTOM("allow_other".to_string()),
        MountOption::FSName("xfs".to_string()),
    ];

    // Config is non-exhaustive, need to use Default and modify via mutable reference
    let mut config = Config::default();
    config.mount_options = options;
    config.acl = SessionACL::All; // Allow all users to access the filesystem

    println!("Mounting xFS at {}...", mountpoint);

    let filesystem = fs::XFS { state: metastore };

    cleanup_mount_point(mountpoint);

    // Fuser 0.17.0 mount2
    fuser::mount2(filesystem, mountpoint, &config).expect("Failed to mount filesystem");
}
