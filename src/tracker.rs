//! File handle tracking for monitoring
//!
//! This module provides lightweight tracking of open file handles
//! for debugging and monitoring purposes. It logs handle open/close
//! events but does NOT change resource management logic.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Simple file handle info for logging/monitoring
#[derive(Debug, Clone)]
pub struct HandleInfo {
    pub ino: u64,
    pub pid: u32,
    pub opened_at: u64,
    pub flags: i32,
}

/// Global file handle tracker
/// 
/// Note: This is for MONITORING only. The actual resource management
/// still relies on kernel FUSE semantics. Adding entries here does NOT
/// prevent resource cleanup.
pub struct HandleTracker {
    handles: HashMap<u64, HandleInfo>,
    next_handle: u64,
}

impl Default for HandleTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl HandleTracker {
    pub fn new() -> Self {
        Self {
            handles: HashMap::new(),
            next_handle: 1,
        }
    }
    
    /// Record a file handle open (for monitoring)
    pub fn open(&mut self, ino: u64, pid: u32, flags: i32) -> u64 {
        let handle = self.next_handle;
        self.next_handle = self.next_handle.wrapping_add(1);
        
        let info = HandleInfo {
            ino,
            pid,
            opened_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            flags,
        };
        
        self.handles.insert(handle, info);
        handle
    }
    
    /// Record a file handle close (for monitoring)
    pub fn close(&mut self, handle: u64) -> Option<HandleInfo> {
        self.handles.remove(&handle)
    }
    
    /// Get handle info
    pub fn get(&self, handle: u64) -> Option<&HandleInfo> {
        self.handles.get(&handle)
    }
    
    /// Number of currently open handles
    pub fn count(&self) -> usize {
        self.handles.len()
    }
    
    /// Check if a handle is open
    pub fn is_open(&self, handle: u64) -> bool {
        self.handles.contains_key(&handle)
    }
    
    /// Get all handles for an inode (for debugging)
    pub fn for_inode(&self, ino: u64) -> Vec<u64> {
        self.handles
            .iter()
            .filter(|(_, info)| info.ino == ino)
            .map(|(&handle, _)| handle)
            .collect()
    }
    
    /// Clear closed handles (for memory management)
    pub fn clear_closed(&mut self) {
        // Note: In a real implementation, we'd track which handles
        // are actually closed vs still open. For now, this is a placeholder.
    }
}

pub type SharedHandleTracker = Arc<RwLock<HandleTracker>>;

pub fn create_handle_tracker() -> SharedHandleTracker {
    Arc::new(RwLock::new(HandleTracker::new()))
}