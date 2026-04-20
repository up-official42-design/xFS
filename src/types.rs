//! Type wrappers for improved type safety
//! 
//! These NewType wrappers help prevent mixing up identifiers like
//! passing a Hash where an InodeId is expected.

use std::fmt;

/// Inode identifier wrapper
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct InodeId(pub u64);

impl InodeId {
    pub fn new(val: u64) -> Self {
        Self(val)
    }
    
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for InodeId {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<InodeId> for u64 {
    fn from(v: InodeId) -> Self {
        v.0
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Content hash wrapper
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    pub fn new(data: [u8; 32]) -> Self {
        Self(data)
    }
    
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 32]
    }
}

impl From<[u8; 32]> for Hash {
    fn from(v: [u8; 32]) -> Self {
        Self(v)
    }
}

impl From<Hash> for [u8; 32] {
    fn from(v: Hash) -> Self {
        v.0
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

/// File/link count wrapper  
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LinkCount(pub u32);

impl LinkCount {
    pub fn new(val: u32) -> Self {
        Self(val)
    }
    
    pub fn as_u32(self) -> u32 {
        self.0
    }
    
    pub fn increment(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
    
    pub fn decrement(self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }
}

impl From<u32> for LinkCount {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<LinkCount> for u32 {
    fn from(v: LinkCount) -> Self {
        v.0
    }
}

/// Chunk reference count wrapper
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChunkRefCount(pub u32);

impl ChunkRefCount {
    pub fn new(val: u32) -> Self {
        Self(val)
    }
    
    pub fn as_u32(self) -> u32 {
        self.0
    }
    
    pub fn increment(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
    
    pub fn decrement(self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }
}

impl From<u32> for ChunkRefCount {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<ChunkRefCount> for u32 {
    fn from(v: ChunkRefCount) -> Self {
        v.0
    }
}