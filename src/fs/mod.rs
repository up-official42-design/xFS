pub mod attr;
pub mod cache;
pub mod dir;
pub mod file;
pub mod fuse;
pub mod link;

use std::time::Duration;

pub const TTL: Duration = Duration::from_secs(1);

pub use fuse::XFS;
