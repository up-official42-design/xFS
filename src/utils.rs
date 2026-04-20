use std::ffi::OsStr;

/// Converts an OsStr to a valid filename String.
/// Returns None for empty names.
/// Uses lossy conversion for non-UTF8 names (preserves data with replacement characters).
pub fn get_valid_name(name: &OsStr) -> Option<String> {
    if name.is_empty() {
        return None;
    }
    let s = name.to_string_lossy();
    if s.is_empty() {
        return None;
    }
    Some(s.into_owned())
}

/// Returns current Unix timestamp
pub fn now_ts() -> u64 {
    chrono::Utc::now().timestamp() as u64
}
