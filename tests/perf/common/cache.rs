//! Cold-cache preparation: posix_fadvise(DONTNEED) + file discovery.

use std::io;
use std::path::{Path, PathBuf};

/// Advise kernel to drop page cache for this file.
/// Unix: calls posix_fadvise(POSIX_FADV_DONTNEED). Returns Ok even if the
/// kernel silently declines (e.g., file has dirty pages).
/// Non-unix: no-op returning Ok.
#[cfg(unix)]
pub fn posix_fadvise_dontneed(path: &Path) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let file = std::fs::File::open(path)?;
    let fd = file.as_raw_fd();
    // safety: fd is valid for the duration of the call
    let rc = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
    if rc == 0 { Ok(()) } else { Err(io::Error::from_raw_os_error(rc)) }
}

#[cfg(not(unix))]
pub fn posix_fadvise_dontneed(path: &Path) -> io::Result<()> {
    let _ = std::fs::metadata(path)?;
    Ok(())
}

/// Enumerate data files under a Thunder database directory.
/// Recursive scan; returns all *.bin files.
pub fn collect_data_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(dir) { Ok(e) => e, Err(_) => return out };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(collect_data_files(&path));
        } else if path.extension().map(|e| e == "bin").unwrap_or(false) {
            out.push(path);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;

    #[test]
    fn fadvise_on_nonexistent_returns_err() {
        let r = posix_fadvise_dontneed(Path::new("/tmp/definitely_not_real_xyzabc_9876.bin"));
        assert!(r.is_err());
    }

    #[test]
    fn fadvise_on_real_file_succeeds() {
        let tmp = std::env::temp_dir().join("thunderdb_cache_test.bin");
        let mut f = File::create(&tmp).unwrap();
        f.write_all(b"hello world").unwrap();
        drop(f);
        let r = posix_fadvise_dontneed(&tmp);
        let _ = fs::remove_file(&tmp);
        assert!(r.is_ok(), "fadvise failed: {:?}", r);
    }

    #[test]
    fn collect_data_files_recurses_bin_files() {
        let tmp = std::env::temp_dir().join("thunderdb_collect_test");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        fs::create_dir_all(tmp.join("table_a")).unwrap();
        File::create(tmp.join("table_a").join("pages.bin")).unwrap();
        File::create(tmp.join("config.json")).unwrap();
        let found = collect_data_files(&tmp);
        fs::remove_dir_all(&tmp).unwrap();
        assert_eq!(found.len(), 1);
        assert!(found[0].ends_with("pages.bin"));
    }
}
