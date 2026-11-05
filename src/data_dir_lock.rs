//! Process-lifetime ownership of a Durable Streams data directory.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// A non-blocking advisory lock held for as long as the server owns `data_dir`.
/// Keeping the file open is intentional: closing it releases the OS lock.
pub struct DataDirLock {
    _file: File,
}

impl DataDirLock {
    pub fn acquire(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        Self::lock(data_dir, true)
    }

    /// Lock a data directory whose lock file a server already created, creating
    /// nothing: offline tools must leave a directory they only inspect unchanged,
    /// and must not leave a root-owned lock file a non-root server cannot open.
    pub fn acquire_existing(data_dir: &Path) -> io::Result<Self> {
        Self::lock(data_dir, false)
    }

    fn lock(data_dir: &Path, create: bool) -> io::Result<Self> {
        let path = data_dir.join(".durable-streams.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(&path)
            .map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    io::Error::new(
                        error.kind(),
                        format!(
                            "{} does not exist; no server has opened this data directory",
                            path.display()
                        ),
                    )
                } else {
                    error
                }
            })?;

        #[cfg(unix)]
        {
            // SAFETY: `file` remains open in this value until process shutdown.
            let rc = unsafe {
                libc::flock(
                    std::os::fd::AsRawFd::as_raw_fd(&file),
                    libc::LOCK_EX | libc::LOCK_NB,
                )
            };
            if rc != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("data directory is already locked: {}", path.display()),
                ));
            }
        }

        #[cfg(not(unix))]
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "exclusive data-directory locking requires Unix",
            ));
        }

        Ok(Self { _file: file })
    }
}
