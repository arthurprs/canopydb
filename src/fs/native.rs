use std::{
    fs::{self, File},
    io::{self, IoSlice},
    os::fd::AsRawFd,
    path::Path,
    sync::Arc,
};

use super::*;

pub struct NativeFile(pub File);

impl FsFile for NativeFile {
    #[cfg(unix)]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        fail::fail_point!("fread", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fread {:?}", s)
        )));
        std::os::unix::fs::FileExt::read_exact_at(&self.0, buf, offset)
    }
    #[cfg(not(unix))]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match std::os::windows::fs::FileExt::seek_read(self, buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        } else {
            Ok(())
        }
    }
    #[cfg(unix)]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        fail::fail_point!("fwrite", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fwrite {:?}", s)
        )));
        std::os::unix::fs::FileExt::write_all_at(&self.0, buf, offset)
    }
    #[cfg(not(unix))]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match std::os::windows::fs::FileExt::seek_write(self, buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    #[cfg(unix)]
    fn write_all_vectored_at(&self, mut bufs: &mut [IoSlice<'_>], offset: u64) -> io::Result<()> {
        fail::fail_point!("fwrite", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fwrite {:?}", s)
        )));
        const MAX_VECTORS: usize = 128;
        while !bufs.is_empty() {
            let result = nix::Error::result(unsafe {
                libc::pwritev(
                    self.0.as_raw_fd(),
                    // Safety: This transmute is guaranteed to work on unix system, see IoSlice documentation.
                    bufs.as_ptr().cast(),
                    std::cmp::min(bufs.len(), MAX_VECTORS) as libc::c_int,
                    offset as libc::off_t,
                )
            })
            .map_err(io::Error::from);
            match result {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => IoSlice::advance_slices(&mut bufs, n as usize),
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    fn set_len(&self, size: u64) -> io::Result<()> {
        fail::fail_point!("ftruncate", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint ftruncate {:?}", s)
        )));
        self.0.set_len(size)
    }
    fn len(&self) -> io::Result<u64> {
        self.0.metadata().map(|m| m.len())
    }
    fn sync_all(&self) -> io::Result<()> {
        self.0.sync_all()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }
    fn fallocate(&self, len: u64) -> io::Result<()> {
        fail::fail_point!("ftruncate", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint ftruncate {:?}", s)
        )));
        #[cfg(not(miri))]
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            // Use fallocate instead of posix_fallocate to avoid writing zeroes on COW filesystems
            let res =
                unsafe { libc::fallocate64(self.0.as_raw_fd(), 0, 0, len.try_into().unwrap()) };
            match nix::Error::result(res) {
                Ok(_) => return Ok(()),
                Err(nix::errno::Errno::ENOTSUP) => (),
                Err(e) => return Err(e.into()),
            }
        }
        #[cfg(not(miri))]
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        unsafe {
            let fstore = libc::fstore_t {
                fst_flags: libc::F_ALLOCATEALL, // Allocate all requested space or no space at all.
                fst_posmode: libc::F_PEOFPOSMODE, // Allocate from the physical end of file.
                fst_offset: 0,
                fst_length: len.try_into().unwrap(),
                fst_bytesalloc: 0, // output size, can be > length
            };
            let res = libc::fcntl(self.0.as_raw_fd(), libc::F_PREALLOCATE, &fstore);
            match nix::Error::result(res) {
                Ok(_) => (), // we still need to call ftruncate (set_len) below
                Err(nix::errno::Errno::ENOTSUP) => (),
                Err(e) => return Err(e.into()),
            }
        }
        self.0.set_len(len)?;
        Ok(())
    }
    fn fadvise(&self, offset: u64, len: u64, advice: FileAdvice) -> io::Result<()> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            use nix::fcntl::{posix_fadvise, PosixFadviseAdvice};
            let adv = match advice {
                FileAdvice::Normal => PosixFadviseAdvice::POSIX_FADV_NORMAL,
                FileAdvice::Sequential => PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
                FileAdvice::Random => PosixFadviseAdvice::POSIX_FADV_RANDOM,
                FileAdvice::WillNeed => PosixFadviseAdvice::POSIX_FADV_WILLNEED,
                FileAdvice::DontNeed => PosixFadviseAdvice::POSIX_FADV_DONTNEED,
                FileAdvice::NoReuse => PosixFadviseAdvice::POSIX_FADV_NOREUSE,
            };
            posix_fadvise(self.0.as_raw_fd(), offset as i64, len as i64, adv)
                .map_err(|e| io::Error::from_raw_os_error(e as i32))
        }
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        unsafe {
            match advice {
                FileAdvice::Sequential | FileAdvice::Random => {
                    let read_ahead = matches!(advice, FileAdvice::Sequential);
                    let _ = libc::fcntl(
                        self.0.as_raw_fd(),
                        libc::F_RDAHEAD,
                        read_ahead as libc::c_int,
                    );
                }
            };
            Ok(())
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "android",
            target_os = "macos",
            target_os = "ios"
        )))]
        {
            Ok(())
        }
    }
    fn fsync_range(&self, offset: u64, len: u64) -> io::Result<()> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            unsafe {
                libc::sync_file_range(
                    self.0.as_raw_fd(),
                    offset as _,
                    len as _,
                    libc::SYNC_FILE_RANGE_WRITE,
                );
            }
            Ok(())
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            self.0.sync_data()
        }
    }
}

pub struct NativeFS;

impl FileSystem for NativeFS {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path)?;
        Ok(Arc::new(NativeFile(file)))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        Ok(Arc::new(NativeFile(file)))
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }
    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<String>> {
        fs::read_dir(path)?
            .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect()
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            std::fs::File::open(path)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
            use std::os::windows::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(path)?
                .sync_all()
        }
    }
}
