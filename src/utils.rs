#[cfg(unix)]
use std::os::unix::prelude::AsRawFd;
use std::{
    fs::{self, File},
    io::{self, IoSlice, Write},
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use crate::{
    shim::{
        sync::{self, atomic::AtomicBool, mpsc},
        thread,
    },
    Error,
};

pub fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(&x, &y)| x == y).count()
}

pub fn fallocate(file: &File, len: u64) -> Result<(), io::Error> {
    fail::fail_point!("ftruncate", |s| Err(io::Error::new(
        io::ErrorKind::Other,
        format!("failpoint ftruncate {:?}", s)
    )));
    #[cfg(not(miri))]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // Use fallocate instead of posix_fallocate to avoid writing zeroes on COW filesystems
        let res = unsafe { libc::fallocate64(file.as_raw_fd(), 0, 0, len.try_into().unwrap()) };
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
        let res = libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &fstore);
        match nix::Error::result(res) {
            Ok(_) => (), // we still need to call ftruncate (set_len) below
            Err(nix::errno::Errno::ENOTSUP) => (),
            Err(e) => return Err(e.into()),
        }
    }
    file.set_len(len)?;
    Ok(())
}

#[allow(unused_variables)]
pub fn fadvise_wont_need(f: &File, len: u64) -> io::Result<()> {
    #[cfg(any(target_os = "android", target_os = "linux"))]
    {
        let _ = nix::fcntl::posix_fadvise(
            f.as_raw_fd(),
            0,
            len as _,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
        );
    }
    Ok(())
}

#[allow(unused_variables)]
pub fn fadvise_read_ahead(file: &File, read_ahead: bool) -> io::Result<()> {
    #[cfg(any(target_os = "android", target_os = "linux"))]
    let _ = nix::fcntl::posix_fadvise(
        file.as_raw_fd(),
        0,
        0,
        if read_ahead {
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL
        } else {
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_RANDOM
        },
    );
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    unsafe {
        let _ = libc::fcntl(file.as_raw_fd(), libc::F_RDAHEAD, read_ahead as libc::c_int);
    }
    Ok(())
}

#[allow(unused_variables)]
pub fn fsync_range(file: &File, sync_offset: u64, sync_len: u64) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            sync_offset as _,
            sync_len as _,
            libc::SYNC_FILE_RANGE_WRITE,
        );
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let _ = file.sync_data();
    Ok(())
}

/// Atomically writes the file by first writing to a .tmp sibling file and then renaming.
/// The file contents are fsynced but not its parent directory.
pub fn atomic_file_write(path: &Path, contents: &[u8]) -> io::Result<()> {
    let ext = [
        path.extension()
            .unwrap_or_default()
            .to_string_lossy()
            .as_ref(),
        ".tmp",
    ]
    .concat();
    let tmp_path = path.with_extension(ext);
    let mut f = File::create(&tmp_path)?;
    f.write_all(contents)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp_path, path)
}

pub fn create_direct_io_file(path: &Path) -> io::Result<File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true).create(true).truncate(false);
    #[cfg(any(target_os = "android", target_os = "linux"))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags((nix::fcntl::OFlag::O_DIRECT | nix::fcntl::OFlag::O_DSYNC).bits());
    }
    opts.open(path)
}

pub fn sync_dir(path: &Path) -> io::Result<()> {
    fail::fail_point!("fsync", |s| Err(io::Error::new(
        io::ErrorKind::Other,
        format!("failpoint fsync {:?}", s)
    )));
    #[cfg(unix)]
    {
        // On unix directory opens must be read only.
        fs::File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        // On windows we must use FILE_FLAG_BACKUP_SEMANTICS to get a handle to the file
        // From: https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
        use std::os::windows::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)?
            .sync_all()
    }
}

#[derive(Debug, Deref, DerefMut)]
pub struct FileWithPath {
    #[deref]
    #[deref_mut]
    pub file: File,
    pub path: PathBuf,
}

pub trait FileExt {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn read_exact_at_to_vec(&self, vec: &mut Vec<u8>, offset: u64, len: usize) -> io::Result<()>;
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn write_all_vectored_at(&self, bufs: &mut [IoSlice<'_>], offset: u64) -> io::Result<()>;
}

impl FileExt for std::fs::File {
    #[cfg(unix)]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        fail::fail_point!("fread", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fread {:?}", s)
        )));
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        fail::fail_point!("fread", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fread {:?}", s)
        )));
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

    fn read_exact_at_to_vec(
        &self,
        vec: &mut Vec<u8>,
        offset: u64,
        read_len: usize,
    ) -> io::Result<()> {
        fail::fail_point!("fread", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fread {:?}", s)
        )));
        vec.reserve_exact(read_len);
        unsafe {
            // Safety: the methods and syscalls involved are safe to use with uninitialized data
            // The trait read_exact_at handles the slice argument like a
            // ReadBuf from https://rust-lang.github.io/rfcs/2930-read-buf.html
            // and will eventually be replaced with it.
            let uninit_slice: &mut [MaybeUninit<u8>] = &mut vec.spare_capacity_mut()[..read_len];
            // Replaces the unstable method MaybeUninit::slice_assume_init_mut
            let assumed_init: &mut [u8] =
                &mut *(uninit_slice as *mut [MaybeUninit<u8>] as *mut [u8]);
            self.read_exact_at(assumed_init, offset)?;
            vec.set_len(vec.len() + read_len);
        }
        Ok(())
    }

    #[cfg(unix)]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        fail::fail_point!("fwrite", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fwrite {:?}", s)
        )));
        std::os::unix::fs::FileExt::write_all_at(self, buf, offset)
    }

    #[cfg(all(unix, not(miri)))]
    fn write_all_vectored_at(&self, mut bufs: &mut [IoSlice<'_>], offset: u64) -> io::Result<()> {
        fail::fail_point!("fwrite", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fwrite {:?}", s)
        )));
        const MAX_VECTORS: usize = 128;
        while !bufs.is_empty() {
            let result = nix::Error::result(unsafe {
                libc::pwritev(
                    self.as_raw_fd(),
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

    #[cfg(any(not(unix), miri))]
    fn write_all_vectored_at(&self, bufs: &mut [IoSlice<'_>], mut offset: u64) -> io::Result<()> {
        fail::fail_point!("fwrite", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fwrite {:?}", s)
        )));
        for buf in bufs {
            self.write_all_at(buf, offset)?;
            offset += buf.len() as u64;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        fail::fail_point!("fwrite", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fwrite {:?}", s)
        )));
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
}

/// Replacement for the unstable method Vec::extract_if
/// but might shuffle the original vector and also returns the items in arbitrary order.
pub fn vec_drain_if<T>(vec: &mut Vec<T>, mut cond: impl FnMut(&T) -> bool) -> std::vec::Drain<T> {
    assert_ne!(size_of::<T>(), 0);
    let mut slice = vec.as_mut_slice();
    while let Some((head, tail)) = slice.split_first_mut() {
        slice = if cond(head) {
            if let Some(tail_last) = tail.last_mut() {
                std::mem::swap(head, tail_last);
            }
            let len: usize = slice.len();
            &mut slice[..len - 1]
        } else {
            &mut slice[1..]
        };
    }
    let slice_offset = (slice.as_ptr() as usize - vec.as_ptr() as usize) / size_of::<T>();
    vec.drain(slice_offset..)
}

/// Automatically calls the enclosed function on panics
pub struct FnTrap<T: FnOnce()>(Option<T>);

impl<T: FnOnce()> FnTrap<T> {
    #[must_use]
    #[inline]
    pub fn new(f: T) -> Self {
        Self(Some(f))
    }

    #[inline]
    pub fn disarm(mut self) {
        self.0.take().unwrap();
    }
}

impl<T: FnOnce()> Drop for FnTrap<T> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

/// Automatically aborts the transaction on unhandled errors
#[derive(Debug, Default)]
pub struct Trap(AtomicBool);

pub struct TrapGuard<'a>(Option<&'a AtomicBool>);

impl Trap {
    #[inline]
    pub fn setup(&self) -> Result<TrapGuard, Error> {
        self.check()?;
        Ok(TrapGuard(Some(&self.0)))
    }

    #[inline]
    pub fn check(&self) -> Result<(), Error> {
        if !self.0.load(sync::atomic::Ordering::Relaxed) {
            Ok(())
        } else {
            Err(Error::TransactionAborted)
        }
    }
}

impl TrapGuard<'_> {
    #[inline]
    pub fn disarm(mut self) {
        debug_assert!(self.0.is_some());
        self.0 = None;
    }
}

impl Drop for TrapGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(arc) = &self.0 {
            arc.store(true, sync::atomic::Ordering::Relaxed);
        }
    }
}

pub trait TrapResult {
    fn guard_trap(self, guard: TrapGuard) -> Self;
}

impl<T, E> TrapResult for Result<T, E> {
    #[inline]
    fn guard_trap(self, guard: TrapGuard) -> Self {
        if self.is_ok() {
            guard.disarm()
        }
        self
    }
}

#[derive(Debug)]
pub struct SharedJoinHandle<T> {
    handle: sync::Mutex<Option<thread::JoinHandle<T>>>,
    tx: sync::mpsc::SyncSender<()>,
}

impl<T> SharedJoinHandle<T> {
    pub fn spawn<F>(name: String, f: F) -> Self
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel(0);
        let handle = thread::Builder::new()
            .name(name)
            .spawn(move || {
                let result = f();
                drop(rx);
                result
            })
            .unwrap();
        Self {
            handle: sync::Mutex::new(Some(handle)),
            tx,
        }
    }

    pub fn join(&self) -> Option<T> {
        let _ = self.tx.send(());
        let mut locked_handle = self.handle.lock().unwrap();
        let thread_handle = locked_handle.take()?;
        Some(thread_handle.join().unwrap())
    }
}

#[derive(Debug)]
pub struct WaitJoinHandle<T>(Option<thread::JoinHandle<T>>);

impl<T> WaitJoinHandle<T> {
    pub fn new(handle: thread::JoinHandle<T>) -> Self {
        Self(Some(handle))
    }
}

impl<T> Drop for WaitJoinHandle<T> {
    fn drop(&mut self) {
        if let Some(t) = self.0.take() {
            let _ = t.join();
        }
    }
}

impl<T> WaitJoinHandle<T> {
    pub fn join(mut self) -> thread::Result<T> {
        self.0.take().unwrap().join()
    }
}

#[derive(Display, PartialEq, Eq)]
#[display("{:?}", self)]
/// Outputs bytes as escaped ascii strings
pub struct EscapedBytes<'a>(pub &'a [u8]);

impl std::fmt::Debug for EscapedBytes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut end_zeroes = 0;
        if cfg!(any(fuzzing, test)) {
            end_zeroes = self.0.iter().rev().take_while(|b| **b == 0).count();
            if end_zeroes <= 5 {
                end_zeroes = 0;
            }
        }
        for &b in &self.0[..self.0.len() - end_zeroes] {
            write!(f, "{}", std::ascii::escape_default(b))?
        }
        if end_zeroes != 0 {
            write!(f, "…\\0*{end_zeroes}")?;
        }
        Ok(())
    }
}

#[derive(Display)]
#[display("{:?}", self)]
/// Outputs bytes sizes as human sizes
pub struct ByteSize(pub u64);

impl std::fmt::Debug for ByteSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * 1024;
        const GB: u64 = 1024 * 1024 * 1024;
        let (value, suffix) = match self.0 {
            v @ 0..KB => (v as f64, "B"),
            v @ KB..MB => (v as f64 / KB as f64, "KB"),
            v @ MB..GB => (v as f64 / MB as f64, "MB"),
            v @ GB.. => (v as f64 / GB as f64, "GB"),
        };
        write!(f, "{value:.3}{suffix}")
    }
}

pub trait CellExt<T> {
    fn reset<F: FnOnce(T) -> T>(&self, f: F);
}

impl<T: Copy> CellExt<T> for std::cell::Cell<T> {
    #[inline]
    fn reset<F: FnOnce(T) -> T>(&self, f: F) {
        let mut v = self.get();
        v = f(v);
        self.set(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract() {
        let mut numbers = vec![1, 2, 3, 4, 5, 6, 8, 9, 11, 13, 14, 15];
        let mut evens = vec_drain_if(&mut numbers, |x| *x % 2 == 0).collect::<Vec<_>>();
        let mut odds = numbers;
        evens.sort_unstable();
        odds.sort_unstable();
        assert_eq!(evens, vec![2, 4, 6, 8, 14]);
        assert_eq!(odds, vec![1, 3, 5, 9, 11, 13, 15]);
    }
}
