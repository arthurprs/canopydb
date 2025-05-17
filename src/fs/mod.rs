use std::{
    io::{self, IoSlice},
    path::Path,
    sync::Arc,
};

mod mem;
mod native;

#[derive(Debug, Clone, Copy)]
pub enum FileAdvice {
    Normal,
    Sequential,
    Random,
    WillNeed,
    DontNeed,
    NoReuse,
}

pub trait FsFile: Send + Sync {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;

    fn read_exact_at_to_vec(
        &self,
        vec: &mut Vec<u8>,
        offset: u64,
        read_len: usize,
    ) -> io::Result<()> {
        vec.reserve_exact(read_len);
        unsafe {
            let uninit_slice: &mut [std::mem::MaybeUninit<u8>] =
                &mut vec.spare_capacity_mut()[..read_len];
            let assumed_init: &mut [u8] =
                &mut *(uninit_slice as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]);
            self.read_exact_at(assumed_init, offset)?;
            vec.set_len(vec.len() + read_len);
        }
        Ok(())
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn write_all_vectored_at(&self, bufs: &mut [IoSlice<'_>], offset: u64) -> io::Result<()> {
        for buf in bufs.iter() {
            self.write_all_at(buf, offset)?;
        }
        Ok(())
    }
    fn set_len(&self, size: u64) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn sync_all(&self) -> io::Result<()>;
    fn sync_data(&self) -> io::Result<()> {
        self.sync_all()
    }
    fn fallocate(&self, len: u64) -> io::Result<()> {
        self.set_len(len)
    }
    fn fadvise(&self, _offset: u64, _len: u64, _advice: FileAdvice) -> io::Result<()> {
        Ok(())
    }
    fn fsync_range(&self, _offset: u64, _len: u64) -> io::Result<()> {
        self.sync_data()
    }
}

pub trait FileSystem: Send + Sync {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn FsFile>>;
    fn create(&self, path: &Path) -> io::Result<Arc<dyn FsFile>>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn exists(&self, path: &Path) -> bool;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;
    fn read_dir(&self, path: &Path) -> io::Result<Vec<String>>;
    fn sync_dir(&self, path: &Path) -> io::Result<()>;
    fn atomic_file_write(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        let ext = [
            path.extension()
                .unwrap_or_default()
                .to_string_lossy()
                .as_ref(),
            ".tmp",
        ]
        .concat();
        let tmp_path = path.with_extension(ext);
        let f = self.create(&tmp_path)?;
        f.write_all_at(contents, 0)?;
        f.sync_all()?;
        drop(f);
        self.rename(&tmp_path, path)
    }
}
