use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    hash::Hasher,
    io::{self, BufRead, BufReader, IoSlice, Read, Seek, Write},
    mem::size_of,
    ops::Range,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use smallvec::SmallVec;
use triomphe::Arc;
use zerocopy::*;

use crate::{
    error::{io_map, Error},
    group_commit::GroupCommit,
    shim::{
        parking_lot::{Condvar, Mutex, RwLock, RwLockUpgradableReadGuard},
        sync::atomic::{self, AtomicBool},
        thread,
    },
    utils::{self, ByteSize, FileExt, FileWithPath},
};

pub use crate::repr::WalIdx;

const WAL_HEADER_MAGIC: u64 = 0xFA1A511793959196;
const WAL_FOOTER_MAGIC: u64 = 0xF35C835A2600946B;
const HEADER_BYTES_IGNORED_BY_CHECKSUM: usize = size_of::<u64>() * 2;
const INITIAL_WAL_IDX: u64 = 0;

#[derive(Default, Copy, Debug, Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct CommitHeader {
    magic: u64,
    checksum: u64,
    total_byte_len: u64,
    first_item_idx: u64,
    items_byte_len: u64,
    items_len: u64,
}

#[derive(Default, Copy, Debug, Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct CommitFooter {
    magic: u64,
    checksum: u64,
}

#[derive(Debug)]
pub struct Options {
    pub use_direct_io: bool,
    pub use_blocks: bool,
    pub min_file_size: u64,
    pub max_file_size: u64,
    pub max_iter_mem: usize,
    pub group_commit_wait: Option<Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            use_blocks: false,
            use_direct_io: false,
            min_file_size: 1024 * 1024,
            max_file_size: 256 * 1024 * 1024,
            max_iter_mem: 4 * 1024 * 1024,
            group_commit_wait: None,
        }
    }
}

pub trait WalRead: BufRead + Send {
    fn is_incremental(&self) -> bool;
    fn len(&mut self) -> io::Result<u64>;
}

impl WalRead for &[u8] {
    fn is_incremental(&self) -> bool {
        false
    }

    fn len(&mut self) -> io::Result<u64> {
        Ok(<[u8]>::len(self) as u64)
    }
}

impl<T: Read + Seek + Send> WalRead for io::BufReader<T> {
    fn is_incremental(&self) -> bool {
        true
    }

    fn len(&mut self) -> io::Result<u64> {
        use std::io::SeekFrom;
        let old_pos = self.stream_position()?;
        let len = self.seek(SeekFrom::End(0))?;
        self.seek(SeekFrom::Start(old_pos))?;
        Ok(len)
    }
}

impl WalRead for io::Cursor<Vec<u8>> {
    fn is_incremental(&self) -> bool {
        false
    }

    fn len(&mut self) -> io::Result<u64> {
        use std::io::SeekFrom;
        let old_pos = self.position();
        let len = self.seek(SeekFrom::End(0))?;
        self.seek(SeekFrom::Start(old_pos))?;
        Ok(len)
    }
}

/// Group fsync state
#[derive(Debug, Default)]
struct SyncState {
    /// First index that is un-synced (exclusive)
    synced_up_to: u64,
    /// First index that is being synced (exclusive)
    pending_sync_up_to: u64,
    /// Last file index that synced in the parent dir (inclusive)
    last_file_dir_fsync: Option<u64>,
}

#[derive(Debug)]
pub struct Wal {
    halted: AtomicBool,
    wal_prefix: String,
    dir: PathBuf,
    files: RwLock<BTreeMap<u64, RwLock<WalFile>>>,
    group_commit: GroupCommit,
    sync_state: Mutex<SyncState>,
    sync_condvar: Condvar,
    #[debug(skip)]
    buffer: Mutex<Vec<Block>>,
    options: Options,
}

#[derive(Debug)]
struct WalFile {
    file: Arc<FileWithPath>,
    direct_io_file: Option<File>,
    /// The file Idx (idx of first item)
    range: Range<WalIdx>,
    /// Current write position
    offset: u64,
    /// Backing file len
    /// Note that in failure cases it can be < the actual file length
    file_len: u64,
    advise_offset: u64,
}

struct WalFileIter {
    max_mem: usize,
    file: Arc<FileWithPath>,
    end_offset: u64,
    range: Range<u64>,
    commit_offset: u64,
    next_commit_offset: u64,
    next_item_offset: u64,
    next_item_idx: WalIdx,
    next_item_lens: VecDeque<u64>,
    hasher_buffer: Vec<u8>,
    done: bool,
}

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, align(4096))]
struct Block([u8; 4096]);

impl Default for Block {
    fn default() -> Self {
        Self([0u8; 4096])
    }
}

const BLOCK_SIZE_U64: u64 = 4096;
const HASH_BUFFER_SIZE: usize = 32 * 1024;

fn ceil_block_offset(offset: u64) -> u64 {
    offset.div_ceil(BLOCK_SIZE_U64) * BLOCK_SIZE_U64
}

impl WalFileIter {
    fn load_next_commit(&mut self) -> io::Result<()> {
        // commit previously yielded offset & end
        self.commit_offset = self.next_commit_offset;
        self.range.end = self.next_item_idx;
        if self.commit_offset >= self.end_offset {
            return Ok(());
        }

        let mut header = CommitHeader::default();
        let mut offset = self.commit_offset;
        self.file.read_exact_at(header.as_mut_bytes(), offset)?;
        offset += header.as_bytes().len() as u64;

        if header.magic != WAL_HEADER_MAGIC {
            return Err(if header.as_bytes().iter().all(|b| *b == 0) {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Reached a zero filled area, possibly end of stream",
                )
            } else {
                io::Error::new(io::ErrorKind::InvalidData, "Wrong header magic number")
            });
        }
        if header.first_item_idx != self.range.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unexpected commit sequence",
            ));
        }

        self.next_item_lens.resize(header.items_len as usize, 0);
        let items_lens = self.next_item_lens.make_contiguous();
        self.file.read_exact_at(items_lens.as_mut_bytes(), offset)?;
        offset += items_lens.as_bytes().len() as u64;

        if items_lens.iter().copied().sum::<u64>() != header.items_byte_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid item offsets",
            ));
        }

        let calc_total_byte_len =
            offset - self.commit_offset + header.items_byte_len + size_of::<CommitFooter>() as u64;
        if calc_total_byte_len != header.total_byte_len
            && ceil_block_offset(calc_total_byte_len) != header.total_byte_len
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid byte lengths",
            ));
        }

        let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
        hasher.write(&header.as_bytes()[HEADER_BYTES_IGNORED_BY_CHECKSUM..]);
        hasher.write(items_lens.as_bytes());
        if hasher.finish() != header.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Header checksum mismatch",
            ));
        }

        self.next_item_offset = offset;
        let mut left_to_hash = header.items_byte_len;
        while left_to_hash != 0 {
            self.hasher_buffer.clear();
            let read_len = left_to_hash.min(self.hasher_buffer.capacity() as u64);
            self.file
                .read_exact_at_to_vec(&mut self.hasher_buffer, offset, read_len as usize)?;
            hasher.write(&self.hasher_buffer);
            offset += read_len;
            left_to_hash -= read_len;
        }
        let mut footer = CommitFooter::default();
        self.file.read_exact_at(footer.as_mut_bytes(), offset)?;

        if footer.magic != WAL_FOOTER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Wrong footer magic number",
            ));
        }
        if hasher.finish() != footer.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Footer checksum mismatch",
            ));
        }

        debug_assert!(offset - self.commit_offset <= header.total_byte_len);
        self.next_commit_offset += header.total_byte_len;
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for WalFileIter {
    fn drop(&mut self) {
        let _ = utils::fadvise_wont_need(&self.file, 0);
    }
}

impl Iterator for WalFileIter {
    type Item = io::Result<(WalIdx, Box<dyn Read>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if self.next_item_lens.is_empty() {
            if let Err(e) = self.load_next_commit() {
                self.done = true;
                return Some(Err(e));
            }
            if self.next_item_lens.is_empty() {
                self.done = true;
                return None;
            }
        }

        let item_len = self.next_item_lens.pop_front().unwrap();
        let item_offset = self.next_item_offset;
        let item_idx = self.next_item_idx;
        self.next_item_offset += item_len;
        self.next_item_idx += 1;

        let item_read = || -> io::Result<Box<dyn Read>> {
            if item_len as usize <= self.max_mem {
                let mut buf = Vec::new();
                self.file
                    .read_exact_at_to_vec(&mut buf, item_offset, item_len as usize)?;
                Ok(Box::new(io::Cursor::new(buf)))
            } else {
                let mut file = File::try_clone(&self.file)?;
                file.seek(io::SeekFrom::Start(item_offset))?;
                Ok(Box::new(BufReader::new(file.take(item_len))))
            }
        }();
        Some(item_read.map(|i| (item_idx, i)))
    }
}

impl WalFile {
    fn new(options: &Options, first_idx: u64, path: PathBuf) -> io::Result<WalFile> {
        fail::fail_point!("fopen", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fopen {:?}", s)
        )));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let file_len = file.metadata()?.len();
        let mut direct_io_file = None;
        if options.use_direct_io {
            direct_io_file = Some(utils::create_direct_io_file(&path)?);
        }
        Ok(WalFile {
            range: first_idx..first_idx,
            file: Arc::new(FileWithPath { file, path }),
            direct_io_file,
            offset: 0,
            file_len,
            advise_offset: 0,
        })
    }

    fn file_for_read(&self) -> io::Result<Arc<FileWithPath>> {
        let file = self.file.clone();
        // Hint OS to perform liberal read ahead
        // This may also affect the write file descriptor but maybe it doesn't matter.
        utils::fadvise_read_ahead(&file, true)?;
        Ok(file)
    }

    fn file_for_sync(&self) -> io::Result<Arc<FileWithPath>> {
        Ok(self.file.clone())
    }

    fn target_advise_offset(&self) -> u64 {
        if self.offset >= self.file_len {
            self.file_len
        } else {
            const ONEMB_U64: u64 = 1024 * 1024;
            self.file_len / ONEMB_U64 * ONEMB_U64
        }
    }

    fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    fn iter(&self, options: &Options) -> io::Result<WalFileIter> {
        fail::fail_point!("fopen", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint fopen {:?}", s)
        )));
        Ok(WalFileIter {
            max_mem: options.max_iter_mem,
            file: self.file_for_read()?,
            end_offset: self.offset,
            commit_offset: 0,
            range: self.range.start..self.range.start,
            next_commit_offset: 0,
            next_item_idx: self.range.start,
            next_item_lens: Default::default(),
            next_item_offset: Default::default(),
            hasher_buffer: Vec::with_capacity(HASH_BUFFER_SIZE),
            done: false,
        })
    }

    fn write(
        this: RwLockUpgradableReadGuard<Self>,
        options: &Options,
        buffer: &mut Vec<Block>,
        items: &mut [&mut dyn WalRead],
    ) -> io::Result<Option<WalIdx>> {
        let header_size = size_of::<CommitHeader>() + items.len() * size_of::<u64>();
        let footer_size = size_of::<CommitFooter>();
        let mut incremental_items = false;
        let mut items_total_size = 0;
        for item in items.iter_mut() {
            items_total_size += item.len()?;
            incremental_items |= item.is_incremental();
        }
        let mut needed_size = (header_size + footer_size) as u64 + items_total_size;
        if options.use_blocks {
            needed_size = ceil_block_offset(needed_size);
        }
        let new_file_len;
        {
            let free_space = this.file_len - this.offset;
            if needed_size > free_space {
                if this.offset == 0 || this.offset + needed_size <= options.max_file_size {
                    let exp_growth = (this.file_len * 2).min(options.max_file_size);
                    let min_size = (this.offset + needed_size).max(options.min_file_size);
                    new_file_len = exp_growth.max(min_size);
                } else if free_space >= this.file_len / 100 * 10 {
                    // If we'd waste >= 10%, extend the same file
                    new_file_len = this.offset + needed_size;
                } else {
                    RwLockUpgradableReadGuard::upgrade(this).truncate_tail()?;
                    return Ok(None);
                }
                debug!(
                    "Extending wal file from {} to {}",
                    ByteSize(this.file_len),
                    ByteSize(new_file_len)
                );
                crate::utils::fallocate(&this.file, new_file_len)?;
                // Even if we fail halfway the function, before updating self.file_len, the logic is correct
            } else {
                new_file_len = this.file_len;
            }
        }

        trace!(
            "Writing {} wal items {} to idx {}",
            items.len(),
            ByteSize(items_total_size),
            this.range().end
        );
        let start_offset = this.offset;

        buffer.resize(
            (ceil_block_offset(if options.use_direct_io {
                needed_size
            } else {
                header_size as u64
            }) / BLOCK_SIZE_U64) as usize,
            Block::default(),
        );
        let (commit_header, rest) =
            Ref::<_, CommitHeader>::from_prefix(buffer.as_mut_bytes()).unwrap();
        let commit_header = Ref::into_mut(commit_header);
        let (item_lens, _buffer_rest) =
            Ref::<_, [u64]>::from_prefix_with_elems(rest, items.len()).unwrap();
        let item_lens = Ref::into_mut(item_lens);

        let first_item_idx = this.range.end;
        *commit_header = CommitHeader {
            magic: WAL_HEADER_MAGIC,
            checksum: 0,
            first_item_idx,
            total_byte_len: needed_size,
            items_byte_len: 0,
            items_len: items.len() as _,
        };

        for (item, i_len) in items.iter_mut().zip(item_lens.iter_mut()) {
            *i_len = item.len()?;
            commit_header.items_byte_len += *i_len;
        }
        let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
        hasher.write(&commit_header.as_bytes()[HEADER_BYTES_IGNORED_BY_CHECKSUM..]);
        hasher.write(item_lens.as_bytes());
        commit_header.checksum = hasher.finish();

        // If the write includes the footer it must be treated as fatal as it could be written and recoverable
        if !incremental_items && this.direct_io_file.is_some() {
            debug_assert_eq!(buffer.as_bytes().len() as u64, needed_size);
            let mut buffer_cursor = &mut buffer.as_mut_bytes()[header_size..];
            for item in items.iter_mut() {
                let buf = item.fill_buf()?;
                hasher.write(buf);
                buffer_cursor.write_all(buf)?;
            }
            let footer = CommitFooter {
                magic: WAL_FOOTER_MAGIC,
                checksum: hasher.finish(),
            };
            buffer_cursor[..size_of::<CommitFooter>()].copy_from_slice(footer.as_bytes());
            this.direct_io_file
                .as_ref()
                .unwrap()
                .write_all_at(buffer.as_bytes(), start_offset)?;
        } else if !incremental_items {
            #[allow(clippy::needless_late_init)] // false-positive
            let commit_footer;
            let commit_header = &buffer.as_bytes()[..header_size];
            let mut io_slices = SmallVec::<_, 16>::with_capacity(2 + items.len());
            io_slices.push(IoSlice::new(commit_header));
            let mut bytes_written = commit_header.len() as u64;
            for item in items.iter_mut() {
                let buf = item.fill_buf()?;
                hasher.write(buf);
                io_slices.push(IoSlice::new(buf));
                bytes_written += buf.len() as u64;
            }
            commit_footer = CommitFooter {
                magic: WAL_FOOTER_MAGIC,
                checksum: hasher.finish(),
            };
            io_slices.push(IoSlice::new(commit_footer.as_bytes()));
            bytes_written += commit_footer.as_bytes().len() as u64;
            if options.use_blocks {
                bytes_written = ceil_block_offset(bytes_written);
            }
            debug_assert_eq!(bytes_written, needed_size);
            this.file
                .write_all_vectored_at(&mut io_slices, start_offset)?;
        } else {
            let header = &buffer.as_bytes()[..header_size];
            this.file.write_all_at(header, start_offset)?;
            let mut bytes_written = header.len() as u64;
            for item in items.iter_mut() {
                bytes_written += copy_and_hash_bufread(
                    *item,
                    &this.file,
                    start_offset + bytes_written,
                    &mut hasher,
                )?;
            }
            let commit_footer = CommitFooter {
                magic: WAL_FOOTER_MAGIC,
                checksum: hasher.finish(),
            };
            this.file
                .write_all_at(commit_footer.as_bytes(), start_offset + bytes_written)?;
            bytes_written += commit_footer.as_bytes().len() as u64;
            if options.use_blocks {
                bytes_written = ceil_block_offset(bytes_written);
            }
            debug_assert_eq!(bytes_written, needed_size);
        }

        let mut this = RwLockUpgradableReadGuard::upgrade(this);
        this.offset += needed_size;
        this.file_len = new_file_len.max(this.offset);
        this.range.end += items.len() as u64;

        Ok(Some(first_item_idx))
    }

    fn truncate_tail(&mut self) -> io::Result<()> {
        fail::fail_point!("ftruncate", |s| Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failpoint ftruncate {:?}", s)
        )));
        if self.file_len > self.offset {
            self.file.set_len(self.offset as _)?;
            self.file_len = self.offset;
        }
        Ok(())
    }
}

fn copy_and_hash_bufread(
    item: &mut dyn WalRead,
    file: &File,
    mut offset: u64,
    hasher: &mut xxhash_rust::xxh3::Xxh3Default,
) -> io::Result<u64> {
    let original_offset = offset;
    loop {
        match item.fill_buf() {
            Ok([]) => break,
            Ok(bytes) => {
                hasher.write(bytes);
                file.write_all_at(bytes, offset).unwrap();
                let amt = bytes.len();
                item.consume(amt);
                offset += amt as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(offset - original_offset)
}

impl Wal {
    pub fn new(dir: PathBuf) -> io::Result<Self> {
        Self::with_options(dir, Options::default())
    }

    pub fn with_options(dir: PathBuf, mut options: Options) -> io::Result<Self> {
        if options.use_direct_io {
            let path = dir.join("DIRECT_IO_TEST");
            let support_check = (|| {
                let mut file = utils::create_direct_io_file(&path)?;
                let buffer = Block::default();
                file.write_all(&buffer.0)
            })();
            let _ = std::fs::remove_file(&path);
            if let Err(e) = support_check {
                warn!(
                    "WAL direct_io not supported under path {}: {e}",
                    dir.display()
                );
                options.use_direct_io = false;
            } else {
                options.use_blocks = true;
            }
        }

        let mut wal = Self {
            wal_prefix: "WAL-".to_string(),
            dir,
            files: Default::default(),
            group_commit: GroupCommit::new(options.group_commit_wait),
            buffer: Default::default(),
            options,
            halted: Default::default(),
            sync_state: Default::default(),
            sync_condvar: Default::default(),
        };

        let file_paths = wal.list_wal_files()?;
        debug!("Wal files found {:?}", file_paths);

        for (first_idx, path) in file_paths {
            let wal_file = WalFile::new(&wal.options, first_idx, path)?;
            wal.files.get_mut().insert(first_idx, RwLock::new(wal_file));
        }

        Ok(wal)
    }

    fn parse_wal_file_name(&self, path: &Path) -> Option<u64> {
        if let Some(p) = path.file_name().and_then(|p| p.to_str()) {
            if p.starts_with(&self.wal_prefix) {
                if let Some(suffix) = p.get(self.wal_prefix.len()..) {
                    return suffix.parse().ok();
                }
            }
        }
        None
    }

    fn file_path(&self, idx: u64) -> PathBuf {
        self.dir.join(format!("{}{:010}", self.wal_prefix, idx))
    }

    fn create_new_file(&self, next_idx: u64) -> io::Result<WalFile> {
        let next_file_path = self.file_path(next_idx);
        info!("Creating new wal file {}", next_file_path.display());
        WalFile::new(&self.options, next_idx, next_file_path)
    }

    fn list_wal_files(&self) -> io::Result<Vec<(u64, PathBuf)>> {
        let mut file_paths = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(idx) = self.parse_wal_file_name(&path) {
                file_paths.push((idx, path));
            }
        }
        file_paths.sort_unstable_by_key(|(k, _)| *k);
        Ok(file_paths)
    }

    pub fn recover(
        &self,
    ) -> io::Result<impl Iterator<Item = io::Result<(WalIdx, Box<dyn Read>)>> + '_> {
        let mut files_lock = self
            .files
            .try_write()
            .expect("Wal was locked during recovery");
        let mut files = Vec::with_capacity(files_lock.len());
        for (first_idx, wf) in &*files_lock {
            let wf = wf.read();
            let mut wf_iter = wf.iter(&self.options)?;
            assert_eq!(wf_iter.end_offset, 0);
            wf_iter.end_offset = wf.file_len;
            files.push((*first_idx, wf_iter))
        }
        let mut expected_idx = files.first().map_or(INITIAL_WAL_IDX, |(i, _)| *i);
        let mut files = files.into_iter();
        let mut curr_recovery: Option<WalFileIter> = None;
        let mut terminated = false;

        Ok(std::iter::from_fn(move || {
            if terminated {
                return None;
            }
            loop {
                if let Some(recovery) = &mut curr_recovery {
                    match recovery.next() {
                        Some(Ok(next)) => return Some(Ok(next)),
                        Some(Err(e)) => {
                            info!("Error reading Wal file {:?}: {}", recovery.range, e);
                            terminated = true;
                        }
                        None => (),
                    }
                }
                // wrap up current file recovery, either due to EOF or error
                if let Some(recovery) = curr_recovery.take() {
                    let mut wal_file = files_lock.get_mut(&recovery.range.start).unwrap().write();
                    assert_eq!(wal_file.range.start, recovery.range.start);
                    wal_file.offset = recovery.commit_offset;
                    wal_file.range.end = recovery.range.end;
                    if let Err(e) = wal_file.truncate_tail() {
                        error!("Error truncating Wal file {:?}: {}", wal_file.range(), e);
                        terminated = true;
                        return Some(Err(e));
                    }
                    info!(
                        "Wal file {} loaded with {:?}",
                        wal_file.range.start,
                        wal_file.range()
                    );
                    expected_idx = wal_file.range.end;
                }
                if terminated {
                    return None;
                }
                // proceed to the next file
                let (idx, wal_file) = files.next()?;
                if idx != expected_idx {
                    error!("Expected Wal file idx {} got {}", expected_idx, idx);
                    terminated = true;
                    return None;
                }
                curr_recovery = Some(wal_file);
            }
        }))
    }

    pub fn finish_recover(&self) -> io::Result<()> {
        let mut files = self
            .files
            .try_write()
            .expect("Wal was locked during recovery");
        let mut files_to_discard = Vec::new();
        while let Some(mut o) = files.last_entry() {
            let wf = o.get_mut().get_mut();
            if wf.offset == 0 && wf.range.is_empty() {
                let wf = o.remove();
                files_to_discard.push(wf);
            } else {
                break;
            }
        }
        if !files_to_discard.is_empty() {
            info!("Discarding {} Wal files", files_to_discard.len());
            self.remove_wal_files(files_to_discard.into_iter())?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn tail(&self) -> WalIdx {
        self.files
            .read()
            .first_key_value()
            .map_or(WalIdx::default(), |(i, _)| *i)
    }

    pub fn head(&self) -> WalIdx {
        self.files
            .read()
            .last_key_value()
            .map_or(WalIdx::default(), |(_, w)| w.read().range.end)
    }

    #[cfg(any(fuzzing, test))]
    pub fn iter(
        &self,
        from: WalIdx,
    ) -> io::Result<impl Iterator<Item = io::Result<(WalIdx, Vec<u8>)>> + '_> {
        let mut iters = Vec::new();
        for wf in self.files.read().values() {
            let wf = wf.read();
            if wf.range().end <= from {
                continue;
            }
            iters.push(wf.iter(&self.options)?);
        }
        Ok(iters
            .into_iter()
            .flatten()
            .filter(move |res| res.as_ref().map_or(true, |(i, _)| *i >= from))
            .map(|r| {
                r.and_then(|(i, mut r)| {
                    let mut v = Vec::new();
                    r.read_to_end(&mut v)?;
                    Ok((i, v))
                })
            }))
    }

    #[allow(dead_code)]
    pub fn clear(&self) -> io::Result<()> {
        let mut locked_files = self.files.try_write().unwrap();
        let files = std::mem::take(&mut *locked_files);
        drop(locked_files);
        self.remove_wal_files(files.into_values())
    }

    /// Remove entries up to `until` (exclusive).
    pub fn trim(&self, until: WalIdx) -> io::Result<()> {
        let mut files_to_delete: Vec<_> = self
            .files
            .read()
            .values()
            .map(|f| f.read().range())
            .take_while(|r| !r.is_empty() && r.end <= until)
            .map(|r| (r, None))
            .collect();
        if files_to_delete.is_empty() {
            return Ok(());
        }
        debug!("Trimming Wal ..{}", until);
        {
            let mut files = self.files.write();
            // If we're gonna remove all files add a new one at the end.
            // To keep things consistent on errors we will add before removing.
            if files_to_delete
                .last()
                .zip(files.last_key_value())
                .is_some_and(|((l_d, _), (&l_f, _))| l_d.start == l_f)
            {
                let next_idx = files_to_delete.last().unwrap().0.end;
                let wal_file = self.create_new_file(next_idx)?;
                files.insert(next_idx, RwLock::new(wal_file));
            }
            for (i, f) in &mut files_to_delete {
                *f = files.remove(&i.start);
            }
        }
        self.remove_wal_files(files_to_delete.into_iter().filter_map(|(_, wf)| wf))?;
        Ok(())
    }

    fn remove_wal_files(
        &self,
        files_to_delete: impl Iterator<Item = RwLock<WalFile>>,
    ) -> Result<(), io::Error> {
        for wf in files_to_delete {
            let WalFile {
                mut file, file_len, ..
            } = wf.into_inner();
            // Syncs can clone the file, so we must wait until we have exclusive access to it
            let path = loop {
                match Arc::try_unwrap(file) {
                    Ok(f) => {
                        drop(f.file);
                        break f.path;
                    }
                    Err(f) => {
                        file = f;
                        thread::sleep(Duration::from_micros(100));
                    }
                }
            };
            info!(
                "Removing Wal file {} ({})",
                path.display(),
                ByteSize(file_len)
            );
            fail::fail_point!("unlink", |s| Err(io::Error::new(
                io::ErrorKind::Other,
                format!("failpoint unlink {:?}", s)
            )));
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn internal_write(&self, items: &mut [&mut dyn WalRead]) -> Result<WalIdx, Error> {
        assert!(!items.is_empty());
        let mut buffer = self.buffer.lock();
        let mut files = self.files.upgradable_read();
        // We must check for halted after acquiring the locks,
        // this way this writer is guaranteed to see the latest, possibly halted, state.
        self.check_halted()?;
        loop {
            let next_idx = if let Some((_, file)) = files.last_key_value() {
                let wf = file.upgradable_read();
                let wf_range = wf.range();
                if let Some(wal_idx) = WalFile::write(wf, &self.options, &mut buffer, items)
                    .map_err(|e| {
                        // TODO: catch out of space errors, which shouldn't be fatal
                        let path = self.file_path(wf_range.start);
                        self.halt();
                        Error::FatalIo(io_map!(e, "{e} ({})", path.display()))
                    })?
                {
                    return Ok(wal_idx);
                }
                wf_range.end
            } else {
                INITIAL_WAL_IDX
            };
            let wf = self.create_new_file(next_idx)?;
            files.with_upgraded(|files| {
                files.insert(next_idx, RwLock::new(wf));
            });
        }
    }

    pub fn write(&self, items: &mut [&mut dyn WalRead]) -> Result<WalIdx, Error> {
        self.group_commit
            .write(items, |items| self.internal_write(items))
    }

    pub fn disk_size(&self) -> u64 {
        self.files.read().values().map(|f| f.read().file_len).sum()
    }

    pub fn num_files(&self) -> usize {
        self.files.read().len()
    }

    pub fn switch_to_fresh_file(&self) -> io::Result<()> {
        let files = self.files.upgradable_read();
        let next_idx = if let Some((_, wf)) = files.last_key_value() {
            let mut wf = wf.write();
            if wf.offset == 0 {
                return Ok(());
            }
            wf.truncate_tail()?;
            wf.range().end
        } else {
            INITIAL_WAL_IDX
        };
        let wf = self.create_new_file(next_idx)?;
        let mut files = RwLockUpgradableReadGuard::upgrade(files);
        files.insert(next_idx, RwLock::new(wf));
        Ok(())
    }

    pub fn sync(&self) -> Result<(), Error> {
        self.sync_up_to(WalIdx::MAX)
    }

    pub fn fadvise(&self) -> Result<(), Error> {
        self.check_halted()?;
        if !cfg!(unix) {
            return Ok(());
        }

        let files_to_advise = self
            .files
            .read()
            .iter()
            .rev()
            .map(|(_, wf)| wf.read())
            .take_while(|wf| wf.advise_offset < wf.target_advise_offset())
            .map(|wf| {
                Ok((
                    wf.range().start,
                    wf.target_advise_offset(),
                    wf.file_for_sync()?,
                ))
            })
            .collect::<io::Result<SmallVec<_, 2>>>()?;
        for (.., adv_offset, f) in &files_to_advise {
            utils::fadvise_wont_need(f, *adv_offset)?;
        }
        let files = self.files.read();
        for &(start, adv_offset, ..) in &files_to_advise {
            if let Some(f) = files.get(&start) {
                let mut f = f.write();
                f.advise_offset = f.advise_offset.max(adv_offset);
            }
        }
        Ok(())
    }

    /// Ensure entries up until sync_target (exclusive) are durable.
    pub fn sync_up_to(&self, sync_target: WalIdx) -> Result<(), Error> {
        // Ordering of lock acquisition is important here,
        // check and update halted while holding the mutex.
        let mut sync_state = self.sync_state.lock();
        self.check_halted()?;
        if sync_state.synced_up_to >= sync_target {
            return Ok(());
        }
        if sync_state.pending_sync_up_to >= sync_target {
            // wait for other thread(s) to sync the missing range or fail
            loop {
                self.check_halted()?;
                self.sync_condvar.wait(&mut sync_state);
                if sync_state.synced_up_to >= sync_target {
                    return Ok(());
                }
            }
        }
        let sync_starting_point = sync_state.pending_sync_up_to;
        let prev_file_dir_fsync = sync_state.last_file_dir_fsync;
        let start = Instant::now();
        let files_to_sync = self
            .files
            .read()
            .iter()
            .rev()
            .map(|(_, wf)| wf.read())
            .take_while(|wf| wf.range().end > sync_starting_point)
            .map(|wf| Ok((wf.range(), wf.file_for_sync()?)))
            .collect::<io::Result<SmallVec<_, 2>>>()?;
        // Files to sync is newest to oldest, but we sync oldest to newest
        let Some(newest_file) = files_to_sync.first() else {
            return Ok(());
        };
        let new_last_file_dir_fsync = newest_file.0.start;
        let new_synced_up_to = newest_file.0.end;
        sync_state.pending_sync_up_to = new_synced_up_to;
        drop(sync_state);

        debug!("Sync {sync_starting_point}..{new_synced_up_to} (target {sync_target})",);
        for (_, file) in files_to_sync.into_iter().rev() {
            debug!("Syncing file {}", file.path.display());
            fail::fail_point!("fsync", |s| Err(Error::FatalIo(io::Error::new(
                io::ErrorKind::Other,
                format!("failpoint fsync {:?}", s)
            ))));
            if let Err(e) = file.sync_data() {
                error!("Error fsyncing wal file {}: {e}", file.path.display());
                let _ss = self.sync_state.lock();
                self.halt();
                self.sync_condvar.notify_all();
                return Err(Error::FatalIo(io_map!(e, "{e} ({})", file.path.display())));
            }
        }

        if Some(new_last_file_dir_fsync) > prev_file_dir_fsync {
            debug!("Syncing directory {}", self.dir.display());
            if let Err(e) = crate::utils::sync_dir(&self.dir) {
                error!("Error syncing parent directory {}: {e}", self.dir.display());
                let _ss = self.sync_state.lock();
                self.halt();
                self.sync_condvar.notify_all();
                return Err(Error::FatalIo(io_map!(e, "{e} ({})", self.dir.display())));
            }
        }

        let mut sync_state = self.sync_state.lock();
        while sync_state.synced_up_to != sync_starting_point {
            self.check_halted()?;
            self.sync_condvar.wait(&mut sync_state);
        }
        sync_state.synced_up_to = new_synced_up_to;
        sync_state.last_file_dir_fsync = Some(new_last_file_dir_fsync);
        drop(sync_state);
        self.sync_condvar.notify_all();

        debug!(
            "Sync {}..{} took {:?}",
            sync_starting_point,
            new_synced_up_to,
            start.elapsed()
        );
        Ok(())
    }

    #[cold]
    pub fn halt(&self) {
        if !self.halted.swap(true, atomic::Ordering::Release) {
            error!("Halting Wal {}", self.dir.display());
        }
    }

    fn check_halted(&self) -> Result<(), Error> {
        if !self.halted.load(atomic::Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::WalHalted)
        }
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_write_fresh_file_then_trim() {
        let _ = env_logger::try_init();
        let folder = tempfile::tempdir().unwrap();

        let wal = Wal::with_options(folder.path().into(), Options::default()).unwrap();
        assert!(wal
            .iter(0)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap()
            .is_empty());

        assert_eq!(wal.write(&mut [&mut &b"a"[..]]).unwrap(), 0);
        assert_eq!(wal.write(&mut [&mut &b"b"[..], &mut &b"c"[..]]).unwrap(), 1);
        assert_eq!(wal.write(&mut [&mut &b"d"[..]]).unwrap(), 3);

        assert_ne!(wal.disk_size(), 0);
        wal.switch_to_fresh_file().unwrap();
        wal.trim(4).unwrap();
        assert_eq!(wal.disk_size(), 0);
    }

    fn test_with_options(options: Options) {
        let _ = env_logger::try_init();
        let mut tmp_file = tempfile::tempfile().unwrap();
        tmp_file.write_all(b"tmp_file").unwrap();
        tmp_file.seek(io::SeekFrom::Start(0)).unwrap();
        let mut tmp_file = BufReader::new(tmp_file);

        let folder = tempfile::tempdir().unwrap();

        let wal = Wal::with_options(folder.path().into(), options).unwrap();
        assert!(wal
            .iter(0)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap()
            .is_empty());

        assert_eq!(wal.write(&mut [&mut &b"a"[..]]).unwrap(), 0);
        assert_eq!(wal.write(&mut [&mut &b"b"[..], &mut &b"c"[..]]).unwrap(), 1);
        assert_eq!(wal.write(&mut [&mut &b"d"[..]]).unwrap(), 3);
        assert_eq!(wal.write(&mut [&mut tmp_file]).unwrap(), 4);
        assert_eq!(
            wal.iter(0)
                .unwrap()
                .collect::<io::Result<Vec<_>>>()
                .unwrap()
                .into_iter()
                .map(|p| p.1)
                .collect::<Vec<_>>(),
            vec![&b"a"[..], b"b", b"c", b"d", b"tmp_file"]
        );

        drop(wal);

        for limit in [0, usize::MAX] {
            let mut wal = Wal::new(folder.path().into()).unwrap();
            wal.options.max_iter_mem = limit;
            wal.recover().unwrap().for_each(|_| ());
            wal.finish_recover().unwrap();
            assert_eq!(
                wal.iter(0)
                    .unwrap()
                    .collect::<io::Result<Vec<_>>>()
                    .unwrap()
                    .into_iter()
                    .map(|p| p.1)
                    .collect::<Vec<_>>(),
                vec![&b"a"[..], b"b", b"c", b"d", b"tmp_file"]
            );
        }
    }

    #[test]
    fn test_basic() {
        test_with_options(Options::default());
    }

    #[test]
    fn test_direct_io() {
        let options = Options {
            use_direct_io: true,
            ..Default::default()
        };
        test_with_options(options);
    }

    #[test]
    fn test_use_blocks() {
        let options = Options {
            use_blocks: true,
            ..Default::default()
        };
        test_with_options(options);
    }
}
