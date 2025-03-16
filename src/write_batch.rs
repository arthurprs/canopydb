use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    mem,
    ops::Bound,
};

use crate::{
    repr::{DbId, TreeId},
    utils::EscapedBytes,
    EnvOptions, TreeOptions,
};

use smallvec::SmallVec;
use triomphe::Arc;
use zerocopy::IntoBytes;

#[derive(Debug)]
pub struct WriteBatch {
    options: Arc<EnvOptions>,
    inner: WriteBatchInner,
}

#[derive(Debug)]
enum WriteBatchInner {
    #[debug("InMemory({})", _0.len())]
    InMemory(Vec<u8>),
    #[debug("DiskWriter(..)")]
    DiskWriter(BufWriter<File>),
}

mod op_bytes {
    pub const DATABASE: u8 = 0;
    pub const INSERT: u8 = 1;
    pub const DELETE: u8 = 2;
    pub const DELETE_RANGE: u8 = 3;
    pub const CREATE_TREE: u8 = 10;
    pub const DELETE_TREE: u8 = 11;
    pub const RENAME_TREE: u8 = 12;
}

#[derive(Debug)]
pub enum Operation {
    Database(DbId),
    #[debug("Insert({_0}, {}, {})", EscapedBytes(_1), EscapedBytes(_2))]
    Insert(TreeId, Vec<u8>, Vec<u8>),
    #[debug("Delete({_0}, {})", EscapedBytes(_1))]
    Delete(TreeId, Vec<u8>),
    #[debug("DeleteRange({_0}, ({:?}, {:?}))", _1.0.as_ref().map(|a| EscapedBytes(a)),  _1.1.as_ref().map(|a| EscapedBytes(a)))]
    DeleteRange(TreeId, (Bound<Vec<u8>>, Bound<Vec<u8>>)),
    #[debug("CreateTree({_0}, {}, {_2:?})", EscapedBytes(_1))]
    CreateTree(TreeId, Vec<u8>, TreeOptions),
    #[debug("DeleteTree({})", EscapedBytes(_0))]
    DeleteTree(Vec<u8>),
    #[debug("RenameTree({}, {})", EscapedBytes(_0), EscapedBytes(_1))]
    RenameTree(Vec<u8>, Vec<u8>),
}

impl WriteBatch {
    pub fn new(options: Arc<EnvOptions>) -> WriteBatch {
        Self {
            options,
            inner: WriteBatchInner::InMemory(Vec::new()),
        }
    }

    pub fn as_reader(&mut self) -> io::Result<WriteBatchReader<'_>> {
        match &mut self.inner {
            WriteBatchInner::InMemory(mem) => Ok(WriteBatchReader::Bytes(&mem[..])),
            inner @ WriteBatchInner::DiskWriter(_) => {
                let WriteBatchInner::DiskWriter(file) =
                    mem::replace(inner, WriteBatchInner::InMemory(Vec::new()))
                else {
                    unreachable!()
                };
                let mut file = file.into_inner().map_err(|e| e.into_error())?;
                file.seek(io::SeekFrom::Start(0))?;
                Ok(WriteBatchReader::File(BufReader::new(file)))
            }
        }
    }

    pub fn iter_operations(
        bytes: &mut dyn Read,
    ) -> impl Iterator<Item = io::Result<Operation>> + '_ {
        let read_bytes = &(|src: &mut dyn Read, len: usize| -> io::Result<Vec<u8>> {
            // TODO: sad vec initialization
            let mut out = vec![0; len];
            src.read_exact(&mut out)?;
            Ok(out)
        });
        let read_u32_prefixed_bytes = |src: &mut dyn Read| -> io::Result<Vec<u8>> {
            let mut len = 0u32;
            src.read_exact(len.as_mut_bytes())?;
            read_bytes(src, len as usize)
        };

        std::iter::from_fn(move || {
            let mut op_code = 0u8;
            match bytes.read(op_code.as_mut_bytes()) {
                Ok(0) => return None,
                Ok(_) => (),
                Err(e) => return Some(Err(e)),
            }
            Some(op_code).map(|op_code| {
                let operation = match op_code {
                    op_bytes::DATABASE => {
                        let mut db_id = DbId::default();
                        bytes.read_exact(db_id.as_mut_bytes())?;
                        Operation::Database(db_id)
                    }
                    op_bytes::INSERT => {
                        let mut tree_id = TreeId::default();
                        bytes.read_exact(tree_id.as_mut_bytes())?;
                        let key = read_u32_prefixed_bytes(bytes)?;
                        let value = read_u32_prefixed_bytes(bytes)?;
                        Operation::Insert(tree_id, key, value)
                    }
                    op_bytes::DELETE => {
                        let mut tree_id = TreeId::default();
                        bytes.read_exact(tree_id.as_mut_bytes())?;
                        let key = read_u32_prefixed_bytes(bytes)?;
                        Operation::Delete(tree_id, key)
                    }
                    op_bytes::DELETE_RANGE => {
                        let mut tree_id = TreeId::default();
                        bytes.read_exact(tree_id.as_mut_bytes())?;
                        let mut byte = [0u8];
                        let mut start = Bound::Unbounded;
                        let mut end = Bound::Unbounded;
                        for bound in [&mut start, &mut end] {
                            bytes.read_exact(&mut byte)?;
                            *bound = match byte[0] {
                                0 => Bound::Unbounded,
                                1 => Bound::Included(read_u32_prefixed_bytes(bytes)?),
                                2 => Bound::Excluded(read_u32_prefixed_bytes(bytes)?),
                                i => {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("Unknown bound byte {i}"),
                                    ))
                                }
                            };
                        }
                        Operation::DeleteRange(tree_id, (start, end))
                    }
                    op_bytes::CREATE_TREE => {
                        let mut tree_id = TreeId::default();
                        bytes.read_exact(tree_id.as_mut_bytes())?;
                        let tree_name = read_u32_prefixed_bytes(bytes)?;
                        let options = read_u32_prefixed_bytes(bytes)?;
                        let options = TreeOptions::from_bytes(&options)?;
                        Operation::CreateTree(tree_id, tree_name, options)
                    }
                    op_bytes::DELETE_TREE => {
                        let tree_name = read_u32_prefixed_bytes(bytes)?;
                        Operation::DeleteTree(tree_name)
                    }
                    op_bytes::RENAME_TREE => {
                        let old_name = read_u32_prefixed_bytes(bytes)?;
                        let new_name = read_u32_prefixed_bytes(bytes)?;
                        Operation::RenameTree(old_name, new_name)
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Unknown op-code {op_code}"),
                        ))
                    }
                };
                Ok(operation)
            })
        })
        .fuse()
    }

    pub fn push_db(&mut self, db_id: DbId) -> io::Result<()> {
        self.write_many(&[&[op_bytes::DATABASE], db_id.as_bytes()])
    }

    pub fn push_insert(&mut self, tree: TreeId, k: &[u8], v: &[u8]) -> io::Result<()> {
        self.write_many(&[
            &[op_bytes::INSERT],
            tree.as_bytes(),
            (k.len() as u32).as_bytes(),
            k,
            (v.len() as u32).as_bytes(),
            v,
        ])
    }

    pub fn push_delete(&mut self, tree: TreeId, k: &[u8]) -> io::Result<()> {
        self.write_many(&[
            &[op_bytes::DELETE],
            tree.as_bytes(),
            (k.len() as u32).as_bytes(),
            k,
        ])
    }

    pub fn push_delete_range(
        &mut self,
        tree: TreeId,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> io::Result<()> {
        let mut se = ((0u8, 0u32), (0u8, 0u32));
        let mut parts = SmallVec::<&[u8], 8>::new();
        parts.push(&[op_bytes::DELETE_RANGE]);
        parts.push(tree.as_bytes());
        for (bound, (u, l)) in [(start, &mut se.0), (end, &mut se.1)] {
            match bound {
                Bound::Unbounded => parts.push(&[0]),
                Bound::Included(b) | Bound::Excluded(b) => {
                    *u = 1 + matches!(bound, Bound::Excluded(_)) as u8;
                    *l = b.len() as u32;
                    parts.extend_from_slice(&[std::slice::from_ref(u), l.as_bytes(), b]);
                }
            }
        }
        self.write_many(&parts)
    }

    pub fn push_create_tree(
        &mut self,
        tree_id: &TreeId,
        tree: &[u8],
        options: &TreeOptions,
    ) -> io::Result<()> {
        let options = options.to_bytes();
        self.write_many(&[
            &[op_bytes::CREATE_TREE],
            tree_id.as_bytes(),
            (tree.len() as u32).as_bytes(),
            tree,
            (options.len() as u32).as_bytes(),
            &options,
        ])
    }

    pub fn push_delete_tree(&mut self, tree: &[u8]) -> io::Result<()> {
        self.write_many(&[
            &[op_bytes::DELETE_TREE],
            (tree.len() as u32).as_bytes(),
            tree,
        ])
    }

    pub fn push_rename_tree(&mut self, old: &[u8], new: &[u8]) -> io::Result<()> {
        self.write_many(&[
            &[op_bytes::RENAME_TREE],
            (old.len() as u32).as_bytes(),
            old,
            (new.len() as u32).as_bytes(),
            new,
        ])
    }

    #[cold]
    fn spill(&mut self) -> io::Result<&mut BufWriter<File>> {
        let mut file = BufWriter::new(tempfile::tempfile_in(
            &self.options.wal_write_batch_tempfile_dir,
        )?);
        if let WriteBatchInner::InMemory(mem) = &self.inner {
            file.write_all(mem)?;
        } else {
            unreachable!()
        }
        self.inner = WriteBatchInner::DiskWriter(file);
        if let WriteBatchInner::DiskWriter(file) = &mut self.inner {
            Ok(file)
        } else {
            unreachable!()
        }
    }

    #[inline]
    fn write_many(&mut self, parts: &[&[u8]]) -> io::Result<()> {
        let file = match &mut self.inner {
            WriteBatchInner::InMemory(mem) => {
                let additional: usize = parts.iter().map(|i| i.len()).sum();
                if additional + mem.len()
                    > self
                        .options
                        .wal_write_batch_memory_limit
                        .max(mem.capacity())
                {
                    self.spill()?
                } else {
                    mem.reserve(additional);
                    for part in parts {
                        mem.extend_from_slice(part);
                    }
                    return Ok(());
                }
            }
            WriteBatchInner::DiskWriter(file) => file,
        };
        for part in parts {
            file.write_all(part)?;
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        match &mut self.inner {
            WriteBatchInner::InMemory(mem) => mem.clear(),
            WriteBatchInner::DiskWriter(_) => self.inner = WriteBatchInner::InMemory(Vec::new()),
        }
    }
}

pub enum WriteBatchReader<'a> {
    Bytes(&'a [u8]),
    File(BufReader<File>),
}

impl WriteBatchReader<'_> {
    pub fn as_wal_read(&mut self) -> &mut dyn crate::wal::WalRead {
        match self {
            WriteBatchReader::Bytes(b) => &mut *b,
            WriteBatchReader::File(f) => &mut *f,
        }
    }
}
