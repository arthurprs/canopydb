use std::{
    fmt,
    mem::{size_of, size_of_val},
    ops::{Deref, DerefMut},
};
use zerocopy::*;

use crate::{node::NodeType, utils::EscapedBytes, PAGE_SIZE};

pub type DbId = u128;
pub type TreeId = u64;
pub type TxId = u64;
pub type PageId = u32;
pub type WalIdx = u64;

pub const METAPAGE_MAGIC: u64 = 0x7BB61DB6F32611B1;
pub const FIRST_COMPRESSED_PAGE: PageId = ((PageId::MAX as u64 + 1) / 4 * 3) as PageId;

#[derive(Default, Copy, Debug, Clone, FromZeroes, FromBytes, AsBytes, PartialEq, Eq)]
#[repr(C)]
pub struct PageFlags(u8);

bitflags::bitflags! {
    impl PageFlags: u8 {
        /// Set if the page is a compressed page
        const Compressed = 0b1000000;
    }
}

#[derive(FromZeroes, FromBytes, AsBytes, Unaligned)]
#[repr(packed)]
pub struct IndirectionValue {
    pub pid: PageId,
    pub span: U24,
}

#[derive(Default, Copy, Debug, Clone, FromZeroes, FromBytes, AsBytes, PartialEq, Eq)]
#[repr(C)]
#[debug("{}", u32::from(*self))]
pub struct U24([u8; 3]);

impl From<u16> for U24 {
    fn from(value: u16) -> Self {
        let a = value.to_le_bytes();
        Self([a[0], a[1], 0])
    }
}

impl TryFrom<u32> for U24 {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        let a = value.to_le_bytes();
        if a[3] == 0 {
            Ok(Self([a[0], a[1], a[2]]))
        } else {
            Err(())
        }
    }
}

impl From<U24> for u32 {
    fn from(v: U24) -> Self {
        u32::from_le_bytes([v.0[0], v.0[1], v.0[2], 0])
    }
}

impl From<U24> for usize {
    fn from(v: U24) -> Self {
        u32::from(v) as usize
    }
}

pub trait PageTrait {
    fn is_compressed(&self) -> bool;
}

impl PageTrait for PageId {
    fn is_compressed(&self) -> bool {
        *self >= FIRST_COMPRESSED_PAGE
    }
}

pub enum MaybeValue<'a> {
    Bytes(&'a [u8]),
    Overflow([PageId; 2]),
    Delete,
}

impl fmt::Debug for MaybeValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(arg0) => f.debug_tuple("Bytes").field(&EscapedBytes(arg0)).finish(),
            Self::Overflow(arg0) => f.debug_tuple("Overflow").field(arg0).finish(),
            Self::Delete => write!(f, "Delete"),
        }
    }
}

impl MaybeValue<'_> {
    #[inline]
    pub fn repr_len(&self) -> usize {
        match self {
            MaybeValue::Bytes(b) => b.len(),
            MaybeValue::Overflow(o) => size_of_val(o),
            MaybeValue::Delete => 0,
        }
    }

    #[inline]
    pub fn repr_bytes(&self) -> &[u8] {
        match self {
            MaybeValue::Bytes(b) => b,
            MaybeValue::Overflow(p) => p.as_bytes(),
            MaybeValue::Delete => &[],
        }
    }

    #[inline]
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete)
    }

    #[inline]
    pub fn is_overflow(&self) -> bool {
        matches!(self, Self::Overflow(_))
    }

    #[inline]
    pub fn overflow_from_bytes(b: &[u8]) -> MaybeValue<'static> {
        let mut overflow = [PageId::default(); 2];
        overflow.as_bytes_mut().copy_from_slice(b);
        MaybeValue::Overflow(overflow)
    }
}

#[derive(Default, Copy, Debug, Clone, FromZeroes, FromBytes, AsBytes, PartialEq, Eq)]
#[repr(C)]
pub struct PageHeader {
    pub checksum: u32,
    pub id: PageId,
    pub span: U24,
    pub flags: PageFlags,
}

#[derive(Default, Copy, Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
pub struct NodeHeader {
    pub page_header: PageHeader,
    pub tail_curr_size: u32,
    pub tail_real_size: u32,
    pub num_keys: u16,
    pub key_prefix_len: u16,
    /// the level of the node, the leaf level is at level 0
    pub level: u8,
    pub fixed_key_len: i8,
    pub fixed_value_len: i8,
    #[debug(skip)]
    pub _padding: [u8; 1],
}

#[derive(Default, Copy, Debug, Clone, FromZeroes, FromBytes, AsBytes, Deref, DerefMut)]
#[repr(C)]
pub struct LeafHeader {
    #[deref]
    #[deref_mut]
    pub node_header: NodeHeader,
}

#[derive(Copy, Clone, FromZeroes, FromBytes, AsBytes, Unaligned)]
#[repr(packed)]
pub struct Offset {
    /// Slot offset from the beginning of the page
    pub offset: u32,
    pub prefix: u32,
}

impl fmt::Debug for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Offset").field(&{ self.offset }).finish()
    }
}

#[derive(Copy, Clone, FromZeroes, FromBytes, AsBytes, Unaligned)]
#[repr(packed)]
pub struct VarRepr {
    pub len: u32,
}

#[derive(Default, Copy, Debug, Clone, FromZeroes, FromBytes, AsBytes, Deref, DerefMut)]
#[repr(C)]
pub struct BranchHeader {
    #[deref]
    #[deref_mut]
    pub node_header: NodeHeader,
    pub leftmost_pointer: PageId,
}

#[derive(Default, Copy, Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
pub struct MetapageHeader {
    pub page_header: PageHeader,
    pub magic: [u8; 8],
    pub freelist_root: PageId,
    pub tx_id: TxId,
    /// First WalIdx part of the snapshot (inclusive)
    pub wal_start: WalIdx,
    /// Last WalIdx part of the snapshot (exclusive)
    pub wal_end: WalIdx,
    pub snapshot_tx_id: TxId,
    pub next_tree_id: TreeId,
    pub trees_tree: TreeValue,
    pub _padding1: u8,
    pub indirections_tree: TreeValue,
    pub _padding2: u8,
}

#[derive(Copy, Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
pub struct CompressedPageHeader {
    pub page_header: PageHeader,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

#[derive(Copy, Clone, FromZeroes, FromBytes, AsBytes, Unaligned)]
#[repr(packed)]
pub struct BranchKeyRepr {
    pub child: PageId,
}

#[derive(Copy, Clone, FromZeroes, FromBytes, AsBytes, Unaligned)]
#[repr(packed)]
pub struct LeafPairRepr {
    pub flags: u8,
}

impl LeafPairRepr {
    pub const OVERFLOW_MASK: u8 = 0b01u8;

    #[inline]
    pub fn is_overflow(&self) -> bool {
        self.flags & Self::OVERFLOW_MASK != 0
    }
}

#[derive(Default, Copy, Clone, FromZeroes, FromBytes, AsBytes, Unaligned)]
#[repr(packed)]
pub struct TreeValue {
    pub id: TreeId,
    pub root: PageId,
    pub num_keys: u64,
    pub level: u8,
    pub min_branch_node_pages: u8,
    pub min_leaf_node_pages: u8,
    pub fixed_key_len: i8,
    pub fixed_value_len: i8,
    pub nodes_compressed: u8,
    pub overflow_compressed: u8,
}

impl std::fmt::Debug for TreeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id = self.id;
        let root = self.root;
        let num_keys = self.num_keys;
        f.debug_struct("TreeValue")
            .field("id", &id)
            .field("root", &root)
            .field("num_keys", &num_keys)
            .field("nodes_compressed", &self.nodes_compressed)
            .field("overflow_compressed", &self.overflow_compressed)
            .field("min_branch_node_pages", &self.min_branch_node_pages)
            .field("min_leaf_node_pages", &self.min_leaf_node_pages)
            .field("level", &self.level)
            .field("fixed_key_len", &self.fixed_key_len)
            .field("fixed_value_len", &self.fixed_value_len)
            .finish()
    }
}

impl TreeValue {
    pub fn should_compress_level(&self, level: u8, span: PageId) -> bool {
        if cfg!(any(fuzzing, feature = "shuttle")) {
            return self.nodes_compressed != 0 && level <= self.level.saturating_sub(1);
        }
        self.nodes_compressed != 0 && level == 0 && span > self.overflow_compressed as PageId
    }

    pub fn should_compress_overflow(&self, span: PageId) -> bool {
        self.overflow_compressed != 0 && span > self.overflow_compressed as PageId
    }

    pub fn min_branch_span(&self, _level: u8) -> PageId {
        self.min_branch_node_pages as PageId
    }
}

#[derive(Default, Copy, Clone, FromZeroes, FromBytes, AsBytes, Unaligned, PartialEq, Eq)]
#[repr(packed)]
pub struct MappingValue {
    pub compressed_page_id: PageId,
}

pub trait NodeRepr: NodeType {
    type Header: Default + FromBytes + AsBytes + Deref<Target = NodeHeader> + DerefMut;
    type Repr: Unaligned + FromBytes + AsBytes;
    const N_VAR: usize;

    fn repr_size() -> usize {
        size_of::<Self::Repr>()
    }
}

/// Assert that the start of the page can be cast to a header
#[inline(always)]
pub(crate) fn header_cast<T: AsBytes + FromBytes, P: HeaderProvider + ?Sized>(slice: &P) -> &T {
    slice.cast()
}

#[inline(always)]
pub(crate) fn header_cast_mut<T: AsBytes + FromBytes, P: HeaderProvider + ?Sized>(
    slice: &mut P,
) -> &mut T {
    slice.cast_mut()
}

pub(crate) trait HeaderProvider {
    fn cast<T: AsBytes + FromBytes>(&self) -> &T;
    fn cast_mut<T: AsBytes + FromBytes>(&mut self) -> &mut T;
    fn split_off<T: AsBytes + FromBytes>(&self) -> &[u8];
    fn split_off_mut<T: AsBytes + FromBytes>(&mut self) -> &mut [u8];
}

// Safety: Page invariant is that it's backing data are well aligned bytes multiple of PAGE_SIZE
impl<const CLONE: bool> HeaderProvider for crate::page::Page<CLONE> {
    #[inline]
    fn cast<T: AsBytes + FromBytes>(&self) -> &T {
        assert_valid_page_and_header::<T>(self.data());
        unsafe { &*self.data().as_ptr().cast::<T>() }
    }

    #[inline]
    fn cast_mut<T: AsBytes + FromBytes>(&mut self) -> &mut T {
        assert_valid_page_and_header::<T>(self.data_mut());
        unsafe { &mut *self.data_mut().as_mut_ptr().cast::<T>() }
    }

    #[inline]
    fn split_off<T: AsBytes + FromBytes>(&self) -> &[u8] {
        assert_valid_page_and_header::<T>(self.data());
        unsafe { self.data().get_unchecked(size_of::<T>()..) }
    }

    #[inline]
    fn split_off_mut<T: AsBytes + FromBytes>(&mut self) -> &mut [u8] {
        assert_valid_page_and_header::<T>(self.data_mut());
        unsafe { self.data_mut().get_unchecked_mut(size_of::<T>()..) }
    }
}

#[inline]
fn assert_valid_page_and_header<T: AsBytes + FromBytes>(slice: &[u8]) {
    // Ensure we're actually using page sizes and the aligned to 8.
    // In practice all(?) modern allocators align to 16.
    debug_assert_ne!(slice.len() / PAGE_SIZE as usize, 0);
    debug_assert_eq!(slice.len() % PAGE_SIZE as usize, 0);
    debug_assert_eq!(slice.as_ptr() as usize % 8, 0);
    // Ensure T size and alignment
    debug_assert!(std::mem::align_of::<T>() <= 8);
    debug_assert!(std::mem::size_of::<T>() <= PAGE_SIZE as usize);
}

impl HeaderProvider for [u8] {
    #[inline]
    fn cast<T: AsBytes + FromBytes>(&self) -> &T {
        Ref::<_, T>::new_from_prefix(self).unwrap().0.into_ref()
    }

    #[inline]
    fn cast_mut<T: AsBytes + FromBytes>(&mut self) -> &mut T {
        Ref::<_, T>::new_from_prefix(self).unwrap().0.into_mut()
    }

    #[inline]
    fn split_off<T: AsBytes + FromBytes>(&self) -> &[u8] {
        &self[size_of::<T>()..]
    }

    #[inline]
    fn split_off_mut<T: AsBytes + FromBytes>(&mut self) -> &mut [u8] {
        &mut self[size_of::<T>()..]
    }
}
