use std::{
    borrow::Cow,
    cmp::Ordering,
    io::Write,
    mem::{self, size_of},
};

use zerocopy::{IntoBytes, Ref};

use crate::{
    page::Page,
    repr::{
        header_cast, header_cast_mut, BranchHeader, BranchKeyRepr, HeaderProvider, LeafHeader,
        LeafPairRepr, MaybeValue, NodeHeader, NodeRepr, Offset, PageId, TreeValue, VarRepr,
    },
    utils::{common_prefix_len, EscapedBytes},
    Error, MAX_PREFIX_SIZE, MIN_PREFIX_SIZE,
};

#[derive(Debug)]
pub(crate) enum FitError {
    WontFit,
    _WrongPrefix,
}

#[derive(Deref)]
pub(crate) struct Node<TYPE: NodeType, const CLONE: bool = true> {
    #[deref]
    page: Page<CLONE>,
    ty: std::marker::PhantomData<TYPE>,
}

impl<TYPE: NodeType> Clone for Node<TYPE, true> {
    fn clone(&self) -> Self {
        Self {
            page: self.page.clone(),
            ty: std::marker::PhantomData,
        }
    }
}

#[derive(Debug, Deref)]
#[repr(transparent)]
pub(crate) struct DirtyNode<'node, TYPE: NodeType> {
    node: &'node mut Node<TYPE, false>,
}

pub(crate) type PrefixDelta<'a> = Result<usize, &'a [u8]>;

#[derive(Debug, Default)]
pub struct NodePrefix<'p> {
    // TODO we could try to stash the parent dynamic prefix to ensure we can
    // yield the longest prefix even if child_idx is the left/right most child
    // parent_prefix: Option<&'pp NodePrefix<'pp>>,
    parent: Option<&'p BranchNode<false>>,
    child_idx: usize,
}

impl<'p> NodePrefix<'p> {
    #[inline]
    pub fn parent_prefix(&self) -> &[u8] {
        if let Some(parent) = self.parent {
            parent.key_prefix()
        } else {
            b""
        }
    }

    #[inline]
    pub fn parent_prefix_len(&self) -> usize {
        if let Some(parent) = self.parent {
            parent.key_prefix_len()
        } else {
            0
        }
    }

    #[inline]
    pub fn max_prefix(&self) -> Cow<'p, [u8]> {
        self.child_prefix(0)
    }

    #[inline]
    pub fn max_prefix_merge_right(&self) -> Cow<'p, [u8]> {
        self.child_prefix(1)
    }

    pub fn max_prefix_split<'a>(&self, full_separator: &'a [u8]) -> (Cow<'a, [u8]>, Cow<'a, [u8]>)
    where
        'p: 'a,
    {
        let Some(parent) = self.parent else {
            return (Cow::Borrowed(b""), Cow::Borrowed(b""));
        };
        if parent.header().fixed_key_len >= 0 {
            return (Cow::Borrowed(b""), Cow::Borrowed(b""));
        }
        debug_assert!(full_separator.starts_with(parent.key_prefix()));
        let separator = &full_separator[parent.key_prefix_len()..];

        let parent_prefix = parent.key_prefix();
        let lb = self.child_idx.checked_sub(1).map(|i| parent.key_at(i));
        let ub = (self.child_idx < parent.num_keys()).then(|| parent.key_at(self.child_idx));
        (
            lb.map_or(Cow::Borrowed(b""), |lb| {
                Self::compute_prefix(lb, separator, parent_prefix)
            }),
            ub.map_or(Cow::Borrowed(b""), |ub| {
                Self::compute_prefix(separator, ub, parent_prefix)
            }),
        )
    }

    #[inline]
    pub fn prefix_delta<'c>(current: &'c [u8], max: &[u8]) -> PrefixDelta<'c> {
        if max.len() >= current.len() {
            debug_assert!(max.starts_with(current));
            Ok(max.len() - current.len())
        } else {
            debug_assert!(current.starts_with(max));
            Err(&current[max.len()..])
        }
    }

    fn child_prefix(&self, right_offset: usize) -> Cow<'p, [u8]> {
        let Some(parent) = self.parent else {
            return Cow::Borrowed(b"");
        };
        if parent.header().fixed_key_len >= 0 {
            return Cow::Borrowed(b"");
        }

        let parent_prefix = parent.key_prefix();
        if self.child_idx == 0 || self.child_idx + right_offset >= parent.num_keys() {
            return Cow::Borrowed(parent_prefix);
        }
        let lb = parent.key_at(self.child_idx - 1);
        let ub = parent.key_at(self.child_idx + right_offset);
        Self::compute_prefix(lb, ub, parent_prefix)
    }

    fn compute_prefix<'a>(lb: &'a [u8], ub: &[u8], parent_prefix: &[u8]) -> Cow<'a, [u8]> {
        let common_prefix_len = common_prefix_len(lb, ub);
        if parent_prefix.len() + common_prefix_len < MIN_PREFIX_SIZE {
            return Cow::Borrowed(b"");
        }
        let common_prefix = &lb[..common_prefix_len.min(MAX_PREFIX_SIZE - parent_prefix.len())];
        if parent_prefix.is_empty() {
            Cow::Borrowed(common_prefix)
        } else {
            Cow::Owned([parent_prefix, common_prefix].concat())
        }
    }
}

pub trait NodeType {
    const IS_LEAF: bool;
    const IS_BRANCH: bool;
}

pub(crate) struct NodeTypeNone;

impl NodeType for NodeTypeNone {
    const IS_LEAF: bool = false;
    const IS_BRANCH: bool = false;
}

pub(crate) struct NodeTypeLeaf;

impl NodeType for NodeTypeLeaf {
    const IS_LEAF: bool = true;
    const IS_BRANCH: bool = false;
}
pub(crate) struct NodeTypeBranch;

impl NodeType for NodeTypeBranch {
    const IS_LEAF: bool = false;
    const IS_BRANCH: bool = true;
}

pub(crate) type UntypedNode = Node<NodeTypeNone>;
pub(crate) type LeafNode<const CLONE: bool = true> = Node<NodeTypeLeaf, CLONE>;
pub(crate) type BranchNode<const CLONE: bool = true> = Node<NodeTypeBranch, CLONE>;
pub(crate) type DirtyLeafNode<'node> = DirtyNode<'node, NodeTypeLeaf>;
pub(crate) type DirtyBranchNode<'node> = DirtyNode<'node, NodeTypeBranch>;

#[derive(Debug)]
struct InlineOffset<'node, Repr: NodeRepr> {
    key_len: usize,
    value_len: usize,
    repr: std::marker::PhantomData<Repr>,
    bytes: &'node [u8],
    len: usize,
}

impl<'node, TYPE: NodeRepr> InlineOffset<'node, TYPE> {
    #[inline]
    fn unit_size(&self) -> usize {
        match TYPE::N_VAR {
            1 => TYPE::repr_size() + self.key_len,
            2 => TYPE::repr_size() + self.key_len + self.value_len,
            _ => unimplemented!(),
        }
    }

    #[inline]
    fn repr(&self, i: usize) -> &'node TYPE::Repr {
        let start = self.unit_size() * i;
        Ref::into_ref(
            Ref::<_, TYPE::Repr>::from_bytes(&self.bytes[start..][..TYPE::repr_size()]).unwrap(),
        )
    }

    #[inline]
    fn key(&self, i: usize) -> &'node [u8] {
        let start = self.unit_size() * i + size_of::<TYPE::Repr>();
        &self.bytes[start..][..self.key_len]
    }

    #[inline]
    fn triplet(&self, i: usize) -> (&'node TYPE::Repr, &'node [u8], &'node [u8]) {
        assert_eq!(TYPE::N_VAR, 2);
        let rkv = &self.bytes[self.unit_size() * i..][..self.unit_size()];
        let (r, kv) = rkv.split_at(TYPE::repr_size());
        let (k, v) = kv.split_at(self.key_len);
        let repr = Ref::into_ref(Ref::<_, TYPE::Repr>::from_bytes(r).unwrap());
        (repr, k, v)
    }

    #[inline]
    fn head_size(&self) -> usize {
        self.unit_size() * self.len
    }

    #[inline]
    fn split_at(&self, split_point: usize) -> (Self, Self) {
        let (left, right) = self.bytes.split_at(self.unit_size() * split_point);
        (
            Self {
                key_len: self.key_len,
                value_len: self.value_len,
                repr: self.repr,
                bytes: left,
                len: split_point,
            },
            Self {
                key_len: self.key_len,
                value_len: self.value_len,
                repr: self.repr,
                bytes: right,
                len: self.len - split_point,
            },
        )
    }

    fn binary_search(&self, search_key: &[u8]) -> Result<usize, usize> {
        let unit_size = self.unit_size();
        let _bounds_check = &self.bytes[..self.len * unit_size];
        // copied from stdlib but stripped of some comments
        let mut size = self.len;
        let mut left = 0;
        let mut right = size;
        while left < right {
            // SAFETY: the while condition means `size` is strictly positive, so
            // `size/2 < size`. Thus `left + size/2 < left + size`, which
            // coupled with the `left + size <= self.len()` invariant means
            // we have `left + size/2 < self.len()`, and this is in-bounds.
            let mid = left + size / 2;
            let k_start = unit_size * mid + size_of::<TYPE::Repr>();
            let key = unsafe { self.bytes.get_unchecked(k_start..k_start + self.key_len) };
            let cmp = key.cmp(search_key);

            left = if cmp == Ordering::Less { mid + 1 } else { left };
            right = if cmp == Ordering::Greater { mid } else { right };
            if cmp == Ordering::Equal {
                return Ok(mid);
            }

            size = right - left;
        }
        Err(left)
    }
}

#[derive(Debug)]
struct ExternalOffset<'node, Repr> {
    offsets: &'node [Offset],
    page_bytes: &'node [u8],
    repr: std::marker::PhantomData<Repr>,
}

impl<'node, TYPE: NodeRepr> ExternalOffset<'node, TYPE> {
    #[inline]
    fn var_repr(&self, i: usize) -> &'node [VarRepr] {
        let offset = self.offsets[i].offset as usize;
        Ref::into_ref(
            Ref::<_, [VarRepr]>::from_bytes(
                &self.page_bytes[offset..][..size_of::<VarRepr>() * TYPE::N_VAR],
            )
            .unwrap(),
        )
    }

    #[inline]
    fn repr(&self, i: usize) -> &'node TYPE::Repr {
        let start = self.offsets[i].offset as usize + size_of::<VarRepr>() * TYPE::N_VAR;
        Ref::into_ref(
            Ref::<_, TYPE::Repr>::from_bytes(&self.page_bytes[start..][..size_of::<TYPE::Repr>()])
                .unwrap(),
        )
    }

    #[inline]
    fn key(&self, i: usize) -> &'node [u8] {
        let var_reprs = self.var_repr(i);
        let start = self.offsets[i].offset as usize
            + size_of::<VarRepr>() * TYPE::N_VAR
            + size_of::<TYPE::Repr>();
        &self.page_bytes[start..][..var_reprs[0].len as usize]
    }

    #[inline]
    fn head_size(&self) -> usize {
        self.offsets.as_bytes().len()
    }

    fn head_tail_size(&self, prefix_delta: &PrefixDelta<'_>) -> usize {
        let mut size =
            (size_of::<Offset>() + size_of::<VarRepr>() * TYPE::N_VAR + size_of::<TYPE::Repr>())
                * self.offsets.len();
        size += (0..self.offsets.len())
            .map(|i| {
                self.var_repr(i)
                    .iter()
                    .map(|a| a.len as usize)
                    .sum::<usize>()
            })
            .sum::<usize>();
        match prefix_delta {
            Ok(i) => {
                size -= i * self.offsets.len();
            }
            Err(p) => {
                size += p.len() * self.offsets.len();
            }
        }
        size
    }

    fn iter_tail_data(
        &self,
        start: usize,
        end: usize,
    ) -> impl Iterator<Item = (u32, &'node [u8])> + '_ {
        self.offsets[start..end].iter().map(move |o| {
            let bytes = &self.page_bytes[o.offset as usize..];
            let var_reprs = Ref::into_ref(
                Ref::<_, [VarRepr]>::from_bytes(&bytes[..size_of::<VarRepr>() * TYPE::N_VAR])
                    .unwrap(),
            );
            let byte_len = size_of::<VarRepr>() * TYPE::N_VAR
                + size_of::<TYPE::Repr>()
                + var_reprs.iter().map(|a| a.len as usize).sum::<usize>();
            (o.prefix, &bytes[..byte_len])
        })
    }
    fn iter_tail_data_split(
        &self,
        start: usize,
        end: usize,
    ) -> impl Iterator<Item = (&'node [u8], &'node [u8])> + '_ {
        self.offsets[start..end].iter().map(move |o| {
            let bytes = &self.page_bytes[o.offset as usize..];
            let var_reprs = Ref::into_ref(
                Ref::<_, [VarRepr]>::from_bytes(&bytes[..size_of::<VarRepr>() * TYPE::N_VAR])
                    .unwrap(),
            );
            let prefix_len = size_of::<VarRepr>() * TYPE::N_VAR + size_of::<TYPE::Repr>();
            let suffix_len = var_reprs.iter().map(|a| a.len as usize).sum::<usize>();
            bytes[..prefix_len + suffix_len].split_at(prefix_len)
        })
    }
    #[inline]
    fn split_at(&self, split_point: usize) -> (Self, Self) {
        let (left, right) = self.offsets.split_at(split_point);
        (
            Self {
                offsets: left,
                repr: self.repr,
                page_bytes: self.page_bytes,
            },
            Self {
                offsets: right,
                repr: self.repr,
                page_bytes: self.page_bytes,
            },
        )
    }

    fn binary_search(&self, search_key: &[u8]) -> Result<usize, usize> {
        let search_prefix = read_prefix_u32(search_key);
        self.offsets.binary_search_by(move |o| {
            match { o.prefix }.cmp(&search_prefix) {
                Ordering::Equal => (),
                ord => return ord,
            }

            let (var_reprs, after) = Ref::<_, [VarRepr]>::from_prefix_with_elems(
                &self.page_bytes[o.offset as usize..],
                TYPE::N_VAR,
            )
            .unwrap();
            let key_len = Ref::into_ref(var_reprs)[0].len;
            let key = &after[TYPE::repr_size()..][..key_len as usize];
            debug_assert_eq!(read_prefix_u32(key), { o.prefix });
            key.cmp(search_key)
        })
    }

    fn triplet(&self, i: usize) -> (&'node TYPE::Repr, &'node [u8], &'node [u8]) {
        assert_eq!(TYPE::N_VAR, 2);
        let var_reprs: &[VarRepr; 2] = self.var_repr(i).try_into().unwrap();
        let start = self.offsets[i].offset as usize + size_of::<[VarRepr; 2]>();
        let rkv = &self.page_bytes[start..]
            [..size_of::<TYPE::Repr>() + var_reprs[0].len as usize + var_reprs[1].len as usize];
        let (r, kv) = rkv.split_at(size_of::<TYPE::Repr>());
        let (k, v) = kv.split_at(var_reprs[0].len as usize);
        let repr = Ref::into_ref(Ref::<_, TYPE::Repr>::from_bytes(r).unwrap());
        (repr, k, v)
    }
}

#[derive(Debug)]
enum Offsets<'node, TYPE: NodeRepr> {
    External(ExternalOffset<'node, TYPE>),
    Inline(InlineOffset<'node, TYPE>),
}

#[derive(Debug)]
struct OffsetsMut<'a, 'node, TYPE: NodeRepr> {
    node: &'a mut DirtyNode<'node, TYPE>,
}

#[inline]
fn read_prefix_u32(bytes: &[u8]) -> u32 {
    match bytes.len() {
        4.. => u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        2 | 3 => {
            u32::from_be_bytes([bytes[0], bytes[1], 0, 0])
                | u32::from_be_bytes([0, 0, bytes.get(2).copied().unwrap_or(0), 0])
        }
        1 => u32::from_be_bytes([bytes[0], 0, 0, 0]),
        0 => 0,
    }
}

impl<'node, TYPE: NodeRepr> Offsets<'node, TYPE> {
    fn new<const CLONE: bool>(node: &'node Node<TYPE, CLONE>) -> Self {
        Self::from_bytes(node.node_header(), &node.page.raw_data, 0, node.num_keys())
    }

    fn from_bytes(header: &NodeHeader, page_bytes: &'node [u8], start: usize, end: usize) -> Self {
        let node_header_size = Node::<TYPE>::static_header_size(header.key_prefix_len as usize);
        if header.fixed_key_len < 0 {
            let offsets = Ref::into_ref(
                Ref::<_, [Offset]>::from_bytes(
                    &page_bytes[node_header_size..]
                        [size_of::<Offset>() * start..size_of::<Offset>() * end],
                )
                .unwrap(),
            );
            Offsets::External(ExternalOffset {
                offsets,
                repr: std::marker::PhantomData,
                page_bytes,
            })
        } else {
            debug_assert!(header.fixed_value_len >= 0);
            let mut inline_len = TYPE::repr_size() + header.fixed_key_len as usize;
            if TYPE::N_VAR == 2 {
                inline_len += header.fixed_value_len as usize;
            }
            let inline_bytes =
                &page_bytes[node_header_size..][inline_len * start..inline_len * end];
            Offsets::Inline(InlineOffset {
                key_len: header.fixed_key_len as usize,
                value_len: header.fixed_value_len as usize,
                repr: std::marker::PhantomData,
                bytes: inline_bytes,
                len: end - start,
            })
        }
    }

    fn key(&self, i: usize) -> &'node [u8] {
        match self {
            Offsets::External(e) => e.key(i),
            Offsets::Inline(n) => n.key(i),
        }
    }

    fn triplet(&self, i: usize) -> (&'node TYPE::Repr, &'node [u8], &'node [u8]) {
        assert_eq!(TYPE::N_VAR, 2);
        match self {
            Offsets::External(e) => e.triplet(i),
            Offsets::Inline(it) => it.triplet(i),
        }
    }

    fn repr(&self, i: usize) -> &'node TYPE::Repr {
        match self {
            Offsets::External(e) => e.repr(i),
            Offsets::Inline(n) => n.repr(i),
        }
    }

    fn head_size(&self) -> usize {
        match self {
            Offsets::External(e) => e.head_size(),
            Offsets::Inline(n) => n.head_size(),
        }
    }

    fn head_tail_size(&self, prefix_delta: &PrefixDelta<'_>) -> usize {
        match self {
            Offsets::External(e) => e.head_tail_size(prefix_delta),
            Offsets::Inline(n) => {
                debug_assert_eq!(prefix_delta, &PrefixDelta::Ok(0));
                n.head_size()
            }
        }
    }

    fn split_at(&self, split_point: usize) -> (Self, Self) {
        match self {
            Offsets::External(e) => {
                let (left, right) = e.split_at(split_point);
                (Offsets::External(left), Offsets::External(right))
            }
            Offsets::Inline(n) => {
                let (left, right) = n.split_at(split_point);
                (Offsets::Inline(left), Offsets::Inline(right))
            }
        }
    }

    fn binary_search_by(&self, search_key: &[u8]) -> Result<usize, usize> {
        match self {
            Offsets::External(e) => e.binary_search(search_key),
            Offsets::Inline(n) => n.binary_search(search_key),
        }
    }
}

impl<TYPE: NodeRepr> OffsetsMut<'_, '_, TYPE> {
    fn compact_tail(&mut self, scratch: &mut Vec<u8>) {
        if !self.is_external() {
            return;
        }
        // grow scratch if needed
        let scratch_needed = self.node.tail_real_size();
        scratch.resize(scratch.len().max(scratch_needed), 0);
        let scratch = &mut scratch[..scratch_needed];
        let header_size = self.node.header_size();
        let num_keys = self.node.num_keys();
        let mut new_offset = self.node.data().len();
        let head_len = self.node.head_size();
        let (head_data, tail_data) = self.node.data_mut().split_at_mut(head_len);
        let mut range_to_copy = new_offset..new_offset;
        let mut scratch_range = scratch.len()..scratch.len();
        let offsets = Ref::into_mut(
            Ref::<_, [Offset]>::from_bytes(
                &mut head_data[header_size..][..size_of::<Offset>() * num_keys],
            )
            .unwrap(),
        );
        for offset_ptr in offsets.iter_mut() {
            let tail_offset = offset_ptr.offset as usize - head_len;
            let repr_total_len = {
                let var_repr = Ref::into_ref(
                    Ref::<_, [VarRepr]>::from_bytes(
                        &tail_data[tail_offset..][..size_of::<VarRepr>() * TYPE::N_VAR],
                    )
                    .unwrap(),
                );
                size_of::<VarRepr>() * TYPE::N_VAR
                    + TYPE::repr_size()
                    + var_repr.iter().map(|l| l.len as usize).sum::<usize>()
            };
            if offset_ptr.offset as usize + repr_total_len == range_to_copy.start {
                range_to_copy.start = offset_ptr.offset as usize;
            } else {
                let tail_range = range_to_copy.start - head_len..range_to_copy.end - head_len;
                if tail_range.end == tail_data.len()
                    && scratch_range.start + tail_range.len() == scratch.len()
                {
                    // a suffix of the tail is already correct, do not copy it
                    scratch_range.end = scratch_range.start;
                } else {
                    scratch[scratch_range.start..][..tail_range.len()]
                        .copy_from_slice(&tail_data[tail_range]);
                }
                range_to_copy =
                    offset_ptr.offset as usize..offset_ptr.offset as usize + repr_total_len;
            }
            scratch_range.start -= repr_total_len;
            new_offset -= repr_total_len;
            offset_ptr.offset = new_offset as u32;
        }

        let tail_range = range_to_copy.start - head_len..range_to_copy.end - head_len;
        scratch[scratch_range.start..][..tail_range.len()].copy_from_slice(&tail_data[tail_range]);

        tail_data[new_offset - head_len..][..scratch_range.len()]
            .copy_from_slice(&scratch[scratch_range]);
        self.node.header_mut().tail_curr_size = (self.node.page.data().len() - new_offset) as u32;
        assert_eq!(
            self.node.header().tail_curr_size,
            self.node.header().tail_real_size
        );
    }

    fn copy_reprs_from_external(
        &mut self,
        key_prefix_delta: &PrefixDelta<'_>,
        other: &ExternalOffset<'_, TYPE>,
        from: usize,
        to: usize,
    ) {
        let old_num_keys = self.node.num_keys();
        let new_num_keys = old_num_keys + (to - from);
        let header_size = self.node.header_size();
        let head_len = header_size + size_of::<Offset>() * new_num_keys;
        let mut offset = self.node.page.data().len() - self.node.tail_curr_size();
        let (head_data, tail_data) = self.node.data_mut().split_at_mut(head_len);
        let offsets =
            Ref::into_mut(Ref::<_, [Offset]>::from_bytes(&mut head_data[header_size..]).unwrap());
        if *key_prefix_delta == Ok(0) {
            for (offset_ptr, (o_prefix, part)) in &mut offsets[old_num_keys..]
                .iter_mut()
                .zip(other.iter_tail_data(from, to))
            {
                offset -= part.len();
                offset_ptr.offset = offset as u32;
                offset_ptr.prefix = o_prefix;
                tail_data[offset - head_len..][..part.len()].copy_from_slice(part);
            }
        } else if let Ok(key_prefix_elided) = *key_prefix_delta {
            for (offset_ptr, (prefix, suffix)) in &mut offsets[old_num_keys..]
                .iter_mut()
                .zip(other.iter_tail_data_split(from, to))
            {
                let part_len = prefix.len() + suffix.len() - key_prefix_elided;
                offset -= part_len;
                let (prefix_mut, suffix_mut) =
                    tail_data[offset - head_len..][..part_len].split_at_mut(prefix.len());
                prefix_mut.copy_from_slice(prefix);
                let var_reprs = Ref::into_mut(
                    Ref::<_, [VarRepr]>::from_prefix_with_elems(prefix_mut, TYPE::N_VAR)
                        .unwrap()
                        .0,
                );
                var_reprs[0].len -= key_prefix_elided as u32;
                suffix_mut.copy_from_slice(&suffix[key_prefix_elided..]);
                offset_ptr.prefix = read_prefix_u32(&suffix_mut[..var_reprs[0].len as usize]);
                offset_ptr.offset = offset as u32;
            }
        } else {
            let key_prefix_added = key_prefix_delta.unwrap_err();
            for (offset_ptr, (prefix, suffix)) in &mut offsets[old_num_keys..]
                .iter_mut()
                .zip(other.iter_tail_data_split(from, to))
            {
                let part_len = prefix.len() + suffix.len() + key_prefix_added.len();
                offset -= part_len;
                let (prefix_mut, suffix_mut) =
                    tail_data[offset - head_len..][..part_len].split_at_mut(prefix.len());
                prefix_mut.copy_from_slice(prefix);
                let var_reprs = Ref::into_mut(
                    Ref::<_, [VarRepr]>::from_prefix_with_elems(prefix_mut, TYPE::N_VAR)
                        .unwrap()
                        .0,
                );
                var_reprs[0].len += key_prefix_added.len() as u32;
                suffix_mut[..key_prefix_added.len()].copy_from_slice(key_prefix_added);
                suffix_mut[key_prefix_added.len()..].copy_from_slice(suffix);
                offset_ptr.prefix = read_prefix_u32(&suffix_mut[..var_reprs[0].len as usize]);
                offset_ptr.offset = offset as u32;
            }
        }
        let new_tail_size = self.node.page.data().len() - offset;
        self.node.header_mut().tail_real_size +=
            (new_tail_size - self.node.tail_curr_size()) as u32;
        self.node.header_mut().tail_curr_size = new_tail_size as u32;
        assert!(self.node.head_size() + self.node.tail_curr_size() <= self.node.page_size());
    }

    fn copy_reprs_from_inline<Repr: NodeRepr>(
        &mut self,
        other: &InlineOffset<'_, Repr>,
        from: usize,
        to: usize,
    ) {
        debug_assert!(!self.is_external());
        let unit_size = self.inline_unit_size();
        let repr_end = self.node.header_size() + unit_size * self.node.num_keys();
        let to_copy = &other.bytes[from * unit_size..to * unit_size];
        self.node.data_mut()[repr_end..][..to_copy.len()].copy_from_slice(to_copy);
    }

    fn copy_reprs_from(
        &mut self,
        key_prefix_delta: &PrefixDelta<'_>,
        other: &Offsets<'_, TYPE>,
        from: usize,
        to: usize,
    ) {
        match other {
            Offsets::External(e) => self.copy_reprs_from_external(key_prefix_delta, e, from, to),
            Offsets::Inline(i) => self.copy_reprs_from_inline(i, from, to),
        }
    }

    fn remove(&mut self, i: usize) {
        if self.is_external() {
            let external_offsets = self.external_offsets();
            let start = external_offsets[i].offset as usize;
            external_offsets.copy_within(i + 1.., i);
            let removed_len = {
                let var_repr = Ref::into_ref(
                    Ref::<_, [VarRepr]>::from_bytes(
                        &self.node.page.raw_data[start..][..size_of::<VarRepr>() * TYPE::N_VAR],
                    )
                    .unwrap(),
                );
                size_of::<VarRepr>() * TYPE::N_VAR
                    + TYPE::repr_size()
                    + var_repr.iter().map(|l| l.len as usize).sum::<usize>()
            };
            if self.node.header().tail_real_size == self.node.header().tail_curr_size
                && self.node.page_size() - start == self.node.header().tail_curr_size as usize
            {
                self.node.node_header_mut().tail_curr_size -= removed_len as u32;
            }
            self.node.node_header_mut().tail_real_size -= removed_len as u32;
        } else {
            let unit_size = self.inline_unit_size();
            let header_size = self.node.header_size();
            let repr_end = header_size + unit_size * self.node.num_keys();
            let deleted_unit_start = header_size + unit_size * i;
            self.node
                .data_mut()
                .copy_within(deleted_unit_start + unit_size..repr_end, deleted_unit_start);
        }
    }

    #[inline]
    fn is_external(&mut self) -> bool {
        self.node.node_header().fixed_key_len < 0
    }

    #[inline]
    fn external_offsets(&mut self) -> &mut [Offset] {
        self.external_offsets_with_len(self.node.num_keys())
    }

    #[inline]
    fn external_offsets_with_len(&mut self, len: usize) -> &mut [Offset] {
        let header_size = self.node.header_size();
        Ref::into_mut(
            Ref::<_, [Offset]>::from_bytes(
                &mut self.node.data_mut()[header_size..][..size_of::<Offset>() * len],
            )
            .unwrap(),
        )
    }

    #[inline]
    fn inline_unit_size(&mut self) -> usize {
        TYPE::repr_size()
            + self.node.node_header().fixed_key_len as usize
            + if TYPE::N_VAR == 2 {
                self.node.node_header().fixed_value_len as usize
            } else {
                0
            }
    }

    fn insert_k(&mut self, i: usize, k: &[u8], repr: TYPE::Repr) {
        debug_assert_eq!(TYPE::N_VAR, 1);
        debug_assert!(i <= self.node.num_keys());
        let mut write_area = if self.is_external() {
            let tail_total_len = size_of::<VarRepr>() + TYPE::repr_size() + k.len();
            let end = self.node.page_size() - self.node.header_mut().tail_curr_size as usize;
            let start = end - tail_total_len;
            debug_assert!(start >= self.node.head_size() + size_of::<Offset>());
            self.node.header_mut().tail_curr_size += tail_total_len as u32;
            self.node.header_mut().tail_real_size += tail_total_len as u32;
            let offsets = self.external_offsets_with_len(self.node.num_keys() + 1);
            offsets.copy_within(i..offsets.len() - 1, i + 1);
            offsets[i] = Offset {
                offset: start as u32,
                prefix: read_prefix_u32(k),
            };
            let mut write_area = &mut self.node.data_mut()[start..end];
            write_area
                .write_all(
                    VarRepr {
                        len: k.len() as u32,
                    }
                    .as_bytes(),
                )
                .unwrap();
            write_area
        } else {
            let unit_size = self.inline_unit_size();
            let header_size = self.node.header_size();
            let repr_end = header_size + unit_size * self.node.num_keys();
            let start = header_size + unit_size * i;
            debug_assert_eq!(k.len(), self.node.node_header().fixed_key_len as usize);
            let end = start + TYPE::repr_size() + self.node.node_header().fixed_key_len as usize;
            let raw_data_mut = self.node.data_mut();
            raw_data_mut.copy_within(start..repr_end, end);
            &mut raw_data_mut[start..end]
        };
        write_area.write_all(repr.as_bytes()).unwrap();
        write_area.write_all(k).unwrap();
        debug_assert!(write_area.is_empty());
    }

    fn insert_kv(&mut self, i: usize, k: &[u8], v: &MaybeValue, repr: TYPE::Repr) {
        debug_assert_eq!(TYPE::N_VAR, 2);
        debug_assert!(i <= self.node.num_keys());
        let mut write_area = if self.is_external() {
            let v_len = v.repr_len();
            let tail_total_len = size_of::<[VarRepr; 2]>() + TYPE::repr_size() + k.len() + v_len;
            let end = self.node.page_size() - self.node.header_mut().tail_curr_size as usize;
            let start = end - tail_total_len;
            debug_assert!(start >= self.node.head_size() + size_of::<Offset>());
            self.node.header_mut().tail_curr_size += tail_total_len as u32;
            self.node.header_mut().tail_real_size += tail_total_len as u32;
            let offsets = self.external_offsets_with_len(self.node.num_keys() + 1);
            offsets.copy_within(i..offsets.len() - 1, i + 1);
            offsets[i] = Offset {
                offset: start as u32,
                prefix: read_prefix_u32(k),
            };
            let mut write_area = &mut self.node.data_mut()[start..end];
            for len in [k.len() as u32, v_len as u32] {
                write_area.write_all(VarRepr { len }.as_bytes()).unwrap();
            }
            write_area
        } else {
            let unit_size = self.inline_unit_size();
            let header_size = self.node.header_size();
            let repr_end = header_size + unit_size * self.node.num_keys();
            let start = header_size + unit_size * i;
            let node_header = self.node.node_header();
            debug_assert_eq!(k.len(), node_header.fixed_key_len as usize);
            let end = start + TYPE::repr_size() + node_header.fixed_key_len as usize + v.repr_len();
            #[cfg(debug_assertions)]
            match v {
                MaybeValue::Bytes(b) => {
                    assert_eq!(b.len(), node_header.fixed_value_len as usize)
                }
                MaybeValue::Overflow(_) => unreachable!(),
                MaybeValue::Delete => (),
            }
            let raw_data_mut = self.node.data_mut();
            raw_data_mut.copy_within(start..repr_end, end);
            &mut raw_data_mut[start..end]
        };
        write_area.write_all(repr.as_bytes()).unwrap();
        write_area.write_all(k).unwrap();
        write_area.write_all(v.repr_bytes()).unwrap();
        debug_assert!(write_area.is_empty());
    }

    fn update_v(&mut self, i: usize, v: &MaybeValue, repr: TYPE::Repr) -> Result<(), usize> {
        debug_assert_eq!(TYPE::N_VAR, 2);
        debug_assert!(i < self.node.num_keys());
        let (mut write_area, k_len) = if self.is_external() {
            let offsets = self.external_offsets();
            let start = offsets[i].offset as usize;
            let var_reprs = Ref::into_mut(
                Ref::<_, [VarRepr; 2]>::from_bytes(
                    &mut self.node.data_mut()[start..][..size_of::<[VarRepr; 2]>()],
                )
                .unwrap(),
            );
            let v_len = v.repr_len();
            let prev_v_len = var_reprs[1].len as usize;
            if prev_v_len < v_len {
                return Err(prev_v_len);
            }
            var_reprs[1].len = v_len as u32;
            let k_len = var_reprs[0].len as usize;
            self.node.header_mut().tail_real_size -= (prev_v_len - v_len) as u32;
            let start = start + size_of::<[VarRepr; 2]>();
            let end = start + TYPE::repr_size() + k_len + v_len;
            (&mut self.node.data_mut()[start..end], k_len)
        } else {
            let start = self.node.header_size() + self.inline_unit_size() * i;
            let node_header = self.node.node_header();
            let end = start + TYPE::repr_size() + node_header.fixed_key_len as usize + v.repr_len();
            #[cfg(debug_assertions)]
            match v {
                MaybeValue::Bytes(b) => {
                    assert_eq!(b.len(), node_header.fixed_value_len as usize)
                }
                MaybeValue::Overflow(_) => unreachable!(),
                MaybeValue::Delete => (),
            }
            let k_len = node_header.fixed_key_len as usize;
            (&mut self.node.data_mut()[start..end], k_len)
        };
        write_area.write_all(repr.as_bytes()).unwrap();
        write_area = &mut write_area[k_len..]; // skip over key
        write_area.write_all(v.repr_bytes()).unwrap();
        debug_assert!(write_area.is_empty());
        Ok(())
    }

    fn set_repr_mut(&mut self, i: usize, repr: TYPE::Repr) {
        debug_assert!(i < self.node.num_keys());
        let start = if self.is_external() {
            self.external_offsets()[i].offset as usize + size_of::<VarRepr>() * TYPE::N_VAR
        } else {
            self.node.header_size() + self.inline_unit_size() * i
        };
        let repr_mut = Ref::into_mut(
            Ref::<_, TYPE::Repr>::from_bytes(
                &mut self.node.data_mut()[start..][..TYPE::repr_size()],
            )
            .unwrap(),
        );
        *repr_mut = repr
    }
}

impl<TYPE: NodeType, const CLONE: bool> std::fmt::Debug for Node<TYPE, CLONE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut df = f.debug_struct("Node");
        df.field("page", &self.page);
        if self.is_leaf() {
            let leaf = self.as_leaf();
            df.field("header", &leaf.header());
            df.field("prefix", &EscapedBytes(leaf.key_prefix()));
            // df.field("offsets", &leaf.offsets());
            for i in 0..leaf.num_keys() {
                let (k, v) = leaf.key_value_at(i);
                df.field(&format!("kv {i}"), &(&EscapedBytes(k), v));
            }
        }
        if self.is_branch() {
            let branch = self.as_branch();
            df.field("header", &branch.header());
            df.field("prefix", &EscapedBytes(branch.key_prefix()));
            // df.field("offsets", &branch.offsets());
            for i in 0..branch.num_keys() + 1 {
                df.field(&format!("ptr {i}"), &branch.pointer_at(i));
                if i < self.num_keys() {
                    df.field(&format!("key {i}"), &EscapedBytes(branch.key_at(i)));
                }
            }
        }
        df.finish()
    }
}

impl NodeRepr for NodeTypeLeaf {
    type Header = LeafHeader;
    type Repr = LeafPairRepr;
    const N_VAR: usize = 2;
}

impl NodeRepr for NodeTypeBranch {
    type Header = BranchHeader;
    type Repr = BranchKeyRepr;
    const N_VAR: usize = 1;
}

impl UntypedNode {
    pub fn from_page(page: Page) -> Result<Self, Error> {
        Ok(Node {
            page,
            ty: std::marker::PhantomData,
        })
    }
}

impl<TYPE: NodeType, const CLONE: bool> Node<TYPE, CLONE> {
    #[inline]
    pub fn make_dirty(&mut self) -> DirtyNode<'_, TYPE> {
        self.page.raw_data.make_unique();
        self.page.dirty = true;
        DirtyNode {
            node: unsafe { mem::transmute(self) },
        }
    }

    #[inline]
    pub fn into_page(self) -> Page<CLONE> {
        self.page
    }

    #[inline]
    fn as_not_clone(&self) -> &Node<TYPE, false> {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    pub fn as_leaf(&self) -> &LeafNode<CLONE> {
        assert!(self.is_leaf());
        unsafe { mem::transmute(self) }
    }

    #[inline]
    pub fn as_branch(&self) -> &BranchNode<CLONE> {
        assert!(self.is_branch());
        unsafe { mem::transmute(self) }
    }

    #[inline]
    pub fn as_dirty(&mut self) -> DirtyNode<'_, TYPE> {
        assert!(self.dirty);
        assert!(self.page.raw_data.is_unique());
        DirtyNode {
            node: unsafe { mem::transmute(self) },
        }
    }

    #[inline]
    pub fn into_untyped(self) -> UntypedNode {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    pub fn into_leaf(self) -> LeafNode {
        assert!(self.is_leaf());
        unsafe { mem::transmute(self) }
    }

    #[inline]
    pub fn into_branch(self) -> BranchNode {
        assert!(self.is_branch());
        unsafe { mem::transmute(self) }
    }

    #[inline]
    pub fn page_size(&self) -> usize {
        self.page.data().len()
    }

    #[inline]
    pub fn node_header(&self) -> &NodeHeader {
        header_cast(&self.page)
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        TYPE::IS_LEAF || self.node_header().level == 0
    }

    #[inline]
    pub fn is_branch(&self) -> bool {
        TYPE::IS_BRANCH || self.node_header().level != 0
    }

    #[inline]
    pub fn num_keys(&self) -> usize {
        self.node_header().num_keys as usize
    }

    #[inline]
    pub fn key_prefix_len(&self) -> usize {
        self.node_header().key_prefix_len as usize
    }
}

impl<TYPE: NodeType> DirtyNode<'_, TYPE> {
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.node.page.raw_data.as_ref()
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8] {
        debug_assert!(self.node.page.dirty);
        debug_assert!(self.node.page.raw_data.is_unique());
        unsafe { self.node.page.raw_data.as_mut_unchecked() }
    }

    #[inline]
    pub fn page_mut(&mut self) -> &mut Page<false> {
        debug_assert!(self.node.page.dirty);
        debug_assert!(self.node.page.raw_data.is_unique());
        &mut self.node.page
    }

    #[inline]
    pub fn node_header_mut(&mut self) -> &mut NodeHeader {
        header_cast_mut(&mut self.node.page)
    }

    #[allow(dead_code)]
    #[inline]
    pub fn as_leaf(&mut self) -> DirtyLeafNode {
        assert!(self.is_leaf());
        unsafe { mem::transmute_copy(self) }
    }

    #[inline]
    pub fn as_branch(&mut self) -> DirtyBranchNode {
        assert!(self.is_branch());
        unsafe { mem::transmute_copy(self) }
    }
}

impl<TYPE: NodeRepr, const CLONE: bool> Node<TYPE, CLONE> {
    #[inline]
    pub fn new(tree_value: &TreeValue, level: u8, page: Page<CLONE>) -> Self {
        let mut node = Node {
            page,
            ty: std::marker::PhantomData,
        };
        let mut header = <TYPE as NodeRepr>::Header::default();
        header.level = level;
        header.fixed_key_len = tree_value.fixed_key_len;
        header.fixed_value_len = tree_value.fixed_value_len;
        header.page_header = *node.page.header();
        *node.as_dirty().header_mut() = header;
        node
    }

    #[inline]
    pub fn new_branch(tree_value: &TreeValue, level: u8, page: Page<CLONE>) -> BranchNode<CLONE> {
        debug_assert_ne!(level, 0);
        BranchNode::new(tree_value, level, page)
    }

    #[inline]
    pub fn new_leaf(tree_value: &TreeValue, page: Page<CLONE>) -> LeafNode<CLONE> {
        LeafNode::new(tree_value, 0, page)
    }

    #[inline]
    pub fn new_overflow(tree_value: &TreeValue, page: Page<CLONE>) -> LeafNode<CLONE> {
        let mut leaf = LeafNode::new(tree_value, 0, page);
        let mut left_mut = leaf.as_dirty();
        left_mut.node_header_mut().fixed_key_len = -1;
        left_mut.node_header_mut().fixed_value_len = -1;
        leaf
    }

    #[inline]
    fn repr_size() -> usize {
        TYPE::repr_size()
    }

    #[inline]
    pub fn header(&self) -> &<TYPE as NodeRepr>::Header {
        header_cast(&self.page)
    }

    #[inline]
    pub fn keys_split_sizes(
        &self,
        split_point: usize,
        left_prefix_delta: &PrefixDelta<'_>,
        right_prefix_delta: &PrefixDelta<'_>,
    ) -> (usize, usize) {
        let (offsets_left, offsets_right) = self.offsets().split_at(split_point);
        (
            offsets_left.head_tail_size(left_prefix_delta),
            offsets_right.head_tail_size(right_prefix_delta),
        )
    }

    #[inline]
    fn offsets(&self) -> Offsets<'_, TYPE> {
        Offsets::new(self)
    }

    #[inline]
    pub fn key_at(&self, i: usize) -> &[u8] {
        self.offsets().key(i)
    }

    #[inline]
    pub fn full_key_at(&self, i: usize) -> Cow<[u8]> {
        let suffix = self.offsets().key(i);
        if self.key_prefix_len() == 0 {
            Cow::Borrowed(suffix)
        } else {
            Cow::Owned([self.key_prefix(), suffix].concat())
        }
    }

    #[inline]
    pub fn curr_occupied_size(&self) -> usize {
        self.head_size() + self.tail_curr_size()
    }

    #[inline]
    pub fn real_occupied_size(&self) -> usize {
        self.head_size() + self.tail_real_size()
    }

    #[inline]
    pub fn real_occupied_size_with_prefix(&self, delta: &PrefixDelta<'_>) -> usize {
        let mut occupied = self.real_occupied_size();
        match delta {
            Ok(0) => (),
            Ok(i) => {
                occupied += i;
                occupied -= self.num_keys() * i;
            }
            Err(p) => {
                occupied -= p.len();
                occupied += self.num_keys() * p.len();
            }
        }
        occupied
    }

    #[inline]
    pub fn real_size_left(&self) -> usize {
        self.page_size() - self.real_occupied_size()
    }

    #[inline]
    pub fn curr_size_left(&self) -> usize {
        self.page_size() - self.curr_occupied_size()
    }

    #[inline]
    pub fn header_size(&self) -> usize {
        size_of::<TYPE::Header>() + self.node_header().key_prefix_len as usize
    }

    #[inline]
    pub fn static_header_size(key_prefix_len: usize) -> usize {
        size_of::<TYPE::Header>() + key_prefix_len
    }

    #[inline]
    fn head_size(&self) -> usize {
        self.header_size() + self.offsets().head_size()
    }

    #[inline]
    pub fn tail_real_size(&self) -> usize {
        debug_assert!(
            (self.header().tail_real_size as usize + self.head_size()) <= self.page_size()
        );
        self.header().tail_real_size as usize
    }

    #[inline]
    pub fn tail_curr_size(&self) -> usize {
        debug_assert!(
            (self.header().tail_curr_size as usize + self.head_size()) <= self.page_size()
        );
        self.header().tail_curr_size as usize
    }

    #[inline]
    pub fn key_prefix(&self) -> &[u8] {
        &self.page.split_off::<<TYPE as NodeRepr>::Header>()[..self.key_prefix_len()]
    }

    pub fn search_keys(&self, mut search_key: &[u8]) -> Result<usize, usize> {
        let prefix = self.key_prefix();
        if !prefix.is_empty() {
            match search_key[..prefix.len().min(search_key.len())].cmp(prefix) {
                Ordering::Equal => (),
                Ordering::Less => return Err(0),
                Ordering::Greater => return Err(self.num_keys()),
            }
            search_key = &search_key[prefix.len()..];
        }
        self.offsets().binary_search_by(search_key)
    }
}

impl<'node, TYPE: NodeRepr> DirtyNode<'node, TYPE> {
    #[inline]
    fn header_mut(&mut self) -> &mut TYPE::Header {
        header_cast_mut(&mut self.node.page)
    }

    #[inline]
    fn offsets_mut(&mut self) -> OffsetsMut<'_, 'node, TYPE> {
        OffsetsMut { node: self }
    }

    #[inline]
    pub fn set_key_prefix(&mut self, prefix: &[u8]) {
        debug_assert!(prefix.len() <= MAX_PREFIX_SIZE);
        debug_assert_eq!(self.key_prefix_len(), 0);
        debug_assert_eq!(self.num_keys(), 0);
        self.header_mut().key_prefix_len = prefix.len() as u16;
        self.data_mut()[size_of::<TYPE::Header>()..][..prefix.len()].copy_from_slice(prefix);
    }

    fn copy_reprs_from(
        &mut self,
        key_prefix_delta: &PrefixDelta<'_>,
        other: &Node<TYPE>,
        start: usize,
        end: usize,
    ) {
        if key_prefix_delta.is_ok() {
            debug_assert!(self.key_prefix().starts_with(other.key_prefix()));
        }
        self.offsets_mut()
            .copy_reprs_from(key_prefix_delta, &other.offsets(), start, end);
    }

    pub fn copy_keys_from(
        &mut self,
        key_prefix_delta: &PrefixDelta<'_>,
        other: &Node<TYPE>,
        start: usize,
        end: usize,
    ) {
        let old_num_keys = self.num_keys();
        let new_num_keys = old_num_keys + (end - start);
        if self.is_branch() {
            // TODO: move this part to callers
            self.as_branch()
                .set_pointer_at(old_num_keys, other.as_branch().pointer_at(start));
        }
        self.copy_reprs_from(key_prefix_delta, other, start, end);
        self.header_mut().num_keys = new_num_keys as u16;
    }

    pub fn copy_everything_with_prefix(
        &mut self,
        other: &Node<TYPE>,
        key_prefix: &[u8],
        key_prefix_delta: &PrefixDelta<'_>,
    ) {
        debug_assert_eq!(self.num_keys(), 0);
        debug_assert_eq!(self.key_prefix_len(), 0);
        self.set_key_prefix(key_prefix);
        self.copy_keys_from(key_prefix_delta, other, 0, other.num_keys());
        debug_assert_eq!(self.real_size_left(), self.curr_size_left());
    }

    pub fn compact_tail(&mut self, scratch: &mut Vec<u8>) {
        self.offsets_mut().compact_tail(scratch);
    }

    pub fn remove_key_repr_at(&mut self, idx: usize) {
        debug_assert!(idx < self.num_keys());
        self.offsets_mut().remove(idx);
        self.header_mut().num_keys -= 1;
    }
}

impl<const CLONE: bool> BranchNode<CLONE> {
    #[inline]
    pub fn size_for_kp(tree_value: &TreeValue, k: &[u8]) -> usize {
        Self::repr_size()
            + if tree_value.fixed_key_len < 0 {
                size_of::<Offset>() + size_of::<VarRepr>() + k.len()
            } else {
                debug_assert_eq!(k.len(), tree_value.fixed_key_len as usize);
                tree_value.fixed_key_len as usize
            }
    }
    #[inline]
    pub fn node_size_for_kp(&self, k: &[u8]) -> usize {
        let node_header = self.node_header();
        Self::repr_size()
            + if node_header.fixed_key_len < 0 {
                size_of::<Offset>() + size_of::<VarRepr>() + k.len()
            } else {
                debug_assert_eq!(k.len(), node_header.fixed_key_len as usize);
                node_header.fixed_key_len as usize
            }
    }

    #[inline]
    pub fn root_size_for(tree_value: &TreeValue, k: &[u8]) -> usize {
        Self::static_header_size(0) + Self::size_for_kp(tree_value, k)
    }

    pub fn children(&self) -> impl Iterator<Item = PageId> + '_ {
        let offsets = self.offsets();
        (0..self.num_keys() + 1).map(move |i| {
            if i == 0 {
                self.header().leftmost_pointer
            } else {
                offsets.repr(i - 1).child
            }
        })
    }

    pub fn search_child(&self, search_key: &[u8]) -> (usize, PageId) {
        let offsets = self.offsets();
        let prefix = self.key_prefix();
        let prefix_search = if !prefix.is_empty() {
            match search_key[..prefix.len().min(search_key.len())].cmp(prefix) {
                Ordering::Equal => Ok(()),
                Ordering::Less => Err(0),
                Ordering::Greater => Err(self.num_keys()),
            }
        } else {
            Ok(())
        };
        let search = match prefix_search {
            Ok(()) => {
                let search_key = &search_key[prefix.len()..];
                offsets.binary_search_by(search_key)
            }
            Err(i) => Err(i),
        };
        let idx = match search {
            Err(0) => return (0, self.header().leftmost_pointer),
            Err(i) => i,
            Ok(i) => i + 1,
        };
        (idx, offsets.repr(idx - 1).child)
    }

    #[inline]
    pub fn prefix_for_child(&self, child_idx: usize) -> NodePrefix<'_> {
        NodePrefix {
            parent: Some(self.as_not_clone()),
            child_idx,
        }
    }

    #[inline]
    pub fn pointer_at(&self, i: usize) -> PageId {
        if i == 0 {
            self.header().leftmost_pointer
        } else {
            self.offsets().repr(i - 1).child
        }
    }
}

impl DirtyBranchNode<'_> {
    #[inline]
    pub fn set_pointer_at(&mut self, i: usize, child: PageId) {
        if i == 0 {
            self.header_mut().leftmost_pointer = child;
        } else {
            self.offsets_mut()
                .set_repr_mut(i - 1, BranchKeyRepr { child });
        }
    }

    pub fn insert_kp(
        &mut self,
        location: Result<usize, usize>,
        key: &[u8],
        child: PageId,
    ) -> Result<(), FitError> {
        let needed = self.node_size_for_kp(key);
        if self.curr_size_left() < needed {
            return Err(FitError::WontFit);
        }
        match location {
            Err(i) => {
                self.offsets_mut().insert_k(i, key, BranchKeyRepr { child });
                self.node_header_mut().num_keys += 1;
            }
            Ok(_i) => unimplemented!(),
        }
        Ok(())
    }
}

impl<const CLONE: bool> LeafNode<CLONE> {
    #[inline]
    pub fn size_for_kv(tree_value: &TreeValue, k: &[u8], v: &MaybeValue) -> usize {
        Self::repr_size()
            + if tree_value.fixed_key_len < 0 {
                size_of::<Offset>() + size_of::<[VarRepr; 2]>() + k.len() + v.repr_len()
            } else {
                tree_value.fixed_key_len as usize + tree_value.fixed_value_len as usize
            }
    }

    #[inline]
    pub fn node_size_for_kv(&self, k: &[u8], v: &MaybeValue) -> usize {
        let header = self.node_header();
        Self::repr_size()
            + if header.fixed_key_len < 0 {
                size_of::<Offset>() + size_of::<[VarRepr; 2]>() + k.len() + v.repr_len()
            } else {
                header.fixed_key_len as usize + header.fixed_value_len as usize
            }
    }

    #[inline]
    pub fn leaf_size_for(
        tree_value: &TreeValue,
        key_prefix_len: usize,
        k: &[u8],
        v: &MaybeValue,
    ) -> usize {
        Self::static_header_size(key_prefix_len) + Self::size_for_kv(tree_value, k, v)
    }

    #[inline]
    pub fn overflow_children(&self) -> impl Iterator<Item = (PageId, PageId)> + '_ {
        self.overflow_children_with_idx().map(|(_, a, b)| (a, b))
    }

    pub fn overflow_children_with_idx(&self) -> impl Iterator<Item = (usize, PageId, PageId)> + '_ {
        let offsets = self.offsets();
        (0..self.num_keys()).filter_map(move |i| {
            let (repr2, _key, v) = offsets.triplet(i);
            if repr2.is_overflow() {
                let MaybeValue::Overflow([page_id, span]) = MaybeValue::overflow_from_bytes(v)
                else {
                    unreachable!()
                };
                Some((i, page_id, span))
            } else {
                None
            }
        })
    }

    #[inline]
    pub fn key_value_at(&self, i: usize) -> (&[u8], MaybeValue) {
        let (repr2, key, v) = self.offsets().triplet(i);
        let value = if repr2.is_overflow() {
            MaybeValue::overflow_from_bytes(v)
        } else {
            MaybeValue::Bytes(v)
        };
        (key, value)
    }

    #[inline]
    pub fn value_at(&self, i: usize) -> MaybeValue {
        self.key_value_at(i).1
    }
}

impl<'node> DirtyLeafNode<'node> {
    pub fn insert_kv(
        &mut self,
        location: Result<usize, usize>,
        key: &[u8],
        value: &MaybeValue,
    ) -> Result<MaybeValue<'node>, FitError> {
        let flags = if value.is_overflow() {
            LeafPairRepr::OVERFLOW_MASK
        } else {
            Default::default()
        };
        let repr = LeafPairRepr { flags };

        let mut existing = MaybeValue::Delete;
        let needed = if let Ok(i) = location {
            let (repr2, _key, v) = self.offsets().triplet(i);
            if repr2.is_overflow() {
                existing = MaybeValue::overflow_from_bytes(v);
            }
            match self.offsets_mut().update_v(i, value, repr) {
                Ok(()) => return Ok(existing),
                Err(needed) => needed,
            }
        } else {
            self.node_size_for_kv(key, value)
        };

        if self.curr_size_left() < needed {
            return Err(FitError::WontFit);
        }

        let mut offsets_mut = self.offsets_mut();
        if let Ok(i) = location {
            offsets_mut.remove(i);
        }
        offsets_mut.insert_kv(location.unwrap_or_else(|i| i), key, value, repr);
        if location.is_err() {
            self.header_mut().num_keys += 1;
        }

        Ok(existing)
    }
}

#[cfg(test)]
#[test]
fn test_read_prefix_u32() {
    let samples: &[&[u8]] = &[
        b"", b"\x00", b"\x01", b"a", b"aa", b"aaa", b"aaaa", b"aaaaa", b"b", b"bb", b"c",
    ];
    let conv = samples
        .iter()
        .copied()
        .map(read_prefix_u32)
        .collect::<Vec<_>>();
    assert!(samples.windows(2).all(|a| a[0] <= a[1]));
    assert!(conv.windows(2).all(|a| a[0] <= a[1]));

    let mut samples = (0..10000)
        .map(|_| {
            let len = rand::random::<u8>() as usize;
            let mut v = vec![0u8; len];
            rand::RngCore::fill_bytes(&mut rand::rng(), &mut v);
            v
        })
        .collect::<Vec<_>>();
    samples.sort();
    let conv = samples
        .iter()
        .map(|v| read_prefix_u32(&v))
        .collect::<Vec<_>>();
    assert!(conv.windows(2).all(|a| a[0] <= a[1]));
}
