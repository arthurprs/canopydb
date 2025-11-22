use std::ops::{Bound, RangeBounds};

use crate::{
    error::Error,
    node::*,
    repr::{MaybeValue, PageId},
    utils::TrapResult,
    Bytes, Tree,
};

/// Key Value pairs iterator
#[derive(Debug)]
pub struct RangeIter<'tree>(pub(crate) BaseIter<'tree>);

impl<'tree> RangeIter<'tree> {
    /// Converts iterator into a [RangeKeysIter] that only returns keys.
    #[inline]
    pub fn into_keys(self) -> RangeKeysIter<'tree> {
        RangeKeysIter(self.0)
    }
}

/// Keys iterator
#[derive(Debug)]
pub struct RangeKeysIter<'tree>(pub(crate) BaseIter<'tree>);

impl<'tree> RangeKeysIter<'tree> {
    /// Converts iterator into a [RangeIter] that returns key value pairs.
    #[inline]
    pub fn into_pairs(self) -> RangeIter<'tree> {
        RangeIter(self.0)
    }

    /// Returns value associated with the last key returned by next().
    #[inline]
    pub fn value(&mut self) -> Result<Option<Bytes>, Error> {
        if !self.0.start_yielded || self.0.exhausted {
            return Ok(None);
        }
        self.0.start.peek_value_bytes()
    }

    /// Returns value associated with the last key returned by next_back().
    #[inline]
    pub fn back_value(&mut self) -> Result<Option<Bytes>, Error> {
        if !self.0.end_yielded || self.0.exhausted {
            return Ok(None);
        }
        self.0.end.peek_value_bytes()
    }
}

impl std::iter::Iterator for RangeIter<'_> {
    type Item = Result<(Bytes, Bytes), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next(|c| c.peek_bytes())
    }
}

impl std::iter::DoubleEndedIterator for RangeIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back(|c| c.peek_bytes())
    }
}

impl std::iter::Iterator for RangeKeysIter<'_> {
    type Item = Result<Bytes, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next(|c| c.peek_key_bytes())
    }
}

impl std::iter::DoubleEndedIterator for RangeKeysIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back(|c| c.peek_key_bytes())
    }
}

pub(crate) struct BaseIter<'tree> {
    start: Cursor<'tree, 'tree>,
    end: Cursor<'tree, 'tree>,
    start_yielded: bool,
    end_yielded: bool,
    exhausted: bool,
}

impl std::fmt::Debug for BaseIter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseIter")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

impl<'tree> BaseIter<'tree> {
    pub(crate) fn new<K: AsRef<[u8]>>(
        tree: &'tree Tree<'tree>,
        bounds: impl RangeBounds<K>,
    ) -> Result<Self, Error> {
        let mut exhausted = false;
        let mut start = Cursor::new(tree);
        if let Bound::Included(b) | Bound::Excluded(b) = bounds.start_bound() {
            if start.seek(b.as_ref(), false)? && matches!(bounds.start_bound(), Bound::Excluded(_))
            {
                exhausted |= !start.next()?;
            }
            exhausted |= start.stack.is_empty();
        }
        let mut end = Cursor::new(tree);
        if let Bound::Included(b) | Bound::Excluded(b) = bounds.end_bound() {
            if end.seek(b.as_ref(), true)? && matches!(bounds.end_bound(), Bound::Excluded(_)) {
                exhausted |= !end.prev()?;
            }
            exhausted |= end.stack.is_empty();
        }
        // Check if start > end here so that the check in next/prev can be constant cost (leafs only)
        // Stacks could be exhausted or unbounded here, either is fine.
        for ((_, a), (_, b)) in start.stack.iter().zip(end.stack.iter()) {
            if a != b {
                exhausted |= a > b;
                break;
            }
        }
        Ok(Self {
            start,
            end,
            start_yielded: false,
            end_yielded: false,
            exhausted,
        })
    }

    #[inline]
    fn next<I>(
        &mut self,
        map: impl FnOnce(&mut Cursor) -> Result<Option<I>, Error>,
    ) -> Option<Result<I, Error>> {
        if self.exhausted {
            return None;
        }
        if !self.start_yielded {
            self.start_yielded = true;
            // initialize unbounded start if needed
            if self.start.stack.capacity() == 0 {
                if let Err(e) = self.start.first() {
                    return Some(Err(e));
                }
            }
        } else if let Err(e) = self.start.next() {
            return Some(Err(e));
        }
        if let (Some((na, a)), Some((nb, b))) = (self.start.stack.last(), self.end.stack.last()) {
            if a >= b && na.id() == nb.id() {
                self.exhausted = true;
                if a == b && self.end_yielded {
                    return None;
                }
            }
        }
        let result = map(&mut self.start).transpose();
        self.exhausted |= result.is_none();
        result
    }

    #[inline]
    fn next_back<I>(
        &mut self,
        map: impl FnOnce(&mut Cursor) -> Result<Option<I>, Error>,
    ) -> Option<Result<I, Error>> {
        if self.exhausted {
            return None;
        }
        if !self.end_yielded {
            self.end_yielded = true;
            // initialize unbounded end if needed
            if self.end.stack.capacity() == 0 {
                if let Err(e) = self.end.last() {
                    return Some(Err(e));
                }
            }
        } else if let Err(e) = self.end.prev() {
            return Some(Err(e));
        }
        if let (Some((na, a)), Some((nb, b))) = (self.start.stack.last(), self.end.stack.last()) {
            if a >= b && na.id() == nb.id() {
                self.exhausted = true;
                if a == b && self.start_yielded {
                    return None;
                }
            }
        }
        let result = map(&mut self.end).transpose();
        self.exhausted |= result.is_none();
        result
    }
}

pub(crate) struct Cursor<'tx, 'tree> {
    pub(crate) tree: &'tree Tree<'tx>,
    stack: Vec<(UntypedNode, usize)>,
    full_key: Vec<u8>,
    overflow_node: Option<UntypedNode>,
}

impl std::fmt::Debug for Cursor<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cursor")
            .field("tree", &self.tree)
            .field("stack len", &self.stack.len())
            .field("full_key len", &self.full_key.len())
            .field("overflow_node present", &self.overflow_node.is_some())
            .finish()
    }
}

#[allow(dead_code)] // this may become public eventually
impl<'tx, 'tree> Cursor<'tx, 'tree> {
    pub(crate) fn new(tree: &'tree Tree<'tx>) -> Self {
        Cursor {
            stack: Default::default(),
            tree,
            full_key: Default::default(),
            overflow_node: Default::default(),
        }
    }

    pub fn peek_key(&mut self) -> Result<Option<&[u8]>, Error> {
        match Self::stack_peek(&self.stack) {
            Some((_, b"", k, _)) => Ok(Some(k)),
            Some((_, kp, ks, _)) => {
                self.full_key.clear();
                self.full_key.extend_from_slice(kp);
                self.full_key.extend_from_slice(ks);
                Ok(Some(&self.full_key))
            }
            None => Ok(None),
        }
    }

    pub fn peek_value(&mut self) -> Result<Option<&[u8]>, Error> {
        match self.peek() {
            Ok(Some((_k, v))) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn peek(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        let (k, v) = match Self::stack_peek(&self.stack) {
            Some((_, b"", k, v)) => (k, v),
            Some((_, kp, ks, v)) => {
                self.full_key.clear();
                self.full_key.extend_from_slice(kp);
                self.full_key.extend_from_slice(ks);
                (self.full_key.as_slice(), v)
            }
            None => return Ok(None),
        };
        match v {
            MaybeValue::Bytes(v) => Ok(Some((k, v))),
            MaybeValue::Overflow([overflow_page_id, _span]) => {
                self.tree.tx.trap.check()?;
                if self.overflow_node.is_none() {
                    self.overflow_node = Some(self.tree.tx.clone_node(overflow_page_id)?);
                }
                debug_assert_eq!(self.overflow_node.as_ref().unwrap().id(), overflow_page_id);
                let overflow_node = self.overflow_node.as_ref().unwrap();
                let MaybeValue::Bytes(v) = overflow_node.as_leaf().value_at(0) else {
                    unreachable!()
                };
                Ok(Some((k, v)))
            }
            MaybeValue::Delete => unreachable!(),
        }
    }

    fn peek_bytes(&mut self) -> Result<Option<(Bytes, Bytes)>, Error> {
        let Some((leaf, k0, k1, v)) = Self::stack_peek(&self.stack) else {
            return Ok(None);
        };
        let k = Self::build_key_bytes(leaf, k0, k1);
        let v = Self::build_value_bytes(self.tree, leaf, &mut self.overflow_node, v)?;
        Ok(Some((k, v)))
    }

    fn peek_key_bytes(&mut self) -> Result<Option<Bytes>, Error> {
        let Some((leaf, k0, k1, _)) = Self::stack_peek(&self.stack) else {
            return Ok(None);
        };
        Ok(Some(Self::build_key_bytes(leaf, k0, k1)))
    }

    fn peek_value_bytes(&mut self) -> Result<Option<Bytes>, Error> {
        let Some((leaf, .., v)) = Self::stack_peek(&self.stack) else {
            return Ok(None);
        };
        Self::build_value_bytes(self.tree, leaf, &mut self.overflow_node, v).map(Some)
    }

    #[inline]
    fn build_key_bytes(leaf: &Node<NodeTypeLeaf>, k0: &[u8], k1: &[u8]) -> Bytes {
        if k0.is_empty() {
            leaf.raw_data.restrict(k1)
        } else {
            Bytes::from_slices(&[k0, k1])
        }
    }

    #[inline]
    fn build_value_bytes(
        tree: &'tree Tree<'tx>,
        leaf: &Node<NodeTypeLeaf>,
        overflow_node: &mut Option<Node<NodeTypeNone>>,
        v: MaybeValue<'_>,
    ) -> Result<Bytes, Error> {
        match v {
            MaybeValue::Bytes(v) => Ok(leaf.raw_data.restrict(v)),
            MaybeValue::Overflow([overflow_page_id, _span]) => {
                tree.tx.trap.check()?;
                if overflow_node.is_none() {
                    *overflow_node = Some(tree.tx.clone_node(overflow_page_id)?);
                }
                debug_assert_eq!(overflow_node.as_ref().unwrap().id(), overflow_page_id);
                let overflow_node = overflow_node.as_ref().unwrap();
                let MaybeValue::Bytes(v) = overflow_node.as_leaf().value_at(0) else {
                    unreachable!()
                };
                Ok(overflow_node.raw_data.restrict(v))
            }
            MaybeValue::Delete => unreachable!(),
        }
    }

    fn stack_peek(
        stack: &[(UntypedNode, usize)],
    ) -> Option<(&LeafNode, &[u8], &[u8], MaybeValue<'_>)> {
        if let Some(&(ref node, idx)) = stack.last() {
            let leaf = node.as_leaf();
            if idx < leaf.num_keys() {
                let (k1, v) = leaf.key_value_at(idx);
                return Some((leaf, leaf.key_prefix(), k1, v));
            }
        }
        None
    }

    pub fn next(&mut self) -> Result<bool, Error> {
        let guard = self.tree.tx.trap.setup()?;
        let mut moves = 0;
        let mut next_page = PageId::default();
        self.overflow_node = None;
        for (node, idx) in self.stack.iter_mut().rev() {
            let next_idx = *idx + 1;
            if node.is_leaf() {
                if next_idx < node.num_keys() {
                    *idx = next_idx;
                    guard.disarm();
                    return Ok(true);
                }
            } else if next_idx <= node.num_keys() {
                *idx = next_idx;
                next_page = node.as_branch().pointer_at(next_idx);
                break;
            }
            moves += 1;
        }
        self.stack.truncate(self.stack.len() - moves);
        if self.stack.is_empty() {
            guard.disarm();
            Ok(false)
        } else {
            self.descend_to_first(next_page).guard_trap(guard)
        }
    }

    pub fn prev(&mut self) -> Result<bool, Error> {
        // go up until we find a way to go left
        let guard = self.tree.tx.trap.setup()?;
        let mut moves = 0;
        let mut next_page = PageId::default();
        self.overflow_node = None;
        for (node, idx) in self.stack.iter_mut().rev() {
            if let Some(prev_idx) = idx.checked_sub(1) {
                if node.is_leaf() {
                    *idx = prev_idx;
                    guard.disarm();
                    return Ok(true);
                } else {
                    *idx = prev_idx;
                    next_page = node.as_branch().pointer_at(prev_idx);
                    break;
                }
            }
            moves += 1;
        }
        self.stack.truncate(self.stack.len() - moves);
        if self.stack.is_empty() {
            guard.disarm();
            Ok(false)
        } else {
            self.descend_to_last(next_page).guard_trap(guard)
        }
    }

    pub fn first(&mut self) -> Result<bool, Error> {
        let guard = self.tree.tx.trap.setup()?;
        self.stack.clear();
        self.overflow_node = None;
        if self.tree.value.root == PageId::default() {
            guard.disarm();
            return Ok(false);
        }
        self.descend_to_first(self.tree.value.root)
            .guard_trap(guard)
    }

    pub fn last(&mut self) -> Result<bool, Error> {
        let guard = self.tree.tx.trap.setup()?;
        self.stack.clear();
        self.overflow_node = None;
        if self.tree.value.root == PageId::default() {
            guard.disarm();
            return Ok(false);
        }
        self.descend_to_last(self.tree.value.root).guard_trap(guard)
    }

    fn descend_to_last(&mut self, mut next_page: PageId) -> Result<bool, Error> {
        loop {
            let node = self.tree.tx.clone_node(next_page)?;
            let num_keys = node.num_keys();
            if node.is_leaf() {
                self.stack.push((node, num_keys - 1));
                return Ok(true);
            }
            next_page = node.as_branch().pointer_at(num_keys);
            self.stack.push((node, num_keys));
        }
    }

    fn descend_to_first(&mut self, mut next_page: PageId) -> Result<bool, Error> {
        loop {
            let node = self.tree.tx.clone_node(next_page)?;
            if node.is_leaf() {
                self.stack.push((node, 0));
                return Ok(true);
            }
            next_page = node.as_branch().pointer_at(0);
            self.stack.push((node, 0));
        }
    }

    pub fn seek(&mut self, key: &[u8], for_prev: bool) -> Result<bool, Error> {
        let guard = self.tree.tx.trap.setup()?;
        if self.tree.value.root == PageId::default() {
            guard.disarm();
            return Ok(false);
        }
        self.stack.clear();
        self.overflow_node = None;
        let mut expected_level = self.tree.value.level;
        let mut node_id = self.tree.value.root;
        loop {
            let node = if node_id == self.tree.value.root {
                let mut cached_root = self.tree.cached_root.borrow_mut();
                if let Some(cached) = &*cached_root {
                    cached
                } else {
                    cached_root.insert(self.tree.tx.clone_node(self.tree.value.root)?)
                }
                .clone()
            } else {
                self.tree.tx.clone_node(node_id)?
            };
            debug_assert_eq!(expected_level, node.node_header().level);
            if node.is_leaf() {
                let leaf = node.as_leaf();
                let leaf_num_keys = leaf.num_keys();
                let search = leaf.search_keys(key);
                self.stack.push((node, search.unwrap_or_else(|i| i)));
                if for_prev {
                    if search.is_err() {
                        self.prev()?;
                    }
                } else if search == Err(leaf_num_keys) {
                    self.next()?;
                }
                guard.disarm();
                return Ok(search.is_ok());
            }
            let child_idx;
            (child_idx, node_id) = node.as_branch().search_child(key);
            self.stack.push((node, child_idx));
            expected_level -= 1;
        }
    }

    pub fn tree(&self) -> &Tree<'tx> {
        self.tree
    }
}
