use crate::{
    cursor::{BaseIter, Cursor, RangeIter, RangeKeysIter},
    error::{error_validation, Error},
    node::*,
    page::Page,
    repr::*,
    total_size_to_span,
    utils::{common_prefix_len, EscapedBytes, TrapResult},
    Bytes, PageId, Transaction,
};

use smallvec::SmallVec;
use std::{
    cell::RefCell,
    cmp::Ordering,
    ops::{Bound, RangeBounds},
};
use triomphe::Arc;

pub(crate) const MIN_BRANCH_KEYS: usize = 1; // needs 2xMin + 1 to split
pub(crate) const MIN_LEAF_KEYS: usize = 1; // needs 2xMin to split
pub(crate) const MIN_PREFIX_SIZE: usize = 4;
pub(crate) const MAX_PREFIX_SIZE: usize = u16::MAX as usize;

// Considering that
// * The widest page/node is theoretically = (2^24 - 1) * PAGE_SIZE ~= 64GB.
// But in practice is limited by the allocator to 4GB.
// * The offsets inside nodes are tracked with a u32, which can address the entire 4GB.
// * Min branch/leaf keys is 1
// * Max prefix length is 2**16 - 1
// The max key length must allows fitting at least MIN_BRANCH_KEYS * 2 + 1 keys
// into branch nodes and MIN_LEAF_KEYS * 2 keys in leaves, plus plenty of slack for header,
// prefix and possibly small inline values (for leaves). Note that values larger than
// MAX_INLINE_VALUE_LEN go into overflow leaves and do not influence this calculation.
pub(crate) const MAX_KEY_SIZE: usize = 1 << 30; // 1GB
pub(crate) const MAX_VALUE_SIZE: usize = 3 << 30; // 3GB
pub(crate) const MAX_TREE_NAME_LEN: usize = MAX_KEY_SIZE;
// Roughly PAGE_SIZE/2 (2048), but a little less as we still want to be able to fit
// 2 small sized keys + 2 max-inline-len values in a leaf
pub(crate) const MAX_INLINE_VALUE_LEN: usize = 1950;

/// Database Tree Instance
///
/// A tree is a named and unique key space inside a database, it offers an ordered map interface (similar to `std::collections::BTreeMap`).
/// Keys and values are bytes (e.g. `Vec<u8>` or `&[u8]`) and keys are lexicographically (byte order) sorted.
///
/// # The Tree struct
///
/// The [Tree] struct represents a instance of a named tree within a transaction.
///
/// In _write_ transactions, only one instance of each named tree can be active at a given time.
/// Attempting to re-open, rename, or delete the tree while an instance of it is still
/// active will return an error.
///
/// # B+Tree implementation
///
/// Trees are [B+Trees](https://en.wikipedia.org/wiki/B%2B_tree) and allow efficient storage of ordered
/// key value pairs over pages. Read performance is great and write performance is also great while
/// the workload fits in memory (less so after that, but engineering is about tradeoffs).
///
/// # Prefix and suffix truncation
///
/// Tree nodes often have a shared key prefix. This shared prefix is only stored once per node and removed from the keys
/// stored in the nodes. This is referred to as prefix compression.
/// When deciding the separator keys in branch nodes, the Tree also chooses the shortest separator possible.
/// This is referred to as suffix compression.
///
/// Prefix and suffix truncation work together to lower space overhead and increase branching factor, yielding
/// a more compact tree with less nodes and levels.
///
/// # Overflow values (Large values)
///
/// Overflow values are values with largar than 2KB approximately, that are stored individually and outside
/// the main tree structure. This is also referred to as key-value separation. Canopydb supports
/// efficient compression of large values (See [crate::TreeOptions::compress_overflow_values])
pub struct Tree<'tx> {
    pub(crate) tx: &'tx Transaction,
    pub(crate) name: Option<Arc<[u8]>>,
    pub(crate) value: TreeValue,
    pub(crate) dirty: bool,
    pub(crate) cached_root: RefCell<Option<UntypedNode>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TreeState {
    Available { value: TreeValue, dirty: bool },
    Deleted,
    InUse { value: TreeValue },
}

enum MutateResult {
    Ok(PageId),
    Split(PageId, PageId, Vec<u8>),
    Underflow(PageId),
}

impl std::fmt::Debug for Tree<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree")
            .field("name", &self.name.as_deref().map(EscapedBytes))
            .field("len", &{ self.value.num_keys })
            .field("level", &self.value.level)
            .field("dirty", &self.dirty)
            .finish()
    }
}

impl Drop for Tree<'_> {
    fn drop(&mut self) {
        if !self.tx.is_write_or_checkpoint_txn() {
            return;
        }
        if let Some(name) = &self.name {
            *self.tx.trees.borrow_mut().get_mut(name).unwrap() = TreeState::Available {
                value: self.value,
                dirty: self.dirty,
            };
        }
    }
}

impl std::fmt::Debug for MutateResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok(arg0) => f.debug_tuple("Ok").field(arg0).finish(),
            Self::Split(arg0, arg1, arg2) => f
                .debug_tuple("Split")
                .field(arg0)
                .field(arg1)
                .field(&EscapedBytes(arg2))
                .finish(),
            Self::Underflow(arg0) => f.debug_tuple("Underflow").field(arg0).finish(),
        }
    }
}

impl<'tx> Tree<'tx> {
    fn set_root(&mut self, root: PageId) {
        if self.value.root != root {
            trace!(
                "set_root {:?} {} -> {}",
                self.name.as_deref().map(EscapedBytes),
                { self.value.root },
                root
            );
            self.value.root = root;
            self.dirty = true;
        }
    }

    fn set_level(&mut self, level: u8) {
        if self.value.level != level {
            trace!(
                "set_level {:?} level {} -> {}",
                self.name.as_deref().map(EscapedBytes),
                self.value.level,
                level
            );
            self.value.level = level;
            self.dirty = true;
        }
    }

    #[inline]
    fn inc_num_keys(&mut self, delta: i64) {
        match delta.cmp(&0) {
            Ordering::Less => self.value.num_keys -= (-delta) as u64,
            Ordering::Greater => self.value.num_keys += delta as u64,
            Ordering::Equal => return,
        }
        self.dirty = true;
    }

    #[cfg(any(fuzzing, test))]
    pub(crate) fn iter_pages(&self, cb: &mut dyn FnMut(PageId, PageId)) -> Result<(), Error> {
        self.iter_nodes(&mut |node| cb(node.id(), node.span()))
    }

    #[cfg(any(fuzzing, test))]
    pub(crate) fn iter_nodes(&self, cb: &mut dyn FnMut(&UntypedNode)) -> Result<(), Error> {
        fn recurse(
            tree: &Tree<'_>,
            page_id: PageId,
            cb: &mut dyn FnMut(&UntypedNode),
        ) -> Result<(), Error> {
            let node = tree.tx.clone_node(page_id)?;
            if node.is_branch() {
                for child in node.as_branch().children() {
                    recurse(tree, child, cb)?;
                }
            } else {
                for (child, _) in node.as_leaf().overflow_children() {
                    recurse(tree, child, cb)?;
                }
            }
            cb(&node);
            Ok(())
        }

        let guard = self.tx.trap.setup()?;
        if self.value.root == PageId::default() {
            guard.disarm();
            return Ok(());
        }
        recurse(self, self.value.root, cb).guard_trap(guard)
    }

    /// Returns whether the Tree is empty (0 length).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.value.num_keys == 0
    }

    /// Returns the number of key-value pairs in the Tree.
    #[inline]
    pub fn len(&self) -> u64 {
        self.value.num_keys
    }

    /// Returns the value corresponding to the key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        let guard = self.tx.trap.setup()?;
        let mut node_id = self.value.root;
        if node_id == PageId::default() {
            guard.disarm();
            return Ok(None);
        }
        let mut expected_level = self.value.level;
        let mut cached_root = self.cached_root.borrow_mut();
        let mut cloned_node;
        let mut node;
        let search = loop {
            node = if node_id == self.value.root {
                if let Some(cached) = &*cached_root {
                    cached
                } else {
                    cached_root.insert(self.tx.clone_node(node_id)?)
                }
            } else {
                cloned_node = self.tx.clone_node(node_id)?;
                &cloned_node
            };
            debug_assert_eq!(expected_level, node.node_header().level);
            if node.is_leaf() {
                break node.as_leaf().search_keys(key);
            }
            (_, node_id) = node.as_branch().search_child(key);
            expected_level -= 1;
        };
        let v = if let Ok(i) = search {
            match node.as_leaf().value_at(i) {
                MaybeValue::Bytes(b) => Some(node.raw_data.restrict(b)),
                MaybeValue::Overflow([overflow_page_id, _]) => {
                    cloned_node = self.tx.clone_node(overflow_page_id)?;
                    let MaybeValue::Bytes(b) = cloned_node.as_leaf().value_at(0) else {
                        unreachable!()
                    };
                    Some(cloned_node.raw_data.restrict(b))
                }
                MaybeValue::Delete => None,
            }
        } else {
            None
        };
        guard.disarm();
        Ok(v)
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did have this key present, the value is updated.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let guard = self.tx.trap.setup()?;
        self.mutate(key, Some(value)).guard_trap(guard)
    }

    /// Delete `key` from the Tree.
    ///
    /// Returns a boolean indicating whether an entry was deleted.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, Error> {
        let guard = self.tx.trap.setup()?;
        let prev_keys = self.value.num_keys;
        self.mutate(key, None).guard_trap(guard)?;
        Ok(self.value.num_keys != prev_keys)
    }

    /// Delete the specified key range from the Tree.
    pub fn delete_range<K: AsRef<[u8]>>(
        &mut self,
        key_range: impl RangeBounds<K>,
    ) -> Result<(), Error> {
        self.delete_range_internal(
            key_range.start_bound().map(|a| a.as_ref()),
            key_range.end_bound().map(|a| a.as_ref()),
        )
    }

    fn delete_range_internal(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Error> {
        trace!(
            "delete_range {} {:?} {:?}",
            { self.value.root },
            start.map(EscapedBytes),
            end.map(EscapedBytes)
        );
        let guard = self.tx.trap.setup()?;
        if self.value.root == PageId::default() {
            guard.disarm();
            return Ok(());
        }
        *self.cached_root.get_mut() = None;
        self.tx.mark_dirty();
        if self.name.is_some() {
            if let Some(write_batch) = &mut *self.tx.wal_write_batch.borrow_mut() {
                write_batch.push_delete_range(self.value.id, start, end)?;
            }
        }
        match remove_range_node(self, self.value.root, start, end)? {
            MutateResult::Ok(new_root_id) => {
                self.set_root(new_root_id);
            }
            MutateResult::Underflow(0) => {
                self.set_root(0);
                self.set_level(0);
            }
            MutateResult::Underflow(mut root_page_id) => {
                let mut root = self.tx.pop_node(root_page_id)?;
                while root.num_keys() == 0 {
                    self.tx.free_page(&root)?;
                    if root.is_branch() {
                        root_page_id = root.as_branch().pointer_at(0);
                        if root_page_id == PageId::default() {
                            break;
                        } else {
                            root = self.tx.pop_node(root_page_id)?;
                        }
                    } else {
                        root_page_id = PageId::default();
                        break;
                    }
                }
                if root_page_id == PageId::default() {
                    self.set_level(0);
                } else {
                    self.set_level(root.node_header().level);
                    self.tx.stash_node(root)?;
                }
                self.set_root(root_page_id);
            }
            MutateResult::Split(..) => unreachable!(),
        }
        guard.disarm();
        Ok(())
    }

    fn mutate(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        if !self.tx.is_write_or_checkpoint_txn() {
            return Err(Error::WriteTransactionRequired);
        }
        self.validate_key_value_lengths(key, value)?;

        *self.cached_root.get_mut() = None;
        self.tx.mark_dirty();
        if self.name.is_some() {
            if let Some(write_batch) = &mut *self.tx.wal_write_batch.borrow_mut() {
                if let Some(value) = value {
                    write_batch.push_insert(self.value.id, key, value)?;
                } else {
                    write_batch.push_delete(self.value.id, key)?;
                }
            }
        }

        let value = &(match value {
            Some(b) if b.len() <= MAX_INLINE_VALUE_LEN => MaybeValue::Bytes(b),
            Some(b) => {
                let bytes_v = MaybeValue::Bytes(b);
                let required_size = LeafNode::<false>::leaf_size_for(&self.value, 0, b"", &bytes_v);
                let overflow_page = self.allocate_node(required_size, None)?;
                let overflow = [overflow_page.id(), overflow_page.span()];
                trace!("Allocated overflow page {overflow:?}");
                let mut overflow_node = LeafNode::new_overflow(&self.value, overflow_page);
                overflow_node
                    .as_dirty()
                    .insert_kv(Err(0), b"", &bytes_v)
                    .unwrap();
                self.tx.stash_node(overflow_node)?;
                MaybeValue::Overflow(overflow)
            }
            None => MaybeValue::Delete,
        });

        let insert_res = if self.value.root == PageId::default() {
            // empty tree
            if matches!(value, MaybeValue::Delete) {
                return Ok(());
            }
            let node = LeafNode::new_leaf(
                &self.value,
                self.allocate_node(
                    LeafNode::<false>::leaf_size_for(&self.value, 0, key, value),
                    Some(0),
                )?,
            );
            Self::leaf_mutate(self, &NodePrefix::default(), node, key, value)
        } else {
            Self::node_mutate(self, &NodePrefix::default(), self.value.root, key, value)
        };

        let (child_left, child_right, new_key) = match insert_res? {
            MutateResult::Ok(root_page_id) => {
                self.set_root(root_page_id);
                return Ok(());
            }
            MutateResult::Split(left, right, new_key) => (left, right, new_key),
            MutateResult::Underflow(0) => {
                self.set_root(0);
                self.set_level(0);
                return Ok(());
            }
            MutateResult::Underflow(mut root_page_id) => {
                let root = self.tx.pop_node(root_page_id)?;
                let mut root_level = root.node_header().level;
                if root.num_keys() == 0 {
                    if root.is_branch() {
                        root_page_id = root.as_branch().pointer_at(0);
                        root_level -= 1;
                    } else {
                        root_page_id = PageId::default();
                    }
                    self.tx.free_page(&root)?;
                } else {
                    self.tx.stash_node(root)?;
                };
                self.set_root(root_page_id);
                self.set_level(root_level);
                return Ok(());
            }
        };

        // create new root
        self.set_level(self.value.level + 1);
        let mut root = BranchNode::new_branch(
            &self.value,
            self.value.level,
            self.allocate_node(
                BranchNode::<false>::root_size_for(&self.value, &new_key),
                Some(self.value.level),
            )?,
        );
        let mut root_mut = root.as_dirty();
        root_mut.set_pointer_at(0, child_left);
        root_mut
            .insert_kp(Err(0), &new_key, child_right)
            .expect("Didn't fit new root");

        // update root page id
        self.set_root(root.id());
        self.tx.stash_node(root)?;

        Ok(())
    }

    fn validate_key_value_lengths(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(), Error> {
        if self.value.fixed_key_len >= 0 {
            if key.len() != self.value.fixed_key_len as usize {
                return Err(error_validation!(
                    "Tree only accepts keys of length {}",
                    self.value.fixed_key_len
                ));
            }
        } else if key.len() > MAX_KEY_SIZE {
            return Err(error_validation!(
                "Key length ({}) exceeds maximum key length {MAX_KEY_SIZE}",
                key.len()
            ));
        }
        if self.value.fixed_value_len >= 0 {
            if value.map_or(false, |v| v.len() != self.value.fixed_value_len as usize) {
                return Err(error_validation!(
                    "Tree only accepts values of length {}",
                    self.value.fixed_value_len
                ));
            }
        } else if value.map_or(0, |v| v.len()) > MAX_VALUE_SIZE {
            return Err(error_validation!(
                "Value length ({}) exceeds maximum key length {MAX_VALUE_SIZE}",
                value.unwrap().len()
            ));
        }
        Ok(())
    }

    #[inline]
    fn free_value(&self, value: &MaybeValue) -> Result<(), Error> {
        match value {
            MaybeValue::Bytes(_) | MaybeValue::Delete => Ok(()),
            &MaybeValue::Overflow([page_id, span]) => self.tx.free_page_with_id(page_id, span),
        }
    }

    fn allocate_node(&self, size: usize, level: Option<u8>) -> Result<Page, Error> {
        let span = total_size_to_span(size)?;
        let compressed = if let Some(level) = level {
            self.value.should_compress_level(level, span)
        } else {
            self.value.should_compress_overflow(span)
        };
        self.tx.allocate_page(span, compressed)
    }

    fn node_mutate(
        tree: &mut Tree,
        parent_prefix: &NodePrefix<'_>,
        node_id: PageId,
        full_key: &[u8],
        value: &MaybeValue,
    ) -> Result<MutateResult, Error> {
        let node = tree.tx.pop_node(node_id)?;
        if node.is_leaf() {
            Self::leaf_mutate(tree, parent_prefix, Node::into_leaf(node), full_key, value)
        } else {
            Self::branch_mutate(
                tree,
                parent_prefix,
                Node::into_branch(node),
                full_key,
                value,
            )
        }
    }

    fn branch_mutate(
        tree: &mut Tree,
        prefix: &NodePrefix<'_>,
        mut node: BranchNode,
        full_key: &[u8],
        value: &MaybeValue,
    ) -> Result<MutateResult, Error> {
        Self::ensure_correct_prefix(tree, prefix, &mut node, full_key)?;
        let (mut ptr_idx, child_id) = node.search_child(full_key);
        let insert = Self::node_mutate(
            tree,
            &node.prefix_for_child(ptr_idx),
            child_id,
            full_key,
            value,
        )?;

        let (child_left_id, child_right_id, new_key) = match insert {
            MutateResult::Ok(new_child_id) => {
                if new_child_id != child_id {
                    tree.tx
                        .make_dirty(&mut node)?
                        .set_pointer_at(ptr_idx, new_child_id);
                }
                let (id, num_keys) = (node.id(), node.num_keys());
                tree.tx.stash_node(node)?;
                return Ok(if num_keys >= MIN_BRANCH_KEYS {
                    MutateResult::Ok(id)
                } else {
                    MutateResult::Underflow(id)
                });
            }
            MutateResult::Split(left, right, new_key) => (left, right, new_key),
            MutateResult::Underflow(0) => {
                let mut node_mut = tree.tx.make_dirty(&mut node)?;
                if ptr_idx == 0 {
                    node_mut.set_pointer_at(0, node_mut.pointer_at(1));
                }
                node_mut.remove_key_repr_at(ptr_idx.saturating_sub(1));
                let (id, num_keys) = (node.id(), node.num_keys());
                tree.tx.stash_node(node)?;
                return Ok(if num_keys >= MIN_BRANCH_KEYS {
                    MutateResult::Ok(id)
                } else {
                    MutateResult::Underflow(id)
                });
            }
            MutateResult::Underflow(new_child_id) => {
                if new_child_id != child_id {
                    tree.tx
                        .make_dirty(&mut node)?
                        .set_pointer_at(ptr_idx, new_child_id);
                }
                match branch_merge_children(tree, &mut node, ptr_idx, true)? {
                    ir @ MutateResult::Ok(_) | ir @ MutateResult::Underflow(_) => {
                        tree.tx.stash_node(node)?;
                        return Ok(ir);
                    }
                    MutateResult::Split(left, right, new_key) => {
                        ptr_idx = ptr_idx.saturating_sub(1);
                        (left, right, new_key)
                    }
                }
            }
        };

        let needed = match Tree::insert_kpp(
            tree,
            prefix,
            &mut node,
            ptr_idx,
            &new_key,
            child_left_id,
            child_right_id,
        )? {
            Ok(()) => {
                let id = node.id();
                tree.tx.stash_node(node)?;
                return Ok(MutateResult::Ok(id));
            }
            Err(needed) => needed,
        };
        branch_split_insert_child(
            tree,
            prefix,
            node,
            ptr_idx,
            needed,
            new_key,
            child_left_id,
            child_right_id,
        )
    }

    fn leaf_mutate(
        tree: &mut Tree,
        prefix: &NodePrefix<'_>,
        mut node: LeafNode,
        full_key: &[u8],
        value: &MaybeValue,
    ) -> Result<MutateResult, Error> {
        Self::ensure_correct_prefix(tree, prefix, &mut node, full_key)?;
        let search = node.search_keys(full_key);
        if !value.is_delete() {
            tree.inc_num_keys(search.is_err() as i64);
            match Self::insert_kv(tree, prefix, &mut node, search, full_key, value)? {
                Ok(()) => {
                    trace!(
                        "inserted {:?} in leaf {} {:?}, num_keys {}",
                        &EscapedBytes(full_key),
                        node.id(),
                        search,
                        { tree.value.num_keys }
                    );
                }
                Err(needed) => {
                    return leaf_split_insert_kv(
                        tree, prefix, node, search, needed, full_key, value,
                    );
                }
            };
        } else if let Ok(idx) = search {
            tree.inc_num_keys(-1);
            trace!(
                "delete {:?} in leaf {} pos {:?}",
                &EscapedBytes(full_key),
                node.id(),
                search
            );
            let mut node_mut = tree.tx.make_dirty(&mut node)?;
            tree.free_value(&node_mut.value_at(idx))?;
            node_mut.remove_key_repr_at(idx);
        }

        let (id, num_keys) = (node.id(), node.num_keys());
        if num_keys == 0 {
            tree.tx.free_page(&node)?;
            return Ok(MutateResult::Underflow(0));
        }
        tree.tx.stash_node(node)?;
        Ok(if num_keys >= MIN_LEAF_KEYS {
            MutateResult::Ok(id)
        } else {
            MutateResult::Underflow(id)
        })
    }

    /// Reduce/increase prefix if needed
    #[inline]
    fn ensure_correct_prefix<TYPE: NodeRepr>(
        tree: &mut Tree,
        prefix: &NodePrefix<'_>,
        node: &mut Node<TYPE>,
        full_key: &[u8],
    ) -> Result<(), Error> {
        let node_prefix_len = node.key_prefix_len();
        if node_prefix_len <= prefix.parent_prefix_len() {
            debug_assert!(prefix.parent_prefix().starts_with(node.key_prefix()));
            debug_assert!(full_key.starts_with(node.key_prefix()));
            return Ok(());
        }
        debug_assert!(node.key_prefix().starts_with(prefix.parent_prefix()));
        if full_key.starts_with(node.key_prefix()) {
            return Ok(());
        }

        *node = Self::resize_with_prefix_internal(tree, node, &prefix.max_prefix(), 0)?;
        Ok(())
    }

    fn resize_with_prefix_internal<TYPE: NodeRepr>(
        tree: &mut Tree,
        node: &Node<TYPE>,
        max_prefix: &[u8],
        needed: usize,
    ) -> Result<Node<TYPE>, Error> {
        let key_prefix = node.key_prefix();
        let key_prefix_delta = &NodePrefix::prefix_delta(key_prefix, max_prefix);
        let required_size = node.real_occupied_size_with_prefix(key_prefix_delta) + needed;
        let new_page = if node.dirty && node.page_size() >= required_size {
            Page::new(node.id(), true, node.span())
        } else {
            tree.tx.free_page(node)?;
            tree.allocate_node(required_size, Some(node.header().level))?
        };
        let mut new = Node::new(&tree.value, node.header().level, new_page);
        new.as_dirty()
            .copy_everything_with_prefix(node, max_prefix, key_prefix_delta);
        Ok(new)
    }

    #[inline]
    fn resize_with_largest_prefix<TYPE: NodeRepr>(
        tree: &mut Tree,
        node: &mut Node<TYPE>,
        max_prefix: &[u8],
        needed: usize,
    ) -> Result<(), Error> {
        let largest_prefix = if max_prefix.len() > node.key_prefix_len() {
            max_prefix
        } else {
            node.key_prefix()
        };
        *node = Self::resize_with_prefix_internal(tree, node, largest_prefix, needed)?;
        Ok(())
    }

    fn insert_kpp(
        tree: &mut Tree,
        prefix: &NodePrefix<'_>,
        node: &mut BranchNode,
        ptr_idx: usize,
        full_key: &[u8],
        child_left: PageId,
        child_right: PageId,
    ) -> Result<Result<(), usize>, Error> {
        debug_assert!(full_key.starts_with(node.key_prefix()));
        let mut key_suffix = &full_key[node.key_prefix_len()..];
        let mut needed = BranchNode::<false>::size_for_kp(&tree.value, key_suffix);
        // If we ended up with an oversized node because of an underflow + merge after remove_range, attemp to fix it here
        if node.span() > tree.value.min_branch_node_pages as PageId
            && node.num_keys() >= MIN_BRANCH_KEYS * 2
        {
            trace!(
                "splitting big branch {}, span {}, keys {}",
                node.id(),
                node.span(),
                node.num_keys()
            );
            return Ok(Err(needed));
        }
        if node.curr_size_left() < needed {
            debug_assert!(
                node.num_keys() != 0 || node.pointer_at(0) != PageId::default(),
                "Failed to insert in empty node {needed}"
            );
            let available_after_compact = node.real_size_left();
            if available_after_compact < needed {
                let original_needed = needed;
                let max_prefix = prefix.max_prefix();
                let prefix_inc = max_prefix.len().saturating_sub(node.key_prefix_len());
                key_suffix = &key_suffix[prefix_inc..];
                needed -= prefix_inc;
                let real_occupied_size = node.real_occupied_size_with_prefix(&Ok(prefix_inc));
                let required_span = total_size_to_span(real_occupied_size + needed)?;
                if node.num_keys() >= MIN_BRANCH_KEYS * 2
                    && required_span > tree.value.min_branch_span(node.header().level)
                {
                    return Ok(Err(original_needed));
                }
                Self::resize_with_largest_prefix(tree, node, &max_prefix, needed)?;
            } else {
                tree.tx
                    .make_dirty(node)?
                    .compact_tail(&mut tree.tx.scratch_buffer.borrow_mut());
                assert_eq!(node.curr_size_left(), available_after_compact);
            }
        }
        let mut node_mut = tree.tx.make_dirty(node)?;
        node_mut
            .insert_kp(Err(ptr_idx), key_suffix, child_right)
            .unwrap();
        node_mut.set_pointer_at(ptr_idx, child_left);

        Ok(Ok(()))
    }

    fn insert_kv(
        tree: &mut Tree,
        prefix: &NodePrefix<'_>,
        node: &mut LeafNode,
        mut location: Result<usize, usize>,
        full_key: &[u8],
        value: &MaybeValue,
    ) -> Result<Result<(), usize>, Error> {
        debug_assert!(full_key.starts_with(node.key_prefix()));
        let mut key_suffix = &full_key[node.key_prefix_len()..];
        let mut needed = match location {
            Ok(i) if node.value_at(i).repr_len() >= value.repr_len() => 0,
            _ => LeafNode::<false>::size_for_kv(&tree.value, key_suffix, value),
        };
        if node.curr_size_left() < needed {
            debug_assert_ne!(
                node.num_keys(),
                0,
                "Failed to insert in empty node {} {}",
                node.curr_size_left(),
                needed
            );
            let available_after_compact = node.real_size_left()
                + if let Ok(i) = location {
                    let (k, v) = node.key_value_at(i);
                    LeafNode::<false>::size_for_kv(&tree.value, k, &v)
                } else {
                    0
                };
            if available_after_compact < needed {
                let original_needed = needed;
                let max_prefix = prefix.max_prefix();
                let prefix_inc = max_prefix.len().saturating_sub(node.key_prefix_len());
                key_suffix = &key_suffix[prefix_inc..];
                if location.is_err() {
                    needed -= prefix_inc;
                }
                let real_occupied_size = node.real_occupied_size_with_prefix(&Ok(prefix_inc));
                let required_span = total_size_to_span(real_occupied_size + needed)?;
                if required_span > tree.value.min_leaf_node_pages as PageId
                    && node.num_keys() + (location.is_err() as usize) >= MIN_LEAF_KEYS * 2
                {
                    return Ok(Err(original_needed));
                }
                Self::resize_with_largest_prefix(tree, node, &max_prefix, needed)?;
            } else {
                let mut node_mut = tree.tx.make_dirty(node)?;
                if let Ok(i) = location {
                    tree.free_value(&node_mut.value_at(i))?;
                    node_mut.remove_key_repr_at(i);
                    location = Err(i);
                }
                node_mut.compact_tail(&mut tree.tx.scratch_buffer.borrow_mut());
                assert_eq!(node_mut.curr_size_left(), available_after_compact);
            }
        }

        let mut node_mut = tree.tx.make_dirty(node)?;
        let existing = node_mut.insert_kv(location, key_suffix, value).unwrap();
        tree.free_value(&existing)?;

        Ok(Ok(()))
    }

    /// Removes all entries from the Tree.
    #[inline]
    pub fn clear(&mut self) -> Result<(), Error> {
        self.delete_range::<&[u8]>(..)
    }

    /// Returns the Transaction used by the Tree
    #[inline]
    pub fn tx(&self) -> &'tx Transaction {
        self.tx
    }

    /// Returns the Tree's Cursor
    pub(crate) fn cursor(&self) -> Cursor<'tx, '_> {
        Cursor::new(self)
    }

    /// Returns an iterator over the key value pairs of the specified range.
    ///
    /// If values aren't required, [Tree::range_keys] may be more efficient.
    pub fn range<K: AsRef<[u8]>>(
        &self,
        bounds: impl RangeBounds<K>,
    ) -> Result<RangeIter<'_>, Error> {
        BaseIter::new(self, bounds).map(RangeIter)
    }

    /// Returns an iterator over the keys in the specified range.
    pub fn range_keys<K: AsRef<[u8]>>(
        &self,
        bounds: impl RangeBounds<K>,
    ) -> Result<RangeKeysIter<'_>, Error> {
        BaseIter::new(self, bounds).map(RangeKeysIter)
    }

    /// Returns an iterator over the prefix.
    ///
    /// If values aren't required, [Tree::prefix_keys] may be more efficient.
    pub fn prefix<K: AsRef<[u8]>>(&self, prefix: &K) -> Result<RangeIter<'_>, Error> {
        let start = prefix.as_ref();
        if start.is_empty() {
            return self.iter();
        }
        let mut end = SmallVec::<u8, 256>::from_slice(start);
        match end.last_mut() {
            Some(last) if *last < u8::MAX => *last += 1,
            _ => end.push(0x00),
        }
        BaseIter::new(self, start..end.as_slice()).map(RangeIter)
    }

    /// Returns an iterator over the prefix.
    #[inline]
    pub fn prefix_keys<K: AsRef<[u8]>>(&self, prefix: &K) -> Result<RangeKeysIter<'_>, Error> {
        self.prefix(prefix).map(RangeIter::into_keys)
    }

    /// Returns an iterator over the entire tree.
    ///
    /// If values aren't required, [Tree::keys] may be more efficient.
    #[inline]
    pub fn iter(&self) -> Result<RangeIter<'_>, Error> {
        self.range::<&[u8]>(..)
    }

    /// Returns an iterator over all keys of the tree..
    #[inline]
    pub fn keys(&self) -> Result<RangeKeysIter<'_>, Error> {
        self.range_keys::<&[u8]>(..)
    }

    pub(crate) fn compact(&mut self, page_id_threshold: PageId) -> Result<(), Error> {
        fn make_dirty_inc_parents<'node, TYPE: NodeType>(
            tree: &mut Tree<'_>,
            node: &'node mut Node<TYPE>,
            make_parent_dirty: &mut dyn FnMut(&mut Tree<'_>) -> Result<bool, Error>,
        ) -> Result<Option<DirtyNode<'node, TYPE>>, Error> {
            if node.dirty {
                return Ok(Some(node.as_dirty()));
            }
            if !make_parent_dirty(tree)? {
                return Ok(None);
            }
            tree.tx.make_dirty(node).map(Some)
        }

        fn recurse(
            tree: &mut Tree<'_>,
            page_id: PageId,
            page_id_threshold: PageId,
            parent_make_dirty: &mut dyn FnMut(&mut Tree) -> Result<bool, Error>,
        ) -> Result<(PageId, PageId), Error> {
            if page_id.is_compressed() {
                if let Some((comp_id, _)) = tree.tx.read_compressed_page_details(page_id)? {
                    if comp_id < page_id_threshold {
                        return Ok((page_id, 0));
                    }
                } else {
                    return Ok((page_id, 0));
                }
            }
            let mut node = tree.tx.pop_node(page_id)?;
            if node.compressed_page.map_or(node.id(), |(id, _)| id) >= page_id_threshold
                && make_dirty_inc_parents(tree, &mut node, parent_make_dirty)?.is_none()
            {
                let result = (node.id(), node.span());
                tree.tx.stash_node(node)?;
                return Ok(result);
            }
            if node.is_branch() {
                let mut branch = node.into_branch();
                let children = branch.children().collect::<Vec<_>>();
                for (i, child_id) in children.into_iter().enumerate() {
                    let (new_child_id, _) =
                        recurse(tree, child_id, page_id_threshold, &mut |tree| {
                            make_dirty_inc_parents(tree, &mut branch, parent_make_dirty)
                                .map(|o| o.is_some())
                        })?;
                    if new_child_id != child_id {
                        if let Some(mut branch) =
                            make_dirty_inc_parents(tree, &mut branch, parent_make_dirty)?
                        {
                            branch.set_pointer_at(i, new_child_id);
                        } else {
                            break;
                        }
                    }
                }
                node = branch.into_untyped();
            } else {
                let mut leaf = node.into_leaf();
                let children = leaf.overflow_children_with_idx().collect::<Vec<_>>();
                for (i, child_id, _) in children {
                    let (new_child_id, new_span) =
                        recurse(tree, child_id, page_id_threshold, &mut |tree| {
                            make_dirty_inc_parents(tree, &mut leaf, parent_make_dirty)
                                .map(|o| o.is_some())
                        })?;
                    if new_child_id != child_id {
                        if let Some(mut leaf) =
                            make_dirty_inc_parents(tree, &mut leaf, parent_make_dirty)?
                        {
                            leaf.insert_kv(
                                Ok(i),
                                b"",
                                &MaybeValue::Overflow([new_child_id, new_span]),
                            )
                            .unwrap();
                        } else {
                            break;
                        }
                    }
                }
                node = leaf.into_untyped();
            }
            let result = (node.id(), node.span());
            tree.tx.stash_node(node)?;
            Ok(result)
        }

        if self.value.root != PageId::default() {
            let (new_root_id, _) =
                recurse(self, self.value.root, page_id_threshold, &mut |_| Ok(true))?;
            self.set_root(new_root_id);
        }
        Ok(())
    }
}

fn node_split_right<TYPE: NodeRepr>(
    tree: &mut Tree,
    prefix: &NodePrefix<'_>,
    node: &mut Node<TYPE>,
    full_key: Option<&[u8]>,
    needed_idx: Result<usize, usize>,
    needed_size: usize,
) -> Result<(bool, Vec<u8>, Node<TYPE>), Error> {
    trace!("node_split_right {}", node.id());
    if node.is_leaf() {
        debug_assert!(node.num_keys() >= MIN_LEAF_KEYS);
    } else {
        debug_assert!(needed_idx.is_err() || (needed_idx == Ok(0) && needed_size == 0));
        debug_assert!(node.num_keys() + 1 >= MIN_BRANCH_KEYS * 2 + 1);
    }
    let node_key_prefix = node.key_prefix();
    // init lower and upper bounds with the provided key
    let mut sep_lb = if let Some(full_key) = full_key {
        Ok(&full_key[node_key_prefix.len()..])
    } else {
        debug_assert_eq!((needed_idx, needed_size), (Ok(0), 0));
        Ok(node.key_at(0))
    };
    let mut sep_ub = sep_lb;
    let mid_key_idx = node.num_keys() / 2;
    let (needed_goes_on_left, key_split_idx) = match needed_idx {
        // for branches mid_key_idx is >= 1 and the parent will pull the min key from the right
        Err(i) if node.is_branch() && i < mid_key_idx => {
            sep_ub = Err(mid_key_idx - 1);
            (true, mid_key_idx - 1)
        }
        // i >= mid_key_idx
        Err(i) if node.is_branch() => {
            if i != mid_key_idx {
                sep_ub = Err(mid_key_idx)
            };
            (false, mid_key_idx)
        }
        // -- for leafs mid_key_idx is >= 0
        // Optimize for sequential inserts
        Err(i) if i == node.num_keys() => {
            sep_lb = Err(node.num_keys() - 1);
            (false, node.num_keys())
        }
        Err(i) if i <= mid_key_idx => {
            if i != mid_key_idx {
                sep_lb = Err(mid_key_idx - 1)
            };
            sep_ub = Err(mid_key_idx);
            (true, mid_key_idx)
        }
        // i > mid_key_idx
        Err(i) => {
            sep_lb = Err(mid_key_idx);
            if i != mid_key_idx + 1 {
                sep_ub = Err(mid_key_idx + 1)
            };
            (false, mid_key_idx + 1)
        }
        Ok(i) => {
            sep_lb = Err(mid_key_idx - 1);
            sep_ub = Err(mid_key_idx);
            (i < mid_key_idx, mid_key_idx)
        }
    };
    debug_assert!(key_split_idx != 0 || needed_idx == Err(0));
    let sep_ub = sep_ub.unwrap_or_else(|i| node.key_at(i));
    let separator_suffix = if node.is_leaf() && tree.value.fixed_key_len < 0 {
        // use suffix compression for leaf separators w/o fixed len keys
        let sep_lb = sep_lb.unwrap_or_else(|i| node.key_at(i));
        &sep_ub[..common_prefix_len(sep_lb, sep_ub) + 1]
    } else {
        sep_ub
    };
    let full_separator = [node_key_prefix, separator_suffix].concat();
    let (left_prefix, right_prefix) = prefix.max_prefix_split(&full_separator);

    // Right doesn't take anything from left
    if key_split_idx == node.num_keys() {
        debug_assert!(!needed_goes_on_left);
        let mut right_size = Node::<TYPE>::static_header_size(right_prefix.len()) + needed_size;
        // needed_size was calculated based on the old prefix, account for any deltas
        match NodePrefix::prefix_delta(node_key_prefix, &right_prefix) {
            Ok(i) => right_size -= i,
            Err(p) => right_size += p.len(),
        }
        let mut right = Node::new(
            &tree.value,
            node.header().level,
            tree.allocate_node(right_size, Some(node.header().level))?,
        );
        right.as_dirty().set_key_prefix(&right_prefix);
        return Ok((false, full_separator, right));
    }

    let left_prefix_delta = &NodePrefix::prefix_delta(node_key_prefix, &left_prefix);
    let right_prefix_delta = &NodePrefix::prefix_delta(node_key_prefix, &right_prefix);
    let mut left_size = Node::<TYPE>::static_header_size(left_prefix.len());
    let mut right_size = Node::<TYPE>::static_header_size(right_prefix.len());
    // needed_size was calculated based on the old prefix, account for any deltas
    if needed_goes_on_left {
        left_size += needed_size;
        match (*left_prefix_delta, needed_idx) {
            (Ok(_), Ok(_)) => (),
            (Ok(i), Err(_)) => left_size -= i,
            (Err(p), _) => left_size += p.len(),
        }
    } else {
        right_size += needed_size;
        match (*right_prefix_delta, needed_idx) {
            (Ok(_), Ok(_)) => (),
            (Ok(i), Err(_)) => right_size -= i,
            (Err(p), _) => right_size += p.len(),
        }
    }
    {
        let (a, b) = node.keys_split_sizes(key_split_idx, left_prefix_delta, right_prefix_delta);
        left_size += a;
        right_size += b;
    }

    tree.tx.free_page(node)?;
    let mut left = Node::new(
        &tree.value,
        node.header().level,
        tree.allocate_node(left_size, Some(node.header().level))?,
    );
    let mut left_mut = left.as_dirty();
    left_mut.set_key_prefix(&left_prefix);
    left_mut.copy_keys_from(left_prefix_delta, node, 0, key_split_idx);
    let mut right = Node::new(
        &tree.value,
        node.header().level,
        tree.allocate_node(right_size, Some(node.header().level))?,
    );
    let mut right_mut = right.as_dirty();
    right_mut.set_key_prefix(&right_prefix);
    right_mut.copy_keys_from(right_prefix_delta, node, key_split_idx, node.num_keys());

    *node = left;
    if node.is_branch() {
        debug_assert_eq!(
            node.as_branch().pointer_at(node.num_keys()),
            right.as_branch().pointer_at(0)
        );
    }

    Ok((needed_goes_on_left, full_separator, right))
}

fn leaf_split_insert_kv(
    tree: &mut Tree,
    prefix: &NodePrefix<'_>,
    node: LeafNode,
    search: Result<usize, usize>,
    needed: usize,
    full_key: &[u8],
    value: &MaybeValue,
) -> Result<MutateResult, Error> {
    trace!(
        "splitting leaf {} ({}) w/ {} keys (search {:?} needed {})",
        node.id(),
        node.span(),
        node.num_keys(),
        search,
        needed
    );

    let _old_node_id = node.id();
    let mut left = node;
    let (goes_on_left, separator, mut right) =
        node_split_right(tree, prefix, &mut left, Some(full_key), search, needed)?;
    trace!(
        "leaf split {} into {}({}) {}({}), pos {:?}, goes_on_left {goes_on_left:?}, sizes {} {}",
        _old_node_id,
        left.id(),
        left.span(),
        right.id(),
        right.span(),
        search,
        left.num_keys(),
        right.num_keys()
    );
    let existing = if goes_on_left {
        let key_suffix = &full_key[left.key_prefix_len()..];
        left.as_dirty().insert_kv(search, key_suffix, value)
    } else {
        let key_suffix = &full_key[right.key_prefix_len()..];
        let location = search
            .map(|i| i - left.num_keys())
            .map_err(|i| i - left.num_keys());
        right.as_dirty().insert_kv(location, key_suffix, value)
    };
    tree.free_value(&existing.unwrap())?;

    // debug_assert!(
    //     (0..left.num_keys())
    //         .map(|i| left.full_key_at(i))
    //         .chain((0..right.num_keys()).map(|i| right.full_key_at(i)))
    //         .is_sorted(),
    //     "keys not sorted\n{:#?}\n{:#?}",
    //     left,
    //     right
    // );
    debug_assert!(left.full_key_at(left.num_keys() - 1) < right.full_key_at(0));
    debug_assert!(
        (left.num_keys() != 0 && right.num_keys() != 0)
            && (right.num_keys() == 1 || left.num_keys().abs_diff(right.num_keys()) <= 1),
        "{} {}",
        left.num_keys(),
        right.num_keys()
    );
    debug_assert!(left.full_key_at(left.num_keys() - 1) < right.full_key_at(0));
    let left_id = left.id();
    let right_id = right.id();
    tree.tx.stash_node(left)?;
    tree.tx.stash_node(right)?;
    Ok(MutateResult::Split(left_id, right_id, separator))
}

fn branch_split_insert_child(
    tree: &mut Tree,
    parent_prefix: &NodePrefix<'_>,
    node: BranchNode,
    ptr_idx: usize,
    needed: usize,
    new_key: Vec<u8>,
    child_left_id: PageId,
    child_right_id: PageId,
) -> Result<MutateResult, Error> {
    trace!("splitting branch {} insert child", node.id());
    let _old_node_id = node.id();
    let mut left = node;
    let (goes_on_left, separator, mut right) = node_split_right(
        tree,
        parent_prefix,
        &mut left,
        Some(&new_key),
        Err(ptr_idx),
        needed,
    )?;
    trace!(
        "insert child branch split {} into {}({}) {}({}), ptr_idx {:?}, sizes {} {}",
        _old_node_id,
        left.id(),
        left.span(),
        right.id(),
        right.span(),
        ptr_idx,
        left.num_keys(),
        right.num_keys(),
    );

    // Insert separator and child pointer in one of the resulting nodes
    let mut left_mut = left.as_dirty();
    if goes_on_left {
        let key_suffix = &new_key[left_mut.key_prefix_len()..];
        left_mut
            .insert_kp(Err(ptr_idx), key_suffix, child_right_id)
            .unwrap();
        left_mut.set_pointer_at(ptr_idx, child_left_id);
    } else {
        let right_idx = ptr_idx - left_mut.num_keys();
        let key_suffix = &new_key[right.key_prefix_len()..];
        let mut right_mut = right.as_dirty();
        right_mut
            .insert_kp(Err(right_idx), key_suffix, child_right_id)
            .unwrap();
        if right_idx == 0 {
            left_mut.set_pointer_at(left_mut.num_keys(), child_left_id);
        } else {
            right_mut.set_pointer_at(right_idx, child_left_id);
        }
    }
    debug_assert!(left.full_key_at(left.num_keys() - 1) < right.full_key_at(0));
    // TODO: it's inefficient for this key to be handled as part the split just to be taken out
    let mut right_mut = right.as_dirty();
    right_mut.set_pointer_at(0, right_mut.pointer_at(1));
    right_mut.remove_key_repr_at(0);
    debug_assert!(
        (left.num_keys() != 0 && right.num_keys() != 0)
            && left.num_keys().abs_diff(right.num_keys()) <= 1,
        "{} {}",
        left.num_keys(),
        right.num_keys()
    );
    let left_id = left.id();
    let right_id = right.id();
    tree.tx.stash_node(left)?;
    tree.tx.stash_node(right)?;
    Ok(MutateResult::Split(left_id, right_id, separator))
}

fn branch_merge_children(
    tree: &mut Tree,
    node: &mut BranchNode,
    ptr_idx: usize,
    single_delete_mode: bool,
) -> Result<MutateResult, Error> {
    // In single delete mode the result can be a Split, in case the items are rebalanced between siblings.
    // Else, in range delete, the result won't ever be a Split and there's a downwards merge step trying
    // to connect branch grandchildren that underflown and couldn't be merged with a sibling.
    trace!("branch_merge_children {} idx {}", node.id(), ptr_idx);
    let left_idx = ptr_idx.saturating_sub(1);
    let child_left = tree.tx.pop_node(node.pointer_at(left_idx))?;
    let child_right = tree.tx.pop_node(node.pointer_at(left_idx + 1))?;
    trace!(
        "branch_merge_children {} left {} right {} level {}",
        node.id(),
        child_left.id(),
        child_right.id(),
        child_left.node_header().level
    );
    let min_span;
    let min_keys_to_split;
    // let mutsingle_grandchild_child = None;
    let merged;
    if child_left.is_branch() {
        let child_w_single_child = if child_left.num_keys() == 0 {
            Some(0)
        } else if child_right.num_keys() == 0 {
            Some(child_left.num_keys() + 1)
        } else {
            None
        };
        min_keys_to_split = MIN_BRANCH_KEYS * 2 + 1;
        min_span = tree.value.min_branch_node_pages as PageId;
        let mut child = node_merge_right_into_left(
            tree,
            node,
            left_idx,
            Node::into_branch(child_left),
            Node::into_branch(child_right),
        )?;
        match child_w_single_child {
            Some(grandchild_idx) if !single_delete_mode => {
                let grandchild_id = child.as_branch().pointer_at(grandchild_idx);
                if tree.tx.clone_node(grandchild_id)?.num_keys() == 0 {
                    branch_merge_children(tree, &mut child, grandchild_idx, false)?;
                }
            }
            _ => (),
        }
        merged = child.into_untyped();
    } else {
        min_keys_to_split = MIN_LEAF_KEYS * 2;
        min_span = tree.value.min_leaf_node_pages as PageId;
        merged = node_merge_right_into_left(
            tree,
            node,
            left_idx,
            Node::into_leaf(child_left),
            Node::into_leaf(child_right),
        )
        .map(Node::into_untyped)?;
    }
    trace!(
        "branch_merge_children {} done merged child {} is {}",
        node.id(),
        ptr_idx,
        merged.id()
    );
    if single_delete_mode && merged.span() > min_span && merged.num_keys() >= min_keys_to_split {
        trace!(
            "splitting big merged, span {}, keys {}, leaf {:?}",
            merged.span(),
            merged.num_keys(),
            merged.is_leaf(),
        );
        let prefix = &node.prefix_for_child(left_idx);
        let (left, new_key, right) = if merged.is_leaf() {
            let mut merged = merged.into_leaf();
            let (_, new_key, right) = node_split_right(tree, prefix, &mut merged, None, Ok(0), 0)?;
            (merged.into_untyped(), new_key, right.into_untyped())
        } else {
            let mut merged = merged.into_branch();
            let (_, new_key, mut right) =
                node_split_right(tree, prefix, &mut merged, None, Ok(0), 0)?;
            // TODO: it's inefficient for this key to be handled as part the split just to be taken out
            let mut right_mut = right.as_dirty();
            right_mut.set_pointer_at(0, right_mut.pointer_at(1));
            right_mut.remove_key_repr_at(0);
            (merged.into_untyped(), new_key, right.into_untyped())
        };
        let left_id = left.id();
        let right_id = right.id();
        tree.tx.stash_node(left)?;
        tree.tx.stash_node(right)?;
        Ok(MutateResult::Split(left_id, right_id, new_key))
    } else {
        tree.tx.stash_node(merged)?;
        Ok(if node.num_keys() >= MIN_BRANCH_KEYS {
            MutateResult::Ok(node.id())
        } else {
            MutateResult::Underflow(node.id())
        })
    }
}

fn node_merge_right_into_left<TYPE: NodeRepr>(
    tree: &mut Tree,
    node: &mut BranchNode,
    left_idx: usize,
    mut left_child: Node<TYPE>,
    right_child: Node<TYPE>,
) -> Result<Node<TYPE>, Error> {
    assert_eq!(left_child.is_leaf(), right_child.is_leaf());
    let middle_key = left_child.is_branch().then(|| node.full_key_at(left_idx));
    let middle_key = middle_key.as_deref();
    trace!(
        "Merging nodes, Left {:?} ({}k), Right {:?} ({}k), MiddleK {:?}, Level {}",
        left_child.id(),
        left_child.num_keys(),
        right_child.id(),
        right_child.num_keys(),
        middle_key.map(EscapedBytes),
        left_child.header().level
    );

    let needed_as_is = if left_child.key_prefix() == right_child.key_prefix() {
        right_child.real_occupied_size()
            - Node::<TYPE>::static_header_size(right_child.key_prefix_len())
            + middle_key.map_or(0, |mk| {
                // when merging branch nodes the merge key needs to be put in between the left and right keys
                BranchNode::<false>::size_for_kp(&tree.value, mk) - right_child.key_prefix_len()
            })
    } else {
        usize::MAX
    };

    // Merge right page into left page
    tree.tx.free_page(&right_child)?;

    let mut left_child_mut;
    let right_prefix_delta;
    if left_child.real_size_left() >= needed_as_is {
        left_child_mut = tree.tx.make_dirty(&mut left_child)?;
        if left_child_mut.curr_size_left() < needed_as_is {
            left_child_mut.compact_tail(&mut tree.tx.scratch_buffer.borrow_mut());
        }
        right_prefix_delta = Ok(0);
    } else {
        let max_prefix = node.prefix_for_child(left_idx).max_prefix_merge_right();
        let left_prefix_delta = NodePrefix::prefix_delta(left_child.key_prefix(), &max_prefix);
        right_prefix_delta = NodePrefix::prefix_delta(right_child.key_prefix(), &max_prefix);
        let total_required = left_child.real_occupied_size_with_prefix(&left_prefix_delta)
            + right_child.real_occupied_size_with_prefix(&right_prefix_delta)
            - Node::<TYPE>::static_header_size(max_prefix.len())
            + middle_key.map_or(0, |mk| {
                // when merging branch nodes the merge key needs to be put in between the left and right keys
                BranchNode::<false>::size_for_kp(&tree.value, mk) - max_prefix.len()
            });

        tree.tx.free_page(&left_child)?;
        let mut new_left_child = Node::new(
            &tree.value,
            left_child.header().level,
            tree.allocate_node(total_required, Some(left_child.header().level))?,
        );

        new_left_child.as_dirty().copy_everything_with_prefix(
            &left_child,
            &max_prefix,
            &left_prefix_delta,
        );
        left_child = new_left_child;
        left_child_mut = left_child.as_dirty();
    }

    if let Some(mk) = middle_key {
        let mut left_child_mut = left_child_mut.as_branch();
        let right_child = right_child.as_branch();
        let mk_suffix = &mk[left_child_mut.key_prefix_len()..];
        left_child_mut
            .insert_kp(
                Err(left_child_mut.num_keys()),
                mk_suffix,
                right_child.pointer_at(0),
            )
            .unwrap();
    }

    // Remove key in parent consumed during merging
    let mut node_mut = tree.tx.make_dirty(node)?;
    node_mut.remove_key_repr_at(left_idx);
    node_mut.set_pointer_at(left_idx, left_child_mut.id());

    if right_child.is_leaf() || right_child.as_branch().pointer_at(0) != PageId::default() {
        left_child_mut.copy_keys_from(&right_prefix_delta, &right_child, 0, right_child.num_keys());
    } else {
        // right child is completely empty
        debug_assert_eq!(right_child.num_keys(), 0);
    }

    Ok(left_child)
}

fn remove_range_node(
    tree: &mut Tree,
    node_id: PageId,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Result<MutateResult, Error> {
    fn search_bound<TYPE: NodeType + NodeRepr>(
        node: &Node<TYPE>,
        bound: Bound<&[u8]>,
        is_end: bool,
    ) -> usize {
        match bound {
            Bound::Included(b) | Bound::Excluded(b) => match node.search_keys(b) {
                Ok(i) if is_end => i + matches!(bound, Bound::Included(_)) as usize,
                Ok(i) => i + matches!(bound, Bound::Excluded(_)) as usize,
                Err(i) => i,
            },
            Bound::Unbounded if is_end => node.num_keys(),
            Bound::Unbounded => 0,
        }
    }

    let node = tree.tx.pop_node(node_id)?;
    if node.is_leaf() {
        let mut node = node.into_leaf();
        let start_i = search_bound(&node, start, false);
        let end_i = search_bound(&node, end, true);
        if start_i >= end_i {
            let id = node.id();
            tree.tx.stash_node(node)?;
            return Ok(MutateResult::Ok(id));
        }
        tree.inc_num_keys(-((end_i - start_i) as i64));
        if start_i == 0 && end_i == node.num_keys() {
            for (ov_id, ov_span) in node.overflow_children() {
                tree.tx.free_page_with_id(ov_id, ov_span)?;
            }
            tree.tx.free_page(&node)?;
            return Ok(MutateResult::Underflow(0));
        }
        let mut leaf_mut = tree.tx.make_dirty(&mut node)?;
        for i in (start_i..end_i).rev() {
            tree.free_value(&leaf_mut.value_at(i))?;
            leaf_mut.remove_key_repr_at(i);
        }
        let (id, num_keys) = (node.id(), node.num_keys());
        tree.tx.stash_node(node)?;
        return Ok(if num_keys >= MIN_LEAF_KEYS {
            MutateResult::Ok(id)
        } else {
            MutateResult::Underflow(id)
        });
    }
    let mut node = node.into_branch();
    if matches!((&start, &end), (Bound::Unbounded, Bound::Unbounded)) {
        for child_id in node.children() {
            remove_range_node(tree, child_id, Bound::Unbounded, Bound::Unbounded)?;
        }
        tree.tx.free_page(&node)?;
        return Ok(MutateResult::Underflow(0));
    }
    let mut child_idx = search_bound(&node, start, false);
    let mut child_end_idx = search_bound(&node, end, true) + 1;
    let mut optimized_start = start;
    while child_idx < child_end_idx {
        let optimized_end = if child_idx + 1 == child_end_idx {
            end
        } else {
            Bound::Unbounded
        };
        let child_id = node.pointer_at(child_idx);
        match remove_range_node(tree, child_id, optimized_start, optimized_end)? {
            MutateResult::Underflow(0) => {
                let mut branch = tree.tx.make_dirty(&mut node)?;
                if child_idx != 0 {
                    branch.remove_key_repr_at(child_idx - 1);
                } else {
                    let mut new_ptr_0 = 0;
                    if branch.num_keys() != 0 {
                        new_ptr_0 = branch.pointer_at(1);
                        branch.remove_key_repr_at(0);
                    };
                    branch.set_pointer_at(0, new_ptr_0);
                }
                child_end_idx -= 1;
                optimized_start = Bound::Unbounded;
            }
            // merge two children if possible, if at the end we're left w/ a single child this node
            // will underflow and the caller will deal with it.
            MutateResult::Underflow(new_child_id) if node.num_keys() != 0 => {
                if new_child_id != child_id {
                    tree.tx
                        .make_dirty(&mut node)?
                        .set_pointer_at(child_idx, new_child_id);
                }
                branch_merge_children(tree, &mut node, child_idx, false)?;
                if child_idx != 0 {
                    child_idx -= 1;
                    optimized_start = start;
                }
                child_end_idx -= 1;
            }
            MutateResult::Underflow(new_child_id) | MutateResult::Ok(new_child_id) => {
                if new_child_id != child_id {
                    tree.tx
                        .make_dirty(&mut node)?
                        .set_pointer_at(child_idx, new_child_id);
                }
                child_idx += 1;
                optimized_start = Bound::Unbounded;
            }
            MutateResult::Split(..) => unreachable!(),
        }
    }
    let (id, num_keys) = (node.id(), node.num_keys());
    if num_keys == 0 && node.pointer_at(0) == PageId::default() {
        tree.tx.free_page(&node)?;
        return Ok(MutateResult::Underflow(0));
    }
    tree.tx.stash_node(node)?;
    Ok(if num_keys >= MIN_BRANCH_KEYS {
        MutateResult::Ok(id)
    } else {
        MutateResult::Underflow(id)
    })
}
