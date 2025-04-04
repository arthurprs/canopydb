use std::mem;

use dashmap::DashMap;
use smallvec::SmallVec;

use crate::{
    bytes::Bytes,
    page::Page,
    repr::{PageId, TxId},
    ReservedPageHeader, SharedBytes, PAGE_SIZE,
};

// Do not use shuttle atomics, as they may trigger shuttle yields
// while possibly holding locks to dashmap shards.
use std::sync::atomic::{AtomicIsize, Ordering};

#[derive(Debug, Clone)]
pub enum Item {
    /// An uncompressed clean page
    Page(Page),
    /// Page is now available at the file
    /// (page_id, span, latest)
    /// TODO: explain latest
    Redirected(PageId, PageId, bool /* latest */),
}

#[derive(Debug)]
enum InnerItem {
    Page(SharedBytes),
    Redirected(PageId, PageId, bool),
    None,
}

impl InnerItem {
    #[inline]
    fn to_item(&self) -> Option<Item> {
        match self {
            Self::Page(bytes) => Some(Item::Page(Page {
                dirty: false,
                compressed_page: None,
                raw_data: Bytes::from_arc_bytes(bytes.clone()),
            })),
            Self::Redirected(a, b, c) => Some(Item::Redirected(*a, *b, *c)),
            Self::None => None,
        }
    }

    #[inline]
    fn into_item(self) -> Option<Item> {
        match self {
            Self::Page(bytes) => Some(Item::Page(Page {
                dirty: false,
                compressed_page: None,
                raw_data: Bytes::from_arc_bytes(bytes),
            })),
            Self::Redirected(a, b, c) => Some(Item::Redirected(a, b, c)),
            Self::None => None,
        }
    }

    #[inline]
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    #[inline]
    fn pages(&self) -> usize {
        if let Self::Page(page) = self {
            (size_of::<ReservedPageHeader>() + page.as_slice().len()) / PAGE_SIZE as usize
        } else {
            0
        }
    }
}

impl From<Item> for InnerItem {
    #[inline]
    fn from(item: Item) -> Self {
        match item {
            Item::Page(page) => {
                debug_assert!(!page.dirty);
                debug_assert_eq!(page.compressed_page, None);
                let ptr_range = page.raw_data.as_ref().as_ptr_range();
                let bytes = page.raw_data.into_shared_bytes();
                debug_assert_eq!(bytes.as_slice().as_ptr_range(), ptr_range);
                Self::Page(bytes)
            }
            Item::Redirected(a, b, c) => Self::Redirected(a, b, c),
        }
    }
}

#[derive(Debug, Default)]
#[repr(C, align(64))]
pub(crate) struct PageTable {
    // Using quality for the time being to avoid degenerate cases https://github.com/rust-lang/hashbrown/issues/577
    table: DashMap<PageId, SmallVec<(TxId, InnerItem), 1>, foldhash::quality::RandomState>,
    spans_used: AtomicIsize,
}

impl PageTable {
    pub fn len_upper_bound(&self) -> usize {
        self.table.len()
    }

    /// Shadows a page. If this mutation shadowed a Page entry return its TxId
    pub fn insert(
        &self,
        tx_id: TxId,
        page_id: PageId,
        item: impl Into<Option<Item>>,
        force: bool,
    ) -> Option<TxId> {
        self.insert_with(
            tx_id,
            page_id,
            item,
            force,
            |last_tx_id, last_i| match last_i {
                InnerItem::Page(..) => Some(last_tx_id),
                InnerItem::Redirected(..) | InnerItem::None => None,
            },
        )
    }

    pub fn insert_w_shadowed(
        &self,
        tx_id: TxId,
        page_id: PageId,
        item: impl Into<Option<Item>>,
        force: bool,
    ) -> Option<(TxId, Item)> {
        self.insert_with(tx_id, page_id, item, force, |last_tx_id, last_i| {
            Some((last_tx_id, last_i.to_item()?))
        })
    }

    #[inline]
    fn insert_with<T>(
        &self,
        tx_id: TxId,
        page_id: PageId,
        item: impl Into<Option<Item>>,
        force: bool,
        map: impl FnOnce(TxId, &InnerItem) -> Option<T>,
    ) -> Option<T> {
        let item = item.into();
        trace!(
            "{} tx_id {tx_id} page {page_id} is_page {:?}",
            if item.is_some() { "insert" } else { "remove" },
            matches!(item, Some(Item::Page(_)))
        );
        let i_item;
        let create = if let Some(item) = item {
            i_item = InnerItem::from(item);
            self.spans_used
                .fetch_add(i_item.pages() as isize, Ordering::Relaxed);
            true
        } else {
            i_item = InnerItem::None;
            force
        };
        let mut values = if create {
            self.table.entry(page_id).or_default()
        } else {
            self.table.get_mut(&page_id)?
        };
        let result = values.last().and_then(|(last_tx_id, last_i)| {
            debug_assert!(tx_id > *last_tx_id);
            map(*last_tx_id, last_i)
        });
        values.push((tx_id, i_item));
        result
    }

    pub fn replace_at(&self, tx_id: TxId, page_id: PageId, item: Item) -> Option<(Item, bool)> {
        trace!("replace at tx_id {tx_id} page {page_id}");
        let item = InnerItem::from(item);
        let item_pages = item.pages() as isize;
        let dashmap::mapref::entry::Entry::Occupied(mut o) = self.table.entry(page_id) else {
            return None;
        };
        let values = o.get_mut();
        let (latest, replaced) = match values.binary_search_by(|(i, _)| i.cmp(&tx_id)) {
            Ok(i) => (i + 1 == values.len(), mem::replace(&mut values[i].1, item)),
            Err(_) => return None,
        };
        drop(o);
        let page_delta = item_pages - replaced.pages() as isize;
        self.spans_used.fetch_add(page_delta, Ordering::Relaxed);
        Some((replaced.into_item()?, latest))
    }

    pub fn remove_at(&self, tx_id: TxId, page_id: PageId) -> Option<Item> {
        trace!("remove_at tx_id {tx_id} page {page_id}");
        let _removed_vec; // make sure removed vecs are dropped outside the lock
        let removed;
        if let dashmap::mapref::entry::Entry::Occupied(mut o) = self.table.entry(page_id) {
            let values = o.get_mut();
            let Ok(i) = values.binary_search_by(|(i, _)| i.cmp(&tx_id)) else {
                return None;
            };
            removed = mem::replace(&mut values[i].1, InnerItem::None);
            if i == 0 {
                let nones = 1 + values[1..].iter().take_while(|(_, i)| i.is_none()).count();
                if nones < values.len() {
                    values.drain(..nones);
                } else {
                    _removed_vec = o.remove();
                }
            }
        } else {
            return None;
        }

        self.spans_used
            .fetch_sub(removed.pages() as isize, Ordering::Relaxed);
        removed.into_item()
    }

    #[inline]
    fn peek<T>(
        &self,
        tx_id: TxId,
        page_id: PageId,
        map: impl FnOnce(TxId, &InnerItem) -> Option<T>,
    ) -> Option<T> {
        if let Some(values) = self.table.get(&page_id) {
            // look at the latest entry before falling back to a binary search
            let (last, rest) = values.split_last().unwrap();
            let (from, item) = if last.0 <= tx_id {
                last
            } else {
                match rest.binary_search_by(|(i, _)| i.cmp(&tx_id)) {
                    Ok(i) => &rest[i],
                    Err(0) => return None,
                    Err(i) => &rest[i - 1],
                }
            };
            map(*from, item)
        } else {
            None
        }
    }

    pub fn get(&self, tx_id: TxId, page_id: PageId) -> Option<(TxId, Item)> {
        trace!("get tx_id {tx_id} page {page_id}");
        self.peek(tx_id, page_id, |from, item| Some((from, item.to_item()?)))
    }

    pub fn is_latest_from_lte(&self, tx_id: TxId, page_id: PageId) -> bool {
        trace!("is_latest_from_lte tx_id {tx_id} page {page_id}");
        if let Some(values) = self.table.get(&page_id) {
            values.last().is_none_or(|&(from, _)| from <= tx_id)
        } else {
            true
        }
    }

    pub fn is_page_from_snapshot(
        &self,
        tx_id: TxId,
        ongoing_snapshot: Option<TxId>,
        page_id: PageId,
    ) -> bool {
        trace!("peek_is_page_from_snapshot tx_id {tx_id} page {page_id}");
        self.peek(tx_id, page_id, |from, i| match (i, ongoing_snapshot) {
            (InnerItem::Page(..), None) => Some(()),
            (InnerItem::Page(..), Some(ockp)) => (from > ockp).then_some(()),
            (InnerItem::Redirected(..) | InnerItem::None, _) => None,
        })
        .is_none()
    }

    pub fn iter_latest_items(
        &self,
        tx_id: TxId,
    ) -> impl Iterator<Item = (PageId, TxId, Item)> + '_ {
        trace!("iter_latest_items start");
        self.table.iter().filter_map(move |kv| {
            let (last, rest) = kv.value().split_last().unwrap();
            let (from, item) = if last.0 <= tx_id {
                last
            } else {
                match rest.binary_search_by(|(i, _)| i.cmp(&tx_id)) {
                    Ok(i) => &rest[i],
                    Err(0) => return None,
                    Err(i) => &rest[i - 1],
                }
            };
            Some((*kv.key(), *from, item.to_item()?))
        })
    }

    pub fn iter_all_items(&self, tx_id: TxId, mut cb: impl FnMut(PageId, TxId, Item, bool)) {
        trace!("iter_all_items start");
        for kv in &self.table {
            let mut latest = true;
            for (from, item) in kv
                .value()
                .iter()
                .rev()
                .skip_while(|(from, _)| *from > tx_id)
            {
                if let Some(item) = item.to_item() {
                    cb(*kv.key(), *from, item, latest);
                }
                latest = false;
            }
        }
    }

    pub fn clear_latest_tx(&self, tx_id: TxId) {
        trace!("clear_latest_tx {tx_id}");
        let mut drop_list = SmallVec::<InnerItem, 20>::new();
        self.table.retain(|_page_id, values| {
            debug_assert!(values.last().unwrap().0 <= tx_id);
            if values.last().is_some_and(|(i, _)| i == &tx_id) {
                let (_, pop) = values.pop().unwrap();
                drop_list.push(pop);
                !values.is_empty()
            } else {
                true
            }
        });
        let pages_delta = drop_list.into_iter().map(|d| d.pages()).sum::<usize>();
        self.spans_used
            .fetch_sub(pages_delta as isize, Ordering::Relaxed);
    }

    pub fn spans_used(&self) -> usize {
        let pages_used = self.spans_used.load(Ordering::Relaxed);
        debug_assert!(pages_used >= 0);
        pages_used as usize
    }
}
