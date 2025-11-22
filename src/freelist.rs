use std::{
    io::{self, Read, Write},
    mem,
};

use crate::error::{io_invalid_data, Error};
use smallvec::SmallVec;
use triomphe::Arc;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

type PageId = u32;

/// Max allocation span, 4GB wide.
const MAX_ALLOCATION_SPAN: u32 = ((4u64 << 30) / crate::PAGE_SIZE) as u32;

const SHARD_MAX_LEN: PageId = if cfg!(fuzzing) {
    1024
} else {
    u16::MAX as PageId + 1
};

/// SHARD_MAX_LEN can only be up to 2**16 due to u16 being used throughout to save space.
const _: () = assert!(SHARD_MAX_LEN <= u16::MAX as PageId + 1);

// 3 shards can cover up to 786MB with a 4KB page size
type ShardsVec = SmallVec<Shard, 3>;

// Inline length adjusted so that the SmallVec size is 128 bytes
type RangeVec = SmallVec<Range, 29>;

/// A COW freelist
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Freelist {
    shards: Option<Arc<ShardsVec>>,
}

/// Like free list but can deffer merges and serve bulk_allocate from deferred Freelists
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DeferredFreelist {
    freelist: Freelist,
    deferred: Vec<Freelist>,
}

impl DeferredFreelist {
    pub fn bulk_allocate(
        &mut self,
        min_span: PageId,
        bulk_span: PageId,
        precise: bool,
    ) -> Result<Freelist, Error> {
        if self.deferred.last().is_some_and(|f| f.len() >= min_span) {
            return Ok(self.deferred.pop().unwrap());
        }
        Ok(self.merged()?.bulk_allocate(bulk_span, precise))
    }

    pub fn append(&mut self, other: Freelist) {
        if !other.is_empty() {
            self.deferred.push(other);
        }
    }

    pub fn into_merged(mut self) -> Result<Freelist, Error> {
        Ok(mem::take(self.merged()?))
    }

    pub fn merged(&mut self) -> Result<&mut Freelist, Error> {
        for f in self.deferred.drain(..) {
            self.freelist.merge(&f)?;
        }
        Ok(&mut self.freelist)
    }

    pub fn max_serialized_size(&self) -> usize {
        self.freelist.serialized_size()
            + self
                .deferred
                .iter()
                .map(Freelist::serialized_size)
                .sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.freelist.is_empty() && self.deferred.is_empty()
    }

    #[cfg(test)]
    pub fn len(&self) -> PageId {
        self.freelist.len() + self.deferred.iter().map(Freelist::len).sum::<PageId>()
    }
}

/// A Freelist shard that can track pages in the range of base..base+SHARD_MAX_LEN.
/// For 4K pages and SHARD_MAX_LEN 2**16, each shard spans 256MB.
#[derive(Debug, Clone, PartialEq)]
struct Shard {
    /// The start page for this shard, a multiple of SHARD_MAX_LEN
    base: PageId,
    /// How many pages currently held in the shard.
    /// Not to be confused with the number of ranges.
    len: PageId,
    /// Ranges size histogram used to quickly rule out queries that wouldn't succeed
    /// Most implementations track the ranges for a given size class instead,
    /// but that's too expensive for COW.
    hist: Histogram,
    /// Invariant: all ranges are disjoint and sorted descending by position
    ranges: Arc<RangeVec>,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, IntoBytes, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
#[debug("Range({page}, {zspan})")]
struct Range {
    /// The start of the range within the shard
    page: u16,
    /// A zero based span (e.g. 0 zspan == 1 span)
    zspan: u16,
}

macro_rules! freelist_error {
    ($($arg:tt)*) => {{
        let msg = ::std::fmt::format(::std::format_args!($($arg)*));
        crate::Error::FreeList(Box::new(msg))
    }}
}

impl Range {
    #[inline]
    fn page(&self) -> PageId {
        self.page as PageId
    }
    #[inline]
    fn span(&self) -> PageId {
        self.zspan as PageId + 1
    }
    #[inline]
    fn page_end(&self) -> PageId {
        self.page() + self.span()
    }
}

/// Histogram for a range of disjoint u16s.
///
/// Stores exact value buckets then log2 buckets.
#[derive(Debug, Clone, PartialEq)]
struct Histogram([u16; Self::EXACT_BUCKETS + u16::BITS as usize]);

impl Default for Histogram {
    fn default() -> Self {
        Self([0; Self::EXACT_BUCKETS + u16::BITS as usize])
    }
}

impl Histogram {
    /// Numbers in 0..EXACT_BUCKETS will be mapped into buckets that only contain themselves
    const EXACT_BUCKETS: usize = 6;

    #[inline]
    fn class(zspan: u16) -> usize {
        if zspan <= Self::EXACT_BUCKETS as u16 {
            zspan as usize
        } else {
            Self::EXACT_BUCKETS + (zspan - Self::EXACT_BUCKETS as u16).ilog2() as usize
        }
    }
    #[inline]
    fn inc(&mut self, zspan: u16, amount: u16) {
        self.0[Self::class(zspan)] += amount;
    }
    #[inline]
    fn dec(&mut self, zspan: u16, amount: u16) {
        self.0[Self::class(zspan)] -= amount;
    }
    #[inline]
    fn any_gte(&self, zspan: u16) -> bool {
        self.0[Self::class(zspan)..].iter().any(|i| *i != 0)
    }
    fn merge(&mut self, other: &Self) {
        for (this, that) in self.0.iter_mut().zip(other.0.iter()) {
            *this += *that;
        }
    }
}

impl Shard {
    #[cfg(any(test, fuzzing))]
    fn iter_pages(&self) -> impl DoubleEndedIterator<Item = PageId> + '_ {
        self.ranges
            .iter()
            .rev()
            .flat_map(|r| (self.base + r.page())..(self.base + r.page_end()))
    }

    fn iter_spans(&self) -> impl DoubleEndedIterator<Item = (PageId, PageId)> + '_ {
        self.ranges
            .iter()
            .rev()
            .map(|r| (self.base + r.page(), r.span()))
    }

    fn first(&self) -> Option<(PageId, PageId)> {
        self.ranges.last().map(|r| (self.base + r.page(), r.span()))
    }

    fn last(&self) -> Option<(PageId, PageId)> {
        self.ranges
            .first()
            .map(|r| (self.base + r.page(), r.span()))
    }

    fn pop_last(&mut self) -> Option<(PageId, PageId)> {
        if self.ranges.is_empty() {
            return None;
        }
        let p = range_vec_make_mut(&mut self.ranges, 0).remove(0);
        self.hist.dec(p.zspan, 1);
        self.len -= p.span();
        Some((self.base + p.page(), p.span()))
    }

    /// Shift all contiguous ranges lower starting from `pos`.
    /// E.g. 10(..250), 260(..20) -> 10(..255), 275(0..5)
    /// E.g. 10(..10), 10(..20) -> 10(..30)
    fn shift_lower(pos: usize, ranges: &mut RangeVec, hist: &mut Histogram) {
        let (left, right) = ranges.split_at_mut(pos);
        let next = left.last_mut();
        let curr = right.first_mut();
        match (curr, next) {
            // Move spans from B to A
            (Some(a), Some(b)) if a.page_end() == b.page() => {
                hist.dec(a.zspan, 1);
                hist.dec(b.zspan, 1);
                let total_span = a.span() + b.span();
                a.zspan = (total_span - 1) as u16;
                hist.inc(a.zspan, 1);
                ranges.remove(pos - 1);
            }
            _ => (),
        }
    }

    pub fn free(&mut self, page_id: PageId, span: PageId) -> Result<(), Error> {
        debug_assert!(span > 0);
        debug_assert!(span <= SHARD_MAX_LEN);
        debug_assert!(page_id.checked_add(span).is_some());
        debug_assert_eq!(page_id / SHARD_MAX_LEN * SHARD_MAX_LEN, self.base);
        let page_id = page_id % SHARD_MAX_LEN;

        let ranges = range_vec_make_mut(&mut self.ranges, 1);
        let pos = match ranges.binary_search_by(|&Range { page, .. }| (page_id as u16).cmp(&page)) {
            Ok(i) | Err(i) => i,
        };

        let (left, right) = ranges.split_at_mut(pos);
        let next = left.last();
        let curr = right.first();
        match (curr, next) {
            (Some(a), _) if a.page_end() > page_id => {
                return Err(freelist_error!(
                    "Freed {} ({}) overlap with {} ({})",
                    page_id + self.base,
                    span,
                    a.page() + self.base,
                    a.span()
                ))
            }
            (_, Some(b)) if page_id + span > b.page() => {
                return Err(freelist_error!(
                    "Freed {} ({}) overlap with {} ({})",
                    page_id + self.base,
                    span,
                    b.page() + self.base,
                    b.span()
                ))
            }
            _ => (),
        }

        let next = left.last_mut();
        let curr = right.first_mut();
        match (curr, next) {
            // a < page_id and b > page_id
            (Some(a), Some(b)) if a.page_end() == page_id && page_id + span == b.page() => {
                self.hist.dec(a.zspan, 1);
                self.hist.dec(b.zspan, 1);
                let new_a_span = a.span() + span + b.span();
                self.hist.inc((new_a_span - 1) as u16, 1);
                a.zspan = (new_a_span - 1) as u16;
                ranges.remove(pos - 1);
            }
            // a < page_id
            (Some(a), _) if a.page_end() == page_id => {
                self.hist.dec(a.zspan, 1);
                let new_a_span = a.span() + span;
                a.zspan = (new_a_span - 1) as u16;
                self.hist.inc(a.zspan, 1);
            }
            // b > page_id
            (_, Some(b)) if page_id + span == b.page() => {
                self.hist.dec(b.zspan, 1);
                let new_b_span = b.span() + span;
                *b = Range {
                    page: page_id as u16,
                    zspan: (new_b_span - 1) as u16,
                };
                self.hist.inc(b.zspan, 1);
            }
            _ => {
                self.hist.inc((span - 1) as u16, 1);
                ranges.insert(
                    pos,
                    Range {
                        page: page_id as u16,
                        zspan: (span - 1) as u16,
                    },
                );
            }
        }

        self.len += span;
        Ok(())
    }

    fn remove(&mut self, mut page_id: PageId, mut span: PageId) -> PageId {
        debug_assert!(span > 0);
        debug_assert!(page_id.checked_add(span).is_some());
        debug_assert_eq!(page_id / SHARD_MAX_LEN * SHARD_MAX_LEN, self.base);
        let initial_len = self.len;
        page_id %= SHARD_MAX_LEN;

        let ranges = range_vec_make_mut(&mut self.ranges, 0);
        let mut pos = match ranges.binary_search_by(|r| page_id.cmp(&r.page_end())) {
            Err(0) | Ok(0) => return 0,
            Err(i) | Ok(i) => i - 1, // may overlap with higher
        };

        while span != 0 && pos < ranges.len() {
            let r = &mut ranges[pos];
            let rem = r.page().max(page_id)..r.page_end().min(page_id + span);
            let span_to_remove = rem.len() as PageId;
            if span_to_remove == 0 {
                break;
            }
            debug_assert!(r.page_end() > page_id);
            self.hist.dec(r.zspan, 1);
            if r.span() == span_to_remove {
                if pos + 1 == ranges.len() {
                    // Pop avoids a call to memmove
                    ranges.pop();
                } else {
                    ranges.remove(pos);
                }
            } else {
                debug_assert!(span_to_remove < SHARD_MAX_LEN);
                if r.page() >= page_id {
                    r.page += span_to_remove as u16;
                    r.zspan -= span_to_remove as u16;
                    self.hist.inc(r.zspan, 1);
                } else {
                    let rem_span = r.page_end() - (page_id + span_to_remove);
                    r.zspan = (page_id - r.page() - 1) as u16;
                    self.hist.inc(r.zspan, 1);
                    if rem_span != 0 {
                        let rem_range = Range {
                            page: (page_id + span_to_remove) as u16,
                            zspan: (rem_span - 1) as u16,
                        };
                        self.hist.inc(rem_range.zspan, 1);
                        ranges.insert(pos, rem_range);
                    }
                }
            }

            span -= span_to_remove;
            page_id += span_to_remove;
            self.len -= span_to_remove;
            pos = pos.saturating_sub(1);
        }
        initial_len - self.len
    }

    pub fn allocate(&mut self, span: PageId) -> Option<PageId> {
        debug_assert!(span > 0);
        let zspan = (span - 1) as u16;
        let pos = if zspan == 0 {
            self.ranges.len().checked_sub(1)?
        } else if self.hist.any_gte(zspan) {
            Self::find_zspan_gte(&self.ranges, zspan)?
        } else {
            return None;
        };

        self.len -= span;
        let ranges = range_vec_make_mut(&mut self.ranges, 0);
        let r = ranges[pos];
        debug_assert!(r.zspan >= zspan, "{pos}");
        self.hist.dec(r.zspan, 1);
        if r.zspan == zspan {
            if pos + 1 == ranges.len() {
                // Pop avoids a call to memmove
                ranges.pop();
            } else {
                ranges.remove(pos);
            }
        } else {
            self.hist.inc(r.zspan - (zspan + 1), 1);
            ranges[pos] = Range {
                page: r.page + zspan + 1,
                zspan: r.zspan - (zspan + 1),
            };
        }
        Some(self.base + r.page())
    }

    #[cfg(not(feature = "nightly"))]
    fn find_zspan_gte(ranges: &[Range], zspan: u16) -> Option<usize> {
        const BEST_FIT_LOOK_AHEAD: usize = 8;
        let offset_from_end = ranges.iter().rev().position(|a| a.zspan >= zspan)?;
        ranges
            .iter()
            .rev()
            .skip(offset_from_end)
            .take(BEST_FIT_LOOK_AHEAD)
            .min_by_key(|a| a.zspan.wrapping_sub(zspan))
            .map(|p| (p as *const Range as usize - ranges.as_ptr() as usize) / size_of::<Range>())
    }

    #[cfg(feature = "nightly")]
    fn find_zspan_gte(ranges: &[Range], zspan: u16) -> Option<usize> {
        use std::simd::{cmp::*, num::*, *};
        type SimdTy = u16x16;
        let span_simd = SimdTy::splat(zspan);
        let chunks = ranges.rchunks_exact(SimdTy::LEN);
        let remainder = chunks.remainder();
        for ck in chunks {
            let mut chunk_simd =
                SimdTy::from_array(std::array::from_fn(|i| ck[ck.len() - 1 - i].zspan));
            if chunk_simd.simd_ge(span_simd).any() {
                chunk_simd -= span_simd;
                let min_simd = SimdTy::splat(chunk_simd.reduce_min());
                let eq_min_bitmask = chunk_simd.simd_eq(min_simd).to_bitmask();
                let ck_offset =
                    (ck.as_ptr() as usize - ranges.as_ptr() as usize) / size_of::<Range>();
                return Some(ck_offset + ck.len() - 1 - eq_min_bitmask.trailing_zeros() as usize);
            }
        }
        remainder
            .iter()
            .rev()
            .find(|a| a.zspan >= zspan)
            .map(|p| (p as *const Range as usize - ranges.as_ptr() as usize) / size_of::<Range>())
    }

    #[must_use]
    fn validate(&self) -> bool {
        if !self.base.is_multiple_of(SHARD_MAX_LEN) {
            return false;
        }
        for w in self.ranges.windows(2) {
            if w[0].page() <= w[1].page_end() {
                return false;
            }
        }
        let (hist, len) = Self::calc_histogram_and_len(&self.ranges);
        if self.hist != hist || self.len != len {
            return false;
        }
        true
    }

    fn calc_histogram_and_len(ranges: &[Range]) -> (Histogram, PageId) {
        let mut hist = Histogram::default();
        let mut len = 0;
        for r in ranges.iter() {
            hist.inc(r.zspan, 1);
            len += r.span();
        }
        (hist, len)
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), Error> {
        debug_assert_eq!(self.base, other.base);
        if other.is_empty() {
            return Ok(());
        }
        if self.is_empty() {
            *self = other.clone();
        } else if self.ranges.first().unwrap().page_end() <= other.ranges.last().unwrap().page() {
            let ranges = range_vec_make_mut(&mut self.ranges, other.ranges.len());
            ranges.insert_from_slice_copy(0, &other.ranges);
            self.hist.merge(&other.hist);
            self.len += other.len;
            Self::shift_lower(other.ranges.len(), ranges, &mut self.hist);
        } else if self.ranges.last().unwrap().page() >= other.ranges.first().unwrap().page_end() {
            let ranges = range_vec_make_mut(&mut self.ranges, other.ranges.len());
            ranges.extend_from_slice(&other.ranges);
            self.hist.merge(&other.hist);
            self.len += other.len;
            Self::shift_lower(ranges.len() - other.ranges.len(), ranges, &mut self.hist);
        } else if other.ranges.len() > self.ranges.len() {
            let prev_self = mem::replace(self, other.clone());
            return self.merge(&prev_self);
        } else {
            for r in other.ranges.iter() {
                self.free(self.base + r.page(), r.span())?;
            }
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        debug_assert_eq!(self.ranges.is_empty(), self.len == 0);
        debug_assert_eq!(self.ranges.is_empty(), self.hist == Default::default());
        self.len == 0
    }

    fn bulk_allocate(&mut self, mut bulk_span: PageId) -> Self {
        debug_assert!(bulk_span != 0);
        debug_assert!(bulk_span < self.len);
        let mut other = Self {
            base: self.base,
            len: 0,
            hist: Default::default(),
            ranges: Default::default(),
        };
        let ranges = range_vec_make_mut(&mut self.ranges, 0);
        let other_ranges = range_vec_make_mut(&mut other.ranges, 0);
        for (i, r) in ranges.iter_mut().rev().enumerate() {
            if r.span() <= bulk_span {
                self.len -= r.span();
                other.len += r.span();
                bulk_span -= r.span();
                self.hist.dec(r.zspan, 1);
                other.hist.inc(r.zspan, 1);
                if bulk_span == 0 {
                    other_ranges.extend_from_slice(&ranges[ranges.len() - 1 - i..]);
                    ranges.truncate(ranges.len() - i - 1);
                    break;
                }
            } else {
                self.len -= bulk_span;
                other.len += bulk_span;
                other.hist.inc((bulk_span - 1) as u16, 1);
                other_ranges.push(Range {
                    page: r.page,
                    zspan: (bulk_span - 1) as u16,
                });
                self.hist.dec(r.zspan, 1);
                r.page += bulk_span as u16;
                r.zspan -= bulk_span as u16;
                self.hist.inc(r.zspan, 1);
                other_ranges.extend_from_slice(&ranges[ranges.len() - i..]);
                ranges.truncate(ranges.len() - i);
                break;
            }
        }
        other
    }

    fn split_off(&mut self, right_gte: PageId) -> Self {
        let mut other = Self {
            base: self.base,
            len: 0,
            hist: Default::default(),
            ranges: Default::default(),
        };
        if self
            .ranges
            .first()
            .is_none_or(|r| self.base + r.page_end() <= right_gte)
        {
            return other;
        }
        let right_gte = right_gte - self.base;
        let ranges = range_vec_make_mut(&mut self.ranges, 0);
        let other_ranges = range_vec_make_mut(&mut other.ranges, 0);
        let mut full_moves = 0;
        for r in &mut ranges[..] {
            if r.page_end() <= right_gte {
                break;
            }
            let span_to_move = r.span().min(r.page_end() - right_gte);
            self.len -= span_to_move;
            other.len += span_to_move;
            let m = Range {
                page: (r.page_end() - span_to_move) as u16,
                zspan: (span_to_move - 1) as u16,
            };
            self.hist.dec(r.zspan, 1);
            other.hist.inc(m.zspan, 1);
            other_ranges.push(m);
            let left = r.span() - span_to_move;
            if left != 0 {
                r.zspan = (left - 1) as u16;
                self.hist.inc(r.zspan, 1);
                break;
            }
            full_moves += 1;
        }
        ranges.drain(0..full_moves);
        other
    }

    fn subtract(&mut self, other_shard: &Self) {
        if other_shard.ranges.len() < self.ranges.len() {
            for (page_id, span) in other_shard.iter_spans() {
                self.remove(page_id, span);
            }
            return;
        }
        self.len = 0;
        self.hist.0.fill(0);
        let ranges = range_vec_make_mut(&mut self.ranges, 0);
        let this_ranges = mem::take(ranges)
            .into_iter()
            .map(|r| r.page()..r.page_end());
        let mut other_ranges = other_shard
            .ranges
            .iter()
            .map(|r| r.page()..r.page_end())
            .peekable();
        let mut push_non_empty = |r: std::ops::Range<PageId>| {
            let range = Range {
                page: r.start as u16,
                zspan: (r.len() - 1) as u16,
            };
            self.hist.inc(range.zspan, 1);
            self.len += range.span();
            ranges.push(range);
        };
        for mut r in this_ranges {
            loop {
                if let Some(d) = other_ranges.peek() {
                    if d.start >= r.end {
                        other_ranges.next();
                        continue;
                    }
                    let intersect = r.start.max(d.start)..r.end.min(d.end);
                    let hi = intersect.end.max(r.start)..r.end;
                    r = r.start..intersect.start.min(r.end);
                    if !hi.is_empty() {
                        push_non_empty(hi);
                    }
                    if r.is_empty() {
                        break;
                    }
                } else {
                    push_non_empty(r);
                    break;
                }
            }
        }
    }
}

impl Freelist {
    pub fn deserialize(mut bytes: &[u8]) -> Result<(Self, usize), Error> {
        let original_len = bytes.len();
        let mut num_shards = 0u32;
        bytes.read_exact(num_shards.as_mut_bytes())?;
        let mut shards = ShardsVec::with_capacity(num_shards as usize);
        for _ in 0..num_shards {
            let mut base = 0u32;
            bytes.read_exact(base.as_mut_bytes())?;
            let mut num_ranges = 0u32;
            bytes.read_exact(num_ranges.as_mut_bytes())?;
            let ranges_num_bytes = mem::size_of::<Range>() * num_ranges as usize;
            let ranges_bytes = bytes
                .get(..ranges_num_bytes)
                .ok_or_else(|| io_invalid_data!("Not enough range bytes: {}", ranges_num_bytes))?;
            bytes = &bytes[ranges_num_bytes..];
            let mut ranges = smallvec::smallvec![Range::default(); num_ranges as usize];
            ranges.as_mut_bytes().copy_from_slice(ranges_bytes);
            let (hist, len) = Shard::calc_histogram_and_len(&ranges);
            shards.push(Shard {
                len,
                base,
                ranges: Arc::new(ranges),
                hist,
            })
        }
        let shards = if !shards.is_empty() {
            Some(Arc::new(shards))
        } else {
            None
        };
        let result = Self { shards };
        if result.validate() {
            Ok((result, original_len - bytes.len()))
        } else {
            Err(io_invalid_data!("Freelist validation failed"))
        }
    }

    #[cfg(any(test, fuzzing))]
    pub fn iter_pages(&self) -> impl DoubleEndedIterator<Item = PageId> + '_ {
        self.shards
            .iter()
            .flat_map(|s| s.iter())
            .flat_map(|s| s.iter_pages())
    }

    // NOTE: spans crossing shards may be returned as multiple spans even if they are contiguous
    pub fn iter_spans(&self) -> impl DoubleEndedIterator<Item = (PageId, PageId)> + '_ {
        self.shards
            .iter()
            .flat_map(|s| s.iter())
            .flat_map(|s| s.iter_spans())
    }

    pub fn serialized_size(&self) -> usize {
        mem::size_of::<u32>()
            + self.shards.as_ref().map_or(0, |shards| {
                (mem::size_of::<u32>() + mem::size_of::<u32>()) * shards.len()
                    + shards
                        .iter()
                        .map(|s| s.ranges.as_bytes().len())
                        .sum::<usize>()
            })
    }

    pub fn serialize_into(&self, mut w: impl Write) -> io::Result<()> {
        let Some(shards) = &self.shards else {
            return w.write_all(0u32.as_bytes());
        };
        w.write_all((shards.len() as u32).as_bytes())?;
        for shard in shards.iter() {
            w.write_all(shard.base.as_bytes())?;
            w.write_all((shard.ranges.len() as u32).as_bytes())?;
            w.write_all(shard.ranges.as_bytes())?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.shards.as_ref().is_none_or(|s| s.is_empty())
    }

    pub fn len(&self) -> PageId {
        self.shards
            .as_ref()
            .map_or(0, |s| s.iter().map(|s| s.len).sum())
    }

    pub fn num_spans(&self) -> PageId {
        self.shards
            .as_ref()
            .map_or(0, |s| s.iter().map(|s| s.ranges.len()).sum()) as PageId
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        if let Some(b) = self.shards.as_mut().and_then(Arc::get_mut) {
            b.clear();
        } else {
            self.shards = Default::default();
        }
    }

    pub fn last_piece(&self) -> Option<(PageId, PageId)> {
        self.shards.as_ref()?.last()?.last()
    }

    pub fn allocate_last_piece(&mut self) -> Option<(PageId, PageId)> {
        let shards = self.shards.as_mut().map(Arc::make_mut)?;
        if let Some(shard) = shards.last_mut() {
            let result = shard.pop_last();
            if shard.is_empty() {
                shards.pop();
            }
            result
        } else {
            None
        }
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), Error> {
        let Some(other_shards) = &other.shards else {
            return Ok(());
        };
        if self.is_empty() {
            *self = other.clone();
            return Ok(());
        }
        let shards = Arc::make_mut(self.shards.get_or_insert_with(Default::default));
        for other_shard in other_shards.iter() {
            match shards.binary_search_by_key(&other_shard.base, |s| s.base) {
                Ok(i) => shards[i].merge(other_shard)?,
                Err(i) => {
                    shards.insert(i, other_shard.clone());
                }
            }
        }
        #[cfg(any(test, fuzzing))]
        debug_assert!(self.validate());
        Ok(())
    }

    pub fn split(mut self, right_gte: PageId) -> (Self, Self) {
        let right = self.split_off(right_gte);
        (self, right)
    }

    pub fn split_off(&mut self, right_gte: PageId) -> Self {
        let Some(shards) = &mut self.shards else {
            return Self::default();
        };
        let from_idx = match shards.binary_search_by_key(&right_gte, |s| s.base) {
            Ok(i) | Err(i) => i,
        };
        let shards = Arc::make_mut(shards);
        let mut other_shards = shards.split_off(from_idx);
        if let Some(last) = shards.last_mut() {
            let other_shard = last.split_off(right_gte);
            if last.is_empty() {
                shards.pop();
            }
            if !other_shard.is_empty() {
                other_shards.push(other_shard);
            }
        }

        Self {
            shards: (!other_shards.is_empty())
                .then_some(other_shards)
                .map(Arc::new),
        }
    }

    pub fn bulk_allocate(&mut self, mut bulk_span: PageId, precise: bool) -> Self {
        if bulk_span == 0 || self.is_empty() {
            return Default::default();
        }
        let original_bulk_span = bulk_span;
        let mut other = Self::default();
        let shards = Arc::make_mut(self.shards.get_or_insert_with(Default::default));
        let other_shards = Arc::make_mut(other.shards.get_or_insert_with(Default::default));
        let mut i = 0;
        while i < shards.len() {
            let shard = &shards[i];
            debug_assert!(!shard.is_empty());
            if !precise || shard.len <= bulk_span {
                i += 1;
                bulk_span = bulk_span.saturating_sub(shard.len);
                if bulk_span == 0 {
                    *other_shards = shards.split_off(i);
                    mem::swap(shards, other_shards);
                    break;
                }
            } else {
                let shard = &mut shards[i];
                let other_shard = shard.bulk_allocate(bulk_span);
                debug_assert!(!shard.is_empty());
                debug_assert!(!other_shard.is_empty());
                *other_shards = shards.split_off(i);
                mem::swap(shards, other_shards);
                other_shards.push(other_shard);
                break;
            }
        }
        if other_shards.is_empty() {
            *other_shards = mem::take(shards);
        }
        if cfg!(any(test, fuzzing)) {
            debug_assert!(self.validate());
            debug_assert!(other.validate());
        }
        debug_assert!(!precise || other.len() <= original_bulk_span);
        other
    }

    fn get_or_create_shard(&mut self, base: PageId) -> &mut Shard {
        debug_assert_eq!(base % SHARD_MAX_LEN, 0);
        let shards = Arc::make_mut(self.shards.get_or_insert_with(Default::default));
        match shards.binary_search_by_key(&base, |s| s.base) {
            Ok(i) => &mut shards[i],
            Err(i) => {
                shards.insert(
                    i,
                    Shard {
                        len: 0,
                        base,
                        ranges: Default::default(),
                        hist: Default::default(),
                    },
                );
                &mut shards[i]
            }
        }
    }

    pub fn free(&mut self, mut page_id: PageId, mut span: PageId) -> Result<(), Error> {
        while span != 0 {
            let base = page_id / SHARD_MAX_LEN * SHARD_MAX_LEN;
            let base_end = base + SHARD_MAX_LEN;
            let span_for_shard = span.min(base_end - page_id);
            self.get_or_create_shard(base)
                .free(page_id, span_for_shard)?;
            span -= span_for_shard;
            page_id += span_for_shard;
        }
        #[cfg(any(test, fuzzing))]
        debug_assert!(self.validate());
        Ok(())
    }

    fn allocate_cross_shard(
        shards: &mut ShardsVec,
        span: PageId,
        shards_considered: std::ops::Range<usize>,
    ) -> Option<PageId> {
        let mut pos = 0..0;
        let min_wsize = span.div_ceil(SHARD_MAX_LEN).max(2) as usize;
        'outer: for start_idx in shards_considered {
            let shards = &shards[start_idx..];
            if shards.len() < min_wsize {
                return None;
            }
            let (head, rest) = shards.split_first().unwrap();
            let (span_start, mut span_len) = head.last().unwrap();
            let mut end_idx = start_idx + 1;
            for tail in rest {
                let (tail_start, tail_len) = tail.first().unwrap();
                if tail_start != span_start + span_len {
                    break;
                }
                end_idx += 1;
                span_len += tail_len;
                if span_len >= span {
                    pos = start_idx..end_idx;
                    break 'outer;
                }
            }
        }

        let [head, middle @ .., tail] = &mut shards[pos.clone()] else {
            return None;
        };
        let head_last = head.last().unwrap();
        head.remove(head_last.0, head_last.1);
        let remove_from_tail = span - head_last.1 - middle.len() as PageId * SHARD_MAX_LEN;
        tail.remove(tail.base, remove_from_tail);

        let drain_start = pos.start + !head.is_empty() as usize;
        let drain_end = pos.end - !tail.is_empty() as usize;
        shards.drain(drain_start..drain_end);
        Some(head_last.0)
    }

    pub fn allocate(&mut self, span: PageId) -> Option<PageId> {
        debug_assert_ne!(span, 0);
        if span == 0 || span > MAX_ALLOCATION_SPAN {
            return None;
        }
        #[cfg(debug_assertions)]
        let len_before = self.len();
        let shards = Arc::make_mut(self.shards.as_mut()?);
        let mut result = None;
        if span <= SHARD_MAX_LEN {
            let mut i = 0;
            while i < shards.len() {
                let shard = &mut shards[i];
                if let Some(p) = shard.allocate(span) {
                    if shard.is_empty() {
                        shards.remove(i);
                    }
                    #[cfg(debug_assertions)]
                    debug_assert_eq!(len_before - span, self.len());
                    result = Some(p);
                    break;
                }
                result = Self::allocate_cross_shard(shards, span, i..i + 1);
                if result.is_some() {
                    break;
                }
                i += 1;
            }
        } else {
            result = Self::allocate_cross_shard(shards, span, 0..shards.len());
        }
        #[cfg(debug_assertions)]
        debug_assert_eq!(len_before - result.map_or(0, |_| span), self.len());
        #[cfg(any(test, fuzzing))]
        debug_assert!(self.validate());
        result
    }

    pub fn remove(&mut self, mut page_id: PageId, mut span: PageId) -> PageId {
        #[cfg(debug_assertions)]
        let len_before = self.len();
        let Some(shards) = self.shards.as_mut() else {
            return 0;
        };
        let mut removed = 0;
        while span != 0 {
            let base = page_id / SHARD_MAX_LEN * SHARD_MAX_LEN;
            let base_end = base + SHARD_MAX_LEN;
            let span_for_shard = span.min(base_end - page_id);
            if let Ok(shard_pos) = shards.binary_search_by_key(&base, |s| s.base) {
                let shards = Arc::make_mut(shards);
                let shard = &mut shards[shard_pos];
                removed += shard.remove(page_id, span_for_shard);
                if shard.is_empty() {
                    shards.remove(shard_pos);
                }
            }
            span -= span_for_shard;
            page_id += span_for_shard;
        }
        #[cfg(debug_assertions)]
        debug_assert_eq!(len_before - self.len(), removed);
        removed
    }

    pub fn subtract(&mut self, other: &Self) {
        let Some((shards, other_shards)) = self.shards.as_mut().zip(other.shards.as_deref()) else {
            return;
        };
        for other_shard in other_shards {
            let Ok(shard_pos) = shards.binary_search_by_key(&other_shard.base, |s| s.base) else {
                continue;
            };
            let shards = Arc::make_mut(shards);
            let shard = &mut shards[shard_pos];
            shard.subtract(other_shard);
            if shard.is_empty() {
                shards.remove(shard_pos);
            }
        }
        #[cfg(any(test, fuzzing))]
        debug_assert!(self.validate());
    }

    fn validate(&self) -> bool {
        self.shards
            .as_ref()
            .is_none_or(|s| s.iter().all(|s| s.validate() && !s.is_empty()))
    }
}

/// Faster version of Arc::make_mut
/// It uses a faster cloning method for SmallVec (as T: Copy)
/// and it also takes into account extra reservations while doing so.
#[inline]
fn range_vec_make_mut(this: &mut Arc<RangeVec>, reserve: usize) -> &mut RangeVec {
    #[cold]
    fn cold_range_vec_make_mut(this: &mut Arc<RangeVec>, reserve: usize) -> &mut RangeVec {
        let cap = if this.len() + reserve <= RangeVec::inline_size() {
            RangeVec::inline_size()
        } else {
            (this.len() * 5 / 4).max(this.len() + reserve)
        };
        let new = Arc::new(RangeVec::with_capacity(cap));
        let new_mut = unsafe { &mut *Arc::as_ptr(&new).cast_mut() };
        new_mut.extend_from_slice(this);
        *this = new;
        new_mut
    }
    let this_ptr = this as *mut Arc<RangeVec>;
    if let Some(v) = Arc::get_mut(this) {
        v
    } else {
        cold_range_vec_make_mut(unsafe { &mut *this_ptr }, reserve)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hist_class() {
        let classes = (0u16..20)
            .chain([u16::MAX - 1, u16::MAX])
            .map(Histogram::class)
            .collect::<Vec<_>>();
        let expected = vec![
            0, 1, 2, 3, 4, 5, 6, 6, 7, 7, 8, 8, 8, 8, 9, 9, 9, 9, 9, 9, 21, 21,
        ];
        assert_eq!(classes, expected)
    }

    #[test]
    fn test_free_huge() {
        let mut a = Freelist::default();
        a.free(0, u16::MAX as PageId + 2).unwrap();
        assert_eq!(a.allocate(SHARD_MAX_LEN), Some(0));
        assert_eq!(a.allocate(1), Some(SHARD_MAX_LEN));
        a.free(1, SHARD_MAX_LEN).unwrap();
        assert_eq!(a.allocate(SHARD_MAX_LEN - 1), Some(1));
        assert_eq!(a.allocate(1), Some(SHARD_MAX_LEN));
        a.free(1, SHARD_MAX_LEN - 1).unwrap();
        assert_eq!(a.allocate(SHARD_MAX_LEN - 1), Some(1));
    }

    #[test]
    fn test_allocate_huge() {
        let mut a = Freelist::default();
        for p in [0, 70_000, 70_000 * 2] {
            a.free(p, 70_000).unwrap();
        }
        assert_eq!(a.allocate(65537), Some(0));
        assert_eq!(a.allocate(65537), Some(65537));
        assert_eq!(a.allocate(65537), Some(65537 * 2));
        assert_eq!(a.allocate(70_000 * 3 - 65537 * 3), Some(65537 * 3));
        assert_eq!(a.len(), 0);

        for p in [0, 70_000, 70_000 * 2, 70_000 * 3] {
            a.free(p, 70_000).unwrap();
        }
        assert_eq!(a.allocate(50000), Some(0));
        assert_eq!(a.allocate(70_000 * 3), Some(50_000));
        assert_eq!(a.allocate(20_000), Some(70_000 * 3 + 50_000));
        assert!(a.validate());
    }

    #[test]
    fn test_medium() {
        let mut a = Freelist::default();
        a.free(1, 256 + 2).unwrap();
        assert_eq!(a.allocate(256 + 2), Some(1));
        a.free(127, 256 + 256).unwrap();
        assert_eq!(a.allocate(256 + 256), Some(127));
        a.free(0, 255).unwrap();
        a.free(256, 256).unwrap();
        assert_eq!(a.allocate(256 + 256), None);
        a.free(512 + 1, 254).unwrap();
        a.free(512, 1).unwrap();
        a.free(512 + 255, 1).unwrap();
        assert_eq!(a.allocate(256 + 256), Some(256));
        assert_eq!(a.allocate(255), Some(0));

        for i in [0, 500] {
            a.free(i, 500).unwrap();
        }
        assert_eq!(a.allocate(257), Some(0));
        assert_eq!(a.allocate(257), Some(257));
        assert_eq!(a.allocate(257), Some(257 * 2));
        assert_eq!(a.allocate(1000 - 257 * 3), Some(257 * 3));
    }

    #[test]
    fn test_merge() {
        for size in [0, 1, 255, 256, 257, 500] {
            for start in [0, 1, 255, 256, 257] {
                for alloc in [0, 1, 255, 256, 257, 499, 500] {
                    let mut a = Freelist::default();
                    a.free(start, size).unwrap();
                    let b = a.bulk_allocate(alloc, true);
                    a.merge(&b).unwrap();
                    assert_eq!(a.len(), size);
                }
            }
        }
    }

    #[test]
    fn test_split() {
        let mut a = Freelist::default();
        a.free(0, 10).unwrap();
        let (b, c) = a.clone().split(0);
        assert_eq!((b.len(), c.len()), (0, 10));
        let (b, c) = a.clone().split(10);
        assert_eq!((b.len(), c.len()), (10, 0));
        let (b, c) = a.split(5);
        assert_eq!((b.len(), c.len()), (5, 5));
        let mut a = Freelist::default();
        a.free(SHARD_MAX_LEN - 10, 10).unwrap();
        a.free(SHARD_MAX_LEN, 10).unwrap();
        let (b, c) = a.clone().split(0);
        assert_eq!((b.len(), c.len()), (0, 20));
        let (b, c) = a.clone().split(10);
        assert_eq!((b.len(), c.len()), (0, 20));
        let (b, c) = a.clone().split(SHARD_MAX_LEN - 5);
        assert_eq!((b.len(), c.len()), (5, 15));
        let (b, c) = a.clone().split(SHARD_MAX_LEN);
        assert_eq!((b.len(), c.len()), (10, 10));
        let (b, c) = a.clone().split(SHARD_MAX_LEN + 5);
        assert_eq!((b.len(), c.len()), (15, 5));
        let (b, c) = a.clone().split(SHARD_MAX_LEN + 10);
        assert_eq!((b.len(), c.len()), (20, 0));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    prop_compose! {
        fn divisible_integer(max: u32, by: u32)(base in 0..max/by) -> u32 { base * by }
    }

    impl Freelist {
        fn from_set(set: &BTreeSet<u32>) -> Self {
            let mut a = Freelist::default();
            for &page in set {
                a.free(page, 1).unwrap();
            }
            a
        }

        fn to_set(&self) -> BTreeSet<u32> {
            BTreeSet::from_iter(self.iter_pages())
        }
    }

    proptest! {
        #[test]
        fn freelist_serde(set in prop::collection::btree_set(0u32..1_000_000, 0..5_000)) {
            let a = Freelist::from_set(&set);
            let mut serialized = Vec::new();
            a.serialize_into(&mut serialized).unwrap();
            let b = Freelist::deserialize(&serialized).unwrap().0;
            assert_eq!(b, a);
        }

        #[test]
        fn freelist_merge(mut a in prop::collection::btree_set(0u32..3_000, 0..2_000), mut b in prop::collection::btree_set(0u32..3_000, 0..2_000)) {
            a.retain(|i| !b.contains(i));
            let mut la = Freelist::from_set(&a);
            let lb = Freelist::from_set(&b);
            la.merge(&lb).unwrap();
            a.append(&mut b);
            assert_eq!(a, la.to_set());
        }

        #[test]
        fn freelist_bulk_allocate(pages in prop::collection::btree_set(divisible_integer(10_000, 100), 0..100), n in 0u32..3_000) {
            let mut set = pages.iter().flat_map(|&i| i..i+100).collect::<BTreeSet<_>>();
            let mut right = Freelist::default();
            for &page in &pages {
                right.free(page, 100).unwrap();
            }
            let left = right.bulk_allocate(n, true);

            let set_right = if let Some(&i) = set.iter().nth(n as usize) {
                set.split_off(&i)
            } else {
                Default::default()
            };
            assert_eq!(left.to_set(), set);
            assert_eq!(right.to_set(), set_right);
            assert!(left.validate());
            assert!(right.validate());
        }

        #[test]
        fn freelist_split(pages in prop::collection::btree_set(divisible_integer(10_000, 100), 0..1_000), split_at in 0u32..100_100) {
            let mut set = pages.iter().flat_map(|&i| i..i+100).collect::<BTreeSet<_>>();
            let mut left = Freelist::default();
            for &page in &pages {
                left.free(page, 100).unwrap();
            }
            let right = left.split_off(split_at);
            let set_right = set.split_off(&split_at);
            assert_eq!(left.to_set(), set);
            assert_eq!(right.to_set(), set_right);
            assert!(left.validate());
            assert!(right.validate());
        }

        #[test]
        fn freelist_free_1(pages in prop::collection::btree_set(0u32..1000, 0..1000).prop_map(|s| s.into_iter().collect::<Vec<_>>()).prop_shuffle()) {
            let set = pages.iter().cloned().collect::<BTreeSet<_>>();
            let mut a = Freelist::default();
            for &page in &pages {
                a.free(page, 1).unwrap();
            }
            assert_eq!(a.to_set(), set);
            assert!(a.validate());
            for &page in &pages {
                a.remove(page, 1);
            }
            assert!(a.is_empty());
        }

        #[test]
        fn freelist_free_3(pages in prop::collection::btree_set(divisible_integer(2000, 3), 0..1000).prop_map(|s| s.into_iter().collect::<Vec<_>>()).prop_shuffle()) {
            let set = pages.iter().flat_map(|&i| i..i+3).collect::<BTreeSet<_>>();
            let mut a = Freelist::default();
            for &page in &pages {
                a.free(page, 3).unwrap();
            }
            assert_eq!(a.to_set(), set);
            assert!(a.validate());
            for &page in &pages {
                a.remove(page, 3);
            }
            assert!(a.is_empty());
        }

        #[test]
        fn freelist_free_100(pages in prop::collection::btree_set(divisible_integer(1_000, 100), 0..100).prop_map(|s| s.into_iter().collect::<Vec<_>>()).prop_shuffle()) {
            let set = pages.iter().flat_map(|&i| i..i+100).collect::<BTreeSet<_>>();
            let mut a = Freelist::default();
            for &page in &pages {
                a.free(page, 100).unwrap();
            }
            assert_eq!(a.to_set(), set);
            assert!(a.validate());
            for &page in &pages {
                a.remove(page, 100);
            }
            assert!(a.is_empty());
        }

        #[test]
        fn freelist_free_300(pages in prop::collection::btree_set(divisible_integer(10_000, 300), 0..200).prop_map(|s| s.into_iter().collect::<Vec<_>>()).prop_shuffle()) {
            let set = pages.iter().flat_map(|&i| i..i+300).collect::<BTreeSet<_>>();
            let mut a = Freelist::default();
            for &page in &pages {
                a.free(page, 300).unwrap();
            }
            assert_eq!(a.to_set(), set);
            assert!(a.validate());
            for &page in &pages {
                a.remove(page, 300);
            }
            assert!(a.is_empty());
        }

        #[ignore]
        #[test]
        fn freelist_free_70_000(pages in prop::collection::btree_set(divisible_integer(1_000_000, 70_000), 0..10).prop_map(|s| s.into_iter().collect::<Vec<_>>()).prop_shuffle()) {
            let set = pages.iter().flat_map(|&i| i..i+70_000).collect::<BTreeSet<_>>();
            let mut a = Freelist::default();
            for &page in &pages {
                a.free(page, 70_000).unwrap();
            }
            assert_eq!(a.to_set(), set);
            assert!(a.validate());
            for &page in &pages {
                a.remove(page, 70_000);
            }
            assert!(a.is_empty());
        }

        #[test]
        fn freelist_free(set in prop::collection::btree_set(0u32..1000, 500..1_000)) {
            let mut a = Freelist::default();
            let mut last = None;
            let mut span = 0;
            for &page in &set {
                if let Some(l) = last {
                    if l + span == page {
                        span += 1;
                    } else {
                        a.free(l, span).unwrap();
                        last = Some(page);
                        span = 1;
                    }
                } else {
                    last = Some(page);
                    span = 1;
                }
            }
            if let Some(l) = last {
                a.free(l, span).unwrap();
            }
            assert_eq!(a.to_set(), set);
            assert!(a.validate());
        }

        #[test]
        fn subtract(
            mut this in prop::collection::btree_set(60_000u32..70_000, 0..10_000),
            that in prop::collection::btree_set(60_000u32..70_000, 0..10_000)
        ) {
            let mut a = Freelist::from_set(&this);
            let b = Freelist::from_set(&that);
            a.subtract(&b);
            assert!(a.validate());
            this.retain(|i| !that.contains(i));
            assert_eq!(a.to_set(), this);
        }

        #[test]
        fn subtract_merge(
            m in 1u32..5,
            this in  prop::collection::btree_set(divisible_integer(500_000, 60_000), 0..8),
            that in prop::collection::btree_set(divisible_integer(500_000, 60_000), 0..8)
        ) {
            let mut a = Freelist::default();
            for &i in &this {
                a.free(i * m, 60_000* m).unwrap();
            }
            let mut b = Freelist::default();
            for &i in &that {
                b.free(i * m, 60_000* m).unwrap();
            }
            a.subtract(&b);
            let a_ = a.clone();
            for &expected in this.difference(&that) {
                let allocation = a.allocate(60_000* m);
                assert_eq!(allocation, Some(expected * m));
            }
            assert!(a.is_empty(), "{a:?}");
            a = a_;
            a.merge(&b).unwrap();
            for &expected in this.union(&that) {
                let allocation = a.allocate(60_000* m);
                assert_eq!(allocation, Some(expected * m));
            }
            assert!(a.is_empty(), "{a:?}");
        }

        #[test]
        fn freelist_allocate_huge(
            _pages in prop::collection::btree_set(divisible_integer(5_000_000, 70_000), 1..100),
            allocations in prop::collection::vec(35_000u32..=200_000, 1..50)
        ) {
            let mut a = Freelist::default();
            let mut pages = BTreeSet::from_iter(_pages.iter().flat_map(|&p| p..p + 70_000));
            for page in _pages {
                a.free(page, 70_000).unwrap();
            }
            assert!(a.validate());
            for span in allocations {
                if let Some(start) = a.allocate(span) {
                    for page in start..start+span {
                        assert!(pages.remove(&page), "{page}");
                    }
                }
            }
            proptest::prop_assert_eq!(a.to_set(), pages);
            assert!(a.validate());
        }

        #[test]
        fn freelist_allocate_small(mut pages in prop::collection::btree_set(0u32..1000, 10..1000), allocations in prop::collection::vec(1u32..=10, 1..50)) {
            let mut a = Freelist::default();
            for &page in &pages {
                a.free(page, 1).unwrap();
            }
            assert!(a.validate());
            assert_eq!(a.to_set(), pages);
            for span in allocations {
                if let Some(start) = a.allocate(span) {
                    for page in start..start+span {
                        assert!(pages.remove(&page));
                    }
                } else if span == 1 {
                    assert_eq!(a.len(), 0);
                    assert_eq!(pages.len(), 0);
                    break;
                }
            }
            assert_eq!(a.to_set(), pages);
            assert!(a.validate());
        }
    }
}
