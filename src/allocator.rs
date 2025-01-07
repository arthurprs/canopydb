use std::{
    cmp::Ordering,
    io::{self, Read},
    mem::{self, size_of},
};

use crate::{
    error::{io_invalid_data, Error},
    freelist::Freelist,
    repr::{PageId, FIRST_COMPRESSED_PAGE},
    shim::parking_lot::Mutex,
    DatabaseInner, DeferredFreelist, FreePage, TxId,
};

use triomphe::Arc;
use zerocopy::AsBytes;

#[derive(Debug, Default)]
pub(crate) struct MainAllocator {
    /// Freelists pending a multi-writer tx to clear
    pub pending_free: Vec<(TxId, Freelist, Freelist)>,
    /// Free Page Ids
    pub free: DeferredFreelist,
    /// Free Indirection Page Ids
    pub indirection_free: DeferredFreelist,
    /// Page Ids freed from the current snapshot (metapage.snapshot_tx_id)
    /// Will be merged w/ free once the is no longer required
    pub snapshot_free: DeferredFreelist,
    /// Same as above, but for the ongoing checkpoint (state.ongoing_snapshot_tx_id)
    pub next_snapshot_free: DeferredFreelist,
    /// Allocation horizons
    pub next_page_id: PageId,
    pub next_indirection_id: PageId,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Allocator {
    main: Option<Arc<Mutex<MainAllocator>>>,
    pub is_checkpointer: bool,
    pub is_compactor: bool,
    /// Tracks all allocations from main. Contains both normal and indirect pages
    pub all_allocations: Freelist,
    /// Free Page Ids
    pub free: Freelist,
    /// Free Indirection Page Ids
    pub indirection_free: Freelist,
    /// Page Ids freed from the current snapshot (metapage.snapshot_tx_id)
    /// Will be merged w/ free once the is no longer required
    pub snapshot_free: Freelist,
    /// Same as above, but for the ongoing checkpoint (state.ongoing_snapshot_tx_id)
    pub next_snapshot_free: Freelist,
    /// Eligible to be freed from the buffer when this transaction is no longer accessible
    pub buffer_free: Vec<FreePage>,
    /// Number of times this allocator reserved more space in bulk from the main allocator
    main_bulk_reservations: u64,
    /// Allocation horizons
    pub main_next_page_id: PageId,
    pub main_next_indirection_id: PageId,
    /// Checkpointer only fields
    /// Pages reserved by another allocator, will count as freepages for this snapshot
    externally_allocated: Freelist,
}

impl MainAllocator {
    pub fn truncate_end(&mut self) -> Result<(), Error> {
        let mut returned = 0;
        let free = self.free.merged()?;
        while let Some((page, span)) = free.last_piece() {
            if page + span == self.next_page_id {
                assert_eq!(free.allocate_last_piece(), Some((page, span)));
                self.next_page_id -= span;
                returned += span;
            } else {
                break;
            }
        }
        let mut returned_ind = 0;
        let indirection_free = self.indirection_free.merged()?;
        while let Some((page, span)) = indirection_free.last_piece() {
            if page + span == self.next_indirection_id {
                let last_piece = indirection_free.allocate_last_piece();
                debug_assert_eq!(last_piece, Some((page, span)));
                self.next_indirection_id -= span;
                returned_ind += span;
            } else {
                break;
            }
        }
        if returned != 0 || returned_ind != 0 {
            trace!("Returned {returned} to next_id {returned_ind} pages to next_indirection_id");
        }
        Ok(())
    }

    pub fn from_bytes(mut data: &[u8]) -> Result<Self, Error> {
        let mut result = Self::default();
        data.read_exact(result.next_page_id.as_bytes_mut())?;
        data.read_exact(result.next_indirection_id.as_bytes_mut())?;
        let mut num_parts = 0u8;
        data.read_exact(num_parts.as_bytes_mut())?;
        if num_parts != 4 {
            return Err(io_invalid_data!("Expected 4 freelists"));
        }

        for _ in 0..num_parts {
            let (fl, read_len) = Freelist::deserialize(data)?;
            data = &data[read_len..];
            let (left, right) = fl.split(FIRST_COMPRESSED_PAGE);
            result.free.merged()?.merge(&left)?;
            result.indirection_free.merged()?.merge(&right)?;
        }

        Ok(result)
    }

    pub fn freelist_write_size(&self) -> usize {
        size_of::<PageId>() // next_page_id
        + size_of::<PageId>() // next_indirection_id
        + size_of::<u8>() // num freelists
        + self.free.max_serialized_size()
        + self.indirection_free.max_serialized_size()
        + self.snapshot_free.max_serialized_size()
        + self.next_snapshot_free.max_serialized_size()
    }
}

impl Allocator {
    const INITIAL_ALLOC_MIN: PageId = 10;
    const INITIAL_ALLOC: PageId = 100;
    const ALLOCATION_BATCH: PageId = 100;

    pub fn new_transaction(inner: &DatabaseInner, exclusive: bool) -> Result<Allocator, Error> {
        let mut result = Self {
            is_checkpointer: false,
            main: Some(inner.allocator.clone()),
            ..Default::default()
        };
        let mut main = inner.allocator.lock();
        result.main_next_page_id = main.next_page_id;
        result.main_next_indirection_id = main.next_indirection_id;
        if exclusive {
            mem::swap(&mut result.free, main.free.merged()?);
        } else {
            result.free =
                main.free
                    .bulk_allocate(Self::INITIAL_ALLOC_MIN, Self::INITIAL_ALLOC, true)?
        }
        drop(main);
        result.all_allocations.merge(&result.free)?;
        Ok(result)
    }

    pub fn new_checkpoint(inner: &DatabaseInner) -> Result<Allocator, Error> {
        let mut result = Self {
            is_checkpointer: true,
            main: Some(inner.allocator.clone()),
            ..Default::default()
        };
        let mut main = inner.allocator.lock();
        result.main_next_page_id = main.next_page_id;
        result.main_next_indirection_id = main.next_indirection_id;
        result.indirection_free = main.indirection_free.merged()?.clone();
        // Whatever is left in free goes into externally_allocated and we won't be able to use
        // it anymore. If reserve_from_global_state is required later it'll have to remove
        // from externally_allocated to make sure pages aren't duplicated.
        result.externally_allocated.merge(main.free.merged()?)?;
        result
            .externally_allocated
            .merge(main.snapshot_free.merged()?)?;
        // next_snapshot_free can only grow while a checkpointer is running
        assert!(main.next_snapshot_free.is_empty());
        let old_snapshots = inner.old_snapshots.lock();
        for l in old_snapshots.values() {
            result.externally_allocated.merge(l)?;
        }
        // result.snapshot_free and next_snapshot_free will capture _additional_ freed pages,
        // which later will be merged with main.snapshot_free
        Ok(result)
    }

    pub fn reserve_from_main(&mut self, mut span: PageId, bulk: bool) -> Result<(), Error> {
        trace!("reserve_from_main span {span} bulk {bulk:?}");
        self.main_bulk_reservations += bulk as u64;
        if bulk {
            span = span.max(
                PageId::try_from(Self::ALLOCATION_BATCH as u64 * self.main_bulk_reservations)
                    .unwrap_or(PageId::MAX),
            );
        }
        let mut main = self.main.as_ref().unwrap().lock();
        if self.is_checkpointer {
            match self.main_next_page_id.cmp(&main.next_page_id) {
                Ordering::Equal => (),
                Ordering::Less => {
                    self.externally_allocated.free(
                        self.main_next_page_id,
                        main.next_page_id - self.main_next_page_id,
                    )?;
                }
                Ordering::Greater => {
                    self.externally_allocated.remove(
                        main.next_page_id,
                        self.main_next_page_id - main.next_page_id,
                    );
                }
            }
        }

        if bulk {
            let reserved = main.free.bulk_allocate(span, span, true)?;
            self.all_allocations.merge(&reserved)?;
            self.free.merge(&reserved)?;
            if self.is_checkpointer {
                self.externally_allocated.subtract(&reserved);
            }
            span = span.saturating_sub(reserved.len());
        } else if let Some(allocation) = main.free.merged()?.allocate(span) {
            self.all_allocations.free(allocation, span)?;
            self.free.free(allocation, span)?;
            if self.is_checkpointer {
                self.externally_allocated.remove(allocation, span);
            }
            span = 0;
        }
        if span != 0 {
            // The compactor can't increase the size of the file
            if self.is_compactor {
                return Err(Error::CantCompact);
            }

            let left = FIRST_COMPRESSED_PAGE - main.next_page_id;
            if left < span {
                if !bulk {
                    return Err(Error::io_other("No free pages left"));
                }
                span = left;
            }
            if self.is_checkpointer {
                self.externally_allocated.remove(main.next_page_id, span);
            }
            self.all_allocations.free(main.next_page_id, span)?;
            self.free.free(main.next_page_id, span)?;
            main.next_page_id += span;
        }

        self.main_next_page_id = main.next_page_id;
        Ok(())
    }

    pub fn allocate(&mut self, span: PageId) -> Result<PageId, Error> {
        let mut bulk = None;
        loop {
            if let Some(id) = self.free.allocate(span) {
                return Ok(id);
            }
            // Try bulk allocation if this is the first reservation attempt and either: the desired allocation
            // is larger than a tenth of a batch OR the number of spans in the freelist is smaller than a batch.
            // Bulk allocations get larger over time in order to effective amortize allocations in large transactions.
            let bulk = bulk.get_or_insert_with(|| {
                span > Self::ALLOCATION_BATCH as PageId / 10
                    || self.free.len() < Self::ALLOCATION_BATCH as PageId
            });
            self.reserve_from_main(span, *bulk)?;
            *bulk = false;
        }
    }

    #[cold]
    fn reserve_indirections_from_main(&mut self) -> Result<(), Error> {
        let mut main = self.main.as_ref().unwrap().lock();
        if !main.indirection_free.is_empty() {
            // TODO: fix for multi-writers
            self.all_allocations
                .merge(main.indirection_free.merged()?)?;
            self.indirection_free = mem::take(main.indirection_free.merged()?);
        } else {
            const MIN_ALLOCATION: PageId = 100;
            let left = PageId::MAX - main.next_indirection_id;
            if left == 0 {
                return Err(Error::io_other("No free indirect pages left"));
            }
            let from_horizon = MIN_ALLOCATION.min(left);
            self.all_allocations
                .free(main.next_indirection_id, from_horizon)?;
            self.indirection_free
                .free(main.next_indirection_id, from_horizon)?;
            main.next_indirection_id += from_horizon;
        }
        Ok(())
    }

    pub fn allocate_indirection(&mut self) -> Result<PageId, Error> {
        assert!(!self.is_checkpointer);
        if let Some(id) = self.indirection_free.allocate(1) {
            return Ok(id);
        }
        self.reserve_indirections_from_main()?;
        Ok(self.indirection_free.allocate(1).unwrap())
    }

    pub fn allocate_spans(&mut self, span: PageId) -> Result<Freelist, Error> {
        assert!(self.is_checkpointer);
        if self.free.len() < span {
            self.reserve_from_main(span - self.free.len(), true)?;
        }
        let cut_off = self.free.bulk_allocate(span, true);
        assert!(cut_off.len() >= span);
        Ok(cut_off)
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        let mut main = self.main.as_ref().unwrap().lock();
        main.free.append(mem::take(&mut self.free));
        main.snapshot_free
            .append(mem::take(&mut self.snapshot_free));
        main.next_snapshot_free
            .append(mem::take(&mut self.next_snapshot_free));
        if !self.is_checkpointer {
            main.indirection_free
                .append(mem::take(&mut self.indirection_free));
        } else {
            // force free to be merged, as we could be returning a large number of pages
            main.free.merged()?;
            // indirection_free from the ckp allocator is a copy
            // of main allocator indirection_free when it was created.
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), Error> {
        let mut main = self.main.as_ref().unwrap().lock();
        let indirect_allocations = self.all_allocations.split_off(FIRST_COMPRESSED_PAGE);
        main.free.append(mem::take(&mut self.all_allocations));
        main.indirection_free.append(indirect_allocations);
        Ok(())
    }

    pub fn write_size(&self) -> usize {
        size_of::<PageId>() // next_page_id
        + size_of::<PageId>() // next_indirection_id
        + size_of::<u8>() // num freelists
        + self.free.serialized_size()
        + self.indirection_free.serialized_size()
        + self.snapshot_free.serialized_size()
        + self.externally_allocated.serialized_size()
    }

    pub fn write(&self, mut w: impl io::Write) -> io::Result<()> {
        assert!(self.is_checkpointer);
        w.write_all(self.main_next_page_id.as_bytes())?;
        w.write_all(self.main_next_indirection_id.as_bytes())?;

        let freelists = [
            &self.free,
            &self.indirection_free,
            &self.snapshot_free,
            &self.externally_allocated,
        ];
        #[cfg(debug_assertions)]
        {
            let mut merged = Freelist::default();
            for fl in freelists {
                merged.merge(fl).unwrap();
            }
        }
        w.write_all((freelists.len() as u8).as_bytes())?;
        for fl in freelists {
            fl.serialize_into(&mut w)?;
        }
        Ok(())
    }

    pub fn all_freespace_span(&self) -> PageId {
        self.free.len()
            + self.snapshot_free.len()
            + self.next_snapshot_free.len()
            + self.externally_allocated.len()
    }
}
