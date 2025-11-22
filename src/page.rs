use std::mem::size_of;

use crate::{
    error::io_invalid_input,
    repr::{header_cast, header_cast_mut, HeaderProvider, PageHeader, PageId},
    Bytes, Error, ReservedPageHeader, PAGE_SIZE,
};

#[derive(Debug)]
pub struct Page<const CLONE: bool = true> {
    pub dirty: bool,
    // TODO: we could use NonZeroPageId to reduce size
    // but due to alignment it saves no space...
    pub compressed_page: Option<(PageId, u32)>,
    // The main Page invariant is that it's backing data are well aligned bytes multiple of PAGE_SIZE
    // But this raw_data starts at the Pageheader (after the ReservedPageHeader/ArcBytesHeader)
    #[debug(skip)]
    pub raw_data: Bytes,
}

impl Clone for Page<true> {
    fn clone(&self) -> Self {
        Self {
            dirty: self.dirty,
            compressed_page: self.compressed_page,
            raw_data: self.raw_data.clone(),
        }
    }
}

impl<const CLONE: bool> Page<CLONE> {
    pub fn from_bytes(raw_data: Bytes) -> Result<Self, Error> {
        let page = Self {
            compressed_page: None,
            dirty: false,
            raw_data,
        };
        let total_page_size = size_of::<ReservedPageHeader>() + page.raw_data.len();
        if !(page.raw_data.as_ptr() as usize).is_multiple_of(8) {
            Err(io_invalid_input!("Bad page alignment"))
        } else if !total_page_size.is_multiple_of(PAGE_SIZE as usize)
            || page.span() != ((total_page_size / PAGE_SIZE as usize) as PageId)
        {
            Err(io_invalid_input!("Bad page len"))
        } else {
            Ok(page)
        }
    }

    pub fn new(id: PageId, dirty: bool, span: PageId) -> Self {
        assert!(span > 0);

        // Using an uninit bytes is memory safe but then Page would create slices over uninitialized bytes, which is
        // referred to as undefined behavior (UB). In practice this isn't materially different from slicing into a mmap file
        // and this was present in the std in the past, so it's unclear whether the UB label actually holds water in practice
        // (https://github.com/rust-lang/rust/blob/3802025f400af7817ba4874587e6a2df95abd65d/library/std/src/io/mod.rs#L377-L385).
        // Regardless, if uninit page bytes ever get exposed via the public API then it stands to reason that
        // newly allocated pages should be zeroed.
        let mut page = Self {
            compressed_page: None,
            dirty,
            raw_data: Bytes::new_zeroed(
                span as usize * PAGE_SIZE as usize - size_of::<ReservedPageHeader>(),
            ),
        };
        *page.header_mut() = PageHeader {
            id,
            span: span.try_into().unwrap(),
            ..Default::default()
        };
        page
    }

    fn calc_checksum(&self) -> u32 {
        let checksum = xxhash_rust::xxh3::xxh3_64(&self.raw_data[size_of::<u32>()..]) as u32;
        if checksum == u32::default() {
            !u32::default()
        } else {
            checksum
        }
    }

    pub fn check_checksum(&self) -> Option<bool> {
        if self.header().checksum == u32::default() {
            None
        } else {
            Some(self.header().checksum == self.calc_checksum())
        }
    }

    pub fn set_checksum(&mut self, real: bool) {
        self.header_mut().checksum = if real {
            self.calc_checksum()
        } else {
            u32::default()
        }
    }

    pub unsafe fn set_checksum_non_mut(&self, real: bool) {
        // TODO: make checksum an AtomicU32 so this is "safe"
        let checksum_ptr = &self.header().checksum as *const u32 as *mut u32;
        checksum_ptr.write(if real {
            self.calc_checksum()
        } else {
            u32::default()
        })
    }

    #[inline]
    pub fn header(&self) -> &PageHeader {
        header_cast(self)
    }

    #[inline]
    pub fn header_mut(&mut self) -> &mut PageHeader {
        header_cast_mut(self)
    }

    #[inline]
    pub fn span(&self) -> PageId {
        let span = ((size_of::<ReservedPageHeader>() + self.raw_data.len()) / PAGE_SIZE as usize)
            as PageId;
        debug_assert_eq!(span, PageId::from(self.header().span));
        span
    }

    #[inline]
    pub fn id(&self) -> PageId {
        self.header().id
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        self.raw_data.as_ref()
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8] {
        // TODO: assert!(!CLONE);
        self.raw_data.as_mut()
    }

    #[inline]
    pub fn usable_data(&self) -> &[u8] {
        self.split_off::<PageHeader>()
    }

    #[inline]
    pub fn usable_data_mut(&mut self) -> &mut [u8] {
        self.split_off_mut::<PageHeader>()
    }
}
