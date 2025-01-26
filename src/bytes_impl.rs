use std::ptr::{addr_of, addr_of_mut, NonNull};

use std::mem::{self, MaybeUninit};
use std::sync::atomic;

#[repr(C, align(8))]
pub(crate) struct ArcBytesHeader {
    count: atomic::AtomicU32,
    pub(crate) len: u32,
    data: [MaybeUninit<u8>; 0],
    // actual data follows
}

/// A dynamically sized, reference-counted byte buffer, that guarantees that the data is allocated according to ArcBytesHeader.
/// This is the unique and uinitialized version of ArcBytes.
/// This is a simplified version of `triomphe::ThinArc<(), u8>` that we have control over the memory layout.
pub(crate) struct UniqueArcBytes(NonNull<ArcBytesHeader>);

unsafe impl Send for UniqueArcBytes {}

unsafe impl Sync for UniqueArcBytes {}

impl Clone for UniqueArcBytes {
    fn clone(&self) -> Self {
        let mut new = Self::new(self.header().len as usize);
        new.data_mut().copy_from_slice(self.data());
        new
    }
}

impl UniqueArcBytes {
    #[inline(never)]
    unsafe fn drop_slow(&mut self) {
        use std::alloc::Layout;
        let layout = Layout::new::<ArcBytesHeader>()
            .extend(Layout::array::<u8>(self.header().len as usize).unwrap_unchecked())
            .unwrap_unchecked()
            .0
            .pad_to_align();
        std::alloc::dealloc(self.0.as_ptr().cast(), layout);
    }

    #[inline]
    pub(crate) fn new(len: usize) -> Self {
        assert!(len <= u32::MAX as usize);
        use std::alloc::Layout;
        let layout = Layout::new::<ArcBytesHeader>()
            .extend(Layout::array::<u8>(len).unwrap())
            .unwrap()
            .0
            .pad_to_align();
        unsafe {
            let result: NonNull<ArcBytesHeader> =
                NonNull::new(std::alloc::alloc(layout).cast()).expect("Out of memory");
            result.write(ArcBytesHeader {
                count: 1.into(),
                len: len as u32,
                data: [],
            });
            Self(result)
        }
    }

    #[inline]
    pub(crate) fn data_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                addr_of_mut!((*self.0.as_ptr()).data).cast(),
                self.header().len as usize,
            )
        }
    }

    #[inline]
    pub(crate) fn data(&self) -> &[MaybeUninit<u8>] {
        unsafe {
            std::slice::from_raw_parts(self.header().data.as_ptr(), self.header().len as usize)
        }
    }

    #[inline]
    fn header(&self) -> &ArcBytesHeader {
        unsafe { self.0.as_ref() }
    }

    #[inline]
    pub(crate) unsafe fn assume_init(self) -> ArcBytes {
        let ptr = self.0;
        mem::forget(self);
        ArcBytes(ptr)
    }
}

impl Drop for UniqueArcBytes {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.drop_slow() };
    }
}

/// A dynamically sized, reference-counted byte buffer, that guarantees that the data is allocated according to ArcBytesHeader.
/// The initialized and cloneable version of UniqueArcBytes.
pub(crate) struct ArcBytes(NonNull<ArcBytesHeader>);

unsafe impl Send for ArcBytes {}

unsafe impl Sync for ArcBytes {}

impl Clone for ArcBytes {
    #[inline]
    fn clone(&self) -> Self {
        // Using a relaxed ordering is alright here, as knowledge of the
        // original reference prevents other threads from erroneously deleting
        // the object.
        //
        // As explained in the [Boost documentation][1], Increasing the
        // reference counter can always be done with memory_order_relaxed: New
        // references to an object can only be formed from an existing
        // reference, and passing an existing reference from one thread to
        // another must already provide any required synchronization.
        //
        // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
        let old_size = self.header().count.fetch_add(1, atomic::Ordering::Relaxed);

        // However we need to guard against massive refcounts in case someone
        // is `mem::forget`ing Arcs. If we don't do this the count can overflow
        // and users will use-after free. We racily saturate to `isize::MAX` on
        // the assumption that there aren't ~2 billion threads incrementing
        // the reference count at once. This branch will never be taken in
        // any realistic program.
        //
        // We abort because such a program is incredibly degenerate, and we
        // don't care to support it.
        const MAX_REFCOUNT: u32 = u32::MAX / 2;
        if old_size > MAX_REFCOUNT {
            std::process::abort();
        }

        Self(self.0)
    }
}

impl Drop for ArcBytes {
    #[inline]
    fn drop(&mut self) {
        // Because `fetch_sub` is already atomic, we do not need to synchronize
        // with other threads unless we are going to delete the object.
        if self.header().count.fetch_sub(1, atomic::Ordering::Release) != 1 {
            return;
        }

        // FIXME(bholley): Use the updated comment when [2] is merged.
        //
        // This load is needed to prevent reordering of use of the data and
        // deletion of the data.  Because it is marked `Release`, the decreasing
        // of the reference count synchronizes with this `Acquire` load. This
        // means that use of the data happens before decreasing the reference
        // count, which happens before this load, which happens before the
        // deletion of the data.
        //
        // As explained in the [Boost documentation][1],
        //
        // > It is important to enforce any possible access to the object in one
        // > thread (through an existing reference) to *happen before* deleting
        // > the object in a different thread. This is achieved by a "release"
        // > operation after dropping a reference (any access to the object
        // > through this reference must obviously happened before), and an
        // > "acquire" operation before deleting the object.
        //
        // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
        // [2]: https://github.com/rust-lang/rust/pull/41714
        self.header().count.load(atomic::Ordering::Acquire);

        UniqueArcBytes(self.0);
    }
}

impl ArcBytes {
    #[inline]
    fn header(&self) -> &ArcBytesHeader {
        unsafe { self.0.as_ref() }
    }

    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                addr_of!((*self.0.as_ptr()).data).cast(),
                self.header().len as usize,
            )
        }
    }

    /// Whether or not the `Arc` is uniquely owned (is the refcount 1?).
    #[inline]
    pub(crate) fn is_unique(&mut self) -> bool {
        // See the extensive discussion in [1] for why this needs to be Acquire.
        //
        // [1] https://github.com/servo/servo/issues/21186
        self.header().count.load(atomic::Ordering::Acquire) == 1
    }

    #[inline]
    pub(crate) fn header_ptr(&self) -> NonNull<ArcBytesHeader> {
        self.0
    }
}
