use std::{
    mem::{self, ManuallyDrop, MaybeUninit},
    ptr::NonNull,
};

use sptr::Strict;
use triomphe::{Arc, ThinArc, UniqueArc};
use zerocopy::ByteSlice;

pub(crate) struct UninitBytes(
    UniqueArc<triomphe::HeaderSlice<triomphe::HeaderWithLength<()>, [MaybeUninit<u8>]>>,
);

impl UninitBytes {
    pub fn new(len: usize) -> Self {
        Self(UniqueArc::from_header_and_uninit_slice(
            triomphe::HeaderWithLength::new((), len),
            len,
        ))
    }

    pub unsafe fn assume_init(mut self) -> Bytes {
        let slice = self.as_slice_mut();
        Bytes {
            ptr: NonNull::new_unchecked(slice.as_mut_ptr().cast()),
            len: slice.len(),
            backing: BackingPtr::new(Backing::Shared(Arc::into_thin(
                self.0.assume_init_slice_with_header().shareable(),
            ))),
        }
    }

    pub fn as_slice_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        &mut self.0.slice
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
#[debug("SharedBytes({})", self.as_slice().len())]
pub(crate) struct SharedBytes(ThinArc<(), u8>);

impl SharedBytes {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0.slice
    }
}

/// Database Bytes
///
/// Roughly equivalent to an `Arc<Vec<u8>>`.
///
/// Implements `PartialEq`, `Eq`, `PartialOrd`, `Ord`, `Hash`, `Borrow`, `AsRef`, `Deref` as a `&[u8]`
#[derive(Debug)]
pub struct Bytes {
    ptr: NonNull<u8>,
    len: usize,
    backing: BackingPtr,
}

unsafe impl Send for Bytes {}
unsafe impl Sync for Bytes {}

impl Bytes {
    pub(crate) fn new_zeroed(len: usize) -> Self {
        unsafe {
            let mut bytes = UninitBytes::new(len);
            bytes.as_slice_mut().fill(MaybeUninit::new(0));
            bytes.assume_init()
        }
    }

    #[inline]
    pub(crate) fn from_slices(vs: &[&[u8]]) -> Self {
        unsafe {
            let mut bytes = UninitBytes::new(vs.iter().map(|v| v.len()).sum());
            let mut slice = bytes.as_slice_mut();
            for v in vs {
                let u;
                (u, slice) = slice.split_at_mut_unchecked(v.len());
                std::ptr::copy_nonoverlapping(v.as_ptr(), u.as_mut_ptr().cast(), v.len());
            }
            bytes.assume_init()
        }
    }

    #[inline]
    pub(crate) fn from_slice(v: &[u8]) -> Self {
        unsafe {
            let mut bytes = UninitBytes::new(v.len());
            let slice = bytes.as_slice_mut();
            std::ptr::copy_nonoverlapping(v.as_ptr(), slice.as_mut_ptr().cast(), v.len());
            bytes.assume_init()
        }
    }

    #[inline]
    pub(crate) fn from_arc_bytes(v: SharedBytes) -> Self {
        Self {
            ptr: unsafe { NonNull::new_unchecked(v.0.slice.as_ptr().cast_mut()) },
            len: v.0.header.length,
            backing: BackingPtr::new(Backing::Shared(v.0)),
        }
    }

    #[inline]
    pub(crate) fn from_mmap(m: Arc<memmap2::Mmap>, from: usize, to: usize) -> Self {
        let slice = &m[from..to];
        Self {
            ptr: unsafe { NonNull::new_unchecked(slice.as_ptr().cast_mut()) },
            len: slice.len(),
            backing: BackingPtr::new(Backing::Mmap(m)),
        }
    }

    #[inline]
    pub(crate) fn restrict(&self, reference: &[u8]) -> Self {
        let self_range = self.as_ref().as_ptr_range();
        let range = reference.as_ptr_range();
        if range.start < self_range.start || range.end > self_range.end {
            panic!("Invalid reference");
        }
        Self {
            ptr: unsafe { NonNull::new_unchecked(reference.as_ptr().cast_mut()) },
            len: reference.len(),
            backing: self.backing.internal_clone(),
        }
    }

    #[inline]
    pub(crate) fn as_mut(&mut self) -> &mut [u8] {
        self.make_unique();
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub(crate) unsafe fn as_mut_unchecked(&mut self) -> &mut [u8] {
        debug_assert!(self.is_unique());
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub(crate) fn make_unique(&mut self) {
        if !self.is_unique() {
            #[cold]
            fn cold_make_unique(this: &mut Bytes) {
                *this = Bytes::from_slice(this.as_ref())
            }
            cold_make_unique(self);
        }
    }

    #[inline]
    pub(crate) fn is_unique(&mut self) -> bool {
        self.backing.is_unique()
    }

    #[inline]
    fn into_inner(mut self) -> Backing {
        unsafe {
            let inner = self.backing.take();
            mem::forget(self);
            inner
        }
    }

    #[inline]
    pub(crate) fn try_into_shared_bytes(self) -> Result<SharedBytes, ()> {
        match self.into_inner() {
            Backing::Shared(bytes) => Ok(SharedBytes(bytes)),
            Backing::Mmap(_) => Err(()),
        }
    }

    #[inline]
    pub(crate) fn truncate(&mut self, len: usize) {
        assert!(len <= self.len);
        self.len = len;
    }
}

impl Drop for Bytes {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.backing.take() };
    }
}

impl Clone for Bytes {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            len: self.len,
            backing: self.backing.internal_clone(),
        }
    }
}

impl AsRef<[u8]> for Bytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::borrow::Borrow<[u8]> for Bytes {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<T: AsRef<[u8]>> PartialEq<T> for Bytes {
    #[inline]
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl<T: AsRef<[u8]>> PartialOrd<T> for Bytes {
    #[inline]
    fn partial_cmp(&self, other: &T) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl std::cmp::Ord for Bytes {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl std::cmp::Eq for Bytes {}

impl std::hash::Hash for Bytes {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

/// Like triomphe::ArcUnion but more specific for this use case
#[repr(transparent)]
struct BackingPtr(NonNull<()>);

impl BackingPtr {
    #[inline]
    fn untagged(&self) -> NonNull<()> {
        unsafe { NonNull::new_unchecked(Strict::map_addr(self.0.as_ptr(), |p| p & !0b11)) }
    }

    #[inline]
    fn tag(&self) -> usize {
        Strict::addr(self.0.as_ptr()) & 0b11
    }

    #[inline]
    fn new(backing: Backing) -> Self {
        unsafe {
            let tagged_ptr = match backing {
                Backing::Shared(s) => mem::transmute::<_, *mut ()>(s),
                Backing::Mmap(s) => Strict::map_addr(mem::transmute::<_, *mut ()>(s), |p| p | 0b01),
            };
            BackingPtr(NonNull::new_unchecked(tagged_ptr))
        }
    }

    #[inline]
    unsafe fn take(&mut self) -> Backing {
        if self.tag() == 0b00 {
            Backing::Shared(mem::transmute(self.0))
        } else {
            Backing::Mmap(mem::transmute(self.untagged()))
        }
    }

    #[inline]
    fn is_unique(&mut self) -> bool {
        if self.tag() != 0 {
            return false;
        }
        unsafe {
            // This cast is very unsafe, but is valid because
            // thriomphe ArcInner is repr(C), Arc and ThinArc and repr(transparent)
            // and Arc::is_unique only touches the ArcInner. See triomphe::ArcUnion.
            let arc: ManuallyDrop<Arc<()>> = mem::transmute(self.0.as_ptr());
            Arc::is_unique(&arc)
        }
    }

    #[inline]
    fn internal_clone(&self) -> Self {
        unsafe {
            // See is_unique for safety of this transmute
            let arc: ManuallyDrop<Arc<()>> = mem::transmute(self.untagged());
            mem::forget(Arc::clone(&arc));
        }
        Self(self.0)
    }
}

impl std::fmt::Debug for BackingPtr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.tag() == 0b00 {
            write!(f, "Shared({:?})", self.0.as_ptr())
        } else {
            write!(f, "MMap({:?})", self.untagged())
        }
    }
}

enum Backing {
    Shared(ThinArc<(), u8>),
    Mmap(Arc<memmap2::Mmap>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempfile;

    #[test]
    fn test_shared_vec() {
        let mut a = Bytes::from_slice(b"\0");
        assert_eq!(a.len(), 1);
        assert_eq!(a[0], 0u8);
        assert_eq!(a.clone().len(), 1);
        assert!(a.is_unique());
        let mut b = a.restrict(&a[1..]);
        assert!(!a.is_unique());
        assert!(!b.is_unique());
        a.make_unique();
        assert!(a.is_unique());
        assert!(b.is_unique());
        assert_eq!(a.as_ref(), b"\0");
        assert_eq!(b.as_ref(), b"");
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_mmap() {
        let mut f = tempfile().unwrap();
        f.write_all(b"\0\0\0").unwrap();
        let mut a = Bytes::from_mmap(Arc::new(unsafe { memmap2::Mmap::map(&f).unwrap() }), 0, 3);
        assert_eq!(a.len(), 3);
        assert_eq!(a.as_ref(), b"\0\0\0");
        assert_eq!(a.clone().len(), 3);
        assert!(!a.is_unique());
        let mut b = a.restrict(&a[1..]);
        assert!(!a.is_unique());
        assert!(!b.is_unique());
        a.make_unique();
        assert!(a.is_unique());
        assert_eq!(a.as_ref(), b"\0\0\0");
        b.make_unique();
        assert!(b.is_unique());
        assert_eq!(b.as_ref(), b"\0\0");
    }
}
