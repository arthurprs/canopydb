use crate::bytes_impl::{ArcBytes, ArcBytesHeader, UniqueArcBytes};
use std::{mem::MaybeUninit, ptr::NonNull};

pub(crate) struct UninitBytes(UniqueArcBytes);

impl UninitBytes {
    pub fn new(len: usize) -> Self {
        Self(UniqueArcBytes::new(len))
    }

    #[inline]
    pub unsafe fn assume_init(self) -> Bytes {
        let backing = self.0.assume_init();
        Bytes {
            ptr: backing.header_ptr().add(1).cast(),
            len: backing.header_ptr().read().len as usize,
            backing: SharedBytes(backing),
        }
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.0.data_mut()
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
#[debug("SharedBytes({})", self.as_slice().len())]
pub(crate) struct SharedBytes(ArcBytes);

impl SharedBytes {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.0.data()
    }
}

/// Database Bytes
///
/// Roughly equivalent to an `Arc<Vec<u8>>`.
///
/// Implements `PartialEq`, `Eq`, `PartialOrd`, `Ord`, `Hash`, `Borrow`, `AsRef`, `Deref` as a `&[u8]`
#[derive(Debug, Clone)]
pub struct Bytes {
    ptr: NonNull<u8>,
    len: usize,
    backing: SharedBytes,
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
            ptr: unsafe { NonNull::new_unchecked(v.0.data().as_ptr().cast_mut()) },
            len: v.0.data().len(),
            backing: v,
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
            backing: self.backing.clone(),
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
        self.backing.0.is_unique()
    }

    #[inline]
    pub(crate) fn truncate(&mut self, len: usize) {
        assert!(len <= self.len);
        self.len = len;
    }

    /// Returns the byte buffer as a slice that includes the prefix to it.
    /// No guarantees are provided for the contents of the prefix.
    /// Panics if `prefix_size` is greater than the size of `ArcBytesHeader`.
    #[inline]
    pub(crate) fn raw_data_with_prefix(&self, prefix_size: usize) -> &[u8] {
        // Due to the usage of SharedBytes, we're guaranteed to have at least size_of<ArcBytesHeader> before the ptr
        assert!(prefix_size <= size_of::<ArcBytesHeader>());
        unsafe {
            std::slice::from_raw_parts(self.ptr.as_ptr().sub(prefix_size), self.len + prefix_size)
        }
    }

    /// Return the backing `SharedBytes` instance.
    pub(crate) fn into_shared_bytes(self) -> SharedBytes {
        self.backing
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_vec() {
        let mut a = Bytes::from_slice(b"\x01");
        assert_eq!(a.len(), 1);
        assert_eq!(a[0], 1u8);
        a.as_mut().fill(0);
        assert_eq!(a[0], 0u8);
        assert_eq!(a.clone().len(), 1);
        assert!(a.is_unique());
        let _ = a.clone();
        let mut b = a.restrict(&a[1..]);
        assert!(!a.is_unique());
        assert!(!b.is_unique());
        a.make_unique();
        assert!(a.is_unique());
        assert!(b.is_unique());
        assert_eq!(a.as_ref(), b"\0");
        assert_eq!(b.as_ref(), b"");
        a.as_mut().fill(0x01);
        assert_eq!(a.as_ref(), b"\x01");
    }

    #[test]
    fn test_from_slices() {
        let slices: [&[u8]; 3] = [b"hello", b" ", b"world"];
        let bytes = Bytes::from_slices(&slices);
        assert_eq!(bytes.as_ref(), b"hello world");
    }

    #[test]
    fn test_truncate() {
        let mut bytes = Bytes::from_slice(b"truncate");
        bytes.truncate(4);
        assert_eq!(bytes.as_ref(), b"trun");
    }

    #[test]
    fn test_raw_data_with_prefix() {
        let bytes = Bytes::from_slice(b"prefix");
        let raw_data = bytes.raw_data_with_prefix(4);
        assert_eq!(&raw_data[4..], b"prefix");
    }

    #[test]
    fn test_into_shared_bytes() {
        let bytes = Bytes::from_slice(b"shared");
        let shared_bytes = bytes.into_shared_bytes();
        assert_eq!(shared_bytes.as_slice(), b"shared");
    }
}
