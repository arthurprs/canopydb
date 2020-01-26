#![allow(dead_code)]

#[cfg(not(feature = "shuttle"))]
pub use std::{sync, thread};

#[cfg(not(feature = "shuttle"))]
pub use parking_lot;

#[cfg(feature = "shuttle")]
pub use shuttle::*;

#[cfg(feature = "shuttle")]
pub mod sync {
    pub use shuttle::sync::*;

    #[derive(Debug)]
    pub struct OnceLock<T>(Mutex<Option<T>>);

    impl<T> Default for OnceLock<T> {
        fn default() -> Self {
            Self(Default::default())
        }
    }

    impl<T> OnceLock<T> {
        pub fn set(&self, value: T) -> Result<(), T> {
            let mut lock = self.0.lock().unwrap();
            if lock.is_some() {
                return Err(value);
            }
            *lock = Some(value);
            Ok(())
        }
        pub fn get(&self) -> Option<&T> {
            let lock = self.0.lock().unwrap();
            let ptr: *const T = lock.as_ref()?;
            drop(lock);
            Some(unsafe { &*ptr })
        }
    }
}

#[cfg(feature = "shuttle")]
pub mod parking_lot {
    use std::cell::RefCell;

    #[derive(Debug, Default)]
    pub struct Condvar(::shuttle::sync::Condvar);

    impl Condvar {
        pub fn new() -> Self {
            Condvar(::shuttle::sync::Condvar::new())
        }

        pub fn notify_one(&self) {
            self.0.notify_one()
        }

        pub fn notify_all(&self) {
            self.0.notify_all();
        }

        pub fn wait<'a, T>(&self, guard: &mut lock_api::MutexGuard<'a, RawMutex, T>) {
            unsafe {
                let old_guard = lock_api::MutexGuard::mutex(guard)
                    .raw()
                    .guard
                    .borrow_mut()
                    .take()
                    .unwrap();
                let new_guard = self.0.wait(old_guard).unwrap();
                assert!(lock_api::MutexGuard::mutex(guard)
                    .raw()
                    .guard
                    .borrow()
                    .is_none());
                *lock_api::MutexGuard::mutex(guard).raw().guard.borrow_mut() = Some(new_guard);
            }
        }
    }

    pub type Mutex<T> = lock_api::Mutex<RawMutex, T>;

    #[derive(Default, Debug)]
    pub struct RawMutex {
        mutex: ::shuttle::sync::Mutex<()>,
        guard: RefCell<Option<::shuttle::sync::MutexGuard<'static, ()>>>,
    }

    unsafe impl Send for RawMutex {}
    unsafe impl Sync for RawMutex {}

    unsafe impl ::lock_api::RawMutex for RawMutex {
        const INIT: Self = RawMutex {
            mutex: ::shuttle::sync::Mutex::new(()),
            guard: RefCell::new(None),
        };

        type GuardMarker = ::lock_api::GuardNoSend;

        fn lock(&self) {
            // println!("will lock {} \n {}", self as *const Self as usize, ::std::backtrace::Backtrace::force_capture());
            let guard = self.mutex.lock().unwrap();
            assert!(self.guard.borrow().is_none());
            *self.guard.borrow_mut() = unsafe { ::std::mem::transmute(guard) };
        }

        fn try_lock(&self) -> bool {
            shuttle::hint::spin_loop();
            if let Ok(guard) = self.mutex.try_lock() {
                shuttle::hint::spin_loop();
                let old = self
                    .guard
                    .borrow_mut()
                    .replace(unsafe { ::std::mem::transmute(guard) });
                drop(old);
                true
            } else {
                false
            }
        }

        unsafe fn unlock(&self) {
            shuttle::hint::spin_loop();
            let guard = self.guard.borrow_mut().take();
            guard.unwrap();
            shuttle::hint::spin_loop();
        }
    }

    #[derive(Default, Debug)]
    pub struct RwLock<T>(::shuttle::sync::RwLock<T>);

    #[derive(Debug, Deref)]
    #[deref(forward)]
    pub struct RwLockReadGuard<'rwlock, T>(::shuttle::sync::RwLockReadGuard<'rwlock, T>);

    #[derive(Debug, Deref, DerefMut)]
    #[deref(forward)]
    #[deref_mut(forward)]
    pub struct RwLockWriteGuard<'rwlock, T>(::shuttle::sync::RwLockWriteGuard<'rwlock, T>);

    #[derive(Debug, Deref)]
    #[deref(forward)]
    pub struct RwLockUpgradableReadGuard<'rwlock, T>(::shuttle::sync::RwLockWriteGuard<'rwlock, T>);

    impl<T> RwLock<T> {
        pub const fn new(t: T) -> Self {
            Self(::shuttle::sync::RwLock::new(t))
        }

        pub fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap()
        }

        pub fn into_inner(self) -> T {
            self.0.into_inner().unwrap()
        }

        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            RwLockReadGuard(self.0.read().unwrap())
        }

        pub fn upgradable_read(&self) -> RwLockUpgradableReadGuard<'_, T> {
            RwLockUpgradableReadGuard(self.0.write().unwrap())
        }

        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            RwLockWriteGuard(self.0.write().unwrap())
        }

        pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
            self.0.try_write().map(RwLockWriteGuard).ok()
        }

        pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
            self.0.try_read().map(RwLockReadGuard).ok()
        }
    }

    impl<'rwlock, T> RwLockUpgradableReadGuard<'rwlock, T> {
        pub fn upgrade(self) -> RwLockWriteGuard<'rwlock, T> {
            RwLockWriteGuard(self.0)
        }
        pub fn with_upgraded<Ret, F: FnOnce(&mut T) -> Ret>(&mut self, f: F) -> Ret {
            f(&mut self.0)
        }
    }
}
