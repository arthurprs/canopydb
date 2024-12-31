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

    struct Sem {
        condvar: ::shuttle::sync::Condvar,
        permits: ::shuttle::sync::Mutex<usize>,
    }

    unsafe impl Send for Sem {}
    unsafe impl Sync for Sem {}

    impl Sem {
        const fn new() -> Self {
            Self {
                condvar: ::shuttle::sync::Condvar::new(),
                permits: ::shuttle::sync::Mutex::new(usize::MAX),
            }
        }

        fn take(&self, n: usize) {
            let mut permits = self.permits.lock().unwrap();
            while *permits < n {
                permits = self.condvar.wait(permits).unwrap();
            }
            *permits -= n;
        }

        fn try_take(&self, n: usize) -> bool {
            let mut permits = self.permits.lock().unwrap();
            if *permits >= n {
                *permits -= n;
                true
            } else {
                false
            }
        }

        fn add(&self, n: usize) {
            *self.permits.lock().unwrap() += n;
            self.condvar.notify_all();
        }
    }

    pub struct RawRwLock {
        upgrade: Sem,
        rwlock: Sem,
    }

    unsafe impl ::lock_api::RawRwLock for RawRwLock {
        const INIT: Self = Self {
            upgrade: Sem::new(),
            rwlock: Sem::new(),
        };

        type GuardMarker = ::lock_api::GuardSend;

        fn lock_shared(&self) {
            self.rwlock.take(1);
        }

        fn try_lock_shared(&self) -> bool {
            self.rwlock.try_take(1)
        }

        unsafe fn unlock_shared(&self) {
            self.rwlock.add(1);
        }

        fn lock_exclusive(&self) {
            self.upgrade.take(usize::MAX);
            self.rwlock.take(usize::MAX);
        }

        fn try_lock_exclusive(&self) -> bool {
            if self.upgrade.try_take(usize::MAX) {
                if self.rwlock.try_take(usize::MAX) {
                    return true;
                }
                self.upgrade.add(usize::MAX);
            }
            false
        }

        unsafe fn unlock_exclusive(&self) {
            self.rwlock.add(usize::MAX);
            self.upgrade.add(usize::MAX);
        }
    }

    unsafe impl ::lock_api::RawRwLockUpgrade for RawRwLock {
        fn lock_upgradable(&self) {
            self.upgrade.take(usize::MAX);
            self.rwlock.take(1);
        }

        fn try_lock_upgradable(&self) -> bool {
            if self.upgrade.try_take(usize::MAX) {
                if self.rwlock.try_take(1) {
                    return true;
                }
                self.upgrade.add(usize::MAX);
            }
            false
        }

        unsafe fn unlock_upgradable(&self) {
            self.rwlock.add(1);
            self.upgrade.add(usize::MAX);
        }

        unsafe fn upgrade(&self) {
            self.rwlock.take(usize::MAX - 1);
        }

        unsafe fn try_upgrade(&self) -> bool {
            self.rwlock.try_take(usize::MAX - 1)
        }
    }

    unsafe impl ::lock_api::RawRwLockDowngrade for RawRwLock {
        unsafe fn downgrade(&self) {
            self.rwlock.add(usize::MAX - 1);
            self.upgrade.add(usize::MAX);
        }
    }

    unsafe impl ::lock_api::RawRwLockUpgradeDowngrade for RawRwLock {
        unsafe fn downgrade_upgradable(&self) {
            self.upgrade.add(usize::MAX);
        }

        unsafe fn downgrade_to_upgradable(&self) {
            self.rwlock.add(usize::MAX - 1);
        }
    }

    pub type RwLock<T> = ::lock_api::RwLock<RawRwLock, T>;
    pub type RwLockReadGuard<'a, T> = ::lock_api::RwLockReadGuard<'a, RawRwLock, T>;
    pub type RwLockWriteGuard<'a, T> = ::lock_api::RwLockWriteGuard<'a, RawRwLock, T>;
    pub type ArcRwLockReadGuard<T> = ::lock_api::ArcRwLockReadGuard<RawRwLock, T>;
    pub type ArcRwLockWriteGuard<T> = ::lock_api::ArcRwLockWriteGuard<RawRwLock, T>;
    pub type RwLockUpgradableReadGuard<'a, T> =
        ::lock_api::RwLockUpgradableReadGuard<'a, RawRwLock, T>;
}
