use std::{mem, ptr::NonNull, time::Duration};

use smallvec::SmallVec;

use crate::{
    repr::WalIdx,
    shim::{
        parking_lot::{Condvar, Mutex},
        thread,
    },
    wal::WalRead,
    Error,
};

#[derive(Debug, Default)]
struct GroupCommitInner {
    // Long lived allocation, not worth having a SmallVec
    queue: Vec<(NonNull<dyn WalRead>, Option<NonNull<Result<WalIdx, Error>>>)>,
}

#[derive(Debug)]
pub struct GroupCommit {
    backoff: Option<Duration>,
    condvar: Condvar,
    group: Mutex<GroupCommitInner>,
}

unsafe impl Send for GroupCommit {}

unsafe impl Sync for GroupCommit {}

impl GroupCommit {
    pub fn new(backoff: Option<Duration>) -> Self {
        Self {
            backoff,
            condvar: Condvar::new(),
            group: Default::default(),
        }
    }

    /// Performs a group commit of writes.
    /// Writes slice must not be empty nor exceed the max_write_count.
    pub fn write(
        &self,
        max_write_count: usize,
        writes: &mut [&mut dyn WalRead],
        execute: impl FnOnce(&mut [&mut dyn WalRead]) -> Result<WalIdx, Error>,
    ) -> Result<WalIdx, Error> {
        assert!((1..=max_write_count).contains(&writes.len()));
        let mut result = Ok(WalIdx::MAX);
        let result_mut = NonNull::from(&mut result);
        let mut group = self.group.lock();
        while group.queue.len() + writes.len() > max_write_count {
            // joining group would cause writes to go over the limit, wait for it to be processed
            self.condvar.wait(&mut group);
        }
        let leader = group.queue.is_empty(); // requires !write.is_empty()
        group.queue.extend(writes.iter_mut().enumerate().map(
            |(i, w): (usize, &mut &mut dyn WalRead)| {
                let wal_read_ptr: NonNull<dyn WalRead> = (*w).into();
                let wal_read_ptr: NonNull<dyn WalRead + 'static> =
                    unsafe { mem::transmute(wal_read_ptr) };
                let result_ptr = (!leader && i == 0).then_some(result_mut);
                (wal_read_ptr, result_ptr)
            },
        ));
        if leader {
            // new group as leader
            #[cfg(not(feature = "shuttle"))]
            if let Some(backoff) = self.backoff {
                lock_api::MutexGuard::unlocked_fair(&mut group, || {
                    thread::sleep(backoff);
                })
            } else {
                lock_api::MutexGuard::bump(&mut group);
            }
            #[cfg(feature = "shuttle")]
            {
                drop(group);
                if let Some(backoff) = self.backoff {
                    thread::sleep(backoff);
                }
                group = self.group.lock();
            }
            let mut batch = group
                .queue
                .iter_mut()
                .map(|(ptr, ..)| unsafe { ptr.as_mut() })
                .collect::<SmallVec<_, 16>>();
            result = execute(&mut batch);
            for (.., result_ptr) in group.queue.drain(..) {
                if let Some(result_ptr) = result_ptr {
                    unsafe {
                        #[allow(clippy::useless_asref)] // false-positive
                        result_ptr.write(
                            result
                                .as_ref()
                                .map(Clone::clone)
                                .map_err(Error::internal_clone),
                        );
                    }
                }
            }
            self.condvar.notify_all();
        } else {
            // join existing group as follower
            self.condvar.wait(&mut group);
        }

        debug_assert!(!matches!(result, Ok(WalIdx::MAX)));
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::shim::sync::{self, Arc};

    #[cfg_attr(not(feature = "shuttle"), test)]
    fn test_write_group() {
        #[cfg(not(feature = "shuttle"))]
        const THREADS: usize = 20;
        #[cfg(not(feature = "shuttle"))]
        const WRITES_PER_THREAD: usize = 10_000;
        #[cfg(feature = "shuttle")]
        const THREADS: usize = 5;
        #[cfg(feature = "shuttle")]
        const WRITES_PER_THREAD: usize = 1_000;
        let g = Arc::new(GroupCommit::new(Some(Duration::default())));
        let b = Arc::new(sync::Barrier::new(THREADS));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut threads = Vec::new();
        for t in 0..THREADS {
            let g = g.clone();
            let b = b.clone();
            let w = writes.clone();
            let t = thread::spawn(move || {
                b.wait();
                let mut results = Vec::with_capacity(WRITES_PER_THREAD);
                for i in 0..WRITES_PER_THREAD {
                    let idx = g
                        .write(
                            &mut [&mut &[t as u8][..], &mut &i.to_be_bytes()[..]],
                            |batch| {
                                let mut w = w.try_lock().unwrap();
                                let mut batch = batch
                                    .iter_mut()
                                    .map(|b| {
                                        let mut v = Vec::new();
                                        b.read_to_end(&mut v).unwrap();
                                        v
                                    })
                                    .collect::<Vec<_>>();
                                batch.sort_unstable();
                                w.push(batch);
                                Ok(w.len() as u64 - 1)
                            },
                        )
                        .unwrap();
                    results.push((t, i, idx));
                }
                results
            });
            threads.push(t);
        }
        let mut results: BTreeMap<u64, Vec<Vec<u8>>> = BTreeMap::new();
        for t in threads {
            let t_results = t.join().unwrap();
            for (t, i, idx) in t_results {
                results
                    .entry(idx)
                    .or_default()
                    .extend([vec![t as u8], i.to_be_bytes().as_slice().to_vec()]);
            }
        }
        let writes = Mutex::into_inner(Arc::try_unwrap(writes).unwrap());
        assert_eq!(results.len(), writes.len());
        for (w, mut r) in writes.into_iter().zip(results.into_values()) {
            r.sort_unstable();
            assert_eq!(w, r);
        }
    }

    #[cfg(feature = "shuttle")]
    #[test]
    fn test_write_group_shuttle() {
        shuttle::check_random(test_write_group, 100);
    }
}
