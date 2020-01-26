use std::{collections::VecDeque, time::Duration};

use crate::{
    repr::TxId,
    shim::{
        parking_lot::{Mutex, RwLock},
        sync::{mpsc, Arc},
    },
    wal::WalIdx,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckpointReason {
    User(TxId),
    WalSize(WalIdx),
    Periodic,
    OnDrop,
    WritesSpilled,
    TargetSize,
    MemoryPressure,
}

impl CheckpointReason {
    fn mutually_exclusive(&self, other: &mut CheckpointReason) -> bool {
        match (self, other) {
            (CheckpointReason::User(a), CheckpointReason::User(b)) => {
                *b = *a.max(b);
                true
            }
            (CheckpointReason::User(_), _) => false,
            (CheckpointReason::WalSize(_), _) => true,
            (CheckpointReason::Periodic, _) => unreachable!("Periodic isn't queued"),
            (CheckpointReason::OnDrop, _) => false,
            (CheckpointReason::WritesSpilled, CheckpointReason::WritesSpilled) => false,
            (CheckpointReason::WritesSpilled, _) => false,
            (CheckpointReason::TargetSize, CheckpointReason::TargetSize) => true,
            (CheckpointReason::TargetSize, _) => false,
            (CheckpointReason::MemoryPressure, CheckpointReason::MemoryPressure) => true,
            (CheckpointReason::MemoryPressure, _) => false,
        }
    }
}

#[derive(Debug, Default)]
struct Queue {
    closed: bool,
    queue: VecDeque<CheckpointReason>,
}

#[derive(Debug)]
pub struct CheckpointQueue {
    tx: mpsc::SyncSender<()>,
    rx: Arc<Mutex<mpsc::Receiver<()>>>,
    queue: Arc<RwLock<Queue>>,
}

impl CheckpointQueue {
    pub fn new() -> CheckpointQueue {
        let (tx, rx) = mpsc::sync_channel(1);
        let queue: Arc<RwLock<Queue>> = Default::default();
        CheckpointQueue {
            queue: queue.clone(),
            rx: Arc::new(Mutex::new(rx)),
            tx,
        }
    }

    pub fn request(&self, r: CheckpointReason) -> bool {
        let mut queue = self.queue.write();
        if queue.closed {
            return false;
        }
        let mut mut_exclusive = false;
        for reason in &mut queue.queue {
            if r.mutually_exclusive(reason) {
                mut_exclusive = true;
                break;
            }
        }
        if !mut_exclusive {
            queue.queue.push_back(r);
        }
        drop(queue);

        let _ = self.tx.try_send(());
        true
    }

    pub fn is_empty(&self) -> bool {
        self.queue.read().queue.is_empty()
    }

    pub fn peek(&self, timeout: Duration) -> Result<CheckpointReason, mpsc::RecvTimeoutError> {
        loop {
            let queue = self.queue.write();
            if queue.closed {
                return Err(mpsc::RecvTimeoutError::Disconnected);
            }
            if let Some(msg) = queue.queue.front().copied() {
                return Ok(msg);
            }
            drop(queue);
            self.rx.lock().recv_timeout(timeout)?;
        }
    }

    pub fn pop(&self, reason: &CheckpointReason) {
        let mut queue = self.queue.write();
        if queue.queue.front() == Some(reason) {
            queue.queue.pop_front();
        }
    }

    pub fn set_closed(&self, closed: bool) {
        self.queue.write().closed = closed;
        if closed {
            let _ = self.tx.try_send(());
        }
    }
}
