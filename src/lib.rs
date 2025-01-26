#![doc = include_str!("../README.md")]
#![allow(
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::missing_transmute_annotations
)]
#![cfg_attr(feature = "nightly", feature(portable_simd))]

#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate log;

mod allocator;
mod bytes;
mod bytes_impl;
mod checkpoint;
mod cursor;
mod env;
#[cfg(fuzzing)]
pub mod freelist;
#[cfg(not(fuzzing))]
mod freelist;
mod node;
mod options;
mod page;
mod pagetable;
#[cfg(fuzzing)]
pub mod utils;
#[cfg(not(fuzzing))]
mod utils;
#[cfg(fuzzing)]
pub mod wal;
#[cfg(not(fuzzing))]
mod wal;
#[macro_use]
mod repr;
mod error;
mod group_commit;
mod shim;
mod tree;
mod write_batch;

use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{btree_map, BTreeMap},
    fs::{File, OpenOptions},
    io::{self, Write},
    mem::{self, size_of, ManuallyDrop},
    ops::Range,
    time::{Duration, Instant},
};

use crate::{
    allocator::{Allocator, MainAllocator},
    bytes::*,
    checkpoint::{CheckpointQueue, CheckpointReason},
    env::{EnvironmentInner, NodeCacheKey, SharedEnvironmentInner},
    error::{error_validation, io_invalid_data, io_invalid_input},
    freelist::*,
    node::*,
    options::EnvDbOptions,
    page::Page,
    pagetable::{Item, PageTable},
    repr::*,
    shim::{
        parking_lot::{Condvar, Mutex, RawMutex, RawRwLock, RwLock},
        sync::{atomic, mpsc, Arc as StdArc, Weak as StdWeak},
        thread,
    },
    tree::*,
    utils::{ByteSize, CellExt, EscapedBytes, FileExt, FnTrap, SharedJoinHandle, Trap, TrapResult},
    write_batch::{self as wb, WriteBatch},
};
pub use crate::{
    bytes::Bytes,
    cursor::{RangeIter, RangeKeysIter},
    env::Environment,
    error::Error,
    options::{DbOptions, EnvOptions, HaltCallbackFn, TreeOptions},
    tree::Tree,
};

use hashbrown::{hash_map, hash_set};

type HashSet<K> = hash_set::HashSet<K, foldhash::fast::RandomState>;
type HashMap<K, V> = hash_map::HashMap<K, V, foldhash::fast::RandomState>;

use env::EnvironmentHandle;
use lock_api::{ArcMutexGuard, ArcRwLockReadGuard, ArcRwLockWriteGuard};
use smallvec::SmallVec;
use triomphe::Arc;
use zerocopy::*;

pub(crate) const PAGE_SIZE: u64 = 4 * 1024;
#[cfg(not(fuzzing))]
pub(crate) const MIN_PAGE_COMPRESSION_BYTES: u64 = 2 * PAGE_SIZE;
#[cfg(fuzzing)]
pub(crate) const MIN_PAGE_COMPRESSION_BYTES: u64 = PAGE_SIZE;

#[derive(Copy, Clone)]
#[repr(packed)]
struct FreePage(TxId, PageId);

impl std::fmt::Debug for FreePage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (a, b) = (self.0, self.1);
        f.debug_tuple("FreePage").field(&a).field(&b).finish()
    }
}

bitflags::bitflags! {
    #[derive(Default, Copy, Clone)]
    struct TransactionFlags: u8 {
        /// Set if the txn was committed or rolledback
        const DONE = 1;
        /// Set if the txn altered anything
        const DIRTY = 1 << 1;
        /// Set for all kinds of write transactions
        const WRITE_TX = 1 << 2;
        /// Set for multi write transactions
        const MULTI_WRITE_TX = 1 << 3;
        /// Set for checkpoint transactions
        const CHECKPOINT_TX = 1 << 4;
        /// Set when the page table is dirty and may need cleanup on rollback
        const PAGE_TABLE_DIRTY = 1 << 5;
    }
}
use TransactionFlags as TF;

/// # Write transaction
///
/// A Write transaction can mutate data and read its own changes. Any changes must be committed
/// by calling [WriteTransaction::commit]. Once committed, subsequent transactions will see changes
/// made by this transaction.
///
/// Dropping the WriteTransaction has the same effect as [WriteTransaction::rollback] and will discard any changes.
///
/// While the write transaction is open/uncommitted, changes are held in memory up to
/// [DbOptions::write_txn_memory_limit]. Changes going over this limit will trigger
/// "spilling", which writes the less recently used pages to the Database file.
/// Note that this limit doesn't include previously committed versions from previous
/// write transactions. Once committed, these changes are considered committed
/// and count towards the [DbOptions::checkpoint_target_size] and other memory limits.
///
/// ## Exclusive Write Transactions
///
/// Only one active write transaction can be active at a given time (similar to a Mutex).
/// This property is automatically enforced by the Database.
///
/// Unlike Concurrent Write Transactions, commit cannot error with [`Error::WriteConflict`].
///
/// ## Concurrent Write Transactions
///
/// Write transactions may run concurrently and conflict detection is performed at commit time.
/// Applications utilizing this often require wraping write transaction blocks in a loop and retry
/// the transaction if [`WriteTransaction::commit`] returns [`Error::WriteConflict`].
///
/// These transactions provide Snapshot-Isolation (SI) instead of _Serializable_-Snapshot-Isolation (SSI) as
/// Write-Skew anomalies can still happen.
///
/// Note that the implementation detects write-write conflicts at the page level granularity so there could
/// be "false" conflicts in case mutated keys fall within the same Leaf page.
///
/// While concurrent write transactions have more individual overhead than exclusive write
/// transactions, they might be useful in cases like:
///
/// * Transactions that run significant amounts of non-database code between database operations and have low conflict chance.
/// * Transactions that affect disjoint Trees of the Database and thus can run concurrently without conflicts.
/// * Larger than memory workloads, as it's useful to load relevant parts of the transaction from the disk concurrently, even if there's a conflict risk.
/// * Random write workloads which have small chances of conflicts and need extra throughput.
#[derive(Debug, Deref, DerefMut)]
pub struct WriteTransaction(Transaction);

/// Read-only transaction
#[derive(Debug, Deref)]
pub struct ReadTransaction(Transaction);

/// Untyped Transaction
///
/// A `&Transacton` be used as an agnostic transaction when only read access is required.
/// Both [ReadTransaction] and [WriteTransaction] automatically dereference to [Transaction].
pub struct Transaction {
    trap: Trap,
    flags: Cell<TransactionFlags>,
    inner: ManuallyDrop<SharedDatabaseInner>,
    state: Cell<DatabaseState>,
    allocator: RefCell<Allocator>,
    /// Trees opened/deleted by the transaction
    trees: RefCell<HashMap<Arc<[u8]>, TreeState>>,
    /// Dirty nodes of a transaction
    nodes: RefCell<DirtyNodes>,
    nodes_spilled_span: Cell<PageId>,
    /// WAL write batch (present in write tx if use_wal is enabled)
    wal_write_batch: RefCell<Option<WriteBatch>>,
    /// Scratch space
    scratch_buffer: RefCell<Vec<u8>>,
    /// held by the exclusive write tx
    exclusive_write_lock: Option<ArcRwLockWriteGuard<RawRwLock, ()>>,
    /// held by a multi writer tx
    multi_write_lock: Option<ArcRwLockReadGuard<RawRwLock, ()>>,
    commit_lock: Option<ArcMutexGuard<RawMutex, CachedWriteState>>,
    tracked_transaction: Option<TxId>,
    /// Handle to the environment, set if this is an user transaction and the Database is open
    env_handle: Option<EnvironmentHandle>,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("state", &self.state.get())
            .field("trees", &self.trees)
            .field("nodes", &self.nodes)
            .field("done", &self.flags.get().contains(TF::DONE))
            .field("write_batch", &self.wal_write_batch)
            .field("exclusive_write_lock", &self.exclusive_write_lock.is_some())
            .field("multi_write_lock", &self.multi_write_lock.is_some())
            .finish()
    }
}

#[derive(Default)]
struct CachedWriteState {
    trees: Option<HashMap<Arc<[u8]>, TreeState>>,
    nodes: Option<DirtyNodes>,
    wal_write_batch: Option<WriteBatch>,
    scratch_buffer: Vec<u8>,
}

#[derive(Debug)]
enum TxNode {
    Stashed(UntypedNode),
    Popped(u64 /* page_size/weight */),
    Spilled {
        span: PageId,
        compressed_page: Option<(PageId, PageId)>,
    },
    Freed {
        from_snapshot: bool,
        span: PageId,
        compressed_page: Option<(PageId, PageId)>,
    },
}

type SharedDatabaseInner = StdArc<DatabaseInner>;
type WeakDatabaseInner = StdWeak<DatabaseInner>;

struct DatabaseRecovery {
    db: Database,
    tx: WriteTransaction,
    batches_recovered: u64,
    ops_recovered: u64,
    last_wal_idx: Option<WalIdx>,
}

struct DatabaseFile {
    file: File,
    file_len: atomic::AtomicU64,
    resize_lock: Mutex<()>,
}

impl DatabaseFile {
    fn new(path: &std::path::Path) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        // TODO: check if this needs to be done after each resize
        utils::fadvise_read_ahead(&file, false)?;
        let file_len = file.metadata()?.len();
        let result = Self {
            resize_lock: Default::default(),
            file,
            file_len: file_len.into(),
        };
        Ok(result)
    }

    fn ensure_file_size(
        &self,
        allow_shrink: bool,
        data_file_required_size: u64,
    ) -> Result<(), Error> {
        // fast-path
        if !allow_shrink && self.file_len() >= data_file_required_size {
            return Ok(());
        }

        // Locked path, to coordinate multiple threads racing.
        // E.g. thread-1 wants to increase it to 10MB and another to 9MB.
        let _resize_lock = self.resize_lock.lock();
        let data_file_curr_size = self.file_len();
        if data_file_curr_size < data_file_required_size {
            info!(
                "Growing data file from {} to {}",
                ByteSize(data_file_curr_size),
                ByteSize(data_file_required_size),
            );
            utils::fallocate(&self.file, data_file_required_size)?;
        } else if allow_shrink && data_file_curr_size > data_file_required_size {
            info!(
                "Shrinking data file from {} to {}",
                ByteSize(data_file_curr_size),
                ByteSize(data_file_required_size),
            );
            self.file.set_len(data_file_required_size)?;
        } else {
            return Ok(());
        }
        self.file_len
            .store(data_file_required_size, atomic::Ordering::Release);
        Ok(())
    }

    fn file_len(&self) -> u64 {
        self.file_len.load(atomic::Ordering::Acquire)
    }
}

#[derive(Default)]
struct RunningStats {
    checkpointer_compression: Option<f64>,
}

/// Metadata for open txns other than the unique write txn,
/// namely read txns and checkpoint txn.
#[derive(Debug)]
struct OpenTxn {
    tx_id: TxId,
    earliest_snapshot_tx_id: TxId,
    ref_count: u32,
    writers: u32,
}

#[derive(Debug, Default)]
struct FreeBuffers {
    free: BTreeMap<TxId, Vec<FreePage>>,
    scan_from: TxId,
}

struct DatabaseInner {
    /// An open transaction holds a handle to the environment.
    /// If a database is closed, no more _user_ transactions can be created,
    /// but user transactions created before closing may still exist.
    /// Note that checkpoint transactions may still be created after closing.
    open: Mutex<Option<EnvironmentHandle>>,
    env: env::SharedEnvironmentInner,
    /// Unique identifier for the database in the Environment
    env_db_id: DbId,
    env_db_name: String,
    file: DatabaseFile,
    // TODO: replace with seqlock
    state: Mutex<DatabaseState>,
    page_table: PageTable,
    allocator: Arc<Mutex<MainAllocator>>,
    /// Page Buffers freed by transactions.
    /// Can be released when they are no longer visible
    /// Map<transaction that freed, Vec<visible from tx (Inc.), buffer idx>>
    free_buffers: Mutex<FreeBuffers>,
    /// Map<checkpoint tx, Freelist>.
    /// Note: Freelists may be empty
    /// Doesn't contain the freelist for the latest snapshot (metapage.snapshot_tx_id) nor for
    /// the ongoing snapshot (ongoing_snapshot_tx_id). Those are tracked in MainAllocator.
    old_snapshots: Mutex<BTreeMap<TxId, Freelist>>,
    checkpoint_lock: Mutex<()>,
    running_stats: Mutex<RunningStats>,
    commit_lock: StdArc<Mutex<CachedWriteState>>,

    write_lock: StdArc<RwLock<()>>,
    /// Open transactions other than the unique write transaction
    /// This is a sorted Vec to avoid hitting the allocator when the collections len fluctuates (0-1-0-1..)
    /// Furthermore, min lookup are push-last are common, which more than make up for small linear removals.
    transactions: Mutex<Vec<OpenTxn>>,
    /// Condvar to wait until transactions empties or new (user) transactions can be added
    transactions_condvar: Condvar,
    /// Env and Db options
    opts: EnvDbOptions,
    /// Communication with Background Thread
    checkpoint_queue: CheckpointQueue,
    /// The Background Thread handle
    bg_thread: Mutex<Option<Arc<SharedJoinHandle<Result<(), Error>>>>>,
}

impl std::fmt::Debug for DatabaseInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseInner")
            .field("name", &self.env_db_name)
            .field("id", &self.env_db_id)
            .finish()
    }
}

#[derive(Default, Debug, Copy, Clone)]
struct DatabaseState {
    metapage: MetapageHeader,
    /// Spilled pages total span
    spilled_total_span: PageId,
    /// TxId currently being snapshotted
    ongoing_snapshot_tx_id: Option<TxId>,
    /// If set the db is in read only mode
    halted: bool,
    /// All user transactions (including readers) will block
    block_user_transactions: bool,
}

impl DatabaseState {
    #[inline]
    fn check_halted(&self) -> Result<(), Error> {
        if self.halted {
            Err(Error::DatabaseHalted)
        } else {
            Ok(())
        }
    }
}

/// Single-writer multiple-reader transactional Database
///
/// # Transactions
///
/// The `Database` may only have active write transaction at any given time. Read transactions are cheap and provide
/// consistent snapshots of database. A database may contain multiple named [Tree]s with independent key value pairs.
///
/// # Multi-Version-Concurrency-Control (MVCC) via Copy-On-Write (COW)
///
/// Canopydb Databases provides MVCC via COW. COW allows efficient mutation of the database by only generating
/// new versions of the modified pages, unmodified parts can be referred to by the new version. Note that
/// this is an optimized COW implementation that (1) avoids rewriting the root to leaf path whenever possible,
/// and (2) efficiently clears in-memory versions not needed even under the presence of long lived transactions.
///
/// Different committed versions of the database are kept until they are no longer needed. These versions provide
/// consistent snapshots for read transactions and also allow cheap rollback of write transactions.
///
/// The database lives part in memory (up to one uncommitted version and multiple committed versions) and part
/// in the corresponding database file. Usually many versions of the database will only exist in memory, and only
/// a few will be persisted to the database file via checkpoints (see below).
///
/// Individual transaction crash recovery and durability, if desired, is achieved by writing Database operations
/// to the [Environment] WAL. Otherwise the database has checkpoint durability (see below).
///
/// # Checkpoints
///
/// Checkpoints write the latest version of the database resident in memory to the database file, creating a new stable
/// snapshot in the database file. As the pages are copied to the database file, the memory associated with them is released.
/// Checkpoints will also allow file space made obsolete by the new checkpoint to be reused.
///
/// If the Write-Ahead-Log (WAL) is being used, the checkpoint also releases the corresponding
/// entries from the Write-Ahead-Log (WAL) as they aren't needed anymore for crash recovery.
///
/// # Background thread
///
/// Each `Database` has a companion background thread that is responsible for performing checkpoints (see [Database::checkpoint]).
///
/// # Unique instance
///
/// Only one instance of each [Database] can be active at a given time. Attempting to re-open the database of modifying
/// it while the corresponding instance is still active will return errors.
///
/// Dropping the Database automatically returns the instance to the [Environment], making it possible to re-open or delete it.
///
/// # Limits
///
/// * Max Tree key length: 1GB
/// * Max Tree value length: 3GB
/// * Max Database size: 12TB
///
/// # Environment
///
/// The `Database` is part of an [Environment] and may share a Write-Ahead-Log (WAL) and page cache with other databases from the same environment.
#[derive(Debug)]
pub struct Database {
    inner: SharedDatabaseInner,
    env_handle: EnvironmentHandle,
}

impl DatabaseRecovery {
    fn get_tree_by_id<'tx, 'a>(
        tx: &'tx WriteTransaction,
        trees_by_id: &'a mut HashMap<TreeId, Tree<'tx>>,
        tree_id: TreeId,
    ) -> Result<&'a mut Tree<'tx>, Error> {
        match trees_by_id.entry(tree_id) {
            hash_map::Entry::Occupied(o) => Ok(o.into_mut()),
            hash_map::Entry::Vacant(v) => {
                if let Some(tree) = tx.get_tree_by_id(tree_id)? {
                    Ok(v.insert(tree))
                } else {
                    Err(io_invalid_data!(
                        "Tree <Id {tree_id}> not found during recovery"
                    ))
                }
            }
        }
    }

    fn apply_write_batch(
        &mut self,
        wal_idx: WalIdx,
        operations: impl Iterator<Item = io::Result<wb::Operation>>,
    ) -> Result<(), Error> {
        if wal_idx < self.tx.state.get_mut().metapage.wal_start {
            debug!(
                "Ignoring batch idx {} (< {})",
                wal_idx,
                self.tx.state.get_mut().metapage.wal_start
            );
            return Ok(());
        }
        debug!("Replaying batch idx {wal_idx}");
        let mut trees_by_id = HashMap::<TreeId, Tree<'_>>::default();
        let mut ops = 0;
        for op in operations {
            let op = op?;
            trace!("Replaying op {:?}", op);
            match op {
                wb::Operation::Database(_) => (),
                wb::Operation::Insert(tree_id, key, value) => {
                    let tree = Self::get_tree_by_id(&self.tx, &mut trees_by_id, tree_id)?;
                    tree.insert(&key, &value)?;
                }
                wb::Operation::Delete(tree_id, key) => {
                    let tree = Self::get_tree_by_id(&self.tx, &mut trees_by_id, tree_id)?;
                    tree.delete(&key)?;
                }
                wb::Operation::DeleteRange(tree_id, bounds) => {
                    let tree = Self::get_tree_by_id(&self.tx, &mut trees_by_id, tree_id)?;
                    tree.delete_range(bounds)?;
                }
                wb::Operation::CreateTree(tree_id, name, options) => {
                    trees_by_id.clear();
                    self.tx.create_tree(tree_id, &name, options)?;
                }
                wb::Operation::DeleteTree(name) => {
                    trees_by_id.clear();
                    if !self.tx.delete_tree(&name)? {
                        return Err(io_invalid_data!(
                            "Tree {} not deleted during recovery",
                            EscapedBytes(&name)
                        ));
                    }
                }
                wb::Operation::RenameTree(old, new) => {
                    trees_by_id.clear();
                    if !self.tx.rename_tree(&old, &new)? {
                        return Err(io_invalid_data!(
                            "Tree {} not renamed to {} during recovery",
                            EscapedBytes(&old),
                            EscapedBytes(&new),
                        ));
                    }
                }
            }
            ops += 1;
        }

        self.batches_recovered += 1;
        self.ops_recovered += ops;
        self.last_wal_idx = Some(wal_idx);
        Ok(())
    }

    fn finish(mut self) -> Result<Database, Error> {
        if self.last_wal_idx.is_none() {
            info!("Nothing recovered from WAL");
            return Ok(self.db);
        };
        // As implemented each batch corresponds to one txn
        let state = self.tx.state.get_mut();
        state.metapage.tx_id += self.batches_recovered - 1;
        state.metapage.wal_end = self.last_wal_idx.unwrap() + 1;
        info!(
            "Recovered from WAL up to txn id {} from {} batches and {} operations",
            state.metapage.tx_id, self.batches_recovered, self.ops_recovered
        );
        self.tx.commit()?;
        Ok(self.db)
    }

    fn no_recovery(self) -> Database {
        self.db
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        #[cfg(any(test, fuzzing))]
        warn!("Drop for Database");
        *self.inner.open.lock() = None;
        if self.inner.opts.checkpoint_db_on_drop {
            self.inner
                .request_checkpoint(checkpoint::CheckpointReason::OnDrop);
        }
    }
}

impl Database {
    /// Creates an [Environment] in the specified `path` and returns
    /// (creating if required) a Database named "default" from it.
    pub fn new(path: impl AsRef<std::path::Path>) -> Result<Database, Error> {
        Self::with_options(EnvOptions::new(path), DbOptions::default())
    }

    /// Creates an [Environment] with the specified `options` and returns
    /// (creating if required) a Database named "default" from it.
    pub fn with_options(env_opts: EnvOptions, db_opts: DbOptions) -> Result<Database, Error> {
        let env = Environment::with_options(env_opts)?;
        let db = env.get_or_create_database_with("default", db_opts)?;
        Ok(db)
    }

    fn new_internal(
        opts: EnvDbOptions,
        env: SharedEnvironmentInner,
        env_handle: EnvironmentHandle,
        env_db_id: DbId,
        env_db_name: String,
    ) -> Result<DatabaseRecovery, Error> {
        let _ = std::fs::create_dir_all(&opts.path);
        let file = DatabaseFile::new(&opts.path.join("DATA"))?;
        opts.db.write_to_db_folder(&opts.path)?;
        let open = Mutex::new(Some(env_handle.clone()));
        let checkpoint_queue = CheckpointQueue::new();
        let inner = SharedDatabaseInner::from(DatabaseInner {
            open,
            env,
            env_db_id,
            env_db_name,
            file,
            state: Default::default(),
            page_table: Default::default(),
            checkpoint_lock: Default::default(),
            write_lock: Default::default(),
            commit_lock: Default::default(),
            old_snapshots: Default::default(),
            allocator: Default::default(),
            free_buffers: Default::default(),
            transactions: Default::default(),
            transactions_condvar: Default::default(),
            running_stats: Default::default(),
            checkpoint_queue,
            bg_thread: Default::default(),
            opts,
        });

        let mut db = Database {
            inner: inner.clone(),
            env_handle,
        };

        if db.inner.file.file_len() < PAGE_SIZE * 2 {
            db.initialize_fresh()?;
        } else {
            match db.load_from_data_file() {
                Ok(()) => (),
                Err(e) if db.inner.file.file_len() == PAGE_SIZE * 2 => {
                    info!("Error loading data file 2 pages wide, possible initialization failure: {e}");
                    db.initialize_fresh()?
                }
                Err(e) => return Err(e),
            }
        }
        DatabaseInner::start_bg_thread(&inner);

        #[cfg(all(any(fuzzing, test), debug_assertions))]
        db.validate_free_space().unwrap();
        let db_recovery = DatabaseRecovery {
            tx: Transaction::new_write(&db.inner, false, false)?,
            db,
            batches_recovered: Default::default(),
            ops_recovered: Default::default(),
            last_wal_idx: Default::default(),
        };

        Ok(db_recovery)
    }

    /// Get the [Environment] associated with this Database.
    pub fn environment(&self) -> Environment {
        EnvironmentInner::upgrade(self.inner.env.clone(), self.env_handle.clone())
    }

    #[cfg(test)]
    pub(crate) fn drop_wait(self) {
        let bg_thread = self.inner.bg_thread.lock().clone();
        drop(self);
        if let Some(bg_thread) = bg_thread {
            let _ = bg_thread.join();
        }
    }

    #[cfg(any(fuzzing, test))]
    pub fn validate_free_space(&self) -> Result<(), Error> {
        let _checkpoint_lock = self.inner.checkpoint_lock.lock();
        let _write_lock = self.inner.write_lock.write();
        // We are using a read tnx to avoid dealing with the write txn reservations.
        // Furthermore, since we're holding checkpoint lock it must be a non-user txn to avoid
        // deadlocks with major operations such as compaction.
        let txn = Transaction::new_read(&self.inner, false)?;
        // Only check for halted once all the exclusive locks are held,
        // so we can observe any failures from other tasks.
        txn.state.get().check_halted()?;
        let mut spans = Freelist::default();
        let mut indirections = Freelist::default();
        let mut cb = &mut |p: PageId, s: PageId| {
            if p.is_compressed() {
                indirections.free(p, 1).unwrap();
            } else {
                spans.free(p, s).unwrap();
            }
        };
        txn.get_trees_tree().iter_pages(cb)?;
        txn.get_indirection_tree().iter_pages(cb)?;
        for tree_name in txn.list_trees()? {
            let tree = txn.get_tree(&tree_name)?.unwrap();
            tree.iter_pages(&mut cb)?;
        }
        {
            for ind_pid in indirections.iter_pages() {
                let node = txn.clone_node(ind_pid)?;
                if let Some((pid, span)) = node.compressed_page {
                    spans.free(pid, span).unwrap();
                }
            }
        }
        {
            let old_snapshots = self.inner.old_snapshots.lock();
            for snapshot_fl in old_snapshots.values() {
                let (snapshots_free_left, snapshots_free_right) =
                    snapshot_fl.clone().split(FIRST_COMPRESSED_PAGE);
                spans.merge(&snapshots_free_left).unwrap();
                indirections.merge(&snapshots_free_right).unwrap();
            }
        }
        if txn.state.get().metapage.freelist_root != PageId::default() {
            let freelist_spans = self
                .inner
                .read_from_pages(txn.state.get().metapage.freelist_root, false)?
                .0;
            spans.merge(&freelist_spans).unwrap();
        }

        let mut main_allocator = self.inner.allocator.lock();
        for (_, free, ind_free) in &main_allocator.pending_free {
            spans.merge(free).unwrap();
            indirections.merge(ind_free).unwrap();
        }
        spans.merge(main_allocator.free.merged().unwrap()).unwrap();
        let (snapshots_free_left, snapshots_free_right) = main_allocator
            .snapshot_free
            .merged()
            .unwrap()
            .clone()
            .split(FIRST_COMPRESSED_PAGE);
        spans.merge(&snapshots_free_left).unwrap();
        indirections.merge(&snapshots_free_right).unwrap();
        assert!(main_allocator.next_snapshot_free.is_empty());
        let last = spans.last_piece().unwrap_or((0, 2));
        let diff = std::collections::BTreeSet::from_iter(2..main_allocator.next_page_id)
            .difference(&std::collections::BTreeSet::from_iter(spans.iter_pages()))
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(last.0 + last.1, main_allocator.next_page_id, "{diff:?}");
        assert_eq!(last.0 + last.1 - 2, spans.len(), "{diff:?}");

        indirections
            .merge(main_allocator.indirection_free.merged().unwrap())
            .unwrap();
        let last = indirections
            .last_piece()
            .unwrap_or((FIRST_COMPRESSED_PAGE, 0));
        assert_eq!(last.0 + last.1, main_allocator.next_indirection_id);
        assert_eq!(last.0 + last.1 - FIRST_COMPRESSED_PAGE, indirections.len());

        Ok(())
    }

    /// Persists any in-memory state of the database to its file.
    /// A checkpoint creates a new stable snapshot in the database file,
    /// releasing the corresponding entries from the Write-Ahead-Log (WAL).
    #[inline]
    pub fn checkpoint(&self) -> Result<(), Error> {
        self.checkpoint_internal(None)
    }

    fn checkpoint_internal(&self, up_to_tx_id: Option<TxId>) -> Result<(), Error> {
        let request_metapage = self.inner.state.lock().metapage;
        let up_to_tx_id = up_to_tx_id.unwrap_or(request_metapage.tx_id);
        if request_metapage.snapshot_tx_id >= up_to_tx_id {
            return Ok(());
        }
        self.inner
            .request_checkpoint(CheckpointReason::User(request_metapage.tx_id));
        loop {
            self.inner.wait_checkpoint();
            let latest_state = self.inner.state.lock();
            latest_state.check_halted()?;
            if latest_state.metapage.snapshot_tx_id >= request_metapage.tx_id {
                return Ok(());
            }
        }
    }

    fn initialize_fresh(&mut self) -> Result<(), Error> {
        debug!("Initializing data file");
        // Note that this must reset anything that load_from_data_file could have partially set
        let mut state = self.inner.state.lock();
        let wal_start = self.inner.env.wal.head();
        state.metapage = MetapageHeader {
            page_header: PageHeader {
                span: 1u16.into(),
                ..Default::default()
            },
            magic: METAPAGE_MAGIC.to_be_bytes(),
            trees_tree: TreeValue {
                min_branch_node_pages: 1,
                min_leaf_node_pages: 1,
                fixed_key_len: -1,
                fixed_value_len: -1,
                ..Default::default()
            },
            indirections_tree: TreeValue {
                min_branch_node_pages: 1,
                min_leaf_node_pages: 1,
                fixed_key_len: size_of::<PageId>() as i8,
                fixed_value_len: size_of::<IndirectionValue>() as i8,
                ..Default::default()
            },
            wal_start,
            wal_end: wal_start,
            ..Default::default()
        };
        self.inner.file.ensure_file_size(true, 2 * PAGE_SIZE)?;
        state.metapage.page_header.id = 0;
        self.inner.write_metapage(&state.metapage)?;
        state.metapage.page_header.id = 1;
        self.inner.write_metapage(&state.metapage)?;
        self.inner.file.file.sync_data()?;
        utils::sync_dir(&self.inner.opts.path)?;
        *self.inner.allocator.lock() = Default::default();
        self.inner.allocator.lock().next_page_id = 2;
        self.inner.allocator.lock().next_indirection_id = FIRST_COMPRESSED_PAGE;
        Ok(())
    }

    fn load_from_data_file(&mut self) -> Result<(), Error> {
        debug!("Loading state from data file");
        let mut state = self.inner.state.lock();
        let mut allocator = self.inner.allocator.lock();

        let latest_metapage = [0, 1]
            .iter()
            .filter_map(|page_id| match self.inner.read_metapage(*page_id) {
                Ok(mp) => Some((page_id, mp)),
                Err(e) => {
                    warn!("Error reading metapage {page_id}: {e}");
                    None
                }
            })
            .max_by_key(|(_, h)| h.tx_id);
        if let Some((page_id, metapage)) = latest_metapage {
            info!(
                "Loading database state from metapage {}, tx id {}",
                page_id, metapage.tx_id
            );
            state.metapage = metapage;
        } else {
            return Err(io_invalid_data!(
                "Data file irrecoverable, metapages are corrupt"
            ));
        };

        if state.metapage.wal_start != state.metapage.wal_end {
            return Err(io_invalid_data!(
                "WAL start/end don't match: {} != {}",
                state.metapage.wal_start,
                state.metapage.wal_end,
            ));
        }

        if state.metapage.freelist_root != PageId::default() {
            debug!(
                "Loading freelist from page {}",
                state.metapage.freelist_root
            );
            let (_fl_spans, fl_data) = self
                .inner
                .read_from_pages(state.metapage.freelist_root, true)?;
            debug!("Parsing freelist from {}", ByteSize(fl_data.len() as u64));
            *allocator = MainAllocator::from_bytes(&fl_data)
                .map_err(|e| io_invalid_data!("Error parsing freelist: {e}"))?;
            let expected_file_size = allocator.next_page_id as u64 * PAGE_SIZE;
            let file_len = self.inner.file.file_len();
            match file_len.cmp(&expected_file_size) {
                Ordering::Equal => (),
                Ordering::Less => {
                    return Err(io_invalid_data!(
                        "Data file is smaller than expected, expected at least {expected_file_size} bytes got {file_len}"
                    ));
                }
                Ordering::Greater => {
                    self.inner.file.ensure_file_size(true, expected_file_size)?;
                }
            }
        } else {
            allocator.next_page_id = 2;
            allocator.next_indirection_id = FIRST_COMPRESSED_PAGE;
        }
        drop(allocator);
        drop(state);
        Ok(())
    }

    /// Durably persists any pending changes to durable storage.
    /// This is equivalent to calling `sync` (aka. fsync) or `datasync` on a `File`.
    pub fn sync(&self) -> Result<(), Error> {
        self.inner.env.wal.sync()
    }

    /// Begins an Exclusive Write transaction. Equivalent to `begin_write_with(false)`.
    #[inline]
    pub fn begin_write(&self) -> Result<WriteTransaction, Error> {
        self.begin_write_with(false)
    }

    /// Begins a Concurrent Write transaction. Equivalent to `begin_write_with(true)`.
    ///
    /// See [`WriteTransaction`] for recommendations.
    #[inline]
    pub fn begin_write_concurrent(&self) -> Result<WriteTransaction, Error> {
        self.begin_write_with(true)
    }

    /// Begins a write transactions
    ///
    /// If `concurrent` is true, the returned transaction may run concurrently with other
    /// _concurrent_ multi write transactions. These transactions run with Snapshot-Isolation (SI)
    /// and commits with Optimistic Concurrency Control (OCC), see [`WriteTransaction`] for details.
    ///
    /// If `concurrent` is false, then the transactions runs in exclusive mode with Serializable-Snapshot-Isolation (SSI).
    ///
    /// Both concurrent modes can be mixed safely. See [`WriteTransaction`] for recommendations.
    ///  
    /// The returned transaction must be committed for the changes to be persisted.
    pub fn begin_write_with(&self, concurrent: bool) -> Result<WriteTransaction, Error> {
        let throttle_spans = self.inner.opts.throttle_memory_limit / PAGE_SIZE as usize;
        let mut tx = Transaction::new_write(&self.inner, concurrent, true)?;
        tx.release_versions(tx.inner.page_table.spans_used() >= throttle_spans);
        if tx.inner.page_table.spans_used() >= throttle_spans {
            // drop tx to avoid deadlocking with checkpoints
            drop(tx);
            info!("Throttling write tx");
            thread::sleep(Duration::from_micros(100));
            tx = Transaction::new_write(&self.inner, concurrent, true)?;
            while {
                tx.release_versions(true);
                tx.inner.page_table.spans_used()
                    >= tx.inner.opts.stall_memory_limit / PAGE_SIZE as usize
            } {
                // drop tx to avoid deadlocking with checkpoints
                drop(tx);
                // Attempt to start a MemoryPressure checkpoint
                self.inner
                    .request_checkpoint(CheckpointReason::MemoryPressure);
                // Wait a bit to improve the changes of the wait below seeing the checkpoint
                thread::sleep(Duration::from_micros(100));
                // Wait for the ongoing checkpoint to finish.
                self.inner.wait_checkpoint();
                tx = Transaction::new_write(&self.inner, concurrent, true)?;
            }
            debug!("Resuming write tx after throttling");
        }
        if tx.inner.opts.use_wal {
            let mut wb = tx
                .commit_lock
                .as_mut()
                .and_then(|s| s.wal_write_batch.take())
                .unwrap_or_else(|| WriteBatch::new(self.inner.env.opts.clone()));
            wb.push_db(self.inner.env_db_id)?;
            tx.wal_write_batch = Some(wb).into();
        }
        if let Err(e) = tx
            .inner
            .release_old_snapshots(tx.state.get().metapage.snapshot_tx_id)
        {
            error!("Error releasing snapshot freelist: {e}");
            self.inner.halt();
            return Err(e);
        }

        Ok(tx)
    }

    /// Begins a read-only transaction.
    ///
    /// The returned transaction contains stable snapshot of the last committed write transaction.
    pub fn begin_read(&self) -> Result<ReadTransaction, Error> {
        Transaction::new_read(&self.inner, true)
    }

    /// Compact (aka. defragment or vaccum) the database in-place by moving pages from the end of the file
    /// to free-space in the beginning.
    ///
    /// Note that this is both expensive (it might scan the entire database multiple times) and best effort.
    /// User transactions will block during the compaction process. It should be used sparingly to try to
    /// shrink the database file, like after bulk deletions.
    ///
    /// Algorithm description
    /// * Waits for any checkpoint and write transactions to finish and prevent new ones from being created.
    /// * Check if the file is large enough (approximately 400KB) to be compacted, and exit otherwise.
    /// * Waits for any read transactions to finish.
    /// * Prevent any user transactions from being created (includes read transactions).
    /// * Make sure any previous writes are in the data file and freespace is available for compaction.
    /// 1. Utilizing the freespace maps, find a point in the file where compaction could move at least 3%+
    ///    of the end of the file to free space before it. Due to fragmentation and parent pages distribution,
    ///    this is best-effort. Exit if such a point cannot be found.
    /// 2. Walk the database (all trees) moving the pages from the right of the compaction point to the left.
    /// 3. Perform 2 checkpoints. The first persists the newly compacted database to the data file and the
    ///    second makes the free space of by the first checkpoint (compaction) available for re-use.
    /// 4. If the file shrinked by at least 3%, go to step 1 again.
    pub fn compact(&self) -> Result<(), Error> {
        info!("Compacting database {}", self.inner.env_db_name);
        let checkpoint_lock = self.inner.checkpoint_lock.lock();
        let write_lock = self.inner.write_lock.write();
        let mut curr_file_size = self.inner.file.file_len();
        // With both checkpoint and write locks we can get a sense of the data file size
        const MIN_SIZE_FOR_COMPACTION: u64 = PAGE_SIZE * 100;
        if curr_file_size < MIN_SIZE_FOR_COMPACTION {
            info!(
                "Database {} is too small ({} < {}) to be compacted",
                ByteSize(curr_file_size),
                ByteSize(MIN_SIZE_FOR_COMPACTION),
                self.inner.env_db_name
            );
            return Ok(());
        }
        let mut transactions = self.inner.transactions.lock();
        self.inner.state.lock().block_user_transactions = true;
        while !transactions.is_empty() {
            info!("Compaction waiting for read transactions to finish...");
            self.inner.transactions_condvar.wait(&mut transactions);
        }
        // Due to the way that the checkpoint requests work (waits by acquiring the checkpoint lock)
        // we won't hold the checkpoint lock in here. While not ideal (concurrent background checkpoints),
        // it's ok.
        drop(checkpoint_lock);
        drop(transactions);
        drop(write_lock);

        let _reset_compacting = FnTrap::new(|| {
            let _transactions = self.inner.transactions.lock();
            self.inner.state.lock().block_user_transactions = false;
            self.inner.transactions_condvar.notify_all();
        });

        info!(
            "Compaction preparing database {}, initial size {}",
            self.inner.env_db_name,
            ByteSize(curr_file_size)
        );
        let mut min_file_size = curr_file_size;
        // up to 2 initial checkpoints to make all free space in flight available for compaction
        if !self.inner.allocator.lock().snapshot_free.is_empty() {
            self.force_checkpoint()?;
        }
        if !self.inner.old_snapshots.lock().is_empty() {
            self.force_checkpoint()?;
        }
        for pass in 1u64.. {
            info!(
                "Compaction pass {pass}, current size {}",
                ByteSize(curr_file_size)
            );
            let mut tx = Transaction::new_write(&self.inner, false, false)?;
            match tx.compact() {
                Ok(true) => (),
                Ok(false) | Err(Error::CantCompact) => break,
                Err(e) => return Err(e),
            }
            let tx_id = match tx.commit() {
                Ok(tx_id) => tx_id,
                Err(Error::CantCompact) => break,
                Err(e) => return Err(e),
            };
            self.checkpoint_internal(Some(tx_id + 1))?;
            // Theoretically one extra checkpoint would be sufficient, but the logic for file shrinking
            // in the allocator is quite limited and depends on the main checkpointer truncating the end.
            for _ in 0..2 {
                self.force_checkpoint()?;
            }
            curr_file_size = self.inner.file.file_len();
            // no further passes if the size wasn't reduced by at least 3%
            if curr_file_size >= min_file_size / 100 * 97 {
                break;
            }
            min_file_size = curr_file_size;
        }
        info!(
            "Compacted database {}, final size {}",
            self.inner.env_db_name,
            ByteSize(curr_file_size)
        );
        Ok(())
    }

    fn force_checkpoint(&self) -> Result<(), Error> {
        let mut metapage = self.inner.state.lock().metapage;
        if metapage.tx_id <= metapage.snapshot_tx_id {
            let tx = Transaction::new_write(&self.inner, false, false)?;
            tx.mark_dirty();
            metapage.tx_id = tx.commit()?;
        }
        self.checkpoint_internal(Some(metapage.tx_id))
    }
}

fn total_size_to_span(needed_size: usize) -> Result<PageId, Error> {
    let span = (size_of::<ReservedPageHeader>() + needed_size).div_ceil(PAGE_SIZE as usize);
    PageId::try_from(span).map_err(|_| Error::FreeList(String::from("Allocation too big").into()))
}

fn usable_size_to_noncontinuous_span(needed_size: usize) -> Result<PageId, Error> {
    const RESTRICTED_PAGE_SIZE: usize =
        PAGE_SIZE as usize - size_of::<PageHeader>() - size_of::<ReservedPageHeader>();
    let span = needed_size.div_ceil(RESTRICTED_PAGE_SIZE);
    PageId::try_from(span).map_err(|_| Error::FreeList(String::from("Allocation too big").into()))
}

impl WriteTransaction {
    /// Renames a tree from `old` to `new`.
    pub fn rename_tree(&self, old: &[u8], new: &[u8]) -> Result<bool, Error> {
        let guard = self.trap.setup()?;
        if self.get_tree_internal(old)?.is_none() {
            return Ok(false);
        }
        self.delete_tree(new)?;
        self.mark_dirty();
        let mut trees = self.trees.borrow_mut();
        let mut old_state = trees.get_mut(old).unwrap();
        let old_value = if let TreeState::Available { dirty, value, .. } = &mut old_state {
            *dirty = true;
            *value
        } else {
            unreachable!();
        };
        let old_state = mem::replace(old_state, TreeState::Deleted { value: old_value });
        let replaced_new_state = trees.insert(new.into(), old_state);
        debug_assert!(matches!(
            replaced_new_state,
            None | Some(TreeState::Deleted { .. })
        ));
        if let Some(batch) = &mut *self.wal_write_batch.borrow_mut() {
            batch.push_rename_tree(old, new)?;
        }
        guard.disarm();
        Ok(true)
    }

    /// Deletes the tree with the corresponding `name`.
    pub fn delete_tree(&self, name: &[u8]) -> Result<bool, Error> {
        let guard = self.trap.setup()?;
        if let Some(mut tree) = self.get_tree_internal(name)? {
            tree.clear()?;
        } else {
            guard.disarm();
            return Ok(false);
        };
        self.mark_dirty();
        let mut trees = self.trees.borrow_mut();
        let state = trees.get_mut(name).unwrap();
        let &mut TreeState::Available { value, .. } = state else {
            unreachable!();
        };
        *state = TreeState::Deleted { value };
        if let Some(batch) = &mut *self.wal_write_batch.borrow_mut() {
            batch.push_delete_tree(name)?;
        }
        guard.disarm();
        Ok(true)
    }

    /// Gets a [Tree] instance with the corresponding name (creating if necessary).
    #[inline]
    pub fn get_or_create_tree(&self, name: &[u8]) -> Result<Tree<'_>, Error> {
        self.get_or_create_tree_with(name, TreeOptions::default())
    }

    /// Gets a [Tree] instance with the corresponding name and options (creating if necessary).
    ///
    /// In case the [Tree] already exists the `options` arguments will be validated against
    /// the existing options for compatibility.
    pub fn get_or_create_tree_with(
        &self,
        name: &[u8],
        options: TreeOptions,
    ) -> Result<Tree<'_>, Error> {
        options.validate()?;
        let guard = self.trap.setup()?;
        if let Some(tree) = self.get_tree_internal(name)? {
            options.validate_value(&tree.value).guard_trap(guard)?;
            return Ok(tree);
        }
        self.create_tree(rand::random(), name, options)
            .guard_trap(guard)
    }

    fn create_tree(
        &self,
        tree_id: TreeId,
        name: &[u8],
        options: TreeOptions,
    ) -> Result<Tree<'_>, Error> {
        self.mark_dirty();
        if let Some(write_batch) = &mut *self.wal_write_batch.borrow_mut() {
            write_batch.push_create_tree(&tree_id, name, &options)?;
        }
        let name: Arc<[u8]> = name.into();
        let value = options.to_value(tree_id);
        self.trees
            .borrow_mut()
            .insert(name.clone(), TreeState::InUse { value });
        Ok(Tree {
            name: Some(name),
            value,
            tx: self,
            len_delta: 0,
            dirty: true,
            cached_root: Default::default(),
        })
    }

    /// Commits the transaction. See [Self::commit_with] for details.
    ///
    /// Equivalent to [Self::commit_with] with the sync argument from [DbOptions::default_commit_sync]
    #[inline]
    pub fn commit(self) -> Result<TxId, Error> {
        let sync = self.inner.opts.default_commit_sync;
        self.commit_with(sync)
    }

    /// Commits the transaction and optionally performs a durable `fsync` to make the transaction
    /// (and all the ones before it) durable in the storage. Returns the new TxId.
    ///
    /// If this is a concurrent write transaction commit will perform conflict checking and will return
    /// [`Error::WriteConflict`] in case of conflicts.
    ///
    /// Note that transactions that aren't dirty (see [WriteTransaction::is_dirty]) are equivalent
    /// to a rollback (nothing is affected) and the transaction will return the original TxId.
    pub fn commit_with(mut self, sync: bool) -> Result<TxId, Error> {
        debug_assert!(self.is_write_tx());
        if !self.is_dirty() {
            trace!("Transaction isn't dirty, commit is a rollback");
            return self.0.do_rollback().map(|_| self.tx_id());
        }
        self.0.commit_start()?;
        self.0.commit_wal(sync)?;
        self.0.commit_finish()?;
        Ok(self.tx_id())
    }

    /// Rolls back the transaction, discarding any changes
    ///
    /// Note that droping the [WriteTransaction] has the same effect but errors
    /// cannot be observed.
    #[inline]
    pub fn rollback(mut self) -> Result<(), Error> {
        debug_assert!(self.is_write_tx());
        self.0.do_rollback()
    }

    /// Returns whether the transation contains any changes.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }

    fn compact(&mut self) -> Result<bool, Error> {
        let guard = self.0.trap.setup()?;
        let end_of_file;
        let mut free_space;
        {
            // note that this only usable free space (e.g. not freespace from snapshots)
            let mut main_allocator = self.0.inner.allocator.lock();
            end_of_file = main_allocator.next_page_id;
            free_space = main_allocator.free.merged()?.clone();
            free_space.merge(&self.0.allocator.get_mut().free)?;
        }
        debug!(
            "Total freespace available {:?}",
            ByteSize(free_space.len() as u64 * PAGE_SIZE)
        );
        for (st, fl) in self.inner.old_snapshots.lock().iter() {
            debug!(
                "Snapshot {st} space available {:?}",
                ByteSize(fl.len() as u64 * PAGE_SIZE)
            );
        }
        debug_assert!(self.inner.allocator.lock().pending_free.is_empty());
        // figure out the amount of data that is moveable using binary search
        let mut lo = 0;
        let mut hi = end_of_file;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (free_before, free_after) = free_space.clone().split(mid);
            let (free_before_len, free_after_len) = (free_before.len(), free_after.len());
            let occupied_after = end_of_file - mid - free_after_len;
            // when calculating fit, assume 50% expansion due to fragmentation
            // and branches that have to be rewritten during compaction.
            if free_before_len <= occupied_after.saturating_add(occupied_after / 2) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // bail if less than 3% of the file can be compacted
        if lo >= end_of_file / 100 * 97 {
            return Ok(false);
        }

        self.mark_dirty();
        self.0.allocator.get_mut().is_compactor = true;
        for tree_name in self.list_trees()? {
            debug!("Compacting tree {}", EscapedBytes(&tree_name));
            self.get_tree(&tree_name)?.unwrap().compact(lo)?;
        }
        self.0.allocator.get_mut().is_compactor = false;
        guard.disarm();
        Ok(true)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.is_write_or_checkpoint_txn() && !self.is_done() {
            let _ = self.do_rollback();
        }
        if let Some(tx_id) = self.tracked_transaction {
            let mut transactions = self.inner.transactions.lock();
            let idx = transactions
                .binary_search_by_key(&tx_id, |ot| ot.tx_id)
                .expect("missing transaction");
            let ot = &mut transactions[idx];
            ot.writers -= self.is_multi_write_tx() as u32;
            ot.ref_count -= 1;
            let removed_tx = ot.ref_count == 0;
            if removed_tx {
                transactions.remove(idx);
            }
            let earliest_open_read_tx = transactions.first().map(|ot| ot.tx_id);
            let earliest_tx_needed = earliest_open_read_tx.or_else(|| {
                // The state read must be done while holding the transactions lock
                // to avoid a new read transaction from being created at the same tx-id as this (X) .
                // While this drop will drop the transactions lock and reads a _newly_ committed
                // tx-id from state (X+1), then proceed to remove the buffers needed by X.
                self.inner
                    .state
                    .try_lock()
                    .map(|state| state.metapage.tx_id)
            });
            if earliest_open_read_tx.is_none() {
                self.inner.transactions_condvar.notify_all();
            }
            drop(transactions);

            match earliest_tx_needed {
                Some(earliest) if earliest > tx_id => {
                    trace!("Calling release_versions_tail from tx {tx_id} drop, earliest tx needed {earliest}");
                    self.release_versions_tail(earliest, false);
                }
                _ if removed_tx => {
                    let mut free_buffers = self.inner.free_buffers.lock();
                    free_buffers.scan_from = free_buffers.scan_from.min(tx_id);
                }
                _ => (),
            }
        }

        // This txn might be the last strong database reference and thus last env reference
        let _maybe_last_env = self.env_handle.take().and_then(|env_handle| {
            // 1 ref here and 1 ref in the bg thread
            if StdArc::strong_count(&self.inner) <= 2 {
                Some(EnvironmentInner::upgrade(
                    self.inner.env.clone(),
                    env_handle,
                ))
            } else {
                None
            }
        });
        // ManuallyDrop required since we want to drop maybe_last_env after the SharedDatabaseInner
        unsafe {
            ManuallyDrop::drop(&mut self.inner);
        }
    }
}

impl Transaction {
    fn new_write(
        inner: &SharedDatabaseInner,
        multi: bool,
        user_txn: bool,
    ) -> Result<WriteTransaction, Error> {
        let mut exclusive_write_lock = None;
        let mut multi_write_lock = None;
        let mut transactions;
        let mut state;
        let mut waited = false;
        loop {
            if multi {
                multi_write_lock = Some(inner.write_lock.read_arc());
            } else {
                exclusive_write_lock = Some(inner.write_lock.write_arc());
            }
            transactions = inner.transactions.lock();
            state = *inner.state.lock();
            state.check_halted()?;
            if !user_txn || !state.block_user_transactions {
                if waited {
                    debug!("new_write resuming after user_transactions are allowed");
                }
                break;
            }
            exclusive_write_lock = None;
            multi_write_lock = None;
            debug!("new_write waiting for user_transactions to be allowed");
            waited = true;
            inner.transactions_condvar.wait(&mut transactions);
            drop(transactions);
        }
        let trees;
        let nodes;
        let scratch_buffer;
        let tracked_transaction;
        let flags;
        let commit_lock = if multi {
            match transactions.last_mut() {
                Some(l) if l.tx_id == state.metapage.tx_id => {
                    l.ref_count += 1;
                    l.writers += 1;
                }
                _ => {
                    transactions.push(OpenTxn {
                        tx_id: state.metapage.tx_id,
                        ref_count: 1,
                        writers: 1,
                        earliest_snapshot_tx_id: state.metapage.snapshot_tx_id,
                    });
                }
            }
            drop(transactions);
            tracked_transaction = Some(state.metapage.tx_id);
            flags = TF::WRITE_TX | TF::MULTI_WRITE_TX;
            trees = None;
            nodes = None;
            scratch_buffer = Default::default();
            None
        } else {
            drop(transactions);
            tracked_transaction = None;
            flags = TF::WRITE_TX;
            let mut commit_lock = inner.commit_lock.lock_arc();
            trees = commit_lock.trees.take();
            nodes = commit_lock.nodes.take();
            scratch_buffer = mem::take(&mut commit_lock.scratch_buffer);
            Some(commit_lock)
        };
        trace!(
            "new_write {} multi {multi:?} user {user_txn:?}",
            state.metapage.tx_id
        );
        let allocator =
            Allocator::new_transaction(inner, !multi && state.ongoing_snapshot_tx_id.is_none())?;
        Ok(WriteTransaction(Transaction {
            flags: Cell::new(flags),
            trap: Default::default(),
            allocator: RefCell::new(allocator),
            inner: ManuallyDrop::new(inner.clone()),
            state: Cell::new(state),
            trees: RefCell::new(trees.unwrap_or_default()),
            nodes: RefCell::new(nodes.unwrap_or_else(|| new_dirty_cache(&inner.opts))),
            scratch_buffer: RefCell::new(scratch_buffer),
            tracked_transaction,
            nodes_spilled_span: Default::default(),
            wal_write_batch: Default::default(),
            exclusive_write_lock,
            multi_write_lock,
            commit_lock,
            env_handle: user_txn.then(|| inner.open.lock().clone()).flatten(),
        }))
    }

    fn new_read(inner: &SharedDatabaseInner, user_txn: bool) -> Result<ReadTransaction, Error> {
        let mut state;
        // Transactions need to be locked before we take the state snapshot to ensure
        // that it's not possible:
        // * for a newly started write txn to not see this read txn. As even if it advances the state first,
        // once it comes around to read the transactions, the lock will be held here.
        // * for this state acquisition to not notice the state.block_user_transactions which is updated
        // while holding all locks (inc. transactions).
        let mut transactions = inner.transactions.lock();
        let mut waited = false;
        loop {
            state = *inner.state.lock();
            if !user_txn || !state.block_user_transactions {
                if waited {
                    debug!("new_write resuming after user_transactions are allowed");
                }
                break;
            }
            debug!("new_read waiting for user_transactions to be allowed");
            waited = true;
            inner.transactions_condvar.wait(&mut transactions);
        }
        match transactions.last_mut() {
            Some(l) if l.tx_id == state.metapage.tx_id => l.ref_count += 1,
            _ => {
                transactions.push(OpenTxn {
                    tx_id: state.metapage.tx_id,
                    ref_count: 1,
                    writers: 0,
                    earliest_snapshot_tx_id: state.metapage.snapshot_tx_id,
                });
            }
        }
        drop(transactions);
        trace!("new_read {}", state.metapage.tx_id);
        Ok(ReadTransaction(Transaction {
            trap: Default::default(),
            flags: Default::default(),
            inner: ManuallyDrop::new(inner.clone()),
            state: Cell::new(state),
            trees: Default::default(),
            nodes: RefCell::new(void_dirty_cache()),
            nodes_spilled_span: Default::default(),
            wal_write_batch: None.into(),
            exclusive_write_lock: None,
            multi_write_lock: None,
            commit_lock: None,
            tracked_transaction: Some(state.metapage.tx_id),
            allocator: RefCell::new(Allocator::default()),
            scratch_buffer: Default::default(),
            env_handle: user_txn.then(|| inner.open.lock().clone()).flatten(),
        }))
    }

    fn convert_read_to_checkpoint(&mut self, _num_reserved_pages: PageId) -> Result<(), Error> {
        debug_assert!(self.flags.get().is_empty());
        self.flags.get_mut().insert(TF::CHECKPOINT_TX | TF::DIRTY);
        *self.nodes.get_mut() = DirtyNodes::with(
            usize::MAX,
            u64::MAX,
            Default::default(),
            Default::default(),
            Default::default(),
        );
        *self.allocator.get_mut() = Allocator::new_checkpoint(&self.inner)?;
        Ok(())
    }

    fn get_trees_tree(&self) -> Tree {
        Tree {
            name: Default::default(),
            value: self.state.get().metapage.trees_tree,
            tx: self,
            len_delta: 0,
            dirty: false,
            cached_root: Default::default(),
        }
    }

    fn get_indirection_tree(&self) -> Tree {
        Tree {
            name: Default::default(),
            value: self.state.get().metapage.indirections_tree,
            tx: self,
            len_delta: 0,
            dirty: false,
            cached_root: Default::default(),
        }
    }

    fn get_indirect_page_id(&self, id: PageId) -> Result<(PageId, PageId), Error> {
        let Some(v) = self.get_indirection_tree().get(&id.to_be_bytes())? else {
            return Err(io_invalid_input!(
                "Compressed page {id} not found in indirections map"
            ));
        };
        let v = Ref::<_, IndirectionValue>::new(v.as_ref())
            .ok_or_else(|| io_invalid_data!("Invalid indirection value length"))?
            .into_ref();
        Ok((v.pid, PageId::from(v.span)))
    }

    fn read_compressed_page(
        &self,
        id: PageId,
        compressed_page_id: Option<(PageId, PageId)>,
    ) -> Result<Page, Error> {
        trace!("read_compressed page {id} {compressed_page_id:?}");
        assert!(id.is_compressed());
        let (compressed_page_id, compressed_span) = {
            if let Some(compressed_page_id) = compressed_page_id {
                compressed_page_id
            } else {
                self.get_indirect_page_id(id)?
            }
        };

        self.inner.read_page_at(
            compressed_page_id,
            compressed_page_id,
            Some(compressed_span),
        )
    }

    fn read_compressed_page_uncompressed(
        &self,
        id: PageId,
        compressed_page_id: Option<(PageId, PageId)>,
    ) -> Result<Page, Error> {
        let mut compressed_page = self.read_compressed_page(id, compressed_page_id)?;
        if !compressed_page
            .header()
            .flags
            .contains(PageFlags::Compressed)
        {
            compressed_page.compressed_page = Some((compressed_page.id(), compressed_page.span()));
            compressed_page.header_mut().id = id;
            return Ok(compressed_page);
        }

        let compressed_header = header_cast::<CompressedPageHeader, _>(&compressed_page);
        #[cfg(not(fuzzing))]
        let decompressed = {
            let mut bytes = Bytes::new_zeroed(compressed_header.uncompressed_len as usize);
            lz4_flex::decompress_into(
                &compressed_page.data()[size_of::<CompressedPageHeader>()..]
                    [..compressed_header.compressed_len as usize],
                bytes.as_mut(),
            )
            .map_err(|e| io_invalid_data!("Error decompressing page {id}: {e}"))?;
            bytes
        };
        #[cfg(fuzzing)]
        let decompressed = Bytes::from_slice(
            &compressed_page.data()[size_of::<CompressedPageHeader>()..]
                [..compressed_header.compressed_len as usize],
        );

        Ok(Page {
            compressed_page: Some((compressed_page.id(), compressed_page.span())),
            dirty: false,
            raw_data: decompressed,
        })
    }

    fn allocate_page_id(
        &self,
        allocator: &mut Allocator,
        span: PageId,
        compressed: bool,
    ) -> Result<PageId, Error> {
        debug_assert!(span > 0);
        trace!("allocate_page_id {span} compressed {compressed:?}");
        if compressed && span as u64 * PAGE_SIZE >= MIN_PAGE_COMPRESSION_BYTES {
            let page_id = allocator.allocate_indirection()?;
            trace!("allocated indirection {page_id}");
            Ok(page_id)
        } else {
            let page_id = allocator.allocate(span)?;
            trace!("allocated {page_id}");
            Ok(page_id)
        }
    }

    fn allocate_page(&self, span: PageId, compressed: bool) -> Result<Page, Error> {
        let id = self.allocate_page_id(&mut self.allocator.borrow_mut(), span, compressed)?;
        let page = Page::new(id, true, span);
        let spilled = self
            .nodes
            .borrow_mut()
            .insert_with_lifecycle(id, TxNode::Popped(span as u64 * PAGE_SIZE));
        self.spill_dirty_nodes(spilled)?;
        Ok(page)
    }

    #[inline]
    fn read_clean_node(&self, id: PageId) -> Result<UntypedNode, Error> {
        self.read_clean_node_opt(id, None, true)
    }

    fn read_clean_node_opt(
        &self,
        id: PageId,
        mut redirect: Option<(TxId, (PageId, PageId))>,
        use_page_table: bool,
    ) -> Result<UntypedNode, Error> {
        trace!("read_clean_node {id}");
        debug_assert!(id > 1);
        // TODO: can we check for? self.tx_id() > self.state.get().metapage.snapshot_tx_id
        if use_page_table {
            redirect = match self.inner.page_table.get(self.tx_id(), id) {
                Some((_, Item::Page(page))) => return Node::from_page(page),
                Some((_, Item::Redirected(r_pid, r_span, true))) => Some((0, (r_pid, r_span))),
                Some((from, Item::Redirected(r_pid, r_span, false))) => {
                    Some((from, (r_pid, r_span)))
                }
                None => None,
            };
        }
        let nck = NodeCacheKey::new(self.inner.env_db_id, redirect.map_or(0, |r| r.0), id);
        if let Some(node) = self.inner.env.shared_cache.get(&nck) {
            return Ok(node);
        }
        let page = if id.is_compressed() {
            self.read_compressed_page_uncompressed(id, redirect.map(|r| r.1))?
        } else {
            self.inner.read_page_at(
                id,
                redirect.map_or(id, |r| r.1 .0),
                redirect.map(|r| r.1 .1),
            )?
        };
        let node = Node::from_page(page)?;
        debug_assert!(!node.dirty);
        self.inner.env.shared_cache.insert(nck, node.clone());
        Ok(node)
    }

    /// Clone a node for reading
    fn clone_node(&self, id: PageId) -> Result<UntypedNode, Error> {
        trace!("clone_node {id}");
        if self.is_write_or_checkpoint_txn() {
            let mut nodes = self.nodes.borrow_mut();
            let mut tx_node = nodes.get_mut(&id);
            match tx_node.as_deref_mut() {
                None => (),
                Some(TxNode::Stashed(node)) => {
                    debug_assert!(node.dirty, "{id} was stashed clean");
                    return Ok(node.clone());
                }
                Some(
                    tx_node @ &mut TxNode::Spilled {
                        span,
                        compressed_page,
                    },
                ) => {
                    let node = self.read_spilled_node(id, span, compressed_page)?;
                    *tx_node = TxNode::Stashed(node.clone());
                    return Ok(node);
                }
                Some(TxNode::Popped(..)) => panic!("{id} is popped"),
                Some(TxNode::Freed { .. }) => panic!("{id} is freed"),
            }
        }
        self.read_clean_node(id)
    }

    /// Take out a node for mutation, it must be freed or stashed afterwards
    fn pop_node(&self, id: PageId) -> Result<UntypedNode, Error> {
        trace!("pop_node {id}");
        if self.is_write_or_checkpoint_txn() {
            let popped = self.nodes.borrow_mut().peek_mut(&id).map(|mut n| {
                let weight = DirtyNodeWeighter::weight(&n);
                mem::replace(&mut *n, TxNode::Popped(weight))
            });
            match popped {
                None => (),
                Some(TxNode::Stashed(node)) => {
                    debug_assert!(node.dirty, "{id} was stashed clean");
                    return Ok(node);
                }
                Some(TxNode::Spilled {
                    span,
                    compressed_page,
                }) => return self.read_spilled_node(id, span, compressed_page),
                Some(TxNode::Popped(..)) => panic!("{id} is already popped"),
                Some(TxNode::Freed { .. }) => panic!("{id} freed"),
            }
        }
        self.read_clean_node(id)
    }

    #[cold]
    fn read_spilled_node(
        &self,
        id: PageId,
        _span: PageId,
        compressed_page: Option<(PageId, PageId)>,
    ) -> Result<UntypedNode, Error> {
        trace!("read_spilled_node {id} {compressed_page:?}");
        let mut node = self.read_clean_node_opt(id, compressed_page.map(|a| (0, a)), false)?;
        let mut node_mut = node.make_dirty();
        if let Some((c_pid, c_span)) = compressed_page {
            debug_assert_eq!(node_mut.page_mut().compressed_page, Some((c_pid, c_span)));
            node_mut.page_mut().compressed_page = None;
            self.allocator.borrow_mut().free.free(c_pid, c_span)?;
            self.nodes_spilled_span.reset(|n| n - c_span);
        } else {
            self.nodes_spilled_span.reset(|n| n - node_mut.span());
        }
        Ok(node)
    }

    #[inline]
    fn stash_node<TYPE: NodeType>(&self, node: Node<TYPE>) -> Result<(), Error> {
        trace!("stash node {} {:?}", node.id(), node.dirty);
        debug_assert!(node.id() > 1);
        debug_assert!(node.span() > 0);
        if !node.dirty {
            return Ok(());
        }
        debug_assert!(self.is_write_or_checkpoint_txn());
        let mut nodes = self.nodes.borrow_mut();
        let mut slot = nodes.get_mut(&node.id()).expect("dirty slot missing");
        debug_assert!(matches!(*slot, TxNode::Popped(..)), "slot wasn't popped");
        *slot = TxNode::Stashed(Node::into_untyped(node));
        Ok(())
    }

    #[inline]
    fn spill_dirty_nodes(&self, evicted: SmallVec<TxNode, 1>) -> Result<(), Error> {
        if evicted.is_empty() {
            Ok(())
        } else {
            self.spill_dirty_nodes_cold(evicted)
        }
    }

    #[cold]
    fn spill_dirty_nodes_cold(&self, evicted: SmallVec<TxNode, 1>) -> Result<(), Error> {
        assert!(self.is_write_tx());
        assert!(!evicted.is_empty());
        if self.nodes_spilled_span.get() == 0 {
            info!("Transaction will spill");
        }
        let mut allocator = self.allocator.borrow_mut();
        for tx_node in evicted {
            let mut node = if let TxNode::Stashed(node) = tx_node {
                debug!("Spilling dirty page {} span {}", node.id(), node.span());
                node
            } else {
                unreachable!()
            };

            let pid = node.id();
            let write_page = if pid.is_compressed() {
                let mut comp_page = DatabaseInner::compress_page(&mut allocator, &node)?;
                comp_page.set_checksum(self.inner.opts.env.use_checksums);
                let compressed_page = Some((comp_page.id(), comp_page.span()));
                let mut node_mut = node.as_dirty();
                node_mut.page_mut().compressed_page = compressed_page;
                node_mut.page_mut().dirty = false;
                *self.nodes.borrow_mut().peek_mut(&pid).unwrap() = TxNode::Spilled {
                    span: node.span(),
                    compressed_page,
                };
                Cow::Owned(comp_page)
            } else {
                let mut node_mut = node.as_dirty();
                node_mut
                    .page_mut()
                    .set_checksum(self.inner.opts.env.use_checksums);
                node_mut.page_mut().dirty = false;
                Cow::Borrowed(&*node)
            };

            self.nodes_spilled_span.reset(|n| n + write_page.span());
            self.inner
                .file
                .ensure_file_size(false, allocator.main_next_page_id as u64 * PAGE_SIZE)?;
            self.inner.write_page(&write_page)?;
            let nck = NodeCacheKey::new(self.inner.env_db_id, 0, pid);
            let _ = self.inner.env.shared_cache.replace(nck, node, false);
        }
        Ok(())
    }

    #[inline]
    fn is_page_part_of_snapshot(&self, page_id: PageId) -> bool {
        self.inner.page_table.is_page_from_snapshot(
            self.tx_id(),
            self.state.get().ongoing_snapshot_tx_id,
            page_id,
        )
    }

    fn free_snapshot_page(
        &self,
        allocator: &mut Allocator,
        page_id: PageId,
        span: PageId,
        compressed_page: Option<(PageId, PageId)>,
    ) -> Result<(), Error> {
        trace!("free_snapshot_page {page_id} ({span}) {compressed_page:?}");
        let tx_id = self.tx_id();
        let ongoing_snapshot_tx_id = self.state.get().ongoing_snapshot_tx_id;
        let is_multi_write = self.is_multi_write_tx();
        let shadowed =
            self.inner
                .page_table
                .insert_w_shadowed(tx_id, page_id, None, is_multi_write);
        let (target, redirected) = match shadowed {
            Some((_from, Item::Page(_))) => {
                // since this was already determined to be a checkpoint page it can only be for
                // the ongoing checkpoint.
                (&mut allocator.next_snapshot_free, None)
            }
            Some((from, Item::Redirected(c_pid, c_span, r_latest))) => {
                // buffer free handled by checkpointer
                debug_assert!(r_latest);
                debug_assert!(page_id.is_compressed());
                let target = if ongoing_snapshot_tx_id.is_some_and(|ockp| from <= ockp) {
                    &mut allocator.next_snapshot_free
                } else {
                    &mut allocator.snapshot_free
                };
                (target, Some((c_pid, c_span)))
            }
            None => {
                if is_multi_write {
                    allocator.buffer_free.push(FreePage(tx_id, page_id));
                }
                (&mut allocator.snapshot_free, None)
            }
        };

        if page_id.is_compressed() {
            if compressed_page.is_some() && redirected.is_some() {
                debug_assert_eq!(compressed_page, redirected);
            }
            target.free(page_id, 1)?;
            if let Some((c_pid, c_span)) = compressed_page.or(redirected) {
                target.free(c_pid, c_span)?;
            }
        } else {
            debug_assert_eq!(compressed_page.or(redirected), None);
            target.free(page_id, span)?;
        }
        Ok(())
    }

    #[inline]
    fn make_dirty<'node, TYPE: NodeType>(
        &self,
        node: &'node mut Node<TYPE>,
    ) -> Result<DirtyNode<'node, TYPE>, Error> {
        if node.dirty {
            Ok(node.as_dirty())
        } else {
            self.make_clean_dirty_internal(node)
        }
    }

    fn make_clean_dirty_internal<'node, TYPE: NodeType>(
        &self,
        node: &'node mut Node<TYPE>,
    ) -> Result<DirtyNode<'node, TYPE>, Error> {
        trace!("make_clean_dirty {} {:?}", node.id(), node.compressed_page);
        debug_assert!(!node.dirty);
        debug_assert!(
            self.nodes.borrow().peek(&node.id()).is_none(),
            "{} is clean and also being tracked",
            node.id()
        );
        let mut node_mut = node.make_dirty();
        if self.is_page_part_of_snapshot(node_mut.id()) {
            trace!("marking {} as freed", node_mut.id());
            let compressed_page = node_mut.page_mut().compressed_page.take();
            let spilled = self.nodes.borrow_mut().insert_with_lifecycle(
                node_mut.id(),
                TxNode::Freed {
                    from_snapshot: true,
                    span: node_mut.span(),
                    compressed_page,
                },
            );
            self.spill_dirty_nodes(spilled)?;
            node_mut.node_header_mut().page_header.id = self.allocate_page_id(
                &mut self.allocator.borrow_mut(),
                node_mut.span(),
                node_mut.id().is_compressed(),
            )?;
        }

        let spilled = self
            .nodes
            .borrow_mut()
            .insert_with_lifecycle(node_mut.id(), TxNode::Popped(node_mut.page_size() as u64));
        self.spill_dirty_nodes(spilled)?;
        Ok(node_mut)
    }

    #[cold]
    fn free_page_with_id(&self, page_id: PageId, span: PageId) -> Result<(), Error> {
        trace!("free_page_with_id {page_id} ({span})");
        let old_tx_node = {
            self.nodes.borrow_mut().peek_mut(&page_id).map(|mut n| {
                let weight = DirtyNodeWeighter::weight(&n);
                mem::replace(&mut *n, TxNode::Popped(weight))
            })
        };
        let (dirty, compressed_page) = match old_tx_node {
            None => (
                false,
                if page_id.is_compressed() {
                    self.read_compressed_page_details(page_id)?
                } else {
                    None
                },
            ),
            Some(TxNode::Stashed(node)) => (node.dirty, node.compressed_page),
            Some(TxNode::Spilled {
                compressed_page, ..
            }) => (true, compressed_page),
            Some(TxNode::Freed { .. }) => panic!("dirty page {page_id} is already freed"),
            Some(TxNode::Popped(..)) => panic!("dirty page {page_id} is popped"),
        };
        let from_snapshot = self.free_page_internal(dirty, page_id, span, compressed_page)?;
        let spilled = self.nodes.borrow_mut().insert_with_lifecycle(
            page_id,
            TxNode::Freed {
                from_snapshot,
                span,
                compressed_page,
            },
        );
        self.spill_dirty_nodes(spilled)?;

        Ok(())
    }

    fn read_compressed_page_details(&self, id: PageId) -> Result<Option<(PageId, PageId)>, Error> {
        debug_assert!(id.is_compressed());
        debug_assert!(self.is_write_tx());
        match self.inner.page_table.get(self.tx_id(), id) {
            Some((_, Item::Page(page))) => {
                debug_assert!(!page.dirty);
                return Ok(page.compressed_page);
            }
            Some((_, Item::Redirected(r_pid, r_span, r_latest))) => {
                debug_assert!(r_latest);
                debug_assert!(id.is_compressed());
                return Ok(Some((r_pid, r_span)));
            }
            None => (),
        }

        let nck = NodeCacheKey::new(self.inner.env_db_id, 0, id);
        if let Some(node) = self.inner.env.shared_cache.peek(&nck) {
            debug_assert!(!node.dirty);
            return Ok(node.compressed_page);
        }
        self.get_indirect_page_id(id).map(Some)
    }

    fn free_page_internal(
        &self,
        dirty: bool,
        page_id: PageId,
        span: PageId,
        compressed_page: Option<(PageId, PageId)>,
    ) -> Result<bool, Error> {
        trace!(
            "free_page_internal {} span {} dirty {:?} compressed {:?}",
            page_id,
            span,
            dirty,
            compressed_page
        );
        debug_assert!(page_id > 1);
        debug_assert!(span > 0);
        debug_assert!(!(dirty && compressed_page.is_some()));
        let mut allocator = self.allocator.borrow_mut();
        let from_snapshot = !dirty && self.is_page_part_of_snapshot(page_id);
        if !from_snapshot {
            if page_id.is_compressed() {
                allocator.indirection_free.free(page_id, 1)?;
                if let Some((c_pid, c_span)) = compressed_page {
                    allocator.free.free(c_pid, c_span)?;
                }
            } else {
                allocator.free.free(page_id, span)?;
            }
        }
        Ok(from_snapshot)
    }

    fn free_page(&self, page: &Page) -> Result<(), Error> {
        trace!("free_page {} {:?}", page.id(), page.compressed_page);
        let from_snapshot =
            self.free_page_internal(page.dirty, page.id(), page.span(), page.compressed_page)?;
        let free_tx_node = TxNode::Freed {
            from_snapshot,
            span: page.span(),
            compressed_page: page.compressed_page,
        };
        if !page.dirty {
            let mut nodes_mut = self.nodes.borrow_mut();
            // Normally inserting a 0 weight item will never evict anything,
            // but the cache may be overweight due to a previous get_mut/peek_mut.
            debug_assert!(nodes_mut.peek(&page.id()).is_none());
            let spilled = nodes_mut.insert_with_lifecycle(page.id(), free_tx_node);
            drop(nodes_mut);
            self.spill_dirty_nodes(spilled)
        } else {
            let mut nodes_mut = self.nodes.borrow_mut();
            match nodes_mut.peek_mut(&page.id()).as_deref_mut() {
                Some(v @ TxNode::Popped(..)) => *v = free_tx_node,
                Some(TxNode::Spilled { .. }) => panic!("Freeing spilled {}", page.id()),
                Some(TxNode::Stashed(_)) => panic!("{} is still stashed", page.id()),
                Some(TxNode::Freed { .. }) => panic!("Freeing already freed {}", page.id()),
                None => panic!("{} was never popped", page.id()),
            }
            Ok(())
        }
    }

    fn do_rollback(&mut self) -> Result<(), Error> {
        debug_assert!(self.is_write_or_checkpoint_txn());
        trace!("Rolling back txn {}", self.tx_id());
        // failures after setting DONE are fatal
        self.flags.get_mut().insert(TF::DONE);
        // if we failed in commit dirty we may have cleanup to do here
        if self.flags.get_mut().contains(TF::PAGE_TABLE_DIRTY) {
            self.inner.page_table.clear_latest_tx(self.tx_id());
        }
        // even if it's clean it may have reservations
        match self.allocator.get_mut().rollback() {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Rollback error: {e}");
                self.inner.halt();
                Err(e)
            }
        }
    }

    fn commit_dirty_nodes(&mut self) -> Result<(), Error> {
        self.flags.get_mut().insert(TF::PAGE_TABLE_DIRTY);
        let tx_id = self.tx_id();
        let is_multi_write = self.is_multi_write_tx();
        let mut nodes = self.nodes.borrow_mut();
        let allocator = &mut self.allocator.borrow_mut();
        allocator.buffer_free.reserve_exact(nodes.len());
        for (page_id, node) in nodes.drain() {
            let shadowed = match node {
                TxNode::Stashed(node) => {
                    trace!("commit_dirty_nodes insert {}", node.id());
                    debug_assert!(node.dirty);
                    debug_assert!(node.num_keys() != 0, "Invalid node {node:#?}");
                    let mut page = node.into_page();
                    page.dirty = false;
                    self.inner
                        .page_table
                        .insert(tx_id, page.id(), Item::Page(page), false)
                }
                TxNode::Spilled {
                    compressed_page, ..
                } => {
                    trace!("commit_dirty_nodes spilled {page_id}");
                    if let Some((c_pid, c_span)) = compressed_page {
                        self.inner.page_table.insert(
                            tx_id,
                            page_id,
                            Item::Redirected(c_pid, c_span, true),
                            false,
                        )
                    } else {
                        self.inner
                            .page_table
                            .insert(tx_id, page_id, None, is_multi_write)
                            .or(is_multi_write.then_some(tx_id))
                    }
                }
                TxNode::Freed {
                    from_snapshot,
                    span,
                    compressed_page,
                } => {
                    trace!("commit_dirty_nodes freed {page_id}");
                    if from_snapshot {
                        self.free_snapshot_page(allocator, page_id, span, compressed_page)?;
                        None
                    } else {
                        self.inner
                            .page_table
                            .insert(tx_id, page_id, None, is_multi_write)
                            .or(is_multi_write.then_some(tx_id))
                    }
                }
                TxNode::Popped(..) => unreachable!("page {page_id} isn't stashed"),
            };
            if let Some(from) = shadowed {
                allocator.buffer_free.push(FreePage(from, page_id));
            }
        }
        Ok(())
    }

    /// Releases all buffers and pending free pages from transactions <= earliest_tx
    fn release_versions_tail(&self, earliest_tx: TxId, can_block: bool) {
        trace!("release_versions_tail earliest_tx {earliest_tx} can_block {can_block:?}");
        {
            let mut main = self.inner.allocator.lock();
            let main = &mut *main;
            let to_drain_count = main
                .pending_free
                .iter()
                .take_while(|(t, ..)| *t <= earliest_tx)
                .count();
            for (_, free, ind_free) in main.pending_free.drain(..to_drain_count) {
                main.free.append(free);
                main.indirection_free.append(ind_free);
            }
        }

        let mut released = SmallVec::<_, 3>::new();
        {
            let mut free_buffers = if can_block {
                self.inner.free_buffers.lock()
            } else if let Some(guard) = self.inner.free_buffers.try_lock() {
                guard
            } else {
                return;
            };
            while let Some(pending) = free_buffers.free.first_entry() {
                if *pending.key() <= earliest_tx {
                    released.push((*pending.key(), pending.remove()));
                } else {
                    break;
                }
            }
        }
        for (_releasing_tx_id, pending) in released {
            trace!(
                "releasing {} buffers freed by txn {_releasing_tx_id}",
                pending.len()
            );
            for FreePage(from_tx_id, page_id) in pending {
                self.inner.page_table.remove_at(from_tx_id, page_id);
            }
        }
    }

    fn release_versions(&self, agressive: bool) {
        trace!("release_versions agressive {agressive:?}");
        debug_assert!(self.is_write_tx());

        let mut earliest_tx_needed = self.tx_id();
        // Drop of old Read/Checkpoint transactions is responsible for
        // tail cleanup, we only have to attempt it here if there aren't any.
        if !self.is_multi_write_tx() && self.inner.transactions.lock().is_empty() {
            self.release_versions_tail(earliest_tx_needed, agressive);
        }

        if !agressive {
            return;
        }
        let mut to_free = SmallVec::<FreePage, 15>::new();
        let mut emptied = SmallVec::<TxId, 7>::new();
        let locked_transactions = self.inner.transactions.lock();
        let mut open_transactions =
            SmallVec::<TxId, 12>::from_iter(locked_transactions.iter().map(|ot| ot.tx_id))
                .into_iter()
                .peekable();
        if self.is_multi_write_tx() {
            earliest_tx_needed = locked_transactions
                .iter()
                .find_map(|ot| (ot.writers != 0).then_some(ot.tx_id))
                .unwrap_or(earliest_tx_needed);
        }
        drop(locked_transactions);
        // Last open tx <= releasing_tx_id
        let mut last_open_tx_lte = 0;
        let mut locked_free_buffers = self.inner.free_buffers.lock();
        let free_buffers = &mut *locked_free_buffers;
        for (&releasing_tx_id, pending) in free_buffers.free.range_mut(free_buffers.scan_from..) {
            if releasing_tx_id > earliest_tx_needed {
                break;
            }
            while let Some(&t) = open_transactions.peek() {
                if t <= releasing_tx_id {
                    last_open_tx_lte = t;
                    open_transactions.next();
                } else {
                    break;
                }
            }
            // visible_from inside pending is <= releasing_tx_id and can be removed if `visible_from > last_open_tx`.
            // Thus the buffer can be removed if `visible_from` is between `previous_tx_id(ex) and releasing_tx_id(ex)`.
            // We can detect if that range is empty and bail early.
            if last_open_tx_lte + 1 >= releasing_tx_id {
                continue;
            }
            to_free.extend(utils::vec_drain_if(
                pending,
                |&FreePage(visible_from, _)| visible_from > last_open_tx_lte,
            ));
            if pending.is_empty() {
                emptied.push(releasing_tx_id);
            }
        }
        for releasing_tx_id in emptied {
            free_buffers.free.remove(&releasing_tx_id);
        }
        free_buffers.scan_from = earliest_tx_needed + 1;
        drop(locked_free_buffers);
        for FreePage(from_tx_id, page_id) in to_free {
            self.inner.page_table.remove_at(from_tx_id, page_id);
        }
    }

    /// The _original_ Id of this transaction
    ///
    /// This corresponds to the starting state of the transaction and corresponds to the last
    /// committed transaction at the time this transaction started.
    pub fn tx_id(&self) -> TxId {
        self.state.get().metapage.tx_id
    }

    fn mark_dirty(&self) {
        self.flags.set(self.flags.get().union(TF::DIRTY));
    }

    fn is_dirty(&self) -> bool {
        self.flags.get().contains(TF::DIRTY)
    }

    fn is_write_tx(&self) -> bool {
        self.flags.get().contains(TF::WRITE_TX)
    }

    fn is_multi_write_tx(&self) -> bool {
        self.flags.get().contains(TF::MULTI_WRITE_TX)
    }

    fn is_done(&self) -> bool {
        self.flags.get().contains(TF::DONE)
    }

    fn is_write_or_checkpoint_txn(&self) -> bool {
        self.flags
            .get()
            .intersects(TF::CHECKPOINT_TX | TF::WRITE_TX)
    }

    fn check_conflicts(&mut self) -> bool {
        debug_assert!(self.is_multi_write_tx());
        debug_assert_eq!(self.tracked_transaction, Some(self.tx_id()));
        let base_tx_id = self.tx_id();
        for (&page_id, _) in self.nodes.get_mut().iter() {
            if !self
                .inner
                .page_table
                .is_latest_from_lte(base_tx_id, page_id)
            {
                return true;
            }
        }
        false
    }

    fn commit_start(&mut self) -> Result<(), Error> {
        debug_assert!(self.is_write_tx());
        let mut check_conflicts = false;
        let old_tx_id = self.state.get_mut().metapage.tx_id;
        if self.is_multi_write_tx() {
            trace!(
                "MultiWrite Commit start {}",
                self.state.get_mut().metapage.tx_id
            );
            self.commit_lock = Some(self.inner.commit_lock.lock_arc());
            let latest_state = *self.inner.state.lock();
            latest_state.check_halted()?;
            if self.tx_id() != latest_state.metapage.tx_id {
                if self.check_conflicts() {
                    return Err(Error::WriteConflict);
                }
                self.state.get_mut().metapage = latest_state.metapage;
                check_conflicts = true;
            }
        }
        self.state.get_mut().metapage.tx_id += 1;
        trace!(
            "Commit start {old_tx_id} -> {}",
            self.state.get_mut().metapage.tx_id
        );
        self.trap.check()?;
        // Commit the dirty trees but also leave it in a clean state in case we end up caching it later
        let mut tx_trees = mem::take(self.trees.get_mut());
        let mut trees_tree = self.get_trees_tree();
        let mut had_clean = false;

        let cached_trees = self.commit_lock.as_ref().unwrap().trees.as_ref();
        let get_existing_tree_value =
            |trees_tree: &Tree<'_>, tree_name: &[u8]| -> Result<Option<TreeValue>, Error> {
                if let Some(TreeState::Available { value, .. }) =
                    cached_trees.and_then(|t| t.get(tree_name))
                {
                    return Ok(Some(*value));
                }
                Ok(trees_tree.get(tree_name)?.map(|v| {
                    *Ref::<_, TreeValue>::new_unaligned(v.as_ref())
                        .unwrap()
                        .into_ref()
                }))
            };

        for (tree_name, tree_state) in &mut tx_trees {
            match tree_state {
                TreeState::Available { dirty: false, .. } => {
                    had_clean = true;
                }
                TreeState::Available {
                    value,
                    dirty,
                    len_delta,
                } if !check_conflicts => {
                    trees_tree.insert(tree_name, value.as_bytes())?;
                    *dirty = false;
                    *len_delta = 0;
                }
                TreeState::Available {
                    value,
                    dirty,
                    len_delta,
                } => {
                    let existing = get_existing_tree_value(&trees_tree, tree_name)?;
                    let mut merged = existing.unwrap_or(*value);
                    let needs_update;
                    if existing.is_some() {
                        if merged.id != value.id {
                            return Err(Error::WriteConflict);
                        }
                        if merged.root != PageId::default() && merged.root != value.root {
                            return Err(Error::WriteConflict);
                        }
                        needs_update = merged.root != value.root
                            || merged.level != value.level
                            || *len_delta != 0;
                        merged.root = value.root;
                        merged.level = value.level;
                        merged.num_keys = merged.num_keys.wrapping_add_signed(*len_delta);
                    } else {
                        needs_update = true;
                    }
                    if needs_update {
                        *value = merged;
                        trees_tree.insert(tree_name, value.as_bytes())?;
                    }
                    *dirty = false;
                    *len_delta = 0;
                }
                TreeState::Deleted { value } => {
                    if check_conflicts {
                        if let Some(existing) = get_existing_tree_value(&trees_tree, tree_name)? {
                            if existing.id != value.id {
                                return Err(Error::WriteConflict);
                            }
                        }
                    }
                    trees_tree.delete(tree_name)?;
                }
                TreeState::InUse { .. } => unreachable!(),
            }
        }
        // clean trees could be outdated, so we won't leave anything for caching
        if had_clean && check_conflicts {
            tx_trees.clear();
        }
        let trees_value = trees_tree.value;
        drop(trees_tree);
        *self.trees.get_mut() = tx_trees;
        self.state.get_mut().metapage.trees_tree = trees_value;

        self.commit_dirty_nodes()?;

        // From this point on the commit is infalible wrt. the data file but can still be rolled back
        Ok(())
    }

    fn commit_wal_internal(
        inner: &DatabaseInner,
        write_batch: &mut WriteBatch,
        sync: bool,
    ) -> Result<WalIdx, Error> {
        let wal_commit_idx = inner
            .env
            .wal
            .write(&mut [write_batch.as_reader()?.as_wal_read()])?;
        let wal_end = wal_commit_idx + 1;
        if sync && !inner.opts.env.disable_fsync {
            inner.env.wal.sync_up_to(wal_end)?;
        }
        Ok(wal_end)
    }

    fn commit_wal(&mut self, sync: bool) -> Result<(), Error> {
        if let Some(write_batch) = &mut *self.wal_write_batch.get_mut() {
            match Self::commit_wal_internal(&self.inner, write_batch, sync) {
                Ok(wal_end) => self.state.get_mut().metapage.wal_end = wal_end,
                Err(e) => {
                    if matches!(e, Error::WalHalted | Error::FatalIo(_)) {
                        self.inner.halt();
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn commit_finish(&mut self) -> Result<(), Error> {
        // Any failures after setting DONE are fatal
        self.flags.get_mut().insert(TF::DONE);
        {
            let tx_id = self.tx_id();
            let is_multi_write_tx = self.is_multi_write_tx();
            let allocator = self.allocator.get_mut();
            if is_multi_write_tx {
                let mut new_free = allocator.free.clone();
                new_free.subtract(&allocator.all_allocations);
                allocator.free.subtract(&new_free);
                let mut new_ind_free = allocator.indirection_free.clone();
                new_ind_free.subtract(&allocator.all_allocations);
                allocator.indirection_free.subtract(&new_ind_free);
                if !new_free.is_empty() || !new_ind_free.is_empty() {
                    self.inner
                        .allocator
                        .lock()
                        .pending_free
                        .push((tx_id, new_free, new_ind_free));
                }
            }
            if let Err(e) = allocator.commit() {
                error!("Commiting allocator error: {e}");
                self.inner.halt();
                return Err(e);
            }
            let mut free_buffers = self.inner.free_buffers.lock();
            match free_buffers.free.entry(self.state.get_mut().metapage.tx_id) {
                btree_map::Entry::Vacant(v) => {
                    v.insert(mem::take(&mut allocator.buffer_free));
                }
                btree_map::Entry::Occupied(mut o) => {
                    o.get_mut().append(&mut allocator.buffer_free);
                }
            }
        }

        // No further fallible actions
        trace!("Making commit {} visible", self.tx_id());
        {
            let mut state = self.inner.state.lock();
            state.metapage = self.state.get_mut().metapage;
            state.spilled_total_span += self.nodes_spilled_span.get();
        }

        if let Some(mut write_lock) = self.commit_lock.take() {
            // Release write lock and move the write state into it
            // TODO: consider limiting the size of some of these
            write_lock.nodes = Some(mem::replace(self.nodes.get_mut(), void_dirty_cache()));
            write_lock.trees = Some(mem::take(self.trees.get_mut()));
            write_lock.scratch_buffer = mem::take(self.scratch_buffer.get_mut());
            write_lock.wal_write_batch = self.wal_write_batch.get_mut().take().map(|mut wb| {
                wb.clear();
                wb
            });
        }
        self.multi_write_lock = None;
        self.exclusive_write_lock = None;

        if self.nodes_spilled_span.get() != 0 {
            self.inner
                .request_checkpoint(CheckpointReason::WritesSpilled);
        } else if self.inner.page_table.spans_used()
            >= self.inner.opts.checkpoint_target_size / PAGE_SIZE as usize
        {
            self.release_versions(true);
            let spans_used = self.inner.page_table.spans_used();
            if spans_used >= self.inner.opts.checkpoint_target_size / PAGE_SIZE as usize
                && self.inner.checkpoint_queue.is_empty()
            {
                let size = ByteSize(spans_used as u64 * PAGE_SIZE);
                debug!("Commit will trigger checkpoint, {size}");
                self.inner.request_checkpoint(CheckpointReason::TargetSize);
            }
        }
        Ok(())
    }

    /// Returns a vector containing the names of existing trees the database.
    pub fn list_trees(&self) -> Result<Vec<Bytes>, Error> {
        let guard = self.trap.setup()?;
        let trees = self.get_trees_tree();
        trees.keys()?.collect::<Result<_, _>>().guard_trap(guard)
    }

    /// Gets a [Tree] instance with the corresponding name.
    pub fn get_tree(&self, name: &[u8]) -> Result<Option<Tree<'_>>, Error> {
        let guard = self.trap.setup()?;
        self.get_tree_internal(name).guard_trap(guard)
    }

    fn get_tree_by_id(&self, id: TreeId) -> Result<Option<Tree<'_>>, Error> {
        for (name, state) in self.trees.borrow_mut().iter_mut() {
            match *state {
                TreeState::Available {
                    value,
                    dirty,
                    len_delta,
                } if value.id == id => {
                    *state = TreeState::InUse { value };
                    return Ok(Some(Tree {
                        name: Some(name.clone()),
                        value,
                        tx: self,
                        dirty,
                        len_delta,
                        cached_root: Default::default(),
                    }));
                }
                TreeState::InUse { value } if value.id == id => {
                    return Err(Error::TreeAlreadyOpen(format!("<id: {id}>").into()));
                }
                TreeState::Deleted { .. }
                | TreeState::Available { .. }
                | TreeState::InUse { .. } => (),
            }
        }

        let guard = self.trap.setup()?;
        for result in self.get_trees_tree().iter()? {
            let (k, v) = result?;
            let value = *Ref::<_, TreeValue>::new_unaligned(v.as_ref())
                .unwrap()
                .into_ref();
            if value.id == id {
                return self.get_tree_internal(&k).guard_trap(guard);
            }
        }
        guard.disarm();
        Ok(None)
    }

    fn get_tree_internal(&self, name: &[u8]) -> Result<Option<Tree<'_>>, Error> {
        if self.is_write_or_checkpoint_txn() {
            if name.len() > MAX_TREE_NAME_LEN {
                return Err(Error::validation("Tree name is too long"));
            }
            if let hashbrown::hash_map::EntryRef::Occupied(mut o) =
                self.trees.borrow_mut().entry_ref(name)
            {
                match *o.get() {
                    TreeState::Available {
                        value,
                        dirty,
                        len_delta,
                    } => {
                        *o.get_mut() = TreeState::InUse { value };
                        return Ok(Some(Tree {
                            name: Some(o.key().clone()),
                            value,
                            tx: self,
                            dirty,
                            len_delta,
                            cached_root: Default::default(),
                        }));
                    }
                    TreeState::Deleted { .. } => {
                        return Ok(None);
                    }
                    TreeState::InUse { .. } => {
                        return Err(Error::tree_already_open(name));
                    }
                }
            }
        }
        let tree = self.get_trees_tree();
        if let Some(cursor_value) = tree.get(name)? {
            let value = *Ref::<_, TreeValue>::new_unaligned(cursor_value.as_ref())
                .unwrap()
                .into_ref();
            let name = Arc::<[u8]>::from(name);
            if self.is_write_or_checkpoint_txn() {
                self.trees
                    .borrow_mut()
                    .insert(name.clone(), TreeState::InUse { value });
            }
            Ok(Some(Tree {
                name: Some(name),
                value,
                tx: self,
                dirty: false,
                len_delta: 0,
                cached_root: Default::default(),
            }))
        } else {
            Ok(None)
        }
    }
}

impl DatabaseInner {
    /// An inactive database has no active transactions
    /// and is not performing a checkpoint. Note that this is racy.
    fn is_active(self: &StdArc<Self>) -> bool {
        self.write_lock.is_locked()
            || !self.checkpoint_queue.is_empty()
            || !self.transactions.lock().is_empty()
    }

    /// If the database is not open no more _user_ transactions can be created.
    /// Note that checkpoint transactions are still possible.
    fn is_open(&self) -> bool {
        self.open.lock().is_some()
    }

    fn is_fully_checkpointed(&self) -> bool {
        self.wal_range().is_empty()
    }

    fn wal_range(&self) -> Range<WalIdx> {
        let state = self.state.lock();
        state.metapage.wal_start..state.metapage.wal_end
    }

    fn wal_tail(&self) -> Option<WalIdx> {
        let state = self.state.lock();
        if state.metapage.wal_start != state.metapage.wal_end {
            Some(state.metapage.wal_start)
        } else {
            None
        }
    }

    fn wait_checkpoint(&self) {
        let _locked = self.checkpoint_lock.lock();
        #[cfg(not(feature = "shuttle"))]
        lock_api::MutexGuard::unlock_fair(_locked);
    }

    fn request_checkpoint(&self, msg: checkpoint::CheckpointReason) {
        self.checkpoint_queue.request(msg);
    }

    fn bg_thread(inner: SharedDatabaseInner) -> Result<(), Error> {
        let trap = FnTrap::new(|| inner.halt());
        // Instant::saturating_add is not a thing, so instead clamp checkpoint interval to 100y
        // to avoid panics if options.checkpoint_interval is really high (e.g. Duration::MAX)
        let clamped_checkpoint_interval = inner
            .opts
            .checkpoint_interval
            .min(Duration::from_secs(60 * 60 * 24 * 365 * 100));
        let mut next_checkpoint = Instant::now() + clamped_checkpoint_interval;
        loop {
            let reason = match inner
                .checkpoint_queue
                .peek(next_checkpoint.saturating_duration_since(Instant::now()))
            {
                Ok(reason) => reason,
                Err(mpsc::RecvTimeoutError::Timeout) => checkpoint::CheckpointReason::Periodic,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            DatabaseInner::checkpoint(&inner, reason)?;
            inner.checkpoint_queue.pop(&reason);
            next_checkpoint = Instant::now() + clamped_checkpoint_interval;
        }
        trap.disarm();
        Ok(())
    }

    fn start_bg_thread(self: &StdArc<Self>) {
        self.checkpoint_queue.set_closed(false);
        let mut bg_thread = self.bg_thread.lock();
        assert!(bg_thread.is_none());
        let inner_cloned = self.clone();
        *bg_thread = Some(
            SharedJoinHandle::spawn("Canopydb Database".into(), move || {
                DatabaseInner::bg_thread(inner_cloned)
            })
            .into(),
        );
    }

    fn stop_bg_thread(&self, wait: bool) {
        self.checkpoint_queue.set_closed(true);
        if wait {
            info!("Waiting for Db Bg thread to exit");
            let bg_thread = self.bg_thread.lock().take();
            if let Some(bg_thread) = bg_thread {
                bg_thread.join();
            }
            info!("Db Bg thread exited");
        }
    }

    #[cold]
    fn halt(&self) {
        let was_halted = mem::replace(&mut self.state.lock().halted, true);
        if !was_halted {
            self.env.halt();
            error!(
                "Database {} is halting and will go to ready only mode",
                self.env_db_name
            );
        }
    }

    fn read_metapage(&self, id: PageId) -> Result<MetapageHeader, Error> {
        trace!("read metapage {id}");
        debug_assert!(id <= 1);
        let page = self.read_page(id)?;
        let header = *header_cast::<MetapageHeader, _>(&page);
        if header.magic == METAPAGE_MAGIC.to_be_bytes() {
            Ok(header)
        } else {
            Err(io_invalid_data!("Invalid magic number in metapage header"))
        }
    }

    fn write_metapage(&self, metapage: &MetapageHeader) -> Result<(), Error> {
        trace!(
            "write metapage {} tx_id {}",
            metapage.page_header.id,
            metapage.tx_id
        );
        let mut metapage_page = Page::new(
            metapage.page_header.id,
            true,
            metapage.page_header.span.into(),
        );
        metapage_page
            .data_mut()
            .write_all(metapage.as_bytes())
            .unwrap();
        metapage_page.set_checksum(true);
        self.write_page(&metapage_page)
    }

    fn write_page(&self, page: &Page) -> Result<(), Error> {
        self.write_page_at(page, page.id())
    }

    fn write_page_at(&self, page: &Page, at: PageId) -> Result<(), Error> {
        trace!("write_page_at {} span {} at {}", page.id(), page.span(), at);
        debug_assert!(!page.id().is_compressed());
        debug_assert_eq!(
            size_of::<ReservedPageHeader>() + page.raw_data.len(),
            page.span() as usize * PAGE_SIZE as usize
        );
        debug_assert_eq!(
            page.raw_data
                .raw_data_with_prefix(size_of::<ReservedPageHeader>())
                .len(),
            page.span() as usize * PAGE_SIZE as usize
        );
        let offset = at as u64 * PAGE_SIZE;
        self.file.file.write_all_at(
            page.raw_data
                .raw_data_with_prefix(size_of::<ReservedPageHeader>()),
            offset,
        )?;
        Ok(())
    }

    fn read_page(&self, id: PageId) -> Result<Page, Error> {
        self.read_page_at(id, id, None)
    }

    fn read_page_at(&self, id: PageId, at: PageId, span: Option<PageId>) -> Result<Page, Error> {
        trace!("read_page_at {} at {}", id, at);
        debug_assert!(!id.is_compressed());
        debug_assert!(!at.is_compressed());

        let initial_span = span.unwrap_or(1);
        let mut bytes = unsafe {
            let mut bytes = UninitBytes::new(
                initial_span as usize * PAGE_SIZE as usize - size_of::<ReservedPageHeader>(),
            );
            self.file
                .file
                .read_exact_at(
                    mem::transmute(bytes.as_slice_mut()),
                    at as u64 * PAGE_SIZE + size_of::<ReservedPageHeader>() as u64,
                )
                .unwrap();
            bytes.assume_init()
        };

        let header = header_cast::<PageHeader, _>(&bytes[..]);
        if header.id != id {
            return Err(io_invalid_data!("PageId mismatch {} != {}", header.id, id));
        }
        let span = PageId::from(header.span);
        if span == 0 {
            return Err(io_invalid_data!("0 length page!?"));
        }

        // TODO: make all pageIds carry a length?
        if span > initial_span {
            unsafe {
                let mut new_bytes = UninitBytes::new(
                    span as usize * PAGE_SIZE as usize - size_of::<ReservedPageHeader>(),
                );
                let (a, b) = new_bytes.as_slice_mut().split_at_mut(bytes.len());
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), a.as_mut_ptr().cast(), bytes.len());
                self.file
                    .file
                    .read_exact_at(mem::transmute(b), (at + initial_span) as u64 * PAGE_SIZE)?;
                bytes = new_bytes.assume_init();
            };
        }

        let page = Page::from_bytes(bytes)?;

        if self.opts.env.use_checksums && page.check_checksum() == Some(false) {
            return Err(io_invalid_data!("Checksum mismatch for page {id}"));
        }

        Ok(page)
    }

    /// Write data over pages contained in `pages`, which should have
    /// len >= usable_size_to_noncontinuous_span(data.len() + pages.serialized_size())
    /// and at least one span capable of fitting a page with pages.serialized_size()
    fn write_to_pages(&self, mut data: &[u8], mut pages: Freelist) -> Result<PageId, Error> {
        trace!("write_to_pages: {} bytes, pages: {:?}", data.len(), pages);
        let first_span = total_size_to_span(pages.serialized_size() + size_of::<PageHeader>())?;
        let Some(first_pid) = pages.allocate(first_span) else {
            return Err(error_validation!(
                "write_to_pages cannot allocate initial span of {first_span}"
            ));
        };
        let mut page = Page::new(first_pid, true, first_span);
        let mut buffer = page.usable_data_mut();
        pages.serialize_into(&mut buffer)?;
        let mut data_to_write;
        (data_to_write, data) = data.split_at(data.len().min(buffer.len()));
        buffer[..data_to_write.len()].copy_from_slice(data_to_write);
        page.set_checksum(self.opts.env.use_checksums);
        self.write_page(&page)?;
        drop(page);
        for (pid, span) in pages.iter_spans() {
            let mut page = Page::new(pid, true, span);
            buffer = page.usable_data_mut();
            (data_to_write, data) = data.split_at(data.len().min(buffer.len()));
            buffer[..data_to_write.len()].copy_from_slice(data_to_write);
            page.set_checksum(self.opts.env.use_checksums);
            self.write_page(&page)?;
        }
        if data.is_empty() {
            Ok(first_pid)
        } else {
            Err(error_validation!(
                "write_to_pages spans are not enough to fit data, {} bytes remaining",
                data.len()
            ))
        }
    }

    fn read_from_pages(
        &self,
        first_page_id: PageId,
        read_data: bool,
    ) -> Result<(Freelist, Vec<u8>), Error> {
        let mut page = self.read_page(first_page_id)?;
        let first_span = page.span();
        let mut first_buffer = page.usable_data();
        let (mut spans, freelist_bytes_len) = Freelist::deserialize(first_buffer)?;
        if !read_data {
            spans.free(first_page_id, first_span)?;
            return Ok((spans, Vec::new()));
        }
        first_buffer = &first_buffer[freelist_bytes_len..];
        let mut result_data = Vec::with_capacity(spans.len() as usize * PAGE_SIZE as usize);
        result_data.extend_from_slice(first_buffer);
        for (pid, span) in spans.iter_spans() {
            page = self.read_page_at(pid, pid, Some(span))?;
            result_data.extend_from_slice(page.usable_data());
        }
        spans.free(first_page_id, first_span)?;
        Ok((spans, result_data))
    }

    fn spawn_early_flush_thread(
        inner: &SharedDatabaseInner,
    ) -> (
        mpsc::SyncSender<(PageId, PageId)>,
        Option<thread::JoinHandle<()>>,
    ) {
        let (flusher_tx, flusher_rx) = mpsc::sync_channel::<(PageId, PageId)>(0);
        if inner.opts.env.disable_fsync {
            return (flusher_tx, None);
        }
        let inner_ = inner.clone();
        let flusher = thread::Builder::new()
            .name("Canopydb Checkpoint Flusher".into())
            .spawn(move || {
                for (min, max) in flusher_rx {
                    let sync_offset = min as u64 * PAGE_SIZE;
                    let sync_len = (max - min + 1) as u64 * PAGE_SIZE;
                    let _ = utils::fsync_range(&inner_.file.file, sync_offset, sync_len);
                }
            })
            .unwrap();
        (flusher_tx, Some(flusher))
    }

    fn compress_page(allocator: &mut Allocator, page: &Page) -> Result<Page, Error> {
        assert!(page.compressed_page.is_none());
        let max_compressed_len = lz4_flex::block::get_maximum_output_size(page.raw_data.len());
        let mut page_bytes =
            Bytes::new_zeroed(size_of::<CompressedPageHeader>() + max_compressed_len);
        #[cfg(not(fuzzing))]
        let compressed_len = lz4_flex::compress_into(
            page.raw_data.as_ref(),
            &mut page_bytes.as_mut()[size_of::<CompressedPageHeader>()..],
        )
        .map_err(|e| error::io_other!("Error compressing page {}: {}", page.id(), e))?;
        #[cfg(fuzzing)]
        let compressed_len = page.data().len();
        let raw_compressed_span =
            total_size_to_span(size_of::<CompressedPageHeader>() + compressed_len)?;

        let compressed_span = raw_compressed_span.min(page.span());
        page_bytes.truncate(
            compressed_span as usize * PAGE_SIZE as usize - size_of::<ReservedPageHeader>(),
        );
        let compressed_page_id = allocator.allocate(compressed_span)?;

        trace!(
            "Compressing page {} -> {}, ratio {:.2}x, effective {:.2}x, {} -> {} pages",
            page.id(),
            compressed_page_id,
            page.raw_data.len() as f64 / compressed_len as f64,
            page.span() as f64 / compressed_span as f64,
            page.span(),
            compressed_span
        );

        // The page is allocated at this point, the rest of the actions must not fail.
        let mut compressed_page = Page {
            dirty: false,
            compressed_page: None,
            raw_data: page_bytes,
        };

        if compressed_span == page.span() {
            compressed_page.data_mut().copy_from_slice(page.data());
            compressed_page.header_mut().id = compressed_page_id;
        } else {
            let compressed_header =
                header_cast_mut::<CompressedPageHeader, _>(&mut compressed_page);
            *compressed_header = CompressedPageHeader {
                page_header: PageHeader {
                    checksum: Default::default(),
                    id: compressed_page_id,
                    span: compressed_span.try_into().unwrap(),
                    flags: PageFlags::Compressed,
                },
                compressed_len: compressed_len as u32,
                uncompressed_len: page.raw_data.len() as u32,
            };
        }
        compressed_page.set_checksum(false);

        Ok(compressed_page)
    }

    fn release_old_snapshots(&self, latest_snapshot: TxId) -> Result<(), Error> {
        let earliest_snapshot_required = self
            .transactions
            .lock()
            .first()
            .map_or(latest_snapshot, |ot| ot.earliest_snapshot_tx_id);

        let mut old_snapshots = self.old_snapshots.lock();
        while let Some(pending) = old_snapshots.first_entry() {
            if *pending.key() < earliest_snapshot_required {
                let snapshot_id = *pending.key();
                let (left, right) = pending.remove().split(FIRST_COMPRESSED_PAGE);
                debug!(
                    "Releasing {} of free space for snapshot {}",
                    ByteSize(left.len() as u64 * PAGE_SIZE),
                    snapshot_id,
                );
                let mut allocator = self.allocator.lock();
                allocator.free.merged()?.merge(&left)?;
                allocator.indirection_free.merged()?.merge(&right)?;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn write_checkpoint_pages(
        inner: &DatabaseInner,
        allocator: &mut Allocator,
        pages: &mut dyn Iterator<Item = (TxId, Page, bool, bool)>,
        written_pages: &mut PageId,
        written_spans: &mut PageId,
        compressed_pages_total_compressed_span: &mut PageId,
        new_indirections: &mut Vec<(PageId, PageId, PageId)>,
        redirected_buffers: &mut Vec<FreePage>,
    ) -> Result<usize, Error> {
        // TODO: perform writes in parallel?
        let mut count = 0;
        for (from, mut buffer_page, latest, prioritize_in_shared_cache) in pages {
            debug_assert!(!buffer_page.dirty);
            count += 1;
            let (write_pid, write_page) = if buffer_page.id().is_compressed() {
                let compressed_page = Self::compress_page(allocator, &buffer_page)?;
                *compressed_pages_total_compressed_span += compressed_page.span();
                buffer_page.compressed_page = Some((compressed_page.id(), compressed_page.span()));
                (compressed_page.id(), compressed_page)
            } else if !latest {
                (allocator.allocate(buffer_page.span())?, buffer_page.clone())
            } else {
                (buffer_page.id(), buffer_page.clone())
            };
            inner
                .file
                .ensure_file_size(false, allocator.main_next_page_id as u64 * PAGE_SIZE)?;
            if inner.opts.env.use_checksums || write_page.header().checksum != u32::default() {
                unsafe {
                    // Cloning the page to set a checksum would be way to wasteful. Even if this is a shared
                    // (uncompressed) page, the checkpointer is the only thread reading/writing the checksum.
                    write_page.set_checksum_non_mut(inner.opts.env.use_checksums);
                }
            }
            inner.write_page_at(&write_page, write_pid)?;
            let write_span = write_page.span();
            drop(write_page);

            // Page is written, only now we can write its side effects which are infallible
            *written_pages += 1;
            *written_spans += write_span;

            // We must invalidate the cache as it could be holding an outdated version from previous checkpoints
            let nck = NodeCacheKey::new(
                inner.env_db_id,
                if latest { 0 } else { from },
                buffer_page.id(),
            );
            if let Ok(buffer_node) = UntypedNode::from_page(buffer_page.clone()) {
                if prioritize_in_shared_cache {
                    inner.env.shared_cache.insert(nck, buffer_node);
                } else {
                    let _ = inner.env.shared_cache.replace(nck, buffer_node, false);
                }
            } else {
                inner.env.shared_cache.remove(&nck);
            }

            if buffer_page.id().is_compressed() || !latest {
                trace!("Redirecting page {} to {}", buffer_page.id(), write_pid);
                #[cfg(feature = "shuttle")]
                thread::yield_now();
                let replaced_latest = inner
                    .page_table
                    .replace_at(
                        from,
                        buffer_page.id(),
                        Item::Redirected(write_pid, write_span, latest),
                    )
                    .map(|r| r.1)
                    .unwrap_or_else(|| {
                        debug_assert!(!latest);
                        false
                    });
                redirected_buffers.push(FreePage(from, buffer_page.id()));
                if latest && buffer_page.id().is_compressed() {
                    new_indirections.push((buffer_page.id(), write_pid, write_span));
                }
                if !latest || !replaced_latest {
                    allocator.next_snapshot_free.free(write_pid, write_span)?;
                }
            } else {
                trace!("Removing buffer for page {}", buffer_page.id());
                inner.page_table.remove_at(from, buffer_page.id());
            }
        }
        Ok(count)
    }

    fn checkpoint(
        inner: &SharedDatabaseInner,
        reason: checkpoint::CheckpointReason,
    ) -> Result<PageId, Error> {
        let trap = FnTrap::new(|| inner.halt());
        debug!("Triggering checkpoint, reason = {reason:?}");
        let start = Instant::now();
        let checkpoint = Self::checkpoint_internal(inner, reason);
        if checkpoint.is_ok() {
            trap.disarm();
        }
        if !matches!(checkpoint, Ok(0)) {
            info!(
                "Checkpoint {:?} done in {:?}, reason = {reason:?}",
                checkpoint,
                start.elapsed()
            );
        }
        checkpoint
    }

    fn checkpoint_internal(
        inner: &SharedDatabaseInner,
        reason: checkpoint::CheckpointReason,
    ) -> Result<PageId, Error> {
        debug!("Acquiring checkpoint lock");
        let _checkpoint_lock = inner.checkpoint_lock.lock();

        match reason {
            CheckpointReason::User(desired_snapshot) => {
                let snapshot_tx_id = inner.state.lock().metapage.snapshot_tx_id;
                if snapshot_tx_id >= desired_snapshot {
                    debug!("Ignoring checkpoint request {reason:?}, latest snapshot is {snapshot_tx_id}");
                    return Ok(0);
                }
            }
            CheckpointReason::TargetSize | CheckpointReason::MemoryPressure => {
                let spans_used = inner.page_table.spans_used();
                if spans_used < inner.opts.checkpoint_target_size / PAGE_SIZE as usize {
                    let size = ByteSize(spans_used as u64 * PAGE_SIZE);
                    debug!("Ignoring checkpoint request {reason:?}, memory use is within bounds {size}");
                    return Ok(0);
                }
            }
            CheckpointReason::WritesSpilled => {
                let spilled_total_span = inner.state.lock().spilled_total_span;
                if spilled_total_span == 0 {
                    debug!("Ignoring checkpoint request {reason:?}, no spilled pages");
                    return Ok(0);
                }
            }
            CheckpointReason::WalSize(min_wal_tail) => {
                let cur_wal_tail = inner.wal_tail();
                if cur_wal_tail.is_none_or(|w| w > min_wal_tail) {
                    debug!("Ignoring checkpoint request {reason:?}, tail is {cur_wal_tail:?}");
                    return Ok(0);
                }
            }
            _ => (),
        }

        let mut checkpoint = Checkpoint::default();

        let result = (|| -> Result<Option<WriteTransaction>, Error> {
            debug!("Acquiring checkpoint start write lock");
            let write_lock = inner.write_lock.write();
            let mut txn = Transaction::new_read(inner, false)?;
            let txn_state = txn.state.get();
            txn_state.check_halted()?;
            if txn_state.metapage.snapshot_tx_id == txn_state.metapage.tx_id {
                debug!(
                    "Checkpoint Tx {} is already in the snapshot, nothing to do",
                    txn_state.metapage.tx_id
                );
                return Ok(None);
            }
            if inner.opts.env.wal_new_file_on_checkpoint && inner.env.wal.num_files() == 1 {
                inner.env.wal.switch_to_fresh_file()?;
            }
            inner.state.lock().ongoing_snapshot_tx_id = Some(txn_state.metapage.tx_id);
            txn.0.convert_read_to_checkpoint(0)?;
            drop(write_lock);
            checkpoint.collect_checkpoint_pages(&txn, reason);

            let num_reserved_pages = if cfg!(any(fuzzing, feature = "shuttle")) {
                usable_size_to_noncontinuous_span(2 * inner.allocator.lock().freelist_write_size())?
            } else {
                let est_freelist_size = usable_size_to_noncontinuous_span(
                    2 * inner.allocator.lock().freelist_write_size(),
                )?;
                let est_checkpointer_compression = inner
                    .running_stats
                    .lock()
                    .checkpointer_compression
                    .unwrap_or(1.0);
                // (1+x)/(x**2)
                let est_compressed_reservation_ratio = (1.0 + est_checkpointer_compression)
                    / (est_checkpointer_compression * est_checkpointer_compression);
                let est_compressed_spans = (est_compressed_reservation_ratio
                    * checkpoint.compressed_pages_total_uncompressed_span as f64)
                    .round() as PageId;
                // Reserve
                // * 64KB worth of space
                // * the overestimated freelist size
                // * the overestimated compressed spans
                64 * 1024 / PAGE_SIZE as PageId + est_freelist_size + est_compressed_spans
            };
            debug!(
                "Checkpoint reserving {} of space",
                ByteSize(num_reserved_pages as u64 * PAGE_SIZE)
            );
            // TODO: maybe reserve half of free on start? to avoid this in some cases?
            debug!("Acquiring checkpoint reservation write lock");
            let _write_lock = inner.write_lock.write();
            txn.0
                .allocator
                .get_mut()
                .reserve_from_main(num_reserved_pages, true)?;
            Ok(Some(WriteTransaction(txn.0)))
        })();

        let mut txn = match result {
            Ok(None) => return Ok(0),
            Ok(Some(txn)) => txn,
            // No extra cleanup actions are necessary
            Err(e) => return Err(e),
        };
        let free_space_span = txn.allocator.get_mut().all_freespace_span();
        let txn_state = &*txn.state.get_mut();
        let old_freelist_spans = if txn_state.metapage.freelist_root != PageId::default() {
            inner
                .read_from_pages(txn_state.metapage.freelist_root, false)?
                .0
        } else {
            Default::default()
        };

        info!(
            "Checkpoint Tx {}..={} WAL {:?}..{:?} Pages {} Pending {} Spilled {} Freespace {}",
            txn_state.metapage.snapshot_tx_id + 1,
            txn_state.metapage.tx_id,
            txn_state.metapage.wal_start,
            txn_state.metapage.wal_end,
            checkpoint.pages_total_span,
            ByteSize(checkpoint.pages_total_span as u64 * PAGE_SIZE),
            ByteSize(txn_state.spilled_total_span as u64 * PAGE_SIZE),
            ByteSize(free_space_span as u64 * PAGE_SIZE),
        );
        let snapshot_tx_id = txn_state.metapage.tx_id;
        let spilled_total_span = txn_state.spilled_total_span;
        let mut new_metapage = txn_state.metapage;
        new_metapage.page_header.id = (new_metapage.page_header.id + 1) % 2;
        new_metapage.wal_start = txn_state.metapage.wal_end;

        let copy_start = Instant::now();
        let (flusher_tx, flusher) = Self::spawn_early_flush_thread(inner);
        // Order local vars to ensure the Sender is dropped before the Thread in case of automatic drops
        let flusher = flusher.map(crate::utils::WaitJoinHandle::new);
        let flusher_tx = flusher_tx;

        let mut prioritize_for_shared_cache = checkpoint.make_shared_cache_prioritizer(&mut txn.0);
        #[cfg(feature = "shuttle")]
        checkpoint.pages.sort_by(|a, b| b.1.id().cmp(&a.1.id()));
        #[cfg(not(feature = "shuttle"))]
        checkpoint
            .pages
            .sort_unstable_by(|a, b| b.1.id().cmp(&a.1.id()));

        if let Err(e) = checkpoint.write_pages(
            &mut txn,
            &flusher_tx,
            &mut prioritize_for_shared_cache,
            &old_freelist_spans,
            &mut new_metapage,
        ) {
            error!("Checkpoint error: {e}");
            inner.halt();
            return Err(e);
        }

        if checkpoint.compressed_pages_count != 0 {
            let compression_ratio = checkpoint.compressed_pages_total_uncompressed_span as f64
                / checkpoint.compressed_pages_total_compressed_span as f64;
            info!(
                "Compressed pages {} {} -> {} ({:.3})",
                checkpoint.compressed_pages_count,
                ByteSize(checkpoint.compressed_pages_total_uncompressed_span as u64 * PAGE_SIZE),
                ByteSize(checkpoint.compressed_pages_total_compressed_span as u64 * PAGE_SIZE),
                compression_ratio,
            );
            inner.running_stats.lock().checkpointer_compression = Some(compression_ratio);
        }
        info!(
            "Checkpoint Tx {} written {} pages, {} of data, in {:?}",
            snapshot_tx_id,
            checkpoint.written_pages,
            ByteSize(checkpoint.written_total_span as u64 * PAGE_SIZE),
            copy_start.elapsed()
        );

        let mut checkpoint_allocator = mem::take(txn.allocator.get_mut());
        txn.flags.get_mut().remove(TF::CHECKPOINT_TX | TF::DIRTY);
        drop(txn);
        drop(flusher_tx);
        checkpoint.new_indirections = Default::default();
        checkpoint.pages = Default::default();

        let sync_start = Instant::now();
        let mut checkpoint_sync_fn = || -> Result<(), Error> {
            if !inner.opts.env.disable_fsync {
                fail::fail_point!("fsync", |s| Err(Error::FatalIo(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failpoint fsync {:?}", s)
                ))));
                // We must ensure WAL is stable up to wal_end, otherwise the snapshot can be
                // _after_ the WAL head after a crash.
                // TODO: is that a bad thing?
                inner.env.wal.sync_up_to(new_metapage.wal_end)?;
                // Sync the pages we wrote for this checkpoint
                inner.file.file.sync_data().map_err(Error::FatalIo)?;
            }

            inner.write_metapage(&new_metapage)?;
            checkpoint.written_total_span += 1; // metapage
            if !inner.opts.env.disable_fsync {
                fail::fail_point!("fsync", |s| Err(Error::FatalIo(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failpoint fsync {:?}", s)
                ))));
                // Sync new metapage
                inner.file.file.sync_data().map_err(Error::FatalIo)?;
            }
            Ok(())
        };

        if let Err(e) = checkpoint_sync_fn() {
            error!("Checkpoint sync error: {e}");
            inner.halt();
            return Err(e);
        }
        debug!("Checkpoint Sync in {:?}", sync_start.elapsed());
        if let Some(flusher) = flusher {
            flusher.join().unwrap();
        }

        debug!("Acquiring checkpoint end write lock");
        {
            let _write_lock = inner.write_lock.write();
            let mut state = inner.state.lock();

            // Redirected buffer pages must be freed at the _next_ tx as this tx
            // may already have read transactions w/o the new indirection map.
            inner
                .free_buffers
                .lock()
                .free
                .insert(state.metapage.tx_id + 1, checkpoint.redirected_buffers);
            state.metapage.page_header.id = new_metapage.page_header.id;
            state.metapage.snapshot_tx_id = snapshot_tx_id;
            state.metapage.wal_start = new_metapage.wal_start;
            state.metapage.indirections_tree = new_metapage.indirections_tree;
            state.metapage.freelist_root = new_metapage.freelist_root;
            state.spilled_total_span -= spilled_total_span;
            state.ongoing_snapshot_tx_id = None;
            drop(state);

            if let Err(e) = checkpoint_allocator.commit() {
                error!("Checkpoint allocator error: {e}");
                inner.halt();
            }

            let mut main_allocator = inner.allocator.lock();
            let snapshot_free = mem::take(&mut main_allocator.snapshot_free);
            main_allocator.snapshot_free = mem::take(&mut main_allocator.next_snapshot_free);
            drop(main_allocator);

            // We must register the old snapshot even if it doesn't have any free pages!
            inner
                .old_snapshots
                .lock()
                .insert(snapshot_tx_id, snapshot_free.into_merged().expect("TODO"));
            // which in turn might release old snapshots
            if let Err(e) = inner.release_old_snapshots(snapshot_tx_id) {
                error!("Error releasing snapshot freelist: {e}");
                inner.halt();
                return Err(e);
            }

            // The checkpoint allocator is committed, older snapshts are released (if possible),
            // and we're holding the write lock. So it's a good opportunity to truncate the freelists.
            inner.allocator.lock().truncate_end().expect("TODO");
        }
        // We are not tracking the file size of all snapshots in use, so we could pick the max.
        // So it's only really safe if the earliest tx is using the latest snapshot.
        let earliest_snapshot_in_use = inner
            .transactions
            .lock()
            .first()
            .map_or(snapshot_tx_id, |ot| ot.earliest_snapshot_tx_id);
        if earliest_snapshot_in_use >= snapshot_tx_id {
            // We must shrink the file while holding the allocator mutex,
            // otherwise we risk truncating the file after being expanded by the write tx.
            let main_allocator = inner.allocator.lock();
            if main_allocator.next_page_id <= checkpoint_allocator.main_next_page_id {
                inner.file.ensure_file_size(
                    true,
                    checkpoint_allocator.main_next_page_id as u64 * PAGE_SIZE,
                )?;
            }
        }

        Ok(checkpoint.written_total_span)
    }
}

#[derive(Default, Debug)]
struct Checkpoint {
    /// (From, Page, latest page)
    pages: Vec<(TxId, Page, bool)>,
    redirected_buffers: Vec<FreePage>,
    new_indirections: Vec<(PageId, PageId, PageId)>,
    pages_total_span: u32,
    compressed_pages_count: usize,
    compressed_pages_total_uncompressed_span: u32,
    compressed_pages_total_compressed_span: u32,
    written_total_span: PageId,
    written_pages: PageId,
}

impl Checkpoint {
    fn collect_checkpoint_pages(&mut self, txn: &Transaction, reason: CheckpointReason) {
        let page_table = &txn.inner.page_table;
        let prev_snapshot_tx_id = txn.state.get().metapage.snapshot_tx_id;
        self.pages.reserve_exact(page_table.len_upper_bound());
        page_table
            .iter_latest_items(txn.tx_id())
            .for_each(|(pid, from, item)| {
                if from > prev_snapshot_tx_id {
                    self.add_page(pid, from, item, true)
                }
            });
        // If any write thread stalled we'll get a MemoryPressure reason.
        // In such cases, if the checkpoint can't lower memory usage to under throttling level,
        // the checkpoint will write _all_ committed dirty pages (instead of only from latest snapshot).
        if matches!(reason, CheckpointReason::MemoryPressure)
            && (txn.inner.page_table.spans_used() - self.pages_total_span as usize
                >= txn.inner.opts.throttle_memory_limit / PAGE_SIZE as usize)
        {
            warn!("Will checkpoint all buffers");
            page_table.iter_all_items(txn.tx_id(), |pid, from, item, latest| {
                if !latest || from <= prev_snapshot_tx_id {
                    self.add_page(pid, from, item, false);
                }
            });
        }
    }

    fn add_page(&mut self, pid: u32, from: u64, item: Item, latest: bool) {
        match item {
            Item::Page(page) => {
                if pid.is_compressed() {
                    self.compressed_pages_count += 1;
                    self.compressed_pages_total_uncompressed_span += page.span();
                }
                self.pages_total_span += page.span();
                self.pages.push((from, page, latest));
            }
            Item::Redirected(c_pid, c_span, r_latest) if latest => {
                debug_assert!(r_latest);
                self.new_indirections.push((pid, c_pid, c_span));
                self.redirected_buffers.push(FreePage(from, pid))
            }
            _ => (),
        }
    }

    /// Heuristic to decide witch checkpoint pages to move to the node cache during checkpoint
    fn make_shared_cache_prioritizer(
        &mut self,
        txn: &mut Transaction,
    ) -> impl FnMut(TxId, PageId, bool) -> bool {
        let shared_cache_capacity = txn.inner.env.shared_cache.capacity();
        let shared_cache_weight = txn.inner.env.shared_cache.weight();
        let shared_cache_len = txn.inner.env.shared_cache.len();
        let txn_state = txn.state.get_mut();
        let total_pages_weight = self.pages_total_span as u64 * PAGE_SIZE;
        // replace up to 50% of the cache or its free-space, whichever is higher.
        let max_weight_to_fill =
            (shared_cache_capacity / 2).max(shared_cache_capacity - shared_cache_weight);
        let mut elidible_count = self.pages.len();
        let mut ratio = 0.0;
        if elidible_count != 0 {
            let avg_node_weight = (total_pages_weight + shared_cache_weight)
                .checked_div((self.pages.len() + shared_cache_len) as u64)
                .unwrap_or(PAGE_SIZE);
            let elidible_weight = elidible_count as u64 * avg_node_weight;
            ratio = (max_weight_to_fill as f64 / elidible_weight as f64).min(1.0);
            elidible_count = (elidible_count as f64 * ratio).ceil() as usize;
        }
        let txn_range = txn_state.metapage.tx_id - txn_state.metapage.snapshot_tx_id;
        // TODO: This is a "prioritize recently modified" heuristic,
        // but it'd be best to capture hit counts in page table and using that instead
        let prioritize_txn_gte =
            txn_state.metapage.tx_id - (txn_range as f64 * ratio).ceil() as TxId;
        move |from: TxId, _page_id: PageId, latest: bool| -> bool {
            let result = latest && elidible_count != 0 && from >= prioritize_txn_gte;
            elidible_count -= result as usize;
            result
        }
    }

    fn write_pages(
        &mut self,
        txn: &mut Transaction,
        flusher_tx: &mpsc::SyncSender<(PageId, PageId)>,
        prioritize_for_shared_cache: &mut impl FnMut(TxId, PageId, bool) -> bool,
        old_freelist_spans: &Freelist,
        new_metapage: &mut MetapageHeader,
    ) -> Result<(), Error> {
        let chunk_size = if cfg!(any(fuzzing, feature = "shuttle")) {
            1
        } else {
            4 * 1024
        };
        let (mut min, mut max) = (PageId::MAX, PageId::MIN);
        let mut pages_drain = self.pages.drain(..);
        while DatabaseInner::write_checkpoint_pages(
            &txn.inner,
            txn.allocator.get_mut(),
            &mut pages_drain.by_ref().take(chunk_size).map(|(f, p, l)| {
                let pid = p.id();
                min = min.min(pid);
                max = max.max(pid + p.span() - 1);
                (f, p, l, prioritize_for_shared_cache(f, pid, l))
            }),
            &mut self.written_pages,
            &mut self.written_total_span,
            &mut self.compressed_pages_total_compressed_span,
            &mut self.new_indirections,
            &mut self.redirected_buffers,
        )? != 0
        {
            if !txn.inner.opts.env.disable_fsync && flusher_tx.try_send((min, max)).is_ok() {
                (min, max) = (PageId::MAX, PageId::MIN);
            }
        }

        // Indirections
        if !self.new_indirections.is_empty() || !txn.allocator.get_mut().indirection_free.is_empty()
        {
            let _indirection_free = txn.allocator.get_mut().indirection_free.clone();
            let mut ind_tree = txn.get_indirection_tree();
            // TODO: little point in deleting in most cases, maybe write a tombstone (or nothing) instead?
            // TODO: instead do delete range for keys >= main_next_indirection_id
            #[cfg(any(test, fuzzing))]
            for ind_page_id in _indirection_free.iter_pages().rev() {
                ind_tree.delete(&ind_page_id.to_be_bytes())?;
            }
            for &(ind_page_id, page_id, page_span) in &*self.new_indirections {
                let value = IndirectionValue {
                    pid: page_id,
                    span: U24::try_from(page_span).unwrap(),
                };
                ind_tree.insert(&ind_page_id.to_be_bytes(), value.as_bytes())?;
            }
            new_metapage.indirections_tree = ind_tree.value;
            drop(ind_tree);
            let txn_id = txn.tx_id();
            let mut nodes_freed = Vec::with_capacity(txn.nodes.get_mut().len() / 2);
            let mut drain_nodes =
                txn.nodes
                    .get_mut()
                    .drain()
                    .filter_map(|(pid, tx_node)| match tx_node {
                        TxNode::Stashed(node) => {
                            let mut page = node.into_page();
                            page.dirty = false;
                            Some((txn_id, page, true, true))
                        }
                        TxNode::Freed {
                            from_snapshot,
                            span,
                            compressed_page,
                        } => {
                            debug_assert_eq!(compressed_page, None);
                            if from_snapshot {
                                nodes_freed.push((pid, span));
                            }
                            None
                        }
                        TxNode::Popped(..) | TxNode::Spilled { .. } => unreachable!(),
                    });
            let mut new_indirections_tmp = Vec::new();
            DatabaseInner::write_checkpoint_pages(
                &txn.inner,
                txn.allocator.get_mut(),
                &mut drain_nodes,
                &mut self.written_pages,
                &mut self.written_total_span,
                &mut self.compressed_pages_total_compressed_span,
                &mut new_indirections_tmp,
                &mut self.redirected_buffers,
            )?;
            // reflect any freed pages
            for (page_id, span) in nodes_freed {
                txn.allocator.get_mut().snapshot_free.free(page_id, span)?;
            }
            // saving indirections must not alter existing ones
            debug_assert_eq!(txn.allocator.get_mut().indirection_free, _indirection_free);
            debug_assert!(new_indirections_tmp.is_empty());
        }

        // Freelist
        let checkpoint_allocator = txn.allocator.get_mut();
        checkpoint_allocator
            .snapshot_free
            .merge(old_freelist_spans)?;
        let mut spans_for_new_freelist;
        let mut allocator_freelist_size = checkpoint_allocator.write_size();
        let mut needed_contiguous_span = 1;
        let mut needed_spans =
            usable_size_to_noncontinuous_span(allocator_freelist_size)? + needed_contiguous_span;
        loop {
            spans_for_new_freelist = checkpoint_allocator.allocate_spans(needed_spans)?;
            if needed_contiguous_span > 1 {
                let allocated = checkpoint_allocator.allocate(needed_contiguous_span)?;
                spans_for_new_freelist.free(allocated, needed_contiguous_span)?;
            }
            allocator_freelist_size = checkpoint_allocator.write_size();
            let spans_serialized_size = spans_for_new_freelist.serialized_size();
            needed_spans =
                usable_size_to_noncontinuous_span(allocator_freelist_size + spans_serialized_size)?;
            if spans_for_new_freelist.len() >= needed_spans {
                needed_contiguous_span = usable_size_to_noncontinuous_span(spans_serialized_size)?;
                if needed_contiguous_span == 1 {
                    break;
                }
                if let Some(allocated) = spans_for_new_freelist.allocate(needed_contiguous_span) {
                    spans_for_new_freelist.free(allocated, needed_contiguous_span)?;
                    break;
                }
            }
            checkpoint_allocator.free.merge(&spans_for_new_freelist)?;
        }
        debug!("Freelist size {}", ByteSize(allocator_freelist_size as u64));
        let mut buffer = Vec::with_capacity(allocator_freelist_size);
        checkpoint_allocator.write(&mut buffer)?;
        assert_eq!(buffer.len(), allocator_freelist_size);
        let spans_for_new_freelist_len = spans_for_new_freelist.len();
        let spans_for_new_freelist_num_spans = spans_for_new_freelist.num_spans();
        new_metapage.freelist_root = txn.inner.write_to_pages(&buffer, spans_for_new_freelist)?;
        self.written_total_span += spans_for_new_freelist_len;
        self.written_pages += spans_for_new_freelist_num_spans;
        debug!("Freelist saved to page {}", new_metapage.freelist_root);
        Ok(())
    }
}

#[cfg(any(test, fuzzing))]
impl Drop for DatabaseInner {
    fn drop(&mut self) {
        warn!("Drop for DatabaseInner");
    }
}

type DirtyNodes = quick_cache::unsync::Cache<
    PageId,
    TxNode,
    DirtyNodeWeighter,
    // Using quality for the time being to avoid degenerate cases https://github.com/rust-lang/hashbrown/issues/577
    foldhash::quality::RandomState,
    DirtyNodeLifecycle,
>;

fn new_dirty_cache(options: &DbOptions) -> DirtyNodes {
    DirtyNodes::with_options(
        quick_cache::OptionsBuilder::new()
            .ghost_allocation(0.5)
            .hot_allocation(0.9)
            .shards(0)
            .estimated_items_capacity(options.write_txn_memory_limit / PAGE_SIZE as usize)
            .weight_capacity(options.write_txn_memory_limit as u64)
            .build()
            .unwrap(),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}

fn void_dirty_cache() -> DirtyNodes {
    DirtyNodes::with_options(
        quick_cache::OptionsBuilder::new()
            .shards(0)
            .estimated_items_capacity(0)
            .weight_capacity(0)
            .build()
            .unwrap(),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}

#[derive(Default)]
struct DirtyNodeWeighter;

impl DirtyNodeWeighter {
    #[inline]
    fn weight(val: &TxNode) -> u64 {
        match val {
            TxNode::Stashed(n) => n.page_size() as u64,
            TxNode::Popped(n) => *n,
            TxNode::Spilled { .. } | TxNode::Freed { .. } => 0,
        }
    }
}

impl quick_cache::Weighter<PageId, TxNode> for DirtyNodeWeighter {
    #[inline]
    fn weight(&self, _key: &PageId, val: &TxNode) -> u64 {
        Self::weight(val)
    }
}

#[derive(Default)]
struct DirtyNodeLifecycle;

impl quick_cache::Lifecycle<PageId, TxNode> for DirtyNodeLifecycle {
    type RequestState = SmallVec<TxNode, 1>;

    #[inline]
    fn begin_request(&self) -> Self::RequestState {
        Default::default()
    }

    #[inline]
    fn is_pinned(&self, _key: &PageId, val: &TxNode) -> bool {
        matches!(val, TxNode::Popped(..))
    }

    #[inline]
    fn before_evict(&self, state: &mut Self::RequestState, _key: &PageId, val: &mut TxNode) {
        let TxNode::Stashed(n) = val else {
            unreachable!()
        };
        let span = n.span();
        state.push(mem::replace(
            val,
            TxNode::Spilled {
                span,
                compressed_page: None,
            },
        ));
    }

    #[inline]
    fn on_evict(&self, _state: &mut Self::RequestState, _key: PageId, _val: TxNode) {
        debug_assert!(
            matches!(_val, TxNode::Popped(..) | TxNode::Freed { .. }),
            "evicted {_val:?}"
        );
    }

    #[inline]
    fn end_request(&self, _state: Self::RequestState) {
        #[cfg(debug_assertions)]
        unreachable!()
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
