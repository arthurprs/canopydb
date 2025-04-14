use std::{
    collections::BTreeMap,
    io,
    path::Path,
    time::{Duration, Instant},
};

use crate::{
    checkpoint::CheckpointReason,
    error::{io_invalid_data, io_invalid_input, io_other, Error},
    hash_map,
    node::UntypedNode,
    options::{DbOptions, EnvDbOptions, EnvOptions},
    repr::{DbId, PageId, TxId},
    shim::{
        parking_lot::Mutex,
        sync::{
            self,
            atomic::{self, AtomicBool},
            Arc as StdArc, OnceLock, Weak as StdWeak,
        },
        thread,
    },
    utils::{self, FnTrap, SharedJoinHandle},
    wal::{Wal, WalIdx},
    write_batch::{Operation, WriteBatch, WriteBatchReader},
    Database, DatabaseInner, HashMap, HashSet, WeakDatabaseInner, WriteTransaction, PAGE_SIZE,
};

use smallvec::SmallVec;
use triomphe::Arc;

const DELETED_SUFFIX: &str = "d";
const LIVE_SUFFIX: &str = "l";

pub(crate) type SharedEnvironmentInner = Arc<EnvironmentInner>;
pub(crate) type EnvironmentHandle = Arc<EnvironmentHandleInner>;

#[derive(Clone, Debug)]
pub(crate) struct EnvironmentHandleInner(SharedEnvironmentInner);

impl Drop for EnvironmentHandleInner {
    fn drop(&mut self) {
        #[cfg(any(test, fuzzing))]
        warn!("Drop for EnvironmentHandleInner");
        self.0.closed.store(true, atomic::Ordering::Release);
        if self.0.opts.wait_bg_threads_on_drop {
            self.0.advance_bg_thread();
            let _ = self.0.bg_thread.get().unwrap().join();
        }
    }
}

#[derive(Debug)]
pub(crate) struct EnvironmentInner {
    /// If set, no handles to the Environment exists and the bg thread should exit
    closed: AtomicBool,
    /// If set, the environment is poisoned and public apis will error
    halted: AtomicBool,
    /// Holds all open dbs.
    ///
    /// It's important that any non fully checkpointed Dbs are present so that the Env
    /// doesn't let its "Wal tail" move past any open Db "Wal tail".
    /// The only exception to the above is during shutdown, when open_dbs is empty.
    ///
    /// Utilizes a BTreeMap for iteration order determinism.
    open_dbs: Arc<Mutex<BTreeMap<String, WeakDatabaseInner>>>,
    pub wal: Arc<Wal>,
    pub shared_cache: SharedCache,
    bg_tx: sync::mpsc::SyncSender<sync::mpsc::Sender<()>>,
    bg_thread: OnceLock<Arc<SharedJoinHandle<Result<(), Error>>>>,
    pub opts: Arc<EnvOptions>,
    /// The last field so it's dropped last
    _lock_file: fslock::LockFile,
}

impl EnvironmentInner {
    pub fn upgrade(inner: SharedEnvironmentInner, handle: EnvironmentHandle) -> Environment {
        Environment { handle, inner }
    }

    pub(crate) fn advance_bg_thread(&self) {
        let (tx, rx) = sync::mpsc::channel();
        let _ = self.bg_tx.send(tx);
        let _ = rx.recv();
    }

    fn bg_thread(
        inner: SharedEnvironmentInner,
        rx: sync::mpsc::Receiver<sync::mpsc::Sender<()>>,
    ) -> Result<(), Error> {
        const DEFAULT_WAKE_INTERVAL: Duration = Duration::from_secs(2);
        let trap = FnTrap::new(|| inner.halt());

        let wake_up_interval: Duration = inner
            .opts
            .wal_background_sync_interval
            .map(|s| s.min(DEFAULT_WAKE_INTERVAL))
            .unwrap_or(DEFAULT_WAKE_INTERVAL);

        let mut last_wal_sync = Instant::now();
        loop {
            let _guard = rx.recv_timeout(wake_up_interval);
            inner.check_halted()?;
            inner.maintain_wal_max_size()?;
            inner.periodic_wal_sync(&mut last_wal_sync)?;
            inner.fadvise_wal()?;
            if !inner.maintain_open_dbs()? {
                break;
            }
        }
        trap.disarm();
        Ok(())
    }

    fn maintain_open_dbs(&self) -> Result<bool, Error> {
        let mut dbs = self.open_dbs.lock();
        let mut dbs_to_remove = Vec::new();
        for (db_name, db_inner) in dbs.iter() {
            if let Some(db_inner) = db_inner.upgrade() {
                if !db_inner.is_open() && !db_inner.is_active() && db_inner.is_fully_checkpointed()
                {
                    info!("Closing down db: {db_name}, reason: fully checkpointed");
                    db_inner.stop_bg_thread(true);
                    StdArc::try_unwrap(db_inner).unwrap();
                    dbs_to_remove.push(db_name.clone());
                } else if !db_inner.is_open() && !db_inner.is_active() {
                    // signal a closed and inactive database to checkpoint so it can be removed sooner?
                    // let _ = db_inner.bg_tx.try_send(true);
                }
            } else {
                dbs_to_remove.push(db_name.clone());
            }
        }
        for db_name in dbs_to_remove.drain(..) {
            dbs.remove(&db_name);
        }
        drop(dbs);
        if self.closed.load(atomic::Ordering::Acquire) {
            // We may be stopping not fully checkpointed Dbs. By taking dbs, Self::wal_tail will return None.
            // So any Db checkpoints racing with this shutdown will complete normally but won't truncate the WAL.
            let taken_dbs = std::mem::take(&mut *self.open_dbs.lock());
            // We must not hold dbs lock to avoid deadlocks as the Dbs being stopped may be calling Self::wal_tail.
            for (db_name, db_inner) in taken_dbs {
                if let Some(db_inner) = db_inner.upgrade() {
                    info!("Closing down db: {}, reason: shutdown", db_name);
                    db_inner.stop_bg_thread(true);
                    StdArc::try_unwrap(db_inner).unwrap();
                }
            }
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn periodic_wal_sync(&self, last_sync: &mut Instant) -> Result<(), Error> {
        let Some(interval) = self.opts.wal_background_sync_interval else {
            return Ok(());
        };
        let now = Instant::now();
        if now.saturating_duration_since(*last_sync) >= interval {
            debug!("Background wal sync");
            if let Err(e) = self.wal.sync() {
                self.halt();
                return Err(e);
            }
            *last_sync = now;
        }
        Ok(())
    }

    fn fadvise_wal(&self) -> Result<(), Error> {
        self.wal.fadvise()
    }

    fn maintain_wal_max_size(&self) -> Result<(), Error> {
        // Truncate wal
        if let Some(until) = self.wal_trim_until() {
            self.wal.trim(until)?;
        }

        // If wal size exceeds the max size trigger the Db with the earliest wal_tail
        // to checkpoint. This checkpoint may trim the tail of the Wal, reducing the Wal size.
        if self.wal.disk_size() <= self.opts.max_wal_size {
            return Ok(());
        }

        if let Some((_name, db, wal_tail)) = self
            .open_dbs
            .lock()
            .iter()
            .filter_map(|(name, db)| {
                let db = db.upgrade()?;
                let wal_tail = db.wal_tail()?;
                Some((name, db, wal_tail))
            })
            .min_by_key(|(.., wal_tail)| *wal_tail)
        {
            db.request_checkpoint(CheckpointReason::WalSize(wal_tail));
        }
        Ok(())
    }

    pub(crate) fn wal_trim_until(&self) -> Option<WalIdx> {
        let mut max_wal_start = None;
        let mut min_wal_tail = None;
        for w in self.open_dbs.lock().values() {
            let Some(db) = w.upgrade() else { continue };
            let wal_range = db.wal_range();
            max_wal_start = max_wal_start.max(Some(wal_range.start));
            if !wal_range.is_empty() {
                let min_wal_tail = min_wal_tail.get_or_insert(wal_range.start);
                *min_wal_tail = (*min_wal_tail).min(wal_range.start)
            }
        }
        min_wal_tail.or(max_wal_start)
    }

    #[cold]
    pub(crate) fn halt(&self) {
        self.wal.halt();
        if !self.halted.swap(true, atomic::Ordering::Release) {
            error!("Halting Environment");
            for db in self.open_dbs.lock().values() {
                let Some(db) = db.upgrade() else { continue };
                db.halt();
                db.stop_bg_thread(false);
            }
            if let Some(cb) = self.opts.halt_callback.as_deref() {
                info!("Invoking user provided halt callback");
                (*cb.lock())();
            }
        }
    }

    fn check_halted(&self) -> Result<(), Error> {
        if !self.halted.load(atomic::Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::EnvironmentHalted)
        }
    }
}

#[cfg(any(test, fuzzing))]
impl Drop for EnvironmentInner {
    fn drop(&mut self) {
        warn!("Drop for EnvironmentInner");
    }
}

/// Environment for Databases
///
/// An Environment is composed of multiple independent Databases, a shared Write-Ahead-Log (WAL) and a shared page cache.
///
/// # Environment struct
///
/// The [Environment] is a reference counted handle to the underlying environment. All objects
/// within the environment implicitly have a handle to their environment. So having a Database open without an Environment is ok.
/// [Environment] implements Clone, Sync and Send and doesn't to be wrapped in an Arc, Mutex or similar.
///
/// # Managing databases
///
/// The environment public functions can create and/or return instances of [Database].
///
/// Note that [Database] instances are unique. Only one instance of each [Database] can be active at a given time. Attempting to re-open the database of modifying
/// it while the corresponding instance is still active will return errors.
///
/// Dropping the Database automatically returns the instance to the [Environment], making it possible to re-open or delete it.
///
/// # Write-Ahead-Log (WAL)
///
/// The Write-Ahead-Log (WAL) are append-only files used for transaction recovery and durability.
/// When WAL is being utilized, database changes are always written first in the log before being reflected
/// to the database file.
///
/// Concurrent writes and fsyncs to the WAL are groupped together in batches, amortizing the costs and
/// increasing overall efficiency.
///
/// The WAL enables tree key things:
/// * after a restart the WAL can be replayed to recover the Database to a consistent point in time,
///   including between different databases.
/// * the WAL can be efficiently `fsync`ed to make transactions durable (in combination with the point above)
/// * transactions from different databases can be committed together atomically. This allows stablishing a
///   consistent state between them.
///
/// # Page Cache
///
/// Database pages read from the Database files are cached in the page cache.
/// All databases in the Environment share the environment page cache, which allows users to controll overall capacity.
///
/// # Background thread
///
/// Each `Environment` has a companion background thread that is responsible for performing Wal and Database maintenance.
///
/// For example, it's possible to [configure](EnvOptions::wal_background_sync_interval) the background thread to perform periodic fsyncs of the WAL.
/// This puts an approximate bound on the amount of data loss when not performing sync commits.
///
/// # File format and typical directory structure
///
/// The file format of CanopyDb is endianess-dependent, so files created by a big-endian system cannot be open by a little-endian system, and vice-versa.
/// Not that this is mostly likely not a concern in practice as nearly all modern processors architectures are little-endian (e.g. x86, arm and RISC-V).
///
/// ```plain
/// (env root directory)
/// ├── LOCK (Env lock file)
/// └── WAL-0000001A00 (Env WAL file, at least one but possibly more)
/// └── WAL-000002B000 (Env WAL file cont.)
/// ├── mydb_A1B05283690D1C49B93175EB2DFAC57B_l (`mydb` database directory)
/// │   ├── DATA (database data file)
/// │   └── OPTIONS (database options file)
/// ├── otherdb_A1B05283690D1C49B93175EB2DFAC57B_l (`otherdb` database directory)
/// | ...
/// ```
#[derive(Debug, Clone)]
pub struct Environment {
    pub(crate) inner: SharedEnvironmentInner,
    handle: EnvironmentHandle,
}

impl Environment {
    fn parse_db_folder(subject: &str) -> Result<(&str, DbId, bool), Error> {
        let split = subject.rsplitn(3, '_').collect::<Vec<_>>();
        match &split[..] {
            // note that the array generated by rsplit is reversed
            &[suffix, db_id, db_name] => {
                Self::validate_db_name(db_name)?;
                let Ok(db_id) = DbId::from_str_radix(db_id, 16) else {
                    return Err(io_invalid_data!("Invalid db Id: {db_id}"));
                };
                Ok((db_name, db_id, suffix == DELETED_SUFFIX))
            }
            _ => Err(io_invalid_data!("Invalid db folder name: {subject}")),
        }
    }

    fn make_db_folder(db_name: &str, db_id: DbId, deleted: bool) -> String {
        let suffix = if deleted { DELETED_SUFFIX } else { LIVE_SUFFIX };
        format!("{db_name}_{db_id:X}_{suffix}")
    }

    fn validate_db_name(db_name: &str) -> Result<(), Error> {
        let mut components = Path::new(db_name).components();
        match (components.next(), components.next()) {
            (Some(std::path::Component::Normal(_)), None) => Ok(()),
            (None, _) => Err(io_invalid_input!("Database names cannot be empty")),
            _ => Err(io_invalid_input!(
                "Invalid characters in db name: {db_name}"
            )),
        }
    }

    fn find_database_id(dir: &Path, needle_db_name: &str) -> Result<Option<DbId>, Error> {
        for sub_dir in Self::list_directories(dir)? {
            if sub_dir.starts_with(needle_db_name) {
                let (db_name, db_id, deleted) = Self::parse_db_folder(&sub_dir)?;
                if db_name == needle_db_name && !deleted {
                    return Ok(Some(db_id));
                }
            }
        }
        Ok(None)
    }

    fn list_directories(dir: &Path) -> io::Result<Vec<String>> {
        let mut sub_dirs = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let file_name = entry.file_name();
            if let Ok(file_name) = file_name.into_string() {
                sub_dirs.push(file_name);
            }
        }
        Ok(sub_dirs)
    }

    /// Opens an Environment in the provided directory path and default [EnvOptions].
    pub fn new(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        Self::with_options(EnvOptions::new(path.as_ref()))
    }

    /// Opens an Environment with the provided [EnvOptions].
    pub fn with_options(options: EnvOptions) -> Result<Self, Error> {
        options.validate()?;
        let opts = Arc::new(options);
        let mut lock_file = fslock::LockFile::open(&opts.path.join("LOCK"))?;
        let start = Instant::now();
        while !lock_file.try_lock()? {
            if start.elapsed() < opts.file_lock_timeout {
                thread::sleep(Duration::from_millis(100));
            } else {
                return Err(Error::EnvironmentLocked);
            }
        }
        let wal = Arc::new(Wal::new(opts.path.clone())?);
        let (bg_tx, bg_rx) = sync::mpsc::sync_channel(64);
        let inner = Arc::new(EnvironmentInner {
            closed: Default::default(),
            halted: Default::default(),
            open_dbs: Default::default(),
            wal: wal.clone(),
            shared_cache: SharedCache::with_options(
                quick_cache::OptionsBuilder::new()
                    .ghost_allocation(0.5)
                    .hot_allocation(0.9)
                    .estimated_items_capacity(opts.page_cache_size / PAGE_SIZE as usize)
                    .weight_capacity(opts.page_cache_size as u64)
                    .build()
                    .unwrap(),
                NodeWeighter,
                Default::default(),
                Default::default(),
            ),
            opts: opts.clone(),
            bg_tx,
            bg_thread: Default::default(),
            _lock_file: lock_file,
        });

        let inner_ = inner.clone();
        inner
            .bg_thread
            .set(
                SharedJoinHandle::spawn("Canopydb Environment".into(), move || {
                    EnvironmentInner::bg_thread(inner_, bg_rx)
                })
                .into(),
            )
            .unwrap();
        let env_handle = Arc::new(EnvironmentHandleInner(inner.clone()));
        let env = Environment {
            handle: env_handle.clone(),
            inner,
        };

        let mut dbs = env.inner.open_dbs.lock();
        let mut existing_dbs = BTreeMap::new();
        for sub_dir in Self::list_directories(&opts.path)? {
            let (db_name, db_id, deleted) = Self::parse_db_folder(&sub_dir)?;
            info!("Detected database `{db_name}` id: {db_id:X} deleted: {deleted:?}");
            if !deleted {
                if existing_dbs.insert(db_id, db_name.to_string()).is_some() {
                    return Err(io_invalid_data!("Cannot open environment with multiple databases with the same name: {db_name}"));
                }
            } else if let Err(e) = std::fs::remove_dir_all(opts.path.join(&sub_dir)) {
                error!("Failed to remove subdirectory for deleted db {db_name}: {sub_dir}");
                return Err(io_other!("Failed removing deleted database {db_name}: {e}"));
            }
        }

        let mut ignored_dbs = HashSet::default();
        let mut recovering_dbs = HashMap::default();
        for wb in wal.recover()? {
            let (wal_idx, mut write_batch) = wb?;
            let mut operations = WriteBatch::iter_operations(&mut write_batch);
            match operations.next() {
                Some(Ok(Operation::Database(db_id))) => {
                    let Some(db_name) = existing_dbs.get(&db_id) else {
                        // ignore recovery for non-existing dbs, since db creation/deletion is durable
                        // we assume any missing dbs were removed.
                        if ignored_dbs.insert(db_id) {
                            debug!("Ignoring recovery for non present db id: {db_id:X}");
                        }
                        continue;
                    };
                    let db = match recovering_dbs.entry(db_id) {
                        hash_map::Entry::Vacant(v) => {
                            debug!("Recovering database `{db_name}` id: {db_id:X}");
                            let db_folder =
                                opts.path.join(Self::make_db_folder(db_name, db_id, false));
                            let db_options =
                                DbOptions::read_from_db_folder(&db_folder)?.unwrap_or_default();
                            let envdb_opts = EnvDbOptions::new(db_folder, opts.clone(), db_options);
                            let env_inner = env.inner.clone();
                            let env_handle = env_handle.clone();
                            let db = Database::new_internal(
                                envdb_opts,
                                env_inner,
                                env_handle,
                                db_id,
                                db_name.to_string(),
                            )?;
                            v.insert(db)
                        }
                        hash_map::Entry::Occupied(o) => o.into_mut(),
                    };
                    db.apply_write_batch(wal_idx, operations)
                        .map_err(|e| io_other!("Error recoverying WAL for db {db_name}: {e}"))?;
                }
                Some(Ok(_)) => {
                    return Err(io_invalid_data!(
                        "First record of batch {} isn't a database record",
                        wal_idx
                    ))
                }
                Some(Err(e)) => return Err(e.into()),
                None => (),
            }
        }
        wal.finish_recover()
            .map_err(|e| io_other!("Error finishing recovery: {e}"))?;

        for db_recovery in recovering_dbs.into_values() {
            let db = db_recovery.finish()?;
            dbs.insert(db.inner.env_db_name.clone(), StdArc::downgrade(&db.inner));
        }
        drop(dbs);

        Ok(env)
    }

    /// Removes the corresponding database.
    pub fn remove_database(&self, db_name: &str) -> Result<bool, Error> {
        self.inner.check_halted()?;
        let Some(mut db) = self.open_database_internal(db_name, false, None)? else {
            return Ok(false);
        };
        let db_id = db.inner.env_db_id;
        // stop db background work
        db.inner.stop_bg_thread(true);
        let mut dbs = self.inner.open_dbs.lock();
        dbs.remove(db_name).unwrap();
        // At this point we have unique ownership of DatabaseInner
        StdArc::get_mut(&mut db.inner).unwrap();
        let old_path = db.inner.opts.path.clone();
        let parent = old_path.parent().unwrap();
        let new_path = parent.join(Self::make_db_folder(db_name, db_id, true));
        drop(db);
        // A rename failure might leave a valid Db in place but the Wal
        // tail could move past the (not) removed Db wal_start, making the
        // left over Db unrecoverable.
        let commit_deletion =
            std::fs::rename(&old_path, &new_path).and_then(|_| utils::sync_dir(parent));
        if let Err(e) = commit_deletion {
            error!("Failed to delete database {db_name}: {e}");
            self.inner.halt();
            return Err(Error::FatalIo(e));
        }
        // Only release the lock after the parent fsync to avoid scenarios
        // where we: remove, re-create the db with the same name, crash;
        // then recovery with 2 alive dbs with the same name.
        drop(dbs);
        // At this point the old db is fully isolated and can be deleted
        std::fs::remove_dir_all(&new_path)?;
        Ok(true)
    }

    /// Returns the database with the corresponding name.
    /// Creates the database if necessary.
    pub fn get_or_create_database(&self, db_name: &str) -> Result<Database, Error> {
        self.get_or_create_database_with(db_name, DbOptions::default())
    }

    /// Returns the database with the corresponding name.
    /// Creates the database if necessary.
    pub fn get_or_create_database_with(
        &self,
        db_name: &str,
        options: DbOptions,
    ) -> Result<Database, Error> {
        self.open_database_internal(db_name, true, Some(options))
            .map(Option::unwrap)
    }

    /// Returns the database with the corresponding name.
    /// Returns None if the database doesn't exists.
    pub fn get_database(&self, db_name: &str) -> Result<Option<Database>, Error> {
        self.open_database_internal(db_name, false, None)
    }

    /// Returns the database with the corresponding name.
    /// Returns None if the database doesn't exists.
    pub fn get_database_with(
        &self,
        db_name: &str,
        options: DbOptions,
    ) -> Result<Option<Database>, Error> {
        self.open_database_internal(db_name, false, Some(options))
    }

    fn open_database_internal(
        &self,
        db_name: &str,
        create_non_existing: bool,
        db_options: Option<DbOptions>,
    ) -> Result<Option<Database>, Error> {
        self.inner.check_halted()?;
        Self::validate_db_name(db_name)?;
        if let Some(db_options) = &db_options {
            db_options.validate()?;
        }
        let mut dbs = self.inner.open_dbs.lock();
        if let Some(mut inner) = dbs.get(db_name).and_then(StdWeak::upgrade) {
            let mut open = inner.open.lock();
            if open.is_some() {
                return Err(Error::database_already_open(db_name));
            }
            let env_handle = self.handle.clone();
            *open = Some(env_handle.clone());
            drop(open);
            if db_options.is_some() && Some(&inner.opts.db) != db_options.as_ref() {
                inner.stop_bg_thread(true);
                dbs.remove(db_name);
                StdArc::get_mut(&mut inner).unwrap().opts.db = db_options.unwrap();
                DatabaseInner::start_bg_thread(&inner);
                dbs.insert(db_name.into(), StdArc::downgrade(&inner));
            }
            return Ok(Some(Database { inner, env_handle }));
        }

        let db_id = if let Some(existing) = Self::find_database_id(&self.inner.opts.path, db_name)?
        {
            info!("Opening existing database name: {db_name} id: {existing}");
            existing
        } else if create_non_existing {
            let new = rand::random();
            info!("Creating new database name: {db_name} id: {new}");
            new
        } else {
            return Ok(None);
        };

        let envdb_opts = EnvDbOptions::new(
            self.inner
                .opts
                .path
                .join(Self::make_db_folder(db_name, db_id, false)),
            self.inner.opts.clone(),
            db_options.unwrap_or_default(),
        );
        let env = self.inner.clone();
        let env_handle = self.handle.clone();
        let db = Database::new_internal(envdb_opts, env, env_handle, db_id, db_name.into())?
            .no_recovery();
        dbs.insert(db_name.into(), StdArc::downgrade(&db.inner));
        Ok(Some(db))
    }

    /// Atomically commit multiple write transactions.
    ///
    /// The `sync` argument specifies whether the wal commit should be made "durable" by issuing
    /// the necessary fsync's to the OS.
    ///
    /// Note that all transactions involved must be from databases of the same Environment,
    /// and all transactions must be from databases with Wal enabled.
    ///
    /// The current limit for the number of transactions in a commit group is 1024 and the function
    /// will return an error if the limit is exceeded.
    pub fn group_commit(
        &self,
        txs: impl IntoIterator<Item = WriteTransaction>,
        sync: bool,
    ) -> Result<(), Error> {
        self.group_commit_internal(
            SmallVec::from_iter(txs.into_iter().filter(|tx| tx.is_dirty())),
            sync,
        )
    }

    fn group_commit_internal(
        &self,
        mut txns: SmallVec<WriteTransaction, 4>,
        sync: bool,
    ) -> Result<(), Error> {
        if txns.is_empty() {
            return Ok(());
        }
        if txns.len() > crate::wal::MAX_ITEMS_PER_COMMIT {
            return Err(Error::validation("Too many transactions"));
        }
        let mut wb_readers = SmallVec::<WriteBatchReader, 4>::with_capacity(txns.len());
        for tx in &mut txns {
            if !Arc::ptr_eq(&tx.inner.env, &self.inner) {
                return Err(Error::validation(
                    "Transaction belongs to a different Environment",
                ));
            }
            tx.commit_start()?;
            if let Some(wb) = tx.wal_write_batch.get_mut() {
                wb_readers.push(wb.as_reader()?);
            } else {
                return Err(Error::validation("Transaction without Wal"));
            }
        }
        let mut wb_read =
            SmallVec::<_, 4>::from_iter(wb_readers.iter_mut().map(WriteBatchReader::as_wal_read));
        let wal_commit_idx = self.inner.wal.write(&mut wb_read)?;
        drop(wb_read);
        drop(wb_readers);
        if sync {
            if let Err(e) = self.inner.wal.sync_up_to(wal_commit_idx + 1) {
                self.inner.halt();
                return Err(e);
            }
        }
        let mut result = Ok(());
        for mut tx in txns {
            tx.0.state.get_mut().metapage.wal_end = wal_commit_idx + 1;
            // Commit finish failures are fatal for the database,
            // but the cross db atomicity was enacted in the wal.
            if let Err(e) = tx.0.commit_finish() {
                error!(
                    "Commit failure for db `{}` post group commit: {}",
                    tx.0.inner.env_db_name, e
                );
                result = result.and(Err(e));
            }
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn drop_wait(self) {
        let inner = self.inner.clone();
        drop(self);
        inner.advance_bg_thread();
        let _ = inner.bg_thread.get().unwrap().join();
        Arc::try_unwrap(inner).expect("Unique EnvironmentInner");
    }
}

#[cfg(any(test, fuzzing))]
impl Drop for Environment {
    fn drop(&mut self) {
        warn!("Drop for Environment");
    }
}

type SharedCache = quick_cache::sync::Cache<NodeCacheKey, UntypedNode, NodeWeighter>;

#[derive(PartialEq, Eq, Hash)]
pub struct NodeCacheKey(u128);

impl NodeCacheKey {
    #[inline]
    pub fn new(db_id: DbId, tx_id: TxId, page_id: PageId) -> Self {
        Self(db_id ^ ((tx_id as u128) << 32) ^ (page_id as u128))
    }
}

#[derive(Clone, Debug)]
pub struct NodeWeighter;

impl quick_cache::Weighter<NodeCacheKey, UntypedNode> for NodeWeighter {
    fn weight(&self, _key: &NodeCacheKey, val: &UntypedNode) -> u64 {
        val.page_size() as u64
    }
}
