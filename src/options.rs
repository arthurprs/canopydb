#[cfg_attr(fuzzing, allow(unused_imports))]
use crate::{
    error::{error_validation, Error},
    repr::{TreeId, TreeValue},
    shim::parking_lot::Mutex,
    utils, PAGE_SIZE,
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use triomphe::Arc;
use zerocopy::IntoBytes;

/// User callback to be called if the database encounters a fatal error
pub type HaltCallbackFn = Box<dyn FnMut() + Send + Sync + 'static>;

/// Options for an Environment
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct EnvOptions {
    pub(crate) path: PathBuf,
    /// Disables all calls to `fsync`. Takes precedence over all other durability options.
    ///
    /// Use cases include speeding up tests or throw away databases.
    ///
    /// Default: `false`
    pub disable_fsync: bool,
    /// Callback to be called when the environment encounters a fatal error and goes into read only mode.
    #[debug("{:?}", halt_callback.as_ref().map(Arc::as_ptr))]
    pub(crate) halt_callback: Option<Arc<Mutex<HaltCallbackFn>>>,
    /// If set to true, when the last object belonging to this Environment ([crate::Environment], [crate::Database],
    /// [crate::WriteTransaction], [crate::ReadTransaction]) is dropped, the last drop will also wait for the background
    /// thread(s) to exit.
    ///
    /// Default: `true`
    pub wait_bg_threads_on_drop: bool,
    /// Interval between periodic background triggered wal `fsync`.
    ///
    /// This puts an approximate bound on the amount of data loss when not running with sync commits.
    /// In practice application crashes do not cause data loss, only device crashes. This is because
    /// successful WAL writes (even if not fsyn'ed) are retained by the OS in case the application crashes.
    ///
    /// Default: `1 second`
    pub wal_background_sync_interval: Option<Duration>,
    /// Whether to use checksums on _all_ database pages, which can detect filesystem data corruption.
    /// Note that this is _NOT_ required for durability crash safety, only to detect filesystem corruption.
    ///
    /// Canopy is crash safe and memory safe (the Rust safety definition) _regardless_ of this option,
    /// but file corruption may lead to panics instead of [crate::Error] being returned.
    ///
    /// Default: `false`
    pub use_checksums: bool,
    /// The write batch buffers write transaction changes before they're written to the WAL file.
    /// This settings specifies a memory limit for the write batch, when the limit is exceeded
    /// the write batch automatically becomes a temporary file placed in [EnvOptions::wal_write_batch_tempfile_dir].
    ///
    /// Default: `8 MB`
    pub wal_write_batch_memory_limit: usize,
    /// Directory to use for write batch temporary files.
    ///
    /// In some systems it may be desireable to set this to the value of the `TMPDIR` environment variable.
    ///
    /// Default: `same folder as the Environment`
    pub wal_write_batch_tempfile_dir: PathBuf,
    /// Max WAL total file size before attempting to force Databases with the oldest WAL entries to checkpoint
    /// and thus allow the WAL to free space.
    ///
    /// This option usually becomes relevant when utilizing multiple databases in the same environment
    /// that have significantly different workloads. In such cases the less active database(s) may prevent the
    /// WAL tail from being truncated (due to un-checkpointed changes only present in the WAL) while other database(s)
    /// keep writing to the head of the WAL.
    ///
    /// Default: `256 MB`
    pub max_wal_size: u64,
    /// If set to true, a new WAL file is created after database checkpoints.
    ///
    /// This has the effect of spreading the WAL over approximately checkpoint sized files and thus
    /// reducing its total size. Disabling this can provide a small performance improvement in some cases.
    ///
    /// Default: `true`
    pub wal_new_file_on_checkpoint: bool,
    /// How long to wait when acquiring the environment file lock.
    ///
    /// Default: `5 seconds`
    pub file_lock_timeout: Duration,
    /// Database pages read from the Database files are cached in the page cache.
    ///
    /// Note that all databases in the same Environment share the page cache.
    ///
    /// Default: `1 GB`
    pub page_cache_size: usize,
}

#[derive(Debug, Deref)]
pub(crate) struct EnvDbOptions {
    pub env: Arc<EnvOptions>,
    pub path: PathBuf,
    #[deref]
    pub db: DbOptions,
}

/// Options for a Database
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct DbOptions {
    /// Whether to use the Write Ahead Log (WAL).
    ///
    /// Using the WAL allows databases to perform efficient durable commits and also atomically
    /// commit different databases in the same Environment. Each transaction using the WAL
    /// can be configured independently to wait for `fsyncs` or not (sync vs. async durability).
    /// See [DbOptions::default_commit_sync] and [EnvOptions::wal_background_sync_interval] for more information.
    ///
    /// Running without WAL can be useful when when the highest performance is desired,
    /// or periodic durability from checkpoints are sufficient,
    /// or when there's already an external WAL like system (e.g. Kafka).
    ///
    /// Default: `true`
    pub use_wal: bool,
    /// Default `sync` option used by [crate::WriteTransaction::commit].
    /// Can be overwritten in a per-commit basis with [crate::WriteTransaction::commit_with].
    ///
    /// Default: `false`
    pub default_commit_sync: bool,
    /// If set to true, when the [crate::Database] instance is dropped a background checkpoint will be triggered.
    ///
    /// While this introdudes an extra operation on Drop, it has the effect of lowering the startup time
    /// as there's no recovery step (replay WAL) to be performed. Applications interested in this may
    /// want to perform the checkpoint manually for more control over it.
    ///
    /// Default: `false`
    pub checkpoint_db_on_drop: bool,
    /// A background checkpoint will be triggered at the specified interval. The checkpoint will be performed
    /// even if the checkpoint size is below [DbOptions::checkpoint_target_size].
    ///
    /// Default: `30 seconds`
    pub checkpoint_interval: Duration,
    /// Memory limit for write transaction uncommitted changes before it starts to spill data to the database file.
    ///
    /// Note that this limit only applies to an open/uncommitted write transaction. This doesn't include previously
    /// committed changes from previous write transactions.
    ///
    /// Default: `64 MB`
    pub write_txn_memory_limit: usize,
    /// When the amount of committed changes exceed this treshold, a new checkpoint will be triggered in the
    /// background thread to move the in memory state to the database file, creating a new durable version of
    /// the database in the file.
    ///
    /// Larger checkpoints do not necessarily lead to better performance, and may cause too much free space to be
    /// kept in the file (more on that later). When expecting a small databases, consider lowering this value to
    /// reduce space amplification.
    ///
    /// Due to the checkpoint durability and other design trade-offs (e.g. no WAL full page writes like in Postgres),
    /// the checkpoint size will roughly map to the amount free-space present in the file (and WAL size, if used),
    /// even without deletion activity.
    ///
    /// Default: `64 MB`
    pub checkpoint_target_size: usize,
    /// When the committed updates to the database exceed this limit, the creation of new write transactions
    /// will be artificially throttled (a form of back pressure) to allow the checkpoint process to catchup.
    /// The value must be >= [DbOptions::checkpoint_target_size].
    ///
    /// Default: `256 MB`
    pub throttle_memory_limit: usize,
    /// When the committed updates to the database exceed this limit, the creation of new write transactions
    /// will block/stall until the checkpointer process can drain enough pages to lower the memory usage below
    /// the limit. The value must be >= [DbOptions::throttle_memory_limit].
    ///
    /// Default: `256 MB`
    pub stall_memory_limit: usize,
}

impl EnvOptions {
    /// Creates new Options at the specified directory path. The environment will be initialized if it doesn't exists.
    pub fn new(path: impl AsRef<std::path::Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        // On windows call canonicalize so we get UNC paths that support longer paths
        #[cfg(windows)]
        let path = path.canonicalize().unwrap_or(path);
        EnvOptions {
            wal_write_batch_tempfile_dir: path.clone(),
            path,
            disable_fsync: option_env!("CANOPYDB_DISABLE_FSYNC_DEFAULT").is_some(),
            halt_callback: None,
            use_checksums: false,
            wal_background_sync_interval: None,
            max_wal_size: 256 * 1024 * 1024,
            wait_bg_threads_on_drop: true,
            wal_write_batch_memory_limit: 8 * 1024 * 1024,
            page_cache_size: 1024 * 1024 * 1024,
            wal_new_file_on_checkpoint: true,
            file_lock_timeout: Duration::from_secs(5),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Sets the user callback to be called if the database encounters a fatal error
    pub fn set_halt_callback(&mut self, halt_callback: Option<HaltCallbackFn>) {
        self.halt_callback = halt_callback.map(Mutex::new).map(Arc::new);
    }
}

impl EnvDbOptions {
    pub fn new(path: PathBuf, env: Arc<EnvOptions>, db: DbOptions) -> Self {
        Self { env, path, db }
    }
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            use_wal: true,
            default_commit_sync: false,
            checkpoint_db_on_drop: false,
            checkpoint_interval: Duration::from_secs(30),
            write_txn_memory_limit: 64 * 1024 * 1024,
            checkpoint_target_size: 64 * 1024 * 1024,
            throttle_memory_limit: 256 * 1024 * 1024,
            stall_memory_limit: 256 * 1024 * 1024,
        }
    }
}

impl DbOptions {
    /// A new default DbOptions
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.default_commit_sync && !self.use_wal {
            // TODO: impl sync_commit w/o WAL via a checkpoint w/ an existing write lock
            return Err(Error::validation(
                "default_commit_sync = true requires use_wal = true",
            ));
        }
        if self.checkpoint_target_size > self.throttle_memory_limit
            || self.throttle_memory_limit > self.stall_memory_limit
        {
            return Err(Error::validation("memory limits must satisfy checkpoint_target_size <= throttle_memory_limit <= stall_memory_limit"));
        }
        Ok(())
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub(crate) fn read_from_db_folder(db_path: &Path) -> Result<Option<Self>, Error> {
        let path = db_path.join("OPTIONS");
        if path.exists() {
            Self::from_bytes(&std::fs::read(path)?).map(Some)
        } else {
            Ok(None)
        }
    }

    pub(crate) fn write_to_db_folder(&self, db_path: &Path) -> Result<bool, Error> {
        let path = db_path.join("OPTIONS");
        let bytes = self.to_bytes()?;
        if path.exists() && std::fs::read(&path)? == bytes {
            return Ok(false);
        }
        utils::atomic_file_write(&path, &bytes)?;
        utils::sync_dir(db_path)?;
        Ok(true)
    }
}

/// Options for a Tree
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TreeOptions {
    /// Specifies the fixed key length.
    /// Integers < 0 mean variable length keys.
    /// If fixed size (values >= 0), then fixed_value_len must also be set to a fixed length.  
    pub fixed_key_len: i8,
    /// Specified the fixed value length.
    /// Integers < 0 mean variable length keys.
    /// If fixed size (values >= 0), then fixed_key_len must also be set to a fixed length.
    pub fixed_value_len: i8,
    /// Whether to compress overflow values larger than `Some(value)`. Use `None` to disable.
    ///
    /// Page sizes are always multiple of 4KB. As a consequence, if a value between 4KB+ and 8KB compresses
    /// to more than 4KB, no space is saved. In such cases Canopydb will store the data uncompressed and
    /// there will only be a small amount overhead due to page id indirection.
    ///
    /// Compression/decompression has significant CPU costs, but it may be appropriate in some circumstances
    /// where a lot of space can be saved. In general, values larger than 12KB known to be very compressible
    /// (e.g. JSON or similar) are good candidates for compression.
    ///
    /// Compression is performed at checkpoint time, before writing the pages to the database file, meaning that
    /// even if a value is mutated multiple times between checkpoints it's still only compressed once per checkpoint.
    /// Similarly, compressed values read from the database file are cached in the page cache in their decompressed
    /// form, so if they're accessed often (or from multiple threads), then they're only decompressed once. This is
    /// usually significantly more efficient than performing individual compression of values before mutations and
    /// and decompression on reads.
    ///
    /// Note: Values <= `4KB` will have the same affect as `None`.
    /// Note: Values >= `1MB` will have the same affect as `1MB`.
    /// Note: Canopydb currently utilizes the LZ4 compression algorithm.
    ///
    /// Default: `12KB`
    pub compress_overflow_values: Option<u32>,
}

impl TreeOptions {
    /// A new default TreeOptions
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if (self.fixed_key_len < 0) != (self.fixed_value_len < 0) {
            return Err(Error::validation(
                "fixed_key_len and fixed_key_len must both be fixed or both be variable length",
            ));
        }
        Ok(())
    }

    pub(crate) fn to_value(&self, id: TreeId) -> TreeValue {
        // even if tree pages are small it can be useful to compress big overflow pages
        #[cfg(not(fuzzing))]
        let overflow_compressed = self.compress_overflow_values.map_or(0, |v| {
            let mp = v.div_ceil(PAGE_SIZE as u32);
            if mp >= 2 {
                mp.try_into().unwrap_or(u8::MAX)
            } else {
                0
            }
        });
        #[cfg(fuzzing)]
        let overflow_compressed = 1;
        // TODO: not exposed, but also have limited use cases
        let nodes_compressed = 0;
        let min_branch_node_pages = 1;
        let min_leaf_node_pages = 1;
        TreeValue {
            root: Default::default(),
            id,
            min_branch_node_pages,
            min_leaf_node_pages,
            fixed_key_len: self.fixed_key_len,
            fixed_value_len: self.fixed_value_len,
            level: 0,
            num_keys: 0,
            nodes_compressed,
            overflow_compressed,
        }
    }

    pub(crate) fn validate_value(&self, value: &TreeValue) -> Result<(), Error> {
        let options_kv_lens = (self.fixed_key_len, self.fixed_value_len);
        let existing_kv_lens = (value.fixed_key_len, value.fixed_value_len);
        if options_kv_lens != existing_kv_lens {
            return Err(error_validation!(
                "Fixed key/value len doesn't match options {options_kv_lens:?} existing {existing_kv_lens:?}"
            ));
        }
        Ok(())
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        [
            &[self.fixed_key_len as u8],
            &[self.fixed_value_len as u8],
            self.compress_overflow_values.unwrap_or_default().as_bytes(),
        ]
        .concat()
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != 6 {
            return Err(Error::validation(
                "Invalid number of bytes for decoding TreeOptions",
            ));
        }
        let compress_overflow_values = u32::from_ne_bytes(bytes[2..2 + 4].try_into().unwrap());
        let opts = Self {
            fixed_key_len: bytes[0] as i8,
            fixed_value_len: bytes[1] as i8,
            compress_overflow_values: (compress_overflow_values != 0)
                .then_some(compress_overflow_values),
        };
        opts.validate()?;
        Ok(opts)
    }
}

impl Default for TreeOptions {
    fn default() -> Self {
        Self {
            fixed_key_len: -1,
            fixed_value_len: -1,
            compress_overflow_values: Some(12 * 1024),
        }
    }
}
