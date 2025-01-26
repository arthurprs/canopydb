# Canopydb

[![Crates.io](https://img.shields.io/crates/v/canopydb.svg)](https://crates.io/crates/canopydb)
[![Docs](https://docs.rs/canopydb/badge.svg)](https://docs.rs/canopydb/latest)
[![CI](https://github.com/arthurprs/canopydb/actions/workflows/ci.yml/badge.svg)](https://github.com/arthurprs/canopydb/actions/workflows/ci.yml)

Embedded Key-Value Storage Engine

* Fully transactional
* Ordered map API similar to `std::collections::BTreeMap`
* B+Tree implementation with prefix and suffix truncation
* Handles large values efficiently, with optional transparent compression
* Efficient IO utilization, with lower read and write amplification compared to alternatives
* Efficient durable commits via an optional Write-Ahead-Log (WAL)
* Efficient async durability with background WAL fsyncs (e.g. every 500ms)
* Bounded recovery times using an optional Write-Ahead-Log (WAL)
* Concurrent write transactions with Optimistic Concurrency Control (OCC)
* ACID transactions with serializable snapshot isolation (SSI) or snapshot isolation (SI)
* Multi-Version-Concurrency-Control (MVCC) - writers do not block readers and vice versa
* Long running read transactions have limited impact on the database
* Supports larger than memory transactions
* Multiple key spaces per database - key space (Tree) management is fully transactional
* Multiple databases per environment - efficiently shares resources such as the WAL and page cache
* Supports cross database atomic commits

The storage engine is optimized for read heavy and read-modify-write workloads where a lightweight transactional key-value store is desired. If you need a different set of tradeoffs such as extreme random write performance, you're probably looking for [Fjall](https://github.com/fjall-rs/) or [RocksDb](https://github.com/facebook/rocksdb/).

## Examples

Basic usage

```rust
let sample_data: [(&[u8], &[u8]); 3] = [
    (b"baz", b"qux"),
    (b"foo", b"bar"),
    (b"qux", b"quux"),
];
let _dir = tempfile::tempdir().unwrap();
let db = canopydb::Database::new(_dir.path()).unwrap();
let tx = db.begin_write().unwrap();
{
    // multiple trees (keyspaces) per database, fully transactional
    let mut tree1 = tx.get_or_create_tree(b"tree1").unwrap();
    let mut tree2 = tx.get_or_create_tree(b"tree2").unwrap();
    for (k, v) in sample_data {
        tree1.insert(k, v).unwrap();
        tree2.insert(k, v).unwrap();
    }
    // the write transaction can read its own writes
    let maybe_value = tree1.get(b"foo").unwrap();
    assert_eq!(maybe_value.as_deref(), Some(&b"bar"[..]));
}
// commit to persist the changes
tx.commit().unwrap();

// a read only transaction
let rx = db.begin_read().unwrap();
let tree = rx.get_tree(b"tree2").unwrap().unwrap();
let maybe_value = tree.get(b"foo").unwrap();
assert_eq!(maybe_value.as_deref(), Some(&b"bar"[..]));
// range iterators like a BTreeMap
for kv_pair_result in tree.range(&b"foo"[..]..).unwrap() {
    let (db_k, db_v) = kv_pair_result.unwrap();
    println!("{db_k:?} => {db_v:?}");
}
// full scan of the tree
for kv_pair_result in tree.iter().unwrap() {
    let (db_k, db_v) = kv_pair_result.unwrap();
    println!("{db_k:?} => {db_v:?}");
}
```

Concurrent write transactions

```rust
const THREADS: usize = 4;
const INC_PER_THREAD: usize = 5_000;
let _dir = tempfile::tempdir().unwrap();
let db = canopydb::Database::new(_dir.path()).unwrap();

std::thread::scope(|scope| {
    for _thread in 0..THREADS {
        scope.spawn(|| {
            let mut successes = 0;
            while successes < INC_PER_THREAD {
                // This write txn will run concurrently with other concurrent transactions.
                // But they may conflict at commit time.
                let tx = db.begin_write_concurrent().unwrap();
                let mut tree = tx.get_or_create_tree(b"tree").unwrap();
                // Each thread will independently increment a counter INC_PER_THREAD times
                let prev = if let Some(value) = tree.get(b"key").unwrap() {
                    usize::from_ne_bytes(value.as_ref().try_into().unwrap())
                } else {
                    0
                };
                tree.insert(b"key", &(prev + 1).to_ne_bytes()).unwrap();
                drop(tree);
                match tx.commit() {
                    Ok(_tx_id) => {
                        successes += 1;
                    }
                    Err(canopydb::Error::WriteConflict) => {
                        // Conflict, retry...
                    }
                    Err(e) => panic!("Commit error: {e}"),
                }
            }
        });
    }
});
let rx = db.begin_read().unwrap();
let tree = rx.get_tree(b"tree").unwrap().unwrap();
let value = tree.get(b"key").unwrap().unwrap();
let value_usize = usize::from_ne_bytes(value.as_ref().try_into().unwrap());
println!("Final value: {value_usize}");
assert_eq!(THREADS * INC_PER_THREAD, value_usize);
```

Multiple databases per environment

```rust
let sample_data = [
    (&b"baz"[..], &b"qux"[..]),
    (&b"foo"[..], &b"bar"[..]),
    (&b"qux"[..], &b"quux"[..]),
];
let _dir = tempfile::tempdir().unwrap();
let mut options = canopydb::EnvOptions::new(_dir.path());
// all databases in the same environment will share this 1GB cache
options.page_cache_size = 1024 * 1024 * 1024;
let env = canopydb::Environment::new(_dir.path()).unwrap();

let db1 = env.get_or_create_database("db1").unwrap();
let db2 = env.get_or_create_database("db2").unwrap();
// Each database unique write transaction is independent.
let tx1 = db1.begin_write().unwrap();
let tx2 = db2.begin_write().unwrap();
let mut tree = tx1.get_or_create_tree(b"my_tree").unwrap();
tree.insert(b"foo", b"bar").unwrap();
drop(tree);
tx1.commit().unwrap();
tx2.rollback().unwrap();

// Write transactions for databases in the same environment
// can be committed together atomically.
// This allows stablishing a consistent state between them.
let tx1 = db1.begin_write().unwrap();
let tx2 = db2.begin_write().unwrap();
// Use tx1 and tx2 here..
env.group_commit([tx1, tx2], false).unwrap();
```

## Status

Canopydb should be considered early stage software and new releases could be incompatible. Do not trust it with production data.

Help is welcome to test and improve it, hopefully removing the disclaimer above. It's been an experimental project for many years and rewritten a few times. Even though it's reasonably well tested, there could be bugs and sharp API corners.

## Benchmarks

See the [BENCHMARKS.md](https://github.com/arthurprs/canopydb/blob/master/BENCHMARKS.md) file in the repository.

## Comparison with other databases

Note that these are only brief high-level comparisons.

### SQLite

Canopydb, like the rest of the list, is a lower level storage engine with an ordered key-value interface. SQLite is a full-featured relational database, that supports complex SQL queries (e.g. joins and aggregations). While both are embedded databases, SQLite may be better suited in applications that can take advantage of SQL, while Canopydb is optimized for speed and simplicity.

### LMDB

Canopydb and LMDB both implement transactional, ordered key-value embedded databases. Canopydb is implemented in pure Rust and provides a safe and flexible API in addition to other functionalities like concurrent write transactions. Whereas LMDB is implemented in C and has some potential complications such as the usage of memory-mapped files and relatively low max key size (> 511 bytes). Canopydb has a different MVCC implementation and uses an optional Write-Ahead-Log (WAL) which enables more efficient writes. LMDB doesn't use any background threads, whereas Canopydb has one background thread per Database.

### Redb

Canopydb and Redb are similar. Redb has a richer API that includes strongly typed tables, multi-tables and savepoints. Canopydb focuses on a byte oriented API, leaving type (de)serialization up to the user. Canopydb has a different MVCC implementation and an optional Write-Ahead-Log (WAL) which enables more efficient writes. Canopydb also supports concurrent write transactions and efficient transparent compression for large values. Redb doesn't use any background threads, whereas Canopydb has one background thread per Database.

### Fjall and Rocksdb

Rocksdb and Fjall both implement Log-Structured-Merge Trees (LSMs) with optional support for transactions. These implementations can achieve higher random write performance, read-free writes and lower space utilization. Although these may come with tradeoffs like utilizing more file descriptors, more CPU overhead and write transactions that must fit in memory.

## License

This project is licensed under the MIT license.
