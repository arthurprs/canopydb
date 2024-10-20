# Benchmarks

Since this crate is performance oriented it needs some comparisons. That said, benchmarks can be confusing and misleading, so take everything with a pinch of salt.

If you have questions or suggestion for improvement, please open an issue and/or PR.

* [redb benchmarks](#redb-benchmarks)
* [rust-storage-bench benchmarks](#rust-storage-bench-benchmarks)

## redb benchmarks

Benchmarks from <https://github.com/cberner/redb>, with [slight changes][redb commit]

[redb commit]: https://github.com/cberner/redb

* Databases are configured with sync commits and 4GB page cache when applicable
* The workload fits in memory and all commits are synchronous
* All operations are random (including bulk)
* Sled is listed for reference but it's *not* using transactions in the benchmark
* Rocksdb is using the TransactionalDb mode
* LMDB and Rocksdb are C/C++ codebases using their corresponding rust bindings
* These benchmarks have high variance
* Benchmarks ran in a x64 Linux OS with 32GB of main memory and an Intel i9-12900H CPU with turbo boost disabled

### redb `lmdb_benchmark`

|                           | redb       | canopydb       | sled         | sanakirja   | lmdb         | rocksdb        |
|---------------------------|------------|----------------|--------------|-------------|--------------|----------------|
| bulk load                 | 3.57s      | 3.20s          | 7.21s        | ***1.40s*** | 1.51s        | 8.59s          |
| individual writes         | 576.54ms   | ***410.94ms*** | 1.01s        | 1.61s       | 869.69ms     | 694.43ms       |
| batch writes              | 2.09s      | *658.77ms*     | 1.05s        | 1.68s       | 936.33ms     | **620.88ms**   |
| len()                     | 2.84µs     | *2.29µs*       | 622.00ms     | 39.66ms     | **325.00ns** | 302.61ms       |
| random reads              | 1.44s      | *1.12s*        | 2.35s        | 1.39s       | **959.03ms** | 3.72s          |
| random reads              | 1.39s      | *1.09s*        | 2.35s        | 1.39s       | **951.41ms** | 3.82s          |
| random range reads        | 3.86s      | 2.58s          | 7.57s        | *1.99s*     | **1.42s**    | 7.11s          |
| random range reads        | 3.90s      | 2.60s          | 7.78s        | *1.89s*     | **1.41s**    | 6.98s          |
| random reads (4 threads)  | 872.75ms   | *679.06ms*     | 1.30s        | 1.89s       | **475.84ms** | 1.92s          |
| random reads (8 threads)  | 591.19ms   | *439.06ms*     | 870.96ms     | 4.47s       | **304.67ms** | 1.43s          |
| random reads (16 threads) | 397.69ms   | *260.08ms*     | 512.59ms     | 11.06s      | **176.52ms** | 795.67ms       |
| random reads (32 threads) | 362.18ms   | *233.17ms*     | 435.64ms     | 12.19s      | **149.20ms** | 623.82ms       |
| bulk removals             | 2.51s      | 2.40s          | 3.41s        | *1.51s*     | **1.04s**    | 4.47s          |
| size pre-compact          | 771.51 MiB | 670.13 MiB     | *465.01 MiB* | 1020.00 MiB | 583.27 MiB   | **207.82 MiB** |
| compaction                | 956.68ms   | ***624.25ms*** | N/A          | N/A         | N/A          | N/A            |
| size after bench          | 341.20 MiB | *302.46 MiB*   | 465.01 MiB   | 1020.00 MiB | 583.27 MiB   | **207.82 MiB** |

Notes:

* best all-around in **bold** and best-rust-only in *italic*

How to reproduce:

* checkout [redb commit]
* run `cargo +stable bench --bench lmdb_benchmark`

### redb `int_benchmark`

```text
+-----------+--------+----------+--------+-----------+-------+---------+
|           | redb   | canopydb | sled   | sanakirja | lmdb  | rocksdb |
+======================================================================+
| bulk load | 5015ms | 631ms    | 5022ms | 679ms     | 772ms | 6770ms  |
+-----------+--------+----------+--------+-----------+-------+---------+
```

Notes:

* This benchmark bulk loads 1000000 u32 keys and u64 values in 1 transaction
* Despite the fixed key/value sizes, no special database configurations are utilized

How to reproduce:

* checkout [redb commit]
* run `cargo +stable bench --bench int_benchmark`

### redb `large_values_benchmark`

```text
+-----------+---------+----------+---------+---------+---------+
|           | redb    | canopydb | sled    | lmdb    | rocksdb |
+==============================================================+
| bulk load | 45522ms | 27797ms  | 42434ms | 10472ms | 27624ms |
+-----------+---------+----------+---------+---------+---------+
```

Notes:

* This benchmarks bulk loads 4000 2MB items and 1000000 small items in 1 transaction

How to reproduce:

* checkout [redb commit]
* run `cargo +stable bench --bench large_values_benchmark`
* Rocksdb uses *a lot* of memory in this benchmark and may OOM when reproducing

## `rust-storage-bench` benchmarks

<https://github.com/marvin-j97/rust-storage-bench> from the author of [Fjall](https://github.com/fjall-rs/) with [changes][storage bench commit].

[storage bench commit]: https://github.com/marvin-j97/rust-storage-bench

* Databases are configured with a 512MB page cache when applicable and memory is limited to 4GB.
* Values are 512B unless otherwise stated. The values are compressible, which is relevant for databases with compression.
* Reads, scans and overwrites follow a zipfian distribution (s=1) biased towards the most recent writes.
* Inserts target non-existing at a random point in the keyspace, unless specified as sequential.
* Async commit is used unless specified otherwise
* Single threaded benchmarks
* Fjall and Sled are *not* using transactions, which gives them an advantage.
* Redb doesn't have an async commit mode, so in the non sync-commit benchmarks it performs a sync commit every 1000th insert to avoid disk space blowout.
* LMDB doesn't have a safe async commit mode, so it's not included in async commit benchmarks.
* Sled doesn't have a safe sync commit mode, so it's not included in the sync commit benchmarks.
* Benchmarks ran in a x64 Linux OS and a Intel i9-12900H CPU with turbo boost disabled

### Workload H - 50% reads, 20% scans, 10% overwrites and 20% inserts

<img width="500" src="assets/rust-storage-bench/task_h_rand_512v.png">

### Workload H - 50% reads, 20% scans, 10% overwrites and 20% inserts (sequential)

<img width="500" src="assets/rust-storage-bench/task_h_seq_512v.png">

### Workload H (large) - 50% reads, 20% scans, 10% overwrites and 20% inserts

Bulk loaded with 5M entries. The intent here is to capture larger than memory performance. Databases with too low insert performance aren't shown.

<img width="500" src="assets/rust-storage-bench/task_h_large_rand_512v.png">

### Workload H (large) - 20K values, 50% reads, 20% scans, 10% overwrites and 20% inserts

Bulk loaded with 150K entries, values are 20KB. The intent here is to capture larger than memory performance for large values. Databases with too low insert performance aren't shown.

<img width="500" src="assets/rust-storage-bench/task_h_large_rand_20000v.png">

### Workload G - 95% inserts 5% reads (*sync* commit)

<img width="500" src="assets/rust-storage-bench/task_g_sync_rand_512v.png">

### Reproduction

How to reproduce:

* checkout the [commit][storage bench commit]
* run `bash run.sh`
  * Prefix the command with `systemd-run --scope -p MemoryLimit=4G` (on Linux) to limit memory of the benchmark
* enter the `page` folder and run `npm run dev`
* open <http://localhost:5173/rust-storage-bench/> and drag-and-drop related benchmark files to the page
