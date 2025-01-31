# Changelog

## Unreleased

## 0.2.0

- Added support for Concurrent Write Transactions
- Transaction Id semantics changed in order to support Concurrent Write Transactions
  - New tx-ids are now only assigned on commit. `commit()` returns the new tx-id when successful.
  - Write transactions `tx_id()` now return the commit tx-id the transaction snapshot is based on. Instead of `previous-tx-id + 1`.
- Optimized memory usage by reducing internal fragmentation of cached pages. Up to 25% less memory usage in jemalloc and others.
- Improved bulk allocation behavior when allocating large page spans
- Removed support for Memory Mapped Files (mmap)
- Fixed deadlock between compaction and write transactions

## 0.1.0

- initial release
