use canopydb::{EnvOptions, Environment};
use tempfile::tempdir;

fn main() {
    let sample_data = [
        (&b"baz"[..], &b"qux"[..]),
        (&b"foo"[..], &b"bar"[..]),
        (&b"qux"[..], &b"quux"[..]),
    ];
    let _dir = tempdir().unwrap();
    let mut options = EnvOptions::new(_dir.path());
    // all databases in the same environment will share this 1GB cache
    options.page_cache_size = 1024 * 1024 * 1024;
    let env = Environment::with_options(options).unwrap();

    let db1 = env.get_or_create_database("db1").unwrap();
    let db2 = env.get_or_create_database("db2").unwrap();
    // Each database unique write transaction is independent.
    let tx1 = db1.begin_write().unwrap();
    let tx2 = db2.begin_write().unwrap();
    let mut tree = tx1.get_or_create_tree(b"my_tree").unwrap();
    for (k, v) in sample_data {
        tree.insert(k, v).unwrap();
    }
    let maybe_value = tree.get(b"foo").unwrap();
    assert_eq!(maybe_value.as_deref(), Some(&b"bar"[..]));
    drop(tree);
    tx1.commit().unwrap();
    tx2.rollback().unwrap();

    let tx1 = db1.begin_write().unwrap();
    let tx2 = db2.begin_write().unwrap();
    for tx in [&tx1, &tx2] {
        let mut tree: canopydb::Tree = tx.get_or_create_tree(b"my_tree").unwrap();
        for (k, v) in sample_data {
            tree.insert(k, v).unwrap();
        }
    }
    // Optionally, write transactions for databases in the same environment
    // can be committed together atomically.
    // This allows stablishing a consistent state between them.
    env.group_commit([tx1, tx2], false).unwrap();
}
