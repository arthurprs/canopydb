use canopydb::Database;
use tempfile::tempdir;

const TREE: &[u8] = b"tree";
const KEY: &[u8] = b"foo";
const THREADS: usize = 4;
const INC_PER_THREAD: usize = 5_000;

fn main() {
    let _dir = tempdir().unwrap();
    let db = Database::new(_dir.path()).unwrap();

    std::thread::scope(|scope| {
        for _thread in 0..THREADS {
            scope.spawn(|| {
                let mut successes = 0;
                while successes < INC_PER_THREAD {
                    // This write txn will run concurrently with other concurrent transactions.
                    // But they may conflict at commit time.
                    let tx = db.begin_write_concurrent().unwrap();
                    let mut tree = tx.get_or_create_tree(TREE).unwrap();
                    // Each thread will independently increment a counter INC_PER_THREAD times
                    let prev = if let Some(value) = tree.get(KEY).unwrap() {
                        usize::from_ne_bytes(value.as_ref().try_into().unwrap())
                    } else {
                        0
                    };
                    tree.insert(KEY, &(prev + 1).to_ne_bytes()).unwrap();
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
    let tree = rx.get_tree(TREE).unwrap().unwrap();
    let value = tree.get(KEY).unwrap().unwrap();
    let value_usize = usize::from_ne_bytes(value.as_ref().try_into().unwrap());
    println!("Final value: {value_usize}");
    assert_eq!(THREADS * INC_PER_THREAD, value_usize);
}
