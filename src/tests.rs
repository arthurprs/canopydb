use crate::*;
use rand::prelude::*;
use std::{collections::BTreeMap, ops::Bound};
use sysinfo::Pid;
use tempfile::TempDir;

const COMP_DATASET: &[u8] = include_bytes!("../assets/gh_events.json");

fn get_rng() -> impl Rng + Clone {
    let seed: u64 = std::env::var("SEED")
        .map_or_else(|_| thread_rng().gen(), |seed_str| seed_str.parse().unwrap());
    println!("SEED {}", seed);
    SmallRng::seed_from_u64(seed)
}

fn very_rand_bytes(rng: &mut impl Rng, a: usize, b: usize) -> Vec<u8> {
    let len = rng.gen_range(a..=b);
    let mut buffer = vec![0; len];
    rng.fill_bytes(&mut buffer);
    buffer
}

fn rand_str(rng: &mut impl Rng, a: usize, b: usize) -> Vec<u8> {
    let len = rng.gen_range(a..=b);

    let start = rng.gen_range(0..COMP_DATASET.len() - len);
    COMP_DATASET[start..start + len].to_vec()
}

fn is_github_actions() -> bool {
    std::env::var("GITHUB_WORKFLOW").is_ok()
}

fn test_folder() -> TempDir {
    if let Ok(p) = std::env::var("TEST_DATA_FOLDER") {
        let _ = std::fs::create_dir_all(&p);
        tempfile::tempdir_in(&p).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    }
}

#[test]
fn test_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<Environment>();
    assert_send_sync::<Database>();
    assert_send::<ReadTransaction>();
    assert_send::<WriteTransaction>();
}

#[test]
fn env_single_db() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let env = Environment::new(_f.path()).unwrap();
    assert_eq!(env.inner.wal_trim_until(), None);

    let mut db_options = DbOptions::default();
    db_options.stall_memory_limit += 1;

    let mut db;
    {
        assert!(env.get_database("1").unwrap().is_none());
        db = env
            .get_or_create_database_with("1", db_options.clone())
            .unwrap();
        assert_eq!(db.inner.opts.db, db_options.clone());
        assert_eq!(env.inner.wal_trim_until(), Some(0));
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        tree.insert(b"before", b"before").unwrap();
        drop(tree);
        tx.commit().unwrap();
    }
    assert_eq!(env.inner.wal_trim_until(), Some(0));
    assert_eq!(env.inner.wal.tail(), 0);
    assert_eq!(env.inner.wal.head(), 1);
    drop(db);

    {
        db = env.get_database("1").unwrap().unwrap();
        assert_eq!(db.inner.opts.db, db_options.clone());
        let rx = db.begin_read().unwrap();
        let tree = rx.get_tree(b"default").unwrap().unwrap();
        assert_eq!(tree.get(b"before").unwrap().unwrap().as_ref(), b"before");
    }
    assert_eq!(env.inner.wal_trim_until(), Some(0));
    assert_eq!(env.inner.wal.tail(), 0);
    assert_eq!(env.inner.wal.head(), 1);
    drop(db);

    assert!(env.remove_database("1").unwrap());
    {
        assert!(env.get_database("1").unwrap().is_none());
        db = env
            .get_or_create_database_with("1", db_options.clone())
            .unwrap();
        let tx = db.begin_write().unwrap();
        assert!(tx.get_tree(b"default").unwrap().is_none());
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        tree.insert(b"after", b"after").unwrap();
        drop(tree);
        tx.commit().unwrap();
    }
    assert_eq!(env.inner.wal.tail(), 0);
    assert_eq!(env.inner.wal.head(), 2);
    assert_eq!(env.inner.wal_trim_until(), Some(1));
    drop(db);
    assert_eq!(env.inner.wal_trim_until(), Some(1));
    env.drop_wait();

    let env = Environment::new(_f.path()).unwrap();
    {
        db = env.get_database("1").unwrap().unwrap();
        assert_eq!(db.inner.opts.db, db_options.clone());
        let rx = db.begin_read().unwrap();
        let tree = rx.get_tree(b"default").unwrap().unwrap();
        assert_eq!(tree.get(b"after").unwrap().unwrap().as_ref(), b"after");
        db.checkpoint().unwrap();
    }
    drop(db);
    {
        // re-opening with different DbOptions
        db = env
            .get_database_with("1", DbOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(db.inner.opts.db, DbOptions::default());
        let rx = db.begin_read().unwrap();
        let tree = rx.get_tree(b"default").unwrap().unwrap();
        assert_eq!(tree.get(b"after").unwrap().unwrap().as_ref(), b"after");
        db.checkpoint().unwrap();
    }
    env.inner.advance_bg_thread();
    assert_eq!(env.inner.wal_trim_until(), Some(2));
    assert_eq!(env.inner.wal.tail(), 2);
    assert_eq!(env.inner.wal.head(), 2);
    db.drop_wait();
    assert_eq!(env.inner.wal_trim_until(), None);
    env.drop_wait();
}

#[test]
fn env_max_wal_size() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let mut env_opts = EnvOptions::new(_f.path());
    env_opts.max_wal_size = 1024;
    let env = Environment::with_options(env_opts).unwrap();
    let mut db_opts = DbOptions::new();
    db_opts.checkpoint_interval = Duration::MAX;
    assert_eq!(env.inner.wal_trim_until(), None);

    let writes = |db: &Database| {
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for i in 0..100 {
            tree.insert(i.to_string().as_bytes(), i.to_string().as_bytes())
                .unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
    };
    let wait_wal_empty = || {
        for _ in 0..10 {
            env.inner.advance_bg_thread();
            if env.inner.wal.disk_size() == 0 {
                break;
            }
            thread::sleep(Duration::from_secs(1));
        }
    };
    {
        let db1 = env.get_or_create_database("1").unwrap();
        let db2 = env.get_or_create_database("2").unwrap();
        for db in [&db1, &db2] {
            assert_eq!(db.inner.wal_range(), 0..0);
            assert_eq!(db.inner.wal_tail(), None);
            writes(db);
        }
        wait_wal_empty();
        assert_eq!(env.inner.wal.num_files(), 1);
        assert!(db1.inner.is_fully_checkpointed());
        assert!(db2.inner.is_fully_checkpointed());
        assert_eq!(env.inner.wal_trim_until(), Some(2));

        let db3 = env.get_or_create_database("3").unwrap();
        assert_eq!(db3.inner.wal_range(), 2..2);
        assert_eq!(db3.inner.wal_tail(), None);
        assert_eq!(env.inner.wal_trim_until(), Some(2));
        writes(&db3);
        wait_wal_empty();
        assert!(db3.inner.is_fully_checkpointed());
        assert_eq!(env.inner.wal_trim_until(), Some(3));

        assert_eq!(db1.inner.wal_range(), 1..1);
        assert_eq!(db2.inner.wal_range(), 2..2);
        assert_eq!(db3.inner.wal_range(), 3..3);
    }
    env.drop_wait();
}

#[test]
fn env_many_dbs() {
    const THREADS: usize = 8;
    const WRITES_PER_THREAD: usize = 500;
    let _ = env_logger::try_init();
    let rng = get_rng();
    let _f = test_folder();
    let env = Arc::new(Environment::new(_f.path()).unwrap());
    let b = Arc::new(std::sync::Barrier::new(THREADS));
    let mut threads = Vec::new();
    for i in 0..THREADS {
        let env = env.clone();
        let b = b.clone();
        let mut rng = rng.clone();
        let t = thread::spawn(move || {
            let db = Arc::new(env.get_or_create_database(&i.to_string()).unwrap());
            b.wait();
            let mut writes = BTreeMap::new();
            for _round in 0..WRITES_PER_THREAD {
                dbg!(_round);
                let sample: BTreeMap<_, _> = (0..35)
                    .map(|_| {
                        let a = rand_str(&mut rng, 3, 500);
                        let b = rand_str(&mut rng, 3, 500);
                        (a, b)
                    })
                    .collect();
                let tx = db.begin_write().unwrap();
                let mut tree = tx.get_or_create_tree(b"default").unwrap();
                for (k, v) in &sample {
                    tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
                }
                drop(tree);
                tx.commit().unwrap();
                if _round == WRITES_PER_THREAD / 2 {
                    let db = db.clone();
                    thread::spawn(move || {
                        db.checkpoint().unwrap();
                    });
                }
                writes.extend(sample);
            }
            db.validate_free_space().unwrap();
            writes
        });
        threads.push(t);
    }
    let mut thread_results = Vec::new();
    for t in threads {
        thread_results.push(t.join().unwrap());
    }
    Arc::try_unwrap(env).unwrap().drop_wait();

    eprintln!("Re-opening env");
    let env = Arc::new(Environment::new(_f.path()).unwrap());
    eprintln!("Checking");
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();
    for (i, sample) in thread_results.into_iter().enumerate() {
        let env = env.clone();
        let t = thread::spawn(move || {
            let db = env.get_or_create_database(&i.to_string()).unwrap();
            db.validate_free_space().unwrap();
            let rx = db.begin_read().unwrap();
            let tree = rx.get_tree(b"default").unwrap().unwrap();
            for (k, _v) in sample {
                assert!(tree.get(k.as_bytes()).unwrap().is_some());
            }
        });
        threads.push(t);
    }
    for t in threads {
        t.join().unwrap();
    }
    Arc::try_unwrap(env).unwrap().drop_wait();
}

#[test]
fn read_cursor_works() {
    let mut rng = get_rng();
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();

    let mut sample: BTreeMap<_, _> = (0..50_000)
        .map(|_| {
            let a = rand_str(&mut rng, 1, 500);
            let b = rand_str(&mut rng, 1, 500);
            (a, b)
        })
        .collect();

    let mut iter = sample.iter();
    for _ in 0..sample.len().div_ceil(1000) {
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for (k, v) in iter.by_ref().take(1000) {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
    }
    let tx = db.begin_write().unwrap();
    let mut tree = tx.get_or_create_tree(b"default").unwrap();
    for (k, v) in sample.iter_mut().take(1000) {
        v.reverse();
        tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
    }
    drop(tree);
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let tree = tx.get_or_create_tree(b"default").unwrap();
    let mut cursor = tree.cursor();
    assert!(cursor.first().unwrap());
    for (i, (k, v)) in sample.iter().enumerate() {
        let peek = cursor.peek().unwrap().unwrap();
        assert_eq!(peek, (k.as_bytes(), v.as_bytes()));
        assert_eq!(cursor.next().unwrap(), i + 1 < sample.len());
    }
    assert!(!cursor.next().unwrap());
    assert!(!cursor.prev().unwrap());

    assert!(cursor.last().unwrap());
    for (i, (k, v)) in sample.iter().rev().enumerate() {
        let peek = cursor.peek().unwrap().unwrap();
        assert_eq!(peek, (k.as_bytes(), v.as_bytes()));
        assert_eq!(cursor.prev().unwrap(), i + 1 < sample.len());
    }
    assert!(!cursor.prev().unwrap());
    assert!(!cursor.next().unwrap());

    for ratio in [1, 2, 3, 4, 5] {
        let sample_size = sample.len() / ratio;

        // Go to the start, go forward, then go backwards
        assert!(cursor.first().unwrap());
        let mut seen = Vec::new();
        for (i, (k, v)) in sample.iter().take(sample_size).enumerate() {
            seen.push((k, v));
            let peek = cursor.peek().unwrap().unwrap();
            assert_eq!(peek, (k.as_bytes(), v.as_bytes()));
            if i + 1 < sample_size {
                assert!(cursor.next().unwrap());
            }
        }
        for (i, (k, v)) in seen.iter().rev().enumerate() {
            let peek = cursor.peek().unwrap().unwrap();
            assert_eq!(peek, (k.as_bytes(), v.as_bytes()));
            assert_eq!(cursor.prev().unwrap(), i + 1 < seen.len());
        }

        // Go to the end, go back, then go forward
        assert!(cursor.last().unwrap());
        let mut seen = Vec::new();
        for (i, (k, v)) in sample.iter().rev().take(sample_size).enumerate() {
            seen.push((k, v));
            let peek = cursor.peek().unwrap().unwrap();
            assert_eq!(peek, (k.as_bytes(), v.as_bytes()));
            if i + 1 < sample_size {
                assert!(cursor.prev().unwrap());
            }
        }
        for (i, (k, v)) in seen.iter().rev().enumerate() {
            let peek = cursor.peek().unwrap().unwrap();
            assert_eq!(peek, (k.as_bytes(), v.as_bytes()));
            assert_eq!(cursor.next().unwrap(), i + 1 < seen.len());
        }
    }
}

#[test]
fn range_works() {
    range_works_stub(100, 0);
    range_works_stub(1_000, 1);
    range_works_stub(50_000, 2);
}

fn range_works_stub(items: u32, root_level: u8) {
    let mut rng = get_rng();
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();
    let tx = db.begin_write().unwrap();
    let mut tree = tx.get_or_create_tree(b"default").unwrap();
    let mut model = BTreeMap::new();
    for i in 1..=items * 2 {
        if i % 2 == 0 {
            continue;
        }
        tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        model.insert(i, i);
    }
    assert_eq!(tree.value.level, root_level);
    drop(tree);
    tx.commit().unwrap();

    let mut rng_ = rng.clone();
    let mut rng_bound = || {
        if rng_.gen_bool(0.01) {
            Bound::Unbounded
        } else if rng_.gen_bool(0.5) {
            Bound::Excluded(rng_.gen_range(0..=items * 2 + 1))
        } else {
            Bound::Included(rng_.gen_range(0..=items * 2 + 1))
        }
    };

    let rx = db.begin_read().unwrap();
    let tree = rx.get_tree(b"default").unwrap().unwrap();
    for _ in 0..500 {
        let (a, b) = (rng_bound(), rng_bound());
        let (a_bytes, b_bytes) = (a.map(u32::to_be_bytes), b.map(u32::to_be_bytes));
        let results = tree
            .range((a_bytes, b_bytes))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let results_rev = tree
            .range((a_bytes, b_bytes))
            .unwrap()
            .rev()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let invalid_range = match (a, b) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a), Bound::Excluded(b)) => a > b,
            (Bound::Excluded(a), Bound::Included(b)) => a >= b,
            (Bound::Excluded(a), Bound::Excluded(b)) => a >= b,
            _ => false,
        };
        // Btreemap panics if a > b, but we can still assert our result are empty
        if invalid_range {
            assert!(results.is_empty());
            assert!(results_rev.is_empty());
            continue;
        }
        let expect = model.range((a.as_ref(), b.as_ref())).collect::<Vec<_>>();
        for ((rk, rv), (ek, ev)) in results.iter().zip(expect.iter()) {
            let result = (rk.as_ref(), rv.as_ref());
            let expect = (&ek.to_be_bytes()[..], &ev.to_be_bytes()[..]);
            assert_eq!(result, expect);
        }
        for ((rk, rv), (ek, ev)) in results_rev.iter().rev().zip(expect.iter()) {
            let result = (rk.as_ref(), rv.as_ref());
            let expect = (&ek.to_be_bytes()[..], &ev.to_be_bytes()[..]);
            assert_eq!(result, expect);
        }
        assert_eq!(results.len(), expect.len());
        assert_eq!(results_rev.len(), expect.len());

        let mut start = Vec::new();
        let mut end = Vec::new();
        let mut range = tree.range((a_bytes, b_bytes)).unwrap().fuse();

        loop {
            let before = start.len() + end.len();
            if rng.gen_bool(0.5) {
                if let Some(Ok(a)) = range.next() {
                    start.push(a);
                }
            } else if let Some(Ok(a)) = range.next_back() {
                end.push(a);
            }
            if start.len() + end.len() == before {
                break;
            }
        }
        start.extend(end.into_iter().rev());
        assert_eq!(start.len(), expect.len());
        for ((rk, rv), (ek, ev)) in start.iter().zip(expect.iter()) {
            let result = (rk.as_ref(), rv.as_ref());
            let expect = (&ek.to_be_bytes()[..], &ev.to_be_bytes()[..]);
            assert_eq!(result, expect);
        }
        assert_eq!(start.len(), expect.len());
    }
}

#[test]
fn delete_range_works() {
    delete_range_stub(100, 0);
    delete_range_stub(1_000, 1);
    delete_range_stub(50_000, 2);
}

fn delete_range_stub(items: u32, root_level: u8) {
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();
    let tx = db.begin_write().unwrap();
    let mut tree = tx.get_or_create_tree(b"default").unwrap();
    let mut model = BTreeMap::new();
    for i in 1..=items * 2 {
        if i % 2 == 0 {
            continue;
        }
        tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        model.insert(i, i);
    }
    assert_eq!(tree.value.level, root_level);
    drop(tree);
    tx.commit().unwrap();

    let mut rng_ = get_rng();
    let mut rng_bound = || {
        if rng_.gen_bool(0.01) {
            Bound::Unbounded
        } else if rng_.gen_bool(0.5) {
            Bound::Excluded(rng_.gen_range(0..=items * 2 + 1))
        } else {
            Bound::Included(rng_.gen_range(0..=items * 2 + 1))
        }
    };

    for _ in 0..500 * (root_level.max(1) as usize) {
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_tree(b"default").unwrap().unwrap();
        let mut model = model.clone();
        let (a, b) = (rng_bound(), rng_bound());
        let (a_bytes, b_bytes) = (a.map(u32::to_be_bytes), b.map(u32::to_be_bytes));
        tree.delete_range((a_bytes, b_bytes)).unwrap();
        model.retain(|&k, _| {
            !(match a {
                Bound::Included(a) => k >= a,
                Bound::Excluded(a) => k > a,
                Bound::Unbounded => true,
            } && match b {
                Bound::Included(b) => k <= b,
                Bound::Excluded(b) => k < b,
                Bound::Unbounded => true,
            })
        });
        assert_eq!(tree.len() as usize, model.len());
        let results = tree.iter().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        for ((rk, rv), (ek, ev)) in results.iter().zip(model.iter()) {
            let result = (rk.as_ref(), rv.as_ref());
            let expect = (&ek.to_be_bytes()[..], &ev.to_be_bytes()[..]);
            assert_eq!(result, expect);
        }
    }

    {
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_tree(b"default").unwrap().unwrap();
        tree.delete_range::<&[u8]>(..).unwrap();
        assert_eq!(tree.len(), 0);
    }

    {
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_tree(b"default").unwrap().unwrap();
        tree.delete_range(..(&(items * 2 - 1).to_be_bytes()[..]))
            .unwrap();
        assert_eq!(tree.len(), 1);
    }
    {
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_tree(b"default").unwrap().unwrap();
        tree.delete_range((Bound::Included(2u32.to_be_bytes()), Bound::Unbounded))
            .unwrap();
        assert_eq!(tree.len(), 1);
    }
}

#[test]
fn multi_writer() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let mut options = EnvOptions::new(_f.path());
    // all databases in the same environment will share this 1GB cache
    options.page_cache_size = 1024 * 1024 * 1024;
    let env = Environment::with_options(options).unwrap();

    let db1 = env.get_or_create_database("db1").unwrap();
    let tx1 = db1.begin_write().unwrap();
    let mut tree1 = tx1.get_or_create_tree(b"my_tree").unwrap();
    for i in 0..200 {
        tree1.insert(format!("foo{i}").as_bytes(), b"bar").unwrap();
    }
    drop(tree1);
    tx1.commit().unwrap();
    // Each database unique write transaction is independent.
    let tx1 = db1.begin_write().unwrap();
    let tx2 = db1.begin_write().unwrap();
    let mut tree1 = tx1.get_or_create_tree(b"my_tree").unwrap();
    let mut tree2 = tx2.get_or_create_tree(b"my_tree").unwrap();
    tree1.insert(b"foo0", b"bar").unwrap();
    tree2.insert(b"foo99", b"bar").unwrap();
    let maybe_value = tree1.get(b"foo0").unwrap();
    assert_eq!(maybe_value.as_deref(), Some(&b"bar"[..]));
    drop(tree1);
    drop(tree2);
    tx1.commit().unwrap();
    tx2.commit().unwrap();
}

#[test]
fn read_tx_snapshot_works() {
    let _ = env_logger::try_init();
    let mut rng = get_rng();
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();

    let sample: HashMap<_, _> = (0..1_000)
        .map(|_| {
            let a = rand_str(&mut rng, 3, 500);
            let b = rand_str(&mut rng, 3, 5000);
            (a, b)
        })
        .collect();

    let tx = db.begin_write().unwrap();
    let mut tree = tx.get_or_create_tree(b"default").unwrap();
    for (k, v) in &sample {
        tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
    }
    drop(tree);
    tx.commit().unwrap();

    {
        let rx = db.begin_read().unwrap();
        let tree = rx.get_tree(b"default").unwrap().unwrap();
        for (k, _v) in &sample {
            assert!(tree.get(k.as_bytes()).unwrap().is_some());
        }
    }

    let tx = db.begin_write().unwrap();
    tx.get_tree(b"default").unwrap().unwrap().clear().unwrap();
    {
        let rx = db.begin_read().unwrap();
        let tree = rx.get_tree(b"default").unwrap().unwrap();
        for (k, _v) in &sample {
            assert!(tree.get(k.as_bytes()).unwrap().is_some());
        }
    }
    tx.commit().unwrap();
    {
        let rx = db.begin_read().unwrap();
        let tree = rx.get_tree(b"default").unwrap().unwrap();
        for (k, _v) in &sample {
            assert!(tree.get(k.as_bytes()).unwrap().is_none());
        }
    }
}

#[test]
fn delete_works() {
    let _ = env_logger::try_init();
    let mut rng = get_rng();
    let _f = test_folder();
    let env_opts = EnvOptions::new(_f.path());
    let start = Instant::now();
    let db_opts = DbOptions::new();
    let db = Database::with_options(env_opts, db_opts).unwrap();
    let mut master_sample = BTreeMap::new();

    for _round_no in 0..1_000 {
        eprintln!(
            "Round {} used {}, elapsed {:?}",
            _round_no,
            ByteSize(db.inner.page_table.spans_used() as u64 * PAGE_SIZE),
            start.elapsed()
        );
        let sample: BTreeMap<_, _> = (0..500)
            .map(|_| {
                let a = rand_str(&mut rng, 3, 500);
                let b = rand_str(&mut rng, 3, 500);
                (a, b)
            })
            .collect();

        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        master_sample.extend(sample.iter().map(|(k, v)| (k.clone(), v.clone())));

        if _round_no == 500 {
            for (k, _v) in mem::take(&mut master_sample) {
                let deleted = tree.delete(k.as_bytes()).unwrap();
                assert!(deleted);
            }

            let mut cursor = tree.cursor();
            assert!(!cursor.first().unwrap());
            drop(cursor);
            assert_eq!(tree.len(), 0);
        } else if _round_no % 100 == 0 {
            for k in master_sample
                .keys()
                .choose_multiple(&mut rng, master_sample.len() / 10)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
            {
                // println!("deleting {:?}", &k);
                let deleted = tree.delete(k.as_bytes()).unwrap();
                assert!(deleted);
                let deleted = tree.delete(k.as_bytes()).unwrap();
                assert!(!deleted);
                master_sample.remove(&k).unwrap();
            }
        }
        drop(tree);
        tx.commit().unwrap();
        if _round_no % 100 == 0 {
            db.checkpoint().unwrap();
        }
    }

    db.validate_free_space().unwrap();
    drop(db);

    let db = Database::new(_f.path()).unwrap();
    let tx = db.begin_write().unwrap();
    let mut tree = tx.get_or_create_tree(b"default").unwrap();
    let mut shuffled_master_sample = master_sample.into_iter().collect::<Vec<_>>();
    shuffled_master_sample.shuffle(&mut rng);
    for (k, _v) in shuffled_master_sample {
        let deleted = tree.delete(k.as_bytes()).unwrap();
        assert!(deleted);
    }
    let mut cursor = tree.cursor();
    assert!(!cursor.first().unwrap());
    drop(cursor);
    assert_eq!(tree.len(), 0);
    drop(tree);
    tx.commit().unwrap();
}

#[test]
fn test_file_truncation() {
    let _ = env_logger::try_init();
    let mut rng = get_rng();
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();

    const ROUNDS: usize = 10;
    const OPS_PER_ROUND: usize = 250;
    for _ in 0..ROUNDS {
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for _ in 0..OPS_PER_ROUND {
            tree.insert(
                rand_str(&mut rng, 1000, 5000).as_bytes(),
                rand_str(&mut rng, 5000, 50000).as_bytes(),
            )
            .unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
    }
    db.checkpoint().unwrap();
    let before_del = std::fs::metadata(db.inner.opts.path.join("DATA"))
        .unwrap()
        .len();

    eprintln!("Drain starting");
    // drain the database empty in steps
    // so metadata is moved to the beginning of the file
    loop {
        let rx = db.begin_read().unwrap();
        let rx_tree = rx.get_tree(b"default").unwrap().unwrap();
        if rx_tree.len() <= (ROUNDS * OPS_PER_ROUND / 2) as u64 {
            break;
        }
        let tx = db.begin_write().unwrap();
        let mut tx_tree = tx.get_tree(b"default").unwrap().unwrap();
        for k_result in rx_tree.keys().unwrap().rev().take(25) {
            assert!(tx_tree.delete(&k_result.unwrap()).unwrap());
        }
        drop(rx_tree);
        drop(rx);
        eprintln!("Commit deletions");
        drop(tx_tree);
        tx.commit().unwrap();
    }
    db.compact().unwrap();
    db.validate_free_space().unwrap();

    let after_del = std::fs::metadata(db.inner.opts.path.join("DATA"))
        .unwrap()
        .len();
    dbg!(ByteSize(after_del), ByteSize(before_del));
    assert!(after_del < before_del, "{} ! < {}", after_del, before_del);

    // re-open database as a way to validate its state
    drop(db);
    let db = Database::new(_f.path()).unwrap();
    db.validate_free_space().unwrap();
}

#[test]
fn test_snapshot_release() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let env_opts = EnvOptions::new(_f.path());
    let mut db_opts = DbOptions::new();
    db_opts.checkpoint_interval = Duration::MAX;
    db_opts.checkpoint_target_size = usize::MAX;
    db_opts.throttle_memory_limit = usize::MAX;
    db_opts.stall_memory_limit = usize::MAX;
    let db = Database::with_options(env_opts, db_opts).unwrap();

    let write = |db: &Database, range: Range<u64>| {
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for i in range {
            tree.insert(&i.to_be_bytes(), vec![0; 1000].as_slice())
                .unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
    };

    write(&db, 0..10);
    db.checkpoint().unwrap();
    assert_eq!(db.inner.old_snapshots.lock().len(), 1);
    write(&db, 0..10);
    db.checkpoint().unwrap();
    assert_eq!(db.inner.old_snapshots.lock().len(), 1);
    db.begin_write().unwrap().commit().unwrap();
    assert_eq!(db.inner.old_snapshots.lock().len(), 1);
    write(&db, 0..10);
    let rx = db.begin_read().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.inner.old_snapshots.lock().len(), 2);
    write(&db, 0..10);
    assert_eq!(db.inner.old_snapshots.lock().len(), 2);
    drop(rx);
    write(&db, 0..10);
    assert_eq!(db.inner.old_snapshots.lock().len(), 1);
}

#[test]
fn write_to_pages() {
    let _f = test_folder();
    let mut env_opts = EnvOptions::new(_f.path());
    env_opts.use_mmap = false;

    let db_opts = DbOptions::new();
    let db = Database::with_options(env_opts, db_opts).unwrap();
    let data = vec![0; 50_000];
    let mut pages = Freelist::default();
    pages.free(3, 1).unwrap();
    assert!(matches!(
        db.inner.write_to_pages(&data, pages.clone()),
        Err(Error::Validation(e))
        if *e == "write_to_pages spans are not enough to fit data, 45920 bytes remaining"
    ));
    for i in 100..10000 {
        pages.free(i * 2, 1).unwrap();
    }
    assert!(matches!(
        db.inner.write_to_pages(&data, pages.clone()),
        Err(Error::Validation(e))
        if *e == "write_to_pages cannot allocate initial span of 10"
    ));
    pages.free(30, 10).unwrap();
    let root = db.inner.write_to_pages(&data, pages.clone()).unwrap();
    let (read_pages, read_data) = db.inner.read_from_pages(root, true).unwrap();
    assert_eq!(read_pages, pages);
    assert_eq!(&read_data[..data.len()], &data[..]);

    pages.remove(100 * 2, 10000 * 2);
    pages.free(6, 2).unwrap();
    pages.free(10, 2).unwrap();
    let root = db.inner.write_to_pages(&data, pages.clone()).unwrap();
    let (read_pages, read_data) = db.inner.read_from_pages(root, true).unwrap();
    assert_eq!(read_pages, pages);
    assert_eq!(&read_data[..data.len()], &data[..]);
}

#[test]
fn insert_big_kvs_works() {
    let _ = env_logger::try_init();
    let mut rng = get_rng();
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();

    let mut guard = TestGuard::new();
    let mut master_sample = HashMap::default();

    let mut rounds = 1_000;
    if is_github_actions() {
        rounds /= 10;
    }

    for _round_no in 0..rounds {
        eprintln!("Round {}", _round_no);
        let sample: Vec<_> = (0..50)
            .map(|_| {
                let a = rand_str(&mut rng, 3, 5_000);
                let b = rand_str(&mut rng, 3, 50_000);
                (a, b)
            })
            .collect();

        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
            assert_eq!(
                tree.get(k.as_bytes()).unwrap().as_deref(),
                Some(v.as_bytes())
            );
        }
        drop(tree);
        tx.commit().unwrap();
        master_sample.extend(sample.into_iter());
    }

    db.checkpoint().unwrap();
    db.validate_free_space().unwrap();
    guard.print();

    println!(
        "unique data {} keys, {}, data file {}, free space {}",
        master_sample.len(),
        ByteSize(
            master_sample
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>() as u64
        ),
        ByteSize(db.inner.file.file_len()),
        ByteSize(db.inner.allocator.lock().free.len() as u64 * PAGE_SIZE)
    );

    db.validate_free_space().unwrap();
    drop(db);

    let db = Database::new(_f.path()).unwrap();
    let tx = db.begin_read().unwrap();
    let tree = tx.get_tree(b"default").unwrap().unwrap();

    for (k, v) in &master_sample {
        assert_eq!(
            tree.get(k.as_bytes()).unwrap().as_deref(),
            Some(v.as_bytes())
        );
    }
}

#[test]
fn two_trees_one_tx() {
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();

    let tx = db.begin_write().unwrap();
    let mut tree_a = tx.get_or_create_tree(b"a").unwrap();
    tree_a.insert(b"a1", b"a1").unwrap();
    let mut tree_b = tx.get_or_create_tree(b"b").unwrap();
    tree_b.insert(b"b1", b"b1").unwrap();
    drop(tree_a);
    drop(tree_b);
    tx.commit().unwrap();
}

#[test]
fn tree_already_used() {
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();

    let tx = db.begin_write().unwrap();
    let tree_a = tx.get_or_create_tree(b"a").unwrap();
    let _tree_b = tx.get_or_create_tree(b"b").unwrap();
    drop(tree_a);
    let _tree_a = tx.get_or_create_tree(b"a").unwrap();
    tx.get_or_create_tree(b"a").unwrap_err();
}

#[test]
fn tree_manipulation() {
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();

    let tx = db.begin_write().unwrap();
    let mut tree_a = tx.get_or_create_tree(b"a").unwrap();
    tree_a.insert(b"a", b"a").unwrap();
    assert_eq!(tree_a.len(), 1);
    drop(tree_a);
    tx.delete_tree(b"a").unwrap();
    assert!(tx.get_tree(b"a").unwrap().is_none());
    let mut tree_a = tx.get_or_create_tree(b"a").unwrap();
    assert_eq!(tree_a.len(), 0);
    tree_a.insert(b"a", b"a").unwrap();
    drop(tree_a);
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let tree_a = tx.get_tree(b"a").unwrap().unwrap();
    assert_eq!(tree_a.len(), 1);
    drop(tree_a);
    tx.delete_tree(b"a").unwrap();
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    assert!(tx.get_tree(b"a").unwrap().is_none());
    let mut tree_a = tx.get_or_create_tree(b"a").unwrap();
    tree_a.insert(b"a", b"a").unwrap();
    drop(tree_a);
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let mut tree_a = tx.get_tree(b"a").unwrap().unwrap();
    assert_eq!(tree_a.len(), 1);
    tree_a.clear().unwrap();
    assert_eq!(tree_a.len(), 0);
    drop(tree_a);
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let mut tree_a = tx.get_or_create_tree(b"a").unwrap();
    assert_eq!(tree_a.len(), 0);
    tree_a.insert(b"a", b"a").unwrap();
    drop(tree_a);
    assert!(tx.rename_tree(b"a", b"c").unwrap());
    tx.delete_tree(b"c").unwrap();
    assert!(tx.get_tree(b"a").unwrap().is_none());
    assert!(tx.get_tree(b"c").unwrap().is_none());
    let mut tree_a = tx.get_or_create_tree(b"a").unwrap();
    assert_eq!(tree_a.len(), 0);
    tree_a.insert(b"a", b"a").unwrap();
    drop(tree_a);
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let tree_a = tx.get_tree(b"a").unwrap().unwrap();
    assert_eq!(tree_a.len(), 1);
    drop(tree_a);
    assert!(tx.rename_tree(b"a", b"c").unwrap());
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let tree_a = tx.get_tree(b"c").unwrap().unwrap();
    assert_eq!(tree_a.len(), 1);
    drop(tree_a);
    assert!(tx.rename_tree(b"c", b"a").unwrap());
    tx.commit().unwrap();

    db.validate_free_space().unwrap();
}

#[test]
fn many_trees() {
    let _f = test_folder();
    let db = Database::new(_f.path()).unwrap();

    for shard in 0..10 {
        println!("shard {}", shard);
        let tx = db.begin_write().unwrap();
        for i in 0..1_000 {
            tx.get_or_create_tree(format!("database_{}_{}", shard, i).as_bytes())
                .unwrap();
        }
        tx.commit().unwrap();
    }

    eprintln!("checking");
    drop(db);
    let db = Database::new(_f.path()).unwrap();
    let tx = db.begin_read().unwrap();
    let mut all_tress = Vec::new();
    for shard in 0..10 {
        for i in 0..1_000 {
            let tree_name = format!("database_{}_{}", shard, i).into_bytes();
            tx.get_tree(&tree_name).unwrap().unwrap();
            all_tress.push(tree_name);
        }
    }
    all_tress.sort();

    assert_eq!(tx.list_trees().unwrap(), all_tress);
}

#[test]
fn recover_then_checkpoint() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let mut rng = get_rng();
    let mut master_sample: HashMap<Vec<u8>, Vec<u8>> = HashMap::default();
    let mut latest_tx_id = 0;

    let db = Database::new(_f.path()).unwrap();
    for _round_no in 0..50 {
        dbg!(_round_no);
        let sample: Vec<_> = (0..500)
            .map(|_| {
                let a = rand_str(&mut rng, 3, 200);
                let b = rand_str(&mut rng, 3, 600);
                (a, b)
            })
            .collect();

        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        latest_tx_id = tx.tx_id();
        tx.commit().unwrap();
        master_sample.extend(sample.into_iter());
    }

    let wal_range = db.inner.wal_range();
    assert!(!wal_range.is_empty());
    assert_eq!(db.inner.env.wal_trim_until(), Some(wal_range.start));
    assert_ne!(db.inner.env.wal.disk_size(), 0);
    drop(db);

    let db = Database::new(_f.path()).unwrap();
    db.checkpoint().unwrap();
    assert!(db.inner.wal_range().is_empty());
    db.inner.env.advance_bg_thread();
    assert_eq!(db.inner.env.wal.disk_size(), 0);

    let tx = db.begin_read().unwrap();
    assert_eq!(tx.tx_id(), latest_tx_id);
    let tree = tx.get_tree(b"default").unwrap().unwrap();
    for (k, v) in master_sample.iter() {
        assert_eq!(
            tree.get(k.as_bytes()).unwrap().as_deref(),
            Some(v.as_bytes())
        );
    }
}

#[test]
fn recovery() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let mut rng = get_rng();
    let mut master_sample: HashMap<Vec<u8>, Vec<u8>> = HashMap::default();
    let mut latest_tx_id = 0;

    for _round_no in 0..50 {
        dbg!(_round_no);
        let db = Database::new(_f.path()).unwrap();
        if latest_tx_id != 0 {
            let tx = db.begin_read().unwrap();
            assert_eq!(tx.tx_id(), latest_tx_id);
            let tree = tx.get_tree(b"default").unwrap().unwrap();
            for (k, v) in master_sample.iter() {
                assert_eq!(
                    tree.get(k.as_bytes()).unwrap().as_deref(),
                    Some(v.as_bytes())
                );
            }
        }

        let sample: Vec<_> = (0..500)
            .map(|_| {
                let a = rand_str(&mut rng, 3, 200);
                let b = rand_str(&mut rng, 3, 600);
                (a, b)
            })
            .collect();

        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();

        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        latest_tx_id = tx.tx_id();
        tx.commit().unwrap();
        master_sample.extend(sample.into_iter());
    }

    let db = Database::new(_f.path()).unwrap();
    let tx = db.begin_read().unwrap();
    assert_eq!(tx.tx_id(), latest_tx_id);
    let tree = tx.get_tree(b"default").unwrap().unwrap();
    for (k, v) in master_sample.iter() {
        assert_eq!(
            tree.get(k.as_bytes()).unwrap().as_deref(),
            Some(v.as_bytes())
        );
    }
}

#[test]
fn compatible_tree_options() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let env_opts = EnvOptions::new(_f.path());

    let db_opts = DbOptions::new();
    let db = Database::with_options(env_opts, db_opts).unwrap();

    for (k, v, nk, nv) in [(-1, -1, 4, 4), (4, 4, 3, 4), (4, 4, 4, 3)] {
        let tx = db.begin_write().unwrap();
        let mut tree_opt = options::TreeOptions::default();
        tree_opt.fixed_key_len = k;
        tree_opt.fixed_value_len = v;
        _ = tx
            .get_or_create_tree_with(b"default", tree_opt.clone())
            .unwrap();
        _ = tx
            .get_or_create_tree_with(b"default", tree_opt.clone())
            .unwrap();
        tree_opt.fixed_key_len = nk;
        tree_opt.fixed_value_len = nv;
        _ = tx
            .get_or_create_tree_with(b"default", tree_opt)
            .unwrap_err();
    }
}

#[test]
fn insert_fixed_size() {
    const K_LEN: usize = 4;
    const V_LEN: usize = 4;
    let _ = env_logger::try_init();
    let _f = test_folder();

    let mut rng = get_rng();
    let env_opts = EnvOptions::new(_f.path());
    // options.default_commit_sync = true;
    // options.disable_fsync = true;
    // options.use_wal = false;
    // options.checkpoint_target_size = 64 * 1024 * 1024;
    // options.throttle_memory_limit = 256 * 1024 * 1024;
    let db_opts = DbOptions::new();
    let db = Database::with_options(env_opts, db_opts).unwrap();

    let mut master_sample = HashMap::default();
    let mut guard = TestGuard::new();

    for _round_no in 0..448500 {
        if _round_no % 1000 == 0 {
            eprintln!(
                "Round {} used {}, elapsed {:?}",
                _round_no,
                ByteSize(db.inner.page_table.spans_used() as u64 * PAGE_SIZE),
                guard.start.elapsed()
            );
        }
        let sample: Vec<_> = (0..5)
            .map(|_| {
                let a = very_rand_bytes(&mut rng, K_LEN, K_LEN);
                let b = very_rand_bytes(&mut rng, V_LEN, V_LEN);
                (a, b)
            })
            .collect();

        let tx = db.begin_write().unwrap();
        let mut tree_opt = options::TreeOptions::default();
        tree_opt.fixed_key_len = K_LEN.try_into().unwrap();
        tree_opt.fixed_value_len = V_LEN.try_into().unwrap();
        let mut tree = tx.get_or_create_tree_with(b"default", tree_opt).unwrap();

        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
        master_sample.extend(sample.into_iter());
    }

    let mut iter = master_sample.iter_mut();
    for _round_no in 0..40000 {
        if _round_no % 1000 == 0 {
            eprintln!("Round {}", _round_no);
        }
        let sample: Vec<_> = iter.by_ref().take(5).collect();
        if sample.is_empty() {
            break;
        }
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_tree(b"default").unwrap().unwrap();
        for (k, v) in sample {
            *v = very_rand_bytes(&mut rng, V_LEN, V_LEN);
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
    }

    db.checkpoint().unwrap();
    guard.print();
    db.validate_free_space().unwrap();

    let tx = db.begin_write().unwrap();
    tx.commit().unwrap();
    assert_eq!(db.inner.page_table.spans_used(), 0);
    println!(
        "unique data {} keys, {}, data file {}, free space {}",
        master_sample.len(),
        ByteSize(
            master_sample
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>() as u64
        ),
        ByteSize(db.inner.file.file_len()),
        ByteSize(db.inner.allocator.lock().free.len() as u64 * PAGE_SIZE)
    );

    let tx = db.begin_read().unwrap();
    let tree = tx.get_tree(b"default").unwrap().unwrap();
    for _ in 0..2 {
        for (k, v) in master_sample.iter() {
            assert_eq!(
                tree.get(k.as_bytes()).unwrap().as_deref(),
                Some(v.as_bytes())
            );
        }
    }
    guard.print();
}

#[test]
fn spill_works() {
    let _ = env_logger::try_init();
    let t = tempfile::tempdir().unwrap();
    let mut rng = get_rng();
    let env_opts = EnvOptions::new(t.path());
    let mut db_opts = DbOptions::new();
    db_opts.write_txn_memory_limit = 1 * 1024 * 1024;
    db_opts.checkpoint_target_size = 10 * 1024 * 1024;
    let db = Database::with_options(env_opts, db_opts).unwrap();

    let mut master_sample = HashMap::default();
    let guard = TestGuard::new();

    let mut rounds = 500;
    if is_github_actions() {
        rounds /= 10;
    }

    for _round_no in 0..rounds {
        if _round_no % 50 == 0 {
            eprintln!(
                "Round {} used {}, elapsed {:?}",
                _round_no,
                ByteSize(db.inner.page_table.spans_used() as u64 * PAGE_SIZE),
                guard.start.elapsed()
            );
        }
        let mut sample_len = 0usize;
        let mut sample = Vec::new();
        while sample_len < db.inner.opts.write_txn_memory_limit {
            let k = rand_str(&mut rng, 100, 1000);
            let v = rand_str(&mut rng, 100, MIN_PAGE_COMPRESSION_BYTES as usize * 2);
            sample_len += k.len() + v.len();
            sample.push((k, v))
        }

        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();

        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
        master_sample.extend(sample.into_iter());
    }
    db.validate_free_space().unwrap();
}

#[test]
fn seq_insert_works() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let mut rng = get_rng();
    let env_opts = EnvOptions::new(_f.path());
    let db_opts = DbOptions::new();
    let db = Database::with_options(env_opts, db_opts).unwrap();

    let mut master_sample = HashMap::default();
    let mut guard = TestGuard::new();

    let mut rounds = 448500;
    if is_github_actions() {
        rounds /= 10;
    }

    for _round_no in 0..rounds {
        if _round_no % 1000 == 0 {
            eprintln!(
                "Round {} used {}, elapsed {:?}",
                _round_no,
                ByteSize(db.inner.page_table.spans_used() as u64 * PAGE_SIZE),
                guard.start.elapsed()
            );
        }
        let sample: Vec<_> = (0..5)
            .map(|i| {
                let a = [
                    &(master_sample.len() + i).to_be_bytes()[..],
                    &rand_str(&mut rng, 3, 200),
                ]
                .concat();
                let b = rand_str(&mut rng, 3, 600);
                (a, b)
            })
            .collect();

        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
        master_sample.extend(sample.into_iter());
    }

    db.checkpoint().unwrap();
    guard.print();

    println!(
        "unique data {} keys, {}, data file {}, free space {}",
        master_sample.len(),
        ByteSize(
            master_sample
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>() as u64
        ),
        ByteSize(db.inner.file.file_len()),
        ByteSize(db.inner.allocator.lock().free.len() as u64 * PAGE_SIZE),
    );
}

#[test]
fn insert_works() {
    let _ = env_logger::try_init();
    let mut rng = get_rng();
    let _f = test_folder();
    let env_opts = EnvOptions::new(_f.path());
    // env_opts.use_checksums = true;
    // env_opts.default_commit_sync = true;
    // env_opts.page_cache_size = 512 * 1024 * 1024;
    // env_opts.disable_fsync = false;
    // env_opts.use_mmap = true;
    // env_opts.use_mmap = false;
    // env_opts.use_checksums = true;
    // env_opts.checkpoint_target_size = 128 * 1024 * 1024;
    // env_opts.throttle_memory_limit = 2 * 256 * 1024 * 1024;
    // env_opts.stall_memory_limit = 2 * 256 * 1024 * 1024;
    let db_opts = DbOptions::new();
    let db = Database::with_options(env_opts, db_opts).unwrap();

    let mut master_sample = HashMap::default();
    let mut guard = TestGuard::new();
    let mut rounds = 448500;
    if is_github_actions() {
        rounds /= 10;
    }

    for _round_no in 0..rounds {
        if _round_no % 1000 == 0 {
            eprintln!(
                "Round {} used {}, elapsed {:?}",
                _round_no,
                ByteSize(db.inner.page_table.spans_used() as u64 * PAGE_SIZE),
                guard.start.elapsed()
            );
        }
        let sample: Vec<_> = (0..5)
            .map(|_| {
                let a = rand_str(&mut rng, 3, 200);
                let b = rand_str(&mut rng, 3, 600);
                (a, b)
            })
            .collect();

        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();

        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
        master_sample.extend(sample.into_iter());
    }

    let mut iter = master_sample.iter_mut();
    for _round_no in 0..40000 {
        if _round_no % 1000 == 0 {
            eprintln!("Round {}", _round_no);
        }
        let sample: Vec<_> = iter.by_ref().take(5).collect();
        if sample.is_empty() {
            break;
        }
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for (k, v) in sample {
            *v = rand_str(&mut rng, 3, 600);
            // println!("inserting {:?}", k.as_bytes());
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
    }

    db.checkpoint().unwrap();
    guard.print();

    let tx = db.begin_write().unwrap();
    tx.commit().unwrap();
    assert_eq!(db.inner.page_table.spans_used(), 0);
    println!(
        "unique data {} keys, {}, data file {}, free space {}",
        master_sample.len(),
        ByteSize(
            master_sample
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>() as u64
        ),
        ByteSize(db.inner.file.file_len()),
        ByteSize(db.inner.allocator.lock().free.len() as u64 * PAGE_SIZE)
    );

    #[cfg(debug_assertions)]
    db.validate_free_space().unwrap();

    let tx = db.begin_read().unwrap();
    let tree = tx.get_tree(b"default").unwrap().unwrap();
    #[cfg(debug_assertions)]
    {
        dbg!(tree.value.level);
        print_tree_node_stats(&tree);
    }
    for _ in 0..2 {
        for (k, v) in master_sample.iter() {
            assert_eq!(
                tree.get(k.as_bytes()).unwrap().as_deref(),
                Some(v.as_bytes())
            );
        }
    }
    guard.print();
}

#[test]
fn insert_works_seq_then_rand() {
    let _ = env_logger::try_init();
    let _f = test_folder();
    let mut rng = get_rng();
    let env_opts = EnvOptions::new(_f.path());
    // options.use_checksums = true;
    // options.use_mmap = false;
    let db_opts = DbOptions::new();
    let db = Database::with_options(env_opts, db_opts).unwrap();

    let mut guard = TestGuard::new();

    let mut rounds = 448500;
    if is_github_actions() {
        rounds /= 10;
    }
    let mut keys = 0u64;
    for _round_no in 0..rounds {
        if _round_no % 1000 == 0 {
            eprintln!(
                "Round {} used {}, elapsed {:?}",
                _round_no,
                ByteSize(db.inner.page_table.spans_used() as u64 * PAGE_SIZE),
                guard.start.elapsed()
            );
        }
        let sample: Vec<_> = (0..50)
            .map(|_| {
                let a = keys.to_be().as_bytes().to_vec();
                keys += 1;
                let b = rand_str(&mut rng, 3, 1200);
                (a, b)
            })
            .collect();

        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();

        for (k, v) in &sample {
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
    }

    for _round_no in 0..rounds {
        if _round_no % 1000 == 0 {
            eprintln!("Round {}", _round_no);
        }
        let sample: Vec<_> = (0..5)
            .map(|_| rng.gen_range(0..keys).to_be().as_bytes().to_vec())
            .collect::<Vec<_>>();
        let tx = db.begin_write().unwrap();
        let mut tree = tx.get_or_create_tree(b"default").unwrap();
        for k in sample {
            let v = rand_str(&mut rng, 3, 1200);
            // println!("inserting {:?}", k.as_bytes());
            tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
        }
        drop(tree);
        tx.commit().unwrap();
    }
    db.checkpoint().unwrap();

    guard.print();
}

#[cfg(debug_assertions)]
fn print_tree_node_stats(tree: &Tree) {
    let mut num_nodes = 0usize;
    let mut num_with_prefix = 0usize;
    let mut prefix_sum = 0usize;
    tree.iter_nodes(&mut |node| {
        num_nodes += 1;
        num_with_prefix += (node.node_header().key_prefix_len != 0) as usize;
        prefix_sum += node.node_header().key_prefix_len as usize;
    })
    .unwrap();
    dbg!(num_nodes, num_with_prefix);
    dbg!(
        prefix_sum.checked_div(num_nodes),
        prefix_sum.checked_div(num_with_prefix)
    );
}

struct TestGuard {
    sys: sysinfo::System,
    start: Instant,
}

impl TestGuard {
    fn new() -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_process(Pid::from_u32(std::process::id() as _));
        Self {
            start: Instant::now(),
            sys,
        }
    }

    fn print(&mut self) {
        self.sys
            .refresh_process(Pid::from_u32(std::process::id() as _));
        let written = self
            .sys
            .process(Pid::from_u32(std::process::id() as _))
            .unwrap()
            .disk_usage()
            .written_bytes;
        let read = self
            .sys
            .process(Pid::from_u32(std::process::id() as _))
            .unwrap()
            .disk_usage()
            .read_bytes;
        println!(
            "Elapsed {:?}, Written {} {}/s -- Read {} {}/s",
            self.start.elapsed(),
            ByteSize(written),
            ByteSize((written as f64 / self.start.elapsed().as_secs_f64()) as u64),
            ByteSize(read),
            ByteSize((read as f64 / self.start.elapsed().as_secs_f64()) as u64),
        );
        self.start = Instant::now();
    }
}
