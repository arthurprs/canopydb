use crate::*;
use rand::seq::SliceRandom;
use shuttle::{
    rand::Rng,
    sync::{atomic, Mutex},
};

fn rand_str(rng: &mut impl shuttle::rand::Rng, a: usize, b: usize) -> Vec<u8> {
    let len = rng.gen_range(a..=b);
    let mut buffer = vec![0; len];
    rng.fill_bytes(&mut buffer);
    buffer
}

#[test]
fn insert_works() {
    let _ = env_logger::try_init();
    let mut config = shuttle::Config::default();
    config.max_steps = shuttle::MaxSteps::None;
    let check_determinism = std::env::var("CHECK_DETERMINISM").map_or(false, |s| !s.is_empty());
    if let Ok(seed) = std::env::var("SEED") {
        config.failure_persistence = shuttle::FailurePersistence::None;
        let seed = std::fs::read_to_string(&seed).unwrap_or(seed.clone());
        let scheduler = shuttle::scheduler::ReplayScheduler::new_from_encoded(&seed);
        let runner = shuttle::Runner::new(scheduler, config);
        runner.run(insert_works_stub);
    } else {
        config.failure_persistence = shuttle::FailurePersistence::File(None);
        let max_iterations: usize = std::env::var("MAX_ITERATIONS")
            .map(|s| s.parse().unwrap())
            .unwrap_or(100);
        let scheduler = shuttle::scheduler::RandomScheduler::new(max_iterations);
        if check_determinism {
            let scheduler =
                shuttle::scheduler::UncontrolledNondeterminismCheckScheduler::new(scheduler);
            let runner = shuttle::Runner::new(scheduler, config);
            runner.run(insert_works_stub);
        } else {
            let runner = shuttle::Runner::new(scheduler, config);
            runner.run(insert_works_stub);
        }
    }
}

fn insert_works_stub() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut env_opts = EnvOptions::new(temp_dir.path());
    env_opts.disable_fsync = true;
    env_opts.page_cache_size = 1 * 1024 * 1024;
    let mut db_opts = DbOptions::default();
    db_opts.checkpoint_target_size = 1 * 1024 * 1024;
    db_opts.throttle_memory_limit = 2 * 1024 * 1024;
    db_opts.stall_memory_limit = 3 * 1024 * 1024;
    let db = Arc::new(Database::with_options(env_opts, db_opts).unwrap());
    let stop = Arc::new(atomic::AtomicBool::new(false));
    let long_rx = Arc::new(Mutex::new(()));

    let mut threads = Vec::new();
    let db_ = db.clone();
    let stop_ = stop.clone();
    let thread = shuttle::thread::spawn(move || {
        while !stop_.load(atomic::Ordering::SeqCst) {
            let mut thread_rng = shuttle::rand::thread_rng();
            for _ in 0..thread_rng.gen::<u16>() {
                shuttle::thread::yield_now();
            }
            eprintln!("will checkpoint");
            db_.checkpoint().unwrap();
        }
    });
    threads.push(thread);
    let db_ = db.clone();
    let stop_ = stop.clone();
    let long_rx_ = long_rx.clone();
    let thread = shuttle::thread::spawn(move || {
        while !stop_.load(atomic::Ordering::SeqCst) {
            let mut thread_rng = shuttle::rand::thread_rng();
            for _ in 0..thread_rng.gen::<u16>() {
                shuttle::thread::yield_now();
            }
            let _long_rx_guard = long_rx_.lock();
            db_.compact().unwrap();
        }
    });
    threads.push(thread);
    for _ in 0..2 {
        let db_ = db.clone();
        let stop_ = stop.clone();
        let long_rx_ = long_rx.clone();
        let thread = shuttle::thread::spawn(move || {
            let mut thread_rng = shuttle::rand::thread_rng();
            while !stop_.load(atomic::Ordering::SeqCst) {
                shuttle::thread::yield_now();
                let mut rxs = vec![];
                if thread_rng.gen_bool(0.1) {
                    let _long_rx_guard = long_rx_.lock();
                    let (cond_name, limit) = if thread_rng.gen_bool(0.5) {
                        ("throttle", db_.inner.opts.throttle_memory_limit)
                    } else {
                        ("stall", db_.inner.opts.stall_memory_limit)
                    };
                    warn!("Will wait until {cond_name}");
                    while (db_.inner.page_table.spans_used() * PAGE_SIZE as usize) < limit
                        && !stop_.load(atomic::Ordering::SeqCst)
                    {
                        shuttle::thread::yield_now();
                        if thread_rng.gen_bool(1.0 / rxs.len().max(1) as f64) {
                            rxs.push(db_.begin_read().unwrap());
                        }
                    }
                    warn!("{cond_name}?");
                    while (db_.inner.page_table.spans_used() * PAGE_SIZE as usize) >= limit
                        && !stop_.load(atomic::Ordering::SeqCst)
                    {
                        shuttle::thread::yield_now();
                    }
                    warn!("{cond_name} reversed");
                    rxs.shuffle(&mut thread_rng);
                }
                if rxs.is_empty() {
                    rxs.push(db_.begin_read().unwrap());
                }
                for rx in rxs.into_iter().take(10) {
                    if let Some(tree) = rx.get_tree(b"default").unwrap() {
                        let range = tree.range(rand_str(&mut thread_rng, 1, 1)..).unwrap();
                        shuttle::thread::yield_now();
                        for _ in range.take(1_000) {
                            shuttle::thread::yield_now();
                        }
                    }
                    drop(rx);
                }
            }
        });
        threads.push(thread);
    }

    let mut writers = Vec::new();
    for w in 0..3 {
        let db = db.clone();
        let thread = shuttle::thread::spawn(move || {
            let mut rng = shuttle::rand::thread_rng();
            let mut master_sample = HashMap::default();
            for _round_no in 0..3_000 {
                if _round_no % 1_000 == 0 {
                    eprintln!("[{w}] Round {_round_no}");
                }
                // 1% chance of making a huge txn that spills to disk
                let extra_samples = if rng.gen_bool(0.01) { 150 } else { 0 };
                let sample: Vec<_> = (0..5 + extra_samples)
                    .map(|_| {
                        let a = rand_str(&mut rng, 3, 200);
                        let b = rand_str(&mut rng, 3, 4_000);
                        (a, b)
                    })
                    .collect();

                let tx = db.begin_write_with(rng.gen_bool(0.66)).unwrap();
                shuttle::thread::yield_now();
                let mut tree = tx.get_or_create_tree(b"default").unwrap();

                shuttle::thread::yield_now();
                for (k, v) in &sample {
                    shuttle::thread::yield_now();
                    tree.insert(k.as_bytes(), v.as_bytes()).unwrap();
                }
                drop(tree);
                shuttle::thread::yield_now();
                if rng.gen_bool(0.1) {
                    tx.rollback().unwrap();
                } else {
                    match tx.commit() {
                        Ok(x) => master_sample.extend(sample.into_iter().map(|(k, v)| (k, (x, v)))),
                        Err(Error::WriteConflict) => (),
                        Err(e) => std::panic!("{e}"),
                    }
                }

                shuttle::thread::yield_now();
            }
            master_sample
        });
        writers.push(thread);
    }

    let mut master_sample = HashMap::<Vec<u8>, (TxId, Vec<u8>)>::default();
    for (w, t) in writers.into_iter().enumerate() {
        let sample = t.join().unwrap();
        eprintln!("writer {w} finished");
        for (k, v) in sample {
            let entry = master_sample.entry(k).or_default();
            if &v > entry {
                *entry = v;
            }
        }
    }

    db.checkpoint().unwrap();
    db.validate_free_space().unwrap();

    {
        eprintln!("validating");
        let rx = db.begin_read().unwrap();
        let tree = rx.get_tree(b"default").unwrap().unwrap();
        for (k, (_, v)) in master_sample.iter() {
            let from_db = tree.get(k.as_bytes()).unwrap();
            assert_eq!(from_db.as_deref(), Some(v.as_bytes()));
        }
    }

    stop.store(true, atomic::Ordering::SeqCst);
    for thread in threads {
        thread.join().unwrap();
    }
    drop(db);
}
