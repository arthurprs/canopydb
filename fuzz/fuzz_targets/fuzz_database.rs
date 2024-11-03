#![no_main]
use arbitrary::Arbitrary;
use canopydb::{utils::EscapedBytes, Error};
use libfuzzer_sys::fuzz_target;
use rand::{distributions::Alphanumeric, Rng, SeedableRng};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    mem,
    ops::Bound,
    rc::Rc,
};

#[macro_use]
extern crate log;

#[derive(Debug, Clone, Default)]
struct Failure(Option<(&'static str, Rc<str>)>);

impl<'a> Arbitrary<'a> for Failure {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        if cfg!(not(feature = "failpoints")) {
            return Ok(Failure::default());
        }
        const FAILPOINTS: &[&str] = &[
            // "fopen",
            "fsync",
            // "fread",
            "ftruncate",
            "fwrite",
            "unlink",
            "mmap",
        ];

        let a = u.arbitrary::<u8>()?;
        let b = u.arbitrary::<u8>()?;
        let c = a ^ b;
        let config = format!("{0}*off->{1}*return({0}-{1})->off", a % 10, b % 5);
        Ok(Failure(Some((
            FAILPOINTS[c as usize % FAILPOINTS.len()],
            config.into(),
        ))))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <u8 as Arbitrary>::size_hint(depth)
    }
}

impl Failure {
    fn call<T>(&self, fun: impl FnOnce() -> T) -> T {
        if cfg!(not(feature = "failpoints")) {
            return fun();
        }
        if let Some((name, config)) = &self.0 {
            trace!("Setting up failpoint {} {}", name, config);
            fail::cfg(*name, &config).unwrap();
            let result = fun();
            fail::cfg(*name, "off").unwrap();
            result
        } else {
            fun()
        }
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Hash)]
struct Uint<const MIN: usize, const MAX: usize>(usize);

impl<'a, const MIN: usize, const MAX: usize> Arbitrary<'a> for Uint<MIN, MAX> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let n = u.int_in_range(MIN as u32..=MAX as u32)? as usize;
        Ok(Uint(n))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <u32 as Arbitrary>::size_hint(depth)
    }
}

#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Hash)]
struct VecN<const MIN: usize, const MAX: usize, T>(Vec<T>);

impl<'a, const MIN: usize, const MAX: usize, T: Arbitrary<'a>> Arbitrary<'a> for VecN<MIN, MAX, T> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let n = u.int_in_range(MIN as u32..=MAX as u32)?;
        let mut v = Vec::with_capacity(n as usize);
        for _ in 0..n {
            v.push(u.arbitrary()?);
        }
        Ok(VecN(v))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let a = <u32 as Arbitrary>::size_hint(depth).0;
        (a, None)
    }
}

#[derive(Clone, Default, PartialEq, PartialOrd, Eq, Ord, Hash)]
struct BytesN<const MIN: usize, const MAX: usize>(Rc<Vec<u8>>);

impl<'a, const MIN: usize, const MAX: usize> std::fmt::Debug for BytesN<MIN, MAX> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{:?}`", &EscapedBytes(&self.0))
    }
}

impl<'a, const MIN: usize, const MAX: usize> Arbitrary<'a> for BytesN<MIN, MAX> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let seed = u.arbitrary::<u16>()?;
        let len = u.int_in_range(MIN as u16..=MAX as u16)?;
        let mut bytes = Vec::with_capacity(len as usize);
        let mut r = rand::rngs::SmallRng::seed_from_u64(seed as u64 % 20);
        bytes.extend((0..20.min(len)).map(|_| r.sample(Alphanumeric) as u8));
        let mut r = rand::rngs::SmallRng::seed_from_u64(seed as u64);
        bytes.extend((20..len).take(128).map(|_| r.sample(Alphanumeric) as u8));
        bytes.resize(len as usize, 0);
        Ok(BytesN(bytes.into()))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        arbitrary::size_hint::and(
            <u16 as Arbitrary>::size_hint(depth),
            <u16 as Arbitrary>::size_hint(depth),
        )
    }
}

type Bytes = BytesN<0, 8000>;
type ActorId = Uint<0, { NUM_ACTORS - 1 }>;

#[derive(Debug)]
enum Op {
    Checkpoint(ActorId, Failure),
    Rollback(ActorId, Failure),
    Commit(ActorId, Failure),
    Insert(ActorId, Bytes, Bytes, Failure),
    Update(ActorId, usize, Bytes, Failure),
    Delete(ActorId, usize, Failure),
    DeleteRange(ActorId, (Bound<Bytes>, Bound<Bytes>), Failure),
    ReadTx(ActorId, Failure),
}

impl Op {
    fn actor(&self) -> ActorId {
        match self {
            Op::Checkpoint(actor, ..)
            | Op::Rollback(actor, ..)
            | Op::Commit(actor, ..)
            | Op::Insert(actor, ..)
            | Op::Update(actor, ..)
            | Op::Delete(actor, ..)
            | Op::DeleteRange(actor, ..)
            | Op::ReadTx(actor, ..) => *actor,
        }
    }

    fn failure(&self) -> Failure {
        match self {
            Op::Checkpoint(.., failure)
            | Op::Rollback(.., failure)
            | Op::Commit(.., failure)
            | Op::Insert(.., failure)
            | Op::Update(.., failure)
            | Op::Delete(.., failure)
            | Op::DeleteRange(.., failure)
            | Op::ReadTx(.., failure) => failure.clone(),
        }
    }
}

impl<'a> Arbitrary<'a> for Op {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(match u.int_in_range(0u8..=101)? {
            0..=64 => Op::Insert(
                u.arbitrary()?,
                u.arbitrary()?,
                u.arbitrary()?,
                u.arbitrary()?,
            ),
            65..=70 => Op::Update(
                u.arbitrary()?,
                u.arbitrary()?,
                u.arbitrary()?,
                u.arbitrary()?,
            ),
            71..=90 => Op::Delete(u.arbitrary()?, u.arbitrary()?, u.arbitrary()?),
            91..=96 => Op::Commit(u.arbitrary()?, u.arbitrary()?),
            97..=99 => Op::Checkpoint(u.arbitrary()?, u.arbitrary()?),
            100..=100 => Op::Rollback(u.arbitrary()?, u.arbitrary()?),
            101..=101 => Op::DeleteRange(
                u.arbitrary()?,
                (u.arbitrary()?, u.arbitrary()?),
                u.arbitrary()?,
            ),
            // 100..=110 => Op::ReadTx(u.arbitrary()?, u.arbitrary()?),
            _ => unreachable!(),
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let a = arbitrary::size_hint::and_all(&[
            <u8 as Arbitrary>::size_hint(depth),
            ActorId::size_hint(depth),
            Failure::size_hint(depth),
        ]);
        let b = arbitrary::size_hint::and_all(&[
            <u8 as Arbitrary>::size_hint(depth),
            ActorId::size_hint(depth),
            Bytes::size_hint(depth),
            Bytes::size_hint(depth),
            Failure::size_hint(depth),
        ]);
        (a.0, b.1)
    }
}

type ModelTreeMut = BTreeMap<Bytes, Bytes>;
type ModelTree = Rc<ModelTreeMut>;
type ModelTreeRef<'a> = &'a ModelTreeMut;
const NUM_ACTORS: usize = 2;
const MAX_WRITERS: usize = 1;

struct WorldState {
    writers: BTreeSet<ActorId>,
    db: canopydb::Database,
    model: ModelTree,
    read_queue: VecDeque<ActorId>,
    write_queue: VecDeque<ActorId>,
    always_validate: bool,
}

struct World {
    actors: [Actor; NUM_ACTORS],
    state: WorldState,
}

enum Tx {
    None,
    Read(ModelTree, canopydb::ReadTransaction),
    Write(ModelTreeMut, canopydb::WriteTransaction, ModelTree),
}

impl Default for Tx {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Default)]
struct Actor {
    id: ActorId,
    op_queue: VecDeque<Op>,
    enqueued: bool,
    tx: Tx,
}

impl Actor {
    fn op(&mut self, state: &mut WorldState) {
        while let Some(op) = self.op_queue.pop_front() {
            if match op {
                Op::Commit(_, failure) => self.commit(state, failure),
                Op::Rollback(.., failure) => self.rollback(state, failure),
                Op::Insert(..) | Op::Delete(..) | Op::Update(..) | Op::DeleteRange(..) => {
                    self.mutate(state, op)
                }
                Op::ReadTx(.., failure) => self.begin_read(state, failure),
                Op::Checkpoint(..) => self.checkpoint(state, op),
            } {
                break;
            }
        }
    }

    fn commit(&mut self, state: &mut WorldState, failure: Failure) -> bool {
        match mem::take(&mut self.tx) {
            Tx::None => false,
            Tx::Read(model, rx) => {
                Self::validate(state, &model, &rx);
                false
            }
            Tx::Write(model, tx, starting) => {
                let tx_id = tx.tx_id();
                info!("[{}] commiting {}", self.id.0, tx_id);
                Self::validate(state, &model, &tx);
                match failure.call(|| tx.commit()) {
                    Ok(_) => {
                        let state_model = Rc::make_mut(&mut state.model);
                        for (k, v) in &model {
                            if starting.get(&k) != Some(&v) {
                                state_model.insert(k.clone(), v.clone());
                            }
                        }
                        for k in starting.keys() {
                            if !model.contains_key(&k) {
                                state_model.remove(k);
                            }
                        }
                    }
                    Err(e) => error!("commit failed: {e}"),
                }
                Self::validate(state, &state.model, &state.db.begin_read().unwrap());
                // if state.always_validate || tx_id % 8 == 0 {
                //     match state.db.validate_free_space() {
                //         Ok(()) | Err(Error::DatabaseHalted) => (),
                //         Err(e) => panic!("{e}"),
                //     }
                // }
                state.writers.remove(&self.id);
                if !self.op_queue.is_empty() {
                    self.enqueued = true;
                    state.read_queue.push_back(self.id);
                }
                true
            }
        }
    }

    fn rollback(&mut self, state: &mut WorldState, failure: Failure) -> bool {
        match mem::take(&mut self.tx) {
            Tx::None => false,
            Tx::Read(model, rx) => {
                Self::validate(state, &model, &rx);
                false
            }
            Tx::Write(model, tx, ..) => {
                info!("[{}] rolling back {}", self.id.0, tx.tx_id());
                Self::validate(state, &model, &tx);
                if let Err(e) = failure.call(|| tx.rollback()) {
                    error!("rollback failed: {e}");
                }
                state.writers.remove(&self.id);
                if !self.op_queue.is_empty() {
                    self.enqueued = true;
                    state.read_queue.push_back(self.id);
                }
                true
            }
        }
    }

    fn checkpoint(&mut self, state: &mut WorldState, op: Op) -> bool {
        if state.writers.is_empty() {
            if let Err(e) = op.failure().call(|| state.db.checkpoint()) {
                error!("checkpoint failed: {e}");
            }
            Self::validate(state, &state.model, &state.db.begin_read().unwrap());
            match state.db.validate_free_space() {
                Ok(()) | Err(Error::DatabaseHalted) => (),
                Err(e) => panic!("{e}"),
            }
            if !self.op_queue.is_empty() {
                self.enqueued = true;
                state.read_queue.push_back(self.id);
            }
            true
        } else {
            if !self.enqueued {
                self.enqueued = true;
                state.write_queue.push_back(self.id);
            }
            self.op_queue.push_front(op);
            true
        }
    }

    fn mutate(&mut self, state: &mut WorldState, op: Op) -> bool {
        match &mut self.tx {
            Tx::Read(model, rx) => {
                Self::validate(state, &model, rx);
                self.tx = Tx::None;
            }
            _ => (),
        }
        match &mut self.tx {
            Tx::None => {
                if state.writers.len() < MAX_WRITERS {
                    match state.db.begin_write() {
                        Ok(tx) => {
                            self.tx = Tx::Write((*state.model).clone(), tx, state.model.clone())
                        }
                        Err(Error::DatabaseHalted) => return false,
                        Err(e) => panic!("{e}"),
                    }
                    state.writers.insert(self.id);
                } else {
                    if !self.enqueued {
                        self.enqueued = true;
                        state.write_queue.push_back(self.id);
                    }
                    self.op_queue.push_front(op);
                    return true;
                }
            }
            Tx::Read(_, _) => unreachable!(),
            Tx::Write(..) => (),
        }
        match &mut self.tx {
            Tx::None | Tx::Read(_, _) => unreachable!(),
            Tx::Write(model, tx, ..) => {
                let mut tree = match tx.get_or_create_tree(b"default") {
                    Ok(tree) => tree,
                    Err(Error::TransactionAborted) => return false,
                    Err(e) => panic!("{e}"),
                };
                let (k, v, failure) = match op {
                    Op::Insert(_, k, v, f) => (k, Some(v), f),
                    Op::Update(_, i, v, f) => {
                        if model.is_empty() {
                            return false;
                        }
                        let k = model.iter().nth(i % model.len()).unwrap().0.clone();
                        (k, Some(v), f)
                    }
                    Op::Delete(_, i, f) => {
                        if model.is_empty() {
                            return false;
                        }
                        let k = model.iter().nth(i % model.len()).unwrap().0.clone();
                        (k, None, f)
                    }
                    Op::DeleteRange(_, (start, end), failure) => {
                        info!("[{}] delete range {start:?} {end:?}", self.id.0);
                        match failure.call(|| {
                            tree.delete_range::<&[u8]>((
                                start.as_ref().map(|a| &a.0[..]),
                                end.as_ref().map(|a| &a.0[..]),
                            ))
                        }) {
                            Ok(_) => model.retain(|k, _| {
                                !(match &start {
                                    Bound::Included(a) => k >= &a,
                                    Bound::Excluded(a) => k > &a,
                                    Bound::Unbounded => true,
                                } && match &end {
                                    Bound::Included(b) => k <= &b,
                                    Bound::Excluded(b) => k < &b,
                                    Bound::Unbounded => true,
                                })
                            }),
                            Err(e) => error!("delete range failed: {e}"),
                        }
                        return false;
                    }
                    _ => unreachable!(),
                };
                if let Some(v) = v {
                    info!("[{}] inserting {k:?}: {v:?}", self.id.0);
                    match failure.call(|| tree.insert(&k.0, &v.0)) {
                        Ok(_) => {
                            model.insert(k, v);
                        }
                        Err(e) => error!("insert failed: {e}"),
                    }
                } else {
                    info!("[{}] deleting {k:?}", self.id.0);
                    match failure.call(|| tree.delete(&k.0)) {
                        Ok(_) => {
                            model.remove(&k);
                        }
                        Err(e) => error!("delete failed: {e}"),
                    }
                }
                drop(tree);
                // if state.always_validate {
                //     Self::validate(state, &model, tx);
                // }
                false
            }
        }
    }

    fn begin_read(&mut self, state: &mut WorldState, _failure: Failure) -> bool {
        match &mut self.tx {
            Tx::None => (),
            Tx::Read(model, rx) => {
                Self::validate(state, &model, rx);
            }
            _ => return false,
        }
        match state.db.begin_read() {
            Ok(rx) => self.tx = Tx::Read(state.model.clone(), rx),
            Err(Error::DatabaseHalted) => (),
            Err(e) => panic!("{e}"),
        }
        false
    }

    fn validate(state: &WorldState, model: ModelTreeRef, rx: &canopydb::Transaction) {
        let tx_id = rx.tx_id();
        info!("Validating tx id {}", tx_id);
        let tree = match rx.get_tree(b"default") {
            Ok(Some(tree)) => tree,
            Ok(None) => {
                assert!(model.is_empty());
                return;
            }
            Err(Error::TransactionAborted) => return,
            Err(e) => panic!("{e}"),
        };

        assert_eq!(tree.len(), model.len() as u64);

        let (val_gets, val_forw, val_back) = match tx_id % 3 {
            _ if state.always_validate => (true, true, true),
            0 => (false, true, false),
            1 => (false, false, true),
            2 => (true, false, false),
            _ => unreachable!(),
        };

        if val_gets {
            for (k, mv) in model {
                let k = k.0.as_slice();
                let mv = mv.0.as_slice();
                assert_eq!(
                    EscapedBytes(tree.get(k).unwrap().unwrap().as_ref()),
                    EscapedBytes(mv),
                    "key {:?}",
                    EscapedBytes(k.as_ref())
                );
                // let mut range = tree.range(k..=k).unwrap();
                // let (k1, v1) = range.next().unwrap().unwrap();
                // assert_eq!(EscapedBytes(k), EscapedBytes(k1.as_ref()));
                // assert_eq!(
                //     EscapedBytes(mv),
                //     EscapedBytes(v1.as_ref()),
                //     "key {:?}",
                //     EscapedBytes(k.as_ref())
                // );
                // assert!(range.next().is_none());
            }
            assert_eq!(tree.len() as usize, model.len());
        }
        if val_forw {
            let mut iter = tree.iter().unwrap();
            for (k, v) in model.iter() {
                let (ck, cv) = iter.next().unwrap().unwrap();
                assert_eq!(k.0.as_slice(), ck.as_ref());
                assert_eq!(v.0.as_slice(), cv.as_ref());
            }
            assert_eq!(iter.next().map(|r| r.ok()), None);
        }
        if val_back {
            let mut iter = tree.iter().unwrap().rev();
            for (k, v) in model.iter().rev() {
                let (ck, cv) = iter.next().unwrap().unwrap();
                assert_eq!(k.0.as_slice(), ck.as_ref());
                assert_eq!(v.0.as_slice(), cv.as_ref());
            }
            assert_eq!(iter.next().map(|r| r.ok()), None);
        }
    }
}

fuzz_target!(|ops: Vec<Op>| {
    let _ = env_logger::builder().format_timestamp(None).try_init();
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut options = canopydb::EnvOptions::new(tmp_dir.as_ref());
    options.wait_bg_threads_on_drop = true;
    let mut db_options = canopydb::DbOptions::default();
    db_options.checkpoint_interval = std::time::Duration::MAX;
    db_options.use_wal = cfg!(feature = "failpoints");
    db_options.default_commit_sync = false;
    // db_options.checkpoint_target_size = 16 * 4096 as usize;
    let always_validate =
        std::env::var("ALWAYS_VALIDATE").map_or(0, |s| s.parse::<i64>().unwrap()) != 0;
    let db = canopydb::Database::with_options(options.clone(), db_options.clone()).unwrap();
    let mut world = World {
        state: WorldState {
            db,
            model: Default::default(),
            write_queue: Default::default(),
            read_queue: Default::default(),
            writers: Default::default(),
            always_validate,
        },
        actors: Default::default(),
    };
    for (i, a) in world.actors.iter_mut().enumerate() {
        a.id = Uint(i);
    }
    dbg!(ops.len());
    for op in ops {
        let actor_no = op.actor().0;
        world.actors[actor_no].op_queue.push_back(op);
        world.actors[actor_no].op(&mut world.state);
        while !world.state.read_queue.is_empty()
            || (world.state.writers.is_empty() && !world.state.write_queue.is_empty())
        {
            if world.state.writers.is_empty() {
                if let Some(next) = world.state.write_queue.pop_front() {
                    world.actors[next.0].enqueued = false;
                    world.actors[next.0].op(&mut world.state);
                }
            }
            if let Some(next) = world.state.read_queue.pop_front() {
                world.actors[next.0].enqueued = false;
                world.actors[next.0].op(&mut world.state);
            }
        }
    }
    while !world.state.read_queue.is_empty() || !world.state.write_queue.is_empty() {
        if world.state.writers.is_empty() {
            if let Some(next) = world.state.write_queue.pop_front() {
                world.actors[next.0].enqueued = false;
                world.actors[next.0].op(&mut world.state);
            }
        }
        if let Some(next) = world.state.read_queue.pop_front() {
            world.actors[next.0].enqueued = false;
            world.actors[next.0].op(&mut world.state);
        }
        if world.state.read_queue.is_empty()
            && !world.state.write_queue.is_empty()
            && !world.state.writers.is_empty()
        {
            assert!(world.actors[world.state.writers.pop_first().unwrap().0]
                .commit(&mut world.state, Failure::default()));
        }
    }
    assert!(world.actors.iter().all(|a| a.op_queue.is_empty()));

    if !cfg!(feature = "failpoints") {
        return;
    }

    // recover database and re-validate
    let model = world.state.model.clone();
    drop(world);
    let db = canopydb::Database::with_options(options.clone(), db_options.clone()).unwrap();
    db.validate_free_space().unwrap();
    let world = World {
        state: WorldState {
            db,
            model: Default::default(),
            write_queue: Default::default(),
            read_queue: Default::default(),
            writers: Default::default(),
            always_validate,
        },
        actors: Default::default(),
    };
    Actor::validate(&world.state, &model, &world.state.db.begin_read().unwrap());
});
