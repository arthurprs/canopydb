#![no_main]
use std::io::{BufReader, Seek, Write};

use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

use canopydb::wal::{Options, Wal, WalIdx};
use rand::prelude::*;
use tempfile::{tempfile, TempDir};

#[derive(Debug, Arbitrary)]
struct Input {
    use_direct_io: bool,
    use_blocks: bool,
    min_file_size: u16,
    max_file_size: u16,
    max_iter_mem: u16,
    ops: Vec<(u16, u16, bool)>,
}

fuzz_target!(|input: Input| {
    let Input {
        use_direct_io,
        use_blocks,
        min_file_size,
        max_file_size,
        max_iter_mem,
        ops,
    } = input;
    let _ = env_logger::try_init();
    let ops = ops
        .iter()
        .copied()
        .map(|(l, c, f)| (bytes(l, c), f))
        .collect::<Vec<_>>();
    let mut idx = Vec::with_capacity(ops.len());
    let tmp = TempDir::new().unwrap();
    let mut wal = Wal::with_options(
        tmp.path().into(),
        Options {
            min_file_size: min_file_size as u64,
            max_file_size: max_file_size as u64,
            max_iter_mem: max_iter_mem as usize,
            use_blocks,
            use_direct_io,
            ..Default::default()
        },
    )
    .unwrap();
    for _ in 0..2 {
        for (o, use_file) in &ops {
            if *use_file {
                let mut f = tempfile().unwrap();
                f.write_all(o).unwrap();
                f.seek(std::io::SeekFrom::Start(0)).unwrap();
                let mut f = BufReader::new(f);
                idx.push(wal.write(&mut [&mut f]).unwrap());
            } else {
                idx.push(wal.write(&mut [&mut &o[..]]).unwrap());
            }
        }
        drop(wal);
        wal = Wal::new(tmp.path().into()).unwrap();
        recover(&wal, &idx, &ops);
        iter(&wal, &idx, &ops);
    }
});

fn bytes(len: u16, content: u16) -> Vec<u8> {
    let mut r = rand::rngs::SmallRng::seed_from_u64(content as u64);
    let mut bytes = vec![0; len as usize];
    r.fill(&mut bytes[..(len as usize).min(256)]);
    bytes
}

fn recover(wal: &Wal, idx: &[WalIdx], ops: &[(Vec<u8>, bool)]) {
    let mut ops_iter = idx.iter().copied().zip(ops.iter().cycle());
    wal.recover().unwrap().for_each(|i| {
        let (recovered_idx, mut b) = i.unwrap();
        let mut bytes_recovered = Vec::new();
        b.read_to_end(&mut bytes_recovered).unwrap();
        let (expected_idx, (expected_bytes, _f)) = ops_iter.next().unwrap();
        assert_eq!(recovered_idx, expected_idx);
        assert_eq!(&bytes_recovered, expected_bytes);
    });
}

fn iter(wal: &Wal, idx: &[WalIdx], ops: &[(Vec<u8>, bool)]) {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(
        *ops.get(0).map(|a| a.0.get(0)).flatten().unwrap_or(&0) as u64,
    );
    for _ in 0..5 {
        let offset = rng.gen_range(0..=idx.len());
        let mut count = 0;
        for (a, (expected_idx, (expected_bytes, _))) in wal
            .iter(offset as WalIdx)
            .unwrap()
            .zip(idx.iter().copied().zip(ops.iter().cycle()).skip(offset))
        {
            let (idx, from_wal) = a.unwrap();
            assert_eq!(idx, expected_idx);
            assert_eq!(&from_wal, expected_bytes);
            count += 1;
        }
        assert_eq!(count, idx.len() - offset);
    }
}
