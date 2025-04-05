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
    ops: Vec<Op>,
}

#[derive(Debug, Arbitrary, Clone)]
enum Op {
    Writes(Vec<(LazyBytes, u8)>),
    Trim(u16),
    // Temporarely disabled
    // Corrupt(u8, u32, u8),
    FreshFile,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
struct LazyBytes {
    len: u16,
    seed: u16,
}

impl LazyBytes {
    fn bytes(&self) -> Vec<u8> {
        let mut r = rand::rngs::SmallRng::seed_from_u64(self.seed as u64);
        let mut bytes = vec![0; self.len as usize];
        r.fill(&mut bytes[..(self.len as usize).min(256)]);
        bytes
    }
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
    let tmp = TempDir::new().unwrap();
    let new_wal = || {
        Wal::with_options(
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
        .unwrap()
    };
    let mut wal = new_wal();

    let loops = 3;
    let mut expected = Vec::with_capacity(ops.len() * loops);
    let mut expected_idx = 0u64;
    let mut expected_trim_offset = 0;
    let mut has_corruption = false;
    for loop_idx in 0..loops {
        log::info!("loop {}", loop_idx);
        for op in &ops {
            log::info!("op: {:?}", op);
            match op {
                Op::Writes(writes) => {
                    for &(o, rand_u8) in writes {
                        let use_file = rand_u8 == u8::MAX;
                        let bytes = o.bytes();
                        log::info!("Writing {} bytes", bytes.len());
                        let next_idx = if use_file {
                            let mut f = tempfile().unwrap();
                            f.write_all(&bytes).unwrap();
                            f.seek(std::io::SeekFrom::Start(0)).unwrap();
                            let mut f = BufReader::new(f);
                            wal.write(&mut [&mut f]).unwrap()
                        } else {
                            wal.write(&mut [&mut o.bytes().as_slice()]).unwrap()
                        };
                        assert_eq!(next_idx, expected_idx);
                        expected_idx += 1;
                        expected.push((next_idx, o, use_file));
                    }
                }
                &Op::Trim(p) => {
                    log::info!("Trimming {}", p);
                    wal.trim(p as WalIdx).unwrap();
                    expected_trim_offset += expected[expected_trim_offset..]
                        .iter()
                        .take_while(|(i, _b, _f)| *i < p as u64)
                        .count();
                }
                // &Op::Corrupt(f, offset, byte) => {
                //     wal.fuzz_corrupt(f, offset, byte);
                //     has_corruption = true;
                // }
                Op::FreshFile => wal.switch_to_fresh_file().unwrap(),
            }
        }
        drop(wal);
        wal = new_wal();
        let lost_count = recover(&wal, &expected[expected_trim_offset..], has_corruption);
        expected.drain(expected.len() - lost_count..);
        expected_idx -= lost_count as u64;
        has_corruption = false;
    }
});

/// This is a test to check that the WAL can recover correctly
/// Returns the number of lost entries
fn recover(wal: &Wal, expected: &[(WalIdx, LazyBytes, bool)], has_corruption: bool) -> usize {
    log::info!("Recovering");
    let mut expected_it = expected.iter().copied().peekable();
    let mut recovered_count = 0;
    wal.recover().unwrap().for_each(|i| {
        let (recovered_idx, mut b) = i.unwrap();
        log::debug!("Recovered {recovered_idx}");
        let mut bytes_recovered = Vec::new();
        b.read_to_end(&mut bytes_recovered).unwrap();
        let &(expected_idx, expected_bytes, _f) = expected_it.peek().unwrap();
        if recovered_idx < expected_idx {
            log::info!("Recovered {} < expected {}", recovered_idx, expected_idx);
            return;
        }
        assert_eq!(recovered_idx, expected_idx);
        assert_eq!(&bytes_recovered, expected_bytes.bytes().as_slice());
        expected_it.next();
        recovered_count += 1;
    });
    wal.finish_recover().unwrap();
    if !has_corruption {
        assert!(expected_it.peek().is_none());
    }
    if !expected.is_empty() {
        assert_ne!(wal.num_files(), 0);
    }
    expected.len() - recovered_count
}
