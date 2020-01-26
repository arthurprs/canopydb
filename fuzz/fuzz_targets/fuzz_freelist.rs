#![feature(btree_extract_if)]
#![no_main]

use std::collections::BTreeSet;

use canopydb::freelist::Freelist;
use libfuzzer_sys::arbitrary;
use libfuzzer_sys::arbitrary::*;
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
enum Op {
    Free(u16, u16),
    Allocate(u16),
    AllocateBulk(u16),
    Remove(u16, u16),
}

#[derive(Debug, Arbitrary)]
struct Input {
    ops: Vec<Op>,
}

fn btree_remove_range(set: &mut BTreeSet<u32>, start: u32, end: u32) -> u32 {
    set.extract_if(|&i| i >= start && i < end).count() as u32
}

fuzz_target!(|input: Input| {
    let mut subject = Freelist::default();
    let mut model = BTreeSet::<u32>::new();
    for op in input.ops {
        match op {
            Op::Free(start, span) => {
                let span = span.max(1) as u32;
                let start = start as u32;
                let end = start + span;
                if model.range(start..end).next().is_none() {
                    subject.free(start, span).unwrap();
                    model.extend(start..end);
                }
            }
            Op::Remove(start, span) => {
                let span = span as u32;
                let start = start as u32;
                let removed_subj = subject.remove(start, span);
                let removed_model = btree_remove_range(&mut model, start, start + span);
                assert_eq!(removed_subj, removed_model);
            }
            Op::Allocate(span) => {
                let span = span.max(1) as u32;
                if let Some(start) = subject.allocate(span) {
                    let removed = btree_remove_range(&mut model, start, start + span);
                    assert_eq!(removed, span);
                }
            }
            Op::AllocateBulk(span) => {
                let span = span as u32;
                let allocated = subject.bulk_allocate(span, true);
                for i in allocated.iter_pages() {
                    assert!(model.remove(&i));
                }
            }
        }
        assert_eq!(subject.len(), model.len() as u32);
    }
});
