use canopydb::Database;
use tempfile::tempdir;

fn main() {
    let sample_data: [(&[u8], &[u8]); 3] = [(b"baz", b"qux"), (b"foo", b"bar"), (b"qux", b"quux")];
    let _dir = tempdir().unwrap();
    let db = Database::new(_dir.path()).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut tree1 = tx.get_or_create_tree(b"tree1").unwrap();
        let mut tree2 = tx.get_or_create_tree(b"tree2").unwrap();
        for (k, v) in sample_data {
            tree1.insert(k, v).unwrap();
            tree2.insert(k, v).unwrap();
        }
        // the write transaction can read its own writes
        let maybe_value = tree1.get(b"foo").unwrap();
        assert_eq!(maybe_value.as_deref(), Some(&b"bar"[..]));
    }
    // commit to persist the changes
    tx.commit().unwrap();

    // a read only transaction
    let rx = db.begin_read().unwrap();
    let tree = rx.get_tree(b"tree2").unwrap().unwrap();
    let maybe_value = tree.get(b"foo").unwrap();
    assert_eq!(maybe_value.as_deref(), Some(&b"bar"[..]));
    // range iterators are supported
    for (kv_pair_result, sample_pair) in tree
        .range(sample_data[1].0..) // range scan startin from the second item
        .unwrap()
        .zip(sample_data.into_iter().skip(1))
    {
        let (db_k, db_v) = kv_pair_result.unwrap();
        let (s_k, s_v) = sample_pair;
        assert_eq!(db_k.as_ref(), s_k);
        assert_eq!(db_v.as_ref(), s_v);
    }
    // full scan of the tree
    for (kv_pair_result, sample_pair) in tree.iter().unwrap().zip(sample_data) {
        let (db_k, db_v) = kv_pair_result.unwrap();
        let (s_k, s_v) = sample_pair;
        assert_eq!(db_k.as_ref(), s_k);
        assert_eq!(db_v.as_ref(), s_v);
    }
}
