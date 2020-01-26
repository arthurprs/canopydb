use canopydb::{Database, TreeOptions};
use tempfile::tempdir;

fn main() {
    let _dir = tempdir().unwrap();
    let db = Database::new(_dir.path()).unwrap();
    // fixed sized key-value pair enables a highly optimized search tree
    let mut tree_options = TreeOptions::default();
    tree_options.fixed_key_len = 8;
    tree_options.fixed_value_len = 8;
    let tx = db.begin_write().unwrap();

    let mut tree = tx
        .get_or_create_tree_with(b"int_pairs", tree_options)
        .unwrap();
    // use BigEndian integers for keys so they're sorted in the database, which is byte sorted
    tree.insert(&0u64.to_be_bytes(), &0u64.to_ne_bytes())
        .unwrap();
    tree.insert(&1u64.to_be_bytes(), &2u64.to_ne_bytes())
        .unwrap();
    tree.insert(&2u64.to_be_bytes(), &4u64.to_ne_bytes())
        .unwrap();
    drop(tree);
    tx.commit().unwrap();

    let rx = db.begin_read().unwrap();
    let tree = rx.get_tree(b"int_pairs").unwrap().unwrap();

    // get 1
    let maybe_value = tree.get(&1u64.to_be_bytes()).unwrap();
    assert_eq!(maybe_value.as_deref(), Some(&2u64.to_ne_bytes()[..]));

    // range 1..
    let one_key_bytes = 1u64.to_be_bytes();
    let values_from_one = tree.range(&one_key_bytes[..]..).unwrap();
    for pair_result in values_from_one {
        let (k, v) = pair_result.unwrap();
        let k = u64::from_be_bytes(k.as_ref().try_into().unwrap());
        let v = u64::from_ne_bytes(v.as_ref().try_into().unwrap());
        assert!(k >= 1);
        assert_eq!(v, k * 2);
    }
}
