use crate::*;

use crate::sled_adapter::SledDb;
use crate::sqlite_adapter::SqliteDb;

fn test_suite(db: Db) {
	let tree = db.open_tree("tree").unwrap();

	let ka: &[u8] = &b"test"[..];
	let kb: &[u8] = &b"zwello"[..];
	let kint: &[u8] = &b"tz"[..];
	let va: &[u8] = &b"plop"[..];
	let vb: &[u8] = &b"plip"[..];
	let vc: &[u8] = &b"plup"[..];

	tree.insert(ka, va).unwrap();
	assert_eq!(tree.get(ka).unwrap().unwrap(), va);

	let res = db.transaction::<_, (), _>(|tx| {
		assert_eq!(tx.get(&tree, ka).unwrap().unwrap(), va);

		tx.insert(&tree, ka, vb).unwrap();

		assert_eq!(tx.get(&tree, ka).unwrap().unwrap(), vb);

		tx.commit(12)
	});
	assert!(matches!(res, Ok(12)));
	assert_eq!(tree.get(ka).unwrap().unwrap(), vb);

	let res = db.transaction::<(), _, _>(|tx| {
		assert_eq!(tx.get(&tree, ka).unwrap().unwrap(), vb);

		tx.insert(&tree, ka, vc).unwrap();

		assert_eq!(tx.get(&tree, ka).unwrap().unwrap(), vc);

		tx.abort(42)
	});
	assert!(matches!(res, Err(TxError::Abort(42))));
	assert_eq!(tree.get(ka).unwrap().unwrap(), vb);

	let mut iter = tree.iter().unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
	assert!(iter.next().is_none());
	drop(iter);

	tree.insert(kb, vc).unwrap();
	assert_eq!(tree.get(kb).unwrap().unwrap(), vc);

	let mut iter = tree.iter().unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
	assert!(iter.next().is_none());
	drop(iter);

	let mut iter = tree.range(kint..).unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
	assert!(iter.next().is_none());
	drop(iter);

	let mut iter = tree.range_rev(..kint).unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
	assert!(iter.next().is_none());
	drop(iter);

	let mut iter = tree.iter_rev().unwrap();
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (kb, vc));
	let next = iter.next().unwrap().unwrap();
	assert_eq!((next.0.as_ref(), next.1.as_ref()), (ka, vb));
	assert!(iter.next().is_none());
	drop(iter);
}

#[test]
fn test_sled_db() {
	let path = mktemp::Temp::new_dir().unwrap();
	let db = SledDb::init(sled::open(path.to_path_buf()).unwrap());
	test_suite(db);
	drop(path);
}

#[test]
fn test_sqlite_db() {
	let db = SqliteDb::init(rusqlite::Connection::open_in_memory().unwrap());
	test_suite(db);
}
