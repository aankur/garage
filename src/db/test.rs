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
	assert_eq!(tree.get(ka).unwrap(), Some(va.into()));

	let res = db.transaction::<_, (), _>(|tx| {
		assert_eq!(tx.get(&tree, ka).unwrap(), Some(va.into()));

		tx.insert(&tree, ka, vb).unwrap();

		assert_eq!(tx.get(&tree, ka).unwrap(), Some(vb.into()));

		tx.commit(12)
	});
	assert!(matches!(res, Ok(12)));
	assert_eq!(tree.get(ka).unwrap(), Some(vb.into()));

	let res = db.transaction::<(), _, _>(|tx| {
		assert_eq!(tx.get(&tree, ka).unwrap(), Some(vb.into()));

		tx.insert(&tree, ka, vc).unwrap();

		assert_eq!(tx.get(&tree, ka).unwrap(), Some(vc.into()));

		tx.abort(42)
	});
	assert!(matches!(res, Err(TxError::Abort(42))));
	assert_eq!(tree.get(ka).unwrap(), Some(vb.into()));

	let mut iter = tree.iter().unwrap();
	assert_eq!(iter.next().unwrap().unwrap(), (ka.into(), vb.into()));
	assert!(iter.next().is_none());

	tree.insert(kb, vc).unwrap();
	assert_eq!(tree.get(kb).unwrap(), Some(vc.into()));

	let mut iter = tree.iter().unwrap();
	assert_eq!(iter.next().unwrap().unwrap(), (ka.into(), vb.into()));
	assert_eq!(iter.next().unwrap().unwrap(), (kb.into(), vc.into()));
	assert!(iter.next().is_none());

	let mut iter = tree.range(kint..).unwrap();
	assert_eq!(iter.next().unwrap().unwrap(), (kb.into(), vc.into()));
	assert!(iter.next().is_none());

	let mut iter = tree.range_rev(..kint).unwrap();
	assert_eq!(iter.next().unwrap().unwrap(), (ka.into(), vb.into()));
	assert!(iter.next().is_none());

	let mut iter = tree.iter_rev().unwrap();
	assert_eq!(iter.next().unwrap().unwrap(), (kb.into(), vc.into()));
	assert_eq!(iter.next().unwrap().unwrap(), (ka.into(), vb.into()));
	assert!(iter.next().is_none());
}

#[test]
fn test_sled_db() {
	let path = mktemp::Temp::new_dir().unwrap();
	let db = SledDb::new(sled::open(path.to_path_buf()).unwrap());
	test_suite(db);
	drop(path);
}

#[test]
fn test_sqlite_db() {
	let db = SqliteDb::new(rusqlite::Connection::open_in_memory().unwrap());
	test_suite(db);
}
