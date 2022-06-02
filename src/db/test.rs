use crate::*;

use crate::sled_adapter::SledDb;

fn test_suite(db: Db) -> Result<()> {
	let tree = db.open_tree("tree")?;

	let ka: &[u8] = &b"test"[..];
	let kb: &[u8] = &b"zwello"[..];
	let kint: &[u8] = &b"tz"[..];
	let va: &[u8] = &b"plop"[..];
	let vb: &[u8] = &b"plip"[..];
	let vc: &[u8] = &b"plup"[..];

	tree.insert(ka, va)?;
	assert_eq!(tree.get(ka)?, Some(va.into()));

	let res = db.transaction::<_, (), _>(|tx| {
		assert_eq!(tx.get(&tree, ka)?, Some(va.into()));

		tx.insert(&tree, ka, vb)?;

		assert_eq!(tx.get(&tree, ka)?, Some(vb.into()));

		tx.commit(12)
	});
	assert!(matches!(res, Ok(12)));
	assert_eq!(tree.get(ka)?, Some(vb.into()));

	let res = db.transaction::<(), _, _>(|tx| {
		assert_eq!(tx.get(&tree, ka)?, Some(vb.into()));

		tx.insert(&tree, ka, vc)?;

		assert_eq!(tx.get(&tree, ka)?, Some(vc.into()));

		tx.abort(42)
	});
	assert!(matches!(res, Err(TxError::Abort(42))));
	assert_eq!(tree.get(ka)?, Some(vb.into()));

	let mut iter = tree.iter()?;
	assert_eq!(iter.next().unwrap().unwrap(), (ka.into(), vb.into()));
	assert!(iter.next().is_none());

	tree.insert(kb, vc)?;
	assert_eq!(tree.get(kb)?, Some(vc.into()));

	let mut iter = tree.iter()?;
	assert_eq!(iter.next().unwrap().unwrap(), (ka.into(), vb.into()));
	assert_eq!(iter.next().unwrap().unwrap(), (kb.into(), vc.into()));
	assert!(iter.next().is_none());

	let mut iter = tree.range(kint..)?;
	assert_eq!(iter.next().unwrap().unwrap(), (kb.into(), vc.into()));
	assert!(iter.next().is_none());

	let mut iter = tree.range_rev(..kint)?;
	assert_eq!(iter.next().unwrap().unwrap(), (ka.into(), vb.into()));
	assert!(iter.next().is_none());

	let mut iter = tree.iter_rev()?;
	assert_eq!(iter.next().unwrap().unwrap(), (kb.into(), vc.into()));
	assert_eq!(iter.next().unwrap().unwrap(), (ka.into(), vb.into()));
	assert!(iter.next().is_none());

	Ok(())
}

#[test]
fn test_sled_db() -> Result<()> {
	let path = mktemp::Temp::new_dir().unwrap();
	let db = SledDb::new(sled::open(path.to_path_buf()).unwrap());
	test_suite(db)?;
	drop(path);
	Ok(())
}
