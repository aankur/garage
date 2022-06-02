use crate::*;

use crate::sled_adapter::SledDb;

fn test_suite(db: Db) -> Result<()> {
	let tree = db.tree("tree")?;

	let va: &[u8] = &b"plop"[..];
	let vb: &[u8] = &b"plip"[..];
	let vc: &[u8] = &b"plup"[..];

	tree.put(b"test", va)?;
	assert_eq!(tree.get(b"test")?, Some(va.into()));

	let res = db.transaction::<_, (), _>(|tx| {
		assert_eq!(tx.get(&tree, b"test")?, Some(va.into()));

		tx.put(&tree, b"test", vb)?;

		assert_eq!(tx.get(&tree, b"test")?, Some(vb.into()));

		tx.commit(12)
	});
	assert!(matches!(res, Ok(12)));
	assert_eq!(tree.get(b"test")?, Some(vb.into()));

	let res = db.transaction::<(), _, _>(|tx| {
		assert_eq!(tx.get(&tree, b"test")?, Some(vb.into()));

		tx.put(&tree, b"test", vc)?;

		assert_eq!(tx.get(&tree, b"test")?, Some(vc.into()));

		tx.abort(42)
	});
	assert!(matches!(res, Err(TxError::Abort(42))));
	assert_eq!(tree.get(b"test")?, Some(vb.into()));

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
