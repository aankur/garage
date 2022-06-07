//! This hack allows a db tree to keep in RAM a counter of the number of entries
//! it contains, which is used to call .len() on it.  This is usefull only for
//! the sled backend where .len() otherwise would have to traverse the whole
//! tree to count items.  For sqlite and lmdb, this is mostly useless (but
//! hopefully not harmfull!). Note that a CountedTree cannot be part of a
//! transaction.

use std::sync::{
	atomic::{AtomicUsize, Ordering},
	Arc,
};

use crate::{Result, Tree, TxError, Value, ValueIter};

#[derive(Clone)]
pub struct CountedTree(Arc<CountedTreeInternal>);

struct CountedTreeInternal {
	tree: Tree,
	len: AtomicUsize,
}

impl CountedTree {
	pub fn new(tree: Tree) -> Result<Self> {
		let len = tree.len()?;
		Ok(Self(Arc::new(CountedTreeInternal {
			tree,
			len: AtomicUsize::new(len),
		})))
	}

	pub fn len(&self) -> usize {
		self.0.len.load(Ordering::Relaxed)
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Value>> {
		self.0.tree.get(key)
	}

	pub fn first(&self) -> Result<Option<(Value, Value)>> {
		self.0.tree.first()
	}

	pub fn iter(&self) -> Result<ValueIter<'_>> {
		self.0.tree.iter()
	}

	// ---- writing functions ----

	pub fn insert<K, V>(&self, key: K, value: V) -> Result<bool>
	where
		K: AsRef<[u8]>,
		V: AsRef<[u8]>,
	{
		let inserted = self.0.tree.insert(key, value)?;
		if inserted {
			self.0.len.fetch_add(1, Ordering::Relaxed);
		}
		Ok(inserted)
	}

	pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
		let removed = self.0.tree.remove(key)?;
		if removed {
			self.0.len.fetch_sub(1, Ordering::Relaxed);
		}
		Ok(removed)
	}

	/*
	pub fn pop_min(&self) -> Result<Option<(Value, Value)>> {
		let res = self.0.tree.pop_min();
		if let Ok(Some(_)) = &res {
			self.0.len.fetch_sub(1, Ordering::Relaxed);
		};
		res
	}
	*/

	pub fn compare_and_swap<K, OV, NV>(
		&self,
		key: K,
		expected_old: Option<OV>,
		new: Option<NV>,
	) -> Result<bool>
	where
		K: AsRef<[u8]>,
		OV: AsRef<[u8]>,
		NV: AsRef<[u8]>,
	{
		let old_some = expected_old.is_some();
		let new_some = new.is_some();

		match self.0.tree.db().transaction(|mut tx| {
			let old_val = tx.get(&self.0.tree, &key)?;
			if old_val.as_ref().map(|x| &x[..]) == expected_old.as_ref().map(AsRef::as_ref) {
				match &new {
					Some(v) => {
						tx.insert(&self.0.tree, &key, v)?;
					}
					None => {
						tx.remove(&self.0.tree, &key)?;
					}
				}
				tx.commit(())
			} else {
				tx.abort(())
			}
		}) {
			Ok(()) => {
				match (old_some, new_some) {
					(false, true) => {
						self.0.len.fetch_add(1, Ordering::Relaxed);
					}
					(true, false) => {
						self.0.len.fetch_sub(1, Ordering::Relaxed);
					}
					_ => (),
				}
				Ok(true)
			}
			Err(TxError::Abort(())) => Ok(false),
			Err(TxError::Db(e)) => Err(e),
		}
	}
}
