use core::ops::Bound;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwapOption;

use sled::transaction::{
	ConflictableTransactionError, TransactionError, Transactional, TransactionalTree,
	UnabortableTransactionError,
};

use crate::{Db, Error, IDb, ITx, ITxFn, Result, TxError, TxFnResult, TxResult, Value, ValueIter};

pub use sled;

impl From<sled::Error> for Error {
	fn from(e: sled::Error) -> Error {
		Error(format!("{}", e).into())
	}
}

pub struct SledDb {
	db: sled::Db,
	trees: RwLock<(Vec<sled::Tree>, HashMap<String, usize>)>,
}

impl SledDb {
	pub fn new(db: sled::Db) -> Db {
		let s = Self {
			db,
			trees: RwLock::new((Vec::new(), HashMap::new())),
		};
		Db(Arc::new(s))
	}

	fn get_tree(&self, i: usize) -> Result<sled::Tree> {
		self.trees
			.read()
			.unwrap()
			.0
			.get(i)
			.cloned()
			.ok_or(Error("invalid tree id".into()))
	}
}

impl IDb for SledDb {
	fn open_tree(&self, name: &str) -> Result<usize> {
		let mut trees = self.trees.write().unwrap();
		if let Some(i) = trees.1.get(name) {
			Ok(*i)
		} else {
			let tree = self.db.open_tree(name)?;
			let i = trees.0.len();
			trees.0.push(tree);
			trees.1.insert(name.to_string(), i);
			Ok(i)
		}
	}

	fn get<'a>(&'a self, tree: usize, key: &[u8]) -> Result<Option<Value<'a>>> {
		let tree = self.get_tree(tree)?;
		Ok(tree.get(key)?.map(|v| v.to_vec().into()))
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool> {
		let tree = self.get_tree(tree)?;
		Ok(tree.remove(key)?.is_some())
	}

	fn len(&self, tree: usize) -> Result<usize> {
		let tree = self.get_tree(tree)?;
		Ok(tree.len())
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()> {
		let tree = self.get_tree(tree)?;
		tree.insert(key, value)?;
		Ok(())
	}

	fn iter<'a>(&'a self, tree: usize) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		Ok(Box::new(tree.iter().map(|v| {
			v.map(|(x, y)| (x.to_vec().into(), y.to_vec().into()))
				.map_err(Into::into)
		})))
	}

	fn iter_rev<'a>(&'a self, tree: usize) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		Ok(Box::new(tree.iter().rev().map(|v| {
			v.map(|(x, y)| (x.to_vec().into(), y.to_vec().into()))
				.map_err(Into::into)
		})))
	}

	fn range<'a, 'r>(
		&'a self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		Ok(Box::new(tree.range::<&'r [u8], _>((low, high)).map(|v| {
			v.map(|(x, y)| (x.to_vec().into(), y.to_vec().into()))
				.map_err(Into::into)
		})))
	}
	fn range_rev<'a, 'r>(
		&'a self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		Ok(Box::new(tree.range::<&'r [u8], _>((low, high)).rev().map(
			|v| {
				v.map(|(x, y)| (x.to_vec().into(), y.to_vec().into()))
					.map_err(Into::into)
			},
		)))
	}

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()> {
		let trees = self.trees.read().unwrap();
		let res = trees.0.transaction(|txtrees| {
			let tx = SledTx {
				trees: txtrees,
				err: ArcSwapOption::new(None),
			};
			match f.try_on(&tx) {
				TxFnResult::Ok => {
					assert!(tx.err.into_inner().is_none());
					Ok(())
				}
				TxFnResult::Abort => {
					assert!(tx.err.into_inner().is_none());
					Err(ConflictableTransactionError::Abort(()))
				}
				TxFnResult::Err => {
					let err_arc = tx
						.err
						.into_inner()
						.expect("Transaction did not store error");
					let err = Arc::try_unwrap(err_arc).ok().expect("Many refs");
					Err(err.into())
				}
			}
		});
		match res {
			Ok(()) => Ok(()),
			Err(TransactionError::Abort(())) => Err(TxError::Abort(())),
			Err(TransactionError::Storage(s)) => Err(TxError::Db(s.into())),
		}
	}
}

// ----

struct SledTx<'a> {
	trees: &'a [TransactionalTree],
	err: ArcSwapOption<UnabortableTransactionError>,
}

impl<'a> SledTx<'a> {
	fn save_error<R>(&self, v: std::result::Result<R, UnabortableTransactionError>) -> Result<R> {
		match v {
			Ok(x) => Ok(x),
			Err(e) => {
				let txt = format!("{}", e);
				self.err.store(Some(Arc::new(e)));
				Err(Error(txt.into()))
			}
		}
	}
}

impl<'a> ITx<'a> for SledTx<'a> {
	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value<'a>>> {
		let tree = self
			.trees
			.get(tree)
			.ok_or(Error("invalid tree id".into()))?;
		let tmp = self.save_error(tree.get(key))?;
		Ok(tmp.map(|v| v.to_vec().into()))
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()> {
		let tree = self
			.trees
			.get(tree)
			.ok_or(Error("invalid tree id".into()))?;
		self.save_error(tree.insert(key, value))?;
		Ok(())
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool> {
		let tree = self
			.trees
			.get(tree)
			.ok_or(Error("invalid tree id".into()))?;
		Ok(self.save_error(tree.remove(key))?.is_some())
	}
}
