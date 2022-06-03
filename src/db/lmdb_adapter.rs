use core::marker::PhantomPinned;
use core::ops::Bound;
use core::pin::Pin;
use core::ptr::NonNull;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use lmdb::{
	Database, DatabaseFlags, Environment, RoTransaction, RwTransaction, Transaction, WriteFlags,
};

use crate::{
	Db, Error, IDb, ITx, ITxFn, IValue, Result, TxError, TxFnResult, TxResult, Value, ValueIter,
};

pub use lmdb;

// -- err

impl From<lmdb::Error> for Error {
	fn from(e: lmdb::Error) -> Error {
		Error(format!("LMDB: {}", e).into())
	}
}

impl<T> From<lmdb::Error> for TxError<T> {
	fn from(e: lmdb::Error) -> TxError<T> {
		TxError::Db(e.into())
	}
}

// -- db

pub struct LmdbDb {
	db: lmdb::Environment,
	trees: RwLock<(Vec<lmdb::Database>, HashMap<String, usize>)>,
}

impl LmdbDb {
	pub fn init(db: lmdb::Environment) -> Db {
		let s = Self {
			db,
			trees: RwLock::new((Vec::new(), HashMap::new())),
		};
		Db(Arc::new(s))
	}

	fn get_tree(&self, i: usize) -> Result<lmdb::Database> {
		self.trees
			.read()
			.unwrap()
			.0
			.get(i)
			.cloned()
			.ok_or_else(|| Error("invalid tree id".into()))
	}
}

impl IDb for LmdbDb {
	fn open_tree(&self, name: &str) -> Result<usize> {
		let mut trees = self.trees.write().unwrap();
		if let Some(i) = trees.1.get(name) {
			Ok(*i)
		} else {
			let tree = self.db.create_db(Some(name), DatabaseFlags::empty())?;
			let i = trees.0.len();
			trees.0.push(tree);
			trees.1.insert(name.to_string(), i);
			Ok(i)
		}
	}

	fn list_trees(&self) -> Result<Vec<String>> {
		unimplemented!()
	}

	// ----

	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value<'_>>> {
		let tree = self.get_tree(tree)?;

		let res = TxAndValue {
			tx: self.db.begin_ro_txn()?,
			value: NonNull::dangling(),
			_pin: PhantomPinned,
		};
		let mut boxed = Box::pin(res);

		unsafe {
			let tx = NonNull::from(&boxed.tx);
			let val = match tx.as_ref().get(tree, &key) {
				Err(lmdb::Error::NotFound) => return Ok(None),
				v => v?,
			};

			let mut_ref: Pin<&mut TxAndValue<'_>> = Pin::as_mut(&mut boxed);
			Pin::get_unchecked_mut(mut_ref).value = NonNull::from(&val);
		}

		Ok(Some(Value(Box::new(TxAndValuePin(boxed)))))
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool> {
		unimplemented!()
	}

	fn len(&self, tree: usize) -> Result<usize> {
		unimplemented!()
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()> {
		let tree = self.get_tree(tree)?;
		let mut tx = self.db.begin_rw_txn()?;
		tx.put(tree, &key, &value, WriteFlags::empty())?;
		tx.commit()?;
		Ok(())
	}

	fn iter(&self, tree: usize) -> Result<ValueIter<'_>> {
		unimplemented!()
	}

	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'_>> {
		unimplemented!()
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		unimplemented!()
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		unimplemented!()
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()> {
		let trees = self.trees.read().unwrap();
		let mut tx = LmdbTx {
			trees: &trees.0[..],
			tx: self.db.begin_rw_txn()?,
		};

		let res = f.try_on(&mut tx);
		match res {
			TxFnResult::Ok => {
				tx.tx.commit()?;
				Ok(())
			}
			TxFnResult::Abort => {
				tx.tx.abort();
				Err(TxError::Abort(()))
			}
			TxFnResult::DbErr => {
				tx.tx.abort();
				Err(TxError::Db(Error(
					"(this message will be discarded)".into(),
				)))
			}
		}
	}
}

// ----

struct LmdbTx<'a, 'db> {
	trees: &'db [Database],
	tx: RwTransaction<'a>,
}

impl<'a, 'db> LmdbTx<'a, 'db> {
	fn get_tree(&self, i: usize) -> Result<&Database> {
		self.trees.get(i).ok_or_else(|| {
			Error(
				"invalid tree id (it might have been openned after the transaction started)".into(),
			)
		})
	}
}

impl<'a, 'db> ITx for LmdbTx<'a, 'db> {
	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value<'_>>> {
		let tree = self.get_tree(tree)?;
		match self.tx.get::<'a, _>(*tree, &key) {
			Err(lmdb::Error::NotFound) => Ok(None),
			Err(e) => Err(e.into()),
			Ok(v) => Ok(Some(Value(Box::new(v)))),
		}
	}
	fn len(&self, _tree: usize) -> Result<usize> {
		unimplemented!(".len() in transaction not supported with LMDB backend")
	}

	fn insert(&mut self, tree: usize, key: &[u8], value: &[u8]) -> Result<()> {
		let tree = self.get_tree(tree)?;
		self.tx.put(*tree, &key, &value, WriteFlags::empty())?;
		Ok(())
	}
	fn remove(&mut self, tree: usize, key: &[u8]) -> Result<bool> {
		let tree = self.get_tree(tree)?;
		match self.tx.del::<'a, _>(*tree, &key, None) {
			Ok(()) => Ok(true),
			Err(lmdb::Error::NotFound) => Ok(false),
			Err(e) => Err(e.into()),
		}
	}

	fn iter(&self, _tree: usize) -> Result<ValueIter<'_>> {
		unimplemented!("Iterators in transactions not supported with LMDB backend");
	}
	fn iter_rev(&self, _tree: usize) -> Result<ValueIter<'_>> {
		unimplemented!("Iterators in transactions not supported with LMDB backend");
	}

	fn range<'r>(
		&self,
		_tree: usize,
		_low: Bound<&'r [u8]>,
		_high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		unimplemented!("Iterators in transactions not supported with LMDB backend");
	}
	fn range_rev<'r>(
		&self,
		_tree: usize,
		_low: Bound<&'r [u8]>,
		_high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		unimplemented!("Iterators in transactions not supported with LMDB backend");
	}
}

// ----

struct TxAndValue<'a> {
	tx: RoTransaction<'a>,
	value: NonNull<&'a [u8]>,
	_pin: PhantomPinned,
}

struct TxAndValuePin<'a>(Pin<Box<TxAndValue<'a>>>);

impl<'a> IValue<'a> for TxAndValuePin<'a> {
	fn take_maybe(&mut self) -> Vec<u8> {
		self.as_ref().to_vec()
	}
}

impl<'a> AsRef<[u8]> for TxAndValuePin<'a> {
	fn as_ref(&self) -> &[u8] {
		unsafe { self.0.value.as_ref() }
	}
}

impl<'a> std::borrow::Borrow<[u8]> for TxAndValuePin<'a> {
	fn borrow(&self) -> &[u8] {
		self.as_ref()
	}
}
