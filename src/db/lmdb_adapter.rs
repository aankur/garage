use core::marker::PhantomPinned;
use core::ops::Bound;
use core::pin::Pin;
use core::ptr::NonNull;

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::{Arc, RwLock};

use heed::types::ByteSlice;
use heed::{BytesDecode, Env, RoTxn, RwTxn, UntypedDatabase as Database};

use crate::{
	Db, Error, IDb, ITx, ITxFn, IValue, Result, TxError, TxFnResult, TxResult, Value, ValueIter,
};

pub use heed;

// -- err

impl From<heed::Error> for Error {
	fn from(e: heed::Error) -> Error {
		Error(format!("LMDB: {}", e).into())
	}
}

impl<T> From<heed::Error> for TxError<T> {
	fn from(e: heed::Error) -> TxError<T> {
		TxError::Db(e.into())
	}
}

// -- db

pub struct LmdbDb {
	db: heed::Env,
	trees: RwLock<(Vec<Database>, HashMap<String, usize>)>,
}

impl LmdbDb {
	pub fn init(db: Env) -> Db {
		let s = Self {
			db,
			trees: RwLock::new((Vec::new(), HashMap::new())),
		};
		Db(Arc::new(s))
	}

	fn get_tree(&self, i: usize) -> Result<Database> {
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
			let tree = self.db.create_database(Some(name))?;
			let i = trees.0.len();
			trees.0.push(tree);
			trees.1.insert(name.to_string(), i);
			Ok(i)
		}
	}

	fn list_trees(&self) -> Result<Vec<String>> {
		let tree0 = match self.db.open_database::<heed::types::Str, ByteSlice>(None)? {
			Some(x) => x,
			None => return Ok(vec![]),
		};

		let mut ret = vec![];
		let tx = self.db.read_txn()?;
		for item in tree0.iter(&tx)? {
			let (tree_name, _) = item?;
			ret.push(tree_name.to_string());
		}
		drop(tx);

		let mut ret2 = vec![];
		for tree_name in ret {
			if self
				.db
				.open_database::<ByteSlice, ByteSlice>(Some(&tree_name))?
				.is_some()
			{
				ret2.push(tree_name);
			}
		}

		Ok(ret2)
	}

	// ----

	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value<'_>>> {
		let tree = self.get_tree(tree)?;

		let res = TxAndValue {
			tx: self.db.read_txn()?,
			value: NonNull::dangling(),
			_pin: PhantomPinned,
		};
		let mut boxed = Box::pin(res);

		unsafe {
			let tx = NonNull::from(&boxed.tx);
			let val = match tree.get(tx.as_ref(), &key)? {
				None => return Ok(None),
				Some(v) => v,
			};

			let mut_ref: Pin<&mut TxAndValue<'_>> = Pin::as_mut(&mut boxed);
			Pin::get_unchecked_mut(mut_ref).value = NonNull::from(&val);
		}

		Ok(Some(Value(Box::new(TxAndValuePin(boxed)))))
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool> {
		let tree = self.get_tree(tree)?;
		let mut tx = self.db.write_txn()?;
		let deleted = tree.delete(&mut tx, &key)?;
		tx.commit()?;
		Ok(deleted)
	}

	fn len(&self, tree: usize) -> Result<usize> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		Ok(tree.len(&tx)?.try_into().unwrap())
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()> {
		let tree = self.get_tree(tree)?;
		let mut tx = self.db.write_txn()?;
		tree.put(&mut tx, &key, &value)?;
		tx.commit()?;
		Ok(())
	}

	fn iter(&self, tree: usize) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		TxAndIterator::make(tx, |tx| Ok(tree.iter(tx)?))
	}

	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		TxAndIterator::make(tx, |tx| Ok(tree.rev_iter(tx)?))
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		TxAndIterator::make(tx, |tx| Ok(tree.range(tx, &(low, high))?))
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let tx = self.db.read_txn()?;
		TxAndIterator::make(tx, |tx| Ok(tree.rev_range(tx, &(low, high))?))
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()> {
		let trees = self.trees.read().unwrap();
		let mut tx = LmdbTx {
			trees: &trees.0[..],
			tx: self.db.write_txn()?,
		};

		let res = f.try_on(&mut tx);
		match res {
			TxFnResult::Ok => {
				tx.tx.commit()?;
				Ok(())
			}
			TxFnResult::Abort => {
				tx.tx.abort()?;
				Err(TxError::Abort(()))
			}
			TxFnResult::DbErr => {
				tx.tx.abort()?;
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
	tx: RwTxn<'a, 'a>,
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
		match tree.get(&self.tx, &key)? {
			Some(v) => Ok(Some(Value(Box::new(v)))),
			None => Ok(None),
		}
	}
	fn len(&self, _tree: usize) -> Result<usize> {
		unimplemented!(".len() in transaction not supported with LMDB backend")
	}

	fn insert(&mut self, tree: usize, key: &[u8], value: &[u8]) -> Result<()> {
		let tree = *self.get_tree(tree)?;
		tree.put(&mut self.tx, &key, &value)?;
		Ok(())
	}
	fn remove(&mut self, tree: usize, key: &[u8]) -> Result<bool> {
		let tree = *self.get_tree(tree)?;
		let deleted = tree.delete(&mut self.tx, &key)?;
		Ok(deleted)
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
	tx: RoTxn<'a>,
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

// ----

type IteratorItem<'a> = heed::Result<(
	<ByteSlice as BytesDecode<'a>>::DItem,
	<ByteSlice as BytesDecode<'a>>::DItem,
)>;

struct TxAndIterator<'a, I>
where
	I: Iterator<Item = IteratorItem<'a>> + 'a,
{
	tx: RoTxn<'a>,
	iter: Option<I>,
	_pin: PhantomPinned,
}

impl<'a, I> TxAndIterator<'a, I>
where
	I: Iterator<Item = IteratorItem<'a>> + 'a,
{
	fn make<F>(tx: RoTxn<'a>, iterfun: F) -> Result<ValueIter<'a>>
	where
		F: FnOnce(&'a RoTxn<'a>) -> Result<I>,
	{
		let res = TxAndIterator {
			tx,
			iter: None,
			_pin: PhantomPinned,
		};
		let mut boxed = Box::pin(res);

		unsafe {
			let tx = NonNull::from(&boxed.tx);
			let iter = iterfun(tx.as_ref())?;

			let mut_ref: Pin<&mut TxAndIterator<'a, I>> = Pin::as_mut(&mut boxed);
			Pin::get_unchecked_mut(mut_ref).iter = Some(iter);
		}

		Ok(Box::new(TxAndIteratorPin(boxed)))
	}
}

impl<'a, I> Drop for TxAndIterator<'a, I>
where
	I: Iterator<Item = IteratorItem<'a>> + 'a,
{
	fn drop(&mut self) {
		drop(self.iter.take());
	}
}

struct TxAndIteratorPin<'a, I: Iterator<Item = IteratorItem<'a>> + 'a>(
	Pin<Box<TxAndIterator<'a, I>>>,
);

impl<'a, I> Iterator for TxAndIteratorPin<'a, I>
where
	I: Iterator<Item = IteratorItem<'a>> + 'a,
{
	type Item = Result<(Value<'a>, Value<'a>)>;

	fn next(&mut self) -> Option<Self::Item> {
		let iter_ref = unsafe {
			let mut_ref: Pin<&mut TxAndIterator<'a, I>> = Pin::as_mut(&mut self.0);
			Pin::get_unchecked_mut(mut_ref).iter.as_mut()
		};
		match iter_ref.unwrap().next() {
			None => None,
			Some(Err(e)) => Some(Err(e.into())),
			Some(Ok((k, v))) => Some(Ok((k.into(), v.into()))),
		}
	}
}
