pub mod sled_adapter;
pub mod sqlite_adapter;

#[cfg(test)]
pub mod test;

use core::ops::{Bound, RangeBounds};

use std::borrow::Cow;
use std::cell::Cell;
use std::sync::Arc;

use err_derive::Error;

#[derive(Clone)]
pub struct Db(pub(crate) Arc<dyn IDb>);

#[derive(Clone, Copy)]
pub struct Transaction<'a>(pub(crate) &'a dyn ITx<'a>);

#[derive(Clone)]
pub struct Tree(pub(crate) Arc<dyn IDb>, pub(crate) usize);

pub type ValueIter<'a> = Box<dyn std::iter::Iterator<Item = Result<(Value<'a>, Value<'a>)>> + 'a>;

// ----

pub struct Value<'a>(pub(crate) Box<dyn IValue<'a> + 'a>);

pub trait IValue<'a>: AsRef<[u8]> + core::borrow::Borrow<[u8]> {
	fn take_maybe(&mut self) -> Vec<u8>;
}

impl<'a> Value<'a> {
	#[inline]
	pub fn into_vec(mut self) -> Vec<u8> {
		self.0.take_maybe()
	}
}

impl<'a> AsRef<[u8]> for Value<'a> {
	#[inline]
	fn as_ref(&self) -> &[u8] {
		self.0.as_ref().as_ref()
	}
}

impl<'a> std::borrow::Borrow<[u8]> for Value<'a> {
	#[inline]
	fn borrow(&self) -> &[u8] {
		self.0.as_ref().borrow()
	}
}

impl<'a> core::ops::Deref for Value<'a> {
	type Target = [u8];
	#[inline]
	fn deref(&self) -> &[u8] {
		self.0.as_ref().as_ref()
	}
}

impl<'a, T> PartialEq<T> for Value<'a>
where
	T: AsRef<[u8]>,
{
	fn eq(&self, other: &T) -> bool {
		self.as_ref() == other.as_ref()
	}
}

impl<'a> std::fmt::Debug for Value<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for line in hexdump::hexdump_iter(self.as_ref()) {
			f.write_str(&line)?;
			f.write_str("\n")?;
		}
		Ok(())
	}
}

impl<'a> IValue<'a> for Vec<u8> {
	fn take_maybe(&mut self) -> Vec<u8> {
		std::mem::take(self)
	}
}

impl<'a> From<Value<'a>> for Vec<u8> {
	fn from(v: Value<'a>) -> Vec<u8> {
		v.into_vec()
	}
}

impl<'a> From<Vec<u8>> for Value<'a> {
	fn from(v: Vec<u8>) -> Value<'a> {
		Value(Box::new(v))
	}
}

impl<'a> From<&'a [u8]> for Value<'a> {
	fn from(v: &'a [u8]) -> Value<'a> {
		Value(Box::new(v))
	}
}

impl<'a> IValue<'a> for &'a [u8] {
	fn take_maybe(&mut self) -> Vec<u8> {
		self.to_vec()
	}
}

// ----

#[derive(Debug, Error)]
#[error(display = "{}", _0)]
pub struct Error(pub Cow<'static, str>);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum TxError<E> {
	Abort(E),
	Db(Error),
}
pub type TxResult<R, E> = std::result::Result<R, TxError<E>>;

impl<E> From<Error> for TxError<E> {
	fn from(e: Error) -> TxError<E> {
		TxError::Db(e)
	}
}

// ----

impl Db {
	pub fn open_tree<S: AsRef<str>>(&self, name: S) -> Result<Tree> {
		let tree_id = self.0.open_tree(name.as_ref())?;
		Ok(Tree(self.0.clone(), tree_id))
	}

	pub fn list_trees(&self) -> Result<Vec<String>> {
		self.0.list_trees()
	}

	pub fn transaction<R, E, F>(&self, fun: F) -> TxResult<R, E>
	where
		F: Fn(Transaction<'_>) -> TxResult<R, E>,
	{
		let f = TxFn {
			function: fun,
			result: Cell::new(None),
		};
		let tx_res = self.0.transaction(&f);
		let ret = f
			.result
			.into_inner()
			.expect("Transaction did not store result");

		match tx_res {
			Ok(()) => {
				assert!(matches!(ret, Ok(_)));
				ret
			}
			Err(TxError::Abort(())) => {
				assert!(matches!(ret, Err(TxError::Abort(_))));
				ret
			}
			Err(TxError::Db(e2)) => match ret {
				// Ok was stored -> the error occured when finalizing
				// transaction
				Ok(_) => Err(TxError::Db(e2)),
				// An error was already stored: that's the one we want to
				// return
				Err(TxError::Db(e)) => Err(TxError::Db(e)),
				_ => unreachable!(),
			},
		}
	}

	pub fn import(&self, other: &Db) -> Result<()> {
		let existing_trees = self.list_trees()?;
		if !existing_trees.is_empty() {
			return Err(Error(
				format!(
					"destination database already contains data: {:?}",
					existing_trees
				)
				.into(),
			));
		}

		let tree_names = other.list_trees()?;
		for name in tree_names {
			let tree = self.open_tree(&name)?;
			if tree.len()? > 0 {
				return Err(Error(format!("tree {} already contains data", name).into()));
			}

			let ex_tree = other.open_tree(&name)?;

			let mut i = 0;
			for item in ex_tree.iter()? {
				let (k, v) = item?;
				tree.insert(k, v)?;
				i += 1;
				if i % 1000 == 0 {
					println!("{}: imported {}", name, i);
				}
			}
			println!("{}: finished importing, {} items", name, i);
		}
		Ok(())
	}
}

#[allow(clippy::len_without_is_empty)]
impl Tree {
	pub fn db(&self) -> Db {
		Db(self.0.clone())
	}

	pub fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<Value<'_>>> {
		self.0.get(self.1, key.as_ref())
	}
	pub fn len(&self) -> Result<usize> {
		self.0.len(self.1)
	}

	pub fn insert<T: AsRef<[u8]>, U: AsRef<[u8]>>(&self, key: T, value: U) -> Result<()> {
		self.0.insert(self.1, key.as_ref(), value.as_ref())
	}
	pub fn remove<T: AsRef<[u8]>>(&self, key: T) -> Result<bool> {
		self.0.remove(self.1, key.as_ref())
	}

	pub fn iter(&self) -> Result<ValueIter<'_>> {
		self.0.iter(self.1)
	}
	pub fn iter_rev(&self) -> Result<ValueIter<'_>> {
		self.0.iter_rev(self.1)
	}

	pub fn range<K, R>(&self, range: R) -> Result<ValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range(self.1, get_bound(sb), get_bound(eb))
	}
	pub fn range_rev<K, R>(&self, range: R) -> Result<ValueIter<'_>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range_rev(self.1, get_bound(sb), get_bound(eb))
	}
}

#[allow(clippy::len_without_is_empty)]
impl<'a> Transaction<'a> {
	pub fn get<T: AsRef<[u8]>>(&self, tree: &Tree, key: T) -> Result<Option<Value<'a>>> {
		self.0.get(tree.1, key.as_ref())
	}
	pub fn len(&self, tree: &Tree) -> Result<usize> {
		self.0.len(tree.1)
	}

	pub fn insert<T: AsRef<[u8]>, U: AsRef<[u8]>>(
		&self,
		tree: &Tree,
		key: T,
		value: U,
	) -> Result<()> {
		self.0.insert(tree.1, key.as_ref(), value.as_ref())
	}
	pub fn remove<T: AsRef<[u8]>>(&self, tree: &Tree, key: T) -> Result<bool> {
		self.0.remove(tree.1, key.as_ref())
	}

	pub fn iter(&self, tree: &Tree) -> Result<ValueIter<'a>> {
		self.0.iter(tree.1)
	}
	pub fn iter_rev(&self, tree: &Tree) -> Result<ValueIter<'a>> {
		self.0.iter_rev(tree.1)
	}

	pub fn range<K, R>(&self, tree: &Tree, range: R) -> Result<ValueIter<'a>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range(tree.1, get_bound(sb), get_bound(eb))
	}
	pub fn range_rev<K, R>(&self, tree: &Tree, range: R) -> Result<ValueIter<'a>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range_rev(tree.1, get_bound(sb), get_bound(eb))
	}

	// ----

	pub fn abort<R, E>(self, e: E) -> TxResult<R, E> {
		Err(TxError::Abort(e))
	}

	pub fn commit<R, E>(self, r: R) -> TxResult<R, E> {
		Ok(r)
	}
}

// ---- Internal interfaces

pub(crate) trait IDb: Send + Sync {
	fn open_tree(&self, name: &str) -> Result<usize>;
	fn list_trees(&self) -> Result<Vec<String>>;

	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value<'_>>>;
	fn len(&self, tree: usize) -> Result<usize>;

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()>;
	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool>;

	fn iter(&self, tree: usize) -> Result<ValueIter<'_>>;
	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'_>>;

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>>;
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>>;

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()>;
}

pub(crate) trait ITx<'a> {
	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value<'a>>>;
	fn len(&self, tree: usize) -> Result<usize>;

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()>;
	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool>;

	fn iter(&self, tree: usize) -> Result<ValueIter<'a>>;
	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'a>>;

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>>;
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>>;
}

pub(crate) trait ITxFn {
	fn try_on<'a>(&'a self, tx: &'a dyn ITx<'a>) -> TxFnResult;
}

pub(crate) enum TxFnResult {
	Ok,
	Abort,
	DbErr,
}

struct TxFn<F, R, E>
where
	F: Fn(Transaction<'_>) -> TxResult<R, E>,
{
	function: F,
	result: Cell<Option<TxResult<R, E>>>,
}

impl<F, R, E> ITxFn for TxFn<F, R, E>
where
	F: Fn(Transaction<'_>) -> TxResult<R, E>,
{
	fn try_on<'a>(&'a self, tx: &'a dyn ITx<'a>) -> TxFnResult {
		let res = (self.function)(Transaction(tx));
		let res2 = match &res {
			Ok(_) => TxFnResult::Ok,
			Err(TxError::Abort(_)) => TxFnResult::Abort,
			Err(TxError::Db(_)) => TxFnResult::DbErr,
		};
		self.result.set(Some(res));
		res2
	}
}

// ----

fn get_bound<K: AsRef<[u8]>>(b: Bound<&K>) -> Bound<&[u8]> {
	match b {
		Bound::Included(v) => Bound::Included(v.as_ref()),
		Bound::Excluded(v) => Bound::Excluded(v.as_ref()),
		Bound::Unbounded => Bound::Unbounded,
	}
}
