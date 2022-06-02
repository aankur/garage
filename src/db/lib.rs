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

pub type Value<'a> = Cow<'a, [u8]>;
pub type ValueIter<'a> =
	Box<dyn std::iter::Iterator<Item = Result<(Value<'a>, Value<'a>)>> + 'a>;

pub type Exporter<'a> =
	Box<dyn std::iter::Iterator<Item = Result<(String, ValueIter<'a>)>> + 'a>;

// ----

#[derive(Debug, Error)]
#[error(display = "{}", _0)]
pub struct Error(Cow<'static, str>);

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

	pub fn export<'a>(&'a self) -> Result<Exporter<'a>> {
		self.0.export()
	}

	pub fn import<'a>(&self, ex: Exporter<'a>) -> Result<()> {
		self.0.import(ex)
	}
}

impl Tree {
	pub fn db(&self) -> Db {
		Db(self.0.clone())
	}

	pub fn get<'a, T: AsRef<[u8]>>(&'a self, key: T) -> Result<Option<Value<'a>>> {
		self.0.get(self.1, key.as_ref())
	}
	pub fn len(&self) -> Result<usize> {
		self.0.len(self.1)
	}

	pub fn insert<T: AsRef<[u8]>, U: AsRef<[u8]>>(&self, key: T, value: U) -> Result<()> {
		self.0.insert(self.1, key.as_ref(), value.as_ref())
	}
	pub fn remove<'a, T: AsRef<[u8]>>(&'a self, key: T) -> Result<bool> {
		self.0.remove(self.1, key.as_ref())
	}

	pub fn iter<'a>(&'a self) -> Result<ValueIter<'a>> {
		self.0.iter(self.1)
	}
	pub fn iter_rev<'a>(&'a self) -> Result<ValueIter<'a>> {
		self.0.iter_rev(self.1)
	}

	pub fn range<'a, K, R>(&'a self, range: R) -> Result<ValueIter<'a>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range(self.1, get_bound(sb), get_bound(eb))
	}
	pub fn range_rev<'a, K, R>(&'a self, range: R) -> Result<ValueIter<'a>>
	where
		K: AsRef<[u8]>,
		R: RangeBounds<K>,
	{
		let sb = range.start_bound();
		let eb = range.end_bound();
		self.0.range_rev(self.1, get_bound(sb), get_bound(eb))
	}
}

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

	#[must_use]
	pub fn abort<R, E>(self, e: E) -> TxResult<R, E> {
		Err(TxError::Abort(e))
	}

	#[must_use]
	pub fn commit<R, E>(self, r: R) -> TxResult<R, E> {
		Ok(r)
	}
}

// ---- Internal interfaces

pub(crate) trait IDb: Send + Sync {
	fn open_tree(&self, name: &str) -> Result<usize>;

	fn get<'a>(&'a self, tree: usize, key: &[u8]) -> Result<Option<Value<'a>>>;
	fn len(&self, tree: usize) -> Result<usize>;

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()>;
	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool>;

	fn iter<'a>(&'a self, tree: usize) -> Result<ValueIter<'a>>;
	fn iter_rev<'a>(&'a self, tree: usize) -> Result<ValueIter<'a>>;

	fn range<'a, 'r>(
		&'a self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>>;
	fn range_rev<'a, 'r>(
		&'a self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>>;

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()>;

	fn export<'a>(&'a self) -> Result<Exporter<'a>>;
	fn import<'a>(&self, ex: Exporter<'a>) -> Result<()>;
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
