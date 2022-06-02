pub mod sled_adapter;

#[cfg(test)]
pub mod test;

use std::borrow::Cow;
use std::sync::Arc;

use arc_swap::ArcSwapOption;

#[derive(Clone)]
pub struct Db(pub(crate) Arc<dyn IDb>);

#[derive(Clone)]
pub struct Transaction<'a>(pub(crate) &'a dyn ITx<'a>);

#[derive(Clone)]
pub struct Tree(pub(crate) Arc<dyn IDb>, pub(crate) usize);

pub type Value<'a> = Cow<'a, [u8]>;
pub type ValueIter<'a> = Box<dyn std::iter::Iterator<Item = Result<(Value<'a>, Value<'a>)>> + 'a>;

// ----

#[derive(Debug)]
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
	pub fn tree<S: AsRef<str>>(&self, name: S) -> Result<Tree> {
		let tree_id = self.0.tree(name.as_ref())?;
		Ok(Tree(self.0.clone(), tree_id))
	}

	pub fn transaction<R, E, F>(&self, fun: F) -> TxResult<R, E>
	where
		F: Fn(Transaction<'_>) -> TxResult<R, E> + Send + Sync,
		R: Send + Sync,
		E: Send + Sync,
	{
		let f = TxFn {
			function: fun,
			result: ArcSwapOption::new(None),
		};
		match self.0.transaction(&f) {
			Err(TxError::Db(e)) => Err(TxError::Db(e)),
			Err(TxError::Abort(())) => {
				let r_arc = f
					.result
					.into_inner()
					.expect("Transaction did not store result");
				let r = Arc::try_unwrap(r_arc).ok().expect("Many refs");
				assert!(matches!(r, Err(TxError::Abort(_))));
				r
			}
			Ok(()) => {
				let r_arc = f
					.result
					.into_inner()
					.expect("Transaction did not store result");
				let r = Arc::try_unwrap(r_arc).ok().expect("Many refs");
				assert!(matches!(r, Ok(_)));
				r
			}
		}
	}
}

impl Tree {
	pub fn get<'a, T: AsRef<[u8]>>(&'a self, key: T) -> Result<Option<Value<'a>>> {
		self.0.get(self.1, key.as_ref())
	}

	pub fn put<T: AsRef<[u8]>, U: AsRef<[u8]>>(&self, key: T, value: U) -> Result<()> {
		self.0.put(self.1, key.as_ref(), value.as_ref())
	}

	pub fn iter<'a>(&'a self, reverse: bool) -> Result<ValueIter<'a>> {
		self.0.range(self.1, None, reverse)
	}

	pub fn range<'a, T: AsRef<[u8]>>(&'a self, start: T, reverse: bool) -> Result<ValueIter<'a>> {
		self.0.range(self.1, Some(start.as_ref()), reverse)
	}
}

impl<'a> Transaction<'a> {
	pub fn get<T: AsRef<[u8]>>(&self, tree: &Tree, key: T) -> Result<Option<Value<'a>>> {
		self.0.get(tree.1, key.as_ref())
	}

	pub fn put<T: AsRef<[u8]>, U: AsRef<[u8]>>(&self, tree: &Tree, key: T, value: U) -> Result<()> {
		self.0.put(tree.1, key.as_ref(), value.as_ref())
	}

	#[must_use]
	pub fn abort<R, E>(self, e: E) -> TxResult<R, E>
	where
		R: Send + Sync,
		E: Send + Sync,
	{
		Err(TxError::Abort(e))
	}

	#[must_use]
	pub fn commit<R, E>(self, r: R) -> TxResult<R, E>
	where
		R: Send + Sync,
		E: Send + Sync,
	{
		Ok(r)
	}
}

// ---- Internal interfaces

pub(crate) trait IDb: Send + Sync {
	fn tree(&self, name: &str) -> Result<usize>;

	fn get<'a>(&'a self, tree: usize, key: &[u8]) -> Result<Option<Value<'a>>>;
	fn put(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()>;
	fn range<'a>(
		&'a self,
		tree: usize,
		start: Option<&[u8]>,
		reverse: bool,
	) -> Result<ValueIter<'a>>;

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()>;
}

pub(crate) trait ITx<'a> {
	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value<'a>>>;
	fn put(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()>;
}

pub(crate) trait ITxFn: Send + Sync {
	fn try_on<'a>(&'a self, tx: &'a dyn ITx<'a>) -> TxFnResult;
}

enum TxFnResult {
	Abort,
	Ok,
	Err,
}

struct TxFn<F, R, E>
where
	F: Fn(Transaction<'_>) -> TxResult<R, E> + Send + Sync,
	R: Send + Sync,
	E: Send + Sync,
{
	function: F,
	result: ArcSwapOption<TxResult<R, E>>,
}

impl<F, R, E> ITxFn for TxFn<F, R, E>
where
	F: Fn(Transaction<'_>) -> TxResult<R, E> + Send + Sync,
	R: Send + Sync,
	E: Send + Sync,
{
	fn try_on<'a>(&'a self, tx: &'a dyn ITx<'a>) -> TxFnResult {
		let res = (self.function)(Transaction(tx));
		let retval = match &res {
			Ok(_) => TxFnResult::Ok,
			Err(TxError::Abort(_)) => TxFnResult::Abort,
			Err(TxError::Db(_)) => TxFnResult::Err,
		};
		self.result.store(Some(Arc::new(res)));
		retval
	}
}
