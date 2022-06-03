use core::ops::Bound;

use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use rusqlite::{params, Connection, Rows, Statement, Transaction};

use crate::{
	Db, Error, Exporter, IDb, ITx, ITxFn, Result, TxError, TxFnResult, TxResult, Value, ValueIter,
};

pub use rusqlite;

// --- err

impl From<rusqlite::Error> for Error {
	fn from(e: rusqlite::Error) -> Error {
		Error(format!("{}", e).into())
	}
}

impl<T> From<rusqlite::Error> for TxError<T> {
	fn from(e: rusqlite::Error) -> TxError<T> {
		TxError::Db(e.into())
	}
}

// -- db

pub struct SqliteDb {
	db: Mutex<Connection>,
	trees: RwLock<Vec<String>>,
}

impl SqliteDb {
	pub fn new(db: rusqlite::Connection) -> Db {
		let s = Self {
			db: Mutex::new(db),
			trees: RwLock::new(Vec::new()),
		};
		Db(Arc::new(s))
	}

	fn get_tree(&self, i: usize) -> Result<String> {
		self.trees
			.read()
			.unwrap()
			.get(i)
			.cloned()
			.ok_or(Error("invalid tree id".into()))
	}
}

impl IDb for SqliteDb {
	fn open_tree(&self, name: &str) -> Result<usize> {
		let mut trees = self.trees.write().unwrap();
		if let Some(i) = trees.iter().position(|x| x == name) {
			Ok(i)
		} else {
			self.db.lock().unwrap().execute(
				&format!(
					"CREATE TABLE IF NOT EXISTS {} (
						k BLOB PRIMARY KEY,
						v BLOB
					)",
					name
				),
				[],
			)?;
			let i = trees.len();
			trees.push(name.to_string());
			Ok(i)
		}
	}

	// ----

	fn get<'a>(&'a self, tree: usize, key: &[u8]) -> Result<Option<Value<'a>>> {
		let tree = self.get_tree(tree)?;
		let db = self.db.lock().unwrap();
		let mut stmt = db.prepare(&format!("SELECT v FROM {} WHERE k = ?1", tree))?;
		let mut res_iter = stmt.query([key])?;
		match res_iter.next()? {
			None => Ok(None),
			Some(v) => Ok(Some(v.get::<_, Vec<u8>>(0)?.into())),
		}
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool> {
		let tree = self.get_tree(tree)?;
		let db = self.db.lock().unwrap();
		let res = db.execute(&format!("DELETE FROM {} WHERE k = ?1", tree), params![key])?;
		Ok(res > 0)
	}

	fn len(&self, tree: usize) -> Result<usize> {
		let tree = self.get_tree(tree)?;
		let db = self.db.lock().unwrap();
		let mut stmt = db.prepare(&format!("SELECT COUNT(*) FROM {}", tree))?;
		let mut res_iter = stmt.query([])?;
		match res_iter.next()? {
			None => Ok(0),
			Some(v) => Ok(v.get::<_, usize>(0)?.into()),
		}
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()> {
		let tree = self.get_tree(tree)?;
		let db = self.db.lock().unwrap();
		db.execute(
			&format!("INSERT OR REPLACE INTO {} (k, v) VALUES (?1, ?2)", tree),
			params![key, value],
		)?;
		Ok(())
	}

	fn iter<'a>(&'a self, tree: usize) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k ASC", tree);
		DbValueIterator::new(self.db.lock().unwrap(), &sql, [])
	}

	fn iter_rev<'a>(&'a self, tree: usize) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k DESC", tree);
		DbValueIterator::new(self.db.lock().unwrap(), &sql, [])
	}

	fn range<'a, 'r>(
		&'a self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k ASC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();
		DbValueIterator::new::<&[&dyn rusqlite::ToSql]>(
			self.db.lock().unwrap(),
			&sql,
			params.as_ref(),
		)
	}
	fn range_rev<'a, 'r>(
		&'a self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k DESC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();
		DbValueIterator::new::<&[&dyn rusqlite::ToSql]>(
			self.db.lock().unwrap(),
			&sql,
			params.as_ref(),
		)
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<(), ()> {
		let trees = self.trees.read().unwrap();
		let mut db = self.db.lock().unwrap();
		let tx = SqliteTx {
			tx: db.transaction()?,
			trees: trees.as_ref(),
		};
		match f.try_on(&tx) {
			TxFnResult::Ok => {
				tx.tx.commit()?;
				Ok(())
			}
			TxFnResult::Abort => {
				tx.tx.rollback()?;
				Err(TxError::Abort(()))
			}
			TxFnResult::DbErr => {
				tx.tx.rollback()?;
				Err(TxError::Db(Error(
					"(this message will be discarded)".into(),
				)))
			}
		}
	}

	// ----

	fn export<'a>(&'a self) -> Result<Exporter<'a>> {
		unimplemented!()
	}

	fn import<'a>(&self, ex: Exporter<'a>) -> Result<()> {
		unimplemented!()
	}

	// ----
}

// ----

struct SqliteTx<'a> {
	tx: Transaction<'a>,
	trees: &'a [String],
}

impl<'a> SqliteTx<'a> {
	fn get_tree(&self, i: usize) -> Result<String> {
		self.trees.get(i).cloned().ok_or(Error(
			"invalid tree id (it might have been openned after the transaction started)".into(),
		))
	}
}

impl<'a> ITx<'a> for SqliteTx<'a> {
	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value<'a>>> {
		let tree = self.get_tree(tree)?;
		let mut stmt = self
			.tx
			.prepare(&format!("SELECT v FROM {} WHERE k = ?1", tree))?;
		let mut res_iter = stmt.query([key])?;
		match res_iter.next()? {
			None => Ok(None),
			Some(v) => Ok(Some(v.get::<_, Vec<u8>>(0)?.into())),
		}
	}
	fn len(&self, tree: usize) -> Result<usize> {
		let tree = self.get_tree(tree)?;
		let mut stmt = self.tx.prepare(&format!("SELECT COUNT(*) FROM {}", tree))?;
		let mut res_iter = stmt.query([])?;
		match res_iter.next()? {
			None => Ok(0),
			Some(v) => Ok(v.get::<_, usize>(0)?.into()),
		}
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<()> {
		let tree = self.get_tree(tree)?;
		self.tx.execute(
			&format!("INSERT OR REPLACE INTO {} (k, v) VALUES (?1, ?2)", tree),
			params![key, value],
		)?;
		Ok(())
	}
	fn remove(&self, tree: usize, key: &[u8]) -> Result<bool> {
		let tree = self.get_tree(tree)?;
		let res = self
			.tx
			.execute(&format!("DELETE FROM {} WHERE k = ?1", tree), params![key])?;
		Ok(res > 0)
	}

	fn iter(&self, _tree: usize) -> Result<ValueIter<'a>> {
		unimplemented!();
	}
	fn iter_rev(&self, _tree: usize) -> Result<ValueIter<'a>> {
		unimplemented!();
	}

	fn range<'r>(
		&self,
		_tree: usize,
		_low: Bound<&'r [u8]>,
		_high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>> {
		unimplemented!();
	}
	fn range_rev<'r>(
		&self,
		_tree: usize,
		_low: Bound<&'r [u8]>,
		_high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>> {
		unimplemented!();
	}
}

// ----

struct DbValueIterator<'a> {
	db: MutexGuard<'a, Connection>,
	stmt: Option<Statement<'a>>,
	iter: Option<Rows<'a>>,
	_pin: PhantomPinned,
}

impl<'a> DbValueIterator<'a> {
	fn new<P: rusqlite::Params>(
		db: MutexGuard<'a, Connection>,
		sql: &str,
		args: P,
	) -> Result<ValueIter<'a>> {
		let res = DbValueIterator {
			db: db,
			stmt: None,
			iter: None,
			_pin: PhantomPinned,
		};
		let mut boxed = Box::pin(res);

		unsafe {
			let db = NonNull::from(&boxed.db);
			let stmt = db.as_ref().prepare(&sql)?;

			let mut_ref: Pin<&mut DbValueIterator<'a>> = Pin::as_mut(&mut boxed);
			Pin::get_unchecked_mut(mut_ref).stmt = Some(stmt);

			let mut stmt = NonNull::from(&boxed.stmt);
			let iter = stmt.as_mut().as_mut().unwrap().query(args)?;

			let mut_ref: Pin<&mut DbValueIterator<'a>> = Pin::as_mut(&mut boxed);
			Pin::get_unchecked_mut(mut_ref).iter = Some(iter);
		}

		Ok(Box::new(DbValueIteratorPin(boxed)))
	}
}

impl<'a> Drop for DbValueIterator<'a> {
	fn drop(&mut self) {
		drop(self.iter.take());
		drop(self.stmt.take());
	}
}

struct DbValueIteratorPin<'a>(Pin<Box<DbValueIterator<'a>>>);

impl<'a> Iterator for DbValueIteratorPin<'a> {
	type Item = Result<(Value<'a>, Value<'a>)>;

	fn next(&mut self) -> Option<Self::Item> {
		let next = unsafe {
			let mut_ref: Pin<&mut DbValueIterator<'a>> = Pin::as_mut(&mut self.0);
			Pin::get_unchecked_mut(mut_ref).iter.as_mut()?.next()
		};
		let row = match next {
			Err(e) => return Some(Err(e.into())),
			Ok(None) => return None,
			Ok(Some(r)) => r,
		};
		let k = match row.get::<_, Vec<u8>>(0) {
			Err(e) => return Some(Err(e.into())),
			Ok(x) => x,
		};
		let v = match row.get::<_, Vec<u8>>(1) {
			Err(e) => return Some(Err(e.into())),
			Ok(y) => y,
		};
		Some(Ok((k.into(), v.into())))
	}
}

// ----

fn bounds_sql<'r>(low: Bound<&'r [u8]>, high: Bound<&'r [u8]>) -> (String, Vec<Vec<u8>>) {
	let mut sql = String::new();
	let mut params: Vec<Vec<u8>> = vec![];

	match low {
		Bound::Included(b) => {
			sql.push_str(" WHERE k >= ?1");
			params.push(b.to_vec());
		}
		Bound::Excluded(b) => {
			sql.push_str(" WHERE k > ?1");
			params.push(b.to_vec());
		}
		Bound::Unbounded => (),
	};

	match high {
		Bound::Included(b) => {
			if !params.is_empty() {
				sql.push_str(" AND k <= ?2");
			} else {
				sql.push_str(" WHERE k <= ?1");
			}
			params.push(b.to_vec());
		}
		Bound::Excluded(b) => {
			if !params.is_empty() {
				sql.push_str(" AND k < ?2");
			} else {
				sql.push_str(" WHERE k < ?1");
			}
			params.push(b.to_vec());
		}
		Bound::Unbounded => (),
	}

	(sql, params)
}
