use core::ops::Bound;

use std::cell::Cell;
use std::sync::{Arc, Mutex, RwLock, MutexGuard};

use ouroboros::self_referencing;

use rusqlite::{params, Connection, Transaction};

use crate::{
	Db, Error, Exporter, IDb, ITx, ITxFn, Result, TxError, TxFnResult, TxResult, Value, ValueIter,
};

pub use rusqlite;

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
		let db = self.db.lock().unwrap();
		let sql = format!("SELECT k, v FROM {} ORDER BY k ASC", tree);
		let mut stmt = db.prepare(&sql)?; 
		let res = stmt.query([])?;
		unimplemented!();
	}

	fn iter_rev<'a>(&'a self, tree: usize) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		unimplemented!();
	}

	fn range<'a, 'r>(
		&'a self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		unimplemented!();
	}
	fn range_rev<'a, 'r>(
		&'a self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'a>> {
		let tree = self.get_tree(tree)?;
		unimplemented!();
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

