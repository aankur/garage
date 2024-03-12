use core::ops::Bound;

use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex, MutexGuard};

use ouroboros::self_referencing;
use rusqlite::{params, Connection, Rows, Statement, Transaction};

use crate::{
	Db, Error, IDb, ITx, ITxFn, OnCommit, Result, TxError, TxFnResult, TxOpError, TxOpResult,
	TxResult, TxValueIter, Value, ValueIter,
};

pub use rusqlite;

// --- err

impl From<rusqlite::Error> for Error {
	fn from(e: rusqlite::Error) -> Error {
		Error(format!("Sqlite: {}", e).into())
	}
}

impl From<rusqlite::Error> for TxOpError {
	fn from(e: rusqlite::Error) -> TxOpError {
		TxOpError(e.into())
	}
}

// -- db

pub struct SqliteDb(Mutex<SqliteDbInner>);

struct SqliteDbInner {
	db: Connection,
	trees: Vec<String>,
}

impl SqliteDb {
	pub fn init(db: rusqlite::Connection) -> Db {
		let s = Self(Mutex::new(SqliteDbInner {
			db,
			trees: Vec::new(),
		}));
		Db(Arc::new(s))
	}
}

impl SqliteDbInner {
	fn get_tree(&self, i: usize) -> Result<&'_ str> {
		self.trees
			.get(i)
			.map(String::as_str)
			.ok_or_else(|| Error("invalid tree id".into()))
	}

	fn internal_get(&self, tree: &str, key: &[u8]) -> Result<Option<Value>> {
		let mut stmt = self
			.db
			.prepare(&format!("SELECT v FROM {} WHERE k = ?1", tree))?;
		let mut res_iter = stmt.query([key])?;
		match res_iter.next()? {
			None => Ok(None),
			Some(v) => Ok(Some(v.get::<_, Vec<u8>>(0)?)),
		}
	}
}

impl IDb for SqliteDb {
	fn engine(&self) -> String {
		format!("sqlite3 v{} (using rusqlite crate)", rusqlite::version())
	}

	fn open_tree(&self, name: &str) -> Result<usize> {
		let name = format!("tree_{}", name.replace(':', "_COLON_"));
		let mut this = self.0.lock().unwrap();

		if let Some(i) = this.trees.iter().position(|x| x == &name) {
			Ok(i)
		} else {
			trace!("create table {}", name);
			this.db.execute(
				&format!(
					"CREATE TABLE IF NOT EXISTS {} (
						k BLOB PRIMARY KEY,
						v BLOB
					)",
					name
				),
				[],
			)?;
			trace!("table created: {}, unlocking", name);

			let i = this.trees.len();
			this.trees.push(name.to_string());
			Ok(i)
		}
	}

	fn list_trees(&self) -> Result<Vec<String>> {
		let mut trees = vec![];

		trace!("list_trees: lock db");
		let this = self.0.lock().unwrap();
		trace!("list_trees: lock acquired");

		let mut stmt = this.db.prepare(
			"SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE 'tree_%'",
		)?;
		let mut rows = stmt.query([])?;
		while let Some(row) = rows.next()? {
			let name = row.get::<_, String>(0)?;
			let name = name.replace("_COLON_", ":");
			let name = name.strip_prefix("tree_").unwrap().to_string();
			trees.push(name);
		}
		Ok(trees)
	}

	// ----

	fn get(&self, tree: usize, key: &[u8]) -> Result<Option<Value>> {
		trace!("get {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("get {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;
		this.internal_get(tree, key)
	}

	fn len(&self, tree: usize) -> Result<usize> {
		trace!("len {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("len {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;
		let mut stmt = this.db.prepare(&format!("SELECT COUNT(*) FROM {}", tree))?;
		let mut res_iter = stmt.query([])?;
		match res_iter.next()? {
			None => Ok(0),
			Some(v) => Ok(v.get::<_, usize>(0)?),
		}
	}

	fn insert(&self, tree: usize, key: &[u8], value: &[u8]) -> Result<Option<Value>> {
		trace!("insert {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("insert {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;
		let old_val = this.internal_get(tree, key)?;

		let sql = match &old_val {
			Some(_) => format!("UPDATE {} SET v = ?2 WHERE k = ?1", tree),
			None => format!("INSERT INTO {} (k, v) VALUES (?1, ?2)", tree),
		};
		let n = this.db.execute(&sql, params![key, value])?;
		assert_eq!(n, 1);

		Ok(old_val)
	}

	fn remove(&self, tree: usize, key: &[u8]) -> Result<Option<Value>> {
		trace!("remove {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("remove {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;
		let old_val = this.internal_get(tree, key)?;

		if old_val.is_some() {
			let n = this
				.db
				.execute(&format!("DELETE FROM {} WHERE k = ?1", tree), params![key])?;
			assert_eq!(n, 1);
		}

		Ok(old_val)
	}

	fn clear(&self, tree: usize) -> Result<()> {
		trace!("clear {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("clear {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;
		this.db.execute(&format!("DELETE FROM {}", tree), [])?;
		Ok(())
	}

	fn iter(&self, tree: usize) -> Result<ValueIter<'_>> {
		trace!("iter {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("iter {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k ASC", tree);
		make_iterator(this, &sql, [])
	}

	fn iter_rev(&self, tree: usize) -> Result<ValueIter<'_>> {
		trace!("iter_rev {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("iter_rev {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k DESC", tree);
		make_iterator(this, &sql, [])
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		trace!("range {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("range {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k ASC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();

		make_iterator::<&[&dyn rusqlite::ToSql]>(this, &sql, params.as_ref())
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> Result<ValueIter<'_>> {
		trace!("range_rev {}: lock db", tree);
		let this = self.0.lock().unwrap();
		trace!("range_rev {}: lock acquired", tree);

		let tree = this.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k DESC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();

		make_iterator::<&[&dyn rusqlite::ToSql]>(this, &sql, params.as_ref())
	}

	// ----

	fn transaction(&self, f: &dyn ITxFn) -> TxResult<OnCommit, ()> {
		trace!("transaction: lock db");
		let mut this = self.0.lock().unwrap();
		trace!("transaction: lock acquired");

		let this_mut_ref: &mut SqliteDbInner = this.borrow_mut();

		let mut tx = SqliteTx {
			tx: this_mut_ref
				.db
				.transaction()
				.map_err(Error::from)
				.map_err(TxError::Db)?,
			trees: &this_mut_ref.trees,
		};
		let res = match f.try_on(&mut tx) {
			TxFnResult::Ok(on_commit) => {
				tx.tx.commit().map_err(Error::from).map_err(TxError::Db)?;
				Ok(on_commit)
			}
			TxFnResult::Abort => {
				tx.tx.rollback().map_err(Error::from).map_err(TxError::Db)?;
				Err(TxError::Abort(()))
			}
			TxFnResult::DbErr => {
				tx.tx.rollback().map_err(Error::from).map_err(TxError::Db)?;
				Err(TxError::Db(Error(
					"(this message will be discarded)".into(),
				)))
			}
		};

		trace!("transaction done");
		res
	}
}

// ----

struct SqliteTx<'a> {
	tx: Transaction<'a>,
	trees: &'a [String],
}

impl<'a> SqliteTx<'a> {
	fn get_tree(&self, i: usize) -> TxOpResult<&'_ str> {
		self.trees.get(i).map(String::as_ref).ok_or_else(|| {
			TxOpError(Error(
				"invalid tree id (it might have been openned after the transaction started)".into(),
			))
		})
	}

	fn internal_get(&self, tree: &str, key: &[u8]) -> TxOpResult<Option<Value>> {
		let mut stmt = self
			.tx
			.prepare(&format!("SELECT v FROM {} WHERE k = ?1", tree))?;
		let mut res_iter = stmt.query([key])?;
		match res_iter.next()? {
			None => Ok(None),
			Some(v) => Ok(Some(v.get::<_, Vec<u8>>(0)?)),
		}
	}
}

impl<'a> ITx for SqliteTx<'a> {
	fn get(&self, tree: usize, key: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		self.internal_get(tree, key)
	}
	fn len(&self, tree: usize) -> TxOpResult<usize> {
		let tree = self.get_tree(tree)?;
		let mut stmt = self.tx.prepare(&format!("SELECT COUNT(*) FROM {}", tree))?;
		let mut res_iter = stmt.query([])?;
		match res_iter.next()? {
			None => Ok(0),
			Some(v) => Ok(v.get::<_, usize>(0)?),
		}
	}

	fn insert(&mut self, tree: usize, key: &[u8], value: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let old_val = self.internal_get(tree, key)?;

		let sql = match &old_val {
			Some(_) => format!("UPDATE {} SET v = ?2 WHERE k = ?1", tree),
			None => format!("INSERT INTO {} (k, v) VALUES (?1, ?2)", tree),
		};
		let n = self.tx.execute(&sql, params![key, value])?;
		assert_eq!(n, 1);

		Ok(old_val)
	}
	fn remove(&mut self, tree: usize, key: &[u8]) -> TxOpResult<Option<Value>> {
		let tree = self.get_tree(tree)?;
		let old_val = self.internal_get(tree, key)?;

		if old_val.is_some() {
			let n = self
				.tx
				.execute(&format!("DELETE FROM {} WHERE k = ?1", tree), params![key])?;
			assert_eq!(n, 1);
		}

		Ok(old_val)
	}
	fn clear(&mut self, tree: usize) -> TxOpResult<()> {
		let tree = self.get_tree(tree)?;
		self.tx.execute(&format!("DELETE FROM {}", tree), [])?;
		Ok(())
	}

	fn iter(&self, tree: usize) -> TxOpResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k ASC", tree);
		make_tx_iterator(self, &sql, [])
	}
	fn iter_rev(&self, tree: usize) -> TxOpResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree)?;
		let sql = format!("SELECT k, v FROM {} ORDER BY k DESC", tree);
		make_tx_iterator(self, &sql, [])
	}

	fn range<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> TxOpResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k ASC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();

		make_tx_iterator::<&[&dyn rusqlite::ToSql]>(self, &sql, params.as_ref())
	}
	fn range_rev<'r>(
		&self,
		tree: usize,
		low: Bound<&'r [u8]>,
		high: Bound<&'r [u8]>,
	) -> TxOpResult<TxValueIter<'_>> {
		let tree = self.get_tree(tree)?;

		let (bounds_sql, params) = bounds_sql(low, high);
		let sql = format!("SELECT k, v FROM {} {} ORDER BY k DESC", tree, bounds_sql);

		let params = params
			.iter()
			.map(|x| x as &dyn rusqlite::ToSql)
			.collect::<Vec<_>>();

		make_tx_iterator::<&[&dyn rusqlite::ToSql]>(self, &sql, params.as_ref())
	}
}

// ---- iterators outside transactions ----
// complicated, they must hold the Statement and Row objects
// so we need a self_referencing struct

// need to split in two because sequential mutable borrows are broken,
// see https://github.com/someguynamedjosh/ouroboros/issues/100
#[self_referencing]
struct DbValueIterator1<'a> {
	db: MutexGuard<'a, SqliteDbInner>,
	#[borrows(mut db)]
	#[covariant]
	stmt: Statement<'this>,
}

#[self_referencing]
struct DbValueIterator<'a> {
	aux: DbValueIterator1<'a>,
	#[borrows(mut aux)]
	#[covariant]
	iter: Rows<'this>,
}

fn make_iterator<'a, P: rusqlite::Params>(
	db: MutexGuard<'a, SqliteDbInner>,
	sql: &str,
	args: P,
) -> Result<ValueIter<'a>> {
	let aux = DbValueIterator1::try_new(db, |db| db.db.prepare(sql))?;
	let res = DbValueIterator::try_new(aux, |aux| aux.with_stmt_mut(|stmt| stmt.query(args)))?;
	Ok(Box::new(res))
}

impl<'a> Iterator for DbValueIterator<'a> {
	type Item = Result<(Value, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		let next = self.with_iter_mut(|iter| iter.next());
		iter_next_row(next)
	}
}

// ---- iterators within transactions ----
// it's the same except we don't hold a mutex guard,
// only a Statement and a Rows object

#[self_referencing]
struct TxValueIterator<'a> {
	stmt: Statement<'a>,
	#[borrows(mut stmt)]
	#[covariant]
	iter: Rows<'this>,
}

fn make_tx_iterator<'a, P: rusqlite::Params>(
	tx: &'a SqliteTx<'a>,
	sql: &str,
	args: P,
) -> TxOpResult<TxValueIter<'a>> {
	let stmt = tx.tx.prepare(sql)?;
	let res = TxValueIterator::try_new(stmt, |stmt| stmt.query(args))?;
	Ok(Box::new(res))
}

impl<'a> Iterator for TxValueIterator<'a> {
	type Item = TxOpResult<(Value, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		let next = self.with_iter_mut(|iter| iter.next());
		iter_next_row(next)
	}
}

// ---- utility ----

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

fn iter_next_row<E>(
	next_row: rusqlite::Result<Option<&rusqlite::Row>>,
) -> Option<std::result::Result<(Value, Value), E>>
where
	E: From<rusqlite::Error>,
{
	let row = match next_row {
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
	Some(Ok((k, v)))
}
