//! Utility module for retrieving ranges of items in Garage tables
//! Implements parameters (prefix, start, end, limit) as specified
//! for endpoints ReadIndex, ReadBatch and DeleteBatch

use std::sync::Arc;

use garage_table::replication::TableShardedReplication;
use garage_table::*;

use crate::error::*;

/// Read range in a Garage table.
/// Returns (entries, more?, nextStart)
pub(crate) async fn read_range<F>(
	table: &Arc<Table<F, TableShardedReplication>>,
	partition_key: &F::P,
	prefix: &Option<String>,
	start: &Option<String>,
	end: &Option<String>,
	limit: Option<u64>,
	filter: Option<F::Filter>,
) -> Result<(Vec<F::E>, bool, Option<String>), Error>
where
	F: TableSchema<S = String> + 'static,
{
	let mut start = match (prefix, start) {
		(None, None) => "".to_string(),
		(Some(p), None) => p.clone(),
		(None, Some(s)) => s.clone(),
		(Some(p), Some(s)) => {
			if !s.starts_with(p) {
				return Err(Error::BadRequest(format!(
					"Start key '{}' does not start with prefix '{}'",
					s, p
				)));
			}
			s.clone()
		}
	};
	let mut start_ignore = false;

	let mut entries = vec![];
	loop {
		let n_get = std::cmp::min(1000, limit.unwrap_or(u64::MAX) as usize - entries.len() + 2);
		let get_ret = table
			.get_range(partition_key, Some(start.clone()), filter.clone(), n_get)
			.await?;

		let get_ret_len = get_ret.len();

		for entry in get_ret {
			if let Some(p) = prefix {
				if !entry.sort_key().starts_with(p) {
					return Ok((entries, false, None));
				}
			}
			if let Some(e) = end {
				if entry.sort_key() == e {
					return Ok((entries, false, None));
				}
			}
			if let Some(l) = limit {
				if entries.len() >= l as usize {
					return Ok((entries, true, Some(entry.sort_key().clone())));
				}
			}
			if start_ignore && entry.sort_key() == &start {
				continue;
			}
			entries.push(entry);
		}

		if get_ret_len < n_get {
			return Ok((entries, false, None));
		}

		start = entries.last().unwrap().sort_key().clone();
		start_ignore = true;
	}
}
