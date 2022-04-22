use garage_util::data::*;

use crate::index_counter::*;

pub const ENTRIES: &'static str = "entries";
pub const CONFLICTS: &'static str = "conflicts";
pub const VALUES: &'static str = "values";
pub const BYTES: &'static str = "bytes";

#[derive(PartialEq, Clone)]
pub struct K2VCounterTable;

impl CounterSchema for K2VCounterTable {
	const NAME: &'static str = "k2v_index_counter";

	// Partition key = bucket id
	type P = Uuid;
	// Sort key = K2V item's partition key
	type S = String;
}
