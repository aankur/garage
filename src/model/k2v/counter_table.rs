use garage_util::data::*;

use crate::index_counter::*;

#[derive(PartialEq, Clone)]
pub struct K2VCounterTable;

impl CounterSchema for K2VCounterTable {
	const NAME: &'static str = "k2v_index_counter";

	// Partition key = bucket id
	type P = Uuid;
	// Sort key = K2V item's partition key
	type S = String;
}
