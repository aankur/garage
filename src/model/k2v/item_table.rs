use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use garage_util::data::*;

use garage_table::crdt::*;
use garage_table::*;

use crate::k2v::causality::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct K2VItem {
	pub bucket_id: Uuid,
	pub partition_key: String,
	pub sort_key: String,

	items: BTreeMap<K2VNodeId, DvvsEntry>,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
struct DvvsEntry {
	t_discard: u64,
	values: Vec<(u64, DvvsValue)>,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum DvvsValue {
	Value(#[serde(with = "serde_bytes")] Vec<u8>),
	Deleted,
}

impl K2VItem {
	/// Creates a new K2VItem when no previous entry existed in the db
	pub fn new(this_node: Uuid, value: DvvsValue) -> Self {
		unimplemented!(); // TODO
	}
	/// Updates a K2VItem with a new value or a deletion event
	pub fn update(&mut self, this_node: Uuid, context: CausalityContext, new_value: DvvsValue) {
		unimplemented!(); // TODO
	}

	/// Extract the causality context of a K2V Item
	pub fn causality_context(&self) -> CausalityContext {
		unimplemented!(); // TODO
	}

	/// Extract the list of values
	pub fn values(&'_ self) -> Vec<&'_ DvvsValue> {
		unimplemented!(); // TODO
	}
}

impl Crdt for K2VItem {
	fn merge(&mut self, other: &Self) {
		unimplemented!(); // TODO
	}
}

impl Crdt for DvvsEntry {
	fn merge(&mut self, other: &Self) {
		unimplemented!(); // TODO
	}
}
