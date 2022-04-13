use std::collections::BTreeMap;

use garage_util::data::*;

/// Node IDs used in K2V are u64 integers that are the abbreviation
/// of full Garage node IDs which are 256-bit UUIDs.
pub type K2VNodeId = u64;

pub fn make_node_id(node_id: Uuid) -> K2VNodeId {
	let mut tmp = [0u8; 8];
	tmp.copy_from_slice(&node_id.as_slice()[..8]);
	u64::from_be_bytes(tmp)
}


pub struct CausalityContext {
	pub vector_clock: BTreeMap<K2VNodeId, u64>,
}

impl CausalityContext {
	/// Empty causality context
	pub fn new_empty() -> Self {
		Self {
			vector_clock: BTreeMap::new(),
		}
	}
	/// Make binary representation and encode in base64
	pub fn serialize(&self) -> String {
		unimplemented!(); //TODO
	}
	/// Parse from base64-encoded binary representation
	pub fn parse(s: &str) -> Self {
		unimplemented!(); //TODO
	}
}
