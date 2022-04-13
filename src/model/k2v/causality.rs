use std::collections::BTreeMap;
use std::convert::TryInto;

use garage_util::data::*;
use garage_util::error::*;

/// Node IDs used in K2V are u64 integers that are the abbreviation
/// of full Garage node IDs which are 256-bit UUIDs.
pub type K2VNodeId = u64;

pub fn make_node_id(node_id: Uuid) -> K2VNodeId {
	let mut tmp = [0u8; 8];
	tmp.copy_from_slice(&node_id.as_slice()[..8]);
	u64::from_be_bytes(tmp)
}

#[derive(PartialEq, Debug)]
pub struct CausalContext {
	pub vector_clock: BTreeMap<K2VNodeId, u64>,
}

impl CausalContext {
	/// Empty causality context
	pub fn new_empty() -> Self {
		Self {
			vector_clock: BTreeMap::new(),
		}
	}
	/// Make binary representation and encode in base64
	pub fn serialize(&self) -> String {
		let mut ints = Vec::with_capacity(2 * self.vector_clock.len());
		for (node, time) in self.vector_clock.iter() {
			ints.push(*node);
			ints.push(*time);
		}
		let checksum = ints.iter().fold(0, |acc, v| acc ^ *v);

		let mut bytes = u64::to_be_bytes(checksum).to_vec();
		for i in ints {
			bytes.extend(u64::to_be_bytes(i));
		}

		base64::encode(bytes)
	}
	/// Parse from base64-encoded binary representation
	pub fn parse(s: &str) -> Result<Self, Error> {
		let bytes = base64::decode(s).ok_or_message("Invalid causality token (bad base64)")?;
		if bytes.len() % 16 != 8 || bytes.len() < 8 {
			return Err(Error::Message(
				"Invalid causality token (bad length)".into(),
			));
		}

		let checksum = u64::from_be_bytes(bytes[..8].try_into().unwrap());
		let mut ret = CausalContext {
			vector_clock: BTreeMap::new(),
		};

		for i in 0..(bytes.len() / 16) {
			let node_id = u64::from_be_bytes(bytes[8 + i * 16..16 + i * 16].try_into().unwrap());
			let time = u64::from_be_bytes(bytes[16 + i * 16..24 + i * 16].try_into().unwrap());
			ret.vector_clock.insert(node_id, time);
		}

		let check = ret.vector_clock.iter().fold(0, |acc, (n, t)| acc ^ *n ^ *t);

		if check != checksum {
			return Err(Error::Message(
				"Invalid causality token (bad checksum)".into(),
			));
		}

		Ok(ret)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_causality_token_serialization() {
		let ct = CausalContext {
			vector_clock: [(4, 42), (1928131023, 76), (0xefc0c1c47f9de433, 2)]
				.iter()
				.cloned()
				.collect(),
		};

		assert_eq!(CausalContext::parse(&ct.serialize()).unwrap(), ct);
	}
}
