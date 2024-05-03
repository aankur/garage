use garage_util::crdt::AutoCrdt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConsistencyMode {
	/// Read- and Write-quorum are 1
	Dangerous,
	/// Read-quorum is 1
	Degraded,
	/// Read- and Write-quorum are determined for read-after-write-consistency
	#[default]
	Consistent,
}

impl ConsistencyMode {
	pub fn parse(s: &str) -> Option<Self> {
		serde_json::from_value(serde_json::Value::String(s.to_string())).ok()
	}
}

impl AutoCrdt for ConsistencyMode {
	const WARN_IF_DIFFERENT: bool = true;
}
