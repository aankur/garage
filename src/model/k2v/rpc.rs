//! Module that implements RPCs specific to K2V.
//! This is necessary for insertions into the K2V store,
//! as they have to be transmitted to one of the nodes responsible
//! for storing the entry to be processed (the API entry
//! node does not process the entry directly, as this would
//! mean the vector clock gets much larger than needed).

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use garage_util::data::*;
use garage_util::error::*;

use garage_rpc::system::System;
use garage_rpc::*;

use garage_table::replication::{TableReplication, TableShardedReplication};
use garage_table::table::TABLE_RPC_TIMEOUT;
use garage_table::{PartitionKey, Table};

use crate::k2v::causality::*;
use crate::k2v::item_table::*;

/// RPC messages for K2V
#[derive(Debug, Serialize, Deserialize)]
pub enum K2VRpc {
	Ok,
	InsertItem {
		partition: K2VItemPartition,
		sort_key: String,
		causal_context: Option<CausalContext>,
		value: DvvsValue,
	},
}

impl Rpc for K2VRpc {
	type Response = Result<K2VRpc, Error>;
}

/// The block manager, handling block exchange between nodes, and block storage on local node
pub struct K2VRpcHandler {
	system: Arc<System>,
	item_table: Arc<Table<K2VItemTable, TableShardedReplication>>,
	endpoint: Arc<Endpoint<K2VRpc, Self>>,
}

impl K2VRpcHandler {
	pub fn new(
		system: Arc<System>,
		item_table: Arc<Table<K2VItemTable, TableShardedReplication>>,
	) -> Arc<Self> {
		let endpoint = system.netapp.endpoint("garage_model/k2v/Rpc".to_string());

		let rpc_handler = Arc::new(Self {
			system,
			item_table,
			endpoint,
		});
		rpc_handler.endpoint.set_handler(rpc_handler.clone());

		rpc_handler
	}

	// ---- public interface ----

	pub async fn insert(
		&self,
		bucket_id: Uuid,
		partition_key: String,
		sort_key: String,
		causal_context: Option<CausalContext>,
		value: DvvsValue,
	) -> Result<(), Error> {
		let partition = K2VItemPartition {
			bucket_id,
			partition_key,
		};
		let mut who = self
			.item_table
			.data
			.replication
			.write_nodes(&partition.hash());
		who.sort();

		self.system
			.rpc
			.try_call_many(
				&self.endpoint,
				&who[..],
				K2VRpc::InsertItem {
					partition,
					sort_key,
					causal_context,
					value,
				},
				RequestStrategy::with_priority(PRIO_NORMAL)
					.with_quorum(1)
					.with_timeout(TABLE_RPC_TIMEOUT),
			)
			.await?;

		Ok(())
	}

	// ---- internal handlers ----

	#[allow(clippy::ptr_arg)]
	async fn handle_insert(
		&self,
		partition: &K2VItemPartition,
		sort_key: &String,
		causal_context: &Option<CausalContext>,
		value: &DvvsValue,
	) -> Result<K2VRpc, Error> {
		let tree_key = self.item_table.data.tree_key(partition, sort_key);
		let new = self
			.item_table
			.data
			.update_entry_with(&tree_key[..], |ent| {
				let mut ent = ent.unwrap_or_else(|| {
					K2VItem::new(
						partition.bucket_id,
						partition.partition_key.clone(),
						sort_key.clone(),
					)
				});
				ent.update(self.system.id, causal_context, value.clone());
				ent
			})?;

		// Propagate to rest of network
		if let Some(updated) = new {
			self.item_table.insert(&updated).await?;
		}

		Ok(K2VRpc::Ok)
	}
}

#[async_trait]
impl EndpointHandler<K2VRpc> for K2VRpcHandler {
	async fn handle(self: &Arc<Self>, message: &K2VRpc, _from: NodeID) -> Result<K2VRpc, Error> {
		match message {
			K2VRpc::InsertItem {
				partition,
				sort_key,
				causal_context,
				value,
			} => {
				self.handle_insert(partition, sort_key, causal_context, value)
					.await
			}
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}
