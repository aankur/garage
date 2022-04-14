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
use garage_table::Table;

use crate::k2v::causality::*;
use crate::k2v::item_table::*;

/// RPC messages for K2V
#[derive(Debug, Serialize, Deserialize)]
pub enum K2VRpc {
	Ok,
	InsertItem {
		bucket_id: Uuid,
		partition_key: String,
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

	async fn handle_insert(
		&self,
		bucket_id: Uuid,
		partition_key: &str,
		sort_key: &String,
		causal_context: &Option<CausalContext>,
		value: &DvvsValue,
	) -> Result<K2VRpc, Error> {
		unimplemented!() //TODO
	}
}

#[async_trait]
impl EndpointHandler<K2VRpc> for K2VRpcHandler {
	async fn handle(self: &Arc<Self>, message: &K2VRpc, _from: NodeID) -> Result<K2VRpc, Error> {
		match message {
			K2VRpc::InsertItem {
				bucket_id,
				partition_key,
				sort_key,
				causal_context,
				value,
			} => {
				self.handle_insert(*bucket_id, partition_key, sort_key, causal_context, value)
					.await
			}
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}
