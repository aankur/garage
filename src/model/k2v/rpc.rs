//! Module that implements RPCs specific to K2V.
//! This is necessary for insertions into the K2V store,
//! as they have to be transmitted to one of the nodes responsible
//! for storing the entry to be processed (the API entry
//! node does not process the entry directly, as this would
//! mean the vector clock gets much larger than needed).

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::select;

use garage_db as db;

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;

use garage_rpc::system::System;
use garage_rpc::*;

use garage_table::replication::{TableReplication, TableShardedReplication};
use garage_table::{PartitionKey, Table};

use crate::k2v::causality::*;
use crate::k2v::history_table::*;
use crate::k2v::item_table::*;
use crate::k2v::poll::*;

/// RPC messages for K2V
#[derive(Debug, Serialize, Deserialize)]
enum K2VRpc {
	Ok,
	InsertItem(InsertedItem),
	InsertManyItems(Vec<InsertedItem>),
	PollItem {
		key: PollKey,
		causal_context: CausalContext,
		timeout_msec: u64,
	},
	PollItemResponse(Option<K2VItem>),
}

#[derive(Debug, Serialize, Deserialize)]
struct InsertedItem {
	partition: K2VItemPartition,
	sort_key: String,
	causal_context: Option<CausalContext>,
	value: DvvsValue,
}

impl Rpc for K2VRpc {
	type Response = Result<K2VRpc, Error>;
}

/// The block manager, handling block exchange between nodes, and block storage on local node
pub struct K2VRpcHandler {
	system: Arc<System>,
	item_table: Arc<Table<K2VItemTable, TableShardedReplication>>,
	history_table: Arc<Table<K2VHistoryTable, TableShardedReplication>>,
	local_counter_tree: db::Tree,
	endpoint: Arc<Endpoint<K2VRpc, Self>>,
	subscriptions: Arc<SubscriptionManager>,
}

impl K2VRpcHandler {
	pub fn new(
		system: Arc<System>,
		db: &db::Db,
		item_table: Arc<Table<K2VItemTable, TableShardedReplication>>,
		history_table: Arc<Table<K2VHistoryTable, TableShardedReplication>>,
		subscriptions: Arc<SubscriptionManager>,
	) -> Arc<Self> {
		let local_counter_tree = db
			.open_tree("k2v_local_counter")
			.expect("Unable to open DB tree for k2v local counter");
		let endpoint = system.netapp.endpoint("garage_model/k2v/Rpc".to_string());

		let rpc_handler = Arc::new(Self {
			system,
			item_table,
			history_table,
			local_counter_tree,
			endpoint,
			subscriptions,
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
				K2VRpc::InsertItem(InsertedItem {
					partition,
					sort_key,
					causal_context,
					value,
				}),
				RequestStrategy::with_priority(PRIO_NORMAL)
					.with_quorum(1)
					.interrupt_after_quorum(true),
			)
			.await?;

		Ok(())
	}

	pub async fn insert_batch(
		&self,
		bucket_id: Uuid,
		items: Vec<(String, String, Option<CausalContext>, DvvsValue)>,
	) -> Result<(), Error> {
		let n_items = items.len();

		let mut call_list: HashMap<_, Vec<_>> = HashMap::new();

		for (partition_key, sort_key, causal_context, value) in items {
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

			call_list.entry(who).or_default().push(InsertedItem {
				partition,
				sort_key,
				causal_context,
				value,
			});
		}

		debug!(
			"K2V insert_batch: {} requests to insert {} items",
			call_list.len(),
			n_items
		);
		let call_futures = call_list.into_iter().map(|(nodes, items)| async move {
			let resp = self
				.system
				.rpc
				.try_call_many(
					&self.endpoint,
					&nodes[..],
					K2VRpc::InsertManyItems(items),
					RequestStrategy::with_priority(PRIO_NORMAL)
						.with_quorum(1)
						.interrupt_after_quorum(true),
				)
				.await?;
			Ok::<_, Error>((nodes, resp))
		});

		let mut resps = call_futures.collect::<FuturesUnordered<_>>();
		while let Some(resp) = resps.next().await {
			resp?;
		}

		Ok(())
	}

	pub async fn poll_item(
		&self,
		bucket_id: Uuid,
		partition_key: String,
		sort_key: String,
		causal_context: CausalContext,
		timeout_msec: u64,
	) -> Result<Option<K2VItem>, Error> {
		let poll_key = PollKey {
			partition: K2VItemPartition {
				bucket_id,
				partition_key,
			},
			sort_key,
		};
		let nodes = self
			.item_table
			.data
			.replication
			.write_nodes(&poll_key.partition.hash());

		let rpc = self.system.rpc.try_call_many(
			&self.endpoint,
			&nodes[..],
			K2VRpc::PollItem {
				key: poll_key,
				causal_context,
				timeout_msec,
			},
			RequestStrategy::with_priority(PRIO_NORMAL)
				.with_quorum(self.item_table.data.replication.read_quorum())
				.without_timeout(),
		);
		let timeout_duration = Duration::from_millis(timeout_msec) + self.system.rpc.rpc_timeout();
		let resps = select! {
			r = rpc => r?,
			_ = tokio::time::sleep(timeout_duration) => return Ok(None),
		};

		let mut resp: Option<K2VItem> = None;
		for v in resps {
			match v {
				K2VRpc::PollItemResponse(Some(x)) => {
					if let Some(y) = &mut resp {
						y.merge(&x);
					} else {
						resp = Some(x);
					}
				}
				K2VRpc::PollItemResponse(None) => {
					return Ok(None);
				}
				v => return Err(Error::unexpected_rpc_message(v)),
			}
		}

		Ok(resp)
	}

	// ---- internal handlers ----

	async fn handle_insert(&self, item: &InsertedItem) -> Result<K2VRpc, Error> {
		let new = self.local_insert(item)?;

		// Propagate to rest of network
		if let Some(updated) = new {
			self.item_table.insert(&updated).await?;
		}

		Ok(K2VRpc::Ok)
	}

	async fn handle_insert_many(&self, items: &[InsertedItem]) -> Result<K2VRpc, Error> {
		let mut updated_vec = vec![];

		for item in items {
			let new = self.local_insert(item)?;

			if let Some(updated) = new {
				updated_vec.push(updated);
			}
		}

		// Propagate to rest of network
		if !updated_vec.is_empty() {
			self.item_table.insert_many(&updated_vec).await?;
		}

		Ok(K2VRpc::Ok)
	}

	fn local_insert(&self, item: &InsertedItem) -> Result<Option<K2VItem>, Error> {
		let now = now_msec();

		self.item_table
			.data
			.update_entry_with(&item.partition, &item.sort_key, |tx, ent| {
				let old_local_counter = tx
					.get(&self.local_counter_tree, b"counter")?
					.and_then(|x| x.try_into().ok())
					.map(u64::from_be_bytes)
					.unwrap_or_default();

				let mut ent = ent.unwrap_or_else(|| {
					K2VItem::new(
						item.partition.bucket_id,
						item.partition.partition_key.clone(),
						item.sort_key.clone(),
					)
				});
				let new_local_counter = ent.update(
					self.system.id,
					&item.causal_context,
					item.value.clone(),
					old_local_counter,
				);

				tx.insert(
					&self.local_counter_tree,
					b"counter",
					u64::to_be_bytes(new_local_counter),
				)?;

				let hist_entry = K2VHistoryEntry {
					partition: ent.partition.clone(),
					node_counter: K2VHistorySortKey {
						node: make_node_id(self.system.id),
						counter: new_local_counter,
					},
					prev_counter: old_local_counter,
					timestamp: now,
					ins_sort_key: item.sort_key.clone(),
					ins_value: item.value.clone(),
					deleted: false.into(),
				};
				self.history_table.queue_insert(tx, &hist_entry)?;

				Ok(ent)
			})
	}

	async fn handle_poll_item(&self, key: &PollKey, ct: &CausalContext) -> Result<K2VItem, Error> {
		let mut chan = self.subscriptions.subscribe_item(key);

		let mut value = self
			.item_table
			.data
			.read_entry(&key.partition, &key.sort_key)?
			.map(|bytes| self.item_table.data.decode_entry(&bytes[..]))
			.transpose()?
			.unwrap_or_else(|| {
				K2VItem::new(
					key.partition.bucket_id,
					key.partition.partition_key.clone(),
					key.sort_key.clone(),
				)
			});

		while !value.causal_context().is_newer_than(ct) {
			value = chan.recv().await?;
		}

		Ok(value)
	}
}

#[async_trait]
impl EndpointHandler<K2VRpc> for K2VRpcHandler {
	async fn handle(self: &Arc<Self>, message: &K2VRpc, _from: NodeID) -> Result<K2VRpc, Error> {
		match message {
			K2VRpc::InsertItem(item) => self.handle_insert(item).await,
			K2VRpc::InsertManyItems(items) => self.handle_insert_many(&items[..]).await,
			K2VRpc::PollItem {
				key,
				causal_context,
				timeout_msec,
			} => {
				let delay = tokio::time::sleep(Duration::from_millis(*timeout_msec));
				select! {
					ret = self.handle_poll_item(key, causal_context) => ret.map(Some).map(K2VRpc::PollItemResponse),
					_ = delay => Ok(K2VRpc::PollItemResponse(None)),
				}
			}
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}
