use opentelemetry::{global, metrics::*};

use garage_db as db;
use garage_db::counted_tree_hack::CountedTree;

/// TableMetrics reference all counter used for metrics
pub struct TableMetrics {
	pub(crate) _table_size: ObservableGauge<u64>,
	pub(crate) _merkle_tree_size: ObservableGauge<u64>,
	pub(crate) _merkle_todo_len: ObservableGauge<u64>,
	pub(crate) _gc_todo_len: ObservableGauge<u64>,

	pub(crate) get_request_counter: Counter<u64>,
	pub(crate) get_request_duration: Histogram<f64>,
	pub(crate) put_request_counter: Counter<u64>,
	pub(crate) put_request_duration: Histogram<f64>,

	pub(crate) internal_update_counter: Counter<u64>,
	pub(crate) internal_delete_counter: Counter<u64>,

	pub(crate) sync_items_sent: Counter<u64>,
	pub(crate) sync_items_received: Counter<u64>,
}
impl TableMetrics {
	pub fn new(
		table_name: &'static str,
		store: db::Tree,
		merkle_tree: db::Tree,
		merkle_todo: db::Tree,
		gc_todo: CountedTree,
	) -> Self {
		let meter = global::meter(table_name);
		TableMetrics {
			_table_size: meter
				.u64_observable_gauge("table.size")
				.with_description("Number of items in table")
				.init(),
			_merkle_tree_size: meter
				.u64_observable_gauge("table.merkle_tree_size")
				.with_description("Number of nodes in table's Merkle tree")
				.init(),
			_merkle_todo_len: meter
				.u64_observable_gauge("table.merkle_updater_todo_queue_length")
				.with_description("Merkle tree updater TODO queue length")
				.init(),
			_gc_todo_len: meter
				.u64_observable_gauge("table.gc_todo_queue_length")
				.with_description("Table garbage collector TODO queue length")
				.init(),

			get_request_counter: meter
				.u64_counter("table.get_request_counter")
				.with_description("Number of get/get_range requests internally made on this table")
				.init(),
			get_request_duration: meter
				.f64_histogram("table.get_request_duration")
				.with_description("Duration of get/get_range requests internally made on this table, in seconds")
				.init(),
			put_request_counter: meter
				.u64_counter("table.put_request_counter")
				.with_description("Number of insert/insert_many requests internally made on this table")
				.init(),
			put_request_duration: meter
				.f64_histogram("table.put_request_duration")
				.with_description("Duration of insert/insert_many requests internally made on this table, in seconds")
				.init(),

			internal_update_counter: meter
				.u64_counter("table.internal_update_counter")
				.with_description("Number of value updates where the value actually changes (includes creation of new key and update of existing key)")
				.init(),
			internal_delete_counter: meter
				.u64_counter("table.internal_delete_counter")
				.with_description("Number of value deletions in the tree (due to GC or repartitioning)")
				.init(),

			sync_items_sent: meter
				.u64_counter("table.sync_items_sent")
				.with_description("Number of data items sent to other nodes during resync procedures")
				.init(),
			sync_items_received: meter
				.u64_counter("table.sync_items_received")
				.with_description("Number of data items received from other nodes during resync procedures")
				.init(),
		}
	}
}
