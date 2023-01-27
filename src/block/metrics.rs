use opentelemetry::{global, metrics::*};

use garage_db as db;
use garage_db::counted_tree_hack::CountedTree;

/// TableMetrics reference all counter used for metrics
pub struct BlockManagerMetrics {
	pub(crate) compression_level: ObservableGauge<u64>,
	pub(crate) _rc_size: ObservableGauge<u64>,
	pub(crate) _resync_queue_len: ObservableGauge<u64>,
	pub(crate) _resync_errored_blocks: ObservableGauge<u64>,

	pub(crate) resync_counter: Counter<u64>,
	pub(crate) resync_error_counter: Counter<u64>,
	pub(crate) resync_duration: Histogram<f64>,
	pub(crate) resync_send_counter: Counter<u64>,
	pub(crate) resync_recv_counter: Counter<u64>,

	pub(crate) bytes_read: Counter<u64>,
	pub(crate) block_read_duration: Histogram<f64>,
	pub(crate) bytes_written: Counter<u64>,
	pub(crate) block_write_duration: Histogram<f64>,
	pub(crate) delete_counter: Counter<u64>,

	pub(crate) corruption_counter: Counter<u64>,
}

impl BlockManagerMetrics {
	pub fn new(rc_tree: db::Tree, resync_queue: CountedTree, resync_errors: CountedTree) -> Self {
		let meter = global::meter("garage_model/block");
		Self {
			compression_level: meter
				.u64_observable_gauge("block.compression_level")
				.with_description("Garage compression level for node")
				.init(),
			_rc_size: meter
				.u64_observable_gauge("block.rc_size")
				.with_description("Number of blocks known to the reference counter")
				.init(),
			_resync_queue_len: meter
				.u64_observable_gauge("block.resync_queue_length")
				.with_description(
					"Number of block hashes queued for local check and possible resync",
				)
				.init(),
			_resync_errored_blocks: meter
				.u64_observable_gauge("block.resync_errored_blocks")
				.with_description("Number of block hashes whose last resync resulted in an error")
				.init(),

			resync_counter: meter
				.u64_counter("block.resync_counter")
				.with_description("Number of calls to resync_block")
				.init(),
			resync_error_counter: meter
				.u64_counter("block.resync_error_counter")
				.with_description("Number of calls to resync_block that returned an error")
				.init(),
			resync_duration: meter
				.f64_histogram("block.resync_duration")
				.with_description("Duration of resync_block operations")
				.init(),
			resync_send_counter: meter
				.u64_counter("block.resync_send_counter")
				.with_description("Number of blocks sent to another node in resync operations")
				.init(),
			resync_recv_counter: meter
				.u64_counter("block.resync_recv_counter")
				.with_description("Number of blocks received from other nodes in resync operations")
				.init(),

			bytes_read: meter
				.u64_counter("block.bytes_read")
				.with_description("Number of bytes read from disk")
				.init(),
			block_read_duration: meter
				.f64_histogram("block.read_duration")
				.with_description("Duration of block read operations")
				.init(),
			bytes_written: meter
				.u64_counter("block.bytes_written")
				.with_description("Number of bytes written to disk")
				.init(),
			block_write_duration: meter
				.f64_histogram("block.write_duration")
				.with_description("Duration of block write operations")
				.init(),
			delete_counter: meter
				.u64_counter("block.delete_counter")
				.with_description("Number of blocks deleted")
				.init(),

			corruption_counter: meter
				.u64_counter("block.corruption_counter")
				.with_description("Data corruptions detected on block reads")
				.init(),
		}
	}
}
