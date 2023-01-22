use opentelemetry::{global, metrics::*};

/// TableMetrics reference all counter used for metrics
pub struct SystemMetrics {
	pub(crate) _garage_build_info: ObservableGauge<u64>,
	pub(crate) _replication_factor: ObservableGauge<u64>,
	pub(crate) _disk_avail: ObservableGauge<u64>,
	pub(crate) _disk_total: ObservableGauge<u64>,
}

impl SystemMetrics {
	pub fn new() -> Self {
		let meter = global::meter("garage_system");

		Self {
			_garage_build_info: meter
				.u64_observable_gauge("garage_build_info")
				.with_description("Garage build info")
				.init(),
			_replication_factor: meter
				.u64_observable_gauge("garage_replication_factor")
				.with_description("Garage replication factor setting")
				.init(),
			_disk_avail: meter
				.u64_observable_gauge("garage_local_disk_avail")
				.with_description("Garage available disk space on each node")
				.init(),
			_disk_total: meter
				.u64_observable_gauge("garage_local_disk_total")
				.with_description("Garage total disk space on each node")
				.init(),
		}
	}
}
