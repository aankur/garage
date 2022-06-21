use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::*;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::select;
use tokio::sync::{mpsc, watch};
use tracing::*;

use crate::error::Error;

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum WorkerStatus {
	Busy,
	Idle,
	Done,
}

#[async_trait]
pub trait Worker: Send {
	fn name(&self) -> String;

	/// Work: do a basic unit of work, if one is available (otherwise, should return
	/// WorkerStatus::Idle immediately).  We will do our best to not interrupt this future in the
	/// middle of processing, it will only be interrupted at the last minute when Garage is trying
	/// to exit and this hasn't returned yet. This function may return an error to indicate that
	/// its unit of work could not be processed due to an error: the error will be logged and
	/// .work() will be called again immediately.
	async fn work(&mut self, must_exit: &mut watch::Receiver<bool>) -> Result<WorkerStatus, Error>;

	/// Wait for work: await for some task to become available.  This future can be interrupted in
	/// the middle for any reason.  This future doesn't have to await on must_exit.changed(), we
	/// are doing it for you.  Therefore it only receives a read refernce to must_exit which allows
	/// it to check if we are exiting.
	async fn wait_for_work(&mut self, must_exit: &watch::Receiver<bool>) -> WorkerStatus;
}

pub(crate) struct WorkerProcessor {
	stop_signal: watch::Receiver<bool>,
	worker_chan: mpsc::UnboundedReceiver<Box<dyn Worker>>,
}

impl WorkerProcessor {
	pub(crate) fn new(
		worker_chan: mpsc::UnboundedReceiver<Box<dyn Worker>>,
		stop_signal: watch::Receiver<bool>,
	) -> Self {
		Self {
			stop_signal,
			worker_chan,
		}
	}

	pub(crate) async fn run(&mut self) {
		let mut workers = FuturesUnordered::new();
		let mut next_task_id = 1;

		while !*self.stop_signal.borrow() {
			let await_next_worker = async {
				if workers.is_empty() {
					futures::future::pending().await
				} else {
					workers.next().await
				}
			};
			select! {
				new_worker_opt = self.worker_chan.recv() => {
					if let Some(new_worker) = new_worker_opt {
						let task_id = next_task_id;
						next_task_id += 1;
						let stop_signal = self.stop_signal.clone();
						let stop_signal_worker = self.stop_signal.clone();
						workers.push(async move {
							let mut worker = WorkerHandler {
								task_id,
								stop_signal,
								stop_signal_worker,
								worker: new_worker,
								status: WorkerStatus::Busy,
							};
							worker.step().await;
							worker
						}.boxed());
					}
				}
				worker = await_next_worker => {
					if let Some(mut worker) = worker {
						// TODO save new worker status somewhere
						if worker.status == WorkerStatus::Done {
							info!("Worker {} (TID {}) exited", worker.worker.name(), worker.task_id);
						} else {
							workers.push(async move {
								worker.step().await;
								worker
							}.boxed());
						}
					}
				}
				_ = self.stop_signal.changed() => (),
			}
		}

		// We are exiting, drain everything
		let drain_half_time = Instant::now() + Duration::from_secs(5);
		let drain_everything = async move {
			while let Some(mut worker) = workers.next().await {
				if worker.status == WorkerStatus::Done {
					info!(
						"Worker {} (TID {}) exited",
						worker.worker.name(),
						worker.task_id
					);
				} else if Instant::now() > drain_half_time {
					warn!("Worker {} (TID {}) interrupted between two iterations in state {:?} (this should be fine)", worker.worker.name(), worker.task_id, worker.status);
				} else {
					workers.push(
						async move {
							worker.step().await;
							worker
						}
						.boxed(),
					);
				}
			}
		};

		select! {
			_ = drain_everything => {
				info!("All workers exited in time \\o/");
			}
			_ = tokio::time::sleep(Duration::from_secs(9)) => {
				error!("Some workers could not exit in time, we are cancelling some things in the middle");
			}
		}
	}
}

// TODO add tranquilizer
struct WorkerHandler {
	task_id: usize,
	stop_signal: watch::Receiver<bool>,
	stop_signal_worker: watch::Receiver<bool>,
	worker: Box<dyn Worker>,
	status: WorkerStatus,
}

impl WorkerHandler {
	async fn step(&mut self) {
		match self.status {
			WorkerStatus::Busy => match self.worker.work(&mut self.stop_signal).await {
				Ok(s) => {
					self.status = s;
				}
				Err(e) => {
					error!(
						"Error in worker {} (TID {}): {}",
						self.worker.name(),
						self.task_id,
						e
					);
					// Sleep a bit so that error won't repeat immediately
					// (TODO good way to handle errors)
					tokio::time::sleep(Duration::from_secs(10)).await;
				}
			},
			WorkerStatus::Idle => {
				if *self.stop_signal.borrow() {
					select! {
						new_st = self.worker.wait_for_work(&mut self.stop_signal_worker) => {
							self.status = new_st;
						}
						_ = tokio::time::sleep(Duration::from_secs(1)) => {
							// stay in Idle state
						}
					}
				} else {
					select! {
						new_st = self.worker.wait_for_work(&mut self.stop_signal_worker) => {
							self.status = new_st;
						}
						_ = self.stop_signal.changed() => {
							// stay in Idle state
						}
					}
				}
			}
			WorkerStatus::Done => unreachable!(),
		}
	}
}
