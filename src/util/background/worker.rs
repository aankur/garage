use std::time::{Duration, Instant};

use tracing::*;
use async_trait::async_trait;
use futures::future::*;
use tokio::select;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::{mpsc, watch};

use crate::error::Error;

#[derive(PartialEq, Copy, Clone)]
pub enum WorkerStatus {
	Busy,
	Idle,
	Done,
}

#[async_trait]
pub trait Worker: Send {
	fn name(&self) -> String;
	async fn work(&mut self, must_exit: &mut watch::Receiver<bool>) -> Result<WorkerStatus, Error>;
	async fn wait_for_work(&mut self, must_exit: &mut watch::Receiver<bool>) -> WorkerStatus;
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
						workers.push(async move {
							let mut worker = WorkerHandler {
								task_id,
								stop_signal,
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
				if worker.status == WorkerStatus::Busy
					|| (worker.status == WorkerStatus::Idle && Instant::now() < drain_half_time)
				{
					workers.push(async move {
						worker.step().await;
						worker
					}.boxed());
				} else {
					info!("Worker {} (TID {}) exited", worker.worker.name(), worker.task_id);
				}
			}
		};

		select! {
			_ = drain_everything => {
				info!("All workers exited in time \\o/");
			}
			_ = tokio::time::sleep(Duration::from_secs(9)) => {
				warn!("Some workers could not exit in time, we are cancelling some things in the middle.");
			}
		}
	}
}

// TODO add tranquilizer
struct WorkerHandler {
	task_id: usize,
	stop_signal: watch::Receiver<bool>,
	worker: Box<dyn Worker>,
	status: WorkerStatus,
}

impl WorkerHandler {
	async fn step(&mut self) { 
		match self.status {
			WorkerStatus::Busy => {
				match self.worker.work(&mut self.stop_signal).await {
					Ok(s) => {
						self.status = s;
					}
					Err(e) => {
						error!("Error in worker {}: {}", self.worker.name(), e);
					}
				}
			}
			WorkerStatus::Idle => {
				self.status = self.worker.wait_for_work(&mut self.stop_signal).await;
			}
			WorkerStatus::Done => unreachable!()
		}
	}
}
