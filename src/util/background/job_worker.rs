//! Job worker: a generic worker that just processes incoming
//! jobs one by one

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};

use crate::background::worker::*;
use crate::background::*;

pub(crate) struct JobWorker {
	pub(crate) index: usize,
	pub(crate) job_chan: Arc<Mutex<mpsc::UnboundedReceiver<(Job, bool)>>>,
	pub(crate) next_job: Option<Job>,
}

#[async_trait]
impl Worker for JobWorker {
	fn name(&self) -> String {
		format!("Job worker #{}", self.index)
	}

	async fn work(
		&mut self,
		_must_exit: &mut watch::Receiver<bool>,
	) -> Result<WorkerStatus, Error> {
		match self.next_job.take() {
			None => return Ok(WorkerStatus::Idle),
			Some(job) => {
				job.await?;
				Ok(WorkerStatus::Busy)
			}
		}
	}

	async fn wait_for_work(&mut self, must_exit: &mut watch::Receiver<bool>) -> WorkerStatus {
		loop {
			match self.job_chan.lock().await.recv().await {
				Some((job, cancellable)) => {
					if cancellable && *must_exit.borrow() {
						// skip job
						continue;
					}
					self.next_job = Some(job);
					return WorkerStatus::Busy
				}
				None => return WorkerStatus::Done,
			}
		}
	}
}
