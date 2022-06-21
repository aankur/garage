//! Job runner for futures and async functions

pub mod job_worker;
pub mod worker;

use core::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{mpsc, watch, Mutex};

use crate::error::Error;
use worker::{Worker, WorkerProcessor};

pub(crate) type JobOutput = Result<(), Error>;
pub(crate) type Job = Pin<Box<dyn Future<Output = JobOutput> + Send>>;

/// Job runner for futures and async functions
pub struct BackgroundRunner {
	send_job: mpsc::UnboundedSender<(Job, bool)>,
	send_worker: mpsc::UnboundedSender<Box<dyn Worker>>,
}

impl BackgroundRunner {
	/// Create a new BackgroundRunner
	pub fn new(
		n_runners: usize,
		stop_signal: watch::Receiver<bool>,
	) -> (Arc<Self>, tokio::task::JoinHandle<()>) {
		let (send_worker, worker_out) = mpsc::unbounded_channel::<Box<dyn Worker>>();

		let await_all_done =
			tokio::spawn(
				async move { WorkerProcessor::new(worker_out, stop_signal).run().await },
			);

		let (send_job, queue_out) = mpsc::unbounded_channel();
		let queue_out = Arc::new(Mutex::new(queue_out));

		for i in 0..n_runners {
			let queue_out = queue_out.clone();

			send_worker.send(Box::new(job_worker::JobWorker {
				index: i,
				job_chan: queue_out.clone(),
				next_job: None,
			})).ok().unwrap();
		}

		let bgrunner = Arc::new(Self {
			send_job,
			send_worker,
		});
		(bgrunner, await_all_done)
	}

	/// Spawn a task to be run in background
	pub fn spawn<T>(&self, job: T)
	where
		T: Future<Output = JobOutput> + Send + 'static,
	{
		let boxed: Job = Box::pin(job);
		self.send_job
			.send((boxed, false))
			.ok()
			.expect("Could not put job in queue");
	}

	/// Spawn a task to be run in background. It may get discarded before running if spawned while
	/// the runner is stopping
	pub fn spawn_cancellable<T>(&self, job: T)
	where
		T: Future<Output = JobOutput> + Send + 'static,
	{
		let boxed: Job = Box::pin(job);
		self.send_job
			.send((boxed, true))
			.ok()
			.expect("Could not put job in queue");
	}

	pub fn spawn_worker<W>(&self, worker: W)
	where
		W: Worker + 'static,
	{
		self.send_worker
			.send(Box::new(worker))
			.ok()
			.expect("Could not put worker in queue");
	}
}
