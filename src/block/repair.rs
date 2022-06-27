use core::ops::Bound;
use std::convert::TryInto;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::fs;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;
use garage_util::tranquilizer::Tranquilizer;

use crate::manager::*;

const SCRUB_INTERVAL: Duration = Duration::from_secs(3600 * 24 * 30); // full scrub every 30 days
const TIME_LAST_COMPLETE_SCRUB: &[u8] = b"time_last_complete_scrub";

pub struct RepairWorker {
	manager: Arc<BlockManager>,
	next_start: Option<Hash>,
	block_iter: Option<BlockStoreIterator>,
}

impl RepairWorker {
	pub fn new(manager: Arc<BlockManager>) -> Self {
		Self {
			manager,
			next_start: None,
			block_iter: None,
		}
	}
}

#[async_trait]
impl Worker for RepairWorker {
	fn name(&self) -> String {
		"Block repair worker".into()
	}

	fn info(&self) -> Option<String> {
		match self.block_iter.as_ref() {
			None => {
				let idx_bytes = self
					.next_start
					.as_ref()
					.map(|x| x.as_slice())
					.unwrap_or(&[]);
				let idx_bytes = if idx_bytes.len() > 4 {
					&idx_bytes[..4]
				} else {
					idx_bytes
				};
				Some(format!("Phase 1: {}", hex::encode(idx_bytes)))
			}
			Some(bi) => Some(format!("Phase 2: {:.2}% done", bi.progress() * 100.)),
		}
	}

	async fn work(
		&mut self,
		_must_exit: &mut watch::Receiver<bool>,
	) -> Result<WorkerStatus, Error> {
		match self.block_iter.as_mut() {
			None => {
				// Phase 1: Repair blocks from RC table.

				// We have to do this complicated two-step process where we first read a bunch
				// of hashes from the RC table, and then insert them in the to-resync queue,
				// because of SQLite. Basically, as long as we have an iterator on a DB table,
				// we can't do anything else on the DB. The naive approach (which we had previously)
				// of just iterating on the RC table and inserting items one to one in the resync
				// queue can't work here, it would just provoke a deadlock in the SQLite adapter code.
				// This is mostly because the Rust bindings for SQLite assume a worst-case scenario
				// where SQLite is not compiled in thread-safe mode, so we have to wrap everything
				// in a mutex (see db/sqlite_adapter.rs and discussion in PR #322).
				// TODO: maybe do this with tokio::task::spawn_blocking ?
				let mut batch_of_hashes = vec![];
				let start_bound = match self.next_start.as_ref() {
					None => Bound::Unbounded,
					Some(x) => Bound::Excluded(x.as_slice()),
				};
				for entry in self
					.manager
					.rc
					.rc
					.range::<&[u8], _>((start_bound, Bound::Unbounded))?
				{
					let (hash, _) = entry?;
					let hash = Hash::try_from(&hash[..]).unwrap();
					batch_of_hashes.push(hash);
					if batch_of_hashes.len() >= 1000 {
						break;
					}
				}
				if batch_of_hashes.is_empty() {
					// move on to phase 2
					self.block_iter = Some(BlockStoreIterator::new(&self.manager));
					return Ok(WorkerStatus::Busy);
				}

				for hash in batch_of_hashes.into_iter() {
					self.manager.put_to_resync(&hash, Duration::from_secs(0))?;
					self.next_start = Some(hash)
				}

				Ok(WorkerStatus::Busy)
			}
			Some(bi) => {
				// Phase 2: Repair blocks actually on disk
				// Lists all blocks on disk and adds them to the resync queue.
				// This allows us to find blocks we are storing but don't actually need,
				// so that we can offload them if necessary and then delete them locally.
				if let Some(hash) = bi.next().await? {
					self.manager.put_to_resync(&hash, Duration::from_secs(0))?;
					Ok(WorkerStatus::Busy)
				} else {
					Ok(WorkerStatus::Done)
				}
			}
		}
	}

	async fn wait_for_work(&mut self, _must_exit: &watch::Receiver<bool>) -> WorkerStatus {
		unreachable!()
	}
}

// ----

pub struct ScrubWorker {
	manager: Arc<BlockManager>,
	rx_cmd: mpsc::Receiver<ScrubWorkerCommand>,

	work: ScrubWorkerState,
	tranquilizer: Tranquilizer,
	tranquility: u32,

	time_last_complete_scrub: u64,
}

enum ScrubWorkerState {
	Running(BlockStoreIterator),
	Paused(BlockStoreIterator, u64), // u64 = time when to resume scrub
	Finished,
}

impl Default for ScrubWorkerState {
	fn default() -> Self {
		ScrubWorkerState::Finished
	}
}

pub enum ScrubWorkerCommand {
	Start,
	Pause(Duration),
	Resume,
	Cancel,
	SetTranquility(u32),
}

impl ScrubWorker {
	pub fn new(
		manager: Arc<BlockManager>,
		rx_cmd: mpsc::Receiver<ScrubWorkerCommand>,
		tranquility: u32,
	) -> Self {
		let time_last_complete_scrub = match manager
			.state_variables_store
			.get(TIME_LAST_COMPLETE_SCRUB)
			.expect("DB error when initializing scrub worker")
		{
			Some(v) => u64::from_be_bytes(v.try_into().unwrap()),
			None => 0,
		};
		Self {
			manager,
			rx_cmd,
			work: ScrubWorkerState::Finished,
			tranquilizer: Tranquilizer::new(30),
			tranquility,
			time_last_complete_scrub,
		}
	}

	fn handle_cmd(&mut self, cmd: ScrubWorkerCommand) {
		match cmd {
			ScrubWorkerCommand::Start => {
				self.work = match std::mem::take(&mut self.work) {
					ScrubWorkerState::Finished => {
						let iterator = BlockStoreIterator::new(&self.manager);
						ScrubWorkerState::Running(iterator)
					}
					work => {
						error!("Cannot start scrub worker: already running!");
						work
					}
				};
			}
			ScrubWorkerCommand::Pause(dur) => {
				self.work = match std::mem::take(&mut self.work) {
					ScrubWorkerState::Running(it) | ScrubWorkerState::Paused(it, _) => {
						ScrubWorkerState::Paused(it, now_msec() + dur.as_millis() as u64)
					}
					work => {
						error!("Cannot pause scrub worker: not running!");
						work
					}
				};
			}
			ScrubWorkerCommand::Resume => {
				self.work = match std::mem::take(&mut self.work) {
					ScrubWorkerState::Paused(it, _) => ScrubWorkerState::Running(it),
					work => {
						error!("Cannot resume scrub worker: not paused!");
						work
					}
				};
			}
			ScrubWorkerCommand::Cancel => {
				self.work = match std::mem::take(&mut self.work) {
					ScrubWorkerState::Running(_) | ScrubWorkerState::Paused(_, _) => {
						ScrubWorkerState::Finished
					}
					work => {
						error!("Cannot cancel scrub worker: not running!");
						work
					}
				}
			}
			ScrubWorkerCommand::SetTranquility(t) => {
				self.tranquility = t;
			}
		}
	}
}

#[async_trait]
impl Worker for ScrubWorker {
	fn name(&self) -> String {
		"Block scrub worker".into()
	}

	fn info(&self) -> Option<String> {
		match &self.work {
			ScrubWorkerState::Running(bsi) => Some(format!("{:.2}% done", bsi.progress() * 100.)),
			ScrubWorkerState::Paused(_bsi, rt) => {
				Some(format!("Paused, resumes at {}", msec_to_rfc3339(*rt)))
			}
			ScrubWorkerState::Finished => Some(format!(
				"Last completed scrub: {}",
				msec_to_rfc3339(self.time_last_complete_scrub)
			)),
		}
	}

	async fn work(
		&mut self,
		_must_exit: &mut watch::Receiver<bool>,
	) -> Result<WorkerStatus, Error> {
		match self.rx_cmd.try_recv() {
			Ok(cmd) => self.handle_cmd(cmd),
			Err(mpsc::error::TryRecvError::Disconnected) => return Ok(WorkerStatus::Done),
			Err(mpsc::error::TryRecvError::Empty) => (),
		};

		match &mut self.work {
			ScrubWorkerState::Running(bsi) => {
				self.tranquilizer.reset();
				if let Some(hash) = bsi.next().await? {
					let _ = self.manager.read_block(&hash).await;
					Ok(self.tranquilizer.tranquilize_worker(self.tranquility))
				} else {
					self.time_last_complete_scrub = now_msec(); // TODO save to file
					self.manager.state_variables_store.insert(
						TIME_LAST_COMPLETE_SCRUB,
						u64::to_be_bytes(self.time_last_complete_scrub),
					)?;
					self.work = ScrubWorkerState::Finished;
					self.tranquilizer.clear();
					Ok(WorkerStatus::Idle)
				}
			}
			_ => Ok(WorkerStatus::Idle),
		}
	}

	async fn wait_for_work(&mut self, _must_exit: &watch::Receiver<bool>) -> WorkerStatus {
		match &self.work {
			ScrubWorkerState::Running(_) => return WorkerStatus::Busy,
			ScrubWorkerState::Paused(_, resume_time) => {
				let delay = Duration::from_millis(resume_time - now_msec());
				select! {
					_ = tokio::time::sleep(delay) => self.handle_cmd(ScrubWorkerCommand::Resume),
					cmd = self.rx_cmd.recv() => if let Some(cmd) = cmd {
						self.handle_cmd(cmd);
					} else {
						return WorkerStatus::Done;
					}
				}
			}
			ScrubWorkerState::Finished => {
				let delay = SCRUB_INTERVAL
					- Duration::from_secs(now_msec() - self.time_last_complete_scrub);
				select! {
					_ = tokio::time::sleep(delay) => self.handle_cmd(ScrubWorkerCommand::Start),
					cmd = self.rx_cmd.recv() => if let Some(cmd) = cmd {
						self.handle_cmd(cmd);
					} else {
						return WorkerStatus::Done;
					}
				}
			}
		}
		match &self.work {
			ScrubWorkerState::Running(_) => WorkerStatus::Busy,
			_ => WorkerStatus::Idle,
		}
	}
}

// ----

struct BlockStoreIterator {
	path: Vec<ReadingDir>,
}

enum ReadingDir {
	Pending(PathBuf),
	Read {
		subpaths: Vec<fs::DirEntry>,
		pos: usize,
	},
}

impl BlockStoreIterator {
	fn new(manager: &BlockManager) -> Self {
		let root_dir = manager.data_dir.clone();
		Self {
			path: vec![ReadingDir::Pending(root_dir)],
		}
	}

	/// Returns progress done, between 0% and 1%
	fn progress(&self) -> f32 {
		if self.path.is_empty() {
			1.0
		} else {
			let mut ret = 0.0;
			let mut next_div = 1;
			for p in self.path.iter() {
				match p {
					ReadingDir::Pending(_) => break,
					ReadingDir::Read { subpaths, pos } => {
						next_div *= subpaths.len();
						ret += ((*pos - 1) as f32) / (next_div as f32);
					}
				}
			}
			ret
		}
	}

	async fn next(&mut self) -> Result<Option<Hash>, Error> {
		loop {
			let last_path = match self.path.last_mut() {
				None => return Ok(None),
				Some(lp) => lp,
			};

			if let ReadingDir::Pending(path) = last_path {
				let mut reader = fs::read_dir(&path).await?;
				let mut subpaths = vec![];
				while let Some(ent) = reader.next_entry().await? {
					subpaths.push(ent);
				}
				*last_path = ReadingDir::Read { subpaths, pos: 0 };
			}

			let (subpaths, pos) = match *last_path {
				ReadingDir::Read {
					ref subpaths,
					ref mut pos,
				} => (subpaths, pos),
				ReadingDir::Pending(_) => unreachable!(),
			};

			if *pos >= subpaths.len() {
				self.path.pop();
				continue;
			}

			let data_dir_ent = match subpaths.get(*pos) {
				None => {
					self.path.pop();
					continue;
				}
				Some(ent) => {
					*pos += 1;
					ent
				}
			};

			let name = data_dir_ent.file_name();
			let name = if let Ok(n) = name.into_string() {
				n
			} else {
				continue;
			};
			let ent_type = data_dir_ent.file_type().await?;

			let name = name.strip_suffix(".zst").unwrap_or(&name);
			if name.len() == 2 && hex::decode(&name).is_ok() && ent_type.is_dir() {
				let path = data_dir_ent.path();
				self.path.push(ReadingDir::Pending(path));
				continue;
			} else if name.len() == 64 {
				let hash_bytes = if let Ok(h) = hex::decode(&name) {
					h
				} else {
					continue;
				};
				let mut hash = [0u8; 32];
				hash.copy_from_slice(&hash_bytes[..]);
				return Ok(Some(hash.into()));
			}
		}
	}
}
