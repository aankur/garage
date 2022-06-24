use core::ops::Bound;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::fs;
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::tranquilizer::Tranquilizer;

use crate::manager::*;

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
					self.block_iter = Some(BlockStoreIterator::new(&self.manager).await?);
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
	iterator: BlockStoreIterator,
	tranquilizer: Tranquilizer,
	tranquility: u32,
}

impl ScrubWorker {
	pub async fn new(manager: Arc<BlockManager>, tranquility: u32) -> Result<Self, Error> {
		let iterator = BlockStoreIterator::new(&manager).await?;
		Ok(Self {
			manager,
			iterator,
			tranquilizer: Tranquilizer::new(30),
			tranquility,
		})
	}
}

#[async_trait]
impl Worker for ScrubWorker {
	fn name(&self) -> String {
		"Block scrub worker".into()
	}

	async fn work(
		&mut self,
		_must_exit: &mut watch::Receiver<bool>,
	) -> Result<WorkerStatus, Error> {
		self.tranquilizer.reset();
		if let Some(hash) = self.iterator.next().await? {
			let _ = self.manager.read_block(&hash).await;
			Ok(self.tranquilizer.tranquilize_worker(self.tranquility))
		} else {
			Ok(WorkerStatus::Done)
		}
	}

	async fn wait_for_work(&mut self, _must_exit: &watch::Receiver<bool>) -> WorkerStatus {
		unreachable!()
	}
}

// ----

struct BlockStoreIterator {
	path: Vec<fs::ReadDir>,
}

impl BlockStoreIterator {
	async fn new(manager: &BlockManager) -> Result<Self, Error> {
		let root_dir = manager.data_dir.clone();
		let read_root_dir = fs::read_dir(&root_dir).await?;
		Ok(Self {
			path: vec![read_root_dir],
		})
	}

	async fn next(&mut self) -> Result<Option<Hash>, Error> {
		loop {
			if let Some(reader) = self.path.last_mut() {
				if let Some(data_dir_ent) = reader.next_entry().await? {
					let name = data_dir_ent.file_name();
					let name = if let Ok(n) = name.into_string() {
						n
					} else {
						continue;
					};
					let ent_type = data_dir_ent.file_type().await?;

					let name = name.strip_suffix(".zst").unwrap_or(&name);
					if name.len() == 2 && hex::decode(&name).is_ok() && ent_type.is_dir() {
						let read_child_dir = fs::read_dir(&data_dir_ent.path()).await?;
						self.path.push(read_child_dir);
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
				} else {
					self.path.pop();
					continue;
				}
			} else {
				return Ok(None);
			}
		}
	}
}
