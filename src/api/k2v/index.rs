use std::sync::Arc;

use hyper::{Body, Response, StatusCode};
use serde::Serialize;

use garage_util::data::*;
use garage_util::error::Error as GarageError;

use garage_rpc::ring::Ring;

use garage_model::garage::Garage;

use crate::error::*;
use crate::k2v::range::read_range;

pub async fn handle_read_index(
	garage: Arc<Garage>,
	bucket_id: Uuid,
	prefix: Option<String>,
	start: Option<String>,
	end: Option<String>,
	limit: Option<u64>,
) -> Result<Response<Body>, Error> {
	let ring: Arc<Ring> = garage.system.ring.borrow().clone();

	let (partition_keys, more, next_start) = read_range(
		&garage.k2v_counter_table.table,
		&bucket_id,
		&prefix,
		&start,
		&end,
		limit,
		None,
	)
	.await?;

	let s_entries = "entries".to_string();
	let s_values = "values".to_string();
	let s_bytes = "bytes".to_string();

	let resp = ReadIndexResponse {
		prefix,
		start,
		end,
		limit,
		partition_keys: partition_keys
			.into_iter()
			.map(|part| {
				let vals = part.filtered_values(&ring);
				ReadIndexResponseEntry {
					pk: part.sk,
					entries: *vals.get(&s_entries).unwrap_or(&0),
					values: *vals.get(&s_values).unwrap_or(&0),
					bytes: *vals.get(&s_bytes).unwrap_or(&0),
				}
			})
			.collect::<Vec<_>>(),
		more,
		next_start,
	};

	let resp_json = serde_json::to_string_pretty(&resp).map_err(GarageError::from)?;
	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::from(resp_json))?)
}

#[derive(Serialize)]
struct ReadIndexResponse {
	prefix: Option<String>,
	start: Option<String>,
	end: Option<String>,
	limit: Option<u64>,

	#[serde(rename = "partitionKeys")]
	partition_keys: Vec<ReadIndexResponseEntry>,

	more: bool,
	#[serde(rename = "nextStart")]
	next_start: Option<String>,
}

#[derive(Serialize)]
struct ReadIndexResponseEntry {
	pk: String,
	entries: i64,
	values: i64,
	bytes: i64,
}
