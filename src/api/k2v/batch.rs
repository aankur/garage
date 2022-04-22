use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_util::data::*;
use garage_util::error::Error as GarageError;

use garage_model::garage::Garage;
use garage_model::k2v::causality::*;
use garage_model::k2v::item_table::*;

use crate::error::*;
use crate::k2v::range::read_range;

pub async fn handle_insert_batch(
	garage: Arc<Garage>,
	bucket_id: Uuid,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let body = hyper::body::to_bytes(req.into_body()).await?;
	let items: Vec<InsertBatchItem> =
		serde_json::from_slice(&body).ok_or_bad_request("Invalid JSON")?;

	let mut items2 = vec![];
	for it in items {
		let ct = it
			.ct
			.map(|s| CausalContext::parse(&s))
			.transpose()
			.ok_or_bad_request("Invalid causality token")?;
		let v = match it.v {
			Some(vs) => {
				DvvsValue::Value(base64::decode(vs).ok_or_bad_request("Invalid base64 value")?)
			}
			None => DvvsValue::Deleted,
		};
		items2.push((it.pk, it.sk, ct, v));
	}

	garage.k2v_rpc.insert_batch(bucket_id, items2).await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::empty())?)
}

pub async fn handle_read_batch(
	garage: Arc<Garage>,
	bucket_id: Uuid,
	req: Request<Body>,
) -> Result<Response<Body>, Error> {
	let body = hyper::body::to_bytes(req.into_body()).await?;
	let queries: Vec<ReadBatchQuery> =
		serde_json::from_slice(&body).ok_or_bad_request("Invalid JSON")?;

	let resp_results = futures::future::join_all(
		queries.into_iter()
		.map(|q| handle_read_batch_query(&garage, bucket_id, q)))
		.await;

	let mut resps: Vec<ReadBatchResponse> = vec![];
	for resp in resp_results {
		resps.push(resp?);
	}

	let resp_json = serde_json::to_string_pretty(&resps).map_err(GarageError::from)?;
	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::from(resp_json))?)
}

async fn handle_read_batch_query(
	garage: &Arc<Garage>,
	bucket_id: Uuid,
	query: ReadBatchQuery,
) -> Result<ReadBatchResponse, Error> {
	let partition = K2VItemPartition{
		bucket_id,
		partition_key: query.partition_key.clone(),
	};

	let filter = ItemFilter {
		exclude_only_tombstones: !query.tombstones,
		conflicts_only: query.conflicts_only,
	};

	let (items, more, next_start) = if query.single_item {
		let sk = query.start.as_ref() 
			.ok_or_bad_request("start should be specified if single_item is set")?;
		let item = garage
			.k2v_item_table
			.get(&partition, sk)
			.await?;
		match item {
			Some(i) => (vec![ReadBatchResponseItem::from(i)],
				false, None),
			None => (vec![], false, None),
		}
	} else {
		let (items, more, next_start) = read_range(
			&garage.k2v_item_table,
			&partition,
			&query.prefix,
			&query.start,
			&query.end,
			query.limit,
			Some(filter)
			).await?;

		let items = items.into_iter()
			.map(|i| ReadBatchResponseItem::from(i))
			.collect::<Vec<_>>();

		(items, more, next_start)
	};

	Ok(ReadBatchResponse {
		partition_key: query.partition_key,
		prefix: query.prefix,
		start: query.start,
		end: query.end,
		limit: query.limit,
		single_item: query.single_item,
		conflicts_only: query.conflicts_only,
		tombstones: query.tombstones,
		items,
		more,
		next_start,
	})
}

#[derive(Deserialize)]
struct InsertBatchItem {
	pk: String,
	sk: String,
	ct: Option<String>,
	v: Option<String>,
}

#[derive(Deserialize)]
struct ReadBatchQuery {
	#[serde(rename="partitionKey")]
	partition_key: String,
	#[serde(default)]
	prefix: Option<String>,
	#[serde(default)]
	start: Option<String>,
	#[serde(default)]
	end: Option<String>,
	#[serde(default)]
	limit: Option<u64>,
	#[serde(default,rename="singleItem")]
	single_item: bool,
	#[serde(default,rename="conflictsOnly")]
	conflicts_only: bool,
	#[serde(default)]
	tombstones: bool,
}

#[derive(Serialize)]
struct ReadBatchResponse {
	#[serde(rename="partitionKey")]
	partition_key: String,
	prefix: Option<String>,
	start: Option<String>,
	end: Option<String>,
	limit: Option<u64>,
	#[serde(rename="singleItem")]
	single_item: bool,
	#[serde(rename="conflictsOnly")]
	conflicts_only: bool,
	tombstones: bool,

	items: Vec<ReadBatchResponseItem>,
	more: bool,
	#[serde(rename="nextStart")]
	next_start: Option<String>,
}

#[derive(Serialize)]
struct ReadBatchResponseItem {
	sk: String,
	ct: String,
	v: Vec<Option<String>>,
}

impl ReadBatchResponseItem {
	fn from(i: K2VItem) -> Self {
		let ct = i.causality_context().serialize();
		let v = i.values()
					.iter()
					.map(|v| match v {
						DvvsValue::Value(x) => Some(base64::encode(x)),
						DvvsValue::Deleted => None,
					})
					.collect::<Vec<_>>();
		Self {
				sk: i.sort_key,
				ct,
				v,
			}
	}
}
