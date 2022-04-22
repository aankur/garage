use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_util::data::*;

use garage_table::util::*;

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

#[derive(Deserialize)]
struct InsertBatchItem {
	pk: String,
	sk: String,
	ct: Option<String>,
	v: Option<String>,
}
