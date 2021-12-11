use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};

use crate::error::*;
use crate::signature::verify_signed_content;
use garage_model::bucket_table::BucketState;
use garage_model::garage::Garage;
use garage_table::*;
use garage_util::data::Hash;

pub async fn handle_delete_website(
	garage: Arc<Garage>,
	bucket: String,
) -> Result<Response<Body>, Error> {
	let mut bucket = garage
		.bucket_table
		.get(&EmptyKey, &bucket)
		.await?
		.ok_or(Error::NotFound)?;

	if let BucketState::Present(state) = bucket.state.get_mut() {
		state.website.update(false);
		garage.bucket_table.insert(&bucket).await?;
	}

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(Body::from(vec![]))
		.unwrap())
}

pub async fn handle_put_website(
	garage: Arc<Garage>,
	bucket: String,
	req: Request<Body>,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	let body = hyper::body::to_bytes(req.into_body()).await?;
	verify_signed_content(content_sha256, &body[..])?;

	let mut bucket = garage
		.bucket_table
		.get(&EmptyKey, &bucket)
		.await?
		.ok_or(Error::NotFound)?;

	// TODO: parse xml

	if let BucketState::Present(state) = bucket.state.get_mut() {
		state.website.update(true);
		garage.bucket_table.insert(&bucket).await?;
	}

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::from(vec![]))
		.unwrap())
}
