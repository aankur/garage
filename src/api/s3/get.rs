//! Function related to GET and HEAD requests
use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures::future;
use futures::stream::{self, StreamExt};
use http::header::{
	ACCEPT_RANGES, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE,
	CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, EXPIRES, IF_MODIFIED_SINCE, IF_NONE_MATCH,
	LAST_MODIFIED, RANGE,
};
use hyper::{body::Body, Request, Response, StatusCode};
use tokio::sync::mpsc;

use garage_net::stream::ByteStream;
use garage_rpc::replication_mode::ConsistencyMode;
use garage_rpc::rpc_helper::OrderTag;
use garage_table::EmptyKey;
use garage_util::data::*;
use garage_util::error::OkOrMessage;

use garage_model::bucket_table::BucketParams;
use garage_model::garage::Garage;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use crate::helpers::*;
use crate::s3::api_server::ResBody;
use crate::s3::error::*;

const X_AMZ_MP_PARTS_COUNT: &str = "x-amz-mp-parts-count";

#[derive(Default)]
pub struct GetObjectOverrides {
	pub(crate) response_cache_control: Option<String>,
	pub(crate) response_content_disposition: Option<String>,
	pub(crate) response_content_encoding: Option<String>,
	pub(crate) response_content_language: Option<String>,
	pub(crate) response_content_type: Option<String>,
	pub(crate) response_expires: Option<String>,
}

fn object_headers(
	version: &ObjectVersion,
	version_meta: &ObjectVersionMeta,
) -> http::response::Builder {
	debug!("Version meta: {:?}", version_meta);

	let date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
	let date_str = httpdate::fmt_http_date(date);

	let mut resp = Response::builder()
		.header(CONTENT_TYPE, version_meta.headers.content_type.to_string())
		.header(LAST_MODIFIED, date_str)
		.header(ACCEPT_RANGES, "bytes".to_string());

	if !version_meta.etag.is_empty() {
		resp = resp.header(ETAG, format!("\"{}\"", version_meta.etag));
	}

	for (k, v) in version_meta.headers.other.iter() {
		resp = resp.header(k, v.to_string());
	}

	resp
}

/// Override headers according to specific query parameters, see
/// section "Overriding response header values through the request" in
/// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
fn getobject_override_headers(
	overrides: GetObjectOverrides,
	resp: &mut http::response::Builder,
) -> Result<(), Error> {
	// TODO: this only applies for signed requests, so when we support
	// anonymous access in the future we will have to do a permission check here
	let overrides = [
		(CACHE_CONTROL, overrides.response_cache_control),
		(CONTENT_DISPOSITION, overrides.response_content_disposition),
		(CONTENT_ENCODING, overrides.response_content_encoding),
		(CONTENT_LANGUAGE, overrides.response_content_language),
		(CONTENT_TYPE, overrides.response_content_type),
		(EXPIRES, overrides.response_expires),
	];
	for (hdr, val_opt) in overrides {
		if let Some(val) = val_opt {
			let val = val.try_into().ok_or_bad_request("invalid header value")?;
			resp.headers_mut().unwrap().insert(hdr, val);
		}
	}
	Ok(())
}

fn try_answer_cached(
	version: &ObjectVersion,
	version_meta: &ObjectVersionMeta,
	req: &Request<impl Body>,
) -> Option<Response<ResBody>> {
	// <trinity> It is possible, and is even usually the case, [that both If-None-Match and
	// If-Modified-Since] are present in a request. In this situation If-None-Match takes
	// precedence and If-Modified-Since is ignored (as per 6.Precedence from rfc7232). The rational
	// being that etag based matching is more accurate, it has no issue with sub-second precision
	// for instance (in case of very fast updates)
	let cached = if let Some(none_match) = req.headers().get(IF_NONE_MATCH) {
		let none_match = none_match.to_str().ok()?;
		let expected = format!("\"{}\"", version_meta.etag);
		let found = none_match
			.split(',')
			.map(str::trim)
			.any(|etag| etag == expected || etag == "\"*\"");
		found
	} else if let Some(modified_since) = req.headers().get(IF_MODIFIED_SINCE) {
		let modified_since = modified_since.to_str().ok()?;
		let client_date = httpdate::parse_http_date(modified_since).ok()?;
		let server_date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
		client_date >= server_date
	} else {
		false
	};

	if cached {
		Some(
			Response::builder()
				.status(StatusCode::NOT_MODIFIED)
				.body(empty_body())
				.unwrap(),
		)
	} else {
		None
	}
}

/// Handle HEAD request
pub async fn handle_head(
	ctx: ReqCtx,
	req: &Request<impl Body>,
	key: &str,
	part_number: Option<u64>,
) -> Result<Response<ResBody>, Error> {
	handle_head_without_ctx(
		ctx.garage,
		req,
		ctx.bucket_id,
		&ctx.bucket_params,
		key,
		part_number,
	)
	.await
}

/// Handle HEAD request for website
pub async fn handle_head_without_ctx(
	garage: Arc<Garage>,
	req: &Request<impl Body>,
	bucket_id: Uuid,
	bucket_params: &BucketParams,
	key: &str,
	part_number: Option<u64>,
) -> Result<Response<ResBody>, Error> {
	let c = *bucket_params.consistency_mode.get();

	let object = garage
		.object_table
		.get(c, &bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	let object_version = object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_data())
		.ok_or(Error::NoSuchKey)?;

	let version_data = match &object_version.state {
		ObjectVersionState::Complete(c) => c,
		_ => unreachable!(),
	};

	let version_meta = match version_data {
		ObjectVersionData::Inline(meta, _) => meta,
		ObjectVersionData::FirstBlock(meta, _) => meta,
		_ => unreachable!(),
	};

	if let Some(cached) = try_answer_cached(object_version, version_meta, req) {
		return Ok(cached);
	}

	if let Some(pn) = part_number {
		match version_data {
			ObjectVersionData::Inline(_, bytes) => {
				if pn != 1 {
					return Err(Error::InvalidPart);
				}
				Ok(object_headers(object_version, version_meta)
					.header(CONTENT_LENGTH, format!("{}", bytes.len()))
					.header(
						CONTENT_RANGE,
						format!("bytes 0-{}/{}", bytes.len() - 1, bytes.len()),
					)
					.header(X_AMZ_MP_PARTS_COUNT, "1")
					.status(StatusCode::PARTIAL_CONTENT)
					.body(empty_body())?)
			}
			ObjectVersionData::FirstBlock(_, _) => {
				let version = garage
					.version_table
					.get(c, &object_version.uuid, &EmptyKey)
					.await?
					.ok_or(Error::NoSuchKey)?;

				let (part_offset, part_end) =
					calculate_part_bounds(&version, pn).ok_or(Error::InvalidPart)?;

				Ok(object_headers(object_version, version_meta)
					.header(CONTENT_LENGTH, format!("{}", part_end - part_offset))
					.header(
						CONTENT_RANGE,
						format!(
							"bytes {}-{}/{}",
							part_offset,
							part_end - 1,
							version_meta.size
						),
					)
					.header(X_AMZ_MP_PARTS_COUNT, format!("{}", version.n_parts()?))
					.status(StatusCode::PARTIAL_CONTENT)
					.body(empty_body())?)
			}
			_ => unreachable!(),
		}
	} else {
		Ok(object_headers(object_version, version_meta)
			.header(CONTENT_LENGTH, format!("{}", version_meta.size))
			.status(StatusCode::OK)
			.body(empty_body())?)
	}
}

/// Handle GET request
pub async fn handle_get(
	ctx: ReqCtx,
	req: &Request<impl Body>,
	key: &str,
	part_number: Option<u64>,
	overrides: GetObjectOverrides,
) -> Result<Response<ResBody>, Error> {
	handle_get_without_ctx(
		ctx.garage,
		req,
		ctx.bucket_id,
		&ctx.bucket_params,
		key,
		part_number,
		overrides,
	)
	.await
}

/// Handle GET request
pub async fn handle_get_without_ctx(
	garage: Arc<Garage>,
	req: &Request<impl Body>,
	bucket_id: Uuid,
	bucket_params: &BucketParams,
	key: &str,
	part_number: Option<u64>,
	overrides: GetObjectOverrides,
) -> Result<Response<ResBody>, Error> {
	let c = *bucket_params.consistency_mode.get();

	let object = garage
		.object_table
		.get(c, &bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	let last_v = object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_complete())
		.ok_or(Error::NoSuchKey)?;

	let last_v_data = match &last_v.state {
		ObjectVersionState::Complete(x) => x,
		_ => unreachable!(),
	};
	let last_v_meta = match last_v_data {
		ObjectVersionData::DeleteMarker => return Err(Error::NoSuchKey),
		ObjectVersionData::Inline(meta, _) => meta,
		ObjectVersionData::FirstBlock(meta, _) => meta,
	};

	if let Some(cached) = try_answer_cached(last_v, last_v_meta, req) {
		return Ok(cached);
	}

	match (part_number, parse_range_header(req, last_v_meta.size)?) {
		(Some(_), Some(_)) => Err(Error::bad_request(
			"Cannot specify both partNumber and Range header",
		)),
		(Some(pn), None) => handle_get_part(garage, c, last_v, last_v_data, last_v_meta, pn).await,
		(None, Some(range)) => {
			handle_get_range(
				garage,
				c,
				last_v,
				last_v_data,
				last_v_meta,
				range.start,
				range.start + range.length,
			)
			.await
		}
		(None, None) => {
			handle_get_full(garage, c, last_v, last_v_data, last_v_meta, overrides).await
		}
	}
}

async fn handle_get_full(
	garage: Arc<Garage>,
	c: ConsistencyMode,
	version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	overrides: GetObjectOverrides,
) -> Result<Response<ResBody>, Error> {
	let mut resp_builder = object_headers(version, version_meta)
		.header(CONTENT_LENGTH, format!("{}", version_meta.size))
		.status(StatusCode::OK);
	getobject_override_headers(overrides, &mut resp_builder)?;

	match &version_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_, bytes) => {
			Ok(resp_builder.body(bytes_body(bytes.to_vec().into()))?)
		}
		ObjectVersionData::FirstBlock(_, first_block_hash) => {
			let (tx, rx) = mpsc::channel::<ByteStream>(2);

			let order_stream = OrderTag::stream();
			let first_block_hash = *first_block_hash;
			let version_uuid = version.uuid;

			tokio::spawn(async move {
				match async {
					let garage2 = garage.clone();
					let version_fut = tokio::spawn(async move {
						garage2.version_table.get(c, &version_uuid, &EmptyKey).await
					});

					let stream_block_0 = garage
						.block_manager
						.rpc_get_block_streaming(&first_block_hash, Some(order_stream.order(0)))
						.await?;
					tx.send(stream_block_0)
						.await
						.ok_or_message("channel closed")?;

					let version = version_fut.await.unwrap()?.ok_or(Error::NoSuchKey)?;
					for (i, (_, vb)) in version.blocks.items().iter().enumerate().skip(1) {
						let stream_block_i = garage
							.block_manager
							.rpc_get_block_streaming(&vb.hash, Some(order_stream.order(i as u64)))
							.await?;
						tx.send(stream_block_i)
							.await
							.ok_or_message("channel closed")?;
					}

					Ok::<(), Error>(())
				}
				.await
				{
					Ok(()) => (),
					Err(e) => {
						let _ = tx.send(error_stream_item(e)).await;
					}
				}
			});

			let body = response_body_from_block_stream(rx);
			Ok(resp_builder.body(body)?)
		}
	}
}

async fn handle_get_range(
	garage: Arc<Garage>,
	c: ConsistencyMode,
	version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	begin: u64,
	end: u64,
) -> Result<Response<ResBody>, Error> {
	// Here we do not use getobject_override_headers because we don't
	// want to add any overridden headers (those should not be added
	// when returning PARTIAL_CONTENT)
	let resp_builder = object_headers(version, version_meta)
		.header(CONTENT_LENGTH, format!("{}", end - begin))
		.header(
			CONTENT_RANGE,
			format!("bytes {}-{}/{}", begin, end - 1, version_meta.size),
		)
		.status(StatusCode::PARTIAL_CONTENT);

	match &version_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_meta, bytes) => {
			if end as usize <= bytes.len() {
				let body = bytes_body(bytes[begin as usize..end as usize].to_vec().into());
				Ok(resp_builder.body(body)?)
			} else {
				Err(Error::internal_error(
					"Requested range not present in inline bytes when it should have been",
				))
			}
		}
		ObjectVersionData::FirstBlock(_meta, _first_block_hash) => {
			let version = garage
				.version_table
				.get(c, &version.uuid, &EmptyKey)
				.await?
				.ok_or(Error::NoSuchKey)?;

			let body = body_from_blocks_range(garage, version.blocks.items(), begin, end);
			Ok(resp_builder.body(body)?)
		}
	}
}

async fn handle_get_part(
	garage: Arc<Garage>,
	c: ConsistencyMode,
	object_version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	part_number: u64,
) -> Result<Response<ResBody>, Error> {
	// Same as for get_range, no getobject_override_headers
	let resp_builder =
		object_headers(object_version, version_meta).status(StatusCode::PARTIAL_CONTENT);

	match version_data {
		ObjectVersionData::Inline(_, bytes) => {
			if part_number != 1 {
				return Err(Error::InvalidPart);
			}
			Ok(resp_builder
				.header(CONTENT_LENGTH, format!("{}", bytes.len()))
				.header(
					CONTENT_RANGE,
					format!("bytes {}-{}/{}", 0, bytes.len() - 1, bytes.len()),
				)
				.header(X_AMZ_MP_PARTS_COUNT, "1")
				.body(bytes_body(bytes.to_vec().into()))?)
		}
		ObjectVersionData::FirstBlock(_, _) => {
			let version = garage
				.version_table
				.get(c, &object_version.uuid, &EmptyKey)
				.await?
				.ok_or(Error::NoSuchKey)?;

			let (begin, end) =
				calculate_part_bounds(&version, part_number).ok_or(Error::InvalidPart)?;

			let body = body_from_blocks_range(garage, version.blocks.items(), begin, end);

			Ok(resp_builder
				.header(CONTENT_LENGTH, format!("{}", end - begin))
				.header(
					CONTENT_RANGE,
					format!("bytes {}-{}/{}", begin, end - 1, version_meta.size),
				)
				.header(X_AMZ_MP_PARTS_COUNT, format!("{}", version.n_parts()?))
				.body(body)?)
		}
		_ => unreachable!(),
	}
}

fn parse_range_header(
	req: &Request<impl Body>,
	total_size: u64,
) -> Result<Option<http_range::HttpRange>, Error> {
	let range = match req.headers().get(RANGE) {
		Some(range) => {
			let range_str = range.to_str()?;
			let mut ranges =
				http_range::HttpRange::parse(range_str, total_size).map_err(|e| (e, total_size))?;
			if ranges.len() > 1 {
				// garage does not support multi-range requests yet, so we respond with the entire
				// object when multiple ranges are requested
				None
			} else {
				ranges.pop()
			}
		}
		None => None,
	};
	Ok(range)
}

fn calculate_part_bounds(v: &Version, part_number: u64) -> Option<(u64, u64)> {
	let mut offset = 0;
	for (i, (bk, bv)) in v.blocks.items().iter().enumerate() {
		if bk.part_number == part_number {
			let size: u64 = v.blocks.items()[i..]
				.iter()
				.take_while(|(k, _)| k.part_number == part_number)
				.map(|(_, v)| v.size)
				.sum();
			return Some((offset, offset + size));
		}
		offset += bv.size;
	}
	None
}

fn body_from_blocks_range(
	garage: Arc<Garage>,
	all_blocks: &[(VersionBlockKey, VersionBlock)],
	begin: u64,
	end: u64,
) -> ResBody {
	// We will store here the list of blocks that have an intersection with the requested
	// range, as well as their "true offset", which is their actual offset in the complete
	// file (whereas block.offset designates the offset of the block WITHIN THE PART
	// block.part_number, which is not the same in the case of a multipart upload)
	let mut blocks: Vec<(VersionBlock, u64)> = Vec::with_capacity(std::cmp::min(
		all_blocks.len(),
		4 + ((end - begin) / std::cmp::max(all_blocks[0].1.size, 1024)) as usize,
	));
	let mut block_offset: u64 = 0;
	for (_, b) in all_blocks.iter() {
		if block_offset >= end {
			break;
		}
		// Keep only blocks that have an intersection with the requested range
		if block_offset < end && block_offset + b.size > begin {
			blocks.push((*b, block_offset));
		}
		block_offset += b.size;
	}

	let order_stream = OrderTag::stream();
	let (tx, rx) = mpsc::channel::<ByteStream>(2);

	tokio::spawn(async move {
		match async {
			let garage = garage.clone();
			for (i, (block, block_offset)) in blocks.iter().enumerate() {
				let block_stream = garage
					.block_manager
					.rpc_get_block_streaming(&block.hash, Some(order_stream.order(i as u64)))
					.await?
					.scan(*block_offset, move |chunk_offset, chunk| {
						let r = match chunk {
							Ok(chunk_bytes) => {
								let chunk_len = chunk_bytes.len() as u64;
								let r = if *chunk_offset >= end {
									// The current chunk is after the part we want to read.
									// Returning None here will stop the scan, the rest of the
									// stream will be ignored
									None
								} else if *chunk_offset + chunk_len <= begin {
									// The current chunk is before the part we want to read.
									// We return a None that will be removed by the filter_map
									// below.
									Some(None)
								} else {
									// The chunk has an intersection with the requested range
									let start_in_chunk = if *chunk_offset > begin {
										0
									} else {
										begin - *chunk_offset
									};
									let end_in_chunk = if *chunk_offset + chunk_len < end {
										chunk_len
									} else {
										end - *chunk_offset
									};
									Some(Some(Ok(chunk_bytes
										.slice(start_in_chunk as usize..end_in_chunk as usize))))
								};
								*chunk_offset += chunk_bytes.len() as u64;
								r
							}
							Err(e) => Some(Some(Err(e))),
						};
						futures::future::ready(r)
					})
					.filter_map(futures::future::ready);

				let block_stream: ByteStream = Box::pin(block_stream);
				tx.send(Box::pin(block_stream))
					.await
					.ok_or_message("channel closed")?;
			}

			Ok::<(), Error>(())
		}
		.await
		{
			Ok(()) => (),
			Err(e) => {
				let _ = tx.send(error_stream_item(e)).await;
			}
		}
	});

	response_body_from_block_stream(rx)
}

fn response_body_from_block_stream(rx: mpsc::Receiver<ByteStream>) -> ResBody {
	let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx)
		.flatten()
		.map(|x| {
			x.map(hyper::body::Frame::data)
				.map_err(|e| Error::from(garage_util::error::Error::from(e)))
		});
	ResBody::new(http_body_util::StreamBody::new(body_stream))
}

fn error_stream_item<E: std::fmt::Display>(e: E) -> ByteStream {
	let err = std::io::Error::new(
		std::io::ErrorKind::Other,
		format!("Error while getting object data: {}", e),
	);
	Box::pin(stream::once(future::ready(Err(err))))
}
