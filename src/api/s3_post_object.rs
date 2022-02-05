use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use hyper::{header, Body, Request, Response, StatusCode};
use multer::{Constraints, Multipart, SizeLimit};
use serde::Deserialize;

use garage_model::garage::Garage;

use crate::api_server::resolve_bucket;
use crate::error::*;
use crate::s3_put::save_stream;
use crate::signature::payload::{parse_date, verify_v4};

pub async fn handle_post_object(
	garage: Arc<Garage>,
	req: Request<Body>,
	bucket: String,
) -> Result<Response<Body>, Error> {
	let boundary = req
		.headers()
		.get(header::CONTENT_TYPE)
		.and_then(|ct| ct.to_str().ok())
		.and_then(|ct| multer::parse_boundary(ct).ok())
		.ok_or_bad_request("Counld not get multipart boundary")?;

	// these limits are rather arbitrary
	let constraints = Constraints::new().size_limit(
		SizeLimit::new()
			.per_field(32 * 1024)
			.for_field("file", 5 * 1024 * 1024 * 1024),
	);

	let mut multipart = Multipart::with_constraints(req.into_body(), boundary, constraints);

	let mut headers = BTreeMap::new();
	let mut credential = None;
	let mut key = None;
	let mut policy = None;
	let mut signature = None;
	let mut date = None;
	let mut redirect = Err(204);
	while let Some(field) = multipart.next_field().await? {
		let name = if let Some(name) = field.name() {
			name.to_owned()
		} else {
			continue;
		};

		if name != "file" {
			let content = field.text().await?;
			// TODO wouldn't a header map be better?
			match name.to_ascii_lowercase().as_str() {
				// main fields
				"key" => {
					key = Some(content);
				}
				"policy" => {
					policy = Some(content);
				}
				"x-amz-credential" => {
					credential = Some(content);
				}
				"x-amz-signature" => {
					signature = Some(content);
				}
				"x-amz-date" => {
					date = Some(content);
				}
				// special handling
				"success_action_redirect" | "redirect" => {
					// TODO should verify it's a valid looking URI
					redirect = Ok(content);
				}
				"success_action_status" => {
					let code = name.parse::<u16>().unwrap_or(204);
					redirect = Err(code);
				}
				"tagging" => {
					// TODO Garage does not support tagging so this can be left empty. It's essentially
					// a header except it must be parsed from xml to x-www-form-urlencoded
					continue;
				}
				// headers to PutObject
				"cache-control" | "content-type" | "content-encoding" | "expires" => {
					headers.insert(name, content);
				}
				"acl" => {
					headers.insert("x-amz-acl".to_owned(), content);
				}
				_ if name.starts_with("x-amz-") => {
					headers.insert(name, content);
				}
				_ => {
					// TODO should we ignore, error or process?
				}
			}
			continue;
		}

		// Current part is file. Do some checks before handling to PutObject code
		let credential = credential.ok_or_else(|| {
			Error::Forbidden("Garage does not support anonymous access yet".to_string())
		})?;
		let policy = policy.ok_or_bad_request("No policy was provided")?;
		let signature = signature.ok_or_bad_request("No signature was provided")?;
		let date = date.ok_or_bad_request("No date was provided")?;
		let key = key.ok_or_bad_request("No key was provided")?;

		let key = if key.contains("${filename}") {
			let filename = field.file_name();
			// is this correct? Maybe we should error instead of default?
			key.replace("${filename}", filename.unwrap_or_default())
		} else {
			key
		};

		let date = parse_date(&date)?;
		let api_key = verify_v4(&garage, &credential, &date, &signature, policy.as_bytes()).await?;

		let bucket_id = resolve_bucket(&garage, &bucket, &api_key).await?;

		if !api_key.allow_write(&bucket_id) {
			return Err(Error::Forbidden(
				"Operation is not allowed for this key.".to_string(),
			));
		}

		let decoded_policy = base64::decode(&policy)?;
		let _decoded_policy: Policy = serde_json::from_slice(&decoded_policy).unwrap();
		// TODO validate policy against request
		// unsafe to merge until implemented

		let content_type = field
			.content_type()
			.map(ToString::to_string)
			.unwrap_or_else(|| "blob".to_owned());
		let headers = garage_model::object_table::ObjectVersionHeaders {
			content_type,
			other: headers,
		};

		use futures::StreamExt;
		let res = save_stream(
			garage,
			headers,
			field.map(|r| r.map_err(Into::into)),
			bucket_id,
			&key,
			None,
			None,
		)
		.await?;

		let resp = match redirect {
			Err(200) => Response::builder()
				.status(StatusCode::OK)
				.body(Body::empty())?,
			Err(201) => {
				// TODO body should be an XML document, not sure which yet
				Response::builder()
					.status(StatusCode::CREATED)
					.body(res.into_body())?
			}
			// invalid codes are handled as 204
			Err(_) => Response::builder()
				.status(StatusCode::NO_CONTENT)
				.body(Body::empty())?,
			Ok(uri) => Response::builder()
				.status(StatusCode::SEE_OTHER)
				.header(header::LOCATION, uri.clone())
				.body(uri.into())?,
		};

		return Ok(resp);
	}

	Err(Error::BadRequest(
		"Request did not contain a file".to_owned(),
	))
}

// TODO remove allow(dead_code) when policy is verified

#[allow(dead_code)]
#[derive(Deserialize)]
struct Policy {
	expiration: String,
	conditions: Vec<PolicyCondition>,
}

/// A single condition from a policy
#[derive(Deserialize)]
#[serde(untagged)]
enum PolicyCondition {
	// will contain a single key-value pair
	Equal(HashMap<String, String>),
	OtherOp([String; 3]),
	SizeRange(String, u64, u64),
}

#[allow(dead_code)]
#[derive(PartialEq, Eq)]
enum Operation {
	Equal,
	StartsWith,
	StartsWithCT,
	SizeRange,
}
