use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use futures::StreamExt;
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::{Body, Request, Response, StatusCode};
use multer::{Constraints, Multipart, SizeLimit};
use serde::Deserialize;

use garage_model::garage::Garage;

use crate::api_server::resolve_bucket;
use crate::error::*;
use crate::s3_put::{get_headers, save_stream};
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

	// 16k seems plenty for a header. 5G is the max size of a single part, so it seemrs reasonable
	// for a PostObject
	let constraints = Constraints::new().size_limit(
		SizeLimit::new()
			.per_field(16 * 1024)
			.for_field("file", 5 * 1024 * 1024 * 1024),
	);

	let mut multipart = Multipart::with_constraints(req.into_body(), boundary, constraints);

	let mut params = HeaderMap::new();
	while let Some(field) = multipart.next_field().await? {
		let name: HeaderName = if let Some(Ok(name)) = field.name().map(TryInto::try_into) {
			name
		} else {
			continue;
		};
		if name != "file" {
			if let Ok(content) = HeaderValue::from_str(&field.text().await?) {
				match name.as_str() {
					"tag" => (/* tag need to be reencoded, but we don't support them yet anyway */),
					"acl" => {
						params.append("x-amz-acl", content);
					}
					_ => {
						params.append(name, content);
					}
				}
			}
			continue;
		}

		// Current part is file. Do some checks before handling to PutObject code
		let key = params
			.get("key")
			.ok_or_bad_request("No key was provided")?
			.to_str()?;
		let credential = params
			.get("x-amz-credential")
			.ok_or_else(|| {
				Error::Forbidden("Garage does not support anonymous access yet".to_string())
			})?
			.to_str()?;
		let policy = params
			.get("policy")
			.ok_or_bad_request("No policy was provided")?
			.to_str()?;
		let signature = params
			.get("x-amz-signature")
			.ok_or_bad_request("No signature was provided")?
			.to_str()?;
		let date = params
			.get("x-amz-date")
			.ok_or_bad_request("No date was provided")?
			.to_str()?;

		let key = if key.contains("${filename}") {
			let filename = field.file_name();
			// is this correct? Maybe we should error instead of default?
			key.replace("${filename}", filename.unwrap_or_default())
		} else {
			key.to_owned()
		};

		let date = parse_date(date)?;
		let api_key = verify_v4(&garage, credential, &date, signature, policy.as_bytes()).await?;

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
			.map(AsRef::as_ref)
			.map(HeaderValue::from_str)
			.transpose()
			.ok_or_bad_request("Invalid content type")?
			.unwrap_or_else(|| HeaderValue::from_static("blob"));

		params.append(header::CONTENT_TYPE, content_type);
		let headers = get_headers(&params)?;

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

		let resp = if let Some(target) = params
			.get("success_action_redirect")
			.and_then(|h| h.to_str().ok())
		{
			// TODO should validate it's a valid url
			let target = target.to_owned();
			Response::builder()
				.status(StatusCode::SEE_OTHER)
				.header(header::LOCATION, target.clone())
				.body(target.into())?
		} else {
			let action = params
				.get("success_action_status")
				.and_then(|h| h.to_str().ok())
				.unwrap_or("204");
			match action {
				"200" => Response::builder()
					.status(StatusCode::OK)
					.body(Body::empty())?,
				"201" => {
					// TODO body should be an XML document, not sure which yet
					Response::builder()
						.status(StatusCode::CREATED)
						.body(res.into_body())?
				}
				_ => Response::builder()
					.status(StatusCode::NO_CONTENT)
					.body(Body::empty())?,
			}
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
