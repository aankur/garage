use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::{Stream, StreamExt};
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

	// 16k seems plenty for a header. 5G is the max size of a single part, so it seems reasonable
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
		let decoded_policy: Policy =
			serde_json::from_slice(&decoded_policy).ok_or_bad_request("Invalid policy")?;

		let expiration: DateTime<Utc> = DateTime::parse_from_rfc3339(&decoded_policy.expiration)
			.ok_or_bad_request("Invalid expiration date")?
			.into();
		if Utc::now() - expiration > Duration::zero() {
			return Err(Error::BadRequest(
				"Expiration date is in the paste".to_string(),
			));
		}

		let conditions = decoded_policy.into_conditions()?;

		for (param_key, value) in params.iter() {
			let param_key = param_key.as_str();
			if param_key.eq_ignore_ascii_case("content-type") {
				for cond in &conditions.content_type {
					let ok = match cond {
						Operation::Equal(s) => value == s,
						Operation::StartsWith(s) => {
							value.to_str()?.split(',').all(|v| v.starts_with(s))
						}
					};
					if !ok {
						return Err(Error::BadRequest(format!(
							"Key '{}' has value not allowed in policy",
							param_key
						)));
					}
				}
			} else if param_key == "key" {
				let conds = conditions.params.get("key").ok_or_else(|| {
					Error::BadRequest(format!("Key '{}' is not allowed in policy", param_key))
				})?;
				for cond in conds {
					let ok = match cond {
						Operation::Equal(s) => s == &key,
						Operation::StartsWith(s) => key.starts_with(s),
					};
					if !ok {
						return Err(Error::BadRequest(format!(
							"Key '{}' has value not allowed in policy",
							param_key
						)));
					}
				}
			} else {
				let conds = conditions.params.get(param_key).ok_or_else(|| {
					Error::BadRequest(format!("Key '{}' is not allowed in policy", param_key))
				})?;
				for cond in conds {
					let ok = match cond {
						Operation::Equal(s) => s == value,
						Operation::StartsWith(s) => value.to_str()?.starts_with(s),
					};
					if !ok {
						return Err(Error::BadRequest(format!(
							"Key '{}' has value not allowed in policy",
							param_key
						)));
					}
				}
			}
		}

		let content_type = field
			.content_type()
			.map(AsRef::as_ref)
			.map(HeaderValue::from_str)
			.transpose()
			.ok_or_bad_request("Invalid content type")?
			.unwrap_or_else(|| HeaderValue::from_static("blob"));

		params.append(header::CONTENT_TYPE, content_type);
		let headers = get_headers(&params)?;

		let stream = field.map(|r| r.map_err(Into::into));
		let res = save_stream(
			garage,
			headers,
			StreamLimiter::new(stream, conditions.content_length),
			bucket_id,
			&key,
			None,
			None,
		)
		.await?;

		let resp = if let Some(target) = params
			.get("success_action_redirect")
			.and_then(|h| h.to_str().ok())
			.and_then(|u| url::Url::parse(u).ok())
			.filter(|u| u.scheme() == "https" || u.scheme() == "http")
		{
			let target = target.to_string();
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

#[derive(Deserialize)]
struct Policy {
	expiration: String,
	conditions: Vec<PolicyCondition>,
}

impl Policy {
	fn into_conditions(self) -> Result<Conditions, Error> {
		let mut params = HashMap::<_, Vec<_>>::new();
		let mut content_type = Vec::new();

		let mut length = (0, u64::MAX);
		for condition in self.conditions {
			match condition {
				PolicyCondition::Equal(map) => {
					if map.len() != 1 {
						return Err(Error::BadRequest("Invalid policy item".to_owned()));
					}
					let (k, v) = map.into_iter().next().expect("size was verified");
					if k.eq_ignore_ascii_case("content-type") {
						content_type.push(Operation::Equal(v));
					} else {
						params.entry(k).or_default().push(Operation::Equal(v));
					}
				}
				PolicyCondition::OtherOp([cond, mut key, value]) => {
					if key.remove(0) != '$' {
						return Err(Error::BadRequest("Invalid policy item".to_owned()));
					}
					match cond.as_str() {
						"eq" => {
							if key.eq_ignore_ascii_case("content-type") {
								content_type.push(Operation::Equal(value));
							} else {
								params.entry(key).or_default().push(Operation::Equal(value));
							}
						}
						"starts-with" => {
							if key.eq_ignore_ascii_case("content-type") {
								content_type.push(Operation::StartsWith(value));
							} else {
								params
									.entry(key)
									.or_default()
									.push(Operation::StartsWith(value));
							}
						}
						_ => return Err(Error::BadRequest("Invalid policy item".to_owned())),
					}
				}
				PolicyCondition::SizeRange(key, min, max) => {
					if key == "content-length-range" {
						length.0 = length.0.max(min);
						length.1 = length.1.min(max);
					} else {
						return Err(Error::BadRequest("Invalid policy item".to_owned()));
					}
				}
			}
		}
		Ok(Conditions {
			params,
			content_type,
			content_length: RangeInclusive::new(length.0, length.1),
		})
	}
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

struct Conditions {
	params: HashMap<String, Vec<Operation>>,
	content_type: Vec<Operation>,
	#[allow(dead_code)]
	content_length: RangeInclusive<u64>,
}

#[derive(PartialEq, Eq)]
enum Operation {
	Equal(String),
	StartsWith(String),
}

struct StreamLimiter<T> {
	inner: T,
	length: RangeInclusive<u64>,
	read: u64,
}

impl<T> StreamLimiter<T> {
	fn new(stream: T, length: RangeInclusive<u64>) -> Self {
		StreamLimiter {
			inner: stream,
			length,
			read: 0,
		}
	}
}

impl<T> Stream for StreamLimiter<T>
where
	T: Stream<Item = Result<Bytes, Error>> + Unpin,
{
	type Item = Result<Bytes, Error>;
	fn poll_next(
		mut self: std::pin::Pin<&mut Self>,
		ctx: &mut Context<'_>,
	) -> Poll<Option<Self::Item>> {
		let res = std::pin::Pin::new(&mut self.inner).poll_next(ctx);
		match &res {
			Poll::Ready(Some(Ok(bytes))) => {
				self.read += bytes.len() as u64;
				// optimization to fail early when we know before the end it's too long
				if self.length.end() < &self.read {
					return Poll::Ready(Some(Err(Error::BadRequest(
						"File size does not match policy".to_owned(),
					))));
				}
			}
			Poll::Ready(None) => {
				if !self.length.contains(&self.read) {
					return Poll::Ready(Some(Err(Error::BadRequest(
						"File size does not match policy".to_owned(),
					))));
				}
			}
			_ => {}
		}
		res
	}
}
