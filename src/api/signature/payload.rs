use std::collections::HashMap;

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use hmac::Mac;
use hyper::{Body, Method, Request};
use sha2::{Digest, Sha256};

use garage_table::*;
use garage_util::data::Hash;

use garage_model::garage::Garage;
use garage_model::key_table::*;

use super::LONG_DATETIME;
use super::{compute_scope, signing_hmac};

use crate::encoding::uri_encode;
use crate::signature::error::*;

pub async fn check_payload_signature(
	garage: &Garage,
	service: &'static str,
	request: &Request<Body>,
) -> Result<(Option<Key>, Option<Hash>), Error> {
	let mut headers = HashMap::new();
	for (key, val) in request.headers() {
		headers.insert(key.to_string(), val.to_str()?.to_string());
	}
	if let Some(query) = request.uri().query() {
		let query_pairs = url::form_urlencoded::parse(query.as_bytes());
		for (key, val) in query_pairs {
			headers.insert(key.to_lowercase(), val.to_string());
		}
	}

	let authorization = if let Some(authorization) = headers.get("authorization") {
		parse_authorization(authorization, &headers)?
	} else if let Some(algorithm) = headers.get("x-amz-algorithm") {
		parse_query_authorization(algorithm, &headers)?
	} else {
		let content_sha256 = headers.get("x-amz-content-sha256");
		if let Some(content_sha256) = content_sha256.filter(|c| "UNSIGNED-PAYLOAD" != c.as_str()) {
			let sha256 = hex::decode(content_sha256)
				.ok()
				.and_then(|bytes| Hash::try_from(&bytes))
				.ok_or_bad_request("Invalid content sha256 hash")?;
			return Ok((None, Some(sha256)));
		} else {
			return Ok((None, None));
		}
	};

	let canonical_request = canonical_request(
		service,
		request.method(),
		request.uri(),
		&headers,
		&authorization.signed_headers,
		&authorization.content_sha256,
	);
	let (_, scope) = parse_credential(&authorization.credential)?;
	let string_to_sign = string_to_sign(&authorization.date, &scope, &canonical_request);

	trace!("canonical request:\n{}", canonical_request);
	trace!("string to sign:\n{}", string_to_sign);

	let key = verify_v4(
		garage,
		service,
		&authorization.credential,
		&authorization.date,
		&authorization.signature,
		string_to_sign.as_bytes(),
	)
	.await?;

	let content_sha256 = if authorization.content_sha256 == "UNSIGNED-PAYLOAD" {
		None
	} else if authorization.content_sha256 == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		let bytes = hex::decode(authorization.signature).ok_or_bad_request("Invalid signature")?;
		Some(Hash::try_from(&bytes).ok_or_bad_request("Invalid signature")?)
	} else {
		let bytes = hex::decode(authorization.content_sha256)
			.ok_or_bad_request("Invalid content sha256 hash")?;
		Some(Hash::try_from(&bytes).ok_or_bad_request("Invalid content sha256 hash")?)
	};

	Ok((Some(key), content_sha256))
}

struct Authorization {
	credential: String,
	signed_headers: String,
	signature: String,
	content_sha256: String,
	date: DateTime<Utc>,
}

fn parse_authorization(
	authorization: &str,
	headers: &HashMap<String, String>,
) -> Result<Authorization, Error> {
	let first_space = authorization
		.find(' ')
		.ok_or_bad_request("Authorization field to short")?;
	let (auth_kind, rest) = authorization.split_at(first_space);

	if auth_kind != "AWS4-HMAC-SHA256" {
		return Err(Error::bad_request("Unsupported authorization method"));
	}

	let mut auth_params = HashMap::new();
	for auth_part in rest.split(',') {
		let auth_part = auth_part.trim();
		let eq = auth_part
			.find('=')
			.ok_or_bad_request("Field without value in authorization header")?;
		let (key, value) = auth_part.split_at(eq);
		auth_params.insert(key.to_string(), value.trim_start_matches('=').to_string());
	}

	let cred = auth_params
		.get("Credential")
		.ok_or_bad_request("Could not find Credential in Authorization field")?;

	let content_sha256 = headers
		.get("x-amz-content-sha256")
		.ok_or_bad_request("Missing X-Amz-Content-Sha256 field")?;

	let date = headers
		.get("x-amz-date")
		.ok_or_bad_request("Missing X-Amz-Date field")
		.map_err(Error::from)
		.and_then(|d| parse_date(d))?;

	if Utc::now() - date > Duration::hours(24) {
		return Err(Error::bad_request("Date is too old".to_string()));
	}

	let auth = Authorization {
		credential: cred.to_string(),
		signed_headers: auth_params
			.get("SignedHeaders")
			.ok_or_bad_request("Could not find SignedHeaders in Authorization field")?
			.to_string(),
		signature: auth_params
			.get("Signature")
			.ok_or_bad_request("Could not find Signature in Authorization field")?
			.to_string(),
		content_sha256: content_sha256.to_string(),
		date,
	};
	Ok(auth)
}

fn parse_query_authorization(
	algorithm: &str,
	headers: &HashMap<String, String>,
) -> Result<Authorization, Error> {
	if algorithm != "AWS4-HMAC-SHA256" {
		return Err(Error::bad_request(
			"Unsupported authorization method".to_string(),
		));
	}

	let cred = headers
		.get("x-amz-credential")
		.ok_or_bad_request("X-Amz-Credential not found in query parameters")?;
	let signed_headers = headers
		.get("x-amz-signedheaders")
		.ok_or_bad_request("X-Amz-SignedHeaders not found in query parameters")?;
	let signature = headers
		.get("x-amz-signature")
		.ok_or_bad_request("X-Amz-Signature not found in query parameters")?;
	let content_sha256 = headers
		.get("x-amz-content-sha256")
		.map(|x| x.as_str())
		.unwrap_or("UNSIGNED-PAYLOAD");

	let duration = headers
		.get("x-amz-expires")
		.ok_or_bad_request("X-Amz-Expires not found in query parameters")?
		.parse()
		.map_err(|_| Error::bad_request("X-Amz-Expires is not a number".to_string()))?;

	if duration > 7 * 24 * 3600 {
		return Err(Error::bad_request(
			"X-Amz-Expires may not exceed a week".to_string(),
		));
	}

	let date = headers
		.get("x-amz-date")
		.ok_or_bad_request("Missing X-Amz-Date field")
		.map_err(Error::from)
		.and_then(|d| parse_date(d))?;

	if Utc::now() - date > Duration::seconds(duration) {
		return Err(Error::bad_request("Date is too old".to_string()));
	}

	Ok(Authorization {
		credential: cred.to_string(),
		signed_headers: signed_headers.to_string(),
		signature: signature.to_string(),
		content_sha256: content_sha256.to_string(),
		date,
	})
}

fn parse_credential(cred: &str) -> Result<(String, String), Error> {
	let first_slash = cred
		.find('/')
		.ok_or_bad_request("Credentials does not contain '/' in authorization field")?;
	let (key_id, scope) = cred.split_at(first_slash);
	Ok((
		key_id.to_string(),
		scope.trim_start_matches('/').to_string(),
	))
}

pub fn string_to_sign(datetime: &DateTime<Utc>, scope_string: &str, canonical_req: &str) -> String {
	let mut hasher = Sha256::default();
	hasher.update(canonical_req.as_bytes());
	[
		"AWS4-HMAC-SHA256",
		&datetime.format(LONG_DATETIME).to_string(),
		scope_string,
		&hex::encode(hasher.finalize().as_slice()),
	]
	.join("\n")
}

pub fn canonical_request(
	service: &'static str,
	method: &Method,
	uri: &hyper::Uri,
	headers: &HashMap<String, String>,
	signed_headers: &str,
	content_sha256: &str,
) -> String {
	// There seems to be evidence that in AWSv4 signatures, the path component is url-encoded
	// a second time when building the canonical request, as specified in this documentation page:
	// -> https://docs.aws.amazon.com/rolesanywhere/latest/userguide/authentication-sign-process.html
	// However this documentation page is for a specific service ("roles anywhere"), and
	// in the S3 service we know for a fact that there is no double-urlencoding, because all of
	// the tests we made with external software work without it.
	//
	// The theory is that double-urlencoding occurs for all services except S3,
	// which is what is implemented in rusoto_signature:
	// -> https://docs.rs/rusoto_signature/latest/src/rusoto_signature/signature.rs.html#464
	//
	// Digging into the code of the official AWS Rust SDK, we learn that double-URI-encoding can
	// be set or unset on a per-request basis (the signature crates, aws-sigv4 and aws-sig-auth,
	// are agnostic to this). Grepping the codebase confirms that S3 is the only API for which
	// double_uri_encode is set to false, meaning it is true (its default value) for all other
	// AWS services. We will therefore implement this behavior in Garage as well.
	//
	// Note that this documentation page, which is touted as the "authoritative reference" on
	// AWSv4 signatures, makes no mention of either single- or double-urlencoding:
	// -> https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
	// This page of the S3 documentation does also not mention anything specific:
	// -> https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
	//
	// Note that there is also the issue of path normalization, which I hope is unrelated to the
	// one of URI-encoding. At least in aws-sigv4 both parameters can be set independently,
	// and rusoto_signature does not seem to do any effective path normalization, even though
	// it mentions it in the comments (same link to the souce code as above).
	// We make the explicit choice of NOT normalizing paths in the K2V API because doing so
	// would make non-normalized paths invalid K2V partition keys, and we don't want that.
	let path: std::borrow::Cow<str> = if service != "s3" {
		uri_encode(uri.path(), false).into()
	} else {
		uri.path().into()
	};
	[
		method.as_str(),
		&path,
		&canonical_query_string(uri),
		&canonical_header_string(headers, signed_headers),
		"",
		signed_headers,
		content_sha256,
	]
	.join("\n")
}

fn canonical_header_string(headers: &HashMap<String, String>, signed_headers: &str) -> String {
	let signed_headers_vec = signed_headers.split(';').collect::<Vec<_>>();
	let mut items = headers
		.iter()
		.filter(|(key, _)| signed_headers_vec.contains(&key.as_str()))
		.collect::<Vec<_>>();
	items.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
	items
		.iter()
		.map(|(key, value)| key.to_lowercase() + ":" + value.trim())
		.collect::<Vec<_>>()
		.join("\n")
}

fn canonical_query_string(uri: &hyper::Uri) -> String {
	if let Some(query) = uri.query() {
		let query_pairs = url::form_urlencoded::parse(query.as_bytes());
		let mut items = query_pairs
			.filter(|(key, _)| key != "X-Amz-Signature")
			.map(|(key, value)| uri_encode(&key, true) + "=" + &uri_encode(&value, true))
			.collect::<Vec<_>>();
		items.sort();
		items.join("&")
	} else {
		"".to_string()
	}
}

pub fn parse_date(date: &str) -> Result<DateTime<Utc>, Error> {
	let date: NaiveDateTime =
		NaiveDateTime::parse_from_str(date, LONG_DATETIME).ok_or_bad_request("Invalid date")?;
	Ok(DateTime::from_utc(date, Utc))
}

pub async fn verify_v4(
	garage: &Garage,
	service: &str,
	credential: &str,
	date: &DateTime<Utc>,
	signature: &str,
	payload: &[u8],
) -> Result<Key, Error> {
	let (key_id, scope) = parse_credential(credential)?;

	let scope_expected = compute_scope(date, &garage.config.s3_api.s3_region, service);
	if scope != scope_expected {
		return Err(Error::AuthorizationHeaderMalformed(scope.to_string()));
	}

	let key = garage
		.key_table
		.get(&EmptyKey, &key_id)
		.await?
		.filter(|k| !k.state.is_deleted())
		.ok_or_else(|| Error::forbidden(format!("No such key: {}", &key_id)))?;
	let key_p = key.params().unwrap();

	let mut hmac = signing_hmac(
		date,
		&key_p.secret_key,
		&garage.config.s3_api.s3_region,
		service,
	)
	.ok_or_internal_error("Unable to build signing HMAC")?;
	hmac.update(payload);
	let signature = hex::decode(&signature).map_err(|_| Error::forbidden("Invalid signature"))?;
	if hmac.verify_slice(&signature).is_err() {
		return Err(Error::forbidden("Invalid signature"));
	}

	Ok(key)
}
