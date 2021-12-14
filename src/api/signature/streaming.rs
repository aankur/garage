use chrono::{DateTime, Utc};

use garage_model::garage::Garage;
use garage_util::data::Hash;
use hmac::Mac;

use super::signing_hmac;
use super::{LONG_DATETIME, SHORT_DATE};

use crate::error::*;

/// Result of `sha256("")`
const EMPTY_STRING_HEX_DIGEST: &str =
	"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub fn check_streaming_payload_signature(
	garage: &Garage,
	secret_key: &str,
	date: DateTime<Utc>,
	previous_signature: Hash,
	content_sha256: Hash,
) -> Result<Hash, Error> {
	let scope = format!(
		"{}/{}/s3/aws4_request",
		date.format(SHORT_DATE),
		garage.config.s3_api.s3_region
	);

	let string_to_sign = [
		"AWS4-HMAC-SHA256-PAYLOAD",
		&date.format(LONG_DATETIME).to_string(),
		&scope,
		&hex::encode(previous_signature),
		EMPTY_STRING_HEX_DIGEST,
		&hex::encode(content_sha256),
	]
	.join("\n");

	let mut hmac = signing_hmac(&date, secret_key, &garage.config.s3_api.s3_region, "s3")
		.ok_or_internal_error("Unable to build signing HMAC")?;
	hmac.update(string_to_sign.as_bytes());

	Hash::try_from(&hmac.finalize().into_bytes()).ok_or_bad_request("Invalid signature")
}
