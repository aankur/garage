use quick_xml::de::from_reader;
use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use crate::error::*;
use crate::s3_xml::{xmlns_tag, IntValue, Value};
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

	let _conf: WebsiteConfiguration = from_reader(&body as &[u8])?;

	if let BucketState::Present(state) = bucket.state.get_mut() {
		state.website.update(true);
		garage.bucket_table.insert(&bucket).await?;
	}

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::from(vec![]))
		.unwrap())
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct WebsiteConfiguration {
	#[serde(serialize_with = "xmlns_tag", skip_deserializing)]
	pub xmlns: (),
	#[serde(rename = "ErrorDocument")]
	pub error_document: Option<Key>,
	#[serde(rename = "IndexDocument")]
	pub index_document: Option<Suffix>,
	#[serde(rename = "RedirectAllRequestsTo")]
	pub redirect_all_requests_to: Option<Target>,
	#[serde(rename = "RoutingRules")]
	pub routing_rules: Option<Vec<RoutingRule>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutingRule {
	#[serde(rename = "RoutingRule")]
	pub routing_rule: RoutingRuleInner,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutingRuleInner {
	#[serde(rename = "Condition")]
	pub condition: Option<Condition>,
	#[serde(rename = "Redirect")]
	pub redirect: Redirect,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
	#[serde(rename = "Key")]
	pub key: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Suffix {
	#[serde(rename = "Suffix")]
	pub suffix: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Target {
	#[serde(rename = "HostName")]
	pub hostname: Option<Value>,
	#[serde(rename = "Protocol")]
	pub protocol: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Condition {
	#[serde(rename = "HttpErrorCodeReturnedEquals")]
	pub http_error_code: Option<IntValue>,
	#[serde(rename = "KeyPrefixEquals")]
	pub prefix: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Redirect {
	#[serde(rename = "HostName")]
	pub hostname: Option<Value>,
	#[serde(rename = "Protocol")]
	pub protocol: Option<Value>,
	#[serde(rename = "HttpRedirectCode")]
	pub http_redirect_code: Option<IntValue>,
	#[serde(rename = "ReplaceKeyPrefixWith")]
	pub replace_prefix: Option<Value>,
	#[serde(rename = "ReplaceKeyWith")]
	pub replace_full: Option<Value>,
}

#[cfg(test)]
mod tests {
	use super::*;

	use quick_xml::de::from_str;

	#[test]
	fn test_deserialize() {
		let message = r#"<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <ErrorDocument>
         <Key>string</Key>
   </ErrorDocument>
   <IndexDocument>
      <Suffix>string</Suffix>
   </IndexDocument>
   <RedirectAllRequestsTo>
      <HostName>string</HostName>
      <Protocol>string</Protocol>
   </RedirectAllRequestsTo>
   <RoutingRules>
      <RoutingRule>
         <Condition>
            <HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals>
            <KeyPrefixEquals>string</KeyPrefixEquals>
         </Condition>
         <Redirect>
            <HostName>string</HostName>
            <Protocol>string</Protocol>
            <HttpRedirectCode>303</HttpRedirectCode>
            <ReplaceKeyPrefixWith>string</ReplaceKeyPrefixWith>
            <ReplaceKeyWith>string</ReplaceKeyWith>
         </Redirect>
      </RoutingRule>
   </RoutingRules>
</WebsiteConfiguration>"#;
		let _conf: WebsiteConfiguration = from_str(message).unwrap();
		// TODO verify result is ok
		// TODO cycle back and verify if ok
	}
}
