use std::sync::Arc;

use garage_model::garage::Garage;
use serde::{Serialize};
use hyper::{Body, Client, Method, Request};
use hyper_tls::HttpsConnector;

use crate::s3::error::*;

#[derive(Debug, Serialize, PartialEq)]
pub struct ObjectHook {
	pub hook_type: String,
	pub bucket: String,
	pub object: String,
	pub via: String,
}

pub fn create_put_object_hook(bucket: String, obj: &String, via: String) -> ObjectHook {
	return ObjectHook { hook_type: "PutObject".to_string(), bucket: bucket, object: obj.to_string(), via: via }
}

pub async fn call_hook<T: Serialize>(garage: Arc<Garage>, hook: T) -> Result<(), Error> {
	if let Some(uri) = garage.config.webhook_uri.as_ref() {
		let client = Client::builder().build(HttpsConnector::new());
		let b = serde_json::to_string(&hook).unwrap();
		println!("Connecting to {}", uri);
		let req = Request::builder()
			.method(Method::POST)
			.uri(uri)
			.header("Content-Type", "application/json")
			.body(Body::from(b))?;
	
		// even if there is an error with the webhook, do not cause an error
		if let Err(result) = client.request(req).await {
			println!("Error processing webhook to {}: {}", uri, result);
			return Ok(());
		}
	
		return Ok(());
	} else {
   		return Ok(())
	}
}