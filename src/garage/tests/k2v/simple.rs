use std::collections::HashMap;
use crate::common;
use common::custom_requester::BodySignature;

use hyper::Method;

#[tokio::test]
async fn test_simple() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-simple");

	let mut query_params = HashMap::new();
	query_params.insert("sort_key".to_string(), Some("test1".to_string()));

	let res = ctx.k2v.request
		.builder(bucket.clone())
		.method(Method::PUT)
		.path("root".into())
		.query_params(query_params.clone())
		.body(b"Hello, world!".to_vec())
		.body_signature(BodySignature::Classic)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);

	let mut h = HashMap::new();
	h.insert("accept".to_string(), "application/octet-stream".to_string());

	let res2 = ctx.k2v.request
		.builder(bucket.clone())
		.path("root".into())
		.query_params(query_params.clone())
		.signed_headers(h)
		.body_signature(BodySignature::Classic)
		.send()
		.await
		.unwrap();
	assert_eq!(res2.status(), 200);

	let res2_body = hyper::body::to_bytes(res2.into_body()).await.unwrap().to_vec();
	assert_eq!(res2_body, b"Hello, world!");
}
