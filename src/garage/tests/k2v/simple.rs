use crate::common;
use common::custom_requester::BodySignature;

use hyper::Method;

#[tokio::test]
async fn test_simple() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-simple");

	let res = ctx.k2v.request
		.builder(bucket.clone())
		.method(Method::PUT)
		.path("root".into())
		.query_param("sort_key", Some("test1"))
		.body(b"Hello, world!".to_vec())
		.body_signature(BodySignature::Classic)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);

	let res2 = ctx.k2v.request
		.builder(bucket.clone())
		.path("root".into())
		.query_param("sort_key", Some("test1"))
		.signed_header("accept", "application/octet-stream")
		.body_signature(BodySignature::Classic)
		.send()
		.await
		.unwrap();
	assert_eq!(res2.status(), 200);

	let res2_body = hyper::body::to_bytes(res2.into_body()).await.unwrap().to_vec();
	assert_eq!(res2_body, b"Hello, world!");
}
