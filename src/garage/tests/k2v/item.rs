use crate::common;

use assert_json_diff::assert_json_eq;
use serde_json::json;

use super::json_body;
use hyper::Method;

#[tokio::test]
async fn test_items_and_indices() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-item-and-index");

	// ReadIndex -- there should be nothing
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.send()
		.await
		.unwrap();
	let res_body = json_body(res).await;
	assert_json_eq!(
		res_body,
		json!({
			"prefix": null,
			"start": null,
			"end": null,
			"limit": null,
			"partitionKeys": [],
			"more": false,
			"nextStart": null
		})
	);

	let content2_len = "_: hello universe".len();
	let content3_len = "_: concurrent value".len();

	for (i, sk) in ["a", "b", "c", "d"].iter().enumerate() {
		let content = format!("{}: hello world", sk).into_bytes();
		let content2 = format!("{}: hello universe", sk).into_bytes();
		let content3 = format!("{}: concurrent value", sk).into_bytes();

		// Put initially, no causality token
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.body(content.clone())
			.method(Method::PUT)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), 200);

		// Get value back
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("accept", "*/*")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), 200);
		assert_eq!(
			res.headers().get("content-type").unwrap().to_str().unwrap(),
			"application/octet-stream"
		);
		let ct = res
			.headers()
			.get("x-garage-causality-token")
			.unwrap()
			.to_str()
			.unwrap()
			.to_string();
		let res_body = hyper::body::to_bytes(res.into_body())
			.await
			.unwrap()
			.to_vec();
		assert_eq!(res_body, content);

		// ReadIndex -- now there should be some stuff
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.send()
			.await
			.unwrap();
		let res_body = json_body(res).await;
		assert_json_eq!(
			res_body,
			json!({
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"partitionKeys": [
				{
					"pk": "root",
					"entries": i+1,
					"conflicts": i,
					"values": i+i+1,
					"bytes": i*(content2.len() + content3.len()) + content.len(),
				}
				],
				"more": false,
				"nextStart": null
			})
		);

		// Put again, this time with causality token
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("x-garage-causality-token", ct.clone())
			.body(content2.clone())
			.method(Method::PUT)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), 200);

		// Get value back
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("accept", "*/*")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), 200);
		assert_eq!(
			res.headers().get("content-type").unwrap().to_str().unwrap(),
			"application/octet-stream"
		);
		let res_body = hyper::body::to_bytes(res.into_body())
			.await
			.unwrap()
			.to_vec();
		assert_eq!(res_body, content2);

		// ReadIndex -- now there should be some stuff
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.send()
			.await
			.unwrap();
		let res_body = json_body(res).await;
		assert_json_eq!(
			res_body,
			json!({
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"partitionKeys": [
				{
					"pk": "root",
					"entries": i+1,
					"conflicts": i,
					"values": i+i+1,
					"bytes": i*content3.len() + (i+1)*content2.len(),
				}
				],
				"more": false,
				"nextStart": null
			})
		);

		// Put again with same CT, now we have concurrent values
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("x-garage-causality-token", ct.clone())
			.body(content3.clone())
			.method(Method::PUT)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), 200);

		// Get value back
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("accept", "*/*")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), 200);
		assert_eq!(
			res.headers().get("content-type").unwrap().to_str().unwrap(),
			"application/json"
		);
		let res_json = json_body(res).await;
		assert_json_eq!(
			res_json,
			[base64::encode(&content2), base64::encode(&content3)]
		);

		// ReadIndex -- now there should be some stuff
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.send()
			.await
			.unwrap();
		let res_body = json_body(res).await;
		assert_json_eq!(
			res_body,
			json!({
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"partitionKeys": [
				{
					"pk": "root",
					"entries": i+1,
					"conflicts": i+1,
					"values": 2*(i+1),
					"bytes": (i+1)*(content2.len() + content3.len()),
				}
				],
				"more": false,
				"nextStart": null
			})
		);
	}

	// Now delete things
	for (i, sk) in ["a", "b", "c", "d"].iter().enumerate() {
		// Get value back (we just need the CT)
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("accept", "*/*")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), 200);
		let ct = res
			.headers()
			.get("x-garage-causality-token")
			.unwrap()
			.to_str()
			.unwrap()
			.to_string();

		// Delete it
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.method(Method::DELETE)
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("x-garage-causality-token", ct)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), 204);

		// ReadIndex -- now there should be some stuff
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.send()
			.await
			.unwrap();
		let res_body = json_body(res).await;
		if i < 3 {
			assert_json_eq!(
				res_body,
				json!({
					"prefix": null,
					"start": null,
					"end": null,
					"limit": null,
					"partitionKeys": [
					{
						"pk": "root",
						"entries": 3-i,
						"conflicts": 3-i,
						"values": 2*(3-i),
						"bytes": (3-i)*(content2_len + content3_len),
					}
					],
					"more": false,
					"nextStart": null
				})
			);
		} else {
			assert_json_eq!(
				res_body,
				json!({
					"prefix": null,
					"start": null,
					"end": null,
					"limit": null,
					"partitionKeys": [],
					"more": false,
					"nextStart": null
				})
			);
		}
	}
}
