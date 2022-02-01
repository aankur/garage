use std::collections::HashMap;
use std::sync::Arc;

use hyper::{header, Body, Request, Response, StatusCode};

use garage_model::garage::Garage;

use crate::error::Error;

use multer::{Constraints, Multipart, SizeLimit};

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
		.ok_or_else(|| Error::BadRequest("Counld not get multipart boundary".to_owned()))?;

	// these limits are rather arbitrary
	let constraints = Constraints::new().size_limit(
		SizeLimit::new()
			.per_field(32 * 1024)
			.for_field("file", 5 * 1024 * 1024 * 1024),
	);

	let mut multipart = Multipart::with_constraints(req.into_body(), boundary, constraints);

	let mut headers = HashMap::new();
	let mut key_id = None;
	let mut key = None;
	let mut policy = None;
	let mut redirect = Err(204);
	while let Some(mut field) = multipart.next_field().await.unwrap() {
		let name = if let Some(name) = field.name() {
			name.to_owned()
		} else {
			continue;
		};

		if name != "file" {
			let content = field.text().await.unwrap();
			match name.as_str() {
				// main fields
				"AWSAccessKeyId" => {
					key_id = Some(content);
				}
				"key" => {
					key = Some(content);
				}
				"policy" => {
					policy = Some(content);
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
				"acl" | "Cache-Control" | "Content-Type" | "Content-Encoding" | "Expires" => {
					headers.insert(name, content);
				}
				_ if name.starts_with("x-amz-") => {
					headers.insert(name, content);
				}
				_ => {
					// TODO should we ignore or error?
				}
			}
			continue;
		}

		let _file_name = field.file_name();
		let _content_type = field.content_type();
		while let Some(_chunk) = field.chunk().await.unwrap() {}

		let resp = match redirect {
			Err(200) => Response::builder()
				.status(StatusCode::OK)
				.body(Body::empty())?,
			Err(201) => {
				// body should be an XML document, not sure which yet
				Response::builder()
					.status(StatusCode::CREATED)
					.body(todo!())?
			}
			// invalid codes are handled as 204
			Err(_) => Response::builder()
				.status(StatusCode::NO_CONTENT)
				.body(Body::empty())?,
			Ok(uri) => {
				// TODO maybe body should contain a link to the ressource?
				Response::builder()
					.status(StatusCode::SEE_OTHER)
					.header(header::LOCATION, uri)
					.body(Body::empty())?
			}
		};

		return Ok(resp);
	}

	return Err(Error::BadRequest(
		"Request did not contain a file".to_owned(),
	));
}
