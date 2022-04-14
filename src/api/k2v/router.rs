use crate::error::*;

use std::borrow::Cow;

use hyper::{Method, Request};

use crate::helpers::Authorization;
use crate::router_macros::{generateQueryParameters, router_match};

router_match! {@func


/// List of all K2V API endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
	DeleteBatch {
	},
	DeleteItem {
		partition_key: String,
		sort_key: String,
	},
	InsertBatch {
	},
	InsertItem {
		partition_key: String,
		sort_key: String,
	},
	Options,
	PollItem {
		partition_key: String,
		sort_key: String,
		causality_token: String,
	},
	ReadBatch {
	},
	ReadIndex {
		start: Option<String>,
		end: Option<String>,
		limit: Option<u64>,
	},
	ReadItem {
		partition_key: String,
		sort_key: String,
	},
}}

impl Endpoint {
	/// Determine which S3 endpoint a request is for using the request, and a bucket which was
	/// possibly extracted from the Host header.
	/// Returns Self plus bucket name, if endpoint is not Endpoint::ListBuckets
	pub fn from_request<T>(req: &Request<T>) -> Result<(Self, String), Error> {
		let uri = req.uri();
		let path = uri.path().trim_start_matches('/');
		let query = uri.query();

		let (bucket, partition_key) = path
			.split_once('/')
			.map(|(b, p)| (b.to_owned(), p.trim_start_matches('/')))
			.unwrap_or((path.to_owned(), ""));

		if bucket.is_empty() {
			return Err(Error::BadRequest("Missing bucket name".to_owned()));
		}

		if *req.method() == Method::OPTIONS {
			return Ok((Self::Options, bucket));
		}

		let partition_key = percent_encoding::percent_decode_str(partition_key)
			.decode_utf8()?
			.into_owned();

		let mut query = QueryParameters::from_query(query.unwrap_or_default())?;

		let res = match *req.method() {
			Method::GET => Self::from_get(partition_key, &mut query)?,
			//Method::HEAD => Self::from_head(partition_key, &mut query)?,
			Method::POST => Self::from_post(partition_key, &mut query)?,
			Method::PUT => Self::from_put(partition_key, &mut query)?,
			Method::DELETE => Self::from_delete(partition_key, &mut query)?,
			_ => return Err(Error::BadRequest("Unknown method".to_owned())),
		};

		if let Some(message) = query.nonempty_message() {
			debug!("Unused query parameter: {}", message)
		}
		Ok((res, bucket))
	}

	/// Determine which endpoint a request is for, knowing it is a GET.
	fn from_get(partition_key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), partition_key, query, None),
			key: [
				EMPTY if causality_token => PollItem (query::sort_key, query::causality_token),
				EMPTY => ReadItem (query::sort_key),
			],
			no_key: [
				EMPTY => ReadIndex (query_opt::start, query_opt::end, opt_parse::limit),
			]
		}
	}

	/*
	/// Determine which endpoint a request is for, knowing it is a HEAD.
	fn from_head(partition_key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), partition_key, query, None),
			key: [
				EMPTY => HeadObject(opt_parse::part_number, query_opt::version_id),
			],
			no_key: [
				EMPTY => HeadBucket,
			]
		}
	}
	*/

	/// Determine which endpoint a request is for, knowing it is a POST.
	fn from_post(partition_key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), partition_key, query, None),
			key: [
			],
			no_key: [
				EMPTY => InsertBatch,
				DELETE => DeleteBatch,
				SEARCH => ReadBatch,
			]
		}
	}

	/// Determine which endpoint a request is for, knowing it is a PUT.
	fn from_put(partition_key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), partition_key, query, None),
			key: [
				EMPTY => InsertItem (query::sort_key),

			],
			no_key: [
			]
		}
	}

	/// Determine which endpoint a request is for, knowing it is a DELETE.
	fn from_delete(partition_key: String, query: &mut QueryParameters<'_>) -> Result<Self, Error> {
		router_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), partition_key, query, None),
			key: [
				EMPTY => DeleteItem (query::sort_key),
			],
			no_key: [
			]
		}
	}

	/// Get the partition key the request target. Returns None for requests which don't use a partition key.
	#[allow(dead_code)]
	pub fn get_partition_key(&self) -> Option<&str> {
		router_match! {
			@extract
			self,
			partition_key,
			[
				DeleteItem,
				InsertItem,
				PollItem,
				ReadItem,
			]
		}
	}

	/// Get the sort key the request target. Returns None for requests which don't use a sort key.
	#[allow(dead_code)]
	pub fn get_sort_key(&self) -> Option<&str> {
		router_match! {
			@extract
			self,
			sort_key,
			[
				DeleteItem,
				InsertItem,
				PollItem,
				ReadItem,
			]
		}
	}

	/// Get the kind of authorization which is required to perform the operation.
	pub fn authorization_type(&self) -> Authorization {
		let readonly = router_match! {
			@match
			self,
			[
				PollItem,
				ReadBatch,
				ReadIndex,
				ReadItem,
			]
		};
		if readonly {
			Authorization::Read
		} else {
			Authorization::Write
		}
	}
}

// parameter name => struct field
generateQueryParameters! {
	"start" => start,
	"causality_token" => causality_token,
	"end" => end,
	"limit" => limit,
	"sort_key" => sort_key
}

mod keywords {
	//! This module contain all query parameters with no associated value
	//! used to differentiate endpoints.
	pub const EMPTY: &str = "";

	pub const DELETE: &str = "delete";
	pub const SEARCH: &str = "search";
}
