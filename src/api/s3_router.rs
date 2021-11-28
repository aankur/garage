use crate::error::{Error, OkOrBadRequest};

use std::borrow::Cow;

use hyper::header::HeaderValue;
use hyper::{HeaderMap, Method, Request};

macro_rules! s3_match {
    (@extract $enum:expr , $param:ident, [ $($endpoint:ident,)* ]) => {{
        use Endpoint::*;
        match $enum {
            $(
            $endpoint {$param, ..} => Some($param),
            )*
            _ => None
        }
    }};
    (@gen_parser ($keyword:expr, $key:expr, $bucket:expr, $query:expr, $header:expr),
        key: [$($kw_k:ident $(if $required_k:ident)? $(header $header_k:expr)? => $api_k:ident $(($($conv_k:ident :: $param_k:ident),*))?,)*],
        no_key: [$($kw_nk:ident $(if $required_nk:ident)? $(if_header $header_nk:expr)? => $api_nk:ident $(($($conv_nk:ident :: $param_nk:ident),*))?,)*]) => {{
        use Endpoint::*;
        use keywords::*;
        match ($keyword, !$key.is_empty()){
            $(
            ($kw_k, true) if true $(&& $query.$required_k.is_some())? $(&& $header.contains_key($header_k))? => Ok($api_k {
                bucket: $bucket,
                key: $key,
                $($(
                    $param_k: s3_match!(@@parse_param $query, $conv_k, $param_k),
                )*)?
            }),
            )*
            $(
            ($kw_nk, false) $(if $query.$required_nk.is_some())? $(if $header.contains($header_nk))? => Ok($api_nk {
                bucket: $bucket,
                $($(
                    $param_nk: s3_match!(@@parse_param $query, $conv_nk, $param_nk),
                )*)?
            }),
            )*
			_ => Err(Error::BadRequest("Invalid endpoint".to_owned())),
        }
    }};

    (@@parse_param $query:expr, query_opt, $param:ident) => {{
		$query.$param.take().map(|param| param.into_owned())
    }};
    (@@parse_param $query:expr, query, $param:ident) => {{
        $query.$param.take().ok_or_bad_request("Missing argument for endpoint")?.into_owned()
    }};
    (@@parse_param $query:expr, opt_parse, $param:ident) => {{
		$query.$param
            .take()
            .map(|param| param.parse())
            .transpose()
            .map_err(|_| Error::BadRequest("Failed to parse query parameter".to_owned()))?
    }};
    (@@parse_param $query:expr, parse, $param:ident) => {{
        $query.$param.take().ok_or_bad_request("Missing argument for endpoint")?
            .parse()
            .map_err(|_| Error::BadRequest("Failed to parse query parameter".to_owned()))?
    }};
}

/// List of all S3 API endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
	AbortMultipartUpload {
		bucket: String,
		key: String,
		upload_id: String,
	},
	CompleteMultipartUpload {
		bucket: String,
		key: String,
		upload_id: String,
	},
	CopyObject {
		bucket: String,
		key: String,
	},
	CreateBucket {
		bucket: String,
	},
	CreateMultipartUpload {
		bucket: String,
		key: String,
	},
	DeleteBucket {
		bucket: String,
	},
	DeleteBucketAnalyticsConfiguration {
		bucket: String,
		id: String,
	},
	DeleteBucketCors {
		bucket: String,
	},
	DeleteBucketEncryption {
		bucket: String,
	},
	DeleteBucketIntelligentTieringConfiguration {
		bucket: String,
		id: String,
	},
	DeleteBucketInventoryConfiguration {
		bucket: String,
		id: String,
	},
	DeleteBucketLifecycle {
		bucket: String,
	},
	DeleteBucketMetricsConfiguration {
		bucket: String,
		id: String,
	},
	DeleteBucketOwnershipControls {
		bucket: String,
	},
	DeleteBucketPolicy {
		bucket: String,
	},
	DeleteBucketReplication {
		bucket: String,
	},
	DeleteBucketTagging {
		bucket: String,
	},
	DeleteBucketWebsite {
		bucket: String,
	},
	DeleteObject {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	DeleteObjects {
		bucket: String,
	},
	DeleteObjectTagging {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	DeletePublicAccessBlock {
		bucket: String,
	},
	GetBucketAccelerateConfiguration {
		bucket: String,
	},
	GetBucketAcl {
		bucket: String,
	},
	GetBucketAnalyticsConfiguration {
		bucket: String,
		id: String,
	},
	GetBucketCors {
		bucket: String,
	},
	GetBucketEncryption {
		bucket: String,
	},
	GetBucketIntelligentTieringConfiguration {
		bucket: String,
		id: String,
	},
	GetBucketInventoryConfiguration {
		bucket: String,
		id: String,
	},
	GetBucketLifecycleConfiguration {
		bucket: String,
	},
	GetBucketLocation {
		bucket: String,
	},
	GetBucketLogging {
		bucket: String,
	},
	GetBucketMetricsConfiguration {
		bucket: String,
		id: String,
	},
	GetBucketNotificationConfiguration {
		bucket: String,
	},
	GetBucketOwnershipControls {
		bucket: String,
	},
	GetBucketPolicy {
		bucket: String,
	},
	GetBucketPolicyStatus {
		bucket: String,
	},
	GetBucketReplication {
		bucket: String,
	},
	GetBucketRequestPayment {
		bucket: String,
	},
	GetBucketTagging {
		bucket: String,
	},
	GetBucketVersioning {
		bucket: String,
	},
	GetBucketWebsite {
		bucket: String,
	},
	// There are actually many more query parameters, used to add headers to the answer. They were
	// not added here as they are best handled in a dedicated route.
	GetObject {
		bucket: String,
		key: String,
		part_number: Option<u64>,
		version_id: Option<String>,
	},
	GetObjectAcl {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	GetObjectLegalHold {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	GetObjectLockConfiguration {
		bucket: String,
	},
	GetObjectRetention {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	GetObjectTagging {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	GetObjectTorrent {
		bucket: String,
		key: String,
	},
	GetPublicAccessBlock {
		bucket: String,
	},
	HeadBucket {
		bucket: String,
	},
	HeadObject {
		bucket: String,
		key: String,
		part_number: Option<u64>,
		version_id: Option<String>,
	},
	ListBucketAnalyticsConfigurations {
		bucket: String,
		continuation_token: Option<String>,
	},
	ListBucketIntelligentTieringConfigurations {
		bucket: String,
		continuation_token: Option<String>,
	},
	ListBucketInventoryConfigurations {
		bucket: String,
		continuation_token: Option<String>,
	},
	ListBucketMetricsConfigurations {
		bucket: String,
		continuation_token: Option<String>,
	},
	ListBuckets,
	ListMultipartUploads {
		bucket: String,
		delimiter: Option<char>,
		encoding_type: Option<String>,
		key_marker: Option<String>,
		max_uploads: Option<u64>,
		prefix: Option<String>,
		upload_id_marker: Option<String>,
	},
	ListObjects {
		bucket: String,
		delimiter: Option<char>,
		encoding_type: Option<String>,
		marker: Option<String>,
		max_keys: Option<usize>,
		prefix: Option<String>,
	},
	ListObjectsV2 {
		bucket: String,
		list_type: String, // must be 2
		continuation_token: Option<String>,
		delimiter: Option<char>,
		encoding_type: Option<String>,
		fetch_owner: Option<bool>,
		max_keys: Option<usize>,
		prefix: Option<String>,
		start_after: Option<String>,
	},
	ListObjectVersions {
		bucket: String,
		delimiter: Option<char>,
		encoding_type: Option<String>,
		key_marker: Option<String>,
		max_keys: Option<u64>,
		prefix: Option<String>,
		version_id_marker: Option<String>,
	},
	ListParts {
		bucket: String,
		key: String,
		max_parts: Option<u64>,
		part_number_marker: Option<u64>,
		upload_id: String,
	},
	PutBucketAccelerateConfiguration {
		bucket: String,
	},
	PutBucketAcl {
		bucket: String,
	},
	PutBucketAnalyticsConfiguration {
		bucket: String,
		id: String,
	},
	PutBucketCors {
		bucket: String,
	},
	PutBucketEncryption {
		bucket: String,
	},
	PutBucketIntelligentTieringConfiguration {
		bucket: String,
		id: String,
	},
	PutBucketInventoryConfiguration {
		bucket: String,
		id: String,
	},
	PutBucketLifecycleConfiguration {
		bucket: String,
	},
	PutBucketLogging {
		bucket: String,
	},
	PutBucketMetricsConfiguration {
		bucket: String,
		id: String,
	},
	PutBucketNotificationConfiguration {
		bucket: String,
	},
	PutBucketOwnershipControls {
		bucket: String,
	},
	PutBucketPolicy {
		bucket: String,
	},
	PutBucketReplication {
		bucket: String,
	},
	PutBucketRequestPayment {
		bucket: String,
	},
	PutBucketTagging {
		bucket: String,
	},
	PutBucketVersioning {
		bucket: String,
	},
	PutBucketWebsite {
		bucket: String,
	},
	PutObject {
		bucket: String,
		key: String,
	},
	PutObjectAcl {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	PutObjectLegalHold {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	PutObjectLockConfiguration {
		bucket: String,
	},
	PutObjectRetention {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	PutObjectTagging {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	PutPublicAccessBlock {
		bucket: String,
	},
	RestoreObject {
		bucket: String,
		key: String,
		version_id: Option<String>,
	},
	SelectObjectContent {
		bucket: String,
		key: String,
		select_type: String, // should always be 2
	},
	UploadPart {
		bucket: String,
		key: String,
		part_number: u64,
		upload_id: String,
	},
	UploadPartCopy {
		bucket: String,
		key: String,
		part_number: u64,
		upload_id: String,
	},
}

impl Endpoint {
	pub fn from_request<T>(req: &Request<T>, bucket: Option<String>) -> Result<Self, Error> {
		let uri = req.uri();
		let path = uri.path().trim_start_matches('/');
		let query = uri.query();
		if bucket.is_none() && path.is_empty() {
			if query.is_none() {
				return Ok(Self::ListBuckets);
			} else {
				return Err(Error::BadRequest("Invalid ListBuckets query".to_owned()));
			}
		}

		let (bucket, key) = if let Some(bucket) = bucket {
			(bucket, path.to_owned())
		} else {
			path.split_once('/')
				.map(|(b, p)| (b.to_owned(), p.trim_start_matches('/').to_owned()))
				.unwrap_or((path.to_owned(), String::new()))
		};

		let mut query = QueryParameters::from_query(query.unwrap_or_default())?;

		let res = match *req.method() {
			Method::GET => Self::from_get(bucket, key, &mut query)?,
			Method::HEAD => Self::from_head(bucket, key, &mut query)?,
			Method::POST => Self::from_post(bucket, key, &mut query)?,
			Method::PUT => Self::from_put(bucket, key, &mut query, req.headers())?,
			Method::DELETE => Self::from_delete(bucket, key, &mut query)?,
			_ => return Err(Error::BadRequest("Unknown method".to_owned())),
		};

		if let Some(message) = query.nonempty_message() {
			// maybe this should just be a warn! ?
			Err(Error::BadRequest(message.to_owned()))
		} else {
			Ok(res)
		}
	}

	fn from_get(
		bucket: String,
		key: String,
		query: &mut QueryParameters<'_>,
	) -> Result<Self, Error> {
		s3_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, bucket, query, None),
			key: [
				EMPTY if upload_id => ListParts (query::upload_id, opt_parse::max_parts, opt_parse::part_number_marker),
				EMPTY => GetObject (query_opt::version_id, opt_parse::part_number),
				ACL => GetObjectAcl (query_opt::version_id),
				LEGAL_HOLD => GetObjectLegalHold (query_opt::version_id),
				RETENTION => GetObjectRetention (query_opt::version_id),
				TAGGING => GetObjectTagging (query_opt::version_id),
				TORRENT => GetObjectTorrent,
			],
			no_key: [
				EMPTY if list_type => ListObjectsV2 (query::list_type, query_opt::continuation_token,
													 opt_parse::delimiter, query_opt::encoding_type,
													 opt_parse::fetch_owner, opt_parse::max_keys,
													 query_opt::prefix, query_opt::start_after),
				EMPTY => ListObjects (opt_parse::delimiter, query_opt::encoding_type, query_opt::marker,
									  opt_parse::max_keys, opt_parse::prefix),
				ACCELERATE => GetBucketAccelerateConfiguration,
				ACL => GetBucketAcl,
				ANALYTICS if id => GetBucketAnalyticsConfiguration (query::id),
				ANALYTICS => ListBucketAnalyticsConfigurations (query_opt::continuation_token),
				CORS => GetBucketCors,
				ENCRYPTION => GetBucketEncryption,
				INTELLIGENT_TIERING if id => GetBucketIntelligentTieringConfiguration (query::id),
				INTELLIGENT_TIERING => ListBucketIntelligentTieringConfigurations (query_opt::continuation_token),
				INVENTORY if id => GetBucketInventoryConfiguration (query::id),
				INVENTORY => ListBucketInventoryConfigurations (query_opt::continuation_token),
				LIFECYCLE => GetBucketLifecycleConfiguration,
				LOCATION => GetBucketLocation,
				LOGGING => GetBucketLogging,
				METRICS if id => GetBucketMetricsConfiguration (query::id),
				METRICS => ListBucketMetricsConfigurations (query_opt::continuation_token),
				NOTIFICATION => GetBucketNotificationConfiguration,
				OBJECT_LOCK => GetObjectLockConfiguration,
				OWNERSHIP_CONTROLS => GetBucketOwnershipControls,
				POLICY => GetBucketPolicy,
				POLICY_STATUS => GetBucketPolicyStatus,
				PUBLIC_ACCESS_BLOCK => GetPublicAccessBlock,
				REPLICATION => GetBucketReplication,
				REQUEST_PAYMENT => GetBucketRequestPayment,
				TAGGING => GetBucketTagging,
				UPLOADS => ListMultipartUploads (opt_parse::delimiter, query_opt::encoding_type,
												 query_opt::key_marker, opt_parse::max_uploads,
												 query_opt::prefix, query_opt::upload_id_marker),
				VERSIONING => GetBucketVersioning,
				VERSIONS => ListObjectVersions (opt_parse::delimiter, query_opt::encoding_type,
												query_opt::key_marker, opt_parse::max_keys,
												query_opt::prefix, query_opt::version_id_marker),
				WEBSITE => GetBucketWebsite,
			]
		}
	}

	fn from_head(
		bucket: String,
		key: String,
		query: &mut QueryParameters<'_>,
	) -> Result<Self, Error> {
		s3_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, bucket, query, None),
			key: [
				EMPTY => HeadObject(opt_parse::part_number, query_opt::version_id),
			],
			no_key: [
				EMPTY => HeadBucket,
			]
		}
	}

	fn from_post(
		bucket: String,
		key: String,
		query: &mut QueryParameters<'_>,
	) -> Result<Self, Error> {
		s3_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, bucket, query, None),
			key: [
				EMPTY if upload_id  => CompleteMultipartUpload (query::upload_id),
				RESTORE => RestoreObject (query_opt::version_id),
				SELECT => SelectObjectContent (query::select_type),
				UPLOADS => CreateMultipartUpload,
			],
			no_key: [
				DELETE => DeleteObjects,
			]
		}
	}

	fn from_put(
		bucket: String,
		key: String,
		query: &mut QueryParameters<'_>,
		headers: &HeaderMap<HeaderValue>,
	) -> Result<Self, Error> {
		s3_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, bucket, query, headers),
			key: [
				EMPTY if part_number header "x-amz-copy-source" => UploadPartCopy (parse::part_number, query::upload_id),
				EMPTY header "x-amz-copy-source" => CopyObject,
				EMPTY if part_number => UploadPart (parse::part_number, query::upload_id),
				EMPTY => PutObject,
				ACL => PutObjectAcl (query_opt::version_id),
				LEGAL_HOLD => PutObjectLegalHold (query_opt::version_id),
				RETENTION => PutObjectRetention (query_opt::version_id),
				TAGGING => PutObjectTagging (query_opt::version_id),

			],
			no_key: [
				EMPTY => CreateBucket,
				ACCELERATE => PutBucketAccelerateConfiguration,
				ACL => PutBucketAcl,
				ANALYTICS => PutBucketAnalyticsConfiguration (query::id),
				CORS => PutBucketCors,
				ENCRYPTION => PutBucketEncryption,
				INTELLIGENT_TIERING => PutBucketIntelligentTieringConfiguration(query::id),
				INVENTORY => PutBucketInventoryConfiguration(query::id),
				LIFECYCLE => PutBucketLifecycleConfiguration,
				LOGGING => PutBucketLogging,
				METRICS => PutBucketMetricsConfiguration(query::id),
				NOTIFICATION => PutBucketNotificationConfiguration,
				OBJECT_LOCK => PutObjectLockConfiguration,
				OWNERSHIP_CONTROLS => PutBucketOwnershipControls,
				POLICY => PutBucketPolicy,
				PUBLIC_ACCESS_BLOCK => PutPublicAccessBlock,
				REPLICATION => PutBucketReplication,
				REQUEST_PAYMENT => PutBucketRequestPayment,
				TAGGING => PutBucketTagging,
				VERSIONING => PutBucketVersioning,
				WEBSITE => PutBucketWebsite,
			]
		}
	}

	fn from_delete(
		bucket: String,
		key: String,
		query: &mut QueryParameters<'_>,
	) -> Result<Self, Error> {
		s3_match! {
			@gen_parser
			(query.keyword.take().unwrap_or_default().as_ref(), key, bucket, query, None),
			key: [
				EMPTY if upload_id => AbortMultipartUpload (query::upload_id),
				EMPTY => DeleteObject (query_opt::version_id),
				TAGGING => DeleteObjectTagging (query_opt::version_id),
			],
			no_key: [
				EMPTY => DeleteBucket,
				ANALYTICS => DeleteBucketAnalyticsConfiguration (query::id),
				CORS => DeleteBucketCors,
				ENCRYPTION => DeleteBucketEncryption,
				INTELLIGENT_TIERING => DeleteBucketIntelligentTieringConfiguration (query::id),
				INVENTORY => DeleteBucketInventoryConfiguration (query::id),
				LIFECYCLE => DeleteBucketLifecycle,
				METRICS => DeleteBucketMetricsConfiguration (query::id),
				OWNERSHIP_CONTROLS => DeleteBucketOwnershipControls,
				POLICY => DeleteBucketPolicy,
				PUBLIC_ACCESS_BLOCK => DeletePublicAccessBlock,
				REPLICATION => DeleteBucketReplication,
				TAGGING => DeleteBucketTagging,
				WEBSITE => DeleteBucketWebsite,
			]
		}
	}

	pub fn get_bucket(&self) -> Option<&str> {
		s3_match! {
			@extract
			self,
			bucket,
			[
				AbortMultipartUpload,
				CompleteMultipartUpload,
				CopyObject,
				CreateBucket,
				CreateMultipartUpload,
				DeleteBucket,
				DeleteBucketAnalyticsConfiguration,
				DeleteBucketCors,
				DeleteBucketEncryption,
				DeleteBucketIntelligentTieringConfiguration,
				DeleteBucketInventoryConfiguration,
				DeleteBucketLifecycle,
				DeleteBucketMetricsConfiguration,
				DeleteBucketOwnershipControls,
				DeleteBucketPolicy,
				DeleteBucketReplication,
				DeleteBucketTagging,
				DeleteBucketWebsite,
				DeleteObject,
				DeleteObjects,
				DeleteObjectTagging,
				DeletePublicAccessBlock,
				GetBucketAccelerateConfiguration,
				GetBucketAcl,
				GetBucketAnalyticsConfiguration,
				GetBucketCors,
				GetBucketEncryption,
				GetBucketIntelligentTieringConfiguration,
				GetBucketInventoryConfiguration,
				GetBucketLifecycleConfiguration,
				GetBucketLocation,
				GetBucketLogging,
				GetBucketMetricsConfiguration,
				GetBucketNotificationConfiguration,
				GetBucketOwnershipControls,
				GetBucketPolicy,
				GetBucketPolicyStatus,
				GetBucketReplication,
				GetBucketRequestPayment,
				GetBucketTagging,
				GetBucketVersioning,
				GetBucketWebsite,
				GetObject,
				GetObjectAcl,
				GetObjectLegalHold,
				GetObjectLockConfiguration,
				GetObjectRetention,
				GetObjectTagging,
				GetObjectTorrent,
				GetPublicAccessBlock,
				HeadBucket,
				HeadObject,
				ListBucketAnalyticsConfigurations,
				ListBucketIntelligentTieringConfigurations,
				ListBucketInventoryConfigurations,
				ListBucketMetricsConfigurations,
				ListMultipartUploads,
				ListObjects,
				ListObjectsV2,
				ListObjectVersions,
				ListParts,
				PutBucketAccelerateConfiguration,
				PutBucketAcl,
				PutBucketAnalyticsConfiguration,
				PutBucketCors,
				PutBucketEncryption,
				PutBucketIntelligentTieringConfiguration,
				PutBucketInventoryConfiguration,
				PutBucketLifecycleConfiguration,
				PutBucketLogging,
				PutBucketMetricsConfiguration,
				PutBucketNotificationConfiguration,
				PutBucketOwnershipControls,
				PutBucketPolicy,
				PutBucketReplication,
				PutBucketRequestPayment,
				PutBucketTagging,
				PutBucketVersioning,
				PutBucketWebsite,
				PutObject,
				PutObjectAcl,
				PutObjectLegalHold,
				PutObjectLockConfiguration,
				PutObjectRetention,
				PutObjectTagging,
				PutPublicAccessBlock,
				RestoreObject,
				SelectObjectContent,
				UploadPart,
				UploadPartCopy,
			]
		}
	}

	#[allow(dead_code)]
	pub fn get_key(&self) -> Option<&str> {
		s3_match! {
			@extract
			self,
			key,
			[
				AbortMultipartUpload,
				CompleteMultipartUpload,
				CopyObject,
				CreateMultipartUpload,
				DeleteObject,
				DeleteObjectTagging,
				GetObject,
				GetObjectAcl,
				GetObjectLegalHold,
				GetObjectRetention,
				GetObjectTagging,
				GetObjectTorrent,
				HeadObject,
				ListParts,
				PutObject,
				PutObjectAcl,
				PutObjectLegalHold,
				PutObjectRetention,
				PutObjectTagging,
				RestoreObject,
				SelectObjectContent,
				UploadPart,
				UploadPartCopy,
			]
		}
	}

	pub fn authorization_type(&self) -> Authorization<'_> {
		let bucket = if let Some(bucket) = self.get_bucket() {
			bucket
		} else {
			return Authorization::None;
		};
		let readonly = s3_match! {
			@extract
			self,
			bucket,
			[
				GetBucketAccelerateConfiguration,
				GetBucketAcl,
				GetBucketAnalyticsConfiguration,
				GetBucketCors,
				GetBucketEncryption,
				GetBucketIntelligentTieringConfiguration,
				GetBucketInventoryConfiguration,
				GetBucketLifecycleConfiguration,
				GetBucketLocation,
				GetBucketLogging,
				GetBucketMetricsConfiguration,
				GetBucketNotificationConfiguration,
				GetBucketOwnershipControls,
				GetBucketPolicy,
				GetBucketPolicyStatus,
				GetBucketReplication,
				GetBucketRequestPayment,
				GetBucketTagging,
				GetBucketVersioning,
				GetBucketWebsite,
				GetObject,
				GetObjectAcl,
				GetObjectLegalHold,
				GetObjectLockConfiguration,
				GetObjectRetention,
				GetObjectTagging,
				GetObjectTorrent,
				GetPublicAccessBlock,
				HeadBucket,
				HeadObject,
				ListBucketAnalyticsConfigurations,
				ListBucketIntelligentTieringConfigurations,
				ListBucketInventoryConfigurations,
				ListBucketMetricsConfigurations,
				ListMultipartUploads,
				ListObjects,
				ListObjectsV2,
				ListObjectVersions,
				ListParts,
				SelectObjectContent,
			]
		}
		.is_some();
		if readonly {
			Authorization::Read(bucket)
		} else {
			Authorization::Write(bucket)
		}
	}
}

pub enum Authorization<'a> {
	None,
	Read(&'a str),
	Write(&'a str),
}

macro_rules! generateQueryParameters {
    ( $($rest:expr => $name:ident),* ) => {
        #[allow(non_snake_case)]
        #[derive(Debug, Default)]
        struct QueryParameters<'a> {
            keyword: Option<Cow<'a, str>>,
            $(
            $name: Option<Cow<'a, str>>,
            )*
        }

        impl<'a> QueryParameters<'a> {
            fn from_query(query: &'a str) -> Result<Self, Error> {
                let mut res: Self = Default::default();
                for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
                    let repeated = match k.as_ref() {
                        $(
                            $rest => res.$name.replace(v).is_some(),
                        )*
                        _ => {
                            if k.starts_with("response-") {
                                false
                            } else if v.as_ref().is_empty() {
                                if res.keyword.replace(k).is_some() {
                                    return Err(Error::BadRequest("Multiple keywords".to_owned()));
                                }
                                continue;
                            } else {
                                return Err(Error::BadRequest(format!(
                                    "Unknown query parameter '{}'",
                                    k
                                )));
                            }
                        }
                    };
                    if repeated {
                        return Err(Error::BadRequest(format!(
                            "Query parameter repeated: '{}'",
                            k
                        )));
                    }
                }
                Ok(res)
            }

            fn nonempty_message(&self) -> Option<&str> {
                if self.keyword.is_some() {
                    Some("Keyword not used")
                } $(
                    else if self.$name.is_some() {
                        Some(concat!("Query parameter not needed: '", $rest, "'" ))
                    }
                )* else {
                    None
                }
            }
        }
    }
}

generateQueryParameters! {
	"continuation-token" => continuation_token,
	"delimiter" => delimiter,
	"encoding-type" => encoding_type,
	"fetch-owner" => fetch_owner,
	"id" => id,
	"key-marker" => key_marker,
	"list-type" => list_type,
	"marker" => marker,
	"max-keys" => max_keys,
	"max-parts" => max_parts,
	"max-uploads" => max_uploads,
	"partNumber" => part_number,
	"part-number-marker" => part_number_marker,
	"prefix" => prefix,
	"select-type" => select_type,
	"start-after" => start_after,
	"uploadId" => upload_id,
	"upload-id-marker" => upload_id_marker,
	"versionId" => version_id,
	"version-id-marker" => version_id_marker
}

mod keywords {
	pub const EMPTY: &str = "";

	pub const ACCELERATE: &str = "accelerate";
	pub const ACL: &str = "acl";
	pub const ANALYTICS: &str = "analytics";
	pub const CORS: &str = "cors";
	pub const DELETE: &str = "delete";
	pub const ENCRYPTION: &str = "encryption";
	pub const INTELLIGENT_TIERING: &str = "intelligent-tiering";
	pub const INVENTORY: &str = "inventory";
	pub const LEGAL_HOLD: &str = "legal-hold";
	pub const LIFECYCLE: &str = "lifecycle";
	pub const LOCATION: &str = "location";
	pub const LOGGING: &str = "logging";
	pub const METRICS: &str = "metrics";
	pub const NOTIFICATION: &str = "notification";
	pub const OBJECT_LOCK: &str = "object-lock";
	pub const OWNERSHIP_CONTROLS: &str = "ownershipControls";
	pub const POLICY: &str = "policy";
	pub const POLICY_STATUS: &str = "policyStatus";
	pub const PUBLIC_ACCESS_BLOCK: &str = "publicAccessBlock";
	pub const REPLICATION: &str = "replication";
	pub const REQUEST_PAYMENT: &str = "requestPayment";
	pub const RESTORE: &str = "restore";
	pub const RETENTION: &str = "retention";
	pub const SELECT: &str = "select";
	pub const TAGGING: &str = "tagging";
	pub const TORRENT: &str = "torrent";
	pub const UPLOADS: &str = "uploads";
	pub const VERSIONING: &str = "versioning";
	pub const VERSIONS: &str = "versions";
	pub const WEBSITE: &str = "website";
}
