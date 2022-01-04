use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::prelude::*;
use futures::task;
use hyper::body::Bytes;

use garage_model::garage::Garage;
use garage_util::data::Hash;
use hmac::Mac;

use super::sha256sum;
use super::HmacSha256;
use super::{LONG_DATETIME, SHORT_DATE};

use crate::error::*;

/// Result of `sha256("")`
const EMPTY_STRING_HEX_DIGEST: &str =
	"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn compute_streaming_payload_signature(
	garage: &Garage,
	signing_hmac: &HmacSha256,
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

	let mut hmac = signing_hmac.clone();
	hmac.update(string_to_sign.as_bytes());

	Hash::try_from(&hmac.finalize().into_bytes()).ok_or_internal_error("Invalid signature")
}

mod payload {
	use garage_util::data::Hash;

	pub enum Error<I> {
		Parser(nom::error::Error<I>),
		BadSignature,
	}

	impl<I> Error<I> {
		pub fn description(&self) -> &str {
			match *self {
				Error::Parser(ref e) => e.code.description(),
				Error::BadSignature => "Bad signature",
			}
		}
	}

	#[derive(Debug, Clone)]
	pub struct Header {
		pub size: usize,
		pub signature: Hash,
	}

	impl Header {
		pub fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, Error<&[u8]>> {
			use nom::bytes::streaming::tag;
			use nom::character::streaming::hex_digit1;
			use nom::combinator::map_res;
			use nom::number::streaming::hex_u32;

			macro_rules! try_parse {
				($expr:expr) => {
					$expr.map_err(|e| e.map(Error::Parser))?
				};
			}

			let (input, size) = try_parse!(hex_u32(input));
			let (input, _) = try_parse!(tag(";")(input));

			let (input, _) = try_parse!(tag("chunk-signature=")(input));
			let (input, data) = try_parse!(map_res(hex_digit1, hex::decode)(input));
			let signature = Hash::try_from(&data).ok_or(nom::Err::Failure(Error::BadSignature))?;

			let (input, _) = try_parse!(tag("\r\n")(input));

			let header = Header {
				size: size as usize,
				signature,
			};

			Ok((input, header))
		}
	}
}

pub enum SignedPayloadStreamError {
	Stream(Error),
	InvalidSignature,
	Message(String),
}

impl SignedPayloadStreamError {
	fn message(msg: &str) -> Self {
		SignedPayloadStreamError::Message(msg.into())
	}
}

impl From<SignedPayloadStreamError> for Error {
	fn from(err: SignedPayloadStreamError) -> Self {
		match err {
			SignedPayloadStreamError::Stream(e) => e,
			SignedPayloadStreamError::InvalidSignature => {
				Error::BadRequest("Invalid payload signature".into())
			}
			SignedPayloadStreamError::Message(e) => {
				Error::BadRequest(format!("Chunk format error: {}", e))
			}
		}
	}
}

impl<I> From<payload::Error<I>> for SignedPayloadStreamError {
	fn from(err: payload::Error<I>) -> Self {
		Self::message(err.description())
	}
}

impl<I> From<nom::error::Error<I>> for SignedPayloadStreamError {
	fn from(err: nom::error::Error<I>) -> Self {
		Self::message(err.code.description())
	}
}

#[pin_project::pin_project]
pub struct SignedPayloadStream<S>
where
	S: Stream<Item = Result<Bytes, Error>>,
{
	#[pin]
	stream: S,
	buf: bytes::BytesMut,
	garage: Arc<Garage>,
	datetime: DateTime<Utc>,
	signing_hmac: HmacSha256,
	previous_signature: Hash,
}

impl<S> SignedPayloadStream<S>
where
	S: Stream<Item = Result<Bytes, Error>>,
{
	pub fn new(
		stream: S,
		garage: Arc<Garage>,
		datetime: DateTime<Utc>,
		secret_key: &str,
		seed_signature: Hash,
	) -> Result<Self, Error> {
		let signing_hmac =
			super::signing_hmac(&datetime, secret_key, &garage.config.s3_api.s3_region, "s3")
				.ok_or_internal_error("Could not compute signing HMAC")?;

		Ok(Self {
			stream,
			buf: bytes::BytesMut::new(),
			garage,
			datetime,
			signing_hmac,
			previous_signature: seed_signature,
		})
	}
}

impl<S> Stream for SignedPayloadStream<S>
where
	S: Stream<Item = Result<Bytes, Error>> + Unpin,
{
	type Item = Result<Bytes, SignedPayloadStreamError>;

	fn poll_next(
		self: Pin<&mut Self>,
		cx: &mut task::Context<'_>,
	) -> task::Poll<Option<Self::Item>> {
		use std::task::Poll;

		use nom::bytes::streaming::{tag, take};

		let mut this = self.project();

		macro_rules! try_parse {
			($expr:expr) => {
				match $expr {
					Ok(value) => Ok(value),
					Err(nom::Err::Incomplete(_)) => continue,
					Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Err(e),
				}?
			};
		}

		loop {
			match futures::ready!(this.stream.as_mut().poll_next(cx)) {
				Some(Ok(bytes)) => {
					this.buf.extend(bytes);
				}
				Some(Err(e)) => return Poll::Ready(Some(Err(SignedPayloadStreamError::Stream(e)))),
				None => {
					if this.buf.is_empty() {
						return Poll::Ready(None);
					}
				}
			}

			let input: &[u8] = this.buf;

			let (input, header) = try_parse!(payload::Header::parse(input));

			// 0-sized chunk is the last
			if header.size == 0 {
				this.buf.clear();
				return Poll::Ready(None);
			}

			let (input, data) = try_parse!(take::<_, _, nom::error::Error<_>>(header.size)(input));
			let (input, _) = try_parse!(tag::<_, _, nom::error::Error<_>>("\r\n")(input));

			let data = Bytes::from(data.to_vec());
			let data_sha256sum = sha256sum(&data);

			let expected_signature = compute_streaming_payload_signature(
				this.garage,
				this.signing_hmac,
				*this.datetime,
				*this.previous_signature,
				data_sha256sum,
			)
			.map_err(|e| {
				SignedPayloadStreamError::Message(format!("Could not build signature: {}", e))
			})?;

			if header.signature != expected_signature {
				return Poll::Ready(Some(Err(SignedPayloadStreamError::InvalidSignature)));
			}

			*this.buf = input.into();
			*this.previous_signature = header.signature;

			return Poll::Ready(Some(Ok(data)));
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.stream.size_hint()
	}
}
