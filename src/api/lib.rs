//! Crate for serving a S3 compatible API
#[macro_use]
extern crate tracing;

pub mod error;
pub use error::Error;
pub mod helpers;

mod encoding;

/// This mode is public only to help testing. Don't expect stability here
pub mod signature;

pub mod s3;

