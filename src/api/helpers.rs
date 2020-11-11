use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Response};

use garage_util::error::Error as GarageError;

pub trait InfallibleResult {
	fn make_infallible(self) -> Result<Response<Body>, Infallible>;
}

impl InfallibleResult for Result<Response<Body>, GarageError> {
	fn make_infallible(self) -> Result<Response<Body>, Infallible> {
		match self {
			Ok(x) => {
			  debug!("{} {:?}", x.status(), x.headers());
				Ok(x)
			},
			Err(e) => {
				let body: Body = Body::from(format!("{}\n", e));
				let mut http_error = Response::new(body);
				*http_error.status_mut() = e.http_status_code();
				warn!("Response: error {}, {}", e.http_status_code(), e);
				Ok(http_error)
			}
		}
	}
}
