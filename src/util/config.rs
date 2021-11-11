//! Contains type and functions related to Garage configuration file
use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::de::Error as SerdeError;
use serde::{de, Deserialize};

use netapp::util::parse_and_resolve_peer_addr;
use netapp::NodeID;

use crate::error::Error;

/// Represent the whole configuration
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
	/// Path where to store metadata. Should be fast, but low volume
	pub metadata_dir: PathBuf,
	/// Path where to store data. Can be slower, but need higher volume
	pub data_dir: PathBuf,

	/// Size of data blocks to save to disk
	#[serde(default = "default_block_size")]
	pub block_size: usize,

	/// Replication mode. Supported values:
	/// - none, 1 -> no replication
	/// - 2 -> 2-way replication
	/// - 3 -> 3-way replication
	// (we can add more aliases for this later)
	pub replication_mode: String,

	/// RPC secret key: 32 bytes hex encoded
	pub rpc_secret: String,

	/// Address to bind for RPC
	pub rpc_bind_addr: SocketAddr,
	/// Public IP address of this node
	pub rpc_public_addr: Option<SocketAddr>,

	/// Bootstrap peers RPC address
	#[serde(deserialize_with = "deserialize_vec_addr", default)]
	pub bootstrap_peers: Vec<(NodeID, SocketAddr)>,
	/// Consul host to connect to to discover more peers
	pub consul_host: Option<String>,
	/// Consul service name to use
	pub consul_service_name: Option<String>,

	/// Sled cache size, in bytes
	#[serde(default = "default_sled_cache_capacity")]
	pub sled_cache_capacity: u64,

	/// Sled flush interval in milliseconds
	#[serde(default = "default_sled_flush_every_ms")]
	pub sled_flush_every_ms: u64,

	/// Configuration for S3 api
	pub s3_api: ApiConfig,

	/// Configuration for serving files as normal web server
	pub s3_web: WebConfig,
}

/// Configuration for S3 api
#[derive(Deserialize, Debug, Clone)]
pub struct ApiConfig {
	/// Address and port to bind for api serving
	pub api_bind_addr: SocketAddr,
	/// S3 region to use
	pub s3_region: String,
	/// Suffix to remove from domain name to find bucket. If None,
	/// vhost-style S3 request are disabled
	pub root_domain: Option<String>,
}

/// Configuration for serving files as normal web server
#[derive(Deserialize, Debug, Clone)]
pub struct WebConfig {
	/// Address and port to bind for web serving
	pub bind_addr: SocketAddr,
	/// Suffix to remove from domain name to find bucket
	pub root_domain: String,
	/// Suffix to add when user-agent request path end with "/"
	pub index: String,
}

fn default_sled_cache_capacity() -> u64 {
	128 * 1024 * 1024
}
fn default_sled_flush_every_ms() -> u64 {
	2000
}
fn default_block_size() -> usize {
	1048576
}

/// Read and parse configuration
pub fn read_config(config_file: PathBuf) -> Result<Config, Error> {
	let mut file = std::fs::OpenOptions::new()
		.read(true)
		.open(config_file.as_path())?;

	let mut config = String::new();
	file.read_to_string(&mut config)?;

	Ok(toml::from_str(&config)?)
}

fn deserialize_vec_addr<'de, D>(deserializer: D) -> Result<Vec<(NodeID, SocketAddr)>, D::Error>
where
	D: de::Deserializer<'de>,
{
	let mut ret = vec![];

	for peer in <Vec<&str>>::deserialize(deserializer)? {
		let (pubkey, addrs) = parse_and_resolve_peer_addr(peer).ok_or_else(|| {
			D::Error::custom(format!("Unable to parse or resolve peer: {}", peer))
		})?;
		for ip in addrs {
			ret.push((pubkey, ip));
		}
	}

	Ok(ret)
}
