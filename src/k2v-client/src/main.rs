use k2v_client::*;
use rusoto_core::credential::{EnvironmentProvider, ProvideAwsCredentials};
use rusoto_core::Region;

#[tokio::main]
async fn main() -> Result<(), Error> {
	// TODO provide a CLI to perform queries
	let region = Region::Custom {
		name: "us-east-1".to_owned(),
		endpoint: "http://172.30.2.1:3903".to_owned(),
	};

	let creds = EnvironmentProvider::default().credentials().await.unwrap();

	let client = K2vClient::new(region, "my-bucket".to_owned(), creds, None)?;

	client.insert_item("pk", "sk", vec![0x12], None).await?;

	/*
	dbg!(client.read_item("pk", "sk").await?);

	client.delete_item("patate", "patate", "eFmifSwRtcl4WaJ9LBG1ywAAAAAAAAAC".to_owned().into()).await?;

	dbg!(client.read_index(Filter::default()).await?);

	client.insert_batch(&[
		BatchInsertOp {
			partition_key: "pk",
			sort_key: "sk1",
			causality: None,
			value: vec![1,2,3].into(),
		},
		BatchInsertOp {
			partition_key: "pk",
			sort_key: "sk2",
			causality: None,
			value: vec![1,2,4].into(),
		},
	]).await?;

	dbg!(client.read_batch(&[BatchReadOp { partition_key: "pk", ..BatchReadOp::default()}]).await?);

	dbg!(client.delete_batch(&[BatchDeleteOp::new("pk")]).await?);
	*/

	Ok(())
}
