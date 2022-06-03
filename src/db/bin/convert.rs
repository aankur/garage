use std::path::PathBuf;

use garage_db::*;

use clap::{Parser};

/// K2V command line interface
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
	/// Input DB path
	#[clap(short = 'i')]
	input_path: PathBuf,
	/// Input DB engine
	#[clap(short = 'a')]
	input_engine: String,

	/// Output DB path
	#[clap(short = 'o')]
	output_path: PathBuf,
	/// Output DB engine
	#[clap(short = 'b')]
	output_engine: String,
}

fn main() {
	let args = Args::parse();
	match do_conversion(args) {
		Ok(()) => println!("Success!"),
		Err(e) => eprintln!("Error: {}", e),
	}
}

fn do_conversion(args: Args) -> Result<()> {
	let input = open_db(args.input_path, args.input_engine)?;
	let output = open_db(args.output_path, args.output_engine)?;
	output.import(&input)?;
	Ok(())
}

fn open_db(path: PathBuf, engine: String) -> Result<Db> {
	match engine.as_str() {
		"sled" => {
			let db = sled_adapter::sled::Config::default()
				.path(&path)
				.open()?;
			Ok(sled_adapter::SledDb::init(db))
		}
		"sqlite" | "rusqlite" => {
			let db = sqlite_adapter::rusqlite::Connection::open(&path)?;
			Ok(sqlite_adapter::SqliteDb::init(db))
		}
		e => Err(Error(format!("Invalid DB engine: {}", e).into())),
	}
}
