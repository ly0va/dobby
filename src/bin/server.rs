use dobby::{core::Database, grpc, rest};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use structopt::StructOpt;

/// A database engine as poor as a house elf
#[derive(Debug, StructOpt)]
#[structopt(name = "dobby")]
struct Options {
    /// Path to the database directory
    #[structopt(parse(from_os_str))]
    path: PathBuf,

    /// Creates a new database called <name>
    #[structopt(long, name = "name")]
    new: Option<String>,

    /// gRPC server port
    #[structopt(long, default_value = "8080")]
    grpc_port: u16,

    /// REST server port
    #[structopt(long, default_value = "8081")]
    rest_port: u16,
}

#[tokio::main]
async fn main() {
    let opt = Options::from_args();

    let db = {
        let db = if let Some(name) = opt.new {
            Database::create(opt.path, name)
        } else {
            Database::open(opt.path)
        };
        // TODO: maybe better to use one mutex per table instead of a global one?
        Arc::new(Mutex::new(db))
    };

    tokio::select! {
        _ = grpc::serve(Arc::clone(&db), ([0, 0, 0, 0], opt.grpc_port)) => {},
        _ = rest::serve(Arc::clone(&db), ([0, 0, 0, 0], opt.rest_port)) => {},
        _ = tokio::signal::ctrl_c() => { println!("\nShutting down...") },
    };

    // TODO: add my custom types
    // TODO: add logging
    // TODO: add cleanup (remove all deleted entries)
    // TODO: return schemas as response to operations over tables
}
