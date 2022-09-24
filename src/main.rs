mod core;
mod grpc;
mod rest;

use crate::core::Database;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "dobby", about = "A database as poor as a house elf.")]
struct Options {
    #[structopt(parse(from_os_str), help = "Path to the database directory")]
    path: PathBuf,

    #[structopt(long, name = "name", help = "Creates a new database called <name>")]
    new: Option<String>,

    #[structopt(long, default_value = "8080", help = "gRPC server port")]
    grpc_port: u16,

    #[structopt(long, default_value = "8081", help = "REST server port")]
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
    // TODO: implement gRPC client
    // TODO: add cleanup (remove all deleted entries)
    // TODO: return schemas as response to operations over tables
}
