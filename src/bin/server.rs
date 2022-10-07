use dobby::{core::Dobby, grpc, rest};
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

    /// run gRPC server on <port>
    #[structopt(long, name = "grpc-port")]
    grpc: Option<u16>,

    /// run REST server on <port>
    #[structopt(long, name = "rest-port")]
    rest: Option<u16>,
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    let options = Options::from_args();

    if options.grpc.is_none() && options.rest.is_none() {
        panic!("No server specified");
    }

    let db = {
        let db = if let Some(name) = options.new {
            Dobby::create(options.path, name)
        } else {
            Dobby::open(options.path)
        };
        // TODO: maybe better to use one mutex per table instead of a global one?
        Arc::new(Mutex::new(db))
    };

    let grpc_server = options
        .grpc
        .map(|port| grpc::serve(Arc::clone(&db), ([0, 0, 0, 0], port)));

    let rest_server = options
        .rest
        .map(|port| rest::serve(Arc::clone(&db), ([0, 0, 0, 0], port)));

    tokio::select! {
        _ = async { grpc_server.unwrap().await }, if grpc_server.is_some() => {},
        _ = async { rest_server.unwrap().await }, if rest_server.is_some() => {},
        _ = tokio::signal::ctrl_c() => {
            log::info!(target: "dobby::server", "Shutting down...");
        },
    };
}
