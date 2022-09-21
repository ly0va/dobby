mod core;
mod grpc;
mod rest;

use crate::core::Database;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    let db = Arc::new(Mutex::new(Database::open("test".into())));

    tokio::select! {
        _ = grpc::serve(Arc::clone(&db), ([0, 0, 0, 0], 50051)) => {},
        _ = rest::serve(Arc::clone(&db), ([0, 0, 0, 0],  3030)) => {}
    };

    // TODO: add config / cmd arg for the server port and db name
    // TODO: add logging
    // TODO: return schemas as response to operations over tables
}
