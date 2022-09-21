mod core;

use crate::core::database::Database;
use crate::core::types::{DataType, DbError, FieldSet, Query};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use warp::Filter;

#[tokio::main]
async fn main() {
    let db_itself = Arc::new(Mutex::new(Database::open("test".into())));

    let db = Arc::clone(&db_itself);
    let select = warp::get()
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<FieldSet>())
        .and_then(move |from: String, conditions: FieldSet| {
            let db = Arc::clone(&db);
            execute_on(db, Query::Select { from, conditions, columns: vec![] })
        });

    let db = Arc::clone(&db_itself);
    let insert = warp::post()
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |into: String, values: FieldSet| {
            let db = Arc::clone(&db);
            execute_on(db, Query::Insert { into, values })
        });

    let db = Arc::clone(&db_itself);
    let update = warp::put()
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<FieldSet>())
        .and(warp::body::json())
        .and_then(move |table: String, conditions: FieldSet, set: FieldSet| {
            let db = Arc::clone(&db);
            execute_on(db, Query::Update { table, conditions, set })
        });

    let db = Arc::clone(&db_itself);
    let delete = warp::delete()
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<FieldSet>())
        .and_then(move |from: String, conditions: FieldSet| {
            let db = Arc::clone(&db);
            execute_on(db, Query::Delete { from, conditions })
        });

    let db = Arc::clone(&db_itself);
    let drop = warp::delete()
        .and(warp::path::param())
        .and(warp::path("drop"))
        .and(warp::path::end())
        .and_then(move |table: String| {
            let db = Arc::clone(&db);
            execute_on(db, Query::Drop { table })
        });

    let db = Arc::clone(&db_itself);
    let create = warp::post()
        .and(warp::path::param())
        .and(warp::path("create"))
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |table: String, columns: HashMap<String, DataType>| {
            let db = Arc::clone(&db);
            let columns = Vec::from_iter(columns.into_iter());
            execute_on(db, Query::Create { table, columns })
        });

    let db = Arc::clone(&db_itself);
    let alter = warp::put()
        .and(warp::path::param())
        .and(warp::path("alter"))
        .and(warp::path::end())
        .and(warp::query::<HashMap<String, String>>())
        .and_then(move |table: String, rename: HashMap<String, String>| {
            let db = Arc::clone(&db);
            execute_on(db, Query::Alter { table, rename })
        });

    let routes = select
        .or(insert)
        .or(update)
        .or(delete)
        .or(drop)
        .or(create)
        .or(alter)
        .recover(handle_rejection);

    warp::serve(routes).run(([0, 0, 0, 0], 3030)).await;

    // TODO: add config / cmd arg for the server port and db name
    // TODO: add logging
    // TODO: return schemas as response to operations over tables
}

async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    if let Some(error) = err.find::<DbError>() {
        Ok(warp::reply::with_status(
            warp::reply::json(&error),
            error.status_code(),
        ))
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&"Invalid request"),
            warp::http::StatusCode::BAD_REQUEST,
        ))
    }
}

async fn execute_on(
    db: Arc<Mutex<Database>>,
    query: Query,
) -> Result<impl warp::Reply, warp::Rejection> {
    dbg!(&query);
    let result = db.lock().unwrap().execute(query)?;
    Ok(warp::reply::json(&result))
}
