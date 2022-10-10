use crate::core::types::{ColumnSet, DataType, DobbyError, Query};
use crate::core::Database;

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use once_cell::sync::Lazy;
use warp::http::StatusCode;
use warp::Filter;

impl warp::reject::Reject for DobbyError {}

static OPENAPI_SPEC: Lazy<serde_json::Value> = Lazy::new(|| {
    let spec = include_str!("../openapi.yaml");
    serde_yaml::from_str(spec).unwrap()
});

impl DobbyError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            DobbyError::TableAlreadyExists(_) => StatusCode::CONFLICT,
            DobbyError::TableNotFound(_) => StatusCode::NOT_FOUND,
            DobbyError::ColumnAlreadyExists(_, _) => StatusCode::CONFLICT,
            DobbyError::ColumnNotFound(_, _) => StatusCode::NOT_FOUND,
            DobbyError::NoColumns => StatusCode::BAD_REQUEST,
            DobbyError::InvalidName(_) => StatusCode::BAD_REQUEST,
            DobbyError::InvalidValue(_, _) => StatusCode::BAD_REQUEST,
            DobbyError::IncompleteData(_, _) => StatusCode::BAD_REQUEST,
            DobbyError::InvalidDataType(_) => StatusCode::BAD_REQUEST,
            DobbyError::InvalidRange(_, _) => StatusCode::BAD_REQUEST,
            DobbyError::SqlError(_) => StatusCode::BAD_REQUEST,
            DobbyError::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

pub async fn serve(db_itself: Arc<dyn Database>, address: impl Into<SocketAddr>) {
    let db = Arc::clone(&db_itself);
    let select = warp::get()
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<ColumnSet>())
        .and_then(move |from: String, conditions: ColumnSet| {
            let db = Arc::clone(&db);
            execute_on(db, Query::Select { from, conditions, columns: vec![] })
        });

    let db = Arc::clone(&db_itself);
    let insert = warp::post()
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |into: String, values: ColumnSet| {
            let db = Arc::clone(&db);
            execute_on(db, Query::Insert { into, values })
        })
        .map(|reply| warp::reply::with_status(reply, StatusCode::CREATED));

    let db = Arc::clone(&db_itself);
    let update = warp::put()
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<ColumnSet>())
        .and(warp::body::json())
        .and_then(
            move |table: String, conditions: ColumnSet, set: ColumnSet| {
                let db = Arc::clone(&db);
                execute_on(db, Query::Update { table, conditions, set })
            },
        );

    let db = Arc::clone(&db_itself);
    let delete = warp::delete()
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<ColumnSet>())
        .and_then(move |from: String, conditions: ColumnSet| {
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
        })
        .map(|reply| warp::reply::with_status(reply, StatusCode::CREATED));

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

    let db = Arc::clone(&db_itself);
    let schema = warp::get()
        .and(warp::path(".schema"))
        .and(warp::path::end())
        .map(move || warp::reply::json(&db.schema()));

    let openapi = warp::get()
        .and(warp::path("openapi.json"))
        .and(warp::path::end())
        .map(|| warp::reply::json(&*OPENAPI_SPEC));

    let index = warp::get()
        .and(warp::path::end())
        .map(|| warp::reply::html(include_str!("../static/index.html")));

    let routes = select
        .or(insert)
        .or(update)
        .or(delete)
        .or(drop)
        .or(create)
        .or(alter)
        .or(schema)
        .or(openapi)
        .or(index)
        .with(warp::log("api::rest"))
        .recover(handle_rejection);

    warp::serve(routes).run(address).await;
}

async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    if let Some(error) = err.find::<DobbyError>() {
        Ok(warp::reply::with_status(
            warp::reply::json(&error),
            error.status_code(),
        ))
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&"Invalid request"),
            StatusCode::BAD_REQUEST,
        ))
    }
}

async fn execute_on(
    db: Arc<dyn Database>,
    query: Query,
) -> Result<impl warp::Reply, warp::Rejection> {
    let result = db.execute(query)?;
    Ok(warp::reply::json(&result))
}
