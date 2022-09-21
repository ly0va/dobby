pub mod proto {
    tonic::include_proto!("database");
}

use proto::database_server::{self as service, DatabaseServer};
use proto::{query, typed_value};
use tonic::{transport::Server, Request, Response, Status};

use crate::core::types::{Query, TypedValue};
use crate::core::Database;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

pub struct DatabaseService {
    db: Arc<Mutex<Database>>,
}

#[tonic::async_trait]
impl service::Database for DatabaseService {
    async fn execute(
        &self,
        request: Request<proto::Query>,
    ) -> Result<Response<proto::Reply>, Status> {
        let query = request.into_inner();
        let db = Arc::clone(&self.db);
        if let Some(query) = query.query {
            match db.lock().unwrap().execute(query.into()) {
                Ok(result) => Ok(Response::new(proto::Reply {
                    rows: result
                        .into_iter()
                        .map(|row| proto::reply::Row {
                            data: row.into_iter().map(|(k, v)| (k, v.into())).collect(),
                        })
                        .collect(),
                })),
                // TODO: properly match errors
                Err(err) => Err(Status::internal(err.to_string())),
            }
        } else {
            Err(Status::invalid_argument("Query is empty"))
        }
    }
}

pub async fn serve(
    db: Arc<Mutex<Database>>,
    address: impl Into<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = DatabaseService { db: Arc::clone(&db) };

    Server::builder()
        .add_service(DatabaseServer::new(service))
        .serve(address.into())
        .await?;

    Ok(())
}

impl From<proto::query::Query> for Query {
    fn from(query: query::Query) -> Self {
        let convert = |field_set: HashMap<String, proto::TypedValue>| {
            field_set
                .into_iter()
                .filter_map(|(k, v)| v.data.map(|v| (k, v.into())))
                .collect()
        };

        match query {
            query::Query::Select(select) => Query::Select {
                from: select.from,
                columns: select.columns,
                conditions: convert(select.conditions),
            },
            query::Query::Insert(insert) => {
                Query::Insert { into: insert.into, values: convert(insert.values) }
            }
            query::Query::Update(update) => Query::Update {
                table: update.table,
                set: convert(update.set),
                conditions: convert(update.conditions),
            },
            query::Query::Delete(delete) => Query::Delete {
                from: delete.from,
                conditions: convert(delete.conditions),
            },
            query::Query::Drop(drop) => Query::Drop { table: drop.table },
            query::Query::Alter(alter) => Query::Alter { table: alter.table, rename: alter.rename },
        }
    }
}

impl From<typed_value::Data> for TypedValue {
    fn from(data: typed_value::Data) -> Self {
        match data {
            typed_value::Data::IntData(i) => TypedValue::Int(i),
            typed_value::Data::FloatData(f) => TypedValue::Float(f),
            typed_value::Data::StringData(s) => TypedValue::Str(s),
        }
    }
}

impl From<TypedValue> for proto::TypedValue {
    fn from(value: TypedValue) -> Self {
        match value {
            TypedValue::Int(i) => proto::TypedValue { data: Some(typed_value::Data::IntData(i)) },
            TypedValue::Float(f) => {
                proto::TypedValue { data: Some(typed_value::Data::FloatData(f)) }
            }
            TypedValue::Char(c) => proto::TypedValue {
                data: Some(typed_value::Data::StringData(c.to_string())),
            },
            TypedValue::Str(s) => {
                proto::TypedValue { data: Some(typed_value::Data::StringData(s)) }
            }
        }
    }
}
