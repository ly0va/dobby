use proto::database_server::{self as service, DatabaseServer};
use proto::{query, typed_value};
use tonic::{transport::Server, Request, Response, Status};

use crate::core::types::{DbError, FieldSet, Query, TypedValue};
use crate::core::Database;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[allow(clippy::derive_partial_eq_without_eq)]
pub mod proto {
    tonic::include_proto!("database");
}

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
                Ok(result) => Ok(Response::new(result.into())),
                Err(err) => Err(err.into()),
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
    let service = DatabaseService { db };

    Server::builder()
        .add_service(DatabaseServer::new(service))
        .serve(address.into())
        .await?;

    Ok(())
}

impl From<DbError> for Status {
    fn from(err: DbError) -> Self {
        match &err {
            DbError::TableNotFound(_) => Status::not_found(err.to_string()),
            DbError::ColumnNotFound(_, _) => Status::not_found(err.to_string()),
            DbError::TableAlreadyExists(_) => Status::already_exists(err.to_string()),
            DbError::ColumnAlreadyExists(_, _) => Status::already_exists(err.to_string()),
            DbError::NoColumns => Status::invalid_argument(err.to_string()),
            DbError::InvalidName(_) => Status::invalid_argument(err.to_string()),
            DbError::InvalidValue(_, _) => Status::invalid_argument(err.to_string()),
            DbError::InvalidDataType(_) => Status::invalid_argument(err.to_string()),
            DbError::IncompleteData(_, _) => Status::invalid_argument(err.to_string()),
            DbError::IoError(_) => Status::internal(err.to_string()),
        }
    }
}

impl From<Vec<FieldSet>> for proto::Reply {
    fn from(rows: Vec<FieldSet>) -> Self {
        proto::Reply {
            rows: rows
                .into_iter()
                .map(|row| proto::reply::Row {
                    data: row.into_iter().map(|(k, v)| (k, v.into())).collect(),
                })
                .collect(),
        }
    }
}

impl From<proto::Reply> for Vec<FieldSet> {
    fn from(reply: proto::Reply) -> Self {
        reply
            .rows
            .into_iter()
            .map(|row| {
                row.data
                    .into_iter()
                    .filter_map(|(k, v)| v.data.map(|v| (k, v.into())))
                    .collect()
            })
            .collect()
    }
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
            query::Query::Create(create) => Query::Create {
                table: create.table,
                columns: create
                    .columns
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            },
        }
    }
}

impl From<typed_value::Data> for TypedValue {
    fn from(data: typed_value::Data) -> Self {
        match data {
            typed_value::Data::Int(i) => TypedValue::Int(i),
            typed_value::Data::Float(f) => TypedValue::Float(f),
            typed_value::Data::Str(s) => TypedValue::Str(s),
        }
    }
}

impl From<TypedValue> for proto::TypedValue {
    fn from(value: TypedValue) -> Self {
        match value {
            TypedValue::Int(i) => proto::TypedValue { data: Some(typed_value::Data::Int(i)) },
            TypedValue::Float(f) => proto::TypedValue { data: Some(typed_value::Data::Float(f)) },
            TypedValue::Char(c) => {
                proto::TypedValue { data: Some(typed_value::Data::Str(c.to_string())) }
            }
            TypedValue::Str(s) => proto::TypedValue { data: Some(typed_value::Data::Str(s)) },
        }
    }
}
