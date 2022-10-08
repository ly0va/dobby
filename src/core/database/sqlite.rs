use super::Database;
use crate::core::schema::Schema;
use crate::core::types::{ColumnSet, DataType, DobbyError, Query, TypedValue};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

pub struct Sqlite {
    pub db: rusqlite::Connection,
    pub schema: Schema,
}

impl Sqlite {
    pub fn open(path: PathBuf) -> Self {
        let db = Connection::open(&path).expect("Failed to open database");
        Self { db: db.into(), schema: Schema::new("...".into()) }
    }

    pub fn create(path: PathBuf, name: String) -> Self {
        let db = Connection::open(&path).expect("Failed to open database");
        Self { db: db.into(), schema: Schema::new(name) }
    }
}

impl Sqlite {
    pub fn execute(&mut self, query: Query) -> Result<Vec<ColumnSet>, DobbyError> {
        match query {
            Query::Select { from, columns, conditions } => {
                let conditions = if conditions.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "WHERE {}",
                        conditions
                            .iter()
                            .map(|(column, value)| format!("{} = {:?}", column, value))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    )
                };

                let mut stmt = self.db.prepare(&format!(
                    "SELECT {} FROM {} {}",
                    columns.join(", "),
                    from,
                    conditions
                ))?;

                let columns: Vec<_> = stmt
                    .columns()
                    .iter()
                    .map(|c| {
                        let name = c.name().to_string();
                        let data_type = match c.decl_type() {
                            Some("INTEGER") => DataType::Int,
                            Some("TEXT") => DataType::String,
                            Some("REAL") => DataType::Float,
                            _ => unreachable!(),
                        };
                        let index = stmt.column_index(&name).unwrap();
                        (name, data_type, index)
                    })
                    .collect();

                let rows = stmt
                    .query_map([], |row| {
                        let mut result = HashMap::new();
                        for column in columns.iter() {
                            let (name, data_type, index) = column;
                            let value = match data_type {
                                DataType::Int => TypedValue::Int(row.get_unwrap(*index)),
                                DataType::String => TypedValue::String(row.get_unwrap(*index)),
                                DataType::Float => TypedValue::Float(row.get_unwrap(*index)),
                                _ => unreachable!(),
                            };

                            result.insert(name.clone(), value);
                        }
                        Ok(result)
                    })?
                    .collect::<Result<_, _>>()?;

                // TODO: coerce the types to the correct ones
                // FIXME i still have to maintain an extermal schema for this to work
                // - fix alter, rename and create
                // TODO add a RETURNING clause to delete, insert, update
                Ok(rows)
            }
            Query::Insert { into, values } => {
                let mut stmt = self.db.prepare(&format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    into,
                    values
                        .keys()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.keys().map(|_| "?").collect::<Vec<_>>().join(", ")
                ))?;
                // TODO: verify order is the same as of keys
                let values: Vec<_> = values.values().map(|v| v as &dyn rusqlite::ToSql).collect();
                stmt.execute(&values[..])?;
                Ok(vec![])
            }
            Query::Update { table, set: values, conditions } => {
                let conditions = if conditions.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "WHERE {}",
                        conditions
                            .iter()
                            .map(|(column, value)| format!("{} = {:?}", column, value))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    )
                };
                let mut stmt = self.db.prepare(&format!(
                    "UPDATE {} SET {} {}",
                    table,
                    values
                        .iter()
                        .map(|(column, value)| format!("{} = {:?}", column, value))
                        .collect::<Vec<_>>()
                        .join(", "),
                    conditions
                ))?;

                let values: Vec<_> = values.values().map(|v| v as &dyn rusqlite::ToSql).collect();
                stmt.execute(&values[..])?;
                Ok(vec![])
            }
            Query::Delete { from, conditions } => {
                let conditions = if conditions.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "WHERE {}",
                        conditions
                            .iter()
                            .map(|(column, value)| format!("{} = {:?}", column, value))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    )
                };
                let mut stmt = self
                    .db
                    .prepare(&format!("DELETE FROM {} {}", from, conditions))?;
                stmt.execute([])?;
                Ok(vec![])
            }
            Query::Drop { table } => {
                self.schema.drop_table(table.clone())?;
                let mut stmt = self.db.prepare(&format!("DROP TABLE {}", table))?;
                stmt.execute([])?;
                Ok(vec![])
            }
            Query::Create { table, columns } => {
                self.schema.create_table(table.clone(), columns.clone())?;
                let mut stmt = self.db.prepare(&format!(
                    "CREATE TABLE {} ({})",
                    table,
                    columns
                        .iter()
                        .map(|(name, data_type)| format!(
                            "{} {} NOT NULL",
                            name,
                            data_type.to_sql()
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))?;
                stmt.execute([])?;
                Ok(vec![])
            }
            Query::Alter { table, rename } => {
                self.schema.alter_table(table.clone(), rename.clone())?;
                // TODO: do this in a transaction / batch
                for (old, new) in rename {
                    let mut stmt = self.db.prepare(&format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        table, old, new
                    ))?;
                    stmt.execute([])?;
                }

                Ok(vec![])
            }
        }
    }
}
