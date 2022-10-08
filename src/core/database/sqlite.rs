use crate::core::schema::Schema;
use crate::core::types::{ColumnSet, DataType, DobbyError, Query, TypedValue};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct Sqlite {
    db: rusqlite::Connection,
    path: PathBuf,
    pub schema: Schema,
}

impl Query {
    fn sql_conditions(&self) -> String {
        let conditions = match self {
            Query::Select { conditions, .. } => conditions,
            Query::Update { conditions, .. } => conditions,
            Query::Delete { conditions, .. } => conditions,
            _ => return String::new(),
        };

        if conditions.is_empty() {
            String::new()
        } else {
            format!(
                "WHERE {}",
                conditions
                    .iter()
                    .map(|(column, value)| format!("{} = {:?}", column, value))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            )
        }
    }

    fn to_sql(&self) -> String {
        match self {
            Query::Select { columns, from, .. } => {
                format!(
                    "SELECT {} FROM {} {}",
                    if columns.is_empty() {
                        "*".into()
                    } else {
                        columns.join(", ")
                    },
                    from,
                    self.sql_conditions()
                )
            }
            Query::Insert { into, values } => format!(
                "INSERT INTO {} ({}) VALUES ({})",
                into,
                values
                    .keys()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                values.keys().map(|_| "?").collect::<Vec<_>>().join(", ")
            ),
            Query::Update { table, set, .. } => format!(
                "UPDATE {} SET {} {}",
                table,
                set.iter()
                    .map(|(column, value)| format!("{} = {:?}", column, value))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.sql_conditions()
            ),
            Query::Delete { from, .. } => format!("DELETE FROM {} {}", from, self.sql_conditions()),
            Query::Drop { table } => format!("DROP TABLE {}", table),
            Query::Create { table, columns } => format!(
                "CREATE TABLE {} ({})",
                table,
                columns
                    .iter()
                    .map(|(name, data_type)| format!("{} {} NOT NULL", name, data_type.to_sql()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Query::Alter { .. } => "".into(),
        }
    }
}

impl Sqlite {
    pub fn open(path: PathBuf) -> Self {
        log::info!("Opening SQLite database at {:?}", path);
        let sqlite_path = path.join("db.sqlite");
        if !path.is_dir() || !sqlite_path.exists() {
            panic!("Database not found at {:?}", path);
        }
        let db = Connection::open(&sqlite_path).expect("Failed to open database");
        let schema = Schema::load(&path);
        assert!(schema.is_sqlite(), "Wrong schema type");
        Self { db, schema, path }
    }

    pub fn create(path: PathBuf, name: String) -> Self {
        log::info!("Creating SQLite database {} at {:?}", name, path);
        if path.exists() {
            panic!("Path {} already occupied", path.display());
        }
        std::fs::create_dir_all(&path).expect("Failed to create database directory");
        let sqlite_path = path.join("db.sqlite");
        let db = Connection::open(&sqlite_path).expect("Failed to open database");
        Self { db, schema: Schema::new_sqlite(name), path }
    }

    pub fn execute(&mut self, query: Query) -> Result<Vec<ColumnSet>, DobbyError> {
        match &query {
            Query::Select { from, .. } => {
                let mut stmt = self.db.prepare(&query.to_sql())?;
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

                let mut rows: Vec<ColumnSet> = stmt
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

                for row in rows.iter_mut() {
                    for (column, data_type) in self.schema.tables[from].iter() {
                        if row.contains_key(column) {
                            let value = row[column].clone();
                            row.insert(column.clone(), value.coerce(*data_type)?);
                        }
                    }
                }
                // TODO add a RETURNING clause to delete, insert, update
                Ok(rows)
            }
            Query::Insert { values, into } => {
                let mut stmt = self.db.prepare(&query.to_sql())?;
                for (column, data_type) in self.schema.tables[into].iter() {
                    let value = values[column].clone();
                    value.coerce(*data_type)?.validate()?;
                }
                let values: Vec<_> = values.values().map(|v| v as &dyn rusqlite::ToSql).collect();
                stmt.execute(&values[..])?;
                Ok(vec![])
            }
            Query::Update { set, table, .. } => {
                let mut stmt = self.db.prepare(&query.to_sql())?;
                for (column, data_type) in self.schema.tables[table].iter() {
                    if set.contains_key(column) {
                        let value = set[column].clone();
                        value.coerce(*data_type)?.validate()?;
                    }
                }
                let values: Vec<_> = set.values().map(|v| v as &dyn rusqlite::ToSql).collect();
                stmt.execute(&values[..])?;
                Ok(vec![])
            }
            Query::Delete { .. } => {
                let mut stmt = self.db.prepare(&query.to_sql())?;
                stmt.execute([])?;
                Ok(vec![])
            }
            Query::Drop { table } => {
                let mut stmt = self.db.prepare(&query.to_sql())?;
                self.schema.drop_table(table.clone())?;
                stmt.execute([])?;
                Ok(vec![])
            }
            Query::Create { table, columns } => {
                let mut stmt = self.db.prepare(&query.to_sql())?;
                self.schema.create_table(table.clone(), columns.clone())?;
                stmt.execute([])?;
                Ok(vec![])
            }
            Query::Alter { table, rename } => {
                self.schema.alter_table(table.clone(), rename.clone())?;
                let tx = self.db.transaction()?;
                for (old, new) in rename {
                    let mut stmt = tx.prepare(&format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        table, old, new
                    ))?;
                    stmt.execute([])?;
                }
                tx.commit()?;

                Ok(vec![])
            }
        }
    }
}

impl Drop for Sqlite {
    fn drop(&mut self) {
        self.schema.dump(&self.path).expect("Failed to dump schema");
    }
}
