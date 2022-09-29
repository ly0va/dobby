use super::schema::Schema;
use super::table::Table;
use super::types::{ColumnSet, DbError, Query};

use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Database {
    tables: HashMap<String, Table>,
    path: PathBuf,
    schema: Schema,
}

impl Database {
    pub fn open(path: PathBuf) -> Self {
        let schema = Schema::load(&path);
        Database { tables: HashMap::new(), path, schema }
    }

    pub fn create(path: PathBuf, name: String) -> Self {
        if path.exists() {
            panic!("Path {} already occupied", path.display());
        }
        std::fs::create_dir_all(&path).expect("Failed to create database directory");
        let schema = Schema::new(name);
        Database { tables: HashMap::new(), path, schema }
    }

    pub fn table(&mut self, name: &str) -> Result<&mut Table, DbError> {
        if !self.schema.tables.contains_key(name) {
            return Err(DbError::TableNotFound(name.to_string()));
        }

        if !self.tables.contains_key(name) {
            let columns = self.schema.tables[name].clone();
            let table = Table::open(name.to_string(), columns, &self.path);
            self.tables.insert(name.to_string(), table);
        }

        Ok(self.tables.get_mut(name).unwrap())
    }

    pub fn execute(&mut self, query: Query) -> Result<Vec<ColumnSet>, DbError> {
        match query {
            Query::Select { from, columns, conditions } => {
                self.table(&from)?.select(columns, conditions)
            }
            Query::Insert { into, values } => self.table(&into)?.insert(values).map(|v| vec![v]),
            Query::Update { table, set, conditions } => self.table(&table)?.update(set, conditions),
            Query::Delete { from, conditions } => self.table(&from)?.delete(conditions),
            Query::Create { table, columns } => {
                self.schema.create_table(table, columns).map(|_| vec![])
            }
            Query::Drop { table } => self.schema.drop_table(table).map(|_| vec![]),
            Query::Alter { table, rename } => {
                self.schema.alter_table(table, rename).map(|_| vec![])
            }
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.schema
            .dump(&self.path)
            .expect("Failed to save database schema");
    }
}
