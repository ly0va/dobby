use super::schema::Schema;
use super::table::Table;
use super::types::{DbError, Query};

use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
    pub path: PathBuf,
    pub schema: Schema,
}

impl Database {
    pub fn open(path: PathBuf) -> Self {
        let schema = Schema::load(&path).expect("Failed to load schema");
        Database {
            tables: HashMap::new(),
            path,
            schema,
        }
    }

    pub fn create(path: PathBuf, name: String) -> Self {
        let schema = Schema::new(name);
        Database {
            tables: HashMap::new(),
            path,
            schema,
        }
    }

    pub fn table(&mut self, name: &str) -> Result<&mut Table, DbError> {
        if !self.tables.contains_key(name) {
            if !self.schema.tables.contains_key(name) {
                return Err(DbError::TableNotFound(name.to_string()));
            }
            let columns = self.schema.tables[name].clone();
            let table = Table::open(name.to_string(), columns, &self.path);
            self.tables.insert(name.to_string(), table);
        }

        Ok(self.tables.get_mut(name).unwrap())
    }

    pub fn execute(&mut self, query: Query) -> Result<(), DbError> {
        match query {
            Query::Select {
                from,
                columns,
                conditions,
            } => {
                self.table(&from)?.select(columns, conditions)?;
            }
            Query::Insert { into, values } => {
                self.table(&into)?.insert(values)?;
            }
            Query::Update {
                table,
                set,
                conditions,
            } => {
                self.table(&table)?.update(set, conditions)?;
            }
            Query::Delete { from, conditions } => {
                self.table(&from)?.delete(conditions)?;
            }
            Query::Create { table, columns } => {
                self.schema.create_table(table, columns)?;
            }
            Query::Drop { table } => {
                self.schema.drop_table(table)?;
            }
            Query::Alter {
                table,
                add,
                drop,
                rename,
            } => {
                self.schema.alter_table(table, add, drop, rename)?;
            }
        }
        Ok(())
    }
}
