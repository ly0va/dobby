use crate::core::table::Table;
use std::collections::HashMap;
use std::path::PathBuf;

use super::Schema;
use crate::core::types::{ColumnSet, DobbyError, Query};

#[derive(Debug)]
pub struct Dobby {
    tables: HashMap<String, Table>,
    path: PathBuf,
    pub schema: Schema,
}

impl Dobby {
    pub fn execute(&mut self, query: Query) -> Result<Vec<ColumnSet>, DobbyError> {
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
            Query::Drop { table } => {
                self.table(&table)?.drop()?;
                self.tables.remove(&table);
                self.schema.drop_table(table).map(|_| vec![])
            }
            Query::Alter { table, rename } => {
                self.schema.alter_table(table.clone(), rename)?;
                self.update_colunms(table);
                Ok(vec![])
            }
        }
    }

    pub fn open(path: PathBuf) -> Self {
        log::info!("Opening database at {:?}", path);
        if !path.is_dir() {
            panic!("Database not found at {:?}", path);
        }
        let schema = Schema::load(&path);
        assert!(schema.is_dobby(), "Wrong schema type");
        Dobby { tables: HashMap::new(), schema, path }
    }

    pub fn create(path: PathBuf, name: String) -> Self {
        log::info!("Creating database {} at {:?}", name, path);
        if path.exists() {
            panic!("Path {} already occupied", path.display());
        }
        std::fs::create_dir_all(&path).expect("Failed to create database directory");

        Dobby {
            tables: HashMap::new(),
            schema: Schema::new_dobby(name),
            path,
        }
    }

    fn table(&mut self, name: &str) -> Result<&mut Table, DobbyError> {
        if !self.schema.tables.contains_key(name) {
            return Err(DobbyError::TableNotFound(name.to_string()));
        }

        if !self.tables.contains_key(name) {
            let columns = self.schema.tables[name].clone();
            let table = Table::open(name.to_string(), columns, &self.path);
            self.tables.insert(name.to_string(), table);
        }

        Ok(self.tables.get_mut(name).unwrap())
    }

    fn update_colunms(&mut self, table: String) {
        self.tables
            .entry(table.clone())
            .and_modify(|e| e.columns = self.schema.tables[&table].clone());
    }
}

impl Drop for Dobby {
    fn drop(&mut self) {
        self.schema.dump(&self.path).expect("Failed to dump schema");
    }
}
