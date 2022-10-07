use super::schema::Schema;
use super::table::Table;
use super::types::{ColumnSet, DobbyError, Query};

use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Dobby {
    tables: HashMap<String, Table>,
    path: PathBuf,
    pub schema: Schema,
}

impl Dobby {
    pub fn open(path: PathBuf) -> Self {
        log::info!("Opening database at {:?}", path);
        if !path.is_dir() {
            panic!("Database not found at {:?}", path);
        }
        let schema = Schema::load(&path);
        Dobby { tables: HashMap::new(), path, schema }
    }

    pub fn create(path: PathBuf, name: String) -> Self {
        log::info!("Creating database {} at {:?}", name, path);
        if path.exists() {
            panic!("Path {} already occupied", path.display());
        }
        std::fs::create_dir_all(&path).expect("Failed to create database directory");
        let schema = Schema::new(name);
        Dobby { tables: HashMap::new(), path, schema }
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
}

impl Drop for Dobby {
    fn drop(&mut self) {
        self.schema
            .dump(&self.path)
            .expect("Failed to save database schema");
    }
}
