use super::types::DataType;
use super::types::DbError;

use std::collections::{hash_map::Entry, HashMap};
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Schema {
    pub tables: HashMap<String, Vec<(String, DataType)>>,
    name: String,
}

impl Schema {
    pub fn new(name: String) -> Self {
        Schema { tables: HashMap::new(), name }
    }

    pub fn load(path: &Path) -> Schema {
        log::info!("Loading schema...");
        let file = File::open(path.join(".schema")).expect("Schema file not found");
        let mut reader = io::BufReader::new(file).lines();
        let mut tables = HashMap::new();
        let name = reader
            .next()
            .expect("Schema file is empty")
            .expect("Failed to read schema file");
        for line in reader {
            let line = line.expect("Failed to read schema file");
            let (table, columns) = line.split_once('#').expect("Schema file corrupted");
            for column in columns.split(',') {
                let (column, data_type) = column.split_once(':').expect("Schema file corrupted");
                tables
                    .entry(table.to_string())
                    .or_insert_with(Vec::new)
                    .push((
                        column.to_string(),
                        data_type.try_into().expect("Schema file corrupted"),
                    ));
            }
        }
        Schema { tables, name }
    }

    pub fn dump(&self, path: &Path) -> Result<(), io::Error> {
        log::info!("Dumping schema...");
        let mut file = File::create(path.join(".schema"))?;
        file.write_all(self.name.as_bytes())?;
        file.write_all(b"\n")?;
        for (table, columns) in &self.tables {
            let mut table_schema: String = columns
                .iter()
                .map(|(column, data_type)| format!("{}:{:?},", column, data_type))
                .collect();
            table_schema.pop();
            file.write_all(format!("{}#{}\n", table, table_schema).as_bytes())?;
        }
        Ok(())
    }

    pub fn create_table(
        &mut self,
        name: String,
        mut columns: Vec<(String, DataType)>,
    ) -> Result<(), DbError> {
        Self::validate_name(&name)?;
        if columns.is_empty() {
            return Err(DbError::NoColumns);
        }
        if let Entry::Vacant(entry) = self.tables.entry(name.clone()) {
            columns.sort();
            for (i, (column, _)) in columns.iter().enumerate() {
                Self::validate_name(column)?;
                if i > 0 && column == &columns[i - 1].0 {
                    return Err(DbError::ColumnAlreadyExists(column.clone(), name));
                }
            }
            entry.insert(columns);
            Ok(())
        } else {
            Err(DbError::TableAlreadyExists(name))
        }
    }

    pub fn drop_table(&mut self, name: String) -> Result<(), DbError> {
        if let Entry::Occupied(entry) = self.tables.entry(name.clone()) {
            entry.remove();
            Ok(())
        } else {
            Err(DbError::TableNotFound(name))
        }
    }

    pub fn alter_table(
        &mut self,
        table: String,
        mut rename: HashMap<String, String>,
    ) -> Result<(), DbError> {
        if let Entry::Occupied(mut entry) = self.tables.entry(table.clone()) {
            let mut new_columns = Vec::new();

            for (column, data_type) in entry.get().iter() {
                let new_column = if rename.contains_key(column) {
                    Self::validate_name(&rename[column])?;
                    rename.remove(column).unwrap()
                } else {
                    column.clone()
                };
                if new_columns.iter().any(|(c, _)| c == &new_column) {
                    return Err(DbError::ColumnAlreadyExists(new_column, table));
                }
                new_columns.push((new_column, *data_type));
            }

            if !rename.is_empty() {
                Err(DbError::ColumnNotFound(
                    rename.keys().next().unwrap().clone(),
                    table,
                ))
            } else {
                entry.insert(new_columns);
                Ok(())
            }
        } else {
            Err(DbError::TableNotFound(table))
        }
    }

    fn validate_name(name: &str) -> Result<(), DbError> {
        if name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Ok(())
        } else {
            Err(DbError::InvalidName(name.to_string()))
        }
    }
}
