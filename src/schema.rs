use super::types::DataType;
use super::types::DbError;
use std::collections::{hash_map::Entry, HashMap};
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Schema {
    pub tables: HashMap<String, Vec<(String, DataType)>>,
    pub name: String,
}

impl Schema {
    pub fn new(name: String) -> Self {
        Schema {
            tables: HashMap::new(),
            name,
        }
    }

    pub fn load(path: &Path) -> Result<Schema, io::Error> {
        let file = File::open(path.join("schema")).expect("Schema file not found");
        let mut reader = io::BufReader::new(file).lines();
        let mut tables = HashMap::new();
        let name = reader.next().expect("Schema file is empty")?;
        for line in reader {
            let line = line?;
            let (table, columns) = line.split_once('#').expect("Schema file corrupted");
            for column in columns.split(',') {
                let (column, data_type) = column.split_once(':').expect("Schema file corrupted");
                tables
                    .entry(table.to_string())
                    .or_insert_with(Vec::new)
                    .push((column.to_string(), data_type.into()));
            }
        }
        Ok(Schema { tables, name })
    }

    // TODO: dump the schema at some point
    pub fn dump(&self, path: &Path) -> Result<(), io::Error> {
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
        columns: Vec<(String, DataType)>,
    ) -> Result<(), DbError> {
        Self::validate_name(&name)?;
        if let Entry::Vacant(entry) = self.tables.entry(name.clone()) {
            columns
                .iter()
                .try_for_each(|(name, _)| Self::validate_name(name))?;
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
        add: Option<(String, DataType)>,
        drop: Option<String>,
        rename: Option<(String, String)>,
    ) -> Result<(), DbError> {
        if let Entry::Occupied(mut entry) = self.tables.entry(table.clone()) {
            if let Some(name) = drop {
                if let Some(index) = entry.get().iter().position(|(n, _)| n == &name) {
                    entry.get_mut().remove(index);
                } else {
                    return Err(DbError::ColumnNotFound(name, table));
                }
            }

            if let Some((old_name, new_name)) = rename {
                Self::validate_name(&new_name)?;
                if let Some(column) = entry.get_mut().iter_mut().find(|(n, _)| n == &old_name) {
                    column.0 = new_name;
                } else {
                    return Err(DbError::ColumnNotFound(old_name, table));
                }
            }

            if let Some((name, data_type)) = add {
                Self::validate_name(&name)?;
                if entry.get().iter().any(|(n, _)| n == &name) {
                    return Err(DbError::ColumnAlreadyExists(name, table));
                } else {
                    entry.get_mut().push((name, data_type));
                }
            }

            Ok(())
        } else {
            Err(DbError::TableNotFound(table))
        }
    }

    pub fn validate_name(name: &str) -> Result<(), DbError> {
        if name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Ok(())
        } else {
            Err(DbError::InvalidName(name.to_string()))
        }
    }
}
