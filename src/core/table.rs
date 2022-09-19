use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::types::{DataType, DbError, FieldSet, TypedValue};

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<(String, DataType)>,
    pub file: File,
}

#[derive(Debug, Clone)]
struct Row {
    row: FieldSet,
    offset: u64,
}

impl Table {
    fn next_row(&mut self) -> Option<Result<Row, io::Error>> {
        let mut row = HashMap::new();
        let mut deleted = [0];
        let mut offset;
        loop {
            offset = self.file.seek(SeekFrom::Current(0)).unwrap();
            self.file.read_exact(&mut deleted).ok()?;

            for (column, data_type) in &self.columns {
                let value = TypedValue::read(*data_type, &mut self.file);
                match value {
                    Ok(value) => row.insert(column.clone(), value),
                    Err(e) => return Some(Err(e)),
                };
            }

            if deleted[0] == 0 {
                break;
            }
        }

        Some(Ok(Row { offset, row }))
    }

    fn delete_at(&mut self, offset: u64) -> Result<(), io::Error> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&[1])?;
        self.file.seek(SeekFrom::Current(-1))?;
        Ok(())
    }

    pub fn open(name: String, columns: Vec<(String, DataType)>, path: &Path) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join(name.clone()))
            .expect("Failed to open table");
        Self {
            name,
            columns,
            file,
        }
    }

    pub fn insert(&mut self, values: HashMap<String, TypedValue>) -> Result<(), DbError> {
        let mut row = vec![0]; // 0 - "not deleted"
        for (name, data_type) in &self.columns {
            let value = values
                .get(name)
                .ok_or_else(|| DbError::IncompleteData(name.clone(), self.name.clone()))?;
            if value.data_type() != *data_type {
                return Err(DbError::InvalidValue(value.clone(), *data_type));
            }
            row.extend_from_slice(&value.clone().into_bytes());
        }

        self.file.seek(SeekFrom::End(0)).map_err(DbError::IoError)?;
        self.file.write_all(&row).map_err(DbError::IoError)
    }

    pub fn select(
        &mut self,
        columns: Vec<String>,
        conditions: FieldSet,
    ) -> Result<Vec<FieldSet>, DbError> {
        let mut result = Vec::new();
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(DbError::IoError)?;
        'outer: while let Some(row) = self.next_row() {
            let Row { mut row, .. } = row.map_err(DbError::IoError)?;
            for (column, value) in &conditions {
                if let Some(row_value) = row.get(column) {
                    if row_value != value {
                        continue 'outer;
                    }
                } else {
                    return Err(DbError::ColumnNotFound(column.clone(), self.name.clone()));
                }
            }
            let mut selected = HashMap::new();
            for column in &columns {
                selected.insert(
                    column.clone(),
                    row.remove(column).ok_or_else(|| {
                        DbError::ColumnNotFound(column.clone(), self.name.clone())
                    })?,
                );
            }
            result.push(selected);
        }
        Ok(result)
    }

    pub fn update(&mut self, set: FieldSet, conditions: FieldSet) -> Result<(), DbError> {
        let eof = self.file.seek(SeekFrom::End(0)).map_err(DbError::IoError)?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(DbError::IoError)?;
        'outer: while let Some(row) = self.next_row() {
            let Row { offset, mut row } = row.map_err(DbError::IoError)?;
            if offset == eof {
                break;
            }
            for (column, value) in &conditions {
                if let Some(row_value) = row.get(column) {
                    if row_value != value {
                        continue 'outer;
                    }
                } else {
                    return Err(DbError::ColumnNotFound(column.clone(), self.name.clone()));
                }
            }
            let mut updated = false;
            for (column, value) in &set {
                if !row.contains_key(column) {
                    return Err(DbError::ColumnNotFound(column.clone(), self.name.clone()));
                }
                let old_value = row.insert(column.clone(), value.clone());
                updated = updated || old_value != Some(value.clone());
            }
            if updated {
                self.insert(row)?;
                self.delete_at(offset).map_err(DbError::IoError)?;
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, conditions: FieldSet) -> Result<(), DbError> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(DbError::IoError)?;
        'outer: while let Some(row) = self.next_row() {
            let Row { offset, row } = row.map_err(DbError::IoError)?;
            for (column, value) in &conditions {
                if let Some(row_value) = row.get(column) {
                    if row_value != value {
                        continue 'outer;
                    }
                } else {
                    return Err(DbError::ColumnNotFound(column.clone(), self.name.clone()));
                }
            }
            self.delete_at(offset).map_err(DbError::IoError)?;
        }
        Ok(())
    }
}
