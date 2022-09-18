use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::types::{Bytes, DataType, DbError};

pub struct Table {
    pub name: String,
    pub columns: Vec<(String, DataType)>,
    pub file: std::fs::File,
}

struct Row {
    row: HashMap<String, Bytes>,
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
                let mut buffer;
                let result = match data_type {
                    DataType::Int | DataType::Float => {
                        buffer = vec![0; 8];
                        self.file.read_exact(&mut buffer)
                    }
                    DataType::Char => {
                        buffer = vec![0; 1];
                        self.file.read_exact(&mut buffer)
                    }
                    DataType::Str => {
                        let mut length = [0; 8];
                        match self.file.read_exact(&mut length) {
                            Ok(_) => {
                                let length = u64::from_le_bytes(length);
                                buffer = vec![0; length as usize];
                                self.file.read_exact(&mut buffer)
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                };
                match result {
                    Ok(_) => row.insert(column.clone(), buffer),
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
}

impl Table {
    pub fn open(name: String, columns: Vec<(String, DataType)>, path: &Path) -> Self {
        let file = std::fs::OpenOptions::new()
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

    pub fn insert(&mut self, values: HashMap<String, Bytes>) -> Result<(), DbError> {
        let mut row = Vec::new();
        for (name, data_type) in &self.columns {
            let value = values
                .get(name)
                .ok_or_else(|| DbError::ColumnNotFound(name.clone()))?;
            if !data_type.valid(value) {
                return Err(DbError::InvalidValue(format!(
                    "Invalid value for column {}: {:?}",
                    name, value
                )));
            }
            row.extend_from_slice(value);
        }

        self.file.seek(SeekFrom::End(0)).map_err(DbError::IoError)?;
        self.file.write_all(&row).map_err(DbError::IoError)
    }

    pub fn select(
        &mut self,
        columns: Vec<String>,
        conditions: HashMap<String, Bytes>,
    ) -> Result<Vec<HashMap<String, Bytes>>, DbError> {
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
                    return Err(DbError::ColumnNotFound(column.clone()));
                }
            }
            let mut selected = HashMap::new();
            for column in &columns {
                selected.insert(
                    column.clone(),
                    row.remove(column)
                        .ok_or_else(|| DbError::ColumnNotFound(column.clone()))?,
                );
            }
            result.push(selected);
        }
        Ok(result)
    }

    pub fn update(
        &mut self,
        set: HashMap<String, Bytes>,
        conditions: HashMap<String, Bytes>,
    ) -> Result<(), DbError> {
        'outer: while let Some(row) = self.next_row() {
            let Row { offset, mut row } = row.map_err(DbError::IoError)?;
            for (column, value) in &conditions {
                if let Some(row_value) = row.get(column) {
                    if row_value != value {
                        continue 'outer;
                    }
                } else {
                    return Err(DbError::ColumnNotFound(column.clone()));
                }
            }
            for (column, value) in &set {
                if !row.contains_key(column) {
                    return Err(DbError::ColumnNotFound(column.clone()));
                }
                row.insert(column.clone(), value.clone());
            }
            self.insert(row)?;
            self.delete_at(offset).map_err(DbError::IoError)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, conditions: HashMap<String, Bytes>) -> Result<(), DbError> {
        'outer: while let Some(row) = self.next_row() {
            let Row { offset, row } = row.map_err(DbError::IoError)?;
            for (column, value) in &conditions {
                if let Some(row_value) = row.get(column) {
                    if row_value != value {
                        continue 'outer;
                    }
                } else {
                    return Err(DbError::ColumnNotFound(column.clone()));
                }
            }
            self.delete_at(offset).map_err(DbError::IoError)?;
        }
        Ok(())
    }
}
