use super::types::{ColumnSet, DataType, DbError, TypedValue};

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<(String, DataType)>,
    pub file: File,
}

#[derive(Debug, Clone)]
struct Row {
    row: ColumnSet,
    offset: u64,
}

// TODO: add cleanup (remove all deleted entries)
impl Table {
    fn next_row(&mut self) -> Option<Result<Row, io::Error>> {
        let mut row = HashMap::new();
        let mut deleted = [0];
        let mut offset;
        loop {
            offset = self.file.seek(SeekFrom::Current(0)).unwrap();
            self.file.read_exact(&mut deleted).ok()?;

            for (column, data_type) in &self.columns {
                match TypedValue::read(*data_type, &mut self.file) {
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
        log::info!("Opening table `{}`", name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join(name.clone()))
            .expect("Failed to open table");
        Self { name, columns, file }
    }

    fn coerce(&self, mut column_set: ColumnSet) -> Result<ColumnSet, DbError> {
        let mut coerced = HashMap::new();
        for (column, data_type) in &self.columns {
            if let Some((column, value)) = column_set.remove_entry(column) {
                let value = value.coerce(*data_type)?;
                value.validate()?;
                coerced.insert(column, value);
            }
        }
        if column_set.is_empty() {
            Ok(coerced)
        } else {
            Err(DbError::ColumnNotFound(
                column_set.keys().next().unwrap().clone(),
                self.name.clone(),
            ))
        }
    }

    fn check_conditions(&self, row: &ColumnSet, conditions: &ColumnSet) -> Result<bool, DbError> {
        let mut result = true;
        for (column, value) in conditions {
            if let Some(row_value) = row.get(column) {
                result &= row_value == value;
            } else {
                return Err(DbError::ColumnNotFound(column.clone(), self.name.clone()));
            }
        }
        Ok(result)
    }

    pub fn insert(&mut self, values: ColumnSet) -> Result<ColumnSet, DbError> {
        let values = self.coerce(values)?;
        let mut row = vec![0]; // 0 - "not deleted"
        for (name, _type) in &self.columns {
            let value = values
                .get(name)
                .ok_or_else(|| DbError::IncompleteData(name.clone(), self.name.clone()))?;
            row.extend_from_slice(&value.clone().into_bytes());
        }

        self.file.seek(SeekFrom::End(0)).map_err(DbError::IoError)?;
        self.file.write_all(&row).map_err(DbError::IoError)?;
        Ok(values)
    }

    pub fn select(
        &mut self,
        columns: Vec<String>,
        conditions: ColumnSet,
    ) -> Result<Vec<ColumnSet>, DbError> {
        let conditions = self.coerce(conditions)?;
        let mut selected = Vec::new();
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(DbError::IoError)?;
        while let Some(row) = self.next_row() {
            let Row { mut row, .. } = row.map_err(DbError::IoError)?;

            if !self.check_conditions(&row, &conditions)? {
                continue;
            }

            for column in &columns {
                if !row.contains_key(column) {
                    return Err(DbError::ColumnNotFound(column.clone(), self.name.clone()));
                }
            }

            row.retain(|key, _| columns.is_empty() || columns.contains(key));
            selected.push(row);
        }
        Ok(selected)
    }

    pub fn update(
        &mut self,
        set: ColumnSet,
        conditions: ColumnSet,
    ) -> Result<Vec<ColumnSet>, DbError> {
        let set = self.coerce(set)?;
        let conditions = self.coerce(conditions)?;
        let mut updated = Vec::new();
        let eof = self.file.seek(SeekFrom::End(0)).map_err(DbError::IoError)?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(DbError::IoError)?;
        while let Some(row) = self.next_row() {
            let Row { offset, mut row } = row.map_err(DbError::IoError)?;

            if offset == eof {
                break;
            }

            if !self.check_conditions(&row, &conditions)? {
                continue;
            }

            let mut was_updated = false;
            for (column, value) in &set {
                if !row.contains_key(column) {
                    return Err(DbError::ColumnNotFound(column.clone(), self.name.clone()));
                }
                let old_value = row.insert(column.clone(), value.clone());
                was_updated |= old_value != Some(value.clone());
            }

            if was_updated {
                updated.push(row.clone());
                self.insert(row)?;
                self.delete_at(offset).map_err(DbError::IoError)?;
            }
        }
        Ok(updated)
    }

    pub fn delete(&mut self, conditions: ColumnSet) -> Result<Vec<ColumnSet>, DbError> {
        let conditions = self.coerce(conditions)?;
        let mut deleted = Vec::new();
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(DbError::IoError)?;
        while let Some(row) = self.next_row() {
            let Row { offset, row } = row.map_err(DbError::IoError)?;
            if !self.check_conditions(&row, &conditions)? {
                continue;
            }
            deleted.push(row);
            self.delete_at(offset).map_err(DbError::IoError)?;
        }
        Ok(deleted)
    }

    pub fn drop(&mut self) -> Result<(), DbError> {
        self.file.set_len(0).map_err(DbError::IoError)
    }
}
